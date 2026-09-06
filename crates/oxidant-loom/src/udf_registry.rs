//! Session-scoped user-defined functions: SQL `CREATE FUNCTION`, Connect registration, and
//! worker-side JSON sync for distributed execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use oxidant_common::{Error, Result};
use regex::Regex;

/// Serializable UDF definition for worker sync.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UdfDef {
    pub name: String,
    pub sql_body: Option<String>,
    pub param_names: Vec<String>,
    pub return_type: String,
}

/// Per-engine UDF registry (SQL-defined + synced from driver).
#[derive(Debug, Default)]
pub struct UdfRegistry {
    defs: HashMap<String, UdfDef>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_sql_fn(&mut self, def: UdfDef) {
        self.defs.insert(def.name.to_lowercase(), def);
    }

    /// Validate and install `def` on `ctx`, then keep it in the session registry.
    ///
    /// Order matters: inserting first made a rejected body (`length(a)`, unknown
    /// identifier, unsupported return type) survive in `SHOW FUNCTIONS` / worker
    /// JSON even though DataFusion never got the UDF.
    pub fn register_sql_fn_on_context(&mut self, def: UdfDef, ctx: &SessionContext) -> Result<()> {
        register_sql_udf_on_ctx(ctx, &def)?;
        self.register_sql_fn(def);
        Ok(())
    }

    /// The (lowercased) names of every session UDF registered so far. Backs `SHOW FUNCTIONS`.
    pub fn names(&self) -> Vec<String> {
        self.defs.keys().cloned().collect()
    }

    /// Look up a session UDF's definition by (case-insensitive) name. Backs
    /// `DESCRIBE FUNCTION` reporting the SQL body for session-defined functions.
    pub fn get(&self, name: &str) -> Option<UdfDef> {
        self.defs.get(&name.to_lowercase()).cloned()
    }

    pub fn export_json(&self) -> String {
        let list: Vec<&UdfDef> = self.defs.values().collect();
        serde_json::to_string(&list).unwrap_or_else(|_| "[]".into())
    }

    pub fn import_json(&mut self, json: &str) -> Result<()> {
        let list: Vec<UdfDef> =
            serde_json::from_str(json).map_err(|e| Error::Plan(format!("udf json: {e}")))?;
        for def in list {
            self.register_sql_fn(def);
        }
        Ok(())
    }

    pub fn apply_to_context(&self, ctx: &SessionContext) -> Result<()> {
        for def in self.defs.values() {
            register_sql_udf_on_ctx(ctx, def)?;
        }
        Ok(())
    }
}

/// Parse and handle `CREATE [OR REPLACE] FUNCTION … RETURN …` (scalar, v1 subset).
pub fn try_create_function(sql: &str) -> Option<UdfDef> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            "(?is)^\\s*CREATE\\s+(?:OR\\s+REPLACE\\s+)?FUNCTION\\s+([\\w.]+)\\s*\\(([^)]*)\\)\\s*RETURNS\\s+(\\w+)\\s+RETURN\\s+(.+?)\\s*;?\\s*$",
        )
        .expect("create function regex")
    });
    let caps = re.captures(sql.trim())?;
    let name = caps.get(1)?.as_str().to_string();
    let params_raw = caps.get(2)?.as_str().trim();
    let return_type = caps.get(3)?.as_str().to_uppercase();
    let body = caps.get(4)?.as_str().trim().to_string();

    let param_names: Vec<String> = if params_raw.is_empty() {
        vec![]
    } else {
        params_raw
            .split(',')
            .filter_map(|p| {
                let name = p.split_whitespace().next()?;
                Some(name.to_string())
            })
            .collect()
    };

    Some(UdfDef {
        name,
        sql_body: Some(body),
        param_names,
        return_type,
    })
}

fn spark_type_to_arrow(t: &str) -> DataType {
    match t.to_uppercase().as_str() {
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" | "LONG" => DataType::Int64,
        "DOUBLE" | "FLOAT" => DataType::Float64,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "STRING" | "VARCHAR" => DataType::Utf8,
        _ => DataType::Int32,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum BodyExpr {
    Null,
    Int(i64),
    Ident(String),
    Binary(Box<BodyExpr>, char, Box<BodyExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Tok {
    Ident(String),
    Int(i64),
    Op(char),
}

fn tokenize_body(s: &str) -> std::result::Result<Vec<Tok>, String> {
    let mut out = Vec::new();
    let b = s.as_bytes();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i] as char;
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            i += 1;
            while i < b.len() {
                let d = b[i] as char;
                if d.is_ascii_alphanumeric() || d == '_' {
                    i += 1;
                } else {
                    break;
                }
            }
            out.push(Tok::Ident(s[start..i].to_string()));
            continue;
        }
        if c.is_ascii_digit() {
            let start = i;
            while i < b.len() && (b[i] as char).is_ascii_digit() {
                i += 1;
            }
            let n: i64 = s[start..i]
                .parse()
                .map_err(|_| "bad integer in SQL function body".to_string())?;
            out.push(Tok::Int(n));
            continue;
        }
        if matches!(c, '+' | '-' | '*' | '/' | '(' | ')') {
            out.push(Tok::Op(c));
            i += 1;
            continue;
        }
        return Err(format!("unsupported token `{c}` in SQL function body"));
    }
    Ok(out)
}

fn parse_body(s: &str) -> std::result::Result<BodyExpr, String> {
    let toks = tokenize_body(s)?;
    if toks.is_empty() {
        return Err("empty SQL function body".into());
    }
    let (expr, i) = parse_add(&toks, 0)?;
    if i != toks.len() {
        return Err("unsupported SQL function body".into());
    }
    Ok(expr)
}

fn body_unknown_idents<'a>(expr: &'a BodyExpr, params: &[String]) -> Vec<&'a str> {
    let mut out = Vec::new();
    fn walk<'a>(expr: &'a BodyExpr, params: &[String], out: &mut Vec<&'a str>) {
        match expr {
            BodyExpr::Ident(name) => {
                if !params.iter().any(|p| p.eq_ignore_ascii_case(name)) {
                    out.push(name.as_str());
                }
            }
            BodyExpr::Binary(l, _, r) => {
                walk(l, params, out);
                walk(r, params, out);
            }
            BodyExpr::Null | BodyExpr::Int(_) => {}
        }
    }
    walk(expr, params, &mut out);
    out
}

fn parse_add(toks: &[Tok], mut i: usize) -> std::result::Result<(BodyExpr, usize), String> {
    let (mut left, n) = parse_mul(toks, i)?;
    i = n;
    while let Some(Tok::Op(op @ ('+' | '-'))) = toks.get(i) {
        let (right, n) = parse_mul(toks, i + 1)?;
        left = BodyExpr::Binary(Box::new(left), *op, Box::new(right));
        i = n;
    }
    Ok((left, i))
}

fn parse_mul(toks: &[Tok], mut i: usize) -> std::result::Result<(BodyExpr, usize), String> {
    let (mut left, n) = parse_atom(toks, i)?;
    i = n;
    while let Some(Tok::Op(op @ ('*' | '/'))) = toks.get(i) {
        let (right, n) = parse_atom(toks, i + 1)?;
        left = BodyExpr::Binary(Box::new(left), *op, Box::new(right));
        i = n;
    }
    Ok((left, i))
}

fn parse_atom(toks: &[Tok], i: usize) -> std::result::Result<(BodyExpr, usize), String> {
    match toks.get(i) {
        Some(Tok::Op('-')) => {
            let (inner, n) = parse_atom(toks, i + 1)?;
            Ok((
                BodyExpr::Binary(Box::new(BodyExpr::Int(0)), '-', Box::new(inner)),
                n,
            ))
        }
        Some(Tok::Op('(')) => {
            let (inner, n) = parse_add(toks, i + 1)?;
            match toks.get(n) {
                Some(Tok::Op(')')) => Ok((inner, n + 1)),
                _ => Err("unclosed '(' in SQL function body".into()),
            }
        }
        Some(Tok::Int(n)) => Ok((BodyExpr::Int(*n), i + 1)),
        Some(Tok::Ident(name)) if name.eq_ignore_ascii_case("null") => Ok((BodyExpr::Null, i + 1)),
        Some(Tok::Ident(name)) => Ok((BodyExpr::Ident(name.to_lowercase()), i + 1)),
        _ => Err("unsupported SQL function body".into()),
    }
}

fn eval_body(expr: &BodyExpr, env: &HashMap<String, ScalarValue>) -> DfResult<ScalarValue> {
    match expr {
        BodyExpr::Null => Ok(ScalarValue::Null),
        BodyExpr::Int(n) => Ok(ScalarValue::Int64(Some(*n))),
        BodyExpr::Ident(name) => env.get(name).cloned().ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "unknown parameter `{name}` in SQL function body"
            ))
        }),
        BodyExpr::Binary(l, op, r) => {
            let lv = eval_body(l, env)?;
            let rv = eval_body(r, env)?;
            if lv.is_null() || rv.is_null() {
                return Ok(ScalarValue::Null);
            }
            let a = scalar_i64(&lv)?;
            let b = scalar_i64(&rv)?;
            let n = match op {
                '+' => a.checked_add(b),
                '-' => a.checked_sub(b),
                '*' => a.checked_mul(b),
                '/' => {
                    if b == 0 {
                        return exec_err!("division by zero in SQL function");
                    }
                    a.checked_div(b)
                }
                _ => return exec_err!("unsupported operator in SQL function"),
            };
            let n = n.ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "integer overflow in SQL function".into(),
                )
            })?;
            Ok(ScalarValue::Int64(Some(n)))
        }
    }
}

fn scalar_i64(v: &ScalarValue) -> DfResult<i64> {
    match v {
        ScalarValue::Int8(Some(n)) => Ok(i64::from(*n)),
        ScalarValue::Int16(Some(n)) => Ok(i64::from(*n)),
        ScalarValue::Int32(Some(n)) => Ok(i64::from(*n)),
        ScalarValue::Int64(Some(n)) => Ok(*n),
        ScalarValue::UInt8(Some(n)) => Ok(i64::from(*n)),
        ScalarValue::UInt16(Some(n)) => Ok(i64::from(*n)),
        ScalarValue::UInt32(Some(n)) => Ok(i64::from(*n)),
        other => exec_err!("SQL function expected an integer argument, got {other}"),
    }
}

fn cast_to_return(v: ScalarValue, dt: &DataType) -> DfResult<ScalarValue> {
    if v.is_null() {
        return ScalarValue::try_from(dt)
            .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()));
    }
    match dt {
        DataType::Int32 => {
            let n = i32::try_from(scalar_i64(&v)?).map_err(|_| {
                datafusion::common::DataFusionError::Execution(
                    "integer overflow converting SQL function result to INT".into(),
                )
            })?;
            Ok(ScalarValue::Int32(Some(n)))
        }
        DataType::Int64 => Ok(ScalarValue::Int64(Some(scalar_i64(&v)?))),
        _ => exec_err!("unsupported SQL function return type {dt}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SqlUdf {
    name: String,
    body: BodyExpr,
    param_names: Vec<String>,
    return_type: DataType,
}

impl ScalarUDFImpl for SqlUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| {
            Signature::one_of(
                vec![TypeSignature::Exact(vec![]), TypeSignature::VariadicAny],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.len() != self.param_names.len() {
            return exec_err!(
                "SQL function {} expects {} argument(s), got {}",
                self.name,
                self.param_names.len(),
                args.args.len()
            );
        }
        let n = args
            .args
            .iter()
            .map(|a| match a {
                ColumnarValue::Array(arr) => arr.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(1);
        let mut values = Vec::with_capacity(n);
        for row in 0..n {
            let mut env = HashMap::new();
            for (name, arg) in self.param_names.iter().zip(&args.args) {
                let sv = match arg {
                    ColumnarValue::Scalar(s) => s.clone(),
                    ColumnarValue::Array(a) => ScalarValue::try_from_array(a, row)?,
                };
                env.insert(name.to_lowercase(), sv);
            }
            let raw = eval_body(&self.body, &env)?;
            values.push(cast_to_return(raw, &self.return_type)?);
        }
        if values.len() == 1 {
            Ok(ColumnarValue::Scalar(values.pop().unwrap()))
        } else {
            Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
        }
    }
}

fn register_sql_udf_on_ctx(ctx: &SessionContext, def: &UdfDef) -> Result<()> {
    let body = def
        .sql_body
        .as_ref()
        .ok_or_else(|| Error::Plan(format!("udf `{}` has no body", def.name)))?;
    let parsed = parse_body(body).map_err(|e| {
        Error::Plan(format!(
            "unsupported SQL function body for `{}`: {e}",
            def.name
        ))
    })?;
    let unknown = body_unknown_idents(&parsed, &def.param_names);
    if let Some(name) = unknown.first() {
        return Err(Error::Plan(format!(
            "unsupported SQL function body for `{}`: unknown identifier `{name}`",
            def.name
        )));
    }
    let return_type = spark_type_to_arrow(&def.return_type);
    if !matches!(return_type, DataType::Int32 | DataType::Int64) {
        return Err(Error::Plan(format!(
            "unsupported SQL function return type `{}` for `{}`",
            def.return_type, def.name
        )));
    }
    let udf = SqlUdf {
        name: def.name.clone(),
        body: parsed,
        param_names: def.param_names.clone(),
        return_type,
    };
    ctx.register_udf(ScalarUDF::from(udf));
    Ok(())
}

/// Thread-safe wrapper used by [`crate::Engine`].
pub type SharedUdfRegistry = Arc<Mutex<UdfRegistry>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_function() {
        let def = try_create_function("CREATE FUNCTION foo1a0() RETURNS INT RETURN 1;").unwrap();
        assert_eq!(def.name, "foo1a0");
        assert_eq!(def.return_type, "INT");
        assert_eq!(def.sql_body.as_deref(), Some("1"));
    }

    #[test]
    fn parses_parameterized_function() {
        let def =
            try_create_function("CREATE FUNCTION foo1a1(a INT) RETURNS INT RETURN 1;").unwrap();
        assert_eq!(def.param_names, vec!["a".to_string()]);
    }

    async fn i32_col(engine: &crate::Engine, q: &str) -> Vec<i32> {
        use datafusion::arrow::array::Int32Array;
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        let col = batches[0].column(0);
        let arr = col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("{q}: expected Int32, got {:?}", col.data_type()));
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    /// Control: a constant SQL function must still return its literal.
    #[tokio::test]
    async fn sql_udf_constant_body_returns_the_literal() {
        let engine = crate::Engine::new();
        engine
            .sql("CREATE FUNCTION review_seven() RETURNS INT RETURN 7")
            .await
            .unwrap();
        assert_eq!(
            i32_col(&engine, "SELECT review_seven() AS value").await,
            vec![7]
        );
    }

    /// `RETURN a * 2` must use the argument. The previous evaluator returned 1 for
    /// every non-literal body.
    #[tokio::test]
    async fn sql_udf_evaluates_parameter_expression_per_row() {
        let engine = crate::Engine::new();
        engine
            .sql("CREATE FUNCTION review_double(a INT) RETURNS INT RETURN a * 2")
            .await
            .unwrap();
        assert_eq!(
            i32_col(
                &engine,
                "SELECT review_double(v) AS value FROM (VALUES (2), (5)) AS t(v) ORDER BY v"
            )
            .await,
            vec![4, 10]
        );
    }

    #[tokio::test]
    async fn sql_udf_rejects_an_unsupported_body_instead_of_returning_one() {
        let engine = crate::Engine::new();
        let err = engine
            .sql("CREATE FUNCTION review_bad(a INT) RETURNS INT RETURN length(a)")
            .await
            .expect_err("unsupported body must not register");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("unsupported") || msg.contains("invalid"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn sql_udf_two_arg_add() {
        let engine = crate::Engine::new();
        engine
            .sql("CREATE FUNCTION review_add(x INT, y INT) RETURNS INT RETURN x + y")
            .await
            .unwrap();
        assert_eq!(
            i32_col(&engine, "SELECT review_add(2, 3) AS value").await,
            vec![5]
        );
    }

    #[tokio::test]
    async fn sql_udf_rejects_unsupported_return_type() {
        let engine = crate::Engine::new();
        let err = engine
            .sql("CREATE FUNCTION review_d(a INT) RETURNS DOUBLE RETURN a * 2")
            .await
            .expect_err("non-integer return types are out of the supported subset");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(msg.contains("unsupported"), "got {err}");
    }

    #[tokio::test]
    async fn sql_udf_rejects_unknown_identifier_at_create() {
        let engine = crate::Engine::new();
        let err = engine
            .sql("CREATE FUNCTION review_bad_ident(a INT) RETURNS INT RETURN b * 2")
            .await
            .expect_err("unknown identifier must not register");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("unsupported") || msg.contains("unknown"),
            "got {err}"
        );
        let describe = engine
            .sql("DESCRIBE FUNCTION review_bad_ident")
            .await
            .expect_err("rejected CREATE must not leave a session UDF");
        assert!(
            describe
                .to_string()
                .to_ascii_lowercase()
                .contains("review_bad_ident")
                || describe
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("unknown"),
            "got {describe}"
        );
    }
}
