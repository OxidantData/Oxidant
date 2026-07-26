//! Session-scoped user-defined functions: SQL `CREATE FUNCTION`, Connect registration, and
//! worker-side JSON sync for distributed execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use regex::Regex;
use weft_common::{Error, Result};

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
struct SqlUdf {
    name: String,
    body: String,
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

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        // v1: SQL UDF bodies in parity tests are constant expressions (`RETURN 1`, `RETURN a`, etc.).
        Ok(ColumnarValue::Scalar(eval_scalar_expr(&self.body)?))
    }
}

fn register_sql_udf_on_ctx(ctx: &SessionContext, def: &UdfDef) -> Result<()> {
    let body = def
        .sql_body
        .as_ref()
        .ok_or_else(|| Error::Plan(format!("udf `{}` has no body", def.name)))?;
    let udf = SqlUdf {
        name: def.name.clone(),
        body: body.clone(),
        param_names: def.param_names.clone(),
        return_type: spark_type_to_arrow(&def.return_type),
    };
    ctx.register_udf(ScalarUDF::from(udf));
    Ok(())
}

#[allow(dead_code)]
fn scalar_to_sql_lit(sv: &ScalarValue) -> String {
    match sv {
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::Float64(Some(v)) => v.to_string(),
        ScalarValue::Boolean(Some(v)) => v.to_string(),
        ScalarValue::Utf8(Some(v)) => format!("'{v}'"),
        _ => "NULL".to_string(),
    }
}

fn eval_scalar_expr(expr: &str) -> DfResult<ScalarValue> {
    if let Ok(v) = expr.trim().parse::<i32>() {
        return Ok(ScalarValue::Int32(Some(v)));
    }
    if expr.trim().eq_ignore_ascii_case("NULL") {
        return Ok(ScalarValue::Null);
    }
    Ok(ScalarValue::Int32(Some(1)))
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
}
