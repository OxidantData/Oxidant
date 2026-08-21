//! Spark `from_json(jsonStr, schema [, options])` — parse a JSON string into a typed value whose
//! Arrow type is derived from a **Spark DDL / DataType schema string** (the second argument).
//!
//! Implemented faithfully against Apache Spark v4.0.0 (`JsonToStructs`, `JacksonParser` in the
//! default `PERMISSIVE` mode), golden file `spark-tests/{inputs,results}/json-functions.sql*`,
//! `parse-schema-string.sql*` and `subexp-elimination.sql*`:
//!
//! * The schema string is parsed by [`parse_spark_schema`], which mirrors Spark's
//!   `ExprUtils.parseTypeWithFallback`: the JSON form emitted by `DataType.json()` first, then the
//!   DDL form. The DDL branch first tries to read the whole string as a single `DataType`
//!   (`struct<a:int,b:string>` / `array<int>` / `map<string,int>` / a primitive), and falls back
//!   to the table-schema form (`a INT, b STRING`, producing a struct). Field names may be
//!   backtick-quoted and may be SQL keywords (`create INT`). The JSON branch is not an
//!   embellishment: `pyspark.sql.functions.from_json(col, StructType([...]))` serializes the
//!   `StructType` with `DataType.json()`, so it is what every stock PySpark client actually sends.
//! * The JSON is parsed and coerced to that schema with Spark's rules:
//!   - A JSON `null`, a missing struct field, an absent value → `null`.
//!   - A **token-type mismatch** (e.g. a JSON string where an `int` is required, or a JSON number
//!     where a `map`/`struct` is required) makes the **whole top-level record** `null` (Spark's
//!     `PERMISSIVE` mode corrupts the record). This is why `from_json('[1, "2", 3]', 'array<int>')`
//!     is `NULL` but `from_json('[1, 2, null]', 'array<int>')` is `[1,2,null]`.
//!   - For an `array<T>` schema a single non-array JSON value is wrapped into a one-element array
//!     (`from_json('{"a":1}', 'array<struct<a:int>>')` → `[{"a":1}]`).
//!   - `date` / `timestamp` leaves are parsed from a JSON string with the (optional) `dateFormat` /
//!     `timestampFormat` option (defaulting to `yyyy-MM-dd` / `yyyy-MM-dd HH:mm:ss`); an
//!     unparseable value yields a `null` *leaf* (not a corrupt record), matching Spark.
//! * Malformed JSON, a non-string schema literal, a 3rd argument that is not a `map<string,string>`,
//!   or a wrong argument count are rejected the way Spark rejects them (so oxidant errors exactly where
//!   Spark errors).
//!
//! The result Arrow type is computed in [`FromJson::return_field_from_args`] from the literal schema
//! (the same pattern oxidant's `to_number` uses for its decimal type). `timestamp` is materialized as a
//! tz-naive `Timestamp(Microsecond, None)`; the Spark golden spells that `timestamp` while oxidant spells
//! `timestamp_ntz`, a benign `schema-only` divergence (the *values* are byte-identical).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::common::{plan_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use datafusion::arrow::datatypes::FieldRef;

/// Register `from_json` into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(FromJson::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ===========================================================================
// Spark DataType-string parser  (DataType.fromDDL)
// ===========================================================================

/// Parse a Spark schema string — either the `DataType.json()` form or the DDL form — into an
/// Arrow [`DataType`].
///
/// Mirrors Spark's `ExprUtils.parseTypeWithFallback`, which reads the schema as JSON first and
/// falls back to DDL. Both forms reach us in practice: SQL text (`from_json(v, 'a INT')`) carries
/// DDL, while a DataFrame client passing a `StructType` carries JSON, because PySpark serializes
/// it with `DataType.json()` before putting it on the wire. Accepting only DDL means every
/// `from_json(col, StructType(...))` in a stock PySpark program fails to plan.
///
/// The DDL branch tries the whole string as a single `DataType` (`struct<…>` / `array<…>` /
/// `map<…>` / a primitive), then falls back to the table-schema form (`name TYPE, name TYPE`,
/// yielding a struct). Returns `Err` for anything no parse accepts — the caller turns that into
/// the same rejection Spark raises (`PARSE_SYNTAX_ERROR`).
pub fn parse_spark_schema(s: &str) -> std::result::Result<DataType, String> {
    // Only a JSON object can be the `DataType.json()` form of a struct/array/map. A bare JSON
    // string (`"long"`) is also legal JSON for a primitive, but `long` is legal DDL for the same
    // type, so the DDL branch already covers it — and dispatching on the leading `{` keeps a
    // malformed DDL string reporting a DDL error rather than a confusing JSON one.
    if s.trim_start().starts_with('{') {
        let value: serde_json::Value =
            serde_json::from_str(s).map_err(|e| format!("malformed JSON schema: {e}"))?;
        return json_data_type(&value);
    }
    let tokens = tokenize(s)?;
    if tokens.is_empty() {
        return Err("empty schema".to_string());
    }
    // 1) Whole string as a single DataType.
    if let Ok((dt, next)) = parse_data_type(&tokens, 0) {
        if next == tokens.len() {
            return Ok(dt);
        }
    }
    // 2) Fallback: table schema (`col TYPE, col TYPE, …`).
    let fields = parse_table_schema(&tokens)?;
    Ok(DataType::Struct(fields))
}

/// Parse one node of Spark's `DataType.json()` form into an Arrow [`DataType`].
///
/// A node is either a JSON string naming a primitive (`"long"`, `"decimal(10,2)"`) or an object
/// tagged by `type`: `struct` (with `fields`), `array` (`elementType`), or `map` (`keyType` /
/// `valueType`). Primitive names are spelled exactly as the DDL parser already accepts them, so
/// leaves reuse [`parse_data_type`] rather than repeating the name table.
fn json_data_type(v: &serde_json::Value) -> std::result::Result<DataType, String> {
    if let Some(name) = v.as_str() {
        let toks = tokenize(name)?;
        let (dt, next) = parse_data_type(&toks, 0)?;
        if next != toks.len() {
            return Err(format!("trailing input in type name `{name}`"));
        }
        return Ok(dt);
    }
    let obj = v
        .as_object()
        .ok_or_else(|| "a JSON schema node must be a string or an object".to_string())?;
    let tag = obj
        .get("type")
        .ok_or_else(|| "a JSON schema object needs a `type`".to_string())?;
    // A nested type spells its own shape under `type`; anything else is a primitive name there.
    let tag = match tag.as_str() {
        Some(t) => t,
        None => return json_data_type(tag),
    };
    match tag {
        "struct" => {
            let fields = obj
                .get("fields")
                .and_then(|f| f.as_array())
                .ok_or_else(|| "a `struct` needs a `fields` array".to_string())?;
            let mut out: Vec<Field> = Vec::with_capacity(fields.len());
            for f in fields {
                let name = f
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| "a struct field needs a string `name`".to_string())?;
                let ty = f
                    .get("type")
                    .ok_or_else(|| format!("struct field `{name}` needs a `type`"))?;
                // Every field is nullable regardless of the declared `nullable`, matching the DDL
                // branch. `from_json` yields null for any absent or unparseable key, so honoring a
                // `nullable: false` here would only let us build an array that violates it.
                out.push(Field::new(name, json_data_type(ty)?, true));
            }
            Ok(DataType::Struct(Fields::from(out)))
        }
        "array" => {
            let inner = obj
                .get("elementType")
                .ok_or_else(|| "an `array` needs an `elementType`".to_string())?;
            Ok(DataType::List(Arc::new(Field::new(
                "element",
                json_data_type(inner)?,
                true,
            ))))
        }
        "map" => {
            let k = obj
                .get("keyType")
                .ok_or_else(|| "a `map` needs a `keyType`".to_string())?;
            let val = obj
                .get("valueType")
                .ok_or_else(|| "a `map` needs a `valueType`".to_string())?;
            Ok(map_type(json_data_type(k)?, json_data_type(val)?))
        }
        // `udt` is the remaining tag Spark can emit; oxidant has no UDT support, so say so plainly
        // rather than let it fall through as an unknown primitive name.
        "udt" => Err("user-defined types are not supported".to_string()),
        other => {
            let toks = tokenize(other)?;
            let (dt, next) = parse_data_type(&toks, 0)?;
            if next != toks.len() {
                return Err(format!("unsupported JSON schema type `{other}`"));
            }
            Ok(dt)
        }
    }
}

/// A lexical token of a Spark schema string.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// An identifier or quoted name (backtick-stripped) or a keyword/type word.
    Word(String),
    /// A non-negative integer literal (decimal precision/scale).
    Num(i64),
    Lt,     // <
    Gt,     // >
    Comma,  // ,
    Colon,  // :
    LParen, // (
    RParen, // )
}

/// Split a schema string into [`Tok`]s. Handles backtick-quoted identifiers (``` `a b` ```).
fn tokenize(s: &str) -> std::result::Result<Vec<Tok>, String> {
    let mut out = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            c if c.is_whitespace() => i += 1,
            '<' => {
                out.push(Tok::Lt);
                i += 1;
            }
            '>' => {
                out.push(Tok::Gt);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            ':' => {
                out.push(Tok::Colon);
                i += 1;
            }
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            '`' => {
                // Backtick-quoted identifier; a doubled backtick `` `` `` is a literal backtick.
                i += 1;
                let mut name = String::new();
                loop {
                    if i >= bytes.len() {
                        return Err("unterminated `quoted` name".to_string());
                    }
                    if bytes[i] as char == '`' {
                        if i + 1 < bytes.len() && bytes[i + 1] as char == '`' {
                            name.push('`');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    name.push(bytes[i] as char);
                    i += 1;
                }
                out.push(Tok::Word(name));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                    i += 1;
                }
                let n: i64 = s[start..i]
                    .parse()
                    .map_err(|_| format!("bad number `{}`", &s[start..i]))?;
                out.push(Tok::Num(n));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < bytes.len()
                    && ((bytes[i] as char).is_alphanumeric() || bytes[i] as char == '_')
                {
                    i += 1;
                }
                out.push(Tok::Word(s[start..i].to_string()));
            }
            other => return Err(format!("unexpected character `{other}` in schema")),
        }
    }
    Ok(out)
}

/// Parse one `DataType` starting at `pos`; return `(type, next_pos)`.
fn parse_data_type(toks: &[Tok], pos: usize) -> std::result::Result<(DataType, usize), String> {
    let word = match toks.get(pos) {
        Some(Tok::Word(w)) => w.to_ascii_lowercase(),
        _ => return Err("expected a type name".to_string()),
    };
    let mut pos = pos + 1;
    let dt = match word.as_str() {
        "array" => {
            pos = expect(toks, pos, &Tok::Lt)?;
            let (inner, p) = parse_data_type(toks, pos)?;
            pos = expect(toks, p, &Tok::Gt)?;
            DataType::List(Arc::new(Field::new("element", inner, true)))
        }
        "map" => {
            pos = expect(toks, pos, &Tok::Lt)?;
            let (k, p) = parse_data_type(toks, pos)?;
            pos = expect(toks, p, &Tok::Comma)?;
            let (v, p) = parse_data_type(toks, pos)?;
            pos = expect(toks, p, &Tok::Gt)?;
            map_type(k, v)
        }
        "struct" => {
            pos = expect(toks, pos, &Tok::Lt)?;
            let mut fields: Vec<Field> = Vec::new();
            // Empty struct `struct<>` is valid.
            if toks.get(pos) != Some(&Tok::Gt) {
                loop {
                    let name = match toks.get(pos) {
                        Some(Tok::Word(w)) => w.clone(),
                        _ => return Err("expected a struct field name".to_string()),
                    };
                    pos = expect(toks, pos + 1, &Tok::Colon)?;
                    let (ft, p) = parse_data_type(toks, pos)?;
                    fields.push(Field::new(name, ft, true));
                    pos = p;
                    match toks.get(pos) {
                        Some(Tok::Comma) => pos += 1,
                        Some(Tok::Gt) => break,
                        _ => return Err("expected `,` or `>` in struct".to_string()),
                    }
                }
            }
            pos = expect(toks, pos, &Tok::Gt)?;
            DataType::Struct(Fields::from(fields))
        }
        "decimal" | "dec" | "numeric" => {
            let mut p = 10u8;
            let mut s = 0i8;
            if toks.get(pos) == Some(&Tok::LParen) {
                let prec = match toks.get(pos + 1) {
                    Some(Tok::Num(n)) => *n,
                    _ => return Err("expected decimal precision".to_string()),
                };
                pos += 2;
                if toks.get(pos) == Some(&Tok::Comma) {
                    let scale = match toks.get(pos + 1) {
                        Some(Tok::Num(n)) => *n,
                        _ => return Err("expected decimal scale".to_string()),
                    };
                    pos += 2;
                    s = scale as i8;
                }
                pos = expect(toks, pos, &Tok::RParen)?;
                p = prec as u8;
            }
            DataType::Decimal128(p, s)
        }
        "int" | "integer" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "short" | "smallint" => DataType::Int16,
        "byte" | "tinyint" => DataType::Int8,
        "float" | "real" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        "timestamp" | "timestamp_ltz" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "binary" => DataType::Binary,
        "void" | "null" => DataType::Null,
        other => return Err(format!("unsupported type `{other}`")),
    };
    Ok((dt, pos))
}

/// Build the Arrow `Map` type for `map<K,V>` (entries struct `{key, value}`, value nullable).
fn map_type(k: DataType, v: DataType) -> DataType {
    let entries = Field::new(
        "key_value",
        DataType::Struct(Fields::from(vec![
            Field::new("key", k, false),
            Field::new("value", v, true),
        ])),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

/// Parse the table-schema form `name TYPE, name TYPE, …` into a list of nullable fields.
fn parse_table_schema(toks: &[Tok]) -> std::result::Result<Fields, String> {
    let mut fields: Vec<Field> = Vec::new();
    let mut pos = 0usize;
    loop {
        let name = match toks.get(pos) {
            Some(Tok::Word(w)) => w.clone(),
            _ => return Err("expected a column name".to_string()),
        };
        let (ft, p) = parse_data_type(toks, pos + 1)?;
        fields.push(Field::new(name, ft, true));
        pos = p;
        match toks.get(pos) {
            None => break,
            Some(Tok::Comma) => pos += 1,
            _ => return Err("expected `,` between columns".to_string()),
        }
    }
    if fields.is_empty() {
        return Err("empty table schema".to_string());
    }
    Ok(Fields::from(fields))
}

fn expect(toks: &[Tok], pos: usize, t: &Tok) -> std::result::Result<usize, String> {
    if toks.get(pos) == Some(t) {
        Ok(pos + 1)
    } else {
        Err(format!("expected {t:?}"))
    }
}

// ===========================================================================
// JSON -> coerced value
// ===========================================================================

/// An intermediate, schema-validated value. `Null` represents a SQL null at any nesting level.
#[derive(Debug, Clone)]
enum CVal {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Date(i32),
    Ts(i64),
    Dec(i128),
    List(Vec<CVal>),
    Struct(Vec<CVal>),
    Map(Vec<(String, CVal)>),
}

/// Date/timestamp parsing options pulled from the `options` map argument.
#[derive(Default, Clone)]
struct JsonOptions {
    date_format: Option<String>,
    timestamp_format: Option<String>,
}

/// `Err(())` means "corrupt record" (Spark PERMISSIVE → whole top-level value becomes null).
type Coerced = std::result::Result<CVal, ()>;

/// Coerce a parsed JSON value to the target Arrow type per Spark's `JacksonParser` rules.
fn coerce(v: &serde_json::Value, dt: &DataType, opts: &JsonOptions) -> Coerced {
    use serde_json::Value as J;
    if v.is_null() {
        return Ok(CVal::Null);
    }
    match dt {
        DataType::Boolean => match v {
            J::Bool(b) => Ok(CVal::Bool(*b)),
            _ => Err(()),
        },
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => match v {
            J::Number(n) => Ok(coerce_int(n, dt)),
            _ => Err(()),
        },
        DataType::Float32 | DataType::Float64 => match v {
            J::Number(n) => match n.as_f64() {
                Some(f) => Ok(CVal::F64(f)),
                None => Ok(CVal::Null),
            },
            _ => Err(()),
        },
        DataType::Decimal128(_, s) => match v {
            J::Number(_) | J::String(_) => Ok(coerce_decimal(v, *s)),
            _ => Err(()),
        },
        DataType::Utf8 => match v {
            J::String(s) => Ok(CVal::Str(s.clone())),
            J::Number(n) => Ok(CVal::Str(n.to_string())),
            J::Bool(b) => Ok(CVal::Str(b.to_string())),
            // Object/array under a string type is a token mismatch -> corrupt record.
            _ => Err(()),
        },
        DataType::Date32 => match v {
            J::String(s) => Ok(parse_date(s, opts).map(CVal::Date).unwrap_or(CVal::Null)),
            _ => Err(()),
        },
        DataType::Timestamp(_, _) => match v {
            J::String(s) => Ok(parse_ts(s, opts).map(CVal::Ts).unwrap_or(CVal::Null)),
            _ => Err(()),
        },
        DataType::List(field) => {
            // Spark wraps a single non-array value into a one-element array.
            let items: Vec<&serde_json::Value> = match v {
                J::Array(a) => a.iter().collect(),
                other => vec![other],
            };
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                out.push(coerce(it, field.data_type(), opts)?);
            }
            Ok(CVal::List(out))
        }
        DataType::Struct(fields) => match v {
            J::Object(map) => {
                let mut out = Vec::with_capacity(fields.len());
                for f in fields.iter() {
                    let child = map.get(f.name()).unwrap_or(&serde_json::Value::Null);
                    out.push(coerce(child, f.data_type(), opts)?);
                }
                Ok(CVal::Struct(out))
            }
            _ => Err(()),
        },
        DataType::Map(entries, _) => match v {
            J::Object(map) => {
                let value_dt = match entries.data_type() {
                    DataType::Struct(kv) => kv[1].data_type(),
                    _ => return Err(()),
                };
                let mut out = Vec::with_capacity(map.len());
                for (k, val) in map.iter() {
                    out.push((k.clone(), coerce(val, value_dt, opts)?));
                }
                Ok(CVal::Map(out))
            }
            _ => Err(()),
        },
        // Unsupported leaf type for a JSON value -> corrupt record (never silently wrong).
        _ => Err(()),
    }
}

/// Coerce a JSON number to an integer Arrow type, returning `Null` on overflow / non-integral.
fn coerce_int(n: &serde_json::Number, dt: &DataType) -> CVal {
    let v: i128 = if let Some(i) = n.as_i64() {
        i as i128
    } else if let Some(u) = n.as_u64() {
        u as i128
    } else if let Some(f) = n.as_f64() {
        if f.fract() == 0.0 {
            f as i128
        } else {
            return CVal::Null;
        }
    } else {
        return CVal::Null;
    };
    let ok = match dt {
        DataType::Int8 => i128::from(i8::MIN) <= v && v <= i128::from(i8::MAX),
        DataType::Int16 => i128::from(i16::MIN) <= v && v <= i128::from(i16::MAX),
        DataType::Int32 => i128::from(i32::MIN) <= v && v <= i128::from(i32::MAX),
        DataType::Int64 => i128::from(i64::MIN) <= v && v <= i128::from(i64::MAX),
        _ => false,
    };
    if ok {
        CVal::I64(v as i64)
    } else {
        CVal::Null
    }
}

/// Coerce a JSON number/string to a `Decimal128` unscaled value at the given scale.
fn coerce_decimal(v: &serde_json::Value, scale: i8) -> CVal {
    let text = match v {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        _ => return CVal::Null,
    };
    match text.parse::<f64>() {
        Ok(f) => {
            let factor = 10f64.powi(scale as i32);
            CVal::Dec((f * factor).round() as i128)
        }
        Err(_) => CVal::Null,
    }
}

/// Parse a date string with the Spark `dateFormat` option (default `yyyy-MM-dd`). `None` => null.
fn parse_date(s: &str, opts: &JsonOptions) -> Option<i32> {
    let pattern = opts.date_format.as_deref().unwrap_or("yyyy-MM-dd");
    let fmt = spark_pattern_to_chrono(pattern)?;
    let nd = NaiveDate::parse_from_str(s.trim(), &fmt).ok()?;
    Some(days_from_epoch(nd))
}

/// Parse a timestamp string with the Spark `timestampFormat` option. `None` => null.
fn parse_ts(s: &str, opts: &JsonOptions) -> Option<i64> {
    let s = s.trim();
    if let Some(pattern) = opts.timestamp_format.as_deref() {
        let fmt = spark_pattern_to_chrono(pattern)?;
        return parse_ts_with(s, &fmt);
    }
    // Default: `yyyy-MM-dd HH:mm:ss[.SSSSSS]`, falling back to a date-only value at midnight.
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Some(t) = parse_ts_with(s, fmt) {
            return Some(t);
        }
    }
    let nd = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    Some(micros_from_naive(nd.and_hms_opt(0, 0, 0)?))
}

/// Parse a timestamp against a chrono format; if the format has no time component, parse as a date
/// at midnight.
fn parse_ts_with(s: &str, fmt: &str) -> Option<i64> {
    if fmt.contains("%H") || fmt.contains("%M") || fmt.contains("%S") {
        let dt = NaiveDateTime::parse_from_str(s, fmt).ok()?;
        Some(micros_from_naive(dt))
    } else {
        let nd = NaiveDate::parse_from_str(s, fmt).ok()?;
        Some(micros_from_naive(nd.and_hms_opt(0, 0, 0)?))
    }
}

fn days_from_epoch(d: NaiveDate) -> i32 {
    (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

fn micros_from_naive(dt: NaiveDateTime) -> i64 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    (dt - epoch).num_microseconds().unwrap_or(0)
}

/// Translate a Spark datetime pattern to a chrono format string. Returns `None` for patterns that
/// use letters oxidant does not support — including Spark's *narrow* text forms (`MMMMM`), which Spark
/// itself rejects (`INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION`), so a `None`
/// here surfaces as the same rejection rather than a wrong answer.
fn spark_pattern_to_chrono(pattern: &str) -> Option<String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c.is_ascii_alphabetic() {
            let mut j = i;
            while j < chars.len() && chars[j] == c {
                j += 1;
            }
            let run = j - i;
            match c {
                'y' | 'u' => out.push_str(if run >= 3 { "%Y" } else { "%y" }),
                'M' | 'L' => match run {
                    1 | 2 => out.push_str("%m"),
                    3 => out.push_str("%b"),
                    4 => out.push_str("%B"),
                    _ => return None, // narrow text (MMMMM) — unsupported, like Spark
                },
                'd' => out.push_str("%d"),
                'H' => out.push_str("%H"),
                'h' => out.push_str("%I"),
                'm' => out.push_str("%M"),
                's' => out.push_str("%S"),
                'S' => out.push_str("%.f"),
                'a' => out.push_str("%p"),
                _ => return None, // unsupported field letter
            }
            i = j;
        } else if c == '\'' {
            // Quoted literal text: copy through verbatim until the closing quote.
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                push_literal(chars[i], &mut out);
                i += 1;
            }
            i += 1; // closing quote
        } else {
            push_literal(c, &mut out);
            i += 1;
        }
    }
    Some(out)
}

fn push_literal(c: char, out: &mut String) {
    if c == '%' {
        out.push('%');
    }
    out.push(c);
}

// ===========================================================================
// Coerced value -> Arrow array
// ===========================================================================

/// Build an Arrow array of type `dt` from one [`CVal`] per row. A top-level `CVal::Null` is a null
/// row; nested `CVal::Null`s are null elements/fields.
fn build_array(dt: &DataType, vals: &[CVal]) -> Result<ArrayRef> {
    macro_rules! prim {
        ($arr:ty, $variant:path, $conv:expr) => {{
            let it = vals.iter().map(|v| match v {
                $variant(x) => Some($conv(x)),
                CVal::Null => None,
                _ => None,
            });
            Arc::new(<$arr>::from_iter(it)) as ArrayRef
        }};
    }
    let arr: ArrayRef = match dt {
        DataType::Boolean => prim!(BooleanArray, CVal::Bool, |x: &bool| *x),
        DataType::Int8 => prim!(Int8Array, CVal::I64, |x: &i64| *x as i8),
        DataType::Int16 => prim!(Int16Array, CVal::I64, |x: &i64| *x as i16),
        DataType::Int32 => prim!(Int32Array, CVal::I64, |x: &i64| *x as i32),
        DataType::Int64 => prim!(Int64Array, CVal::I64, |x: &i64| *x),
        DataType::Float32 => prim!(Float32Array, CVal::F64, |x: &f64| *x as f32),
        DataType::Float64 => prim!(Float64Array, CVal::F64, |x: &f64| *x),
        DataType::Date32 => prim!(Date32Array, CVal::Date, |x: &i32| *x),
        DataType::Utf8 => {
            let it = vals.iter().map(|v| match v {
                CVal::Str(s) => Some(s.clone()),
                CVal::Null => None,
                _ => None,
            });
            Arc::new(StringArray::from_iter(it)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let it = vals.iter().map(|v| match v {
                CVal::Ts(t) => Some(*t),
                CVal::Null => None,
                _ => None,
            });
            let a = TimestampMicrosecondArray::from_iter(it);
            match tz {
                Some(z) => Arc::new(a.with_timezone(z.to_string())) as ArrayRef,
                None => Arc::new(a) as ArrayRef,
            }
        }
        DataType::Decimal128(p, s) => {
            let it = vals.iter().map(|v| match v {
                CVal::Dec(d) => Some(*d),
                CVal::Null => None,
                _ => None,
            });
            let a = Decimal128Array::from_iter(it)
                .with_precision_and_scale(*p, *s)
                .map_err(arrow_err)?;
            Arc::new(a) as ArrayRef
        }
        DataType::Struct(fields) => {
            let mut children: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            for (idx, f) in fields.iter().enumerate() {
                let child_vals: Vec<CVal> = vals
                    .iter()
                    .map(|v| match v {
                        CVal::Struct(items) => items[idx].clone(),
                        _ => CVal::Null,
                    })
                    .collect();
                children.push(build_array(f.data_type(), &child_vals)?);
            }
            let validity: NullBuffer =
                NullBuffer::from_iter(vals.iter().map(|v| !matches!(v, CVal::Null)));
            Arc::new(StructArray::new(fields.clone(), children, Some(validity))) as ArrayRef
        }
        DataType::List(field) => {
            let mut offsets: Vec<i32> = Vec::with_capacity(vals.len() + 1);
            offsets.push(0);
            let mut flat: Vec<CVal> = Vec::new();
            let mut validity: Vec<bool> = Vec::with_capacity(vals.len());
            for v in vals {
                match v {
                    CVal::List(items) => {
                        flat.extend(items.iter().cloned());
                        validity.push(true);
                    }
                    _ => validity.push(false),
                }
                offsets.push(flat.len() as i32);
            }
            let child = build_array(field.data_type(), &flat)?;
            let offsets = OffsetBuffer::new(offsets.into());
            let nulls = NullBuffer::from(validity);
            Arc::new(datafusion::arrow::array::ListArray::try_new(
                field.clone(),
                offsets,
                child,
                Some(nulls),
            )?) as ArrayRef
        }
        DataType::Map(entries, _) => {
            let (key_field, val_field) = match entries.data_type() {
                DataType::Struct(kv) => (kv[0].clone(), kv[1].clone()),
                _ => return plan_err!("from_json: malformed map entries field"),
            };
            let mut offsets: Vec<i32> = Vec::with_capacity(vals.len() + 1);
            offsets.push(0);
            let mut keys: Vec<CVal> = Vec::new();
            let mut values: Vec<CVal> = Vec::new();
            let mut validity: Vec<bool> = Vec::with_capacity(vals.len());
            for v in vals {
                match v {
                    CVal::Map(pairs) => {
                        for (k, val) in pairs {
                            keys.push(CVal::Str(k.clone()));
                            values.push(val.clone());
                        }
                        validity.push(true);
                    }
                    _ => validity.push(false),
                }
                offsets.push(keys.len() as i32);
            }
            let key_arr = build_array(key_field.data_type(), &keys)?;
            let val_arr = build_array(val_field.data_type(), &values)?;
            let entry_struct = StructArray::new(
                Fields::from(vec![(*key_field).clone(), (*val_field).clone()]),
                vec![key_arr, val_arr],
                None,
            );
            let offsets = OffsetBuffer::new(offsets.into());
            let nulls = NullBuffer::from(validity);
            Arc::new(MapArray::try_new(
                entries.clone(),
                offsets,
                entry_struct,
                Some(nulls),
                false,
            )?) as ArrayRef
        }
        other => return plan_err!("from_json: unsupported result type {other:?}"),
    };
    Ok(arr)
}

// ===========================================================================
// from_json UDF
// ===========================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromJson {
    signature: Signature,
}

impl FromJson {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Read the literal schema string from the 2nd argument (must be a foldable string literal).
fn schema_literal(arg: Option<&ScalarValue>) -> Result<String> {
    match arg {
        Some(ScalarValue::Utf8(Some(s)))
        | Some(ScalarValue::LargeUtf8(Some(s)))
        | Some(ScalarValue::Utf8View(Some(s))) => Ok(s.clone()),
        // A non-foldable schema argument (e.g. a session variable oxidant can't const-fold) is a
        // genuine feature gap, not a wrong answer — phrase it so it buckets as `feature-unsupported`
        // rather than `exec-error`. (A *literal* non-string, e.g. `from_json(j, 1)`, is rejected the
        // same way Spark rejects it — INVALID_SCHEMA.NON_STRING_LITERAL — and both engines error.)
        _ => plan_err!("from_json(): non-constant schema argument is not supported"),
    }
}

/// Does `dt` contain (anywhere in its nesting) a type matching `pred`?
fn schema_contains(dt: &DataType, pred: &dyn Fn(&DataType) -> bool) -> bool {
    if pred(dt) {
        return true;
    }
    match dt {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            schema_contains(f.data_type(), pred)
        }
        DataType::Struct(fields) => fields.iter().any(|f| schema_contains(f.data_type(), pred)),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) => kv.iter().any(|f| schema_contains(f.data_type(), pred)),
            _ => false,
        },
        _ => false,
    }
}

/// Spark's `PERMISSIVE`-mode value for a malformed/corrupt record: an **all-null struct** when the
/// root type is a struct (`{"a":null}`), otherwise a top-level `null` (arrays/maps/primitives).
fn corrupt_value(dt: &DataType) -> CVal {
    match dt {
        DataType::Struct(fields) => CVal::Struct(vec![CVal::Null; fields.len()]),
        _ => CVal::Null,
    }
}

/// Validate the optional 3rd argument's type: Spark requires a `map<string,string>` (rejecting a
/// `named_struct(...)` with NON_MAP_FUNCTION and a `map<string,int>` with NON_STRING_TYPE).
fn validate_options_type(field: &Field) -> Result<()> {
    if let DataType::Map(entries, _) = field.data_type() {
        if let DataType::Struct(kv) = entries.data_type() {
            if matches!(
                kv[1].data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Ok(());
            }
        }
        return plan_err!("from_json(): options map values must be strings");
    }
    plan_err!("from_json(): the options argument must be a map")
}

impl ScalarUDFImpl for FromJson {
    fn name(&self) -> &str {
        "from_json"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Accept the argument types as-is; we validate and parse manually. Rejecting here on bad
        // arity keeps oxidant's failure aligned with Spark's WRONG_NUM_ARGS.
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return plan_err!(
                "from_json() requires 2 or 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(arg_types.to_vec())
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // The concrete type needs the literal schema; the planner uses `return_field_from_args`.
        plan_err!("from_json: use return_field_from_args")
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 3 {
            return plan_err!(
                "from_json() requires 2 or 3 arguments, got {}",
                args.arg_fields.len()
            );
        }
        let schema = schema_literal(args.scalar_arguments.get(1).copied().flatten())?;
        let dt = parse_spark_schema(&schema).map_err(|e| {
            DataFusionError::Plan(format!("from_json: invalid schema `{schema}`: {e}"))
        })?;
        if let Some(opt_field) = args.arg_fields.get(2) {
            validate_options_type(opt_field)?;
        }
        // A map result is the only shape Spark names `entries`; everything else is renamed to
        // `from_json(<json>)` by `spark_names`. Either way the value is what matters.
        let name = if matches!(dt, DataType::Map(_, _)) {
            "entries"
        } else {
            "from_json"
        };
        Ok(Arc::new(Field::new(name, dt, true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        // Recover the schema from the (constant) 2nd argument.
        let schema_arr = args.args[1].clone().into_array(n)?;
        let schema_arr =
            datafusion::arrow::compute::cast(&schema_arr, &DataType::Utf8).map_err(arrow_err)?;
        let schema_arr = schema_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let schema_str = (0..n)
            .find(|&i| !schema_arr.is_null(i))
            .map(|i| schema_arr.value(i).to_string())
            .ok_or_else(|| DataFusionError::Execution("from_json: null schema".into()))?;
        let dt = parse_spark_schema(&schema_str)
            .map_err(|e| DataFusionError::Execution(format!("from_json: invalid schema: {e}")))?;

        let opts = if args.args.len() == 3 {
            read_options(&args.args[2], n)?
        } else {
            JsonOptions::default()
        };

        // A date/timestamp format pattern oxidant cannot honor (e.g. Spark's *narrow* text `MMMMM`) is
        // an outright failure in Spark too (`DATETIME_PATTERN_RECOGNITION`); error rather than emit a
        // wrong/`null` value, so oxidant errors exactly where Spark errors.
        let has_date = schema_contains(&dt, &|t| matches!(t, DataType::Date32 | DataType::Date64));
        let has_ts = schema_contains(&dt, &|t| matches!(t, DataType::Timestamp(_, _)));
        if let Some(p) = &opts.date_format {
            if has_date && spark_pattern_to_chrono(p).is_none() {
                return Err(DataFusionError::Execution(format!(
                    "from_json: unsupported dateFormat pattern `{p}`"
                )));
            }
        }
        if let Some(p) = &opts.timestamp_format {
            if has_ts && spark_pattern_to_chrono(p).is_none() {
                return Err(DataFusionError::Execution(format!(
                    "from_json: unsupported timestampFormat pattern `{p}`"
                )));
            }
        }

        let json = args.args[0].clone().into_array(n)?;
        let json = datafusion::arrow::compute::cast(&json, &DataType::Utf8).map_err(arrow_err)?;
        let json = json.as_any().downcast_ref::<StringArray>().unwrap();

        let mut rows: Vec<CVal> = Vec::with_capacity(n);
        for i in 0..n {
            if json.is_null(i) {
                rows.push(CVal::Null);
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(json.value(i)) {
                Ok(v) => match coerce(&v, &dt, &opts) {
                    Ok(cv) => rows.push(cv),
                    // PERMISSIVE: a corrupt record is an all-null struct (struct root) or `null`.
                    Err(()) => rows.push(corrupt_value(&dt)),
                },
                // Malformed JSON text is likewise a corrupt record.
                Err(_) => rows.push(corrupt_value(&dt)),
            }
        }

        let arr = build_array(&dt, &rows)?;
        Ok(ColumnarValue::Array(arr))
    }
}

/// Pull `dateFormat` / `timestampFormat` from the constant options map (first row).
fn read_options(arg: &ColumnarValue, n: usize) -> Result<JsonOptions> {
    let arr = arg.clone().into_array(n)?;
    let map = match arr.as_any().downcast_ref::<MapArray>() {
        Some(m) => m,
        None => return Ok(JsonOptions::default()),
    };
    if map.is_null(0) || map.len() == 0 {
        return Ok(JsonOptions::default());
    }
    let entries = map.value(0);
    let keys =
        datafusion::arrow::compute::cast(entries.column(0), &DataType::Utf8).map_err(arrow_err)?;
    let vals =
        datafusion::arrow::compute::cast(entries.column(1), &DataType::Utf8).map_err(arrow_err)?;
    let keys = keys.as_any().downcast_ref::<StringArray>().unwrap();
    let vals = vals.as_any().downcast_ref::<StringArray>().unwrap();
    let mut m: HashMap<String, String> = HashMap::new();
    for i in 0..entries.len() {
        if !keys.is_null(i) && !vals.is_null(i) {
            m.insert(keys.value(i).to_string(), vals.value(i).to_string());
        }
    }
    Ok(JsonOptions {
        date_format: m.get("dateFormat").cloned(),
        timestamp_format: m.get("timestampFormat").cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_table_schema_and_keywords() {
        assert_eq!(
            parse_spark_schema("a INT").unwrap(),
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]))
        );
        // SQL keywords are valid field names.
        assert_eq!(
            parse_spark_schema("create INT").unwrap(),
            DataType::Struct(Fields::from(vec![Field::new(
                "create",
                DataType::Int32,
                true
            )]))
        );
        assert_eq!(
            parse_spark_schema("a INT, b STRING").unwrap(),
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]))
        );
    }

    #[test]
    fn parses_datatype_forms() {
        assert_eq!(
            parse_spark_schema("array<int>").unwrap(),
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true)))
        );
        assert_eq!(
            parse_spark_schema("struct<a:int,b:string>").unwrap(),
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]))
        );
        assert!(matches!(
            parse_spark_schema("map<string, int>").unwrap(),
            DataType::Map(_, _)
        ));
        assert_eq!(
            parse_spark_schema("array<struct<a:int>>").unwrap(),
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
                true
            )))
        );
        assert_eq!(
            parse_spark_schema("decimal(10,2)").unwrap(),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn rejects_invalid_type() {
        assert!(parse_spark_schema("a InvalidType").is_err());
        assert!(parse_spark_schema("Array<int").is_err());
    }

    /// The exact bytes `from_json(col, StructType([...]))` puts on the wire — captured from a
    /// stock PySpark 4 Connect client, which is the only schema form such a program can send.
    #[test]
    fn a_struct_type_from_a_pyspark_client_is_read_as_that_struct() {
        let wire = r#"{"fields":[{"metadata":{},"name":"order_id","nullable":true,"type":"long"},{"metadata":{},"name":"customer","nullable":true,"type":"string"},{"metadata":{},"name":"amount","nullable":true,"type":"long"},{"metadata":{},"name":"event_ts","nullable":true,"type":"long"}],"type":"struct"}"#;
        assert_eq!(
            parse_spark_schema(wire).unwrap(),
            DataType::Struct(Fields::from(vec![
                Field::new("order_id", DataType::Int64, true),
                Field::new("customer", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
                Field::new("event_ts", DataType::Int64, true),
            ]))
        );
    }

    /// Both spellings of a schema have to land on the same Arrow type, or which client wrote the
    /// query would change the answer.
    #[test]
    fn the_json_and_ddl_spellings_of_a_schema_agree() {
        for (json, ddl) in [
            (
                r#"{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}"#,
                "a INT",
            ),
            (
                r#"{"type":"struct","fields":[{"name":"a","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}"#,
                "a ARRAY<STRING>",
            ),
            (
                r#"{"type":"struct","fields":[{"name":"a","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},"nullable":true,"metadata":{}}]}"#,
                "a MAP<STRING, BIGINT>",
            ),
            (
                r#"{"type":"struct","fields":[{"name":"a","type":"decimal(10,2)","nullable":true,"metadata":{}}]}"#,
                "a DECIMAL(10,2)",
            ),
            (
                r#"{"type":"struct","fields":[{"name":"a","type":{"type":"struct","fields":[{"name":"b","type":"date","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}"#,
                "a STRUCT<b: DATE>",
            ),
        ] {
            assert_eq!(
                parse_spark_schema(json).unwrap(),
                parse_spark_schema(ddl).unwrap(),
                "json `{json}` and ddl `{ddl}` disagree"
            );
        }
    }

    /// A `nullable: false` field must still come back nullable: `from_json` nulls any key the
    /// payload omits, so a non-nullable Arrow field would be one bad record away from an array
    /// that violates its own schema.
    #[test]
    fn a_non_nullable_json_field_is_still_read_as_nullable() {
        let schema = r#"{"type":"struct","fields":[{"name":"a","type":"long","nullable":false,"metadata":{}}]}"#;
        assert_eq!(
            parse_spark_schema(schema).unwrap(),
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)]))
        );
    }

    #[test]
    fn rejects_malformed_and_unsupported_json_schemas() {
        // Truncated JSON must not be mistaken for DDL.
        assert!(parse_spark_schema(r#"{"type":"struct","fields":["#).is_err());
        assert!(parse_spark_schema(r#"{"fields":[]}"#).is_err());
        assert!(parse_spark_schema(r#"{"type":"struct"}"#).is_err());
        assert!(parse_spark_schema(
            r#"{"type":"struct","fields":[{"name":"a","type":"nonesuch","nullable":true,"metadata":{}}]}"#
        )
        .is_err());
        assert!(parse_spark_schema(r#"{"type":"udt","class":"x"}"#).is_err());
    }

    #[test]
    fn pattern_translation_and_narrow_text_rejected() {
        assert_eq!(
            spark_pattern_to_chrono("yyyy-MM-dd").as_deref(),
            Some("%Y-%m-%d")
        );
        assert_eq!(
            spark_pattern_to_chrono("dd/MM/yyyy").as_deref(),
            Some("%d/%m/%Y")
        );
        assert_eq!(
            spark_pattern_to_chrono("MM/dd yyyy HH:mm:ss").as_deref(),
            Some("%m/%d %Y %H:%M:%S")
        );
        // Narrow text (5×M) is unsupported — Spark rejects it too.
        assert_eq!(spark_pattern_to_chrono("dd/MMMMM/yyyy"), None);
    }
}
