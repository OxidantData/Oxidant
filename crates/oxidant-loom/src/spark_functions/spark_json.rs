//! Spark JSON scalar functions that DataFusion's core does not provide.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics (golden file:
//! `oxidant-spark-compat/spark-tests/{inputs,results}/json-functions.sql*`):
//!
//! - `get_json_object(json, path)` — extract a value from a JSON string using a Spark JSONPath
//!   (`$`, `$.field`, `$['field']`, `$[index]`, and chains thereof). A scalar result is returned as
//!   its bare string form (`1`, `true`, `abc` — *no* surrounding quotes for a JSON string); an
//!   object/array result is re-serialized as compact JSON. Returns `string`, or `NULL` when the
//!   JSON is malformed, the path does not resolve, or any argument is `NULL`.
//! - `json_array_length(json)` — the length of the *top-level* JSON array, as `int`. Returns `NULL`
//!   when the input is `NULL`, empty, malformed, or not a top-level array.
//! - `json_object_keys(json)` — the keys of the *top-level* JSON object, in document order, as
//!   `array<string>`. Returns `NULL` when the input is `NULL`, empty, malformed, or not a top-level
//!   object; an empty object yields `[]`.
//! - `to_json(expr)` — serialize a `struct`/`map`/`array` (arbitrarily nested over those plus the
//!   primitive Arrow types) to a compact Spark-style JSON `string`. Map keys are stringified the way
//!   Spark does (a struct key `{a:1,b:2}` becomes the string `"[1,2]"`). The optional second
//!   `options` map argument (e.g. `timestampFormat`) is **not** honored — temporal values use the
//!   default ISO-ish rendering — and is otherwise ignored.
//!
//! Deferred (with rationale):
//! - `from_json(json, schema[, options])` — requires a full Spark-DDL schema-string parser
//!   (`struct<a:int,b:string>` / `a INT, b STRING` / `map<...>` / `array<...>`) plus typed JSON
//!   coercion; too involved to implement faithfully in this batch.
//! - `json_tuple(json, k1, ...)` and `schema_of_json(json)` — `json_tuple` is a *generator*
//!   (table-valued: one output column per key) and `schema_of_json` needs Spark's type-inference
//!   rules; neither fits the scalar-UDF shape here.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all JSON Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(GetJsonObject::new()));
    ctx.register_udf(ScalarUDF::from(JsonArrayLength::new()));
    ctx.register_udf(ScalarUDF::from(JsonObjectKeys::new()));
    ctx.register_udf(ScalarUDF::from(ToJson::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// get_json_object
// ---------------------------------------------------------------------------

/// `get_json_object(json, path)` — extract via a Spark JSONPath. See module docs for semantics.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GetJsonObject {
    signature: Signature,
}

impl GetJsonObject {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// One step in a Spark JSONPath.
#[derive(Debug, Clone)]
enum PathStep {
    /// `.name` or `['name']` — descend into an object field.
    Key(String),
    /// `[i]` — index into an array.
    Index(usize),
    /// `[*]` / `.*` wildcard — not supported here (returns no match), kept for completeness.
    Wildcard,
}

/// Parse a Spark JSONPath like `$.a.b[0]['c']` into a list of steps. Returns `None` if the path is
/// malformed or does not start with `$` (Spark returns NULL in that case).
fn parse_json_path(path: &str) -> Option<Vec<PathStep>> {
    let bytes = path.as_bytes();
    if bytes.is_empty() || bytes[0] != b'$' {
        return None;
    }
    let mut steps = Vec::new();
    let mut i = 1usize;
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                if i < bytes.len() && bytes[i] == b'*' {
                    steps.push(PathStep::Wildcard);
                    i += 1;
                    continue;
                }
                // Read a dotted key up to the next '.' or '['.
                let start = i;
                while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                    i += 1;
                }
                if i == start {
                    return None;
                }
                steps.push(PathStep::Key(path[start..i].to_string()));
            }
            b'[' => {
                i += 1;
                if i < bytes.len() && bytes[i] == b'*' {
                    // `[*]`
                    i += 1;
                    if i >= bytes.len() || bytes[i] != b']' {
                        return None;
                    }
                    i += 1;
                    steps.push(PathStep::Wildcard);
                    continue;
                }
                if i < bytes.len() && (bytes[i] == b'\'' || bytes[i] == b'"') {
                    // `['name']` / `["name"]`
                    let quote = bytes[i];
                    i += 1;
                    let start = i;
                    while i < bytes.len() && bytes[i] != quote {
                        i += 1;
                    }
                    if i >= bytes.len() {
                        return None;
                    }
                    let key = path[start..i].to_string();
                    i += 1; // closing quote
                    if i >= bytes.len() || bytes[i] != b']' {
                        return None;
                    }
                    i += 1; // closing bracket
                    steps.push(PathStep::Key(key));
                } else {
                    // `[index]`
                    let start = i;
                    while i < bytes.len() && bytes[i] != b']' {
                        i += 1;
                    }
                    if i >= bytes.len() {
                        return None;
                    }
                    let idx: usize = path[start..i].trim().parse().ok()?;
                    i += 1; // closing bracket
                    steps.push(PathStep::Index(idx));
                }
            }
            _ => return None,
        }
    }
    Some(steps)
}

/// Walk `value` following `steps`; return the matched node or `None`.
fn json_walk<'a>(
    value: &'a serde_json::Value,
    steps: &[PathStep],
) -> Option<&'a serde_json::Value> {
    let mut cur = value;
    for step in steps {
        match step {
            PathStep::Key(k) => {
                cur = cur.as_object()?.get(k)?;
            }
            PathStep::Index(idx) => {
                cur = cur.as_array()?.get(*idx)?;
            }
            PathStep::Wildcard => return None,
        }
    }
    Some(cur)
}

/// Render a matched JSON node the way Spark's `get_json_object` does: a JSON string yields its raw
/// contents (no quotes), other scalars yield their literal text, and objects/arrays yield compact
/// JSON.
fn render_json_node(node: &serde_json::Value) -> String {
    match node {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        // Objects/arrays: compact serialization (serde_json default is already compact).
        other => other.to_string(),
    }
}

impl ScalarUDFImpl for GetJsonObject {
    fn name(&self) -> &str {
        "get_json_object"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let json = args.args[0].clone().into_array(n)?;
        let path = args.args[1].clone().into_array(n)?;
        let json = datafusion::arrow::compute::cast(&json, &DataType::Utf8).map_err(arrow_err)?;
        let path = datafusion::arrow::compute::cast(&path, &DataType::Utf8).map_err(arrow_err)?;
        let json = json.as_any().downcast_ref::<StringArray>().unwrap();
        let path = path.as_any().downcast_ref::<StringArray>().unwrap();

        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..n {
            if json.is_null(i) || path.is_null(i) {
                out.append_null();
                continue;
            }
            let steps = match parse_json_path(path.value(i)) {
                Some(s) => s,
                None => {
                    out.append_null();
                    continue;
                }
            };
            let parsed: serde_json::Result<serde_json::Value> = serde_json::from_str(json.value(i));
            match parsed {
                Ok(v) => match json_walk(&v, &steps) {
                    Some(node) => out.append_value(render_json_node(node)),
                    None => out.append_null(),
                },
                Err(_) => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// json_array_length
// ---------------------------------------------------------------------------

/// `json_array_length(json)` — top-level array length, as `int`. Non-array / malformed / NULL / ''
/// => NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayLength {
    signature: Signature,
}

impl JsonArrayLength {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonArrayLength {
    fn name(&self) -> &str {
        "json_array_length"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        // Spark requires a STRING first argument; integral inputs are an analysis error. We only see
        // the runtime type here, so reject a clearly non-string/non-null input to mirror that.
        let dt = args.arg_fields[0].data_type();
        if !matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        ) {
            return exec_err!("json_array_length: first argument must be a string, got {dt:?}");
        }
        let json = args.args[0].clone().into_array(n)?;
        let json = datafusion::arrow::compute::cast(&json, &DataType::Utf8).map_err(arrow_err)?;
        let json = json.as_any().downcast_ref::<StringArray>().unwrap();

        let out: Int32Array = (0..n)
            .map(|i| {
                if json.is_null(i) {
                    return None;
                }
                let parsed: serde_json::Result<serde_json::Value> =
                    serde_json::from_str(json.value(i));
                match parsed {
                    Ok(serde_json::Value::Array(a)) => Some(a.len() as i32),
                    _ => None,
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// json_object_keys
// ---------------------------------------------------------------------------

/// `json_object_keys(json)` — keys of the top-level object, in document order, as `array<string>`.
/// Non-object / malformed / NULL / '' => NULL; `{}` => `[]`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObjectKeys {
    signature: Signature,
}

impl JsonObjectKeys {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonObjectKeys {
    fn name(&self) -> &str {
        "json_object_keys"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "element",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let dt = args.arg_fields[0].data_type();
        if !matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        ) {
            return exec_err!("json_object_keys: first argument must be a string, got {dt:?}");
        }
        let json = args.args[0].clone().into_array(n)?;
        let json = datafusion::arrow::compute::cast(&json, &DataType::Utf8).map_err(arrow_err)?;
        let json = json.as_any().downcast_ref::<StringArray>().unwrap();

        // Build a ListArray<Utf8> by hand: a flat values array of all keys, an offset buffer, and a
        // null buffer marking the rows that produced no list (NULL).
        let mut values: Vec<String> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
        offsets.push(0);
        let mut validity: Vec<bool> = Vec::with_capacity(n);

        for i in 0..n {
            if json.is_null(i) {
                validity.push(false);
                offsets.push(values.len() as i32);
                continue;
            }
            match top_level_object_keys(json.value(i)) {
                Some(keys) => {
                    for k in keys {
                        values.push(k);
                    }
                    validity.push(true);
                }
                None => validity.push(false),
            }
            offsets.push(values.len() as i32);
        }

        let values_arr: ArrayRef = Arc::new(StringArray::from(values));
        let field = Arc::new(Field::new("element", DataType::Utf8, true));
        let offset_buffer = OffsetBuffer::new(offsets.into());
        let null_buffer = datafusion::arrow::buffer::NullBuffer::from(validity);
        let list = ListArray::try_new(field, offset_buffer, values_arr, Some(null_buffer))
            .map_err(arrow_err)?;
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

/// Return the keys of a top-level JSON object in *document order*, or `None` if the text is not a
/// valid top-level JSON object. Uses `serde_json`'s streaming `Deserializer` so we observe keys in
/// the order they appear (the `Value` map, without the `preserve_order` feature, would reorder).
fn top_level_object_keys(text: &str) -> Option<Vec<String>> {
    use serde::de::{Deserializer, MapAccess, Visitor};
    use std::fmt;

    struct KeysVisitor;
    impl<'de> Visitor<'de> for KeysVisitor {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<A: MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Vec<String>, A::Error> {
            let mut keys = Vec::new();
            while let Some(k) = map.next_key::<String>()? {
                keys.push(k);
                // Skip the value (ignore its contents but consume it).
                map.next_value::<serde::de::IgnoredAny>()?;
            }
            Ok(keys)
        }
    }

    let mut de = serde_json::Deserializer::from_str(text);
    let result = de.deserialize_map(KeysVisitor);
    match result {
        Ok(keys) => {
            // Ensure the whole input was consumed (reject trailing garbage like `{} junk`).
            de.end().ok()?;
            Some(keys)
        }
        Err(_) => None,
    }
}

// ---------------------------------------------------------------------------
// to_json
// ---------------------------------------------------------------------------

/// `to_json(expr [, options])` — serialize a struct/map/array value to a compact JSON string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJson {
    signature: Signature,
}

impl ToJson {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToJson {
    fn name(&self) -> &str {
        "to_json"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!(
                "to_json: expected 1 or 2 arguments, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            let mut s = String::new();
            json_encode_value(&arr, i, &mut s)?;
            out.append_value(s);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Append the compact-JSON encoding of element `row` of `arr` to `out`. Mirrors Spark's
/// `StructsToJson`: structs become objects (field order = schema order, null fields skipped), maps
/// become objects (string-coerced keys), lists become arrays, primitives become JSON scalars. A
/// NULL element becomes `null`.
fn json_encode_value(arr: &ArrayRef, row: usize, out: &mut String) -> Result<()> {
    if arr.is_null(row) {
        out.push_str("null");
        return Ok(());
    }
    match arr.data_type() {
        DataType::Null => out.push_str("null"),
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            out.push_str(if a.value(row) { "true" } else { "false" });
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let casted =
                datafusion::arrow::compute::cast(arr, &DataType::Int64).map_err(arrow_err)?;
            let a = casted
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            out.push_str(&a.value(row).to_string());
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let casted =
                datafusion::arrow::compute::cast(arr, &DataType::Float64).map_err(arrow_err)?;
            let a = casted
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .unwrap();
            out.push_str(&a.value(row).to_string());
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            let s = datafusion::arrow::util::display::array_value_to_string(arr, row)
                .map_err(arrow_err)?;
            out.push_str(&s);
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let casted =
                datafusion::arrow::compute::cast(arr, &DataType::Utf8).map_err(arrow_err)?;
            let a = casted.as_any().downcast_ref::<StringArray>().unwrap();
            push_json_string(a.value(row), out);
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_) => {
            // Temporal: render via Arrow's display (ISO-ish), quoted. The `options` timestampFormat
            // is not honored (deferred); this matches the *default* Spark rendering for dates.
            let s = datafusion::arrow::util::display::array_value_to_string(arr, row)
                .map_err(arrow_err)?;
            push_json_string(&s, out);
        }
        DataType::Struct(_) => {
            let st = arr.as_any().downcast_ref::<StructArray>().unwrap();
            out.push('{');
            let columns = st.columns();
            let fields = match st.data_type() {
                DataType::Struct(f) => f,
                _ => unreachable!(),
            };
            let mut first = true;
            for (col, field) in columns.iter().zip(fields.iter()) {
                // Spark's StructsToJson skips null struct fields entirely.
                if col.is_null(row) {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                push_json_string(field.name(), out);
                out.push(':');
                json_encode_value(col, row, out)?;
            }
            out.push('}');
        }
        DataType::List(_) => {
            let la = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let values = la.value(row);
            json_encode_array(&values, out)?;
        }
        DataType::LargeList(_) => {
            let la = arr
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeListArray>()
                .unwrap();
            let values = la.value(row);
            json_encode_array(&values, out)?;
        }
        DataType::FixedSizeList(_, _) => {
            let la = arr
                .as_any()
                .downcast_ref::<datafusion::arrow::array::FixedSizeListArray>()
                .unwrap();
            let values = la.value(row);
            json_encode_array(&values, out)?;
        }
        DataType::Map(_, _) => {
            let ma = arr.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = ma.value(row);
            let keys = entries.column(0);
            let vals = entries.column(1);
            out.push('{');
            for j in 0..entries.len() {
                if j > 0 {
                    out.push(',');
                }
                // Map keys are coerced to a JSON string. For non-string keys (e.g. a struct), Spark
                // renders the key as JSON text and uses that text as the string key.
                let key_str = map_key_to_string(keys, j)?;
                push_json_string(&key_str, out);
                out.push(':');
                json_encode_value(vals, j, out)?;
            }
            out.push('}');
        }
        other => {
            return exec_err!("to_json: unsupported value type {other:?}");
        }
    }
    Ok(())
}

/// Encode every element of `values` as a JSON array into `out`.
fn json_encode_array(values: &ArrayRef, out: &mut String) -> Result<()> {
    out.push('[');
    for j in 0..values.len() {
        if j > 0 {
            out.push(',');
        }
        json_encode_value(values, j, out)?;
    }
    out.push(']');
    Ok(())
}

/// Stringify a map key. A `Utf8` key is used verbatim; a struct key is encoded as a compact JSON
/// *array* of its field values (Spark: `{a:1,b:2}` -> `"[1,2]"`); any other scalar key uses its
/// bare JSON-scalar form.
fn map_key_to_string(keys: &ArrayRef, row: usize) -> Result<String> {
    match keys.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let casted =
                datafusion::arrow::compute::cast(keys, &DataType::Utf8).map_err(arrow_err)?;
            let a = casted.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(a.value(row).to_string())
        }
        DataType::Struct(_) => {
            let st = keys.as_any().downcast_ref::<StructArray>().unwrap();
            let mut s = String::from("[");
            for (idx, col) in st.columns().iter().enumerate() {
                if idx > 0 {
                    s.push(',');
                }
                json_encode_value(col, row, &mut s)?;
            }
            s.push(']');
            Ok(s)
        }
        _ => {
            let mut s = String::new();
            json_encode_value(keys, row, &mut s)?;
            Ok(s)
        }
    }
}

/// Append `s` as a JSON string literal (with the minimal required escaping) to `out`.
fn push_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// Run `q` and return the single scalar cell as a string, mapping NULL to "NULL".
    async fn cell(q: &str) -> String {
        use datafusion::arrow::array::Array;
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        let col = batches[0].column(0);
        if col.is_null(0) {
            return "NULL".to_string();
        }
        let txt = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        txt.lines()
            .nth(3)
            .unwrap()
            .trim_matches(|c| c == '|' || c == ' ')
            .to_string()
    }

    #[tokio::test]
    async fn json_array_length_corpus() {
        assert_eq!(cell("SELECT json_array_length(null) AS x").await, "NULL");
        assert_eq!(cell("SELECT json_array_length('') AS x").await, "NULL");
        assert_eq!(cell("SELECT json_array_length('[]') AS x").await, "0");
        assert_eq!(cell("SELECT json_array_length('[1,2,3]') AS x").await, "3");
        assert_eq!(
            cell("SELECT json_array_length('[[1,2],[5,6,7]]') AS x").await,
            "2"
        );
        assert_eq!(
            cell("SELECT json_array_length('[{\"a\":123},{\"b\":\"hello\"}]') AS x").await,
            "2"
        );
        assert_eq!(
            cell("SELECT json_array_length('[1,2,3,[33,44],{\"key\":[2,3,4]}]') AS x").await,
            "5"
        );
        assert_eq!(
            cell("SELECT json_array_length('{\"key\":\"not a json array\"}') AS x").await,
            "NULL"
        );
        assert_eq!(
            cell("SELECT json_array_length('[1,2,3,4,5') AS x").await,
            "NULL"
        );
        // Non-string input is an error.
        let engine = Engine::new();
        assert!(engine.sql("SELECT json_array_length(2)").await.is_err());
    }

    #[tokio::test]
    async fn json_object_keys_corpus() {
        assert_eq!(cell("SELECT json_object_keys(null) AS x").await, "NULL");
        assert_eq!(cell("SELECT json_object_keys('') AS x").await, "NULL");
        assert_eq!(cell("SELECT json_object_keys('{}') AS x").await, "[]");
        assert_eq!(
            cell("SELECT json_object_keys('{\"key\": 1}') AS x").await,
            "[key]"
        );
        assert_eq!(
            cell("SELECT json_object_keys('{\"key\": \"value\", \"key2\": 2}') AS x").await,
            "[key, key2]"
        );
        assert_eq!(
            cell("SELECT json_object_keys('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\", \"f4\":\"b\"}}') AS x")
                .await,
            "[f1, f2]"
        );
        assert_eq!(
            cell("SELECT json_object_keys('{[1,2]}') AS x").await,
            "NULL"
        );
        assert_eq!(
            cell("SELECT json_object_keys('{\"key\": 45, \"random_string\"}') AS x").await,
            "NULL"
        );
        assert_eq!(
            cell("SELECT json_object_keys('[1, 2, 3]') AS x").await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn get_json_object_basic() {
        assert_eq!(
            cell("SELECT get_json_object('{\"a\":1,\"b\":2}', '$.a') AS x").await,
            "1"
        );
        assert_eq!(
            cell("SELECT get_json_object('{\"a\":\"hi\"}', '$.a') AS x").await,
            "hi"
        );
        assert_eq!(
            cell("SELECT get_json_object('{\"a\":{\"b\":3}}', '$.a.b') AS x").await,
            "3"
        );
        assert_eq!(
            cell("SELECT get_json_object('{\"a\":[10,20,30]}', '$.a[1]') AS x").await,
            "20"
        );
        // object result -> compact JSON
        assert_eq!(
            cell("SELECT get_json_object('{\"a\":{\"b\":3}}', '$.a') AS x").await,
            "{\"b\":3}"
        );
        // missing path -> NULL
        assert_eq!(
            cell("SELECT get_json_object('{\"a\":1}', '$.z') AS x").await,
            "NULL"
        );
        // malformed json -> NULL
        assert_eq!(
            cell("SELECT get_json_object('{not json', '$.a') AS x").await,
            "NULL"
        );
        // bracket-quoted key
        assert_eq!(
            cell("SELECT get_json_object('{\"a b\":7}', '$[''a b'']') AS x").await,
            "7"
        );
    }

    // NOTE: these tests drive `to_json` through DataFusion's *native* constructor spellings
    // (`make_array`, `named_struct`, and the two-array `map(keys, values)` form). The Spark-syntax
    // forms in the golden (`array(...)`, `map('a',1,'b',2)`) are a parser-layer concern handled
    // elsewhere; here we exercise the UDF's value-serialization logic directly.
    #[tokio::test]
    async fn to_json_corpus() {
        assert_eq!(
            cell("SELECT to_json(named_struct('a', 1, 'b', 2)) AS x").await,
            "{\"a\":1,\"b\":2}"
        );
        assert_eq!(
            cell("SELECT to_json(make_array(named_struct('a', 1, 'b', 2))) AS x").await,
            "[{\"a\":1,\"b\":2}]"
        );
        assert_eq!(
            cell("SELECT to_json(map(make_array('a'), make_array(1))) AS x").await,
            "{\"a\":1}"
        );
        assert_eq!(
            cell(
                "SELECT to_json(map(make_array('a'), make_array(named_struct('a', 1, 'b', 2)))) AS x"
            )
            .await,
            "{\"a\":{\"a\":1,\"b\":2}}"
        );
        assert_eq!(
            cell(
                "SELECT to_json(make_array(map(make_array('a'), make_array(1)), map(make_array('b'), make_array(2)))) AS x"
            )
            .await,
            "[{\"a\":1},{\"b\":2}]"
        );
        assert_eq!(
            cell("SELECT to_json(make_array('1', '2', '3')) AS x").await,
            "[\"1\",\"2\",\"3\"]"
        );
        assert_eq!(
            cell("SELECT to_json(make_array(make_array(1, 2, 3), make_array(4))) AS x").await,
            "[[1,2,3],[4]]"
        );
        // map with a struct key -> stringified as a JSON array `"[1,2]"`.
        assert_eq!(
            cell(
                "SELECT to_json(map(make_array(named_struct('a', 1, 'b', 2)), make_array(named_struct('a', 1, 'b', 2)))) AS x"
            )
            .await,
            "{\"[1,2]\":{\"a\":1,\"b\":2}}"
        );
    }
}
