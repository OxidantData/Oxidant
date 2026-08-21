//! Spark-only array / map scalar functions, implemented as DataFusion `ScalarUDF`s.
//!
//! Faithful to Apache Spark v4.0.0 semantics (see
//! `oxidant-spark-compat/spark-tests/{inputs,results}/{array,map,try_element_at}.sql*`):
//!
//! - `array_size(arr)` — number of elements as `int`. `NULL` input → `NULL`; empty array → `0`.
//!   A non-array argument (e.g. a map) is a type error in Spark; we mirror that at runtime.
//!   (DataFusion's `cardinality` is close but returns `UInt64`, not Spark's `int`, so we keep a
//!   thin UDF to nail the output type.)
//! - `sort_array(arr [, ascendingOrder])` — sort the array; ascending by default. NULLs sort
//!   **first** when ascending and **last** when descending (Spark semantics). The optional second
//!   argument must be a boolean literal; `sort_array(arr, CAST(NULL AS BOOLEAN))` yields `NULL`.
//! - `map_contains_key(map, key)` — `boolean`, true iff `key` is present among the map's keys.
//! - `try_element_at(arr_or_map, index_or_key)` — like `element_at` but returns `NULL` instead of
//!   erroring on an out-of-bounds array index or a missing map key. Array indexing is **1-based**,
//!   negative indexes count from the end, and index `0` is still a runtime error
//!   (`INVALID_INDEX_OF_ZERO`), matching Spark.
//!
//! Functions DataFusion already provides under another name are handled as aliases in
//! `crate::register_spark_function_aliases` (e.g. `array` → `make_array`), not here.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, MapArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{cast, concat, SortOptions};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all array/map Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ArraySize::new()));
    ctx.register_udf(ScalarUDF::from(SortArray::new()));
    ctx.register_udf(ScalarUDF::from(MapContainsKey::new()));
    ctx.register_udf(ScalarUDF::from(TryElementAt::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// array_size
// ---------------------------------------------------------------------------

/// `array_size(expr)` — element count as `int`. NULL → NULL, empty → 0; non-array is a type error.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraySize {
    signature: Signature,
}

impl ArraySize {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArraySize {
    fn name(&self) -> &str {
        "array_size"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(args.number_rows)?;
        let out: Int32Array = match &dt {
            DataType::Null => Int32Array::from(vec![None; args.number_rows]),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                let list = datafusion::arrow::array::cast::as_list_array(&arr);
                (0..list.len())
                    .map(|i| (!list.is_null(i)).then(|| list.value(i).len() as i32))
                    .collect()
            }
            other => {
                return exec_err!("array_size: argument 1 requires the ARRAY type, got {other:?}")
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// sort_array
// ---------------------------------------------------------------------------

/// `sort_array(arr [, ascendingOrder])`. Ascending by default; NULLs first when ascending, last
/// when descending. The order flag must be a boolean; a NULL flag makes the whole result NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SortArray {
    signature: Signature,
}

impl SortArray {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SortArray {
    fn name(&self) -> &str {
        "sort_array"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // The result is the same list type as the input array.
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Null),
            dt @ (DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)) => {
                Ok(dt.clone())
            }
            other => exec_err!("sort_array: argument 1 requires the ARRAY type, got {other:?}"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.args.len();
        if !(1..=2).contains(&n) {
            return exec_err!("sort_array: expected 1 or 2 arguments, got {n}");
        }
        let in_dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(args.number_rows)?;

        if in_dt == DataType::Null {
            return Ok(ColumnarValue::Array(arr));
        }

        // Resolve the ascending flag. Default true; must be a boolean. A NULL flag => NULL result.
        let mut ascending = true;
        if n == 2 {
            let flag = args.args[1].clone().into_array(args.number_rows)?;
            if flag.data_type() != &DataType::Boolean {
                return exec_err!(
                    "sort_array: the second argument must be a boolean, got {:?}",
                    flag.data_type()
                );
            }
            let flag = datafusion::arrow::array::cast::as_boolean_array(&flag);
            // Spark requires a foldable (constant) order flag; we read row 0. If it is NULL,
            // the entire result is NULL.
            if flag.is_null(0) {
                return Ok(ColumnarValue::Array(arrow_null_list(
                    &in_dt,
                    args.number_rows,
                )?));
            }
            ascending = flag.value(0);
        }

        // NULLs first when ascending, last when descending.
        let opts = SortOptions {
            descending: !ascending,
            nulls_first: ascending,
        };

        let list = datafusion::arrow::array::cast::as_list_array(&arr);
        let field = match &in_dt {
            DataType::List(f) => f.clone(),
            // For Large/FixedSize we still emitted List in return_type? No — handle List only here.
            _ => Arc::new(Field::new("item", list.value_type().clone(), true)),
        };

        let mut sorted_rows: Vec<Option<ArrayRef>> = Vec::with_capacity(list.len());
        for i in 0..list.len() {
            if list.is_null(i) {
                sorted_rows.push(None);
                continue;
            }
            let elem = list.value(i);
            let sorted = datafusion::arrow::compute::sort(&elem, Some(opts)).map_err(arrow_err)?;
            sorted_rows.push(Some(sorted));
        }

        let out = build_list_array(&sorted_rows, field, &list.value_type())?;
        Ok(ColumnarValue::Array(out))
    }
}

/// Build an all-NULL `List` array of `len` rows with the given list data type.
fn arrow_null_list(list_dt: &DataType, len: usize) -> Result<ArrayRef> {
    let field = match list_dt {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => f.clone(),
        other => return exec_err!("sort_array: not a list type: {other:?}"),
    };
    let empty_values = datafusion::arrow::array::new_empty_array(field.data_type());
    let offsets = OffsetBuffer::<i32>::new(vec![0i32; len + 1].into());
    let nulls = datafusion::arrow::buffer::NullBuffer::new_null(len);
    let list = ListArray::try_new(field, offsets, empty_values, Some(nulls)).map_err(arrow_err)?;
    Ok(Arc::new(list))
}

/// Reassemble per-row (possibly NULL) value arrays into a single `ListArray`.
fn build_list_array(
    rows: &[Option<ArrayRef>],
    field: Arc<Field>,
    value_type: &DataType,
) -> Result<ArrayRef> {
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    let mut null_mask: Vec<bool> = Vec::with_capacity(rows.len());
    let mut pieces: Vec<ArrayRef> = Vec::new();
    let mut acc: i32 = 0;
    for row in rows {
        match row {
            Some(a) => {
                acc += a.len() as i32;
                pieces.push(a.clone());
                null_mask.push(true);
            }
            None => null_mask.push(false),
        }
        offsets.push(acc);
    }
    let values: ArrayRef = if pieces.is_empty() {
        datafusion::arrow::array::new_empty_array(value_type)
    } else {
        let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
        concat(&refs).map_err(arrow_err)?
    };
    let nulls = datafusion::arrow::buffer::NullBuffer::from(null_mask);
    let list = ListArray::try_new(
        field,
        OffsetBuffer::<i32>::new(offsets.into()),
        values,
        Some(nulls),
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

// ---------------------------------------------------------------------------
// map_contains_key
// ---------------------------------------------------------------------------

/// `map_contains_key(map, key)` — true iff `key` is among the map's keys.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapContainsKey {
    signature: Signature,
}

impl MapContainsKey {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapContainsKey {
    fn name(&self) -> &str {
        "map_contains_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let map = args.args[0].clone().into_array(args.number_rows)?;
        let key = args.args[1].clone().into_array(args.number_rows)?;
        let map = match map.data_type() {
            DataType::Map(_, _) => datafusion::arrow::array::cast::as_map_array(&map),
            other => {
                return exec_err!("map_contains_key: first argument must be a map, got {other:?}")
            }
        };
        // Spark compares the probe against the map's keys at their least-common comparison type,
        // so `map_contains_key(map(1,'a',2,'b'), 1.0)` is `true` (int key widened to double). When
        // the two have no common comparison type (e.g. string keys vs an int probe) Spark raises
        // an analysis-time `DATATYPE_MISMATCH`; we mirror that with a plan error (→ both engines
        // reject, an `error-parity` semantic pass) rather than silently returning `false`.
        let key_type = map.keys().data_type();
        let probe_type = key.data_type();
        // Spark forbids comparing a string key with a numeric probe (and vice-versa) — it raises
        // `DATATYPE_MISMATCH` rather than implicitly coercing across the string/numeric boundary,
        // even though DataFusion's `comparison_coercion` would happily pick a common type. Reject
        // that cross-family case up front so we match Spark instead of silently answering.
        let is_str =
            |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
        let cross_family = (is_str(key_type) && probe_type.is_numeric())
            || (is_str(probe_type) && key_type.is_numeric());
        let common = comparison_coercion(key_type, probe_type)
            .filter(|_| !cross_family)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "map_contains_key: cannot compare map key type {key_type:?} with argument \
                     type {probe_type:?}"
                ))
            })?;
        let keys_cast = cast(map.keys(), &common).map_err(arrow_err)?;
        let probe_cast = cast(&key, &common).map_err(arrow_err)?;
        let offsets = map.value_offsets();
        let mut out = BooleanArray::builder(args.number_rows);
        for i in 0..args.number_rows {
            if map.is_null(i) || probe_cast.is_null(i) {
                out.append_null();
                continue;
            }
            let probe = ScalarValue::try_from_array(&probe_cast, i)?;
            let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
            let mut found = false;
            for k in start..end {
                if keys_cast.is_null(k) {
                    continue;
                }
                if ScalarValue::try_from_array(&keys_cast, k)? == probe {
                    found = true;
                    break;
                }
            }
            out.append_value(found);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// try_element_at
// ---------------------------------------------------------------------------

/// `try_element_at(arr_or_map, index_or_key)` — `element_at` that returns NULL on out-of-bounds /
/// missing instead of erroring. Array indexing is 1-based; negative indexes count from the end;
/// index 0 is still a runtime error (Spark `INVALID_INDEX_OF_ZERO`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct TryElementAt {
    signature: Signature,
}

impl TryElementAt {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryElementAt {
    fn name(&self) -> &str {
        "try_element_at"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Null),
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
                Ok(f.data_type().clone())
            }
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => Ok(fields[1].data_type().clone()),
                other => exec_err!("try_element_at: malformed map entries type {other:?}"),
            },
            other => {
                exec_err!("try_element_at: first argument must be an array or a map, got {other:?}")
            }
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let container = args.args[0].clone().into_array(args.number_rows)?;
        let idx = args.args[1].clone().into_array(args.number_rows)?;
        match container.data_type() {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                let list = datafusion::arrow::array::cast::as_list_array(&container);
                // Index must be an integer; cast to Int64 for uniform handling.
                let idx =
                    datafusion::arrow::compute::cast(&idx, &DataType::Int64).map_err(arrow_err)?;
                let idx = datafusion::common::cast::as_int64_array(&idx)?;
                let value_type = list.value_type();
                let mut picked: Vec<Option<ArrayRef>> = Vec::with_capacity(args.number_rows);
                for i in 0..args.number_rows {
                    if list.is_null(i) || idx.is_null(i) {
                        picked.push(None);
                        continue;
                    }
                    let row = list.value(i);
                    let len = row.len() as i64;
                    let raw = idx.value(i);
                    if raw == 0 {
                        return exec_err!(
                            "try_element_at: SQL array indices start at 1 (INVALID_INDEX_OF_ZERO)"
                        );
                    }
                    // 1-based; negatives from the end.
                    let zero_based = if raw > 0 { raw - 1 } else { len + raw };
                    if zero_based < 0 || zero_based >= len {
                        picked.push(None);
                    } else {
                        picked.push(Some(row.slice(zero_based as usize, 1)));
                    }
                }
                let out = concat_or_null(&picked, &value_type)?;
                Ok(ColumnarValue::Array(out))
            }
            DataType::Map(_, _) => {
                let map = datafusion::arrow::array::cast::as_map_array(&container);
                let value_type = map.value_type().clone();
                let mut picked: Vec<Option<ArrayRef>> = Vec::with_capacity(args.number_rows);
                for i in 0..args.number_rows {
                    if map.is_null(i) || idx.is_null(i) {
                        picked.push(None);
                        continue;
                    }
                    let probe = ScalarValue::try_from_array(&idx, i)?;
                    picked.push(map_row_get(map, i, &probe)?);
                }
                let out = concat_or_null(&picked, &value_type)?;
                Ok(ColumnarValue::Array(out))
            }
            other => {
                exec_err!("try_element_at: first argument must be an array or a map, got {other:?}")
            }
        }
    }
}

/// Fetch the value for `probe` in row `i` of `map` as a length-1 array, or `None` if absent.
fn map_row_get(map: &MapArray, i: usize, probe: &ScalarValue) -> Result<Option<ArrayRef>> {
    let offsets = map.value_offsets();
    let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
    let keys = map.keys();
    let values = map.values();
    for k in start..end {
        if keys.is_null(k) {
            continue;
        }
        if &ScalarValue::try_from_array(keys, k)? == probe {
            return Ok(Some(values.slice(k, 1)));
        }
    }
    Ok(None)
}

/// Concatenate per-row length-1 picks into a single array, filling `None` rows with a typed NULL.
fn concat_or_null(picks: &[Option<ArrayRef>], value_type: &DataType) -> Result<ArrayRef> {
    let null_one = ScalarValue::try_from(value_type)
        .map_err(|e| DataFusionError::Execution(format!("try_element_at: {e}")))?
        .to_array_of_size(1)?;
    let pieces: Vec<ArrayRef> = picks
        .iter()
        .map(|p| p.clone().unwrap_or_else(|| null_one.clone()))
        .collect();
    if pieces.is_empty() {
        return Ok(datafusion::arrow::array::new_empty_array(value_type));
    }
    let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
    concat(&refs).map_err(arrow_err)
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    async fn run(q: &str) -> String {
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn array_size_matches_spark() {
        assert!(run("SELECT array_size(array()) AS a").await.contains("| 0"));
        assert!(run("SELECT array_size(array(true)) AS a")
            .await
            .contains("| 1"));
        assert!(run("SELECT array_size(array(2, 1)) AS a")
            .await
            .contains("| 2"));
        // NULL input -> NULL output (rendered as an empty cell).
        assert!(run("SELECT array_size(NULL) AS a").await.contains("|   |"));
        // map argument is a type error.
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT array_size(map('a', 1, 'b', 2))")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn sort_array_ascending_default_and_descending() {
        // Ascending by default.
        assert!(run("SELECT sort_array(array(3, 1, 2)) AS a")
            .await
            .contains("[1, 2, 3]"));
        // Explicit ascending.
        assert!(run("SELECT sort_array(array(3, 1, 2), true) AS a")
            .await
            .contains("[1, 2, 3]"));
        // Descending.
        assert!(run("SELECT sort_array(array(3, 1, 2), false) AS a")
            .await
            .contains("[3, 2, 1]"));
        // Strings.
        assert!(run("SELECT sort_array(array('b', 'a', 'c')) AS a")
            .await
            .contains("[a, b, c]"));
    }

    #[tokio::test]
    async fn sort_array_null_ordering_and_null_flag() {
        // NULLs first ascending.
        let asc = run("SELECT sort_array(array(2, CAST(NULL AS INT), 1)) AS a").await;
        assert!(asc.contains("[, 1, 2]"), "asc null-first: {asc}");
        // NULLs last descending.
        let desc = run("SELECT sort_array(array(2, CAST(NULL AS INT), 1), false) AS a").await;
        assert!(desc.contains("[2, 1, ]"), "desc null-last: {desc}");
        // A NULL boolean order flag => NULL result.
        let nullflag = run("SELECT sort_array(array('b', 'd'), CAST(NULL AS BOOLEAN)) AS a").await;
        assert!(nullflag.contains("|   |"), "null flag => null: {nullflag}");
    }

    #[tokio::test]
    async fn map_contains_key_matches_spark() {
        assert!(run("SELECT map_contains_key(map(1, 'a', 2, 'b'), 5) AS a")
            .await
            .contains("false"));
        assert!(run("SELECT map_contains_key(map(1, 'a', 2, 'b'), 1) AS a")
            .await
            .contains("true"));
        // Spark coerces probe and keys to a common comparison type: int key vs double probe.
        assert!(
            run("SELECT map_contains_key(map(1, 'a', 2, 'b'), 1.0) AS a")
                .await
                .contains("true")
        );
        assert!(
            run("SELECT map_contains_key(map(1.0, 'a', 2, 'b'), 1) AS a")
                .await
                .contains("true")
        );
        // No common comparison type (string keys vs int probe) -> analysis error, like Spark.
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT map_contains_key(map('1', 'a', '2', 'b'), 1)")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn try_element_at_array_indexing() {
        // 1-based positive.
        assert!(run("SELECT try_element_at(array(1, 2, 3), 1) AS a")
            .await
            .contains("| 1"));
        assert!(run("SELECT try_element_at(array(1, 2, 3), 3) AS a")
            .await
            .contains("| 3"));
        // Out of bounds -> NULL.
        assert!(run("SELECT try_element_at(array(1, 2, 3), 4) AS a")
            .await
            .contains("|   |"));
        // Negative from the end.
        assert!(run("SELECT try_element_at(array(1, 2, 3), -1) AS a")
            .await
            .contains("| 3"));
        assert!(run("SELECT try_element_at(array(1, 2, 3), -4) AS a")
            .await
            .contains("|   |"));
        // Index 0 errors.
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT try_element_at(array(1, 2, 3), 0)")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn try_element_at_map_lookup() {
        assert!(run("SELECT try_element_at(map('a','b'), 'a') AS a")
            .await
            .contains("| b"));
        assert!(run("SELECT try_element_at(map('a','b'), 'abc') AS a")
            .await
            .contains("|   |"));
    }
}
