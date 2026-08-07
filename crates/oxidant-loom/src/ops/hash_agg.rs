//! Native grouped aggregation — the first Phase-1 carve-out.
//!
//! The ClickBench total is dominated by high-cardinality `GROUP BY` (Q31–Q35). Architecture
//! §"How a GROUP BY actually runs" prescribes a *radix-partitioned* hash aggregation: hash the
//! group key, route each row to one of `P` partitions, then aggregate every partition
//! **independently and in parallel**. Because partitioning is by key hash, no group ever spans
//! two partitions — the per-partition results are disjoint and concatenate with no merge step.
//! That is exactly the structure here.
//!
//! Scope of this kernel (everything else falls back to DataFusion, which stays correct):
//! - group keys: any Arrow type (encoded with the same order/value-faithful `RowConverter` the
//!   shuffle path uses, so strings/dates/multi-column keys all work);
//! - aggregates: `COUNT(*)` / `COUNT(col)`, and `SUM`/`MIN`/`MAX` over `Int64` and `Float64`.
//!
//! **Measured reality (see `examples/bench_hashagg.rs`): this kernel does NOT beat DataFusion 54
//! for in-process aggregation.** On 16 cores it reaches ~50% of DataFusion's throughput at low
//! cardinality and is worse at very high cardinality (the partial-table merge dominates).
//! DataFusion's vectorized hash aggregation (ahash, tuned `GroupsAccumulator`, repartition
//! parallelism) is already near the hardware limit; beating it decisively is a Velox/DuckDB-class
//! effort, not a quick carve-out. So this is deliberately **not** wired onto the ClickBench query
//! path — routing through it would regress. It is kept as a correct, tested building block for
//! places where Oxidant already owns the data and the boundary (e.g. the distributed shuffle reduce,
//! where inputs are pre-aggregated partials and small). The durable benchmark margin lives in
//! distributed scale-out — not in reimplementing DataFusion's columnar core.

use std::collections::HashMap;
use std::sync::Arc;

use rayon::prelude::*;

use crate::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt32Array};
use crate::arrow::compute::{concat_batches, take};
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use crate::arrow::row::{OwnedRow, RowConverter, SortField};

use oxidant_common::{Error, Result};

/// Which aggregate to compute for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    /// Row count: of all rows (`input = None`, i.e. `COUNT(*)`) or of non-null `input` values.
    Count,
    /// Sum of non-null values.
    Sum,
    /// Minimum non-null value.
    Min,
    /// Maximum non-null value.
    Max,
}

/// One output aggregate column: a function over an optional input column, named `name`.
#[derive(Debug, Clone)]
pub struct AggSpec {
    pub kind: AggKind,
    /// Column index of the aggregate's input; `None` only for `COUNT(*)`.
    pub input: Option<usize>,
    /// Output column name.
    pub name: String,
}

/// An aggregate resolved against the input schema to a concrete (op, type) it can execute.
/// Resolution is where we reject anything outside the supported subset, so the executing loops
/// stay branch-light and infallible on type.
#[derive(Debug, Clone, Copy)]
enum Op {
    Count { input: Option<usize> },
    SumI(usize),
    MinI(usize),
    MaxI(usize),
    SumF(usize),
    MinF(usize),
    MaxF(usize),
}

impl Op {
    fn out_type(self) -> DataType {
        match self {
            Op::Count { .. } | Op::SumI(_) | Op::MinI(_) | Op::MaxI(_) => DataType::Int64,
            Op::SumF(_) | Op::MinF(_) | Op::MaxF(_) => DataType::Float64,
        }
    }
    fn out_nullable(self) -> bool {
        // COUNT is always defined; SUM/MIN/MAX of an all-null group is NULL.
        !matches!(self, Op::Count { .. })
    }
}

/// Running accumulator state for one (group, aggregate). `Copy` so the per-group vectors are
/// cheap to grow and index.
#[derive(Debug, Clone, Copy)]
enum Acc {
    Count(i64),
    I64(Option<i64>),
    F64(Option<f64>),
}

impl Acc {
    fn init(op: &Op) -> Acc {
        match op {
            Op::Count { .. } => Acc::Count(0),
            Op::SumI(_) | Op::MinI(_) | Op::MaxI(_) => Acc::I64(None),
            Op::SumF(_) | Op::MinF(_) | Op::MaxF(_) => Acc::F64(None),
        }
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x100_0000_01b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Partition count: bounded by available cores so the parallel phase saturates the box without
/// over-fragmenting tiny inputs.
fn partition_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(1)
        .max(1)
}

/// Aggregate `batches` grouped by `group_cols`, computing each of `aggs`. Returns a single
/// record batch whose columns are the group keys (in `group_cols` order) followed by the
/// aggregates (in `aggs` order). Output row order is unspecified — like any hash aggregation.
///
/// Returns [`Error::Execution`] for an aggregate/type combination outside the supported subset;
/// the caller should fall back to DataFusion in that case.
pub fn group_aggregate(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    group_cols: &[usize],
    aggs: &[AggSpec],
) -> Result<RecordBatch> {
    let ops = resolve(schema, aggs)?;

    // Order/value-faithful encoding of the key columns (same as the shuffle path).
    let key_fields: Vec<SortField> = group_cols
        .iter()
        .map(|&c| SortField::new(schema.field(c).data_type().clone()))
        .collect();

    // Output schema: group fields (carried verbatim) then one field per aggregate.
    let mut out_fields: Vec<Field> = group_cols
        .iter()
        .map(|&c| {
            let f = schema.field(c);
            Field::new(f.name(), f.data_type().clone(), f.is_nullable())
        })
        .collect();
    for (a, op) in ops.iter().enumerate() {
        out_fields.push(Field::new(&aggs[a].name, op.out_type(), op.out_nullable()));
    }
    let out_schema: SchemaRef = Arc::new(Schema::new(out_fields));

    // Fast path: a single non-null `Int64` group key with integer/count aggregates. This is the
    // common high-cardinality shape (group by an integer id). It skips the `RowConverter` and the
    // `HashMap<OwnedRow>` of the general path — both ruinously slow vs DataFusion — for a tight
    // open-addressing table keyed directly on the `i64`, parallelized across key-hash shards.
    if group_cols.len() == 1
        && matches!(schema.field(group_cols[0]).data_type(), DataType::Int64)
        && !schema.field(group_cols[0]).is_nullable()
        && ops.iter().all(|o| {
            matches!(
                o,
                Op::Count { .. } | Op::SumI(_) | Op::MinI(_) | Op::MaxI(_)
            )
        })
    {
        return agg_i64_key(&out_schema, batches, group_cols[0], &ops);
    }

    // Phase 1 — radix-partition rows by key hash into `p` disjoint buckets.
    let p = partition_count();
    let parts = partition_rows(batches, group_cols, &key_fields, p)?;

    // Phase 2 — aggregate each partition independently and in parallel. Keys never span
    // partitions, so the per-partition outputs are disjoint and need no cross-partition merge.
    let outs: Vec<RecordBatch> = parts
        .par_iter()
        .map(|pb| aggregate_partition(&out_schema, &key_fields, group_cols, &ops, pb))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    if outs.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema));
    }
    concat_batches(&out_schema, &outs)
        .map_err(|e| Error::Execution(format!("concat aggregated partitions: {e}")))
}

// ---------------------------------------------------------------------------------------------
// Fast path: single non-null Int64 group key, integer/count aggregates.
// ---------------------------------------------------------------------------------------------

/// Finalizer mix (fmix64) — spreads an `i64` key across all bits so low-bit slot indexing is
/// well-distributed without a hashing crate.
#[inline]
fn mix(k: i64) -> u64 {
    let mut h = k as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Open-addressing (linear-probe) table mapping an `i64` key to a dense group id, with the
/// per-group accumulators stored flat (`gid * stride + a`).
struct I64Table {
    slot_gid: Vec<u32>, // u32::MAX = empty
    slot_key: Vec<i64>,
    mask: usize,
    keys: Vec<i64>, // gid -> key
    accs: Vec<Acc>, // gid * stride + agg
    stride: usize,
    init: Vec<Acc>,
}

impl I64Table {
    fn new(init: Vec<Acc>) -> Self {
        let cap = 1024;
        I64Table {
            slot_gid: vec![u32::MAX; cap],
            slot_key: vec![0; cap],
            mask: cap - 1,
            keys: Vec::new(),
            accs: Vec::new(),
            stride: init.len(),
            init,
        }
    }

    #[inline]
    fn get_or_insert(&mut self, key: i64) -> usize {
        if (self.keys.len() + 1) * 2 > self.slot_gid.len() {
            self.grow();
        }
        let mut slot = (mix(key) as usize) & self.mask;
        loop {
            let g = self.slot_gid[slot];
            if g == u32::MAX {
                let gid = self.keys.len();
                self.slot_gid[slot] = gid as u32;
                self.slot_key[slot] = key;
                self.keys.push(key);
                self.accs.extend_from_slice(&self.init);
                return gid;
            }
            if self.slot_key[slot] == key {
                return g as usize;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let newcap = self.slot_gid.len() * 2;
        let mask = newcap - 1;
        let mut sg = vec![u32::MAX; newcap];
        let mut sk = vec![0i64; newcap];
        for (gid, &key) in self.keys.iter().enumerate() {
            let mut slot = (mix(key) as usize) & mask;
            while sg[slot] != u32::MAX {
                slot = (slot + 1) & mask;
            }
            sg[slot] = gid as u32;
            sk[slot] = key;
        }
        self.slot_gid = sg;
        self.slot_key = sk;
        self.mask = mask;
    }

    /// Fold another table's partial groups into this one (re-combinable aggregates only).
    fn merge(&mut self, other: &I64Table, ops: &[Op]) {
        for (ogid, &key) in other.keys.iter().enumerate() {
            let g = self.get_or_insert(key);
            let (sb, ob) = (g * self.stride, ogid * self.stride);
            for (a, op) in ops.iter().enumerate() {
                combine(&mut self.accs[sb + a], other.accs[ob + a], op);
            }
        }
    }
}

#[inline]
fn is_null(nulls: Option<&crate::arrow::buffer::NullBuffer>, i: usize) -> bool {
    nulls.is_some_and(|n| n.is_null(i))
}

/// Combine a source partial accumulator into a destination (used by [`I64Table::merge`]).
fn combine(dst: &mut Acc, src: Acc, op: &Op) {
    match (op, dst, src) {
        (Op::Count { .. }, Acc::Count(d), Acc::Count(s)) => *d += s,
        (Op::SumI(_), Acc::I64(d), Acc::I64(s)) => {
            *d = match (*d, s) {
                (None, x) | (x, None) => x,
                (Some(a), Some(b)) => Some(a.wrapping_add(b)),
            }
        }
        (Op::MinI(_), Acc::I64(d), Acc::I64(s)) => {
            *d = match (*d, s) {
                (None, x) | (x, None) => x,
                (Some(a), Some(b)) => Some(a.min(b)),
            }
        }
        (Op::MaxI(_), Acc::I64(d), Acc::I64(s)) => {
            *d = match (*d, s) {
                (None, x) | (x, None) => x,
                (Some(a), Some(b)) => Some(a.max(b)),
            }
        }
        _ => {}
    }
}

/// Aggregate one contiguous slice of batches into a partial [`I64Table`].
fn agg_i64_chunk(
    batches: &[RecordBatch],
    key_col: usize,
    ops: &[Op],
    init: &[Acc],
) -> Result<I64Table> {
    let mut table = I64Table::new(init.to_vec());
    for batch in batches {
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        let kv = i64_col(batch, key_col)?.values();
        // Pre-resolve each aggregate's typed input slice + null buffer once per batch.
        let mut ins: Vec<Option<(&[i64], Option<&crate::arrow::buffer::NullBuffer>)>> =
            Vec::with_capacity(ops.len());
        for op in ops {
            ins.push(match op {
                Op::Count { input: None } => None,
                Op::Count { input: Some(c) } | Op::SumI(c) | Op::MinI(c) | Op::MaxI(c) => {
                    let a = i64_col(batch, *c)?;
                    Some((a.values(), a.nulls()))
                }
                _ => None,
            });
        }
        let stride = table.stride;
        for i in 0..n {
            let base = table.get_or_insert(kv[i]) * stride;
            for (a, op) in ops.iter().enumerate() {
                let acc = &mut table.accs[base + a];
                match op {
                    Op::Count { input } => {
                        let inc = match input {
                            None => true,
                            Some(_) => !is_null(ins[a].unwrap().1, i),
                        };
                        if inc {
                            if let Acc::Count(x) = acc {
                                *x += 1;
                            }
                        }
                    }
                    Op::SumI(_) => {
                        let (s, nulls) = ins[a].unwrap();
                        if !is_null(nulls, i) {
                            if let Acc::I64(d) = acc {
                                *d = Some(d.unwrap_or(0).wrapping_add(s[i]));
                            }
                        }
                    }
                    Op::MinI(_) => {
                        let (s, nulls) = ins[a].unwrap();
                        if !is_null(nulls, i) {
                            if let Acc::I64(d) = acc {
                                *d = Some(d.map_or(s[i], |p| p.min(s[i])));
                            }
                        }
                    }
                    Op::MaxI(_) => {
                        let (s, nulls) = ins[a].unwrap();
                        if !is_null(nulls, i) {
                            if let Acc::I64(d) = acc {
                                *d = Some(d.map_or(s[i], |p| p.max(s[i])));
                            }
                        }
                    }
                    _ => unreachable!("fast path ops are Count/SumI/MinI/MaxI"),
                }
            }
        }
    }
    Ok(table)
}

/// Fast-path driver: aggregate `batches` (single `Int64` key) into the output batch. Shards the
/// input across cores, aggregates each shard into a partial table, then merges (the aggregates
/// are all re-combinable, so per-group merge is correct).
fn agg_i64_key(
    out_schema: &SchemaRef,
    batches: &[RecordBatch],
    key_col: usize,
    ops: &[Op],
) -> Result<RecordBatch> {
    let init: Vec<Acc> = ops.iter().map(Acc::init).collect();
    let stride = init.len();

    let p = partition_count();
    let per = ((batches.len() + p - 1) / p).max(1);
    let chunks: Vec<&[RecordBatch]> = batches.chunks(per).collect();

    let mut partials: Vec<I64Table> = chunks
        .par_iter()
        .map(|c| agg_i64_chunk(c, key_col, ops, &init))
        .collect::<Result<Vec<_>>>()?;

    let mut global = match partials.pop() {
        Some(t) => t,
        None => I64Table::new(init),
    };
    for partial in &partials {
        global.merge(partial, ops);
    }

    // Materialize: key column, then one column per aggregate.
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(stride + 1);
    cols.push(Arc::new(Int64Array::from(global.keys.clone())));
    for (a, op) in ops.iter().enumerate() {
        cols.push(build_col_flat(
            op,
            &global.accs,
            global.keys.len(),
            stride,
            a,
        ));
    }
    RecordBatch::try_new(out_schema.clone(), cols)
        .map_err(|e| Error::Execution(format!("build aggregated batch: {e}")))
}

/// Materialize aggregate `a` from flat per-group accumulators.
fn build_col_flat(op: &Op, accs: &[Acc], ngroups: usize, stride: usize, a: usize) -> ArrayRef {
    match op {
        Op::Count { .. } => {
            let v: Vec<i64> = (0..ngroups)
                .map(|g| match accs[g * stride + a] {
                    Acc::Count(n) => n,
                    _ => 0,
                })
                .collect();
            Arc::new(Int64Array::from(v))
        }
        _ => {
            let v: Vec<Option<i64>> = (0..ngroups)
                .map(|g| match accs[g * stride + a] {
                    Acc::I64(x) => x,
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(v))
        }
    }
}

/// Resolve each [`AggSpec`] against the schema, rejecting unsupported (kind, type) combinations.
fn resolve(schema: &SchemaRef, aggs: &[AggSpec]) -> Result<Vec<Op>> {
    aggs.iter()
        .map(|a| match (a.kind, a.input) {
            (AggKind::Count, input) => Ok(Op::Count { input }),
            (kind, Some(c)) => match schema.field(c).data_type() {
                DataType::Int64 => Ok(match kind {
                    AggKind::Sum => Op::SumI(c),
                    AggKind::Min => Op::MinI(c),
                    AggKind::Max => Op::MaxI(c),
                    AggKind::Count => unreachable!("count handled above"),
                }),
                DataType::Float64 => Ok(match kind {
                    AggKind::Sum => Op::SumF(c),
                    AggKind::Min => Op::MinF(c),
                    AggKind::Max => Op::MaxF(c),
                    AggKind::Count => unreachable!("count handled above"),
                }),
                other => Err(Error::Execution(format!(
                    "native group_aggregate: unsupported {kind:?} over {other:?} (col {c})"
                ))),
            },
            (kind, None) => Err(Error::Execution(format!(
                "native group_aggregate: {kind:?} needs an input column"
            ))),
        })
        .collect()
}

/// Split `batches` into `p` buckets by `fnv1a(key) % p`, keeping every column (downstream
/// aggregation reads both key and input columns by their original index).
fn partition_rows(
    batches: &[RecordBatch],
    group_cols: &[usize],
    key_fields: &[SortField],
    p: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    let mut out: Vec<Vec<RecordBatch>> = (0..p).map(|_| Vec::new()).collect();
    if p == 1 {
        out[0] = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        return Ok(out);
    }
    let converter = RowConverter::new(key_fields.to_vec())
        .map_err(|e| Error::Execution(format!("row converter: {e}")))?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let key_arrays: Vec<ArrayRef> = group_cols
            .iter()
            .map(|&c| batch.column(c).clone())
            .collect();
        let rows = converter
            .convert_columns(&key_arrays)
            .map_err(|e| Error::Execution(format!("convert columns: {e}")))?;
        let mut idx: Vec<Vec<u32>> = (0..p).map(|_| Vec::new()).collect();
        for (i, row) in rows.iter().enumerate() {
            let bucket = (fnv1a(row.as_ref()) % p as u64) as usize;
            idx[bucket].push(i as u32);
        }
        for (bucket, indices) in idx.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let take_idx = UInt32Array::from(indices);
            let cols = batch
                .columns()
                .iter()
                .map(|col| take(col, &take_idx, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::Execution(format!("take: {e}")))?;
            let part = RecordBatch::try_new(batch.schema(), cols)
                .map_err(|e| Error::Execution(format!("build partition batch: {e}")))?;
            out[bucket].push(part);
        }
    }
    Ok(out)
}

/// Aggregate one partition's batches into a single record batch, or `None` if it is empty.
fn aggregate_partition(
    out_schema: &SchemaRef,
    key_fields: &[SortField],
    group_cols: &[usize],
    ops: &[Op],
    batches: &[RecordBatch],
) -> Result<Option<RecordBatch>> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(None);
    }
    let converter = RowConverter::new(key_fields.to_vec())
        .map_err(|e| Error::Execution(format!("row converter: {e}")))?;

    let mut map: HashMap<OwnedRow, usize> = HashMap::new();
    let mut keys: Vec<OwnedRow> = Vec::new();
    let mut accs: Vec<Vec<Acc>> = Vec::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let key_arrays: Vec<ArrayRef> = group_cols
            .iter()
            .map(|&c| batch.column(c).clone())
            .collect();
        let rows = converter
            .convert_columns(&key_arrays)
            .map_err(|e| Error::Execution(format!("convert columns: {e}")))?;

        for i in 0..batch.num_rows() {
            let owned = rows.row(i).owned();
            let gid = match map.get(&owned) {
                Some(&g) => g,
                None => {
                    let g = keys.len();
                    keys.push(owned.clone());
                    accs.push(ops.iter().map(Acc::init).collect());
                    map.insert(owned, g);
                    g
                }
            };
            for (a, op) in ops.iter().enumerate() {
                update(&mut accs[gid][a], op, batch, i)?;
            }
        }
    }

    // Reconstruct the group-key columns, then append one array per aggregate.
    let mut cols: Vec<ArrayRef> = converter
        .convert_rows(keys.iter().map(|r| r.row()))
        .map_err(|e| Error::Execution(format!("convert rows back: {e}")))?;
    for (a, op) in ops.iter().enumerate() {
        cols.push(build_col(op, &accs, a));
    }
    let batch = RecordBatch::try_new(out_schema.clone(), cols)
        .map_err(|e| Error::Execution(format!("build aggregated batch: {e}")))?;
    Ok(Some(batch))
}

fn i64_col(batch: &RecordBatch, c: usize) -> Result<&Int64Array> {
    batch
        .column(c)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Execution(format!("column {c} is not Int64")))
}

fn f64_col(batch: &RecordBatch, c: usize) -> Result<&Float64Array> {
    batch
        .column(c)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::Execution(format!("column {c} is not Float64")))
}

/// Fold row `i` of `batch` into `acc` under `op`. `acc`'s variant always matches `op` (both
/// derive from the same resolved spec), so the inner matches are total.
fn update(acc: &mut Acc, op: &Op, batch: &RecordBatch, i: usize) -> Result<()> {
    match op {
        Op::Count { input } => {
            let inc = match input {
                None => true,
                Some(c) => !batch.column(*c).is_null(i),
            };
            if inc {
                if let Acc::Count(n) = acc {
                    *n += 1;
                }
            }
        }
        Op::SumI(c) => {
            let arr = i64_col(batch, *c)?;
            if !arr.is_null(i) {
                let v = arr.value(i);
                if let Acc::I64(s) = acc {
                    *s = Some(s.unwrap_or(0).wrapping_add(v));
                }
            }
        }
        Op::MinI(c) => {
            let arr = i64_col(batch, *c)?;
            if !arr.is_null(i) {
                let v = arr.value(i);
                if let Acc::I64(s) = acc {
                    *s = Some(s.map_or(v, |p| p.min(v)));
                }
            }
        }
        Op::MaxI(c) => {
            let arr = i64_col(batch, *c)?;
            if !arr.is_null(i) {
                let v = arr.value(i);
                if let Acc::I64(s) = acc {
                    *s = Some(s.map_or(v, |p| p.max(v)));
                }
            }
        }
        Op::SumF(c) => {
            let arr = f64_col(batch, *c)?;
            if !arr.is_null(i) {
                let v = arr.value(i);
                if let Acc::F64(s) = acc {
                    *s = Some(s.unwrap_or(0.0) + v);
                }
            }
        }
        Op::MinF(c) => {
            let arr = f64_col(batch, *c)?;
            if !arr.is_null(i) {
                let v = arr.value(i);
                if let Acc::F64(s) = acc {
                    *s = Some(s.map_or(v, |p| p.min(v)));
                }
            }
        }
        Op::MaxF(c) => {
            let arr = f64_col(batch, *c)?;
            if !arr.is_null(i) {
                let v = arr.value(i);
                if let Acc::F64(s) = acc {
                    *s = Some(s.map_or(v, |p| p.max(v)));
                }
            }
        }
    }
    Ok(())
}

/// Materialize aggregate `a` across all groups into an Arrow array.
fn build_col(op: &Op, accs: &[Vec<Acc>], a: usize) -> ArrayRef {
    match op {
        Op::Count { .. } => {
            let v: Vec<i64> = accs
                .iter()
                .map(|g| match g[a] {
                    Acc::Count(n) => n,
                    _ => 0,
                })
                .collect();
            Arc::new(Int64Array::from(v))
        }
        Op::SumI(_) | Op::MinI(_) | Op::MaxI(_) => {
            let v: Vec<Option<i64>> = accs
                .iter()
                .map(|g| match g[a] {
                    Acc::I64(x) => x,
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(v))
        }
        Op::SumF(_) | Op::MinF(_) | Op::MaxF(_) => {
            let v: Vec<Option<f64>> = accs
                .iter()
                .map(|g| match g[a] {
                    Acc::F64(x) => x,
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(v))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::StringArray;
    use crate::arrow::datatypes::Field;
    use std::collections::BTreeMap;

    fn schema_kv() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    #[test]
    fn single_int_key_sum_count() {
        let schema = schema_kv();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 5, 25, 7])),
            ],
        )
        .unwrap();
        let out = group_aggregate(
            &schema,
            &[batch],
            &[0],
            &[
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some(1),
                    name: "s".into(),
                },
                AggSpec {
                    kind: AggKind::Count,
                    input: None,
                    name: "c".into(),
                },
            ],
        )
        .unwrap();

        let mut got = BTreeMap::new();
        let k = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..out.num_rows() {
            got.insert(k.value(i), (s.value(i), c.value(i)));
        }
        let want = BTreeMap::from([(1, (15, 2)), (2, (45, 2)), (3, (7, 1))]);
        assert_eq!(got, want);
    }

    #[test]
    fn null_inputs_are_skipped_and_all_null_group_is_null_sum() {
        let schema = schema_kv();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![Some(4), None, None])),
            ],
        )
        .unwrap();
        let out = group_aggregate(
            &schema,
            &[batch],
            &[0],
            &[
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some(1),
                    name: "s".into(),
                },
                AggSpec {
                    kind: AggKind::Count,
                    input: Some(1),
                    name: "cv".into(),
                },
            ],
        )
        .unwrap();

        let k = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let cv = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..out.num_rows() {
            match k.value(i) {
                1 => {
                    assert_eq!(s.value(i), 4); // null skipped
                    assert_eq!(cv.value(i), 1); // COUNT(v) ignores null
                }
                2 => {
                    assert!(s.is_null(i)); // all-null group → NULL sum
                    assert_eq!(cv.value(i), 0);
                }
                other => panic!("unexpected key {other}"),
            }
        }
    }

    #[test]
    fn string_key_grouping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        let out = group_aggregate(
            &schema,
            &[batch],
            &[0],
            &[AggSpec {
                kind: AggKind::Sum,
                input: Some(1),
                name: "s".into(),
            }],
        )
        .unwrap();
        let k = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let s = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut got = BTreeMap::new();
        for i in 0..out.num_rows() {
            got.insert(k.value(i).to_string(), s.value(i));
        }
        assert_eq!(got, BTreeMap::from([("a".into(), 9), ("b".into(), 6)]));
    }

    #[test]
    fn empty_input_yields_empty_with_schema() {
        let schema = schema_kv();
        let out = group_aggregate(
            &schema,
            &[],
            &[0],
            &[AggSpec {
                kind: AggKind::Count,
                input: None,
                name: "c".into(),
            }],
        )
        .unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(1).name(), "c");
    }

    #[test]
    fn unsupported_type_is_rejected_for_fallback() {
        // SUM over a string column is outside the subset → caller falls back to DataFusion.
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let err = group_aggregate(
            &schema,
            &[],
            &[0],
            &[AggSpec {
                kind: AggKind::Sum,
                input: Some(1),
                name: "s".into(),
            }],
        );
        assert!(err.is_err());
    }

    /// The oracle test: the native kernel must agree with DataFusion row-for-row on a
    /// many-group, multi-batch input (the shape it exists to accelerate).
    #[tokio::test]
    async fn matches_datafusion_on_many_groups() {
        use crate::Engine;

        let schema = schema_kv();
        // 200 distinct keys, two batches, a null sprinkled in.
        let mk = |off: i64| {
            let ks: Vec<i64> = (0..1000).map(|i| (i % 200) as i64).collect();
            let vs: Vec<Option<i64>> = (0..1000)
                .map(|i| {
                    if (i + off) % 37 == 0 {
                        None
                    } else {
                        Some(i * 3 + off)
                    }
                })
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap()
        };
        let batches = vec![mk(0), mk(1)];

        let native = group_aggregate(
            &schema,
            &batches,
            &[0],
            &[
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some(1),
                    name: "s".into(),
                },
                AggSpec {
                    kind: AggKind::Count,
                    input: None,
                    name: "c".into(),
                },
                AggSpec {
                    kind: AggKind::Min,
                    input: Some(1),
                    name: "mn".into(),
                },
                AggSpec {
                    kind: AggKind::Max,
                    input: Some(1),
                    name: "mx".into(),
                },
            ],
        )
        .unwrap();

        let engine = Engine::new();
        engine.register_batches("t", batches).unwrap();
        let df = engine
            .sql("SELECT k, SUM(v) s, COUNT(*) c, MIN(v) mn, MAX(v) mx FROM t GROUP BY k")
            .await
            .unwrap();

        // Reduce both to key -> (sum, count, min, max), comparing as maps (order-insensitive).
        type Row = (Option<i64>, i64, Option<i64>, Option<i64>);
        fn to_map(batches: &[RecordBatch]) -> BTreeMap<i64, Row> {
            let mut m = BTreeMap::new();
            for b in batches {
                let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let c = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                let mn = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
                let mx = b.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
                let opt = |a: &Int64Array, i: usize| (!a.is_null(i)).then(|| a.value(i));
                for i in 0..b.num_rows() {
                    m.insert(k.value(i), (opt(s, i), c.value(i), opt(mn, i), opt(mx, i)));
                }
            }
            m
        }

        assert_eq!(to_map(&[native]), to_map(&df));
    }
}
