//! Worker-side stage plan cache (R5-4 / KAN-2): plan a distributed stage **once per worker**,
//! not once per task.
//!
//! A Flight worker runs one task per partition per stage, and every task used to pay the full
//! front-end for the SAME stage SQL: parse → Spark rewrites → name resolution → logical plan
//! (then optimize + physical plan) — 16 partitions meant 16 identical plan passes per stage.
//! This module memoizes the per-task-invariant part of that work behind a process-global,
//! entry-capped LRU keyed by everything that determines a stage's plan.
//!
//! # What is cached (and what is not)
//!
//! The cached value is the **unoptimized, Spark-rewritten logical plan** — the output of
//! `Engine::plan_spark` before any DataFusion optimizer pass. Per task the worker then rebinds
//! this template's `shuffle_input*` scans and runs `optimize` + physical planning exactly as
//! before. The split point is deliberate:
//!
//! - **Physical plans cannot be shared**: their scan execs embed the task's MemTable batches.
//! - **The optimizer cannot be cached**: it folds `now()`/`current_timestamp()` against the
//!   per-query start time and applies per-session config — both must stay per task. Caching
//!   *before* the optimizer keeps every volatile/freshness semantic byte-identical to today.
//! - **The KAN-25/KAN-53 join guards consume `df.logical_plan()`** (this exact boundary) and
//!   re-plan physically from it, so every downstream branch of `Engine::sql_stream` runs
//!   unchanged on a cache hit.
//!
//! # Per-task measured statistics (KAN-2 A3)
//!
//! Measured row totals are deliberately **not** part of the cache key. DataFusion logical
//! plans carry no statistics, so one template serves every task of a stage: on a hit the
//! rebind swaps each `shuffle_input*` `TableScan`'s source for THIS task's provider — the
//! [`crate::measured_scan::MeasuredStatsTable`] carrying this task's measured bucket rows —
//! and the per-task optimize + physical planning (join selection) sizes hash builds from
//! that task's own statistics, exactly as an uncached plan would. Including the totals in
//! the key would instead defeat the cache entirely (every partition pulls different buckets).
//!
//! # Cache key and correctness contract
//!
//! [`StagePlanKey`] covers every input that determines the cached plan:
//!
//! - `engine_id` — two engines in one process (in-process test workers) never share entries;
//!   a template embeds its engine's base-table providers.
//! - `catalog_version` — bumped by `Engine` on any non-shuffle catalog mutation
//!   (register/deregister/DDL/UDF sync), so a re-registered base table misses. Registrations
//!   of the per-task localized `shuffle_input__s*_p*` names do NOT bump (they would invalidate
//!   on every task); shuffle inputs are covered by the schema fingerprints below instead.
//! - `stage_id` + canonical (pre-localization) stage SQL — two stages of a multi-shuffle DAG
//!   can share identical SQL over different upstreams; the template's scan names carry the id.
//! - `lakehouse_snapshot_pins` — KAN-48: a re-pinned snapshot is a different key (a miss).
//! - `replicated_tables` — the task-local replicate/shard classification (normalized csv)
//!   changes provider resolution at plan time.
//! - `input_schemas` — Debug fingerprints of the task's shuffle-input schemas, upstream order.
//!
//! A hit NEVER re-resolves base tables: the template's non-shuffle scans keep the builder's
//! providers, which remain valid because every input that could change them is in the key
//! (and listing-based providers re-list at physical-plan time anyway).
//!
//! # Rebind
//!
//! The template keeps the BUILDING task's localized `shuffle_input__s{S}_p{P}(_{i})` table
//! names. A hit does not rename anything — post-analysis the name in a `TableScan` is a label,
//! not a lookup — it only swaps the scan's `source` for the hitting task's provider (upstream
//! index parsed from the name; the partition component is irrelevant). Expression subqueries
//! (`IN (SELECT …)`, `EXISTS`, scalar) embed their own plans outside DataFusion's `TreeNode`
//! traversal, so [`rebind_shuffle_inputs`] recurses into them explicitly.
//!
//! # Single-flight, bounds, observability
//!
//! Concurrent same-stage tasks coalesce on one build (a `watch` slot placeholder): the
//! first task plans, the rest wait and then hit — N tasks cost one plan pass. A failed
//! build caches nothing; waiters retry (and fail) individually, matching uncached behavior.
//! Ready entries are capped by `WEFT_STAGE_PLAN_CACHE_ENTRIES` (default
//! [`DEFAULT_STAGE_PLAN_CACHE_ENTRIES`]; `0` disables caching — re-read on every cache
//! operation, so the toggle applies dynamically and never freezes at first contact) with
//! LRU eviction. Hits/misses/builds/evictions are counted on the global
//! cache ([`StagePlanCache::stats`]).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{Expr, LogicalPlan, Subquery, TableScan};

/// Default cache budget: 256 ready plan templates per worker process (a template is a few
/// KB of plan tree; stages × concurrent queries stays far below this in practice).
const DEFAULT_STAGE_PLAN_CACHE_ENTRIES: usize = 256;

/// Cache key: everything that determines a stage's (unoptimized, Spark-rewritten) logical
/// plan. See the module docs for the per-component contract.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StagePlanKey {
    engine_id: u64,
    catalog_version: u64,
    stage_id: u32,
    sql: String,
    pins: String,
    replicated: String,
    input_schemas: Vec<String>,
}

/// One stage task's plan-cache request: the key plus what a cache hit needs to rebind the
/// template to THIS task — the stage id and this task's registered shuffle-input providers
/// (upstream order), each carrying the task's measured row totals when the ticket shipped
/// them (KAN-2 A3). Built by `Engine::stage_plan_request` on the worker.
pub struct StagePlanRequest {
    key: StagePlanKey,
    stage_id: u32,
    shuffle_inputs: Vec<Arc<dyn TableProvider>>,
}

impl StagePlanRequest {
    pub fn new(
        engine_id: u64,
        catalog_version: u64,
        stage_id: u32,
        canonical_sql: &str,
        pins_json: &str,
        replicated_csv: &str,
        shuffle_inputs: Vec<Arc<dyn TableProvider>>,
    ) -> Self {
        // Normalize the replicated classification so semantically equal sets key equal even
        // if the driver's csv order ever drifts.
        let mut replicated = crate::shard::parse_replicated_tables_csv(replicated_csv);
        replicated.sort_unstable();
        Self {
            key: StagePlanKey {
                engine_id,
                catalog_version,
                stage_id,
                sql: canonical_sql.to_string(),
                pins: pins_json.to_string(),
                replicated: replicated.join(","),
                input_schemas: shuffle_inputs
                    .iter()
                    .map(|p| format!("{:?}", p.schema()))
                    .collect(),
            },
            stage_id,
            shuffle_inputs,
        }
    }

    pub fn key(&self) -> &StagePlanKey {
        &self.key
    }

    pub fn stage_id(&self) -> u32 {
        self.stage_id
    }

    pub fn shuffle_inputs(&self) -> &[Arc<dyn TableProvider>] {
        &self.shuffle_inputs
    }
}

/// Counters + current footprint of a [`StagePlanCache`], for observability and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StagePlanCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub builds: u64,
    pub evictions: u64,
    pub entries: u64,
}

// One lookup per stage task, so the enum's size is not on a hot path — keep the plan
// inline rather than boxing it through every cache operation (same trade-off as
// `shuffle::protocol::Ticket`).
#[allow(clippy::large_enum_variant)]
enum CacheEntry {
    Ready(LogicalPlan),
    /// A build is in flight on another task; waiters `wait_for` the slot's outer `Some`
    /// and then re-lookup (`Some(plan)` → hit, `None` → the build failed and the
    /// placeholder is gone, retry). A watch slot (not an event) so a waiter that arrives
    /// after completion still sees the outcome.
    Building(tokio::sync::watch::Receiver<BuildSlot>),
}

/// In-flight build slot: starts `None` (pending); the builder completes it with
/// `Some(Some(plan))` on success or `Some(None)` on failure.
type BuildSlot = Option<Option<LogicalPlan>>;

/// The outcome of one [`StagePlanCache::lookup`].
// Same size trade-off as [`CacheEntry`] — one per stage task, not per row.
#[allow(clippy::large_enum_variant)]
pub enum PlanLookup {
    /// A ready template: rebind it to this task's providers and plan physically.
    Hit(LogicalPlan),
    /// No entry: this task owns the build and MUST call [`StagePlanCache::complete_build`]
    /// with the result (success or failure) so waiters unblock.
    Build(BuildTicket),
    /// Another task is building this key right now: wait on the slot, then re-lookup.
    Wait(tokio::sync::watch::Receiver<BuildSlot>),
}

/// Ownership token for a single in-flight build, returned by [`PlanLookup::Build`].
pub struct BuildTicket {
    key: StagePlanKey,
    done: tokio::sync::watch::Sender<BuildSlot>,
}

struct StagePlanCacheInner {
    entries: HashMap<StagePlanKey, CacheEntry>,
    /// Front = least recently used. Ready entries only (a Building placeholder is transient
    /// and never evicted). Touched on every hit and completed build.
    lru: VecDeque<StagePlanKey>,
    hits: u64,
    misses: u64,
    builds: u64,
    evictions: u64,
}

/// An entry-capped LRU of unoptimized stage logical plans. Thread-safe: one `Mutex` guards
/// the map, LRU order, and counters — cache operations are cheap pointer moves, so the lock
/// is never held across planning (a build happens *between* [`StagePlanCache::lookup`] and
/// [`StagePlanCache::complete_build`], outside the lock).
pub struct StagePlanCache {
    inner: Mutex<StagePlanCacheInner>,
    cap_entries: usize,
}

impl StagePlanCache {
    fn with_cap(cap_entries: usize) -> Self {
        Self {
            inner: Mutex::new(StagePlanCacheInner {
                entries: HashMap::new(),
                lru: VecDeque::new(),
                hits: 0,
                misses: 0,
                builds: 0,
                evictions: 0,
            }),
            cap_entries,
        }
    }

    /// Whether this cache stores anything. The budget is re-checked dynamically on every
    /// operation ([`StagePlanCache::current_cap`]) so `WEFT_STAGE_PLAN_CACHE_ENTRIES=0`
    /// disables caching — and re-enables on removal — even after the process-global cache
    /// is initialized; a test can toggle it without poisoning the process.
    pub fn enabled(&self) -> bool {
        self.current_cap() > 0
    }

    /// The current entry budget: the `WEFT_STAGE_PLAN_CACHE_ENTRIES` override when set
    /// (`0` disables; unparseable falls back to the default), else the cap the cache was
    /// constructed with (for the process-global cache, the default). Reading it per
    /// operation — instead of freezing the env-derived value at first use — keeps the
    /// disable toggle order-independent: a first contact made while the var is `0` must
    /// not stick the cache in the disabled state after the var is removed.
    fn current_cap(&self) -> usize {
        match std::env::var("WEFT_STAGE_PLAN_CACHE_ENTRIES") {
            Ok(raw) => parse_cap_entries(Some(raw.as_str())),
            Err(_) => self.cap_entries,
        }
    }

    /// Look up `key`. `None` means caching is disabled (plan fresh, touch nothing else).
    /// Otherwise see [`PlanLookup`]; a hit refreshes the entry's LRU position.
    pub fn lookup(&self, key: &StagePlanKey) -> Option<PlanLookup> {
        if !self.enabled() {
            return None;
        }
        let mut inner = self.inner.lock().expect("stage plan cache poisoned");
        match inner.entries.get(key) {
            Some(CacheEntry::Ready(plan)) => {
                let hit = plan.clone();
                if let Some(pos) = inner.lru.iter().position(|k| k == key) {
                    inner.lru.remove(pos);
                }
                inner.lru.push_back(key.clone());
                inner.hits += 1;
                Some(PlanLookup::Hit(hit))
            }
            Some(CacheEntry::Building(rx)) => Some(PlanLookup::Wait(rx.clone())),
            None => {
                let (done, rx) = tokio::sync::watch::channel(None);
                inner.entries.insert(key.clone(), CacheEntry::Building(rx));
                inner.misses += 1;
                Some(PlanLookup::Build(BuildTicket {
                    key: key.clone(),
                    done,
                }))
            }
        }
    }

    /// Complete a build started by [`PlanLookup::Build`]. `Some(plan)` publishes the template
    /// (counting a build, evicting least-recently-used entries past the cap); `None` (a failed
    /// build) caches nothing. Either way waiters on the placeholder are released.
    pub fn complete_build(&self, ticket: BuildTicket, plan: Option<LogicalPlan>) {
        let BuildTicket { key, done } = ticket;
        // Never cache a template holding live shuffle-input batches — strip their sources to
        // schema-only empty MemTables first (the hit path rebinds real providers anyway). A
        // strip failure simply forgoes the cache.
        let plan = plan.and_then(|p| strip_shuffle_input_sources(&p).ok());
        {
            let mut inner = self.inner.lock().expect("stage plan cache poisoned");
            match &plan {
                Some(template) => {
                    if self.enabled() {
                        let cap = self.current_cap();
                        while inner.lru.len() >= cap {
                            let Some(victim) = inner.lru.pop_front() else {
                                break;
                            };
                            if inner.entries.remove(&victim).is_some() {
                                inner.evictions += 1;
                            }
                        }
                        inner
                            .entries
                            .insert(key.clone(), CacheEntry::Ready(template.clone()));
                        inner.lru.push_back(key.clone());
                        inner.builds += 1;
                    } else {
                        inner.entries.remove(&key);
                    }
                }
                None => {
                    inner.entries.remove(&key);
                }
            }
        }
        // Release waiters LAST (after the map update), so a waiter's re-lookup sees the
        // published entry / the removed placeholder, never a stale Building slot.
        done.send_replace(Some(plan));
    }

    /// Snapshot of counters and current footprint.
    pub fn stats(&self) -> StagePlanCacheStats {
        let inner = self.inner.lock().expect("stage plan cache poisoned");
        StagePlanCacheStats {
            hits: inner.hits,
            misses: inner.misses,
            builds: inner.builds,
            evictions: inner.evictions,
            entries: inner.lru.len() as u64,
        }
    }
}

/// The process-global cache. It is constructed with the default cap
/// ([`DEFAULT_STAGE_PLAN_CACHE_ENTRIES`]) regardless of the env at first contact:
/// `WEFT_STAGE_PLAN_CACHE_ENTRIES` is re-read on every cache operation
/// ([`StagePlanCache::current_cap`]), so the `0` disable — and any later budget change —
/// applies dynamically and can never freeze the cache disabled for the process's lifetime
/// (a first contact under `0` used to poison every later lookup). One worker process backs
/// one cluster node, and stage tasks within it come and go — process scope is exactly the
/// sharing boundary (mirrors [`crate::dim_cache::global`]).
pub fn global() -> &'static StagePlanCache {
    STAGE_PLAN_CACHE.get_or_init(|| StagePlanCache::with_cap(DEFAULT_STAGE_PLAN_CACHE_ENTRIES))
}

static STAGE_PLAN_CACHE: OnceLock<StagePlanCache> = OnceLock::new();

fn parse_cap_entries(raw: Option<&str>) -> usize {
    match raw {
        Some(s) => s.trim().parse().unwrap_or(DEFAULT_STAGE_PLAN_CACHE_ENTRIES),
        None => DEFAULT_STAGE_PLAN_CACHE_ENTRIES,
    }
}

/// Whether `name` is a task-localized shuffle-input table (`shuffle_input__s{S}_p{P}` or
/// `shuffle_input__s{S}_p{P}_{i}`) — the names `weft_execution::shuffle::localized_
/// shuffle_input_name` generates on the worker. `Engine` uses this to exempt per-task
/// registrations from its catalog-version bump; keep the shape in sync with that generator.
pub fn is_localized_shuffle_input_name(name: &str) -> bool {
    parse_localized_shuffle_input_name(name).is_some()
}

/// Parse `shuffle_input__s{S}_p{P}(_{i})?` into `(stage_id, partition_id, upstream_idx)`.
/// `None` for anything else — including the planner's un-localized tokens (`shuffle_input`,
/// `shuffle_input_{i}`), which never appear in a template (a stage with upstreams is always
/// localized before planning; a token the localization deliberately left dangling fails at
/// plan time before it can be cached).
fn parse_localized_shuffle_input_name(name: &str) -> Option<(u32, u32, Option<usize>)> {
    let rest = name.strip_prefix("shuffle_input__s")?;
    let (stage, rest) = rest.split_once("_p")?;
    let stage_id: u32 = stage.parse().ok()?;
    // After `_p`: `{partition}` or `{partition}_{idx}`; both numeric, so a stray second `_`
    // segment that isn't a plain number rejects the whole name.
    let (partition, idx) = match rest.split_once('_') {
        Some((p, i)) => (p, Some(i)),
        None => (rest, None),
    };
    let partition_id: u32 = partition.parse().ok()?;
    let upstream_idx = match idx {
        Some(i) => Some(i.parse::<usize>().ok()?),
        None => None,
    };
    Some((stage_id, partition_id, upstream_idx))
}

/// Rebind a cached stage plan template to one task's shuffle inputs: every `TableScan` on a
/// localized `shuffle_input__s{stage_id}_p*(_{i})` name gets THIS task's provider for upstream
/// `i` as its source (the name — and every column qualifier referencing it — is left as the
/// builder wrote it: post-analysis the name is a plan-internal label, and nothing re-resolves
/// it). All other scans (base/replicated tables) keep the builder's providers, valid under
/// the cache key's contract. Expression subqueries embed their own plans outside DataFusion's
/// `TreeNode` traversal, so they are rebound recursively.
pub fn rebind_shuffle_inputs(
    plan: &LogicalPlan,
    stage_id: u32,
    providers: &[Arc<dyn TableProvider>],
) -> DfResult<LogicalPlan> {
    if let LogicalPlan::TableScan(scan) = plan {
        if let Some((scan_stage, _partition, upstream_idx)) =
            parse_localized_shuffle_input_name(scan.table_name.table())
        {
            if scan_stage == stage_id {
                let idx = upstream_idx.unwrap_or(0);
                let Some(provider) = providers.get(idx) else {
                    return Err(DataFusionError::Plan(format!(
                        "stage plan template references shuffle input {idx} of stage \
                         {scan_stage}, but this task registered only {} upstream(s)",
                        providers.len()
                    )));
                };
                let filters = scan
                    .filters
                    .iter()
                    .cloned()
                    .map(|f| rebind_subquery_exprs(f, stage_id, providers))
                    .collect::<DfResult<Vec<_>>>()?;
                return Ok(LogicalPlan::TableScan(TableScan::try_new(
                    scan.table_name.clone(),
                    provider_as_source(Arc::clone(provider)),
                    scan.projection.clone(),
                    filters,
                    scan.fetch,
                )?));
            }
        }
    }
    let exprs = plan
        .expressions()
        .into_iter()
        .map(|e| rebind_subquery_exprs(e, stage_id, providers))
        .collect::<DfResult<Vec<_>>>()?;
    let inputs = plan
        .inputs()
        .into_iter()
        .map(|c| rebind_shuffle_inputs(c, stage_id, providers))
        .collect::<DfResult<Vec<_>>>()?;
    plan.with_new_exprs(exprs, inputs)
}

/// Rebind shuffle-input scans inside `expr`'s embedded subquery plans (the rest of the
/// expression tree is untouched).
fn rebind_subquery_exprs(
    expr: Expr,
    stage_id: u32,
    providers: &[Arc<dyn TableProvider>],
) -> DfResult<Expr> {
    struct SubqueryRebinder<'a> {
        stage_id: u32,
        providers: &'a [Arc<dyn TableProvider>],
    }
    impl SubqueryRebinder<'_> {
        fn rebind(&self, subquery: Subquery) -> DfResult<Subquery> {
            let Subquery {
                subquery,
                outer_ref_columns,
                spans,
            } = subquery;
            Ok(Subquery {
                subquery: Arc::new(rebind_shuffle_inputs(
                    &subquery,
                    self.stage_id,
                    self.providers,
                )?),
                outer_ref_columns,
                spans,
            })
        }
    }
    impl TreeNodeRewriter for SubqueryRebinder<'_> {
        type Node = Expr;
        fn f_up(&mut self, expr: Expr) -> DfResult<Transformed<Expr>> {
            Ok(match expr {
                Expr::ScalarSubquery(subquery) => {
                    Transformed::yes(Expr::ScalarSubquery(self.rebind(subquery)?))
                }
                Expr::Exists(Exists { subquery, negated }) => {
                    Transformed::yes(Expr::Exists(Exists::new(self.rebind(subquery)?, negated)))
                }
                Expr::InSubquery(InSubquery {
                    expr,
                    subquery,
                    negated,
                }) => Transformed::yes(Expr::InSubquery(InSubquery::new(
                    expr,
                    self.rebind(subquery)?,
                    negated,
                ))),
                other => Transformed::no(other),
            })
        }
    }
    Ok(expr
        .rewrite(&mut SubqueryRebinder {
            stage_id,
            providers,
        })?
        .data)
}

/// Strip a freshly-built template's localized shuffle-input scan sources before caching:
/// every `shuffle_input__s*_p*(_{i})` `TableScan` gets a schema-only EMPTY `MemTable` as its
/// source. The hit path ([`rebind_shuffle_inputs`]) always swaps in the hitting task's
/// provider for these scans, so the builder's provider is never read from a cached template —
/// but it *is* retained by the cache, and that provider (the building task's `MemTable` /
/// `MeasuredStatsTable`) pins the task's entire pulled shuffle input. A 256-entry cache of
/// such templates holds GiB-scale Arrow buffers outside the DataFusion memory pool (KAN-2:
/// the ~11-12 GiB worker RSS plateau that degraded SF10 queries 2-10x until a worker
/// restart). Stripping restores the "few KB per template" the cache budget assumes. `Err`
/// aborts the cache insert (the stage is simply not cached — it is never cached holding live
/// input data).
pub fn strip_shuffle_input_sources(plan: &LogicalPlan) -> DfResult<LogicalPlan> {
    if let LogicalPlan::TableScan(scan) = plan {
        if is_localized_shuffle_input_name(scan.table_name.table()) {
            let empty = MemTable::try_new(scan.source.schema(), vec![vec![]])?;
            let filters = scan
                .filters
                .iter()
                .cloned()
                .map(strip_subquery_exprs)
                .collect::<DfResult<Vec<_>>>()?;
            return Ok(LogicalPlan::TableScan(TableScan::try_new(
                scan.table_name.clone(),
                provider_as_source(Arc::new(empty)),
                scan.projection.clone(),
                filters,
                scan.fetch,
            )?));
        }
    }
    let exprs = plan
        .expressions()
        .into_iter()
        .map(strip_subquery_exprs)
        .collect::<DfResult<Vec<_>>>()?;
    let inputs = plan
        .inputs()
        .into_iter()
        .map(strip_shuffle_input_sources)
        .collect::<DfResult<Vec<_>>>()?;
    plan.with_new_exprs(exprs, inputs)
}

/// Strip shuffle-input scans inside `expr`'s embedded subquery plans (the rest of the
/// expression tree is untouched) — the [`strip_shuffle_input_sources`] analog of
/// [`rebind_subquery_exprs`].
fn strip_subquery_exprs(expr: Expr) -> DfResult<Expr> {
    struct SubqueryStripper;
    impl SubqueryStripper {
        fn strip(&self, subquery: Subquery) -> DfResult<Subquery> {
            let Subquery {
                subquery,
                outer_ref_columns,
                spans,
            } = subquery;
            Ok(Subquery {
                subquery: Arc::new(strip_shuffle_input_sources(&subquery)?),
                outer_ref_columns,
                spans,
            })
        }
    }
    impl TreeNodeRewriter for SubqueryStripper {
        type Node = Expr;
        fn f_up(&mut self, expr: Expr) -> DfResult<Transformed<Expr>> {
            Ok(match expr {
                Expr::ScalarSubquery(subquery) => {
                    Transformed::yes(Expr::ScalarSubquery(self.strip(subquery)?))
                }
                Expr::Exists(Exists { subquery, negated }) => {
                    Transformed::yes(Expr::Exists(Exists::new(self.strip(subquery)?, negated)))
                }
                Expr::InSubquery(InSubquery {
                    expr,
                    subquery,
                    negated,
                }) => Transformed::yes(Expr::InSubquery(InSubquery::new(
                    expr,
                    self.strip(subquery)?,
                    negated,
                ))),
                other => Transformed::no(other),
            })
        }
    }
    expr.rewrite(&mut SubqueryStripper).map(|t| t.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{source_as_provider, MemTable};
    use datafusion::prelude::SessionContext;

    fn kv_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn kv_table(rows: &[(i64, i64)]) -> Arc<dyn TableProvider> {
        let batch = RecordBatch::try_new(
            kv_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(kv_schema(), vec![vec![batch]]).unwrap())
    }

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(datafusion::logical_expr::logical_plan::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        })
    }

    fn request(
        engine_id: u64,
        catalog_version: u64,
        stage_id: u32,
        sql: &str,
        pins: &str,
        replicated: &str,
        inputs: Vec<Arc<dyn TableProvider>>,
    ) -> StagePlanRequest {
        StagePlanRequest::new(
            engine_id,
            catalog_version,
            stage_id,
            sql,
            pins,
            replicated,
            inputs,
        )
    }

    #[test]
    fn key_tracks_every_component() {
        let base = request(1, 1, 2, "SELECT 1", "", "a,b", vec![kv_table(&[(1, 2)])]);
        // Identical request ⇒ identical key.
        assert_eq!(
            request(1, 1, 2, "SELECT 1", "", "a,b", vec![kv_table(&[(1, 2)])]).key(),
            base.key()
        );
        // The replicated classification normalizes csv order.
        assert_eq!(
            request(1, 1, 2, "SELECT 1", "", "b,a", vec![kv_table(&[(1, 2)])]).key(),
            base.key()
        );
        for (name, other) in [
            (
                "engine id",
                request(9, 1, 2, "SELECT 1", "", "a,b", vec![kv_table(&[(1, 2)])]),
            ),
            (
                "catalog version",
                request(1, 9, 2, "SELECT 1", "", "a,b", vec![kv_table(&[(1, 2)])]),
            ),
            (
                "stage id",
                request(1, 1, 9, "SELECT 1", "", "a,b", vec![kv_table(&[(1, 2)])]),
            ),
            (
                "sql",
                request(1, 1, 2, "SELECT 2", "", "a,b", vec![kv_table(&[(1, 2)])]),
            ),
            (
                "snapshot pins",
                request(
                    1,
                    1,
                    2,
                    "SELECT 1",
                    r#"{"t":{"format":"delta","version":8}}"#,
                    "a,b",
                    vec![kv_table(&[(1, 2)])],
                ),
            ),
            (
                "replicated set",
                request(1, 1, 2, "SELECT 1", "", "a,c", vec![kv_table(&[(1, 2)])]),
            ),
            (
                "input count",
                request(
                    1,
                    1,
                    2,
                    "SELECT 1",
                    "",
                    "a,b",
                    vec![kv_table(&[(1, 2)]), kv_table(&[(3, 4)])],
                ),
            ),
        ] {
            assert_ne!(other.key(), base.key(), "{name} must change the key");
        }
        // Input schema (types), not row content, is the fingerprint.
        let wide_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let wide: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                wide_schema.clone(),
                vec![vec![RecordBatch::try_new(
                    wide_schema,
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(datafusion::arrow::array::StringArray::from(vec!["x"])),
                    ],
                )
                .unwrap()]],
            )
            .unwrap(),
        );
        assert_ne!(
            request(1, 1, 2, "SELECT 1", "", "a,b", vec![wide]).key(),
            base.key(),
            "input schema must change the key"
        );
        assert_eq!(
            request(1, 1, 2, "SELECT 1", "", "a,b", vec![kv_table(&[(9, 9)])]).key(),
            base.key(),
            "row content must NOT change the key (per-task measured stats stay out)"
        );
    }

    #[test]
    fn parse_cap_entries_defaults_and_zero() {
        assert_eq!(parse_cap_entries(None), DEFAULT_STAGE_PLAN_CACHE_ENTRIES);
        assert_eq!(parse_cap_entries(Some("0")), 0);
        assert_eq!(parse_cap_entries(Some(" 42 ")), 42);
        assert_eq!(
            parse_cap_entries(Some("junk")),
            DEFAULT_STAGE_PLAN_CACHE_ENTRIES
        );
    }

    #[test]
    fn localized_shuffle_input_names_parse() {
        assert_eq!(
            parse_localized_shuffle_input_name("shuffle_input__s2_p7"),
            Some((2, 7, None))
        );
        assert_eq!(
            parse_localized_shuffle_input_name("shuffle_input__s12_p0_3"),
            Some((12, 0, Some(3)))
        );
        for name in [
            "shuffle_input",
            "shuffle_input_2",
            "shuffle_input__s2",
            "shuffle_input__s2_p",
            "shuffle_input__sx_p0",
            "shuffle_input__s2_p0_x",
            "my_shuffle_input__s1_p2",
            "shuffle_input__s2_p0_1_2",
            "orders",
        ] {
            assert_eq!(
                parse_localized_shuffle_input_name(name),
                None,
                "{name} must not parse"
            );
            assert!(!is_localized_shuffle_input_name(name));
        }
        assert!(is_localized_shuffle_input_name("shuffle_input__s0_p0"));
        assert!(is_localized_shuffle_input_name("shuffle_input__s1_p2_1"));
    }

    #[test]
    fn disabled_cache_never_looks_up() {
        let cache = StagePlanCache::with_cap(0);
        let req = request(1, 1, 2, "SELECT 1", "", "", vec![]);
        assert!(cache.lookup(req.key()).is_none());
        assert_eq!(cache.stats().misses, 0);
    }

    #[test]
    fn miss_build_hit_flow_counts() {
        std::env::remove_var("WEFT_STAGE_PLAN_CACHE_ENTRIES");
        let cache = StagePlanCache::with_cap(4);
        let req = request(1, 1, 2, "SELECT 1", "", "", vec![]);
        let PlanLookup::Build(ticket) = cache.lookup(req.key()).unwrap() else {
            panic!("first lookup must own the build");
        };
        cache.complete_build(ticket, Some(empty_plan()));
        let Some(PlanLookup::Hit(_)) = cache.lookup(req.key()) else {
            panic!("second lookup must hit the published template");
        };
        let stats = cache.stats();
        assert_eq!(
            (stats.hits, stats.misses, stats.builds, stats.entries),
            (1, 1, 1, 1)
        );
        // A different key misses again.
        let other = request(1, 1, 3, "SELECT 1", "", "", vec![]);
        assert!(matches!(
            cache.lookup(other.key()),
            Some(PlanLookup::Build(_))
        ));
    }

    #[test]
    fn lru_evicts_least_recently_used_first() {
        std::env::remove_var("WEFT_STAGE_PLAN_CACHE_ENTRIES");
        let cache = StagePlanCache::with_cap(2);
        for stage in 0..3 {
            let req = request(1, 1, stage, "SELECT 1", "", "", vec![]);
            let PlanLookup::Build(ticket) = cache.lookup(req.key()).unwrap() else {
                panic!("each new key must build");
            };
            cache.complete_build(ticket, Some(empty_plan()));
        }
        let stats = cache.stats();
        assert_eq!((stats.evictions, stats.entries), (1, 2));
        let s0 = request(1, 1, 0, "SELECT 1", "", "", vec![]);
        assert!(
            matches!(cache.lookup(s0.key()), Some(PlanLookup::Build(_))),
            "stage 0 was the LRU victim"
        );
        let s2 = request(1, 1, 2, "SELECT 1", "", "", vec![]);
        assert!(matches!(cache.lookup(s2.key()), Some(PlanLookup::Hit(_))));
    }

    #[tokio::test]
    async fn single_flight_coalesces_concurrent_builds() {
        std::env::remove_var("WEFT_STAGE_PLAN_CACHE_ENTRIES");
        let cache = StagePlanCache::with_cap(4);
        let req = request(1, 1, 2, "SELECT 1", "", "", vec![]);
        let PlanLookup::Build(ticket) = cache.lookup(req.key()).unwrap() else {
            panic!("first task builds");
        };
        // Two concurrent same-key tasks wait on the in-flight build.
        let mut waiters = Vec::new();
        for _ in 0..2 {
            let Some(PlanLookup::Wait(rx)) = cache.lookup(req.key()) else {
                panic!("concurrent same-key task must wait");
            };
            waiters.push(rx);
        }
        cache.complete_build(ticket, Some(empty_plan()));
        for mut rx in waiters {
            let published = rx.wait_for(|slot| slot.is_some()).await.unwrap();
            assert!(published.as_ref().unwrap().is_some());
            drop(published);
            assert!(matches!(cache.lookup(req.key()), Some(PlanLookup::Hit(_))));
        }
        let stats = cache.stats();
        assert_eq!(
            (stats.builds, stats.misses, stats.hits),
            (1, 1, 2),
            "three tasks, one plan pass"
        );
    }

    #[tokio::test]
    async fn failed_build_releases_waiters_and_caches_nothing() {
        std::env::remove_var("WEFT_STAGE_PLAN_CACHE_ENTRIES");
        let cache = StagePlanCache::with_cap(4);
        let req = request(1, 1, 2, "SELECT 1", "", "", vec![]);
        let PlanLookup::Build(ticket) = cache.lookup(req.key()).unwrap() else {
            panic!("first task builds");
        };
        let Some(PlanLookup::Wait(mut rx)) = cache.lookup(req.key()) else {
            panic!("second task waits");
        };
        cache.complete_build(ticket, None);
        let published = rx.wait_for(|slot| slot.is_some()).await.unwrap();
        assert!(
            published.as_ref().unwrap().is_none(),
            "failed build publishes nothing"
        );
        drop(published);
        // The waiter loops and becomes the builder itself (a failed plan is never cached).
        assert!(matches!(
            cache.lookup(req.key()),
            Some(PlanLookup::Build(_))
        ));
        let stats = cache.stats();
        assert_eq!(
            (stats.builds, stats.misses, stats.entries),
            (0, 2, 0),
            "the failed build and the retried build each count a miss; waiting does not"
        );
    }

    /// Collect (table name, provider) of every TableScan in `plan`.
    fn table_scans(plan: &LogicalPlan) -> Vec<(String, Arc<dyn TableProvider>)> {
        let mut out = Vec::new();
        if let LogicalPlan::TableScan(scan) = plan {
            out.push((
                scan.table_name.table().to_string(),
                source_as_provider(&scan.source).unwrap(),
            ));
        }
        for input in plan.inputs() {
            out.extend(table_scans(input));
        }
        out
    }

    fn int_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
        let mut out = Vec::new();
        for b in batches {
            let cols: Vec<&Int64Array> = (0..4)
                .map(|i| b.column(i).as_any().downcast_ref::<Int64Array>().unwrap())
                .collect();
            for i in 0..b.num_rows() {
                out.push((
                    cols[0].value(i),
                    cols[1].value(i),
                    cols[2].value(i),
                    cols[3].value(i),
                ));
            }
        }
        out
    }

    /// End-to-end: a hit rebinds only this stage's shuffle-input scans to the hitting task's
    /// providers (base tables keep the builder's), and executing the rebound plan reads the
    /// hitting task's data.
    #[tokio::test]
    async fn rebind_swaps_only_this_stages_shuffle_inputs() {
        let ctx = SessionContext::new();
        let old0 = kv_table(&[(1, 10)]);
        let old1 = kv_table(&[(1, 100)]);
        let dim = kv_table(&[(1, 1000), (2, 2000)]);
        ctx.register_table("shuffle_input__s2_p0_0", old0.clone())
            .unwrap();
        ctx.register_table("shuffle_input__s2_p0_1", old1.clone())
            .unwrap();
        ctx.register_table("dim", dim.clone()).unwrap();
        let template = ctx
            .sql(
                "SELECT a.k AS k, a.v AS av, b.v AS bv, d.v AS dv \
                 FROM shuffle_input__s2_p0_0 a \
                 JOIN shuffle_input__s2_p0_1 b ON a.k = b.k \
                 JOIN dim d ON a.k = d.k",
            )
            .await
            .unwrap()
            .logical_plan()
            .clone();

        // The hitting task (another partition) pulled DIFFERENT buckets.
        let new0 = kv_table(&[(2, 20)]);
        let new1 = kv_table(&[(2, 200)]);
        let providers = vec![new0.clone(), new1.clone()];
        let rebound = rebind_shuffle_inputs(&template, 2, &providers).unwrap();

        let scans = table_scans(&rebound);
        assert_eq!(scans.len(), 3);
        for (name, provider) in &scans {
            match name.as_str() {
                "shuffle_input__s2_p0_0" => assert!(Arc::ptr_eq(provider, &new0)),
                "shuffle_input__s2_p0_1" => assert!(Arc::ptr_eq(provider, &new1)),
                "dim" => assert!(
                    Arc::ptr_eq(provider, &dim),
                    "base tables keep the builder's provider"
                ),
                other => panic!("unexpected scan {other}"),
            }
        }
        let batches = ctx
            .execute_logical_plan(rebound)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            int_rows(&batches),
            vec![(2, 20, 200, 2000)],
            "the rebound plan must execute the hitting task's data"
        );
    }

    /// Expression subqueries embed plans outside TreeNode traversal; the rebind must recurse.
    #[tokio::test]
    async fn rebind_reaches_in_subquery_scans() {
        let ctx = SessionContext::new();
        ctx.register_table("shuffle_input__s2_p0_0", kv_table(&[(1, 10)]))
            .unwrap();
        ctx.register_table("shuffle_input__s2_p0_1", kv_table(&[(1, 100)]))
            .unwrap();
        let template = ctx
            .sql(
                "SELECT k, v FROM shuffle_input__s2_p0_0 \
                 WHERE k IN (SELECT k FROM shuffle_input__s2_p0_1)",
            )
            .await
            .unwrap()
            .logical_plan()
            .clone();
        // The hitting task's subquery input contains k=2; the builder's does not — only a
        // rebound subquery scan returns the row.
        let providers = vec![kv_table(&[(2, 20)]), kv_table(&[(2, 200)])];
        let rebound = rebind_shuffle_inputs(&template, 2, &providers).unwrap();
        let batches = ctx
            .execute_logical_plan(rebound)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "the IN subquery must read the hitting task's data");
    }

    #[test]
    fn strip_empties_localized_scans_and_keeps_base_scans() {
        let localized = LogicalPlan::TableScan(
            TableScan::try_new(
                "shuffle_input__s7_p3",
                provider_as_source(kv_table(&[(1, 2), (3, 4)])),
                None,
                vec![],
                None,
            )
            .unwrap(),
        );
        let base_provider = kv_table(&[(9, 9)]);
        let base = LogicalPlan::TableScan(
            TableScan::try_new(
                "orders",
                provider_as_source(Arc::clone(&base_provider)),
                None,
                vec![],
                None,
            )
            .unwrap(),
        );

        let stripped = strip_shuffle_input_sources(&localized).expect("strip localized");
        let LogicalPlan::TableScan(scan) = stripped else {
            panic!("strip must keep the TableScan node")
        };
        let provider = source_as_provider(&scan.source).unwrap();
        let mem = (provider.as_ref() as &dyn std::any::Any)
            .downcast_ref::<MemTable>()
            .expect("stripped source must be an empty MemTable");
        assert_eq!(
            mem.batches
                .iter()
                .map(|p| p.try_read().unwrap().len())
                .sum::<usize>(),
            0,
            "the cached template must not retain the building task's batches"
        );
        assert_eq!(mem.schema(), kv_schema(), "schema must be preserved");

        let stripped_base = strip_shuffle_input_sources(&base).expect("strip base");
        let LogicalPlan::TableScan(scan) = stripped_base else {
            panic!("strip must keep the TableScan node")
        };
        assert!(
            Arc::ptr_eq(&source_as_provider(&scan.source).unwrap(), &base_provider),
            "non-localized scans keep the builder's provider"
        );
    }

    #[test]
    fn complete_build_stores_stripped_template_and_hit_rebinds() {
        std::env::remove_var("WEFT_STAGE_PLAN_CACHE_ENTRIES");
        let cache = StagePlanCache::with_cap(4);
        let req = request(
            1,
            1,
            7,
            "SELECT k, v FROM shuffle_input__s7_p0",
            "",
            "",
            vec![kv_table(&[(1, 2)])],
        );
        let PlanLookup::Build(ticket) = cache.lookup(req.key()).unwrap() else {
            panic!("first lookup must own the build");
        };
        let live = LogicalPlan::TableScan(
            TableScan::try_new(
                "shuffle_input__s7_p0",
                provider_as_source(kv_table(&[(1, 2), (3, 4)])),
                None,
                vec![],
                None,
            )
            .unwrap(),
        );
        cache.complete_build(ticket, Some(live));

        let Some(PlanLookup::Hit(template)) = cache.lookup(req.key()) else {
            panic!("second lookup must hit the stripped template");
        };
        let LogicalPlan::TableScan(scan) = &template else {
            panic!("template must be a TableScan")
        };
        let provider = source_as_provider(&scan.source).unwrap();
        let mem = (provider.as_ref() as &dyn std::any::Any)
            .downcast_ref::<MemTable>()
            .expect("cached source must be an empty MemTable");
        assert_eq!(
            mem.batches
                .iter()
                .map(|p| p.try_read().unwrap().len())
                .sum::<usize>(),
            0,
            "the cache must not pin the building task's pulled shuffle input"
        );

        // The hit path rebinds the hitting task's provider over the empty shell.
        let hitting = kv_table(&[(5, 50)]);
        let rebound = rebind_shuffle_inputs(&template, 7, std::slice::from_ref(&hitting)).unwrap();
        let LogicalPlan::TableScan(scan) = rebound else {
            panic!("rebind must keep the TableScan node")
        };
        assert!(
            Arc::ptr_eq(&source_as_provider(&scan.source).unwrap(), &hitting),
            "rebind must install the hitting task's provider"
        );
    }

    #[tokio::test]
    async fn rebind_unknown_upstream_index_errors() {
        let ctx = SessionContext::new();
        ctx.register_table("shuffle_input__s2_p0_1", kv_table(&[(1, 100)]))
            .unwrap();
        let template = ctx
            .sql("SELECT k, v FROM shuffle_input__s2_p0_1")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let providers = vec![kv_table(&[(2, 20)])];
        let err = rebind_shuffle_inputs(&template, 2, &providers).unwrap_err();
        assert!(
            err.to_string().contains("shuffle input 1"),
            "unexpected error: {err}"
        );
    }
}
