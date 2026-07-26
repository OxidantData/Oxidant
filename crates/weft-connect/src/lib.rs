//! `weft-connect` — the Spark Connect gRPC front-end.
//!
//! Implements `SparkConnectService` so an unmodified PySpark / Spark SQL client connects
//! via `sc://host:port`. `ExecutePlan` runs a SQL relation, a PySpark `SqlCommand`
//! (`spark.sql(...)` — a query returns a lazy `SqlCommandResult` relation handle; a DDL/DML
//! command runs eagerly and returns its result as a `LocalRelation`), a `LocalRelation`, or a
//! `ShowString` (`DataFrame.show()`) → DataFusion ([`weft_loom::Engine`]) → Arrow IPC +
//! `ResultComplete`. The **DataFrame API** lowers Spark Connect relation/expression trees
//! (`Project`/`Filter`/`Aggregate`/`Join`/`Sort`/`SetOp`/… and their expressions) to DataFusion
//! logical plans in [`translate`], so `df.select(...).filter(...).groupBy(...).agg(...)` runs
//! without SQL. `AnalyzePlan` answers `SparkVersion` and `Schema` (with Arrow→Spark type
//! conversion in [`types`]); `Config` get/set is a real session store. Validated end-to-end
//! against stock `pyspark-connect` 4.0 (`spark.sql(...)` + the DataFrame API).
//!
//! Request path: gRPC `Plan` → SQL / relation → [`weft_loom`] → Arrow IPC back out.

// Every handler returns `Result<_, tonic::Status>` per the gRPC contract; `Status` is a large
// Err type, but it's fixed by the service trait, so boxing it buys nothing.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use sc::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};
use weft_common::{Error, Result};
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_loom::arrow::ipc::writer::StreamWriter;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_observability::{AppStateStore, QueryTracker, SharedStore};
use weft_proto::spark::connect as sc;
use weft_streaming::StreamingQueryManager;

mod catalog;
mod distributed;
mod streaming;
mod translate;
mod types;
mod udf;

/// Max gRPC message size (Spark Connect defaults to 128 MB; we allow 256 MB headroom).
const MAX_MSG: usize = 256 * 1024 * 1024;
/// Rows per Arrow result chunk, so a single gRPC message never carries an oversized batch.
const CHUNK_ROWS: usize = 8192;

/// Server configuration.
#[derive(Clone)]
pub struct ServerConfig {
    /// TCP port. Sail uses 50051; Spark's own server defaults to 15002.
    pub port: u16,
    /// Monitoring UI HTTP port (Spark default 4040). `None` disables the UI server.
    pub ui_port: Option<u16>,
    /// Shared observability store for the UI. Created automatically when `ui_port` is set.
    pub observability: Option<SharedStore>,
    /// Catalogs to declare at startup, as flat `spark.sql.catalog.<name>.*` entries (e.g.
    /// `spark.sql.catalog.prod.type=hive`). Seeds the session config so external catalogs are
    /// live before the first client connects; clients can still add more via the `Config` RPC.
    pub catalogs: std::collections::HashMap<String, String>,
    /// Arrow Flight worker endpoints for distributed execution (`host:port` or
    /// `http(s)://host:port`, comma-separated in env as `WEFT_WORKERS`). When non-empty,
    /// auto-splittable queries route through the driver.
    pub workers: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 50051,
            ui_port: Some(4040),
            observability: None,
            catalogs: std::collections::HashMap::new(),
            workers: distributed::parse_worker_list(None),
        }
    }
}

/// The Spark Connect service implementation.
pub struct WeftService {
    engine: Arc<Engine>,
    /// Server-side session idempotency key (per server lifetime).
    server_session_id: String,
    /// Session SQL config (`spark.sql.*`), set/queried via the `Config` RPC.
    config: std::sync::Mutex<std::collections::HashMap<String, String>>,
    /// Named catalogs + current catalog/db pointers. External catalogs declared via
    /// `spark.sql.catalog.<name>.*` are bridged into the engine and tracked here.
    registry: Arc<weft_catalog::CatalogRegistry>,
    /// Structured Streaming query manager.
    streaming: Arc<StreamingQueryManager>,
    /// Flight worker endpoints for distributed query routing.
    pub workers: Vec<String>,
    /// Python UDF artifact bytes from `AddArtifacts`.
    artifacts: udf::SharedArtifacts,
    /// Buffered completed operation responses for ReattachExecute.
    completed_ops:
        std::sync::Mutex<std::collections::HashMap<String, Vec<sc::ExecutePlanResponse>>>,
    /// Runtime observability store (jobs, stages, SQL plans).
    observability: SharedStore,
}

impl Default for WeftService {
    fn default() -> Self {
        Self::new()
    }
}

impl WeftService {
    /// Build a service with a fresh DataFusion-backed engine.
    pub fn new() -> Self {
        Self::with_store(Arc::new(AppStateStore::new()))
    }

    /// Build with an explicit observability store (tests, history server).
    pub fn with_store(observability: SharedStore) -> Self {
        // Seed the defaults PySpark reads during normal operation (Arrow→pandas timezone; the
        // local-relation cache threshold `createDataFrame` parses as an int).
        let mut config = std::collections::HashMap::new();
        for (k, v) in [
            ("spark.sql.session.timeZone", "UTC"),
            ("spark.sql.session.localRelationCacheThreshold", "67108864"),
            ("spark.sql.execution.arrow.maxRecordsPerBatch", "10000"),
        ] {
            config.insert(k.to_string(), v.to_string());
        }
        Self {
            engine: Arc::new(Engine::new()),
            server_session_id: Uuid::new_v4().to_string(),
            config: std::sync::Mutex::new(config),
            registry: Arc::new(weft_catalog::CatalogRegistry::new()),
            streaming: Arc::new(StreamingQueryManager::new()),
            workers: distributed::parse_worker_list(None),
            artifacts: Arc::new(std::sync::Mutex::new(udf::ArtifactStore::default())),
            completed_ops: std::sync::Mutex::new(std::collections::HashMap::new()),
            observability,
        }
    }

    /// Access the observability store.
    pub fn observability(&self) -> &SharedStore {
        &self.observability
    }

    fn workers_from_config(&self) -> Vec<String> {
        let cfg_workers = self
            .config
            .lock()
            .expect("config poisoned")
            .get("spark.weft.workers")
            .map(|s| distributed::parse_worker_list(Some(s)))
            .filter(|w| !w.is_empty());
        cfg_workers.unwrap_or_else(|| self.workers.clone())
    }

    /// Reconcile declared `spark.sql.catalog.<name>.*` config into live, bridged catalogs.
    ///
    /// Idempotent and best-effort: each not-yet-registered catalog whose config is complete is
    /// built (a cheap, non-networked step — connections open lazily on first table load) and
    /// registered into both the engine (so `cat.ns.tbl` resolves) and the registry (so the
    /// `spark.catalog.*` RPC sees it). Catalogs with incomplete config are retried on the next
    /// `Config` set. Honors `spark.sql.defaultCatalog`.
    /// Borrow the underlying query engine (used by `weft worker` to serve Flight with the same catalogs).
    pub fn engine(&self) -> Arc<Engine> {
        Arc::clone(&self.engine)
    }

    /// Build a service with external catalogs declared up front (flat `spark.sql.catalog.*`
    /// entries). The catalogs are bridged into the engine before any client connects.
    pub fn with_catalogs(catalogs: std::collections::HashMap<String, String>) -> Self {
        Self::with_config(ServerConfig {
            catalogs,
            ..Default::default()
        })
    }
    pub fn with_config(config: ServerConfig) -> Self {
        let store = config
            .observability
            .clone()
            .unwrap_or_else(|| Arc::new(AppStateStore::new()));
        let mut svc = Self::with_store(store);
        if !config.catalogs.is_empty() {
            svc.config
                .lock()
                .expect("config poisoned")
                .extend(config.catalogs);
            svc.sync_catalogs();
        }
        if !config.workers.is_empty() {
            svc.workers = config.workers;
        }
        svc.sync_observability_env();
        svc
    }

    fn sync_observability_env(&self) {
        let snapshot = self.config.lock().expect("config poisoned").clone();
        self.observability.set_environment(snapshot);
    }

    /// Build with a pre-configured engine (tests and embedded driver setups).
    pub fn with_engine(engine: Arc<Engine>) -> Self {
        let mut svc = Self::new();
        svc.engine = engine;
        svc
    }

    fn sync_catalogs(&self) {
        let snapshot = self.config.lock().expect("config poisoned").clone();
        for (name, opts) in catalog::group_catalog_options(&snapshot) {
            if self.registry.contains(&name) {
                continue;
            }
            if let Ok(provider) = catalog::build_provider(&name, &opts) {
                self.engine.register_catalog(&name, provider.clone());
                self.registry.register(&name, provider);
            }
        }
        if let Some(def) = snapshot.get("spark.sql.defaultCatalog") {
            let _ = self.registry.set_current_catalog(def);
        }
    }

    fn response(
        &self,
        session_id: &str,
        operation_id: &str,
        response_type: sc::execute_plan_response::ResponseType,
    ) -> sc::ExecutePlanResponse {
        sc::ExecutePlanResponse {
            session_id: session_id.to_string(),
            server_side_session_id: self.server_session_id.clone(),
            operation_id: operation_id.to_string(),
            response_id: Uuid::new_v4().to_string(),
            response_type: Some(response_type),
            ..Default::default()
        }
    }

    /// Stream `batches` back as chunked Arrow IPC `ArrowBatch` responses + a terminal
    /// `ResultComplete` (the shape PySpark 3.5+ reattachable execution expects).
    fn stream_batches(
        &self,
        session_id: &str,
        operation_id: &str,
        batches: &[RecordBatch],
        stats: Option<&weft_loom::QueryStats>,
    ) -> std::result::Result<Vec<sc::ExecutePlanResponse>, Status> {
        let mut responses = Vec::new();
        for batch in batches {
            // Slice wide results into row-chunks so no single gRPC message is oversized. A
            // zero-row batch still emits exactly one (empty) ArrowBatch so the client always
            // receives a RecordBatch — PySpark's `collect()` asserts it got at least one.
            let n = batch.num_rows();
            let mut off = 0;
            loop {
                let len = (n - off).min(CHUNK_ROWS);
                let slice = batch.slice(off, len);
                let data = encode_ipc_stream(&slice)
                    .map_err(|e| Status::internal(format!("arrow ipc encode: {e}")))?;
                responses.push(self.response(
                    session_id,
                    operation_id,
                    sc::execute_plan_response::ResponseType::ArrowBatch(
                        sc::execute_plan_response::ArrowBatch {
                            row_count: len as i64,
                            data,
                            ..Default::default()
                        },
                    ),
                ));
                off += len;
                if off >= n {
                    break;
                }
            }
        }
        // Carry the result schema on the terminal response (Spark sends this "when collect is
        // called"), so the client builds a correctly-typed table even for an empty / zero-column
        // result (e.g. a DDL command) where no usable ArrowBatch exists.
        let mut complete = self.response(
            session_id,
            operation_id,
            sc::execute_plan_response::ResponseType::ResultComplete(
                sc::execute_plan_response::ResultComplete {},
            ),
        );
        if let Some(first) = batches.first() {
            complete.schema = Some(types::schema_to_spark(first.schema().as_ref()));
        }
        // Carry per-query execution metrics (duration / rows / bytes scanned) on the terminal
        // response so a client (the gateway) can surface Databricks-style observability.
        if let Some(stats) = stats {
            complete.metrics = Some(stats_to_metrics(stats));
        }
        responses.push(complete);
        Ok(responses)
    }

    fn buffer_operation(&self, operation_id: &str, responses: Vec<sc::ExecutePlanResponse>) {
        self.completed_ops
            .lock()
            .expect("completed_ops poisoned")
            .insert(operation_id.to_string(), responses);
    }

    /// Handle a PySpark `SqlCommand`. A query stays lazy — we return a `SqlCommandResult` whose
    /// relation is the `Sql` plan, so the client's `DataFrame` re-executes it on `collect`/`show`
    /// (no large result embedded here). A command (DDL/DML) runs eagerly for its side effect and
    /// its result rows come back inline as a `LocalRelation`.
    async fn run_sql_command(
        &self,
        session_id: &str,
        operation_id: &str,
        sql: &str,
    ) -> std::result::Result<Vec<sc::ExecutePlanResponse>, Status> {
        let relation = if is_query(sql) {
            sql_relation(sql)
        } else {
            let tracker =
                QueryTracker::begin(self.observability.clone(), operation_id, truncate_sql(sql));
            if let Ok(plan) = self.engine.logical_plan(sql).await {
                if let Ok(text) = self.engine.explain(&plan, true).await {
                    tracker.set_plan(text, None);
                }
            }
            let mut tracker = tracker;
            tracker.begin_local_stage("command", 1);
            let task_id = self.observability.alloc_task_id();
            tracker.task_started(0, task_id, "driver");
            let start = std::time::Instant::now();
            let batches = match self.engine.sql(sql).await {
                Ok(b) => b,
                Err(e) => {
                    tracker.finish_error(e.to_string());
                    return Err(err_to_status(e));
                }
            };
            let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
            tracker.task_finished(
                0,
                task_id,
                "driver",
                start.elapsed().as_millis() as i64,
                rows,
                0,
                0,
            );
            tracker.finish_success(rows);
            let data = encode_ipc_multi(&batches)
                .map_err(|e| Status::internal(format!("arrow ipc encode: {e}")))?;
            sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::LocalRelation(sc::LocalRelation {
                    data: Some(data),
                    schema: None,
                })),
            }
        };
        Ok(vec![
            self.response(
                session_id,
                operation_id,
                sc::execute_plan_response::ResponseType::SqlCommandResult(
                    sc::execute_plan_response::SqlCommandResult {
                        relation: Some(relation),
                    },
                ),
            ),
            self.response(
                session_id,
                operation_id,
                sc::execute_plan_response::ResponseType::ResultComplete(
                    sc::execute_plan_response::ResultComplete {},
                ),
            ),
        ])
    }

    /// Resolve a request `Plan` (a `spark.sql(...)` command or a DataFrame relation tree) to a
    /// DataFusion logical plan — the seam `AnalyzePlan(Explain)` uses. A `SqlCommand` plans the
    /// query text; any relation lowers through the DataFrame translator (which handles `Sql`,
    /// `LocalRelation`, and the full relation surface).
    async fn resolve_plan(
        &self,
        plan: &Option<sc::Plan>,
    ) -> std::result::Result<datafusion::logical_expr::LogicalPlan, Status> {
        match plan.as_ref().and_then(|p| p.op_type.as_ref()) {
            Some(sc::plan::OpType::Command(cmd)) => match cmd.command_type.as_ref() {
                Some(sc::command::CommandType::SqlCommand(c)) => {
                    let sql = sql_command_text(c)
                        .ok_or_else(|| Status::invalid_argument("empty SqlCommand"))?;
                    self.engine.logical_plan(&sql).await.map_err(err_to_status)
                }
                _ => Err(Status::unimplemented("AnalyzePlan: unsupported command")),
            },
            Some(sc::plan::OpType::Root(rel)) => translate::to_plan(self.engine.ctx(), rel).await,
            _ => Err(Status::unimplemented("AnalyzePlan: empty plan")),
        }
    }

    /// Resolve the result schema of a plan for `AnalyzePlan(Schema)`.
    async fn plan_schema(
        &self,
        plan: &Option<sc::Plan>,
    ) -> std::result::Result<weft_loom::arrow::datatypes::SchemaRef, Status> {
        match plan.as_ref().and_then(|p| p.op_type.as_ref()) {
            Some(sc::plan::OpType::Command(cmd)) => match cmd.command_type.as_ref() {
                Some(sc::command::CommandType::SqlCommand(c)) => {
                    let sql = sql_command_text(c)
                        .ok_or_else(|| Status::invalid_argument("empty SqlCommand"))?;
                    self.engine.schema(&sql).await.map_err(err_to_status)
                }
                _ => Err(Status::unimplemented(
                    "AnalyzePlan(Schema): unsupported command",
                )),
            },
            Some(sc::plan::OpType::Root(rel)) => self.relation_schema(rel).await,
            _ => Err(Status::unimplemented("AnalyzePlan(Schema): empty plan")),
        }
    }

    /// The result schema of a relation — SQL/LocalRelation directly, ShowString is one string
    /// column, everything else via the relation translator's logical plan.
    async fn relation_schema(
        &self,
        rel: &sc::Relation,
    ) -> std::result::Result<weft_loom::arrow::datatypes::SchemaRef, Status> {
        use weft_loom::arrow::datatypes::{DataType, Field, Schema};
        match rel.rel_type.as_ref() {
            Some(sc::relation::RelType::Sql(sql)) => {
                self.engine.schema(&sql.query).await.map_err(err_to_status)
            }
            Some(sc::relation::RelType::LocalRelation(lr)) => {
                let data = lr.data.as_deref().unwrap_or_default();
                let reader = StreamReader::try_new(Cursor::new(data.to_vec()), None)
                    .map_err(|e| Status::internal(format!("decode local relation: {e}")))?;
                Ok(reader.schema())
            }
            Some(sc::relation::RelType::ShowString(_)) => {
                Ok(Arc::new(Schema::new(vec![Field::new(
                    "show_string",
                    DataType::Utf8,
                    false,
                )])))
            }
            // Resolve a catalog op's result schema statically — no side effects (so a client that
            // probes `df.schema` on `spark.catalog.listTables()` doesn't run the op).
            Some(sc::relation::RelType::Catalog(cat)) => catalog::result_schema(cat)
                .ok_or_else(|| Status::unimplemented("AnalyzePlan(Schema): catalog op")),
            _ => {
                let plan = translate::to_plan(self.engine.ctx(), rel).await?;
                Ok(Arc::new(plan.schema().as_arrow().clone()))
            }
        }
    }

    /// Evaluate a relation to record batches. Handles `ShowString` (PySpark `.show()`) by
    /// formatting its child into a single-cell string table; everything else falls through to
    /// [`Self::base_relation_batches`].
    async fn eval_relation(
        &self,
        rel: &sc::Relation,
        operation_id: Option<&str>,
    ) -> std::result::Result<(Vec<RecordBatch>, Option<weft_loom::QueryStats>), Status> {
        if let Some(sc::relation::RelType::ShowString(s)) = rel.rel_type.as_ref() {
            let child = s
                .input
                .as_deref()
                .ok_or_else(|| Status::invalid_argument("ShowString.input missing"))?;
            let (batches, _) = self.base_relation_batches(child, operation_id).await?;
            let text = show_string(&batches, s.num_rows, s.truncate)?;
            return Ok((vec![show_string_batch(text)], None));
        }
        // `spark.catalog.*` operations (listTables, currentCatalog, setCurrentDatabase, …).
        if let Some(sc::relation::RelType::Catalog(cat)) = rel.rel_type.as_ref() {
            let batches = catalog::handle_catalog(&self.engine, &self.registry, cat).await?;
            return Ok((batches, None));
        }
        self.base_relation_batches(rel, operation_id).await
    }

    /// Evaluate a `Sql` or `LocalRelation` to record batches, with observability hooks. The second
    /// tuple element is the executed-plan stats (rows / bytes scanned / duration) — `Some` only for
    /// a metrics-capturing scan query, `None` otherwise (so we never emit fabricated zero metrics).
    ///
    /// When workers or `WEFT_WORKER_SERVICE` is configured, auto-splittable queries route through
    /// [`distributed::try_run_distributed`] (file sharding via `WEFT_WORKER_COUNT` / `WEFT_SHARD_INDEX`
    /// on workers; replicated dims via `WEFT_REPLICATED_TABLES`). Unsupported shapes fall back locally.
    async fn base_relation_batches(
        &self,
        rel: &sc::Relation,
        operation_id: Option<&str>,
    ) -> std::result::Result<(Vec<RecordBatch>, Option<weft_loom::QueryStats>), Status> {
        match rel.rel_type.as_ref() {
            Some(sc::relation::RelType::Sql(sql)) => {
                let workers = self.workers_from_config();
                let replicated = replicated_tables_from_env();
                let replicated_refs: Vec<&str> = replicated.iter().map(String::as_str).collect();
                let udf_json = self.engine.export_udfs_json();
                let description = truncate_sql(&sql.query);
                let tracker = operation_id.map(|op| {
                    QueryTracker::begin(self.observability.clone(), op, description.clone())
                });
                if let Some(ref t) = tracker {
                    if let Ok(plan) = self.engine.logical_plan(&sql.query).await {
                        if let Ok(text) = self.engine.explain(&plan, true).await {
                            t.set_plan(text, None);
                        }
                    }
                }
                match distributed::try_run_distributed(
                    &self.engine,
                    &workers,
                    &sql.query,
                    &replicated_refs,
                    Some(&udf_json),
                    tracker.as_ref(),
                )
                .await
                {
                    Ok(Some(dist)) => {
                        if let Some(t) = tracker {
                            let rows: i64 = dist.iter().map(|b| b.num_rows() as i64).sum();
                            t.finish_success(rows);
                        }
                        // Distributed execution doesn't surface DataFusion scan metrics → no stats.
                        return Ok((dist, None));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if let Some(t) = tracker {
                            t.finish_error(e.to_string());
                        }
                        return Err(err_to_status(e));
                    }
                }
                let local_tracker = tracker.map(|t| {
                    let mut t = t;
                    t.begin_local_stage("local", 1);
                    t
                });
                let task_id = local_tracker
                    .as_ref()
                    .map(|_| self.observability.alloc_task_id());
                if let (Some(ref t), Some(tid)) = (&local_tracker, task_id) {
                    t.task_started(0, tid, "driver");
                }
                let start = std::time::Instant::now();
                // A plain scan query goes through `sql_with_stats` so the executed plan's metrics
                // (bytes scanned, rows, duration) are captured. Everything else — SHOW / DESCRIBE /
                // DDL / CTAS / INSERT — routes through `engine.sql`, which intercepts the forms
                // DataFusion can't plan; those carry no scan metrics.
                let exec = if is_scan_query(&sql.query) {
                    // Metrics-capturing path: rows / bytes scanned / duration from the executed plan.
                    self.engine
                        .sql_with_stats(&sql.query)
                        .await
                        .map(|(b, s)| (b, Some(s)))
                } else {
                    // SHOW / DESCRIBE / DDL / CTAS / INSERT: no scan metrics — don't fabricate any.
                    self.engine.sql(&sql.query).await.map(|b| (b, None))
                };
                let (mut batches, stats): (Vec<RecordBatch>, Option<weft_loom::QueryStats>) =
                    match exec {
                        Ok(v) => v,
                        Err(e) => {
                            if let Some(t) = local_tracker {
                                t.finish_error(e.to_string());
                            }
                            return Err(err_to_status(e));
                        }
                    };
                // A 0-row result must still carry its schema so the client gets a typed (empty)
                // table. Re-derive the schema only for queries — `engine.schema` plans via
                // `ctx.sql`, which would re-execute a DDL statement (a query has no side effect).
                if batches.is_empty() && is_query(&sql.query) {
                    let schema = self
                        .engine
                        .schema(&sql.query)
                        .await
                        .map_err(err_to_status)?;
                    batches.push(RecordBatch::new_empty(schema));
                }
                let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                if let (Some(t), Some(tid)) = (local_tracker, task_id) {
                    t.task_finished(
                        0,
                        tid,
                        "driver",
                        start.elapsed().as_millis() as i64,
                        rows,
                        0,
                        0,
                    );
                    t.finish_success(rows);
                }
                Ok((batches, stats))
            }
            Some(sc::relation::RelType::LocalRelation(lr)) => {
                let data = lr.data.as_deref().unwrap_or_default();
                let reader = StreamReader::try_new(Cursor::new(data.to_vec()), None)
                    .map_err(|e| Status::internal(format!("decode local relation: {e}")))?;
                let schema = reader.schema();
                let mut batches = reader
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| Status::internal(format!("decode local relation: {e}")))?;
                if batches.is_empty() {
                    batches.push(RecordBatch::new_empty(schema));
                }
                Ok((batches, None))
            }
            // Everything else (Project/Filter/Aggregate/Join/… — the DataFrame API) lowers to a
            // DataFusion logical plan and executes. A 0-row result still carries its schema.
            _ => {
                let plan = translate::to_plan(self.engine.ctx(), rel).await?;
                let schema = Arc::new(plan.schema().as_arrow().clone());
                let workers = self.workers_from_config();
                let replicated = replicated_tables_from_env();
                let replicated_refs: Vec<&str> = replicated.iter().map(String::as_str).collect();
                let udf_json = self.engine.export_udfs_json();
                let tracker = operation_id
                    .map(|op| QueryTracker::begin(self.observability.clone(), op, "DataFrame"));
                if let Some(ref t) = tracker {
                    if let Ok(text) = self.engine.explain(&plan, true).await {
                        t.set_plan(text, None);
                    }
                }
                match distributed::try_run_distributed_plan(
                    &self.engine,
                    &workers,
                    &plan,
                    "DataFrame",
                    &replicated_refs,
                    Some(&udf_json),
                    tracker.as_ref(),
                )
                .await
                {
                    Ok(Some(dist)) => {
                        let mut batches = dist
                            .into_iter()
                            .map(signed_columns)
                            .collect::<std::result::Result<Vec<_>, _>>()?;
                        if batches.is_empty() {
                            batches.push(RecordBatch::new_empty(signed_schema(&schema)));
                        }
                        if let Some(t) = tracker {
                            let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                            t.finish_success(rows);
                        }
                        return Ok((batches, None));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if let Some(t) = tracker {
                            t.finish_error(e.to_string());
                        }
                        return Err(err_to_status(e));
                    }
                }
                let local_tracker = tracker.map(|t| {
                    let mut t = t;
                    t.begin_local_stage("dataframe", 1);
                    t
                });
                let task_id = local_tracker
                    .as_ref()
                    .map(|_| self.observability.alloc_task_id());
                if let (Some(ref t), Some(tid)) = (&local_tracker, task_id) {
                    t.task_started(0, tid, "driver");
                }
                let start = std::time::Instant::now();
                let batches = match self.engine.execute_logical_plan(plan).await {
                    Ok(b) => b,
                    Err(e) => {
                        if let Some(t) = local_tracker {
                            t.finish_error(e.to_string());
                        }
                        return Err(err_to_status(e));
                    }
                };
                let mut batches = batches
                    .into_iter()
                    .map(signed_columns)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                if batches.is_empty() {
                    batches.push(RecordBatch::new_empty(signed_schema(&schema)));
                }
                let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                if let (Some(t), Some(tid)) = (local_tracker, task_id) {
                    t.task_finished(
                        0,
                        tid,
                        "driver",
                        start.elapsed().as_millis() as i64,
                        rows,
                        0,
                        0,
                    );
                    t.finish_success(rows);
                }
                // DataFrame-API path: `execute_logical_plan` doesn't retain the plan for metrics.
                Ok((batches, None))
            }
        }
    }

    /// Look up a config key: stored value, else a lenient default (so PySpark's `conf.get` never
    /// sees `None` and `.lower()`-style client code doesn't crash).
    fn config_get(&self, key: &str) -> sc::KeyValue {
        let value = self
            .config
            .lock()
            .expect("config poisoned")
            .get(key)
            .cloned()
            .unwrap_or_else(|| config_default(key));
        sc::KeyValue {
            key: key.to_string(),
            value: Some(value),
        }
    }
}

type RespStream =
    Pin<Box<dyn Stream<Item = std::result::Result<sc::ExecutePlanResponse, Status>> + Send>>;

#[tonic::async_trait]
impl SparkConnectService for WeftService {
    type ExecutePlanStream = RespStream;
    type ReattachExecuteStream = RespStream;

    async fn execute_plan(
        &self,
        request: Request<sc::ExecutePlanRequest>,
    ) -> std::result::Result<Response<Self::ExecutePlanStream>, Status> {
        let req = request.into_inner();
        let session_id = req.session_id.clone();
        let operation_id = req
            .operation_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let responses = match req.plan.as_ref().and_then(|p| p.op_type.as_ref()) {
            // PySpark `spark.sql(...)`: a query returns a lazy relation handle; a DDL/DML command
            // runs eagerly and returns its result as a LocalRelation.
            Some(sc::plan::OpType::Command(cmd)) => match cmd.command_type.as_ref() {
                Some(sc::command::CommandType::SqlCommand(c)) => {
                    let sql = sql_command_text(c)
                        .ok_or_else(|| Status::invalid_argument("empty SqlCommand"))?;
                    self.run_sql_command(&session_id, &operation_id, &sql)
                        .await?
                }
                Some(sc::command::CommandType::WriteStreamOperationStart(s)) => {
                    let result = self.handle_write_stream_start(s).await?;
                    vec![self.response(
                        &session_id,
                        &operation_id,
                        sc::execute_plan_response::ResponseType::WriteStreamOperationStartResult(
                            result,
                        ),
                    )]
                }
                Some(sc::command::CommandType::StreamingQueryCommand(c)) => {
                    let result = self.handle_streaming_query_command(c).await?;
                    vec![self.response(
                        &session_id,
                        &operation_id,
                        sc::execute_plan_response::ResponseType::StreamingQueryCommandResult(
                            result,
                        ),
                    )]
                }
                Some(sc::command::CommandType::RegisterFunction(rf)) => {
                    // Register in DataFusion's UDF registry so SQL cells can call the UDF.
                    {
                        let registry = self.engine.udf_registry();
                        let mut reg = registry.lock().unwrap();
                        udf::register_connect_udf(self.engine.ctx(), &mut reg, rf)?;
                    }
                    // Also store bytes + forward to pyworker so Python cells can invoke it.
                    if let Some(sc::common_inline_user_defined_function::Function::PythonUdf(
                        py_udf,
                    )) = rf.function.as_ref()
                    {
                        {
                            let mut arts = self.artifacts.lock().expect("artifacts poisoned");
                            arts.insert(
                                format!("__udf__{}", rf.function_name),
                                py_udf.command.clone(),
                            );
                        }
                        if let Ok(base) = std::env::var("WEFT_PYWORKER_URL") {
                            let client = reqwest::Client::new();
                            let _ = client
                                .post(format!("{base}/udfs"))
                                .header("X-Udf-Name", rf.function_name.as_str())
                                .header("X-Session-Id", session_id.as_str())
                                .header("X-Eval-Type", py_udf.eval_type.to_string())
                                .body(py_udf.command.clone())
                                .send()
                                .await;
                        }
                    }
                    vec![self.response(
                        &session_id,
                        &operation_id,
                        sc::execute_plan_response::ResponseType::ResultComplete(
                            sc::execute_plan_response::ResultComplete {},
                        ),
                    )]
                }
                _ => return Err(Status::unimplemented("unsupported command")),
            },
            // A relation (Sql, LocalRelation, ShowString, …): evaluate it and stream the result.
            Some(sc::plan::OpType::Root(rel)) => {
                let (batches, stats) = self.eval_relation(rel, Some(&operation_id)).await?;
                self.stream_batches(&session_id, &operation_id, &batches, stats.as_ref())?
            }
            _ => return Err(Status::unimplemented("empty or unsupported plan")),
        };

        self.buffer_operation(&operation_id, responses.clone());
        let stream = tokio_stream::iter(responses.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn analyze_plan(
        &self,
        request: Request<sc::AnalyzePlanRequest>,
    ) -> std::result::Result<Response<sc::AnalyzePlanResponse>, Status> {
        use sc::analyze_plan_request::Analyze;
        use sc::analyze_plan_response as out;

        let req = request.into_inner();
        let result = match req.analyze {
            Some(Analyze::SparkVersion(_)) => Some(out::Result::SparkVersion(out::SparkVersion {
                // Advertise the protocol version we vendored protos from.
                version: "4.0.0".to_string(),
            })),
            // PySpark `df.schema` / `df.columns` / `printSchema()` — resolve the result schema
            // without executing and return it as a Spark struct `DataType`.
            Some(Analyze::Schema(s)) => {
                let schema = self.plan_schema(&s.plan).await?;
                Some(out::Result::Schema(out::Schema {
                    schema: Some(types::schema_to_spark(&schema)),
                }))
            }
            // PySpark `df.explain()` — render the optimized + physical plan. EXTENDED/COST/FORMATTED
            // also include the logical plans; SIMPLE (and the unspecified default) show physical only.
            Some(Analyze::Explain(e)) => {
                use sc::analyze_plan_request::explain::ExplainMode;
                let plan = self.resolve_plan(&e.plan).await?;
                let extended = matches!(
                    ExplainMode::try_from(e.explain_mode),
                    Ok(ExplainMode::Extended | ExplainMode::Cost | ExplainMode::Formatted)
                );
                let text = self
                    .engine
                    .explain(&plan, extended)
                    .await
                    .map_err(err_to_status)?;
                Some(out::Result::Explain(out::Explain {
                    explain_string: text,
                }))
            }
            // PySpark `df.printSchema()` — the resolved schema as Spark's indented tree.
            Some(Analyze::TreeString(t)) => {
                let schema = self.plan_schema(&t.plan).await?;
                Some(out::Result::TreeString(out::TreeString {
                    tree_string: types::schema_tree_string(&schema),
                }))
            }
            // `df.isLocal()` / `df.isStreaming()` — Weft executes every plan server-side as a batch
            // job, so both are constant `false`. Implemented (rather than `unimplemented`) so client
            // action-dispatch paths that probe these don't abort.
            Some(Analyze::IsLocal(_)) => {
                Some(out::Result::IsLocal(out::IsLocal { is_local: false }))
            }
            Some(Analyze::IsStreaming(s)) => {
                let is_streaming = s
                    .plan
                    .as_ref()
                    .and_then(|p| p.op_type.as_ref())
                    .map(|op| match op {
                        sc::plan::OpType::Root(rel) => {
                            translate::relation::relation_is_streaming(rel)
                        }
                        _ => false,
                    })
                    .unwrap_or(false);
                Some(out::Result::IsStreaming(out::IsStreaming { is_streaming }))
            }
            other => {
                return Err(Status::unimplemented(format!(
                    "AnalyzePlan variant not implemented: {other:?}"
                )))
            }
        };
        Ok(Response::new(sc::AnalyzePlanResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            result,
        }))
    }

    async fn config(
        &self,
        request: Request<sc::ConfigRequest>,
    ) -> std::result::Result<Response<sc::ConfigResponse>, Status> {
        use sc::config_request::operation::OpType;
        let req = request.into_inner();
        let pairs = match req.operation.and_then(|o| o.op_type) {
            Some(OpType::Set(set)) => {
                {
                    let mut store = self.config.lock().expect("config poisoned");
                    for kv in set.pairs {
                        match kv.value {
                            Some(v) => {
                                store.insert(kv.key, v);
                            }
                            None => {
                                store.remove(&kv.key);
                            }
                        }
                    }
                }
                // A `spark.sql.catalog.*` change may have declared a new catalog — reconcile.
                self.sync_catalogs();
                self.sync_observability_env();
                Vec::new()
            }
            Some(OpType::Get(get)) => get.keys.iter().map(|k| self.config_get(k)).collect(),
            Some(OpType::GetWithDefault(g)) => g
                .pairs
                .into_iter()
                .map(|kv| {
                    let value = self
                        .config
                        .lock()
                        .expect("config poisoned")
                        .get(&kv.key)
                        .cloned()
                        .or(kv.value);
                    sc::KeyValue { key: kv.key, value }
                })
                .collect(),
            Some(OpType::GetOption(g)) => g
                .keys
                .into_iter()
                .map(|k| {
                    let value = self
                        .config
                        .lock()
                        .expect("config poisoned")
                        .get(&k)
                        .cloned();
                    sc::KeyValue { key: k, value }
                })
                .collect(),
            Some(OpType::GetAll(g)) => {
                let prefix = g.prefix.unwrap_or_default();
                self.config
                    .lock()
                    .expect("config poisoned")
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| sc::KeyValue {
                        key: k.clone(),
                        value: Some(v.clone()),
                    })
                    .collect()
            }
            Some(OpType::Unset(u)) => {
                let mut store = self.config.lock().expect("config poisoned");
                for k in u.keys {
                    store.remove(&k);
                }
                Vec::new()
            }
            // Everything is modifiable in this session-local store.
            Some(OpType::IsModifiable(m)) => m
                .keys
                .into_iter()
                .map(|k| sc::KeyValue {
                    key: k,
                    value: Some("true".to_string()),
                })
                .collect(),
            None => Vec::new(),
        };

        Ok(Response::new(sc::ConfigResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            pairs,
            ..Default::default()
        }))
    }

    async fn add_artifacts(
        &self,
        request: Request<tonic::Streaming<sc::AddArtifactsRequest>>,
    ) -> std::result::Result<Response<sc::AddArtifactsResponse>, Status> {
        use sc::add_artifacts_request::Payload;

        let mut stream = request.into_inner();
        let mut collected: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        let mut pending: Option<(String, Vec<u8>, i64)> = None;
        let mut spark_session_id = String::new();

        while let Some(req) = stream.message().await? {
            if spark_session_id.is_empty() && !req.session_id.is_empty() {
                spark_session_id = req.session_id.clone();
            }
            match req.payload {
                Some(Payload::Batch(batch)) => {
                    for artifact in batch.artifacts {
                        let data = artifact.data.map(|c| c.data).unwrap_or_default();
                        collected.insert(artifact.name, data);
                    }
                }
                Some(Payload::BeginChunk(begin)) => {
                    let initial = begin.initial_chunk.map(|c| c.data).unwrap_or_default();
                    // num_chunks=0 is malformed; treat it as 1 chunk already in initial_chunk.
                    let remaining = (begin.num_chunks - 1).max(0);
                    pending = Some((begin.name, initial, remaining));
                }
                Some(Payload::Chunk(chunk)) => {
                    let done = if let Some((_, ref mut buf, ref mut remaining)) = pending {
                        buf.extend_from_slice(&chunk.data);
                        *remaining -= 1;
                        *remaining <= 0
                    } else {
                        false
                    };
                    if done {
                        let (name, buf, _) = pending.take().unwrap();
                        collected.insert(name, buf);
                    }
                }
                None => {}
            }
        }
        // Flush any still-pending partial upload (shouldn't happen with a well-formed client).
        if let Some((name, buf, _)) = pending.take() {
            collected.insert(name, buf);
        }

        let summaries: Vec<sc::add_artifacts_response::ArtifactSummary> = {
            let mut store = self.artifacts.lock().expect("artifacts poisoned");
            collected
                .iter()
                .map(|(name, bytes)| {
                    store.insert(name.clone(), bytes.clone());
                    sc::add_artifacts_response::ArtifactSummary {
                        name: name.clone(),
                        is_crc_successful: true,
                    }
                })
                .collect()
        };

        if let Ok(base) = std::env::var("WEFT_PYWORKER_URL") {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .unwrap_or_default();
            for (name, bytes) in &collected {
                let _ = client
                    .post(format!("{base}/artifacts"))
                    .header("X-Artifact-Name", name.as_str())
                    .header("X-Session-Id", spark_session_id.as_str())
                    .body(bytes.clone())
                    .send()
                    .await;
            }
        }

        Ok(Response::new(sc::AddArtifactsResponse {
            server_side_session_id: self.server_session_id.clone(),
            artifacts: summaries,
            ..Default::default()
        }))
    }

    async fn artifact_status(
        &self,
        request: Request<sc::ArtifactStatusesRequest>,
    ) -> std::result::Result<Response<sc::ArtifactStatusesResponse>, Status> {
        let req = request.into_inner();
        let arts = self.artifacts.lock().expect("artifacts poisoned");
        let statuses = req
            .names
            .into_iter()
            .map(|name| {
                let exists = arts.get(&name).is_some();
                (
                    name,
                    sc::artifact_statuses_response::ArtifactStatus { exists },
                )
            })
            .collect();
        Ok(Response::new(sc::ArtifactStatusesResponse {
            statuses,
            ..Default::default()
        }))
    }

    async fn interrupt(
        &self,
        request: Request<sc::InterruptRequest>,
    ) -> std::result::Result<Response<sc::InterruptResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::InterruptResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            ..Default::default()
        }))
    }

    async fn reattach_execute(
        &self,
        request: Request<sc::ReattachExecuteRequest>,
    ) -> std::result::Result<Response<Self::ReattachExecuteStream>, Status> {
        let req = request.into_inner();
        if let Some(buf) = self
            .completed_ops
            .lock()
            .expect("completed_ops poisoned")
            .get(&req.operation_id)
            .cloned()
        {
            let stream = tokio_stream::iter(buf.into_iter().map(Ok));
            return Ok(Response::new(
                Box::pin(stream) as Self::ReattachExecuteStream
            ));
        }
        let complete = self.response(
            &req.session_id,
            &req.operation_id,
            sc::execute_plan_response::ResponseType::ResultComplete(
                sc::execute_plan_response::ResultComplete {},
            ),
        );
        let stream = tokio_stream::iter(vec![Ok(complete)]);
        Ok(Response::new(
            Box::pin(stream) as Self::ReattachExecuteStream
        ))
    }

    async fn release_execute(
        &self,
        request: Request<sc::ReleaseExecuteRequest>,
    ) -> std::result::Result<Response<sc::ReleaseExecuteResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::ReleaseExecuteResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            operation_id: Some(req.operation_id),
        }))
    }

    async fn release_session(
        &self,
        request: Request<sc::ReleaseSessionRequest>,
    ) -> std::result::Result<Response<sc::ReleaseSessionResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::ReleaseSessionResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
        }))
    }

    async fn fetch_error_details(
        &self,
        request: Request<sc::FetchErrorDetailsRequest>,
    ) -> std::result::Result<Response<sc::FetchErrorDetailsResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::FetchErrorDetailsResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            ..Default::default()
        }))
    }

    async fn clone_session(
        &self,
        _request: Request<sc::CloneSessionRequest>,
    ) -> std::result::Result<Response<sc::CloneSessionResponse>, Status> {
        Ok(Response::new(sc::CloneSessionResponse::default()))
    }

    async fn get_status(
        &self,
        request: Request<sc::GetStatusRequest>,
    ) -> std::result::Result<Response<sc::GetStatusResponse>, Status> {
        let req = request.into_inner();
        let mut response = sc::GetStatusResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            ..Default::default()
        };
        if let Some(op_req) = req.operation_status {
            let ids: Vec<String> = if op_req.operation_ids.is_empty() {
                self.observability
                    .all_operation_states()
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect()
            } else {
                op_req.operation_ids
            };
            for op_id in ids {
                if let Some(state) = self.observability.operation_state(&op_id) {
                    let proto_state = match state {
                        weft_observability::OperationState::Running => {
                            sc::get_status_response::operation_status::OperationState::Running
                                as i32
                        }
                        weft_observability::OperationState::Succeeded => {
                            sc::get_status_response::operation_status::OperationState::Succeeded
                                as i32
                        }
                        weft_observability::OperationState::Failed => {
                            sc::get_status_response::operation_status::OperationState::Failed as i32
                        }
                    };
                    response
                        .operation_statuses
                        .push(sc::get_status_response::OperationStatus {
                            operation_id: op_id,
                            state: proto_state,
                            ..Default::default()
                        });
                }
            }
        }
        Ok(Response::new(response))
    }
}

/// Pull the SQL text out of a `SqlCommand`: prefer the modern `input` relation (a `Sql` relation,
/// PySpark 4.x), fall back to the deprecated top-level `sql` string.
fn sql_command_text(c: &sc::SqlCommand) -> Option<String> {
    if let Some(input) = c.input.as_ref() {
        if let Some(sc::relation::RelType::Sql(sql)) = input.rel_type.as_ref() {
            if !sql.query.is_empty() {
                return Some(sql.query.clone());
            }
        }
    }
    #[allow(deprecated)]
    if !c.sql.is_empty() {
        return Some(c.sql.clone());
    }
    None
}

/// A bare `Sql` relation wrapping `query` (the lazy handle returned for a `SqlCommand` query).
fn sql_relation(query: &str) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
            query: query.to_string(),
            ..Default::default()
        })),
    }
}

fn truncate_sql(s: &str) -> String {
    let t = s.trim().replace('\n', " ");
    if t.chars().count() <= 120 {
        t
    } else {
        format!("{}…", t.chars().take(119).collect::<String>())
    }
}

fn is_query(sql: &str) -> bool {
    let kw = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        kw.as_str(),
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "FROM" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN"
    )
}

/// A row-returning scan query that plans directly through DataFusion, so its executed physical plan
/// carries scan metrics (`bytes_scanned`). Deliberately narrower than [`is_query`]: it excludes
/// SHOW/DESCRIBE/EXPLAIN, which `engine.sql` intercepts *before* planning (DataFusion can't plan the
/// Spark spellings) and which carry no scan metrics anyway. Used to decide whether to execute via
/// `sql_with_stats` (metrics-capturing) or plain `engine.sql`.
fn is_scan_query(sql: &str) -> bool {
    let kw = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(kw.as_str(), "SELECT" | "WITH" | "VALUES" | "TABLE")
}

/// Build a Spark Connect `Metrics` message from engine [`weft_loom::QueryStats`] — one synthetic
/// operator ("weft") carrying duration / rows / bytes-scanned counters, enough for a client to
/// render per-query observability.
fn stats_to_metrics(stats: &weft_loom::QueryStats) -> sc::execute_plan_response::Metrics {
    use sc::execute_plan_response::metrics::{MetricObject, MetricValue};
    let mv = |name: &str, value: i64, ty: &str| MetricValue {
        name: name.to_string(),
        value,
        metric_type: ty.to_string(),
    };
    let execution_metrics = std::collections::HashMap::from([
        (
            "duration_ms".to_string(),
            mv("duration_ms", stats.duration_ms as i64, "timing"),
        ),
        (
            "output_rows".to_string(),
            mv("output_rows", stats.output_rows as i64, "sum"),
        ),
        (
            "bytes_scanned".to_string(),
            mv("bytes_scanned", stats.bytes_scanned as i64, "size"),
        ),
    ]);
    sc::execute_plan_response::Metrics {
        metrics: vec![MetricObject {
            name: "weft".to_string(),
            plan_id: 0,
            parent: 0,
            execution_metrics,
        }],
    }
}

/// Materialize Arrow "view" layouts (`Utf8View`/`BinaryView`) to their canonical equivalents
/// (`Utf8`/`Binary`) before handing data to the client. DataFusion 54 uses `StringView` internally
/// for a real speedup on string-heavy work, but Spark Connect clients bundle older Arrow (Spark
/// 3.5's Arrow-Java, pyarrow < 16) that cannot decode view arrays — they fail with an opaque
/// `KeyError: 39` (39 = the `Utf8View` type id). A *drop-in* Spark replacement must return columns
/// any Spark client can read, so we cast at the output boundary only. Result sets here are tiny
/// (the ClickBench queries are `LIMIT 10/25`), so this costs nothing measurable and keeps the
/// internal StringView fast path intact. Top-level columns only — the 43 queries return scalars.
fn materialize_view_types(
    batch: &RecordBatch,
) -> std::result::Result<RecordBatch, weft_loom::arrow::error::ArrowError> {
    use weft_loom::arrow::array::ArrayRef;
    use weft_loom::arrow::compute::cast;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    let schema = batch.schema();
    let has_view = schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView));
    if !has_view {
        return Ok(batch.clone());
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (f, col) in schema.fields().iter().zip(batch.columns()) {
        let target = match f.data_type() {
            DataType::Utf8View => Some(DataType::Utf8),
            DataType::BinaryView => Some(DataType::Binary),
            _ => None,
        };
        match target {
            Some(t) => {
                cols.push(cast(col, &t)?);
                fields.push(Arc::new(Field::new(f.name(), t, f.is_nullable())));
            }
            None => {
                cols.push(col.clone());
                fields.push(f.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
}

/// Encode one record batch as a self-contained Arrow IPC stream (schema + batch), which is
/// exactly what `ExecutePlanResponse.arrow_batch.data` carries.
fn encode_ipc_stream(
    batch: &RecordBatch,
) -> std::result::Result<Vec<u8>, weft_loom::arrow::error::ArrowError> {
    let batch = materialize_view_types(batch)?;
    let mut buf = Vec::new();
    let schema = batch.schema();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Encode many record batches into one self-contained Arrow IPC stream (schema + batches), for a
/// `LocalRelation`. An empty result encodes just an empty schema.
fn encode_ipc_multi(
    batches: &[RecordBatch],
) -> std::result::Result<Vec<u8>, weft_loom::arrow::error::ArrowError> {
    use weft_loom::arrow::datatypes::Schema;
    let materialized: Vec<RecordBatch> = batches
        .iter()
        .map(materialize_view_types)
        .collect::<std::result::Result<_, _>>()?;
    let mut buf = Vec::new();
    let schema = materialized
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
        for b in &materialized {
            writer.write(b)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

/// A lenient default for a config key Spark hasn't set: boolean-ish flags → `"false"`, otherwise
/// the empty string — so client code that does `value.lower() == "true"` never hits `None`.
fn config_default(key: &str) -> String {
    if key.ends_with(".enabled") {
        "false".to_string()
    } else {
        String::new()
    }
}

/// Render record batches as a Spark-style box table for `DataFrame.show()` (`ShowString`). The
/// exact glyphs aren't asserted by the client — it just prints the returned string.
fn show_string(
    batches: &[RecordBatch],
    num_rows: i32,
    truncate: i32,
) -> std::result::Result<String, Status> {
    use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};

    let Some(schema) = batches.first().map(|b| b.schema()) else {
        return Ok("++\n++\n".to_string());
    };
    let headers: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let max_rows = if num_rows <= 0 {
        usize::MAX
    } else {
        num_rows as usize
    };
    let trunc = truncate.max(0) as usize;

    let opts = FormatOptions::default();
    let mut rows: Vec<Vec<String>> = Vec::new();
    'outer: for b in batches {
        let fmts = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Status::internal(e.to_string()))?;
        for r in 0..b.num_rows() {
            if rows.len() >= max_rows {
                break 'outer;
            }
            rows.push(
                fmts.iter()
                    .map(|f| {
                        let mut s = f.value(r).to_string();
                        if trunc > 0 && s.chars().count() > trunc {
                            s = format!(
                                "{}...",
                                &s.chars().take(trunc.saturating_sub(3)).collect::<String>()
                            );
                        }
                        s
                    })
                    .collect(),
            );
        }
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let rule = {
        let mut s = String::from("+");
        for w in &widths {
            s.push_str(&"-".repeat(w + 2));
            s.push('+');
        }
        s
    };
    let line = |cells: &[String]| {
        let mut s = String::from("|");
        for (i, c) in cells.iter().enumerate() {
            s.push_str(&format!(" {:>w$} |", c, w = widths[i]));
        }
        s
    };
    let mut out = String::new();
    out.push_str(&rule);
    out.push('\n');
    out.push_str(&line(&headers));
    out.push('\n');
    out.push_str(&rule);
    out.push('\n');
    for row in &rows {
        out.push_str(&line(row));
        out.push('\n');
    }
    out.push_str(&rule);
    out.push('\n');
    Ok(out)
}

/// Wrap a `ShowString` result string as the single-cell `show_string` relation PySpark expects.
fn show_string_batch(text: String) -> RecordBatch {
    use weft_loom::arrow::array::StringArray;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new(
        "show_string",
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![text]))])
        .expect("show_string batch")
}

/// The signed Arrow type a Spark-unrepresentable unsigned type maps to (widening to stay lossless).
fn signed_target(
    dt: &weft_loom::arrow::datatypes::DataType,
) -> Option<weft_loom::arrow::datatypes::DataType> {
    use weft_loom::arrow::datatypes::DataType::*;
    Some(match dt {
        UInt8 => Int16,
        UInt16 => Int32,
        UInt32 => Int64,
        UInt64 => Int64,
        _ => return None,
    })
}

fn signed_schema(
    schema: &weft_loom::arrow::datatypes::Schema,
) -> weft_loom::arrow::datatypes::SchemaRef {
    use weft_loom::arrow::datatypes::{Field, Schema};
    let fields = schema
        .fields()
        .iter()
        .map(|f| match signed_target(f.data_type()) {
            Some(t) => Arc::new(Field::new(f.name(), t, f.is_nullable())),
            None => f.clone(),
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Cast any unsigned-integer columns to signed so the result is Spark-representable.
fn signed_columns(batch: RecordBatch) -> std::result::Result<RecordBatch, Status> {
    use weft_loom::arrow::compute::cast;
    if batch
        .schema()
        .fields()
        .iter()
        .all(|f| signed_target(f.data_type()).is_none())
    {
        return Ok(batch);
    }
    let schema = signed_schema(batch.schema().as_ref());
    let cols = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| match signed_target(f.data_type()) {
            Some(t) => cast(c, &t).map_err(|e| Status::internal(format!("cast: {e}"))),
            None => Ok(c.clone()),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, cols).map_err(|e| Status::internal(format!("rebuild batch: {e}")))
}

fn err_to_status(e: Error) -> Status {
    let msg = e.to_string();
    match e {
        Error::Plan(_) => Status::invalid_argument(msg),
        Error::Unsupported(_) => Status::unimplemented(msg),
        Error::Execution(_) | Error::Io(_) => Status::internal(msg),
    }
}

fn replicated_tables_from_env() -> Vec<String> {
    std::env::var("WEFT_REPLICATED_TABLES")
        .ok()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Start the Spark Connect server and serve until the process is killed.
pub async fn serve(config: ServerConfig) -> Result<()> {
    let port = config.port;
    let ui_port = config.ui_port;
    let store = config
        .observability
        .clone()
        .unwrap_or_else(|| Arc::new(AppStateStore::new()));
    let mut cfg = config;
    cfg.observability = Some(store.clone());
    let service = WeftService::with_config(cfg);

    if let Some(ui_port) = ui_port {
        let ui_store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = weft_ui_server::serve(weft_ui_server::UiServerConfig {
                port: ui_port,
                store: ui_store,
            })
            .await
            {
                eprintln!("weft ui server error: {e}");
            }
        });
        eprintln!("Weft UI listening on http://0.0.0.0:{ui_port}");
    }

    serve_instance(service, port).await
}

/// Serve a pre-built service instance (tests with a seeded engine).
pub async fn serve_instance(service: WeftService, port: u16) -> Result<()> {
    let addr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| Error::Io(format!("bad listen addr: {e}")))?;
    let grpc = SparkConnectServiceServer::new(service)
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    tonic::transport::Server::builder()
        .add_service(grpc)
        .serve(addr)
        .await
        .map_err(|e| Error::Io(format!("server error: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod view_materialize_tests {
    use super::*;
    use weft_loom::arrow::array::{Array, Int64Array, StringViewArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn utf8view_is_materialized_to_utf8_preserving_values() {
        // A batch as DataFusion 54 hands it back: a StringView column (the layout Spark Connect
        // clients with older Arrow cannot decode) next to a passthrough Int64.
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("n", DataType::Int64, false),
        ]));
        let s = StringViewArray::from(vec![Some("google"), None, Some("yandex")]);
        let n = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(s), Arc::new(n)]).unwrap();

        let out = materialize_view_types(&batch).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);

        // The encoder must also round-trip without a view type leaking into the IPC bytes.
        let bytes = encode_ipc_stream(&batch).unwrap();
        let mut rdr = StreamReader::try_new(bytes.as_slice(), None).unwrap();
        let decoded = rdr.next().unwrap().unwrap();
        assert_eq!(decoded.schema().field(0).data_type(), &DataType::Utf8);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<weft_loom::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "google");
        assert!(col.is_null(1));
        assert_eq!(col.value(2), "yandex");
    }

    #[test]
    fn batch_without_view_types_is_untouched() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let out = materialize_view_types(&batch).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(out.num_rows(), 2);
    }
}
