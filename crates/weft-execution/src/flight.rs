//! Distributed execution over Arrow Flight.
//!
//! A [`Worker`] is an Arrow Flight server. Its `do_get` ticket is one of three things
//! (see [`crate::shuffle::protocol`]):
//!
//! - a legacy raw-SQL string — run it and stream the result (the single-stage MVP);
//! - a [`StageTicket`] — run a stage. A *leaf* stage (no upstreams) runs its SQL on local
//!   data, hash-partitions the output into per-downstream buckets, caches them, and returns an
//!   empty stream; a *consumer* stage (with upstreams) pulls its bucket from every upstream,
//!   registers it as `shuffle_input`, runs its SQL, and streams the result back;
//! - a [`ShuffleReadTicket`] — stream one cached bucket of a prior stage's output.
//!
//! This is the two-stage `partial-agg → hash shuffle → final-agg` shape; the driver in
//! [`crate::driver`] orchestrates it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};
use weft_common::{Error, Result};
use weft_loom::arrow::datatypes::{Schema, SchemaRef};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

use crate::shuffle::protocol::{self, ShuffleExchangeHeader, ShuffleReadTicket, StageTicket};
use crate::shuffle::spill::{BucketCache, SpillStore};
use crate::shuffle::{hash_partition, SHUFFLE_INPUT_TABLE};

/// Flight `do_action` type: evict all cached stage outputs on this worker.
pub const ACTION_CLEAR_STAGES: &str = "clear_stages";
/// Flight `do_action` type: register session UDF definitions (JSON payload).
pub const ACTION_REGISTER_UDFS: &str = "register_udfs";
/// Flight `do_action` type: liveness probe (driver heartbeats).
pub const ACTION_HEALTH: &str = "health";
/// Flight `do_action` type: liveness + slot probe.
pub const ACTION_HEARTBEAT: &str = "heartbeat";
/// Flight `do_action` type: accept/report simple task status payloads.
pub const ACTION_TASK_STATUS: &str = "task_status";

/// One stage's cached output: schema + partitioned buckets (memory or spilled).
type CachedStage = (SchemaRef, BucketCache);

/// Per-stage cached output, partitioned into buckets (one per downstream worker).
type StageCache = Arc<Mutex<HashMap<u32, CachedStage>>>;

/// A Flight worker that runs stages on its local engine and serves shuffle buckets.
pub struct Worker {
    engine: Arc<Engine>,
    stage_outputs: StageCache,
    spill: Option<SpillStore>,
    task_slots: usize,
    active_tasks: Arc<Mutex<usize>>,
    last_task_status: Arc<Mutex<Option<Vec<u8>>>>,
}

impl Worker {
    /// Wrap an engine as a worker.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            stage_outputs: Arc::new(Mutex::new(HashMap::new())),
            spill: SpillStore::from_env(),
            task_slots: worker_task_slots(),
            active_tasks: Arc::new(Mutex::new(0)),
            last_task_status: Arc::new(Mutex::new(None)),
        }
    }

    fn clear_stages(&self) {
        if let Some(spill) = &self.spill {
            let guard = self.stage_outputs.lock().expect("stage cache poisoned");
            for stage_id in guard.keys() {
                spill.clear_stage(*stage_id);
            }
        }
        self.stage_outputs
            .lock()
            .expect("stage cache poisoned")
            .clear();
    }

    fn active_task_count(&self) -> usize {
        *self.active_tasks.lock().expect("task counter poisoned")
    }

    fn heartbeat_payload(&self) -> String {
        serde_json::json!({
            "ok": true,
            "slots_total": self.task_slots,
            "slots_used": self.active_task_count(),
        })
        .to_string()
    }

    fn task_status_payload(&self) -> String {
        let last_task_status = self
            .last_task_status
            .lock()
            .expect("task status poisoned")
            .as_ref()
            .map(|body| String::from_utf8_lossy(body).into_owned());
        serde_json::json!({
            "ok": true,
            "slots_total": self.task_slots,
            "slots_used": self.active_task_count(),
            "last_task_status": last_task_status,
        })
        .to_string()
    }

    fn try_acquire_task_slot(&self) -> std::result::Result<TaskSlotGuard, Status> {
        let mut active = self.active_tasks.lock().expect("task counter poisoned");
        if *active >= self.task_slots {
            return Err(Status::resource_exhausted(format!(
                "no task slots available ({}/{})",
                *active, self.task_slots
            )));
        }
        *active += 1;
        Ok(TaskSlotGuard {
            active_tasks: self.active_tasks.clone(),
        })
    }
}

struct TaskSlotGuard {
    active_tasks: Arc<Mutex<usize>>,
}

impl Drop for TaskSlotGuard {
    fn drop(&mut self) {
        let mut active = self.active_tasks.lock().expect("task counter poisoned");
        *active = active.saturating_sub(1);
    }
}

/// Number of concurrent stage tasks this worker should admit.
pub fn worker_task_slots() -> usize {
    std::env::var("WEFT_WORKER_TASK_SLOTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .max(1)
}

type FlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

fn unimpl<T>(what: &str) -> std::result::Result<Response<T>, Status> {
    Err(Status::unimplemented(format!(
        "flight {what} not implemented"
    )))
}

/// Build a Flight `do_get` response stream from a set of record batches.
fn batches_to_stream(batches: Vec<RecordBatch>) -> FlightStream<FlightData> {
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => Arc::new(Schema::empty()),
    };
    let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
    FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(input)
        .map_err(|e| Status::internal(e.to_string()))
        .boxed()
}

fn action_response(body: impl Into<Vec<u8>>) -> Response<FlightStream<arrow_flight::Result>> {
    let body = arrow_flight::Result {
        body: body.into().into(),
    };
    Response::new(futures::stream::iter(vec![Ok(body)]).boxed())
}

impl Worker {
    /// Run a [`StageTicket`]. First, if it has upstreams, pull this worker's bucket of each upstream
    /// stage from every worker and register them as `shuffle_input` (one upstream) or
    /// `shuffle_input_{i}` (the i-th of several — e.g. a shuffle join's two sides). Then run the
    /// stage SQL. If `produce` is set, hash-partition the result by `hash_key_cols` and cache it for
    /// downstreams (returning empty); otherwise return the result (the output stage). A stage can
    /// both consume *and* produce — an intermediate stage of a multi-shuffle DAG.
    async fn run_stage(&self, t: StageTicket) -> std::result::Result<Vec<RecordBatch>, Status> {
        // Pull + register each upstream's bucket (no-op for a leaf).
        let single = t.upstream_stage_ids.len() == 1;
        for (i, &up_stage) in t.upstream_stage_ids.iter().enumerate() {
            let mut input = Vec::new();
            for ep in &t.upstream_endpoints {
                let part = pull_bucket(ep.clone(), up_stage, t.partition_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                input.extend(part);
            }
            let name = if single {
                SHUFFLE_INPUT_TABLE.to_string()
            } else {
                format!("{SHUFFLE_INPUT_TABLE}_{i}")
            };
            self.engine
                .register_batches(&name, input)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        if t.produce {
            // Producer: capture the output schema up front so an empty bucket can still be served
            // typed, then run, hash-partition, and cache for downstreams.
            let schema = self
                .engine
                .schema(&t.stage_sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let batches = self
                .engine
                .sql(&t.stage_sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let key_cols: Vec<usize> = t.hash_key_cols.iter().map(|&c| c as usize).collect();
            let buckets = hash_partition(&batches, &key_cols, t.num_partitions as usize)
                .map_err(|e| Status::internal(e.to_string()))?;
            let cache =
                BucketCache::maybe_spill(schema.clone(), buckets, t.stage_id, self.spill.as_ref())
                    .map_err(|e| Status::internal(e.to_string()))?;
            self.stage_outputs
                .lock()
                .expect("stage cache poisoned")
                .insert(t.stage_id, (schema, cache));
            Ok(Vec::new())
        } else {
            // Output stage: run and return the result.
            self.engine
                .sql(&t.stage_sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))
        }
    }

    /// Serve one cached shuffle bucket. An empty bucket is served as a single schema-carrying
    /// empty batch (never a truly empty stream) so the consumer can always register the input
    /// table — important for shuffle joins where some key buckets legitimately have no rows.
    fn read_shuffle(&self, r: ShuffleReadTicket) -> Vec<RecordBatch> {
        let guard = self.stage_outputs.lock().expect("stage cache poisoned");
        let Some((schema, cache)) = guard.get(&r.stage_id) else {
            return Vec::new();
        };
        let batches = cache.read_partition(r.target_partition as usize);
        // Empty in-memory buckets carry no schema; consumers still need typed `shuffle_input`
        // (e.g. TPC-H Q8 with few group keys and many partitions).
        if batches.is_empty() {
            return vec![RecordBatch::new_empty(schema.clone())];
        }
        batches
    }

    /// Append one pushed shuffle partition to the stage cache so future pull-based
    /// `ShuffleReadTicket`s observe the same data.
    fn cache_pushed_partition(
        &self,
        header: ShuffleExchangeHeader,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let mut guard = self.stage_outputs.lock().expect("stage cache poisoned");
        match guard.get_mut(&header.stage_id) {
            Some((existing_schema, cache)) => {
                if existing_schema.as_ref() != schema.as_ref() {
                    return Err(Error::Execution(format!(
                        "do_exchange schema mismatch for stage {} partition {}",
                        header.stage_id, header.partition_id
                    )));
                }
                cache.append_partition(
                    existing_schema.clone(),
                    header.stage_id,
                    header.partition_id,
                    batches,
                    self.spill.as_ref(),
                )
            }
            None => {
                let cache = BucketCache::from_partition(
                    schema.clone(),
                    header.stage_id,
                    header.partition_id,
                    batches,
                    self.spill.as_ref(),
                )?;
                guard.insert(header.stage_id, (schema, cache));
                Ok(())
            }
        }
    }
}

#[tonic::async_trait]
impl FlightService for Worker {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;
    type DoExchangeStream = FlightStream<FlightData>;

    /// Dispatch on the ticket kind: legacy SQL, a stage, or a shuffle-bucket read.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let bytes = request.into_inner().ticket.to_vec();
        let ticket = protocol::decode_ticket(&bytes)
            .map_err(|e| Status::invalid_argument(format!("decode ticket: {e}")))?;
        let batches = match ticket {
            protocol::Ticket::Sql(sql) => self
                .engine
                .sql(&sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            protocol::Ticket::Stage(t) => {
                let _slot = self.try_acquire_task_slot()?;
                self.run_stage(t).await?
            }
            protocol::Ticket::ShuffleRead(r) => self.read_shuffle(r),
        };
        Ok(Response::new(batches_to_stream(batches)))
    }

    async fn handshake(
        &self,
        _r: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        unimpl("handshake")
    }
    async fn list_flights(
        &self,
        _r: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        unimpl("list_flights")
    }
    async fn get_flight_info(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        unimpl("get_flight_info")
    }
    async fn poll_flight_info(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        unimpl("poll_flight_info")
    }
    async fn get_schema(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        unimpl("get_schema")
    }
    async fn do_put(
        &self,
        _r: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        unimpl("do_put")
    }
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            ACTION_CLEAR_STAGES => {
                self.clear_stages();
                Ok(action_response(b"ok".to_vec()))
            }
            ACTION_REGISTER_UDFS => {
                let payload = String::from_utf8_lossy(&action.body);
                self.engine
                    .register_udfs_json(&payload)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(action_response(b"ok".to_vec()))
            }
            ACTION_HEALTH => Ok(action_response(b"ok".to_vec())),
            ACTION_HEARTBEAT => Ok(action_response(self.heartbeat_payload().into_bytes())),
            ACTION_TASK_STATUS => {
                if !action.body.is_empty() {
                    *self.last_task_status.lock().expect("task status poisoned") =
                        Some(action.body.to_vec());
                }
                Ok(action_response(self.task_status_payload().into_bytes()))
            }
            other => Err(Status::unimplemented(format!(
                "flight do_action `{other}` not implemented"
            ))),
        }
    }
    async fn list_actions(
        &self,
        _r: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        unimpl("list_actions")
    }
    /// Streaming shuffle exchange. The first frame is a metadata-only exchange header
    /// (`stage_id` + `partition_id`), followed by normal Arrow IPC FlightData frames. The received
    /// batches are appended into the same stage cache used by pull-based `ShuffleReadTicket`.
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("do_exchange header: {e}")))?
            .ok_or_else(|| Status::invalid_argument("do_exchange missing header"))?;
        let header_bytes = if first.app_metadata.is_empty() {
            first.data_header.as_ref()
        } else {
            first.app_metadata.as_ref()
        };
        let header = ShuffleExchangeHeader::decode(header_bytes)
            .map_err(|e| Status::invalid_argument(format!("decode do_exchange header: {e}")))?;

        let mut rb = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|s| FlightError::Tonic(Box::new(s))),
        );
        let mut batches = Vec::new();
        while let Some(batch) = rb.next().await {
            batches.push(batch.map_err(|e| Status::internal(format!("flight decode: {e}")))?);
        }
        let schema = batches
            .first()
            .map(|b| b.schema())
            .or_else(|| rb.schema().cloned())
            .ok_or_else(|| Status::invalid_argument("do_exchange missing Arrow schema"))?;

        self.cache_pushed_partition(header, schema, batches)
            .map_err(|e| Status::internal(e.to_string()))?;

        let ack = FlightData {
            flight_descriptor: None,
            data_header: Vec::new().into(),
            app_metadata: b"ok".to_vec().into(),
            data_body: Vec::new().into(),
        };
        Ok(Response::new(futures::stream::iter(vec![Ok(ack)]).boxed()))
    }
}

/// Serve a worker on `0.0.0.0:port` until the process exits.
pub async fn serve_worker(port: u16, engine: Arc<Engine>) -> Result<()> {
    let addr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| Error::Io(format!("bad worker addr: {e}")))?;
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(Worker::new(engine)))
        .serve(addr)
        .await
        .map_err(|e| Error::Io(format!("worker serve: {e}")))?;
    Ok(())
}

/// Connect to a worker and run one `do_get` with retries on transient errors.
async fn do_get_batches(endpoint: String, ticket_bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
    const MAX_TRIES: u32 = 3;
    let mut last_err = None;
    for attempt in 0..MAX_TRIES {
        match do_get_batches_once(endpoint.clone(), ticket_bytes.clone()).await {
            Ok(b) => return Ok(b),
            Err(e) => {
                let retryable = e.to_string().contains("connect worker")
                    || e.to_string().contains("do_get:")
                    || e.to_string().contains("Unavailable");
                if !retryable || attempt + 1 == MAX_TRIES {
                    return Err(e);
                }
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)))
                    .await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Execution("do_get failed".into())))
}

/// Connect to a Flight worker with short timeouts so CI port conflicts fail fast instead of
/// hanging on the default TCP SYN retry window (~minutes).
async fn connect_flight(
    endpoint: String,
) -> Result<FlightServiceClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .map_err(|e| Error::Io(format!("endpoint: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| Error::Io(format!("connect worker: {e}")))?;
    Ok(FlightServiceClient::new(channel))
}

async fn do_get_batches_once(endpoint: String, ticket_bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
    let mut client = connect_flight(endpoint).await?;
    let ticket = Ticket {
        ticket: ticket_bytes.into(),
    };
    let stream = client
        .do_get(ticket)
        .await
        .map_err(|e| Error::Execution(format!("do_get: {}", e.message())))?
        .into_inner();

    let mut rb = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|s| FlightError::Tonic(Box::new(s))),
    );
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        out.push(batch.map_err(|e| Error::Execution(format!("flight decode: {e}")))?);
    }
    // The Flight encoder sends the schema but drops zero-row batches, so an empty result arrives as
    // no batches at all. Recover a schema-carrying empty batch from the stream so a downstream
    // consumer can still register the (empty) shuffle input — otherwise an all-empty bucket set
    // would surface as "no batches".
    if out.is_empty() {
        if let Some(schema) = rb.schema() {
            out.push(RecordBatch::new_empty(schema.clone()));
        }
    }
    Ok(out)
}

/// Evict all cached shuffle stages on a worker (post-query cleanup).
pub async fn clear_worker_stages(endpoint: String) -> Result<()> {
    do_action(endpoint, ACTION_CLEAR_STAGES, b"").await
}

/// Push UDF definitions to a worker before stage execution.
pub async fn sync_udfs_to_worker(endpoint: String, udf_json: &str) -> Result<()> {
    do_action(endpoint, ACTION_REGISTER_UDFS, udf_json.as_bytes()).await
}

/// Liveness probe — returns `Ok(())` when the worker responds to `ACTION_HEALTH`.
pub async fn health_check_worker(endpoint: String) -> Result<()> {
    do_action(endpoint, ACTION_HEALTH, b"").await
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerHeartbeat {
    pub slots_total: Option<usize>,
    pub slots_used: Option<usize>,
}

impl WorkerHeartbeat {
    pub fn has_available_slot(&self) -> bool {
        match (self.slots_total, self.slots_used) {
            (Some(total), Some(used)) => used < total,
            _ => true,
        }
    }
}

/// Heartbeat probe. New workers return slot metadata; older `ok`-only workers are accepted.
pub async fn heartbeat_worker(endpoint: String) -> Result<WorkerHeartbeat> {
    let bodies = do_action_collect(endpoint, ACTION_HEARTBEAT, b"").await?;
    Ok(parse_heartbeat_bodies(&bodies))
}

/// Send or fetch the worker's simple task status payload.
pub async fn task_status_worker(endpoint: String, payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    do_action_collect(endpoint, ACTION_TASK_STATUS, payload).await
}

async fn do_action(endpoint: String, action_type: &str, body: &[u8]) -> Result<()> {
    do_action_collect(endpoint, action_type, body).await?;
    Ok(())
}

async fn do_action_collect(
    endpoint: String,
    action_type: &str,
    body: &[u8],
) -> Result<Vec<Vec<u8>>> {
    let mut client = connect_flight(endpoint).await?;
    let action = Action {
        r#type: action_type.to_string(),
        body: body.to_vec().into(),
    };
    let mut stream = client
        .do_action(action)
        .await
        .map_err(|e| Error::Execution(format!("do_action: {}", e.message())))?
        .into_inner();
    let mut bodies = Vec::new();
    while let Some(item) = stream.next().await {
        bodies.push(
            item.map_err(|e| Error::Execution(format!("do_action stream: {e}")))?
                .body
                .to_vec(),
        );
    }
    Ok(bodies)
}

fn parse_heartbeat_bodies(bodies: &[Vec<u8>]) -> WorkerHeartbeat {
    bodies
        .iter()
        .find_map(|body| parse_heartbeat_payload(body))
        .unwrap_or_default()
}

fn parse_heartbeat_payload(body: &[u8]) -> Option<WorkerHeartbeat> {
    let text = std::str::from_utf8(body).ok()?.trim();
    if text.is_empty() || text == "ok" {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    Some(WorkerHeartbeat {
        slots_total: value
            .get("slots_total")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize),
        slots_used: value
            .get("slots_used")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize),
    })
}

/// Driver: send raw `sql` to a worker over Flight and collect the result (single-stage path).
pub async fn query_worker(endpoint: String, sql: &str) -> Result<Vec<RecordBatch>> {
    do_get_batches(endpoint, sql.as_bytes().to_vec()).await
}

/// Driver: run a [`StageTicket`] on a worker and collect whatever it streams back.
pub async fn run_stage_on_worker(
    endpoint: String,
    ticket: StageTicket,
) -> Result<Vec<RecordBatch>> {
    do_get_batches(endpoint, ticket.to_ticket_bytes()).await
}

/// Pull one shuffle bucket (`target_partition`) of `stage_id` from a worker.
pub async fn pull_bucket(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
) -> Result<Vec<RecordBatch>> {
    pull_bucket_with_retry(endpoint, stage_id, target_partition).await
}

/// Pull a shuffle bucket with transient retries (shuffle durability on read path).
pub async fn pull_bucket_with_retry(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
) -> Result<Vec<RecordBatch>> {
    const MAX_TRIES: u32 = 3;
    let ticket = ShuffleReadTicket {
        stage_id,
        target_partition,
    };
    let bytes = ticket.to_ticket_bytes();
    let mut last = None;
    for attempt in 0..MAX_TRIES {
        match do_get_batches(endpoint.clone(), bytes.clone()).await {
            Ok(b) => return Ok(b),
            Err(e) if is_pull_retryable(&e) && attempt + 1 < MAX_TRIES => {
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt as u64 + 1)))
                    .await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Execution("pull_bucket failed".into())))
}

/// Push one shuffle bucket to a worker over Flight `do_exchange`.
///
/// The receiver appends the batches into its local cache under `(stage_id, target_partition)`, so
/// the same data remains readable via [`pull_bucket`] as a fallback path.
pub async fn push_bucket(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    push_bucket_with_schema(endpoint, stage_id, target_partition, schema, batches).await
}

/// Push one shuffle bucket with an explicit schema, useful when the bucket has zero rows.
pub async fn push_bucket_with_schema(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    const MAX_TRIES: u32 = 3;
    let mut last = None;
    for attempt in 0..MAX_TRIES {
        match push_bucket_once(
            endpoint.clone(),
            stage_id,
            target_partition,
            schema.clone(),
            batches.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if is_pull_retryable(&e) && attempt + 1 < MAX_TRIES => {
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt as u64 + 1)))
                    .await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Execution("push_bucket failed".into())))
}

async fn push_bucket_once(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let header = exchange_header_frame(stage_id, target_partition);
    let mut frames = vec![header];
    let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
    let mut encoded = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(input);
    while let Some(frame) = encoded.next().await {
        frames.push(frame.map_err(|e| Error::Execution(format!("flight encode: {e}")))?);
    }

    let mut client = connect_flight(endpoint).await?;
    let mut stream = client
        .do_exchange(futures::stream::iter(frames))
        .await
        .map_err(|e| Error::Execution(format!("do_exchange: {}", e.message())))?
        .into_inner();
    while let Some(item) = stream.next().await {
        item.map_err(|e| Error::Execution(format!("do_exchange stream: {e}")))?;
    }
    Ok(())
}

fn exchange_header_frame(stage_id: u32, partition_id: u32) -> FlightData {
    let header = ShuffleExchangeHeader {
        stage_id,
        partition_id,
    }
    .encode();
    FlightData {
        flight_descriptor: None,
        data_header: Vec::new().into(),
        app_metadata: header.to_vec().into(),
        data_body: Vec::new().into(),
    }
}

fn is_pull_retryable(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("connect")
        || s.contains("unavailable")
        || s.contains("do_get")
        || s.contains("do_exchange")
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_loom::arrow::array::Int32Array;
    use weft_loom::arrow::datatypes::{DataType, Field};

    #[test]
    fn heartbeat_payload_parses_slots() {
        let heartbeat =
            parse_heartbeat_payload(br#"{"ok":true,"slots_total":4,"slots_used":2}"#).unwrap();
        assert_eq!(heartbeat.slots_total, Some(4));
        assert_eq!(heartbeat.slots_used, Some(2));
        assert!(heartbeat.has_available_slot());

        let full =
            parse_heartbeat_payload(br#"{"ok":true,"slots_total":4,"slots_used":4}"#).unwrap();
        assert!(!full.has_available_slot());
    }

    #[test]
    fn ok_heartbeat_is_backward_compatible() {
        let heartbeat = parse_heartbeat_bodies(&[b"ok".to_vec()]);
        assert_eq!(heartbeat, WorkerHeartbeat::default());
        assert!(heartbeat.has_available_slot());
    }

    #[tokio::test]
    async fn distributed_single_stage_roundtrip() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        // Retry until the worker is up and the distributed query returns.
        let endpoint = format!("http://127.0.0.1:{port}");
        let mut batches = None;
        for _ in 0..50 {
            match query_worker(endpoint.clone(), "SELECT 21 + 21 AS answer").await {
                Ok(b) => {
                    batches = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let batches = batches.expect("worker did not become ready / query failed");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        // `21 + 21` is Spark `IntegerType` (Int32) — weft types integer literals as Int32 to match
        // Spark (real PySpark `SELECT 21 + 21` → IntegerType), not DataFusion's native i64.
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32")
            .value(0);
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn stage_ticket_runs_as_leaf() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        // A leaf stage caches and returns empty; assert it doesn't error and returns 0 rows.
        let ticket = StageTicket {
            stage_id: 0,
            partition_id: 0,
            num_partitions: 1,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS k, 2 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
        };
        let mut out = None;
        for _ in 0..50 {
            match run_stage_on_worker(endpoint.clone(), ticket.clone()).await {
                Ok(b) => {
                    out = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let out = out.expect("worker not ready");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        // The cached bucket 0 should now be pullable and contain the row.
        let pulled = pull_bucket(endpoint, 0, 0).await.unwrap();
        assert_eq!(pulled.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn empty_shuffle_bucket_carries_producer_schema() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        // One row hashes into exactly one of three buckets; the other two are empty but must still
        // expose the producer schema so consumers can plan `SELECT k FROM shuffle_input`.
        let ticket = StageTicket {
            stage_id: 7,
            partition_id: 0,
            num_partitions: 3,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS k, 2 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
        };
        for _ in 0..50 {
            if run_stage_on_worker(endpoint.clone(), ticket.clone())
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let mut empty_bucket = None;
        for bucket in [1u32, 2] {
            for _ in 0..50 {
                match pull_bucket(endpoint.clone(), 7, bucket).await {
                    Ok(b) if !b.is_empty() => {
                        empty_bucket = Some((bucket, b));
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                }
            }
            if empty_bucket.is_some() {
                break;
            }
        }
        let (bucket, batches) = empty_bucket.expect("expected an empty typed bucket");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(batches[0].schema().field(0).name(), "k");

        // Consumer over an empty upstream bucket must still resolve column names.
        let consume = StageTicket {
            stage_id: 8,
            partition_id: bucket,
            num_partitions: 3,
            upstream_endpoints: vec![endpoint.clone()],
            stage_sql: "SELECT k, sum(v) AS s FROM shuffle_input GROUP BY k".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![],
            upstream_stage_ids: vec![7],
            produce: false,
        };
        let mut out = None;
        for _ in 0..50 {
            match run_stage_on_worker(endpoint.clone(), consume.clone()).await {
                Ok(b) => {
                    out = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        out.expect("consumer over empty typed bucket should plan and run");
    }

    #[tokio::test]
    async fn do_exchange_pushes_partition_then_pull_reads_it() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let mut pushed = false;
        for _ in 0..50 {
            match push_bucket_with_schema(
                endpoint.clone(),
                99,
                2,
                schema.clone(),
                vec![batch.clone()],
            )
            .await
            {
                Ok(()) => {
                    pushed = true;
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        assert!(pushed, "worker did not accept do_exchange push");

        let pulled = pull_bucket(endpoint, 99, 2).await.unwrap();
        assert_eq!(pulled.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let values = pulled[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(2), 30);
    }
}
