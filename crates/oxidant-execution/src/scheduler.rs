//! Task scheduling with fault tolerance: retries, alternate workers, health checks,
//! speculative execution, and stage recomputation.

use std::sync::Arc;
use std::time::Duration;

use oxidant_common::{Error, Result};
use oxidant_loom::arrow::record_batch::RecordBatch;

use crate::driver::{forward_upstreams, ExchangeMode, StageDef};
use crate::flight::{
    health_check_worker, heartbeat_worker, pull_bucket_with_retry, run_stage_on_worker,
};
use crate::lineage::SharedLineage;
use crate::membership::ClusterMembership;
use crate::shuffle::protocol::StageTicket;

/// Max task attempts per endpoint before trying alternates (env: `OXIDANT_TASK_MAX_RETRIES`, default 3).
pub fn task_max_retries() -> u32 {
    std::env::var("OXIDANT_TASK_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .unwrap_or(3)
}

/// Straggler threshold before launching a speculative duplicate task (ms).
pub fn speculative_timeout_ms() -> u64 {
    std::env::var("OXIDANT_SPECULATIVE_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000)
}

/// Whether an execution error is worth retrying on another worker.
///
/// KAN-15: a deadline/timeout on a stage `do_get` is NOT retryable — the original attempt
/// keeps running server-side, so retrying duplicates it (the retry storm that exhausted
/// worker task slots). Genuine connect/refused/unavailable failures stay retryable: they
/// mean the stage never started.
pub fn is_retryable(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    if s.contains("connect worker") {
        return true;
    }
    if s.contains("deadline") || s.contains("timed out") || s.contains("timeout") {
        return false;
    }
    s.contains("unavailable")
        || s.contains("connection")
        || s.contains("reset")
        || s.contains("broken pipe")
        // tonic::transport::Error displays only as "transport error"; with or without the
        // source chain from `status_detail`, that string must trigger channel eviction +
        // retry (SF100 TPC-DS Q10: 16 failed tasks, zero retries).
        || s.contains("transport")
        || s.contains("goaway")
        || s.contains("incomplete message")
        || s.contains("health check failed")
        || s.contains("shuffle")
        || s.contains("empty bucket")
        || s.contains("no task slots")
}

/// Whether the error likely means an upstream producer bucket is missing (recompute candidate).
pub fn needs_upstream_recompute(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("shuffle") || s.contains("empty bucket") || s.contains("no batches")
}

/// Run a stage ticket on `primary`, retrying on transient errors and falling back to alternate
/// workers from `membership` when the primary is unreachable. Records successful producers in
/// `lineage`. On shuffle read failure, recomputes missing upstream producer stages.
pub async fn run_stage_with_retry(
    membership: &Arc<dyn ClusterMembership>,
    primary: String,
    ticket: StageTicket,
    lineage: &SharedLineage,
    stages: &std::collections::HashMap<u32, StageDef>,
) -> Result<Vec<RecordBatch>> {
    if speculative_enabled() {
        return run_stage_speculative(membership, primary, ticket, lineage, stages).await;
    }
    run_stage_inner(membership, primary, ticket, lineage, stages).await
}

fn run_stage_inner<'a>(
    membership: &'a Arc<dyn ClusterMembership>,
    primary: String,
    ticket: StageTicket,
    lineage: &'a SharedLineage,
    stages: &'a std::collections::HashMap<u32, StageDef>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
    Box::pin(run_stage_inner_impl(
        membership, primary, ticket, lineage, stages,
    ))
}

fn speculative_enabled() -> bool {
    std::env::var("OXIDANT_SPECULATIVE")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Launch primary task; if it exceeds the straggler threshold, race a backup on another worker.
async fn run_stage_speculative(
    membership: &Arc<dyn ClusterMembership>,
    primary: String,
    ticket: StageTicket,
    lineage: &SharedLineage,
    stages: &std::collections::HashMap<u32, StageDef>,
) -> Result<Vec<RecordBatch>> {
    let timeout = Duration::from_millis(speculative_timeout_ms());
    let membership2 = membership.clone();
    let ticket2 = ticket.clone();
    let lineage2 = lineage.clone();
    let primary2 = primary.clone();
    let stages2 = stages.clone();

    let primary_fut =
        async move { run_stage_inner(&membership2, primary2, ticket2, &lineage2, &stages2).await };

    let membership3 = membership.clone();
    let ticket3 = ticket.clone();
    let lineage3 = lineage.clone();
    let stages3 = stages.clone();
    let primary3 = primary.clone();

    let backup_fut = async move {
        tokio::time::sleep(timeout).await;
        let alts: Vec<_> = membership3
            .endpoints()
            .into_iter()
            .filter(|e| e != &primary3)
            .collect();
        for alt in alts {
            if worker_accepts_task(alt.clone()).await {
                return run_stage_inner(&membership3, alt, ticket3.clone(), &lineage3, &stages3)
                    .await;
            }
        }
        Err(Error::Execution(
            "speculative backup: no healthy alternate".into(),
        ))
    };

    tokio::select! {
        r = primary_fut => r,
        r = backup_fut => r,
    }
}

async fn run_stage_inner_impl(
    membership: &Arc<dyn ClusterMembership>,
    primary: String,
    ticket: StageTicket,
    lineage: &SharedLineage,
    stages: &std::collections::HashMap<u32, StageDef>,
) -> Result<Vec<RecordBatch>> {
    let max = task_max_retries();
    let mut tried = vec![primary.clone()];
    let mut last_err = None;

    for attempt in 0..max {
        // Skip the slot probe on the first attempt: a saturated worker queues the task
        // server-side (`acquire_task_slot`, bounded by `OXIDANT_TASK_SLOT_WAIT_MS`) instead of
        // rejecting it, so the probe is a wasted RTT on the happy path. Retries and
        // alternate-endpoint fallbacks still probe — skipping a genuinely full or dead
        // worker there saves the whole stage-dispatch round trip.
        if attempt > 0 && !worker_accepts_task(primary.clone()).await {
            last_err = Some(Error::Execution(format!(
                "worker has no free task slots: {primary}"
            )));
            if attempt + 1 < max {
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                continue;
            }
            break;
        }
        match run_stage_on_worker(primary.clone(), ticket.clone()).await {
            Ok(b) => {
                if ticket.produce {
                    lineage.record_producer(ticket.stage_id, ticket.partition_id, &primary);
                }
                return Ok(b);
            }
            Err(e) if is_retryable(&e) && attempt + 1 < max => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                continue;
            }
            Err(e) if is_retryable(&e) => last_err = Some(e),
            Err(e) => return Err(e),
        }
    }

    // Shuffle durability: recompute upstream producers when consumer can't read buckets.
    if !ticket.upstream_stage_ids.is_empty()
        && last_err.as_ref().is_some_and(needs_upstream_recompute)
    {
        if let Err(e) = recompute_upstream_producers(membership, &ticket, lineage, stages).await {
            last_err = Some(e);
        } else if worker_accepts_task(primary.clone()).await {
            match run_stage_on_worker(primary.clone(), ticket.clone()).await {
                Ok(b) => return Ok(b),
                Err(e) => last_err = Some(e),
            }
        } else {
            last_err = Some(Error::Execution(format!(
                "worker has no free task slots: {primary}"
            )));
        }
    }

    // Try alternate healthy workers not yet attempted.
    for alt in membership.endpoints() {
        if tried.contains(&alt) {
            continue;
        }
        if !worker_accepts_task(alt.clone()).await {
            continue;
        }
        tried.push(alt.clone());
        match run_stage_on_worker(alt, ticket.clone()).await {
            Ok(b) => {
                if ticket.produce {
                    lineage.record_producer(
                        ticket.stage_id,
                        ticket.partition_id,
                        tried.last().unwrap(),
                    );
                }
                return Ok(b);
            }
            Err(e) if is_retryable(&e) => last_err = Some(e),
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| Error::Execution("stage task failed on all workers".into())))
}

/// Re-run producer stages for each upstream bucket this consumer needs.
async fn recompute_upstream_producers(
    membership: &Arc<dyn ClusterMembership>,
    consumer: &StageTicket,
    lineage: &SharedLineage,
    stages: &std::collections::HashMap<u32, StageDef>,
) -> Result<()> {
    // AQE: a coalesced consumer reads a whole modulus class of buckets, not just its own —
    // the readability probe must cover the same set the consumer will pull.
    let read_mod = if consumer.coalesce_read_modulus == 0 {
        consumer.num_partitions
    } else {
        consumer.coalesce_read_modulus.min(consumer.num_partitions)
    };
    let needed = crate::aqe::coalesced_read_buckets(
        consumer.num_partitions,
        read_mod,
        consumer.partition_id,
    );
    for &up_stage in &consumer.upstream_stage_ids {
        let stage_def = stages
            .get(&up_stage)
            .ok_or_else(|| Error::Execution(format!("recompute: unknown stage {up_stage}")))?;
        // A Forward upstream is produced once, on the first endpoint — the only endpoint
        // its consumers read (`StageTicket::forward_upstream_stage_ids`); probing the rest
        // would only see their placeholder buckets.
        let probe_endpoints = if stage_def.exchange == ExchangeMode::Forward {
            &consumer.upstream_endpoints[..consumer.upstream_endpoints.len().min(1)]
        } else {
            &consumer.upstream_endpoints[..]
        };
        for (i, ep) in probe_endpoints.iter().enumerate() {
            let mut all_readable = true;
            for &bucket in &needed {
                let readable = pull_bucket_with_retry(ep.clone(), up_stage, bucket)
                    .await
                    .map(|b| !b.is_empty())
                    .unwrap_or(false);
                if !readable {
                    all_readable = false;
                    break;
                }
            }
            if all_readable {
                continue;
            }
            let target = healthy_endpoints(std::slice::from_ref(ep))
                .await
                .into_iter()
                .next()
                .or_else(|| membership.endpoints().into_iter().find(|e| e != ep))
                .ok_or_else(|| Error::Execution("recompute: no healthy worker".into()))?;
            let producer_ticket = StageTicket {
                stage_id: up_stage,
                partition_id: i as u32,
                num_partitions: consumer.num_partitions,
                upstream_endpoints: if stage_def.upstream_stage_ids.is_empty() {
                    vec![]
                } else {
                    consumer.upstream_endpoints.clone()
                },
                stage_sql: stage_def.sql.clone(),
                plan_fragment: stage_def.plan_fragment.clone().unwrap_or_default(),
                hash_key_cols: stage_def.hash_key_cols.clone(),
                upstream_stage_ids: stage_def.upstream_stage_ids.clone(),
                produce: true,
                lakehouse_snapshot_pins: stage_def.lakehouse_snapshot_pins.clone(),
                replicated_tables: stage_def.replicated_tables.clone(),
                // A re-run producer keeps the legacy one-bucket read of its own upstreams.
                coalesce_read_modulus: 0,
                // …but still skips placeholder endpoints of any Forward upstreams it has.
                forward_upstream_stage_ids: forward_upstreams(stage_def, stages),
                // A re-run producer is dispatched outside its query's stage barriers, so the
                // driver holds no measured row counts for its upstreams; the worker falls
                // back to its own MemTable statistics.
                upstream_bucket_rows: vec![],
            };
            run_stage_inner(membership, target, producer_ticket, lineage, stages).await?;
        }
    }
    Ok(())
}

/// Filter endpoints to those that respond to a health check.
pub async fn healthy_endpoints(endpoints: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for ep in endpoints {
        if health_check_worker(ep.clone()).await.is_ok() {
            out.push(ep.clone());
        }
    }
    out
}

/// Test-only count of driver→worker slot probes, asserted by the R5-2 tests (the first
/// task attempt must not probe; retries must).
#[cfg(test)]
static SLOT_PROBES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

async fn worker_accepts_task(endpoint: String) -> bool {
    #[cfg(test)]
    SLOT_PROBES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    match heartbeat_worker(endpoint.clone()).await {
        Ok(heartbeat) => heartbeat.has_available_slot(),
        Err(e) if action_unimplemented(&e) => health_check_worker(endpoint).await.is_ok(),
        Err(_) => false,
    }
}

fn action_unimplemented(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("unimplemented") || s.contains("not implemented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_errors_match_transport_failures() {
        assert!(is_retryable(&Error::Io("connect worker: refused".into())));
        assert!(is_retryable(&Error::Execution(
            "do_get: Unavailable".into()
        )));
        // Opaque tonic transport string (no source chain) and the enriched form.
        assert!(is_retryable(&Error::Execution(
            "do_get: transport error".into()
        )));
        assert!(is_retryable(&Error::Execution(
            "do_get: transport error: connection reset by peer".into()
        )));
        assert!(is_retryable(&Error::Execution(
            "do_get: http2 error: stream error received: goaway".into()
        )));
        assert!(is_retryable(&Error::Execution(
            "do_exchange: incomplete message".into()
        )));
        assert!(!is_retryable(&Error::Plan("bad sql".into())));
    }

    #[test]
    fn deadline_exceeded_stage_do_get_is_not_retryable() {
        // KAN-15: the original attempt is still running server-side; a retry would duplicate it.
        assert!(!is_retryable(&Error::Execution(
            "do_get: Timeout expired".into()
        )));
        assert!(!is_retryable(&Error::Execution(
            "do_get: stage timed out after 600000 ms (OXIDANT_STAGE_TIMEOUT_MS)".into()
        )));
        // A worker-side cancellation is likewise terminal, and plain execution errors
        // no longer ride the blanket `do_get:` match.
        assert!(!is_retryable(&Error::Execution(
            "do_get: stage cancelled by driver".into()
        )));
        assert!(!is_retryable(&Error::Execution(
            "do_get: internal error: bad sql".into()
        )));
        // Genuine connect-level failures (nothing started server-side) stay retryable.
        assert!(is_retryable(&Error::Io(
            "connect worker: deadline has elapsed".into()
        )));
        assert!(is_retryable(&Error::Io(
            "connect worker: connection refused".into()
        )));
    }

    #[test]
    fn needs_recompute_on_shuffle_errors() {
        assert!(needs_upstream_recompute(&Error::Execution(
            "shuffle bucket empty".into()
        )));
    }

    fn leaf_ticket() -> StageTicket {
        StageTicket {
            stage_id: 0,
            partition_id: 0,
            num_partitions: 1,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![],
            upstream_stage_ids: vec![],
            produce: false,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        }
    }

    /// R5-2: the first task attempt dispatches without the heartbeat slot probe (a
    /// saturated worker queues the task server-side); retries still probe. One test
    /// function for both phases so the process-global probe counter can't race.
    #[tokio::test]
    async fn slot_probe_skipped_on_first_attempt_fires_on_retry() {
        use std::sync::atomic::Ordering;

        // Happy path against a live worker: attempt 0 succeeds with no probe at all.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(oxidant_loom::Engine::new());
        tokio::spawn(async move {
            let _ = crate::flight::serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        let mut up = false;
        for _ in 0..50 {
            if health_check_worker(endpoint.clone()).await.is_ok() {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(up, "worker did not become ready at {endpoint}");
        let membership: Arc<dyn ClusterMembership> =
            Arc::new(crate::membership::StaticMembership::new(vec![
                endpoint.clone()
            ]));
        let lineage = Arc::new(crate::lineage::StageLineage::new());
        let stages = std::collections::HashMap::new();
        let before = SLOT_PROBES.load(Ordering::SeqCst);
        let out = run_stage_inner_impl(&membership, endpoint, leaf_ticket(), &lineage, &stages)
            .await
            .expect("leaf stage on a live worker");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(
            SLOT_PROBES.load(Ordering::SeqCst) - before,
            0,
            "a first-attempt dispatch must not heartbeat-probe the worker"
        );

        // Dead endpoint: attempt 0 dispatches (and fails fast at connect); every later
        // attempt probes the worker before re-dispatching.
        let dead_port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let dead = format!("http://127.0.0.1:{dead_port}");
        let membership: Arc<dyn ClusterMembership> =
            Arc::new(crate::membership::StaticMembership::new(vec![dead.clone()]));
        let before = SLOT_PROBES.load(Ordering::SeqCst);
        run_stage_inner_impl(&membership, dead, leaf_ticket(), &lineage, &stages)
            .await
            .expect_err("a dead worker must fail all attempts");
        assert_eq!(
            SLOT_PROBES.load(Ordering::SeqCst) - before,
            (task_max_retries() - 1) as usize,
            "every retry attempt must still probe the worker"
        );
    }
}
