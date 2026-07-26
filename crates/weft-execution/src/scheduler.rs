//! Task scheduling with fault tolerance: retries, alternate workers, health checks,
//! speculative execution, and stage recomputation.

use std::sync::Arc;
use std::time::Duration;

use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;

use crate::driver::StageDef;
use crate::flight::{
    health_check_worker, heartbeat_worker, pull_bucket_with_retry, run_stage_on_worker,
};
use crate::lineage::SharedLineage;
use crate::membership::ClusterMembership;
use crate::shuffle::protocol::StageTicket;

/// Max task attempts per endpoint before trying alternates (env: `WEFT_TASK_MAX_RETRIES`, default 3).
pub fn task_max_retries() -> u32 {
    std::env::var("WEFT_TASK_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .unwrap_or(3)
}

/// Straggler threshold before launching a speculative duplicate task (ms).
pub fn speculative_timeout_ms() -> u64 {
    std::env::var("WEFT_SPECULATIVE_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000)
}

/// Whether an execution error is worth retrying on another worker.
pub fn is_retryable(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("connect worker")
        || s.contains("do_get:")
        || s.contains("unavailable")
        || s.contains("connection")
        || s.contains("deadline")
        || s.contains("reset")
        || s.contains("broken pipe")
        || s.contains("health check failed")
        || s.contains("shuffle")
        || s.contains("empty bucket")
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
    std::env::var("WEFT_SPECULATIVE")
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
        if !worker_accepts_task(primary.clone()).await {
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
    for &up_stage in &consumer.upstream_stage_ids {
        let stage_def = stages
            .get(&up_stage)
            .ok_or_else(|| Error::Execution(format!("recompute: unknown stage {up_stage}")))?;
        for (i, ep) in consumer.upstream_endpoints.iter().enumerate() {
            let readable = pull_bucket_with_retry(ep.clone(), up_stage, consumer.partition_id)
                .await
                .map(|b| !b.is_empty())
                .unwrap_or(false);
            if readable {
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

async fn worker_accepts_task(endpoint: String) -> bool {
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
        assert!(!is_retryable(&Error::Plan("bad sql".into())));
    }

    #[test]
    fn needs_recompute_on_shuffle_errors() {
        assert!(needs_upstream_recompute(&Error::Execution(
            "shuffle bucket empty".into()
        )));
    }
}
