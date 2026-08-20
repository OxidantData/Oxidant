//! Spark Connect Structured Streaming command handlers.

use std::sync::Arc;

use oxidant_proto::spark::connect as sc;
use oxidant_streaming::{
    MicroBatchPipeline, SinkDestination, StartOptions, StreamQueryConfig, StreamingQueryManager,
    Trigger,
};
use tonic::Status;

use crate::translate;
use crate::OxidantService;
use oxidant_loom::Engine;

impl OxidantService {
    #[allow(dead_code)]
    pub(crate) fn streaming_manager(&self) -> &Arc<StreamingQueryManager> {
        &self.streaming
    }

    /// Handle `WriteStreamOperationStart` — register a streaming query. The batch loop runs on
    /// the CALLER's session engine handle (KAN-85): a `USE CATALOG` before starting the stream
    /// steers its batches, exactly as it steers the session's queries. The session cell lives
    /// in the shared registry, so the spawned task holding this handle is safe.
    pub(crate) async fn handle_write_stream_start(
        &self,
        engine: &Engine,
        start: &sc::WriteStreamOperationStart,
    ) -> Result<sc::WriteStreamOperationStartResult, Status> {
        let name = if start.query_name.is_empty() {
            "query".to_string()
        } else {
            start.query_name.clone()
        };
        let checkpoint = start
            .options
            .get("checkpointLocation")
            .cloned()
            .unwrap_or_else(|| format!("/tmp/oxidant-checkpoint-{}", uuid::Uuid::new_v4()));
        let trigger = match &start.trigger {
            Some(sc::write_stream_operation_start::Trigger::Once(true)) => Trigger::Once,
            Some(sc::write_stream_operation_start::Trigger::AvailableNow(true)) => {
                Trigger::AvailableNow
            }
            Some(sc::write_stream_operation_start::Trigger::ProcessingTimeInterval(s)) => {
                Trigger::ProcessingTime(parse_processing_time(s))
            }
            _ => Trigger::ProcessingTime(std::time::Duration::from_secs(1)),
        };
        let destination = match &start.sink_destination {
            Some(sc::write_stream_operation_start::SinkDestination::TableName(t))
                if !t.is_empty() =>
            {
                SinkDestination::Table(t.clone())
            }
            Some(sc::write_stream_operation_start::SinkDestination::Path(p)) if !p.is_empty() => {
                SinkDestination::Path(p.clone())
            }
            _ => SinkDestination::None,
        };

        // The source is described by the streaming `Read` at the bottom of the input relation,
        // NOT by the writer's format/options — conflating the two made a Kafka→Delta pipeline try
        // to read Delta and write Kafka options.
        let (source_format, source_options) = match start.input.as_ref() {
            Some(rel) => match translate::relation::find_streaming_read(rel) {
                Some(read) => translate::relation::streaming_read_spec(read)?,
                None => (String::new(), Default::default()),
            },
            None => (String::new(), Default::default()),
        };
        let config = StreamQueryConfig::from_spark(
            &source_format,
            &source_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            &start.format,
            destination,
            &start.options,
            start.partitioning_column_names.clone(),
        );

        // Translate the DataFrame transformation once; the query manager re-executes it per
        // micro-batch against the swappable streaming input. Capturing the inputs the
        // translation created — rather than looking one up by name — is what gives this query
        // its own buffer, so two queries on the same topic cannot clobber each other.
        let pipeline = match start.input.as_ref() {
            Some(rel) if !source_format.is_empty() => {
                let (plan, inputs) =
                    oxidant_streaming::capture_stream_inputs(translate::to_plan(engine.ctx(), rel))
                        .await;
                let plan = plan?;
                match inputs.len() {
                    0 => None,
                    1 => Some(MicroBatchPipeline {
                        input: inputs.into_iter().next().expect("len checked"),
                        plan,
                        output_schema: None,
                    }),
                    n => {
                        return Err(Status::unimplemented(format!(
                            "this streaming query reads {n} streaming sources; Oxidant runs one \
                             source per query (stream-stream joins are not implemented)"
                        )))
                    }
                }
            }
            _ => None,
        };

        let (current_catalog, current_namespace) = engine.current_catalog_and_namespace();
        let id = self
            .streaming
            .start_with_config(
                engine,
                name.clone(),
                checkpoint,
                trigger.clone(),
                config,
                StartOptions {
                    pipeline,
                    current_catalog,
                    current_namespace,
                    sink_override: None,
                },
            )
            .await
            .map_err(crate::err_to_status)?;

        // Kick off batches: once/availableNow run to completion; processing-time loops.
        let mgr = self.streaming.clone();
        let eng = engine.clone();
        let qid = id.id.clone();
        match trigger {
            Trigger::ProcessingTime(interval) => {
                tokio::spawn(async move {
                    // A fixed-rate schedule, not sleep-after-work. Sleeping the full interval
                    // *after* each batch makes the real period `interval + batch duration`, so a
                    // stream under load silently halves its own trigger rate. `tokio::interval`
                    // with `Delay` missed-tick behaviour fires immediately when a batch overran
                    // and otherwise keeps to the requested cadence.
                    let mut ticker = tokio::time::interval(interval);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        ticker.tick().await;
                        if let Err(e) = mgr.run_batch(&qid, &eng).await {
                            // A batch that fails even after its retries stops the query rather
                            // than spinning on the same error every interval — and leaves the
                            // message on the status, which is where `query.status()` and
                            // `awaitTermination()` look.
                            mgr.fail(&qid, &e.to_string()).await;
                            break;
                        }
                        let active = mgr.status(&qid).await.map(|s| s.is_active).unwrap_or(false);
                        if !active {
                            break;
                        }
                    }
                });
            }
            _ => {
                tokio::spawn(async move {
                    if let Err(e) = mgr.process_all_available(&qid, &eng).await {
                        mgr.fail(&qid, &e.to_string()).await;
                        return;
                    }
                    // `Once` and `AvailableNow` terminate once the data that was there is
                    // processed — that is the whole difference from a processing-time trigger.
                    // Leaving the query active instead makes `awaitTermination()` never return and
                    // `isActive` never go false, so a batch job written against these triggers
                    // hangs forever on a query that already finished its work.
                    mgr.stop(&qid).await;
                });
            }
        }
        Ok(sc::WriteStreamOperationStartResult {
            query_id: Some(sc::StreamingQueryInstanceId {
                id: id.id,
                run_id: id.run_id,
            }),
            name,
            query_started_event_json: None,
        })
    }

    /// Handle `StreamingQueryCommand`. `ProcessAllAvailable` runs batches on the caller's
    /// session engine handle (KAN-85, same as the start path); status/stop don't touch it.
    pub(crate) async fn handle_streaming_query_command(
        &self,
        engine: &Engine,
        cmd: &sc::StreamingQueryCommand,
    ) -> Result<sc::StreamingQueryCommandResult, Status> {
        let qid = cmd
            .query_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing query_id"))?;
        let result_type = match &cmd.command {
            Some(sc::streaming_query_command::Command::Status(true)) => {
                let status = self.streaming.status(&qid.id).await.unwrap_or_default();
                Some(sc::streaming_query_command_result::ResultType::Status(
                    sc::streaming_query_command_result::StatusResult {
                        status_message: status.message,
                        is_data_available: status.is_data_available,
                        is_trigger_active: status.is_trigger_active,
                        is_active: status.is_active,
                    },
                ))
            }
            Some(sc::streaming_query_command::Command::LastProgress(true)) => {
                let progress = self.streaming.last_progress(&qid.id).await;
                let json = progress
                    .map(|p| serde_json::to_string(&p).unwrap_or_default())
                    .unwrap_or_default();
                Some(
                    sc::streaming_query_command_result::ResultType::RecentProgress(
                        sc::streaming_query_command_result::RecentProgressResult {
                            recent_progress_json: if json.is_empty() { vec![] } else { vec![json] },
                        },
                    ),
                )
            }
            Some(sc::streaming_query_command::Command::Stop(true)) => {
                self.streaming.stop(&qid.id).await;
                Some(sc::streaming_query_command_result::ResultType::Status(
                    sc::streaming_query_command_result::StatusResult {
                        status_message: "stopped".into(),
                        is_data_available: false,
                        is_trigger_active: false,
                        is_active: false,
                    },
                ))
            }
            Some(sc::streaming_query_command::Command::ProcessAllAvailable(true)) => {
                let rows = self
                    .streaming
                    .process_all_available(&qid.id, engine)
                    .await
                    .map_err(crate::err_to_status)?;
                Some(sc::streaming_query_command_result::ResultType::Status(
                    sc::streaming_query_command_result::StatusResult {
                        status_message: format!("processed {rows} rows"),
                        is_data_available: rows > 0,
                        is_trigger_active: false,
                        is_active: true,
                    },
                ))
            }
            Some(sc::streaming_query_command::Command::AwaitTermination(_)) => Some(
                sc::streaming_query_command_result::ResultType::AwaitTermination(
                    sc::streaming_query_command_result::AwaitTerminationResult { terminated: true },
                ),
            ),
            _ => None,
        };
        Ok(sc::StreamingQueryCommandResult {
            query_id: Some(qid.clone()),
            result_type,
        })
    }
}

/// Parse Spark's `Trigger.ProcessingTime` interval string.
///
/// PySpark sends whatever the user typed (`"5 seconds"`, `"500 milliseconds"`, `"1 minute"`), so
/// the old `trim_end_matches('s').parse()` turned `"5 seconds"` into a 1-second default and
/// `"500 milliseconds"` into a 500-*second* trigger. Anything unparseable falls back to 1s, which
/// is Spark's behaviour for a trigger of zero.
fn parse_processing_time(spec: &str) -> std::time::Duration {
    use std::time::Duration;

    let spec = spec.trim().to_ascii_lowercase();
    let digits: String = spec.chars().take_while(|c| c.is_ascii_digit()).collect();
    let Ok(n) = digits.parse::<u64>() else {
        return Duration::from_secs(1);
    };
    let unit = spec[digits.len()..].trim();
    match unit {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => Duration::from_secs(n),
        "ms" | "millisecond" | "milliseconds" => Duration::from_millis(n),
        "m" | "min" | "mins" | "minute" | "minutes" => Duration::from_secs(n * 60),
        "h" | "hour" | "hours" => Duration::from_secs(n * 3600),
        _ => Duration::from_secs(1),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_processing_time;
    use std::time::Duration;

    #[test]
    fn processing_time_intervals_parse_their_units() {
        assert_eq!(parse_processing_time("5 seconds"), Duration::from_secs(5));
        assert_eq!(parse_processing_time("5s"), Duration::from_secs(5));
        assert_eq!(
            parse_processing_time("500 milliseconds"),
            Duration::from_millis(500)
        );
        assert_eq!(parse_processing_time("1 minute"), Duration::from_secs(60));
        assert_eq!(parse_processing_time("2 hours"), Duration::from_secs(7200));
    }

    #[test]
    fn an_unparseable_interval_falls_back_to_one_second() {
        assert_eq!(parse_processing_time(""), Duration::from_secs(1));
        assert_eq!(parse_processing_time("soon"), Duration::from_secs(1));
    }
}
