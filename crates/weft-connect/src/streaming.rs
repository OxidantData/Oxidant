//! Spark Connect Structured Streaming command handlers.

use std::sync::Arc;

use tonic::Status;
use weft_proto::spark::connect as sc;
use weft_streaming::{StreamingQueryManager, Trigger};

use crate::WeftService;

impl WeftService {
    #[allow(dead_code)]
    pub(crate) fn streaming_manager(&self) -> &Arc<StreamingQueryManager> {
        &self.streaming
    }

    /// Handle `WriteStreamOperationStart` — register a streaming query.
    pub(crate) async fn handle_write_stream_start(
        &self,
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
            .unwrap_or_else(|| format!("/tmp/weft-checkpoint-{}", uuid::Uuid::new_v4()));
        let trigger = match &start.trigger {
            Some(sc::write_stream_operation_start::Trigger::Once(true)) => Trigger::Once,
            Some(sc::write_stream_operation_start::Trigger::AvailableNow(true)) => {
                Trigger::AvailableNow
            }
            Some(sc::write_stream_operation_start::Trigger::ProcessingTimeInterval(s)) => {
                let secs = s.trim_end_matches('s').parse::<u64>().unwrap_or(1);
                Trigger::ProcessingTime(std::time::Duration::from_secs(secs))
            }
            _ => Trigger::ProcessingTime(std::time::Duration::from_secs(1)),
        };
        let sink_path = match &start.sink_destination {
            Some(sc::write_stream_operation_start::SinkDestination::Path(p)) if !p.is_empty() => {
                Some(p.clone())
            }
            Some(sc::write_stream_operation_start::SinkDestination::TableName(t))
                if !t.is_empty() =>
            {
                Some(t.clone())
            }
            _ => None,
        };
        let config =
            weft_streaming::StreamQueryConfig::from_spark(&start.format, &start.options, sink_path);
        let id = self
            .streaming
            .start_with_config(name.clone(), checkpoint, trigger.clone(), config)
            .await;
        // Kick off batches: once/availableNow run to completion; processing-time loops.
        let mgr = self.streaming.clone();
        let eng = self.engine.clone();
        let qid = id.id.clone();
        match trigger {
            Trigger::ProcessingTime(interval) => {
                tokio::spawn(async move {
                    loop {
                        let _ = mgr.run_batch(&qid, &eng).await;
                        let active = mgr.status(&qid).await.map(|s| s.is_active).unwrap_or(false);
                        if !active {
                            break;
                        }
                        tokio::time::sleep(interval).await;
                    }
                });
            }
            _ => {
                tokio::spawn(async move {
                    let _ = mgr.process_all_available(&qid, &eng).await;
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

    /// Handle `StreamingQueryCommand`.
    pub(crate) async fn handle_streaming_query_command(
        &self,
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
                    .process_all_available(&qid.id, &self.engine)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
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
