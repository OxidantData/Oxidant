use serde::{Deserialize, Serialize};

use crate::events::{JobStatus, StageStatus, TaskStatus};

/// Spark REST `/api/v1/applications` entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationInfo {
    pub id: String,
    pub name: String,
    pub attempts: Vec<ApplicationAttemptInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationAttemptInfo {
    pub attempt_id: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub duration: Option<i64>,
    pub spark_user: String,
    pub app_spark_version: String,
    pub completed: bool,
}

/// Spark REST job data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobData {
    pub job_id: i64,
    pub name: String,
    pub description: Option<String>,
    pub submission_time: Option<String>,
    pub completion_time: Option<String>,
    pub stage_ids: Vec<i32>,
    pub status: String,
    pub num_tasks: i32,
    pub num_active_tasks: i32,
    pub num_completed_tasks: i32,
    pub num_skipped_tasks: i32,
    pub num_failed_tasks: i32,
    pub num_killed_tasks: i32,
    pub num_completed_stages: i32,
    pub num_active_stages: i32,
    pub num_failed_stages: i32,
}

/// Spark REST stage data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageData {
    pub status: String,
    pub stage_id: i32,
    pub attempt_id: i32,
    pub num_tasks: i32,
    pub num_active_tasks: i32,
    pub num_complete_tasks: i32,
    pub num_failed_tasks: i32,
    pub num_killed_tasks: i32,
    pub submission_time: Option<String>,
    pub first_task_launched_time: Option<String>,
    pub completion_time: Option<String>,
    pub executor_run_time: i64,
    pub executor_cpu_time: i64,
    pub input_bytes: i64,
    pub input_records: i64,
    pub output_bytes: i64,
    pub output_records: i64,
    pub shuffle_read_bytes: i64,
    pub shuffle_read_records: i64,
    pub shuffle_write_bytes: i64,
    pub shuffle_write_records: i64,
    pub name: String,
    pub description: Option<String>,
    pub details: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<Vec<TaskData>>,
}

/// Spark REST task data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskData {
    pub task_id: i64,
    pub index: i32,
    pub attempt: i32,
    pub partition_id: i32,
    pub launch_time: Option<String>,
    pub executor_id: String,
    pub host: String,
    pub status: String,
    pub task_locality: String,
    pub speculative: bool,
    pub getting_result_time: Option<String>,
    pub finish_time: Option<String>,
    pub executor_run_time: i64,
    pub executor_cpu_time: i64,
    pub result_size: i64,
    pub disk_bytes_spilled: i64,
    pub memory_bytes_spilled: i64,
    pub input_bytes: i64,
    pub input_records: i64,
    pub output_bytes: i64,
    pub output_records: i64,
    pub shuffle_read_bytes: i64,
    pub shuffle_read_records: i64,
    pub shuffle_write_bytes: i64,
    pub shuffle_write_records: i64,
}

/// Weft extension: SQL execution entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlExecution {
    pub id: i64,
    pub description: String,
    pub submission_time: Option<String>,
    pub completion_time: Option<String>,
    pub duration: Option<i64>,
    pub physical_plan: String,
    pub logical_plan: Option<String>,
    pub job_ids: Vec<i64>,
    pub status: String,
}

/// Spark REST executor summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSummary {
    pub id: String,
    pub host_port: String,
    pub is_active: bool,
    pub rdd_blocks: i32,
    pub memory_used: i64,
    pub disk_used: i64,
    pub total_cores: i32,
    pub max_tasks: i32,
    pub active_tasks: i32,
    pub failed_tasks: i32,
    pub completed_tasks: i32,
    pub total_tasks: i32,
    pub total_duration: i64,
    pub total_gc_time: i64,
    pub total_input_bytes: i64,
    pub total_shuffle_read: i64,
    pub total_shuffle_write: i64,
    pub is_blacklisted: bool,
}

/// Environment key-value pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentEntry {
    pub key: String,
    pub value: String,
}

pub fn ms_to_iso(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| ms.to_string())
}

pub fn job_status_str(s: JobStatus) -> String {
    s.as_str().to_string()
}

pub fn stage_status_str(s: StageStatus) -> String {
    s.as_str().to_string()
}

pub fn task_status_str(s: TaskStatus) -> String {
    s.as_str().to_string()
}
