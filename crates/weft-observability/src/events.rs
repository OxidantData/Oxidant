use serde::{Deserialize, Serialize};

/// Append-only execution events emitted by connect, loom, and execution crates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    JobStarted {
        operation_id: String,
        job_id: i64,
        description: String,
        submission_time_ms: i64,
    },
    JobFinished {
        operation_id: String,
        job_id: i64,
        status: JobStatus,
        completion_time_ms: i64,
        error: Option<String>,
    },
    StageStarted {
        operation_id: String,
        stage_id: i32,
        name: String,
        num_tasks: i32,
        submission_time_ms: i64,
    },
    StageFinished {
        operation_id: String,
        stage_id: i32,
        status: StageStatus,
        completion_time_ms: i64,
        shuffle_read_bytes: i64,
        shuffle_write_bytes: i64,
        input_rows: i64,
        output_rows: i64,
    },
    TaskStarted {
        operation_id: String,
        stage_id: i32,
        task_id: i64,
        executor_id: String,
        launch_time_ms: i64,
    },
    TaskFinished {
        operation_id: String,
        stage_id: i32,
        task_id: i64,
        executor_id: String,
        status: TaskStatus,
        duration_ms: i64,
        shuffle_read_bytes: i64,
        shuffle_write_bytes: i64,
        output_rows: i64,
    },
    SqlPlanCaptured {
        operation_id: String,
        execution_id: i64,
        description: String,
        physical_plan: String,
        logical_plan: Option<String>,
        job_ids: Vec<i64>,
    },
    ExecutorRegistered {
        executor_id: String,
        host_port: String,
    },
    AqeCoalesced {
        operation_id: String,
        stage_id: i32,
        old_partitions: u32,
        new_partitions: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StageStatus {
    Active,
    Complete,
    Pending,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    Running,
    Success,
    Failed,
    Killed,
    Pending,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl StageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Complete => "COMPLETE",
            Self::Pending => "PENDING",
            Self::Failed => "FAILED",
            Self::Skipped => "SKIPPED",
        }
    }
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Success => "SUCCESS",
            Self::Failed => "FAILED",
            Self::Killed => "KILLED",
            Self::Pending => "PENDING",
        }
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
