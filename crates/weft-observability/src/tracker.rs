//! High-level helpers for instrumenting query execution.

use crate::events::{now_ms, ExecutionEvent, JobStatus, StageStatus, TaskStatus};
use crate::store::SharedStore;

/// Tracks a single operation (Spark Connect `operation_id`) through job/stage lifecycle.
pub struct QueryTracker {
    store: SharedStore,
    operation_id: String,
    job_id: i64,
    sql_id: Option<i64>,
    stage_id: i32,
    #[allow(dead_code)]
    started_ms: i64,
}

impl QueryTracker {
    pub fn begin(
        store: SharedStore,
        operation_id: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        let operation_id = operation_id.into();
        let description = description.into();
        let job_id = store.alloc_job_id();
        let sql_id = store.alloc_sql_id();
        let started_ms = now_ms();
        store.emit(ExecutionEvent::JobStarted {
            operation_id: operation_id.clone(),
            job_id,
            description: description.clone(),
            submission_time_ms: started_ms,
        });
        store.emit(ExecutionEvent::SqlPlanCaptured {
            operation_id: operation_id.clone(),
            execution_id: sql_id,
            description,
            physical_plan: String::new(),
            logical_plan: None,
            job_ids: vec![job_id],
        });
        Self {
            store,
            operation_id,
            job_id,
            sql_id: Some(sql_id),
            stage_id: 0,
            started_ms,
        }
    }

    pub fn set_plan(&self, physical: impl Into<String>, logical: Option<String>) {
        if let Some(sql_id) = self.sql_id {
            self.store.emit(ExecutionEvent::SqlPlanCaptured {
                operation_id: self.operation_id.clone(),
                execution_id: sql_id,
                description: String::new(),
                physical_plan: physical.into(),
                logical_plan: logical,
                job_ids: vec![self.job_id],
            });
        }
    }

    pub fn begin_local_stage(&mut self, name: impl Into<String>, num_tasks: i32) {
        self.stage_id = 0;
        self.store.emit(ExecutionEvent::StageStarted {
            operation_id: self.operation_id.clone(),
            stage_id: self.stage_id,
            name: name.into(),
            num_tasks,
            submission_time_ms: now_ms(),
        });
    }

    pub fn begin_stage(&mut self, stage_id: i32, name: impl Into<String>, num_tasks: i32) {
        self.stage_id = stage_id;
        self.store.emit(ExecutionEvent::StageStarted {
            operation_id: self.operation_id.clone(),
            stage_id,
            name: name.into(),
            num_tasks,
            submission_time_ms: now_ms(),
        });
    }

    pub fn finish_stage(
        &self,
        stage_id: i32,
        output_rows: i64,
        shuffle_read: i64,
        shuffle_write: i64,
    ) {
        self.store.emit(ExecutionEvent::StageFinished {
            operation_id: self.operation_id.clone(),
            stage_id,
            status: StageStatus::Complete,
            completion_time_ms: now_ms(),
            shuffle_read_bytes: shuffle_read,
            shuffle_write_bytes: shuffle_write,
            input_rows: 0,
            output_rows,
        });
    }

    pub fn task_started(&self, stage_id: i32, task_id: i64, executor_id: impl Into<String>) {
        let executor_id = executor_id.into();
        self.store.emit(ExecutionEvent::ExecutorRegistered {
            executor_id: executor_id.clone(),
            host_port: executor_id.clone(),
        });
        self.store.emit(ExecutionEvent::TaskStarted {
            operation_id: self.operation_id.clone(),
            stage_id,
            task_id,
            executor_id,
            launch_time_ms: now_ms(),
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn task_finished(
        &self,
        stage_id: i32,
        task_id: i64,
        executor_id: impl Into<String>,
        duration_ms: i64,
        output_rows: i64,
        shuffle_read: i64,
        shuffle_write: i64,
    ) {
        self.store.emit(ExecutionEvent::TaskFinished {
            operation_id: self.operation_id.clone(),
            stage_id,
            task_id,
            executor_id: executor_id.into(),
            status: TaskStatus::Success,
            duration_ms,
            shuffle_read_bytes: shuffle_read,
            shuffle_write_bytes: shuffle_write,
            output_rows,
        });
    }

    pub fn finish_success(&self, output_rows: i64) {
        let end = now_ms();
        self.store.emit(ExecutionEvent::StageFinished {
            operation_id: self.operation_id.clone(),
            stage_id: self.stage_id,
            status: StageStatus::Complete,
            completion_time_ms: end,
            shuffle_read_bytes: 0,
            shuffle_write_bytes: 0,
            input_rows: 0,
            output_rows,
        });
        self.store.emit(ExecutionEvent::JobFinished {
            operation_id: self.operation_id.clone(),
            job_id: self.job_id,
            status: JobStatus::Succeeded,
            completion_time_ms: end,
            error: None,
        });
    }

    pub fn finish_error(&self, error: impl Into<String>) {
        let end = now_ms();
        self.store.emit(ExecutionEvent::StageFinished {
            operation_id: self.operation_id.clone(),
            stage_id: self.stage_id,
            status: StageStatus::Failed,
            completion_time_ms: end,
            shuffle_read_bytes: 0,
            shuffle_write_bytes: 0,
            input_rows: 0,
            output_rows: 0,
        });
        self.store.emit(ExecutionEvent::JobFinished {
            operation_id: self.operation_id.clone(),
            job_id: self.job_id,
            status: JobStatus::Failed,
            completion_time_ms: end,
            error: Some(error.into()),
        });
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn job_id(&self) -> i64 {
        self.job_id
    }

    pub fn store(&self) -> &SharedStore {
        &self.store
    }
}

/// Optional global store for worker-side instrumentation.
static WORKER_STORE: std::sync::OnceLock<SharedStore> = std::sync::OnceLock::new();

pub fn set_worker_store(store: SharedStore) {
    let _ = WORKER_STORE.set(store);
}

pub fn worker_store() -> Option<SharedStore> {
    WORKER_STORE.get().cloned()
}

pub fn emit_worker_task(
    operation_id: &str,
    stage_id: i32,
    task_id: i64,
    executor_id: &str,
    duration_ms: i64,
    output_rows: i64,
    shuffle_write: i64,
) {
    if let Some(store) = worker_store() {
        store.emit(ExecutionEvent::ExecutorRegistered {
            executor_id: executor_id.to_string(),
            host_port: executor_id.to_string(),
        });
        store.emit(ExecutionEvent::TaskStarted {
            operation_id: operation_id.to_string(),
            stage_id,
            task_id,
            executor_id: executor_id.to_string(),
            launch_time_ms: now_ms().saturating_sub(duration_ms),
        });
        store.emit(ExecutionEvent::TaskFinished {
            operation_id: operation_id.to_string(),
            stage_id,
            task_id,
            executor_id: executor_id.to_string(),
            status: TaskStatus::Success,
            duration_ms,
            shuffle_read_bytes: 0,
            shuffle_write_bytes: shuffle_write,
            output_rows,
        });
    }
}
