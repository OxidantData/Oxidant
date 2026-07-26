use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::events::{now_ms, ExecutionEvent, JobStatus, StageStatus, TaskStatus};
use crate::model::{
    job_status_str, ms_to_iso, stage_status_str, task_status_str, ApplicationAttemptInfo,
    ApplicationInfo, EnvironmentEntry, ExecutorSummary, JobData, SqlExecution, StageData, TaskData,
};

const DEFAULT_MAX_QUERIES: usize = 100;

pub type SharedStore = Arc<AppStateStore>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationState {
    Running,
    Succeeded,
    Failed,
}

impl OperationState {
    pub fn as_proto_str(self) -> &'static str {
        match self {
            Self::Running => "OPERATION_STATE_RUNNING",
            Self::Succeeded => "OPERATION_STATE_SUCCEEDED",
            Self::Failed => "OPERATION_STATE_FAILED",
        }
    }
}

struct InnerJob {
    operation_id: String,
    job_id: i64,
    description: String,
    submission_time_ms: i64,
    completion_time_ms: Option<i64>,
    status: JobStatus,
    stage_ids: Vec<i32>,
    error: Option<String>,
}

struct InnerStage {
    operation_id: String,
    stage_id: i32,
    name: String,
    submission_time_ms: i64,
    completion_time_ms: Option<i64>,
    status: StageStatus,
    num_tasks: i32,
    shuffle_read_bytes: i64,
    shuffle_write_bytes: i64,
    input_rows: i64,
    output_rows: i64,
    tasks: HashMap<i64, InnerTask>,
}

struct InnerTask {
    task_id: i64,
    #[allow(dead_code)]
    stage_id: i32,
    executor_id: String,
    launch_time_ms: i64,
    finish_time_ms: Option<i64>,
    status: TaskStatus,
    duration_ms: i64,
    shuffle_read_bytes: i64,
    shuffle_write_bytes: i64,
    output_rows: i64,
}

struct InnerSql {
    operation_id: String,
    execution_id: i64,
    description: String,
    physical_plan: String,
    logical_plan: Option<String>,
    job_ids: Vec<i64>,
    submission_time_ms: i64,
    completion_time_ms: Option<i64>,
    status: JobStatus,
}

struct InnerExecutor {
    host_port: String,
    active_tasks: i32,
    completed_tasks: i32,
    failed_tasks: i32,
    total_shuffle_read: i64,
    total_shuffle_write: i64,
    total_duration: i64,
}

/// In-memory observability store with optional event-log persistence.
pub struct AppStateStore {
    app_id: String,
    app_name: String,
    start_time_ms: i64,
    next_job_id: AtomicI64,
    next_sql_id: AtomicI64,
    next_task_id: AtomicI64,
    max_queries: usize,
    event_log_dir: Option<PathBuf>,
    tx: broadcast::Sender<ExecutionEvent>,
    inner: Mutex<StoreInner>,
}

struct StoreInner {
    operation_order: Vec<String>,
    operations: HashMap<String, OperationState>,
    jobs: HashMap<i64, InnerJob>,
    /// Keyed by `{operation_id}:{stage_id}` so concurrent queries do not collide.
    stages: HashMap<String, InnerStage>,
    sql_executions: HashMap<i64, InnerSql>,
    executors: HashMap<String, InnerExecutor>,
    environment: HashMap<String, String>,
}

fn stage_key(operation_id: &str, stage_id: i32) -> String {
    format!("{operation_id}:{stage_id}")
}

impl AppStateStore {
    pub fn new() -> Self {
        Self::with_options("weft-local", "Weft Application", None, DEFAULT_MAX_QUERIES)
    }

    pub fn with_options(
        app_id: impl Into<String>,
        app_name: impl Into<String>,
        event_log_dir: Option<PathBuf>,
        max_queries: usize,
    ) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            app_id: app_id.into(),
            app_name: app_name.into(),
            start_time_ms: now_ms(),
            next_job_id: AtomicI64::new(0),
            next_sql_id: AtomicI64::new(0),
            next_task_id: AtomicI64::new(0),
            max_queries: max_queries.max(1),
            event_log_dir: event_log_dir
                .or_else(|| std::env::var("WEFT_EVENT_LOG_DIR").ok().map(PathBuf::from)),
            tx,
            inner: Mutex::new(StoreInner {
                operation_order: Vec::new(),
                operations: HashMap::new(),
                jobs: HashMap::new(),
                stages: HashMap::new(),
                sql_executions: HashMap::new(),
                executors: HashMap::new(),
                environment: HashMap::new(),
            }),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.tx.subscribe()
    }

    pub fn set_environment(&self, entries: HashMap<String, String>) {
        let mut inner = self.inner.lock().expect("store poisoned");
        inner.environment = entries;
    }

    pub fn operation_state(&self, operation_id: &str) -> Option<OperationState> {
        self.inner
            .lock()
            .expect("store poisoned")
            .operations
            .get(operation_id)
            .copied()
    }

    pub fn all_operation_states(&self) -> Vec<(String, OperationState)> {
        let inner = self.inner.lock().expect("store poisoned");
        inner
            .operations
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub fn emit(&self, event: ExecutionEvent) {
        self.apply_event(&event);
        if let Some(dir) = &self.event_log_dir {
            if let Ok(json) = serde_json::to_string(&event) {
                let _ = fs::create_dir_all(dir);
                let path = dir.join("events.jsonl");
                if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
                    let _ = writeln!(f, "{json}");
                }
            }
        }
        let _ = self.tx.send(event);
    }

    fn apply_event(&self, event: &ExecutionEvent) {
        let mut inner = self.inner.lock().expect("store poisoned");
        match event {
            ExecutionEvent::JobStarted {
                operation_id,
                job_id,
                description,
                submission_time_ms,
            } => {
                if !inner.operation_order.contains(operation_id) {
                    inner.operation_order.push(operation_id.clone());
                    while inner.operation_order.len() > self.max_queries {
                        if let Some(old) = inner.operation_order.first().cloned() {
                            inner.operation_order.remove(0);
                            inner.operations.remove(&old);
                        }
                    }
                }
                inner
                    .operations
                    .insert(operation_id.clone(), OperationState::Running);
                inner.jobs.insert(
                    *job_id,
                    InnerJob {
                        operation_id: operation_id.clone(),
                        job_id: *job_id,
                        description: description.clone(),
                        submission_time_ms: *submission_time_ms,
                        completion_time_ms: None,
                        status: JobStatus::Running,
                        stage_ids: Vec::new(),
                        error: None,
                    },
                );
            }
            ExecutionEvent::JobFinished {
                operation_id,
                job_id,
                status,
                completion_time_ms,
                error,
            } => {
                let op_state = match status {
                    JobStatus::Succeeded => OperationState::Succeeded,
                    JobStatus::Failed => OperationState::Failed,
                    _ => OperationState::Running,
                };
                inner.operations.insert(operation_id.clone(), op_state);
                if let Some(job) = inner.jobs.get_mut(job_id) {
                    job.status = *status;
                    job.completion_time_ms = Some(*completion_time_ms);
                    job.error = error.clone();
                }
                if let Some(sql) = inner
                    .sql_executions
                    .values_mut()
                    .find(|s| s.operation_id == *operation_id)
                {
                    sql.status = *status;
                    sql.completion_time_ms = Some(*completion_time_ms);
                }
            }
            ExecutionEvent::StageStarted {
                operation_id,
                stage_id,
                name,
                num_tasks,
                submission_time_ms,
            } => {
                if let Some(job) = inner
                    .jobs
                    .values_mut()
                    .find(|j| j.operation_id == *operation_id)
                {
                    if !job.stage_ids.contains(stage_id) {
                        job.stage_ids.push(*stage_id);
                    }
                }
                inner.stages.insert(
                    stage_key(operation_id, *stage_id),
                    InnerStage {
                        operation_id: operation_id.clone(),
                        stage_id: *stage_id,
                        name: name.clone(),
                        submission_time_ms: *submission_time_ms,
                        completion_time_ms: None,
                        status: StageStatus::Active,
                        num_tasks: *num_tasks,
                        shuffle_read_bytes: 0,
                        shuffle_write_bytes: 0,
                        input_rows: 0,
                        output_rows: 0,
                        tasks: HashMap::new(),
                    },
                );
            }
            ExecutionEvent::StageFinished {
                operation_id,
                stage_id,
                status,
                completion_time_ms,
                shuffle_read_bytes,
                shuffle_write_bytes,
                input_rows,
                output_rows,
            } => {
                if let Some(stage) = inner.stages.get_mut(&stage_key(operation_id, *stage_id)) {
                    if stage.operation_id == *operation_id {
                        stage.status = *status;
                        stage.completion_time_ms = Some(*completion_time_ms);
                        stage.shuffle_read_bytes = *shuffle_read_bytes;
                        stage.shuffle_write_bytes = *shuffle_write_bytes;
                        stage.input_rows = *input_rows;
                        stage.output_rows = *output_rows;
                    }
                }
            }
            ExecutionEvent::TaskStarted {
                operation_id,
                stage_id,
                task_id,
                executor_id,
                launch_time_ms,
            } => {
                if let Some(stage) = inner.stages.get_mut(&stage_key(operation_id, *stage_id)) {
                    if stage.operation_id == *operation_id {
                        stage.tasks.insert(
                            *task_id,
                            InnerTask {
                                task_id: *task_id,
                                stage_id: *stage_id,
                                executor_id: executor_id.clone(),
                                launch_time_ms: *launch_time_ms,
                                finish_time_ms: None,
                                status: TaskStatus::Running,
                                duration_ms: 0,
                                shuffle_read_bytes: 0,
                                shuffle_write_bytes: 0,
                                output_rows: 0,
                            },
                        );
                    }
                }
                if let Some(exec) = inner.executors.get_mut(executor_id) {
                    exec.active_tasks += 1;
                }
            }
            ExecutionEvent::TaskFinished {
                operation_id,
                stage_id,
                task_id,
                executor_id,
                status,
                duration_ms,
                shuffle_read_bytes,
                shuffle_write_bytes,
                output_rows,
            } => {
                if let Some(stage) = inner.stages.get_mut(&stage_key(operation_id, *stage_id)) {
                    if stage.operation_id == *operation_id {
                        if let Some(task) = stage.tasks.get_mut(task_id) {
                            task.status = *status;
                            task.finish_time_ms = Some(now_ms());
                            task.duration_ms = *duration_ms;
                            task.shuffle_read_bytes = *shuffle_read_bytes;
                            task.shuffle_write_bytes = *shuffle_write_bytes;
                            task.output_rows = *output_rows;
                        }
                    }
                }
                if let Some(exec) = inner.executors.get_mut(executor_id) {
                    exec.active_tasks = exec.active_tasks.saturating_sub(1);
                    exec.completed_tasks += 1;
                    exec.total_shuffle_read += shuffle_read_bytes;
                    exec.total_shuffle_write += shuffle_write_bytes;
                    exec.total_duration += duration_ms;
                }
            }
            ExecutionEvent::SqlPlanCaptured {
                operation_id,
                execution_id,
                description,
                physical_plan,
                logical_plan,
                job_ids,
            } => {
                if let Some(sql) = inner.sql_executions.get_mut(execution_id) {
                    if !description.is_empty() {
                        sql.description = description.clone();
                    }
                    if !physical_plan.is_empty() {
                        sql.physical_plan = physical_plan.clone();
                    }
                    if logical_plan.is_some() {
                        sql.logical_plan = logical_plan.clone();
                    }
                    if !job_ids.is_empty() {
                        sql.job_ids = job_ids.clone();
                    }
                } else {
                    inner.sql_executions.insert(
                        *execution_id,
                        InnerSql {
                            operation_id: operation_id.clone(),
                            execution_id: *execution_id,
                            description: description.clone(),
                            physical_plan: physical_plan.clone(),
                            logical_plan: logical_plan.clone(),
                            job_ids: job_ids.clone(),
                            submission_time_ms: now_ms(),
                            completion_time_ms: None,
                            status: JobStatus::Running,
                        },
                    );
                }
            }
            ExecutionEvent::ExecutorRegistered {
                executor_id,
                host_port,
            } => {
                inner
                    .executors
                    .entry(executor_id.clone())
                    .or_insert(InnerExecutor {
                        host_port: host_port.clone(),
                        active_tasks: 0,
                        completed_tasks: 0,
                        failed_tasks: 0,
                        total_shuffle_read: 0,
                        total_shuffle_write: 0,
                        total_duration: 0,
                    });
            }
            ExecutionEvent::AqeCoalesced { .. } => {}
        }
    }

    pub fn alloc_job_id(&self) -> i64 {
        self.next_job_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn alloc_sql_id(&self) -> i64 {
        self.next_sql_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn alloc_task_id(&self) -> i64 {
        self.next_task_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn application_info(&self) -> ApplicationInfo {
        let inner = self.inner.lock().expect("store poisoned");
        let end = inner
            .jobs
            .values()
            .filter_map(|j| j.completion_time_ms)
            .max();
        ApplicationInfo {
            id: self.app_id.clone(),
            name: self.app_name.clone(),
            attempts: vec![ApplicationAttemptInfo {
                attempt_id: "1".into(),
                start_time: Some(ms_to_iso(self.start_time_ms)),
                end_time: end.map(ms_to_iso),
                duration: end.map(|e| e - self.start_time_ms),
                spark_user: "weft".into(),
                app_spark_version: "4.0.0-weft".into(),
                completed: false,
            }],
        }
    }

    pub fn list_applications(&self) -> Vec<ApplicationInfo> {
        vec![self.application_info()]
    }

    pub fn list_jobs(&self, status_filter: Option<&str>) -> Vec<JobData> {
        let inner = self.inner.lock().expect("store poisoned");
        let mut jobs: Vec<JobData> = inner
            .jobs
            .values()
            .map(|j| self.job_to_data(j, &inner))
            .collect();
        if let Some(st) = status_filter {
            jobs.retain(|j| j.status.eq_ignore_ascii_case(st));
        }
        jobs.sort_by_key(|j| j.job_id);
        jobs
    }

    fn job_to_data(&self, j: &InnerJob, inner: &StoreInner) -> JobData {
        let stages: Vec<_> = j
            .stage_ids
            .iter()
            .filter_map(|sid| inner.stages.get(&stage_key(&j.operation_id, *sid)))
            .collect();
        let num_tasks: i32 = stages.iter().map(|s| s.num_tasks).sum();
        let num_completed_tasks = stages
            .iter()
            .flat_map(|s| s.tasks.values())
            .filter(|t| t.status == TaskStatus::Success)
            .count() as i32;
        let num_active_tasks = stages
            .iter()
            .flat_map(|s| s.tasks.values())
            .filter(|t| t.status == TaskStatus::Running)
            .count() as i32;
        let num_failed_tasks = stages
            .iter()
            .flat_map(|s| s.tasks.values())
            .filter(|t| t.status == TaskStatus::Failed)
            .count() as i32;
        let num_completed_stages = stages
            .iter()
            .filter(|s| s.status == StageStatus::Complete)
            .count() as i32;
        let num_active_stages = stages
            .iter()
            .filter(|s| s.status == StageStatus::Active)
            .count() as i32;
        let num_failed_stages = stages
            .iter()
            .filter(|s| s.status == StageStatus::Failed)
            .count() as i32;

        JobData {
            job_id: j.job_id,
            name: truncate(&j.description, 80),
            description: Some(j.description.clone()),
            submission_time: Some(ms_to_iso(j.submission_time_ms)),
            completion_time: j.completion_time_ms.map(ms_to_iso),
            stage_ids: j.stage_ids.clone(),
            status: job_status_str(j.status),
            num_tasks,
            num_active_tasks,
            num_completed_tasks,
            num_skipped_tasks: 0,
            num_failed_tasks,
            num_killed_tasks: 0,
            num_completed_stages,
            num_active_stages,
            num_failed_stages,
        }
    }

    pub fn list_stages(&self, status_filter: Option<&str>, with_details: bool) -> Vec<StageData> {
        let inner = self.inner.lock().expect("store poisoned");
        let mut stages: Vec<StageData> = inner
            .stages
            .values()
            .map(|s| self.stage_to_data(s, with_details))
            .collect();
        if let Some(st) = status_filter {
            stages.retain(|s| s.status.eq_ignore_ascii_case(st));
        }
        stages.sort_by_key(|s| s.stage_id);
        stages
    }

    pub fn get_stage(
        &self,
        stage_id: i32,
        attempt_id: i32,
        with_details: bool,
    ) -> Option<StageData> {
        let inner = self.inner.lock().expect("store poisoned");
        inner
            .stages
            .values()
            .filter(|s| s.stage_id == stage_id)
            .max_by_key(|s| s.submission_time_ms)
            .map(|s| self.stage_to_data(s, with_details))
            .map(|mut d| {
                d.attempt_id = attempt_id;
                d
            })
    }

    fn stage_to_data(&self, s: &InnerStage, with_details: bool) -> StageData {
        let tasks: Vec<_> = s.tasks.values().collect();
        let executor_run_time: i64 = tasks.iter().map(|t| t.duration_ms).sum();
        let num_complete = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Success)
            .count() as i32;
        let num_active = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Running)
            .count() as i32;
        let num_failed = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Failed)
            .count() as i32;

        StageData {
            status: stage_status_str(s.status),
            stage_id: s.stage_id,
            attempt_id: 0,
            num_tasks: s.num_tasks.max(tasks.len() as i32),
            num_active_tasks: num_active,
            num_complete_tasks: num_complete,
            num_failed_tasks: num_failed,
            num_killed_tasks: 0,
            submission_time: Some(ms_to_iso(s.submission_time_ms)),
            first_task_launched_time: tasks.iter().map(|t| t.launch_time_ms).min().map(ms_to_iso),
            completion_time: s.completion_time_ms.map(ms_to_iso),
            executor_run_time,
            executor_cpu_time: executor_run_time,
            input_bytes: 0,
            input_records: s.input_rows,
            output_bytes: 0,
            output_records: s.output_rows,
            shuffle_read_bytes: s.shuffle_read_bytes,
            shuffle_read_records: 0,
            shuffle_write_bytes: s.shuffle_write_bytes,
            shuffle_write_records: 0,
            name: s.name.clone(),
            description: Some(s.name.clone()),
            details: s.name.clone(),
            tasks: if with_details {
                Some(
                    tasks
                        .iter()
                        .map(|t| TaskData {
                            task_id: t.task_id,
                            index: t.task_id as i32,
                            attempt: 0,
                            partition_id: t.task_id as i32,
                            launch_time: Some(ms_to_iso(t.launch_time_ms)),
                            executor_id: t.executor_id.clone(),
                            host: t.executor_id.clone(),
                            status: task_status_str(t.status),
                            task_locality: "PROCESS_LOCAL".into(),
                            speculative: false,
                            getting_result_time: None,
                            finish_time: t.finish_time_ms.map(ms_to_iso),
                            executor_run_time: t.duration_ms,
                            executor_cpu_time: t.duration_ms,
                            result_size: 0,
                            disk_bytes_spilled: 0,
                            memory_bytes_spilled: 0,
                            input_bytes: 0,
                            input_records: 0,
                            output_bytes: 0,
                            output_records: t.output_rows,
                            shuffle_read_bytes: t.shuffle_read_bytes,
                            shuffle_read_records: 0,
                            shuffle_write_bytes: t.shuffle_write_bytes,
                            shuffle_write_records: 0,
                        })
                        .collect(),
                )
            } else {
                None
            },
        }
    }

    pub fn list_sql(&self) -> Vec<SqlExecution> {
        let inner = self.inner.lock().expect("store poisoned");
        let mut sql: Vec<SqlExecution> = inner
            .sql_executions
            .values()
            .map(|s| SqlExecution {
                id: s.execution_id,
                description: s.description.clone(),
                submission_time: Some(ms_to_iso(s.submission_time_ms)),
                completion_time: s.completion_time_ms.map(ms_to_iso),
                duration: s.completion_time_ms.map(|c| c - s.submission_time_ms),
                physical_plan: s.physical_plan.clone(),
                logical_plan: s.logical_plan.clone(),
                job_ids: s.job_ids.clone(),
                status: job_status_str(s.status),
            })
            .collect();
        sql.sort_by_key(|s| s.id);
        sql
    }

    pub fn list_executors(&self) -> Vec<ExecutorSummary> {
        let inner = self.inner.lock().expect("store poisoned");
        inner
            .executors
            .iter()
            .map(|(id, e)| ExecutorSummary {
                id: id.clone(),
                host_port: e.host_port.clone(),
                is_active: true,
                rdd_blocks: 0,
                memory_used: 0,
                disk_used: 0,
                total_cores: 1,
                max_tasks: 1,
                active_tasks: e.active_tasks,
                failed_tasks: e.failed_tasks,
                completed_tasks: e.completed_tasks,
                total_tasks: e.completed_tasks + e.active_tasks,
                total_duration: e.total_duration,
                total_gc_time: 0,
                total_input_bytes: 0,
                total_shuffle_read: e.total_shuffle_read,
                total_shuffle_write: e.total_shuffle_write,
                is_blacklisted: false,
            })
            .collect()
    }

    pub fn list_environment(&self) -> Vec<EnvironmentEntry> {
        let inner = self.inner.lock().expect("store poisoned");
        let mut entries: Vec<EnvironmentEntry> = inner
            .environment
            .iter()
            .map(|(k, v)| EnvironmentEntry {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        for (k, v) in std::env::vars() {
            if k.starts_with("WEFT_") || k.starts_with("SPARK_") {
                entries.push(EnvironmentEntry { key: k, value: v });
            }
        }
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries
    }

    /// Load events from a JSONL event log directory (history server).
    pub fn load_event_log(dir: &std::path::Path) -> Self {
        let store = Self::with_options("weft-history", "Weft History", None, DEFAULT_MAX_QUERIES);
        let path = dir.join("events.jsonl");
        if let Ok(content) = fs::read_to_string(path) {
            for line in content.lines() {
                if let Ok(event) = serde_json::from_str::<ExecutionEvent>(line) {
                    store.apply_event(&event);
                }
            }
        }
        store
    }
}

impl Default for AppStateStore {
    fn default() -> Self {
        Self::new()
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!(
            "{}…",
            s.chars().take(max.saturating_sub(1)).collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_stages_do_not_collide() {
        let store = AppStateStore::new();
        for (op, sid) in [("op-a", 0), ("op-b", 0)] {
            store.emit(ExecutionEvent::StageStarted {
                operation_id: op.into(),
                stage_id: sid,
                name: format!("stage for {op}"),
                num_tasks: 1,
                submission_time_ms: 1000,
            });
        }
        let stages = store.list_stages(None, false);
        assert_eq!(stages.len(), 2);
    }

    #[test]
    fn job_lifecycle_updates_store() {
        let store = AppStateStore::new();
        let op = "op-1".to_string();
        let job_id = store.alloc_job_id();
        store.emit(ExecutionEvent::JobStarted {
            operation_id: op.clone(),
            job_id,
            description: "SELECT 1".into(),
            submission_time_ms: 1000,
        });
        store.emit(ExecutionEvent::StageStarted {
            operation_id: op.clone(),
            stage_id: 0,
            name: "local".into(),
            num_tasks: 1,
            submission_time_ms: 1001,
        });
        store.emit(ExecutionEvent::JobFinished {
            operation_id: op.clone(),
            job_id,
            status: JobStatus::Succeeded,
            completion_time_ms: 2000,
            error: None,
        });
        let jobs = store.list_jobs(None);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, "SUCCEEDED");
        assert_eq!(store.operation_state(&op), Some(OperationState::Succeeded));
    }
}
