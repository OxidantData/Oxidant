//! `oxidant-execution` — local and distributed execution of physical plans.
//!
//! Local mode runs the physical plan across a thread pool (morsel-driven). Distributed
//! mode splits the plan into stages at shuffle boundaries: a driver/worker control plane and
//! an Arrow Flight data plane for shuffle + result return. The MVP shape is two-stage
//! `partial-agg → hash shuffle → final-agg` ([`driver::run_distributed`]). Large stage
//! buckets spill to disk when over `OXIDANT_SHUFFLE_SPILL_BYTES` / `OXIDANT_MEMORY_LIMIT_BYTES` (see
//! [`shuffle::spill`]); `do_exchange` consumes shuffle pushes incrementally with backpressure.

pub mod aqe;
pub mod autoscale;
pub mod dag_dispatch;
pub mod driver;
pub mod fault_inject;
pub mod flight;
pub mod lineage;
pub mod membership;
pub mod plan;
pub mod scheduler;
pub mod shuffle;

use std::sync::Arc;

use oxidant_common::{Error, Result};
use oxidant_loom::Engine;

/// Execution mode for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Single process, multi-threaded.
    Local,
    /// Driver + workers, Arrow Flight shuffle.
    Distributed,
}

/// The role this process plays when running distributed.
#[derive(Debug, Clone)]
pub enum Role {
    /// Run as a worker, serving a Flight endpoint on `port`.
    Worker { port: u16 },
    /// Run as the driver, orchestrating `plan` across `workers`.
    Driver {
        workers: Vec<String>,
        plan: driver::DistributedPlan,
    },
}

/// How to run: the mode and (for distributed) the role.
pub struct RunConfig {
    /// The execution mode.
    pub mode: Mode,
    /// The role to play (only consulted for [`Mode::Distributed`]).
    pub role: Option<Role>,
    /// The engine this process uses (a worker's local engine, or the driver's local engine).
    pub engine: Arc<Engine>,
}

/// Drive execution per [`RunConfig`]. For a worker this blocks serving Flight; for a driver it
/// runs the distributed plan and returns its result. Local mode is a no-op seam today (the
/// server executes directly on its [`Engine`]).
pub async fn run(cfg: RunConfig) -> Result<Vec<oxidant_loom::arrow::record_batch::RecordBatch>> {
    match cfg.mode {
        Mode::Local => Ok(Vec::new()),
        Mode::Distributed => match cfg.role {
            Some(Role::Worker { port }) => {
                flight::serve_worker(port, cfg.engine).await?;
                Ok(Vec::new())
            }
            Some(Role::Driver { workers, plan }) => {
                let cluster = driver::Cluster::new(workers);
                driver::run_distributed(&cluster, &plan).await
            }
            None => Err(Error::Unsupported(
                "distributed mode requires a Role (worker or driver)".into(),
            )),
        },
    }
}
