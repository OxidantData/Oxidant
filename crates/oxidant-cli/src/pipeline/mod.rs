//! `oxidant pipeline` — run a declarative table DAG from the config file.
//!
//! Before this, starting a streaming query meant running a Spark Connect server, installing a
//! PySpark client, and executing a Python script. The engine underneath was complete; the only
//! way *in* was a gRPC client. This module is the other door: a config file describes the
//! tables, and the binary builds them.
//!
//! Two kinds of table, and the difference is the whole design:
//!
//! - a **streaming table** declares a `source`, and each trigger appends one micro-batch to it
//!   through the existing [`oxidant_streaming`] engine — same Kafka source, same checkpoints,
//!   same exactly-once commit ordering a PySpark `writeStream` gets;
//! - a **derived table** is defined by SQL over other tables, and each trigger recomputes it in
//!   full and replaces its contents in one atomic Delta commit.
//!
//! Full recompute is a deliberate v1 choice, not an oversight. It is always correct, and it
//! needs no cross-batch state — which the engine does not have. It is also O(table) per update,
//! so a large gold aggregate on a fast trigger will dominate the interval. `docs/pipelines.md`
//! says so where users will read it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxidant_common::{Error, Result};
use oxidant_config::{
    ExpectAction, OxidantConfig, PipelineConfig, TableConfig, TableKind, Trigger,
};
use oxidant_loom::Engine;
use oxidant_streaming::{
    LakeSink, LakeSinkOptions, LakeTarget, MicroBatchInput, MicroBatchPipeline, StartOptions,
    StreamQueryConfig, StreamingQueryManager, Trigger as StreamTrigger,
};

mod expectations;
mod graph;

pub use graph::{Graph, Node};

/// The alias a streaming table's `sql:` uses to read its own source.
///
/// Fixed rather than configurable: a streaming table has exactly one source, so a name to choose
/// would be a name to get wrong.
pub const STREAM_ALIAS: &str = "stream";

/// What `oxidant pipeline` was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Parse, plan, and topologically sort; run nothing.
    Validate,
    /// Print the resolved DAG.
    Show,
    /// Build the tables.
    Run {
        /// Restrict to these tables and their ancestors. Empty means the whole graph.
        tables: Vec<String>,
        /// Force a single pass regardless of the configured trigger.
        once: bool,
    },
}

/// Flags that consume the token after them, so it is not mistaken for the subcommand.
const VALUE_FLAGS: &[&str] = &["--config", "-c", "--table"];

/// The first bare word after `pipeline`, skipping flags and their values.
fn subcommand(args: &[String]) -> Option<&str> {
    let mut rest = args.iter().skip_while(|a| a.as_str() != "pipeline").skip(1);
    while let Some(arg) = rest.next() {
        if VALUE_FLAGS.contains(&arg.as_str()) {
            rest.next();
            continue;
        }
        if arg.starts_with('-') {
            continue;
        }
        return Some(arg.as_str());
    }
    None
}

/// Parse the `pipeline` subcommand's arguments.
pub fn parse_command(args: &[String]) -> Result<Command> {
    // The first non-flag token after `pipeline` is the subcommand. Flags — and the values of the
    // ones that take a value — are skipped rather than mistaken for it, so
    // `oxidant pipeline --once` and `oxidant pipeline -c x.yaml run` both work, instead of failing
    // with "unknown subcommand `--once`" / "unknown subcommand `x.yaml`".
    let sub = subcommand(args);
    match sub {
        Some("validate") => Ok(Command::Validate),
        Some("show") => Ok(Command::Show),
        Some("run") | None => {
            let mut tables = Vec::new();
            for (i, arg) in args.iter().enumerate() {
                if arg == "--table" {
                    if let Some(name) = args.get(i + 1) {
                        tables.push(name.clone());
                    }
                } else if let Some(name) = arg.strip_prefix("--table=") {
                    tables.push(name.to_string());
                }
            }
            Ok(Command::Run {
                tables,
                once: args.iter().any(|a| a == "--once"),
            })
        }
        Some(other) => Err(Error::Io(format!(
            "unknown `oxidant pipeline` subcommand `{other}` (expected run, validate, or show)"
        ))),
    }
}

/// The config a pipeline run needs, with the sections it requires already checked.
struct Plan<'a> {
    config: &'a OxidantConfig,
    pipeline: &'a PipelineConfig,
    graph: Graph,
}

impl<'a> Plan<'a> {
    fn build(config: &'a OxidantConfig) -> Result<Self> {
        let pipeline = config.pipeline.as_ref().ok_or_else(|| {
            Error::Io(
                "this config declares no `pipeline:` section, so there is nothing to run — see \
                 docs/pipelines.md"
                    .into(),
            )
        })?;
        let graph = Graph::build(&config.tables)?;
        Ok(Self {
            config,
            pipeline,
            graph,
        })
    }

    fn table(&self, name: &str) -> Option<&'a TableConfig> {
        self.config.tables.iter().find(|t| t.name.trim() == name)
    }

    /// Fully-qualified target for a table: `{catalog}.{schema}.{name}`.
    fn target_of(&self, name: &str) -> String {
        format!("{}.{}.{name}", self.pipeline.catalog, self.pipeline.schema)
    }

    /// Sink format for a table, falling back to the pipeline default.
    fn format_of(&self, table: &TableConfig) -> String {
        table
            .format
            .clone()
            .unwrap_or_else(|| self.pipeline.format.clone())
    }

    /// Where a table's files live, when the pipeline pins a storage root.
    ///
    /// `None` lets the catalog choose via its warehouse convention, which is what makes a
    /// pipeline portable between a local warehouse and an S3 one.
    fn location_of(&self, name: &str) -> Option<String> {
        self.pipeline
            .storage
            .as_ref()
            .map(|root| format!("{}/{name}/", root.trim_end_matches('/')))
    }
}

/// Entry point for `oxidant pipeline ...`.
pub async fn run(config: Option<OxidantConfig>, command: Command) -> Result<()> {
    let config = config.ok_or_else(|| {
        Error::Io(
            "`oxidant pipeline` needs a config file: pass --config <FILE>, set $OXIDANT_CONFIG, \
             or add ./oxidant.yaml"
                .into(),
        )
    })?;
    let plan = Plan::build(&config)?;

    match command {
        Command::Validate => {
            // Everything above has already parsed the config, resolved every table's SQL to its
            // references, and topologically sorted the graph. Reaching here is the result.
            println!("pipeline `{}` is valid", plan.pipeline.name);
            println!(
                "  {} table(s), update order: {}",
                plan.graph.order.len(),
                plan.graph
                    .order
                    .iter()
                    .map(|n| n.name.as_str())
                    .collect::<Vec<_>>()
                    .join(" -> ")
            );
            Ok(())
        }
        Command::Show => {
            print_graph(&plan);
            Ok(())
        }
        Command::Run { tables, once } => run_pipeline(&plan, &tables, once).await,
    }
}

fn print_graph(plan: &Plan<'_>) {
    println!("pipeline: {}", plan.pipeline.name);
    println!(
        "target:   {}.{}",
        plan.pipeline.catalog, plan.pipeline.schema
    );
    if let Some(storage) = &plan.pipeline.storage {
        println!("storage:  {storage}");
    }
    println!("trigger:  {:?}", plan.pipeline.trigger);
    println!();
    for node in &plan.graph.order {
        let Some(table) = plan.table(&node.name) else {
            continue;
        };
        let kind = match table.kind() {
            TableKind::Streaming => "streaming",
            TableKind::Derived => "derived",
        };
        println!("  {} ({kind}, {})", node.name, plan.format_of(table));
        if !node.depends_on.is_empty() {
            println!("      reads: {}", node.depends_on.join(", "));
        }
        if !table.partition_by.is_empty() {
            println!("      partitioned by: {}", table.partition_by.join(", "));
        }
        for (name, expectation) in &table.expect {
            println!(
                "      expect {name}: {} ({:?})",
                expectation.check, expectation.action
            );
        }
    }
}

/// One table's outcome in a single pass.
#[derive(Debug)]
struct TableOutcome {
    name: String,
    rows: u64,
    elapsed: Duration,
    /// True when the table was left alone because nothing it reads moved this pass.
    unchanged: bool,
    /// `Some` when this table failed; its descendants are skipped for the pass.
    error: Option<String>,
    skipped: bool,
}

async fn run_pipeline(plan: &Plan<'_>, wanted: &[String], force_once: bool) -> Result<()> {
    let nodes = plan.graph.subgraph(wanted)?;
    if nodes.is_empty() {
        return Err(Error::Io("nothing to run".into()));
    }
    // `engine:` was applied in `main`, before the runtime started — `set_var` from inside a
    // multi-threaded runtime races every other thread's `getenv`.
    let catalogs: std::collections::HashMap<String, String> =
        plan.config.catalog_conf().into_iter().collect();
    let service = oxidant_connect::OxidantService::with_catalogs(catalogs).await;
    let engine = service.engine();

    // Create the target database before anything else. The sink would create it too, but doing
    // it here means a pipeline pointed at a catalog it cannot write to fails immediately rather
    // than after reading a source.
    ensure_database(&engine, plan).await?;

    // Point the session's catalog pointers at the pipeline's target. This is what SPI-level
    // operations (DDL, catalog RPCs) resolve against.
    engine.set_current_catalog(&plan.pipeline.catalog).await?;
    engine.set_current_namespace(&plan.pipeline.schema).await?;

    let trigger = if force_once {
        Trigger::Once
    } else {
        plan.pipeline.trigger.clone()
    };

    let mut streams = StreamState::start(&engine, plan, &nodes).await?;
    let mut state = PipelineState::load(&plan.pipeline.checkpoints);
    eprintln!(
        "[oxidant] pipeline `{}`: {} table(s), order: {}",
        plan.pipeline.name,
        nodes.len(),
        nodes
            .iter()
            .map(|n| n.name.as_str())
            .collect::<Vec<_>>()
            .join(" -> ")
    );

    match trigger {
        Trigger::Once | Trigger::AvailableNow => {
            let outcomes = one_pass(&engine, plan, &nodes, &mut streams, &mut state, true).await;
            // Saved even when a table failed: the epochs already issued must never be reused.
            if let Err(e) = state.save(&plan.pipeline.checkpoints) {
                eprintln!("[oxidant] could not persist pipeline state: {e}");
            }
            report(&outcomes);
            if outcomes.iter().any(|o| o.error.is_some()) {
                return Err(Error::Execution(
                    "the pipeline finished with failed tables".into(),
                ));
            }
            Ok(())
        }
        Trigger::ProcessingTime(interval) => {
            // Fixed-rate rather than sleep-after-work: sleeping the full interval *after* each
            // pass makes the real period `interval + pass duration`, so a loaded pipeline
            // silently halves its own trigger rate.
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let outcomes =
                    one_pass(&engine, plan, &nodes, &mut streams, &mut state, false).await;
                if let Err(e) = state.save(&plan.pipeline.checkpoints) {
                    eprintln!("[oxidant] could not persist pipeline state: {e}");
                }
                report(&outcomes);
            }
        }
    }
}

/// Create the pipeline's target database if it does not exist.
async fn ensure_database(engine: &Engine, plan: &Plan<'_>) -> Result<()> {
    let catalog = engine
        .external_catalog(&plan.pipeline.catalog)
        .ok_or_else(|| {
            Error::Plan(format!(
                "pipeline catalog `{}` is not registered — is it declared under `catalogs:`?",
                plan.pipeline.catalog
            ))
        })?;
    catalog
        .create_database(
            &plan.pipeline.schema,
            true,
            Some(format!("Oxidant pipeline `{}`", plan.pipeline.name)),
            None,
        )
        .await
}

/// The streaming queries started for this run, keyed by table name.
struct StreamState {
    manager: Arc<StreamingQueryManager>,
    queries: BTreeMap<String, String>,
}

impl StreamState {
    /// Start a streaming query for every streaming table in `nodes`.
    ///
    /// Started up front, not lazily on the first pass, so a misconfigured source or an
    /// unwritable sink fails while the operator is still watching the command.
    async fn start(engine: &Engine, plan: &Plan<'_>, nodes: &[Node]) -> Result<Self> {
        let manager = Arc::new(StreamingQueryManager::new());
        let mut queries = BTreeMap::new();
        for node in nodes {
            let Some(table) = plan.table(&node.name) else {
                continue;
            };
            if table.kind() != TableKind::Streaming {
                continue;
            }
            let id = start_stream(engine, plan, table, &manager).await?;
            queries.insert(node.name.clone(), id);
        }
        Ok(Self { manager, queries })
    }
}

/// Configure and register one streaming table's query, returning its id.
async fn start_stream(
    engine: &Engine,
    plan: &Plan<'_>,
    table: &TableConfig,
    manager: &StreamingQueryManager,
) -> Result<String> {
    let source = table
        .source
        .as_ref()
        .expect("a streaming table has a source");
    let name = table.name.trim();

    let config = StreamQueryConfig::for_pipeline(
        &source.format,
        source.options.clone().into_iter().collect(),
        &plan.format_of(table),
        plan.target_of(name),
        plan.location_of(name),
        table.partition_by.clone(),
        table.dedup_columns.clone(),
        table.iceberg_compat.unwrap_or(plan.pipeline.iceberg_compat),
        table.iceberg_table_suffix.clone(),
        table.checkpoint_interval,
    );
    // `warn` and `fail` are counted against each micro-batch. `drop` is not here: it composes
    // into the query below as a predicate, so it costs nothing extra and is applied first —
    // meaning a row a `drop` removed is not also counted as a violation.
    let mut config = config;
    config.expectations = expectations::counted(&table.expect)
        .into_iter()
        .map(
            |(label, expectation)| oxidant_streaming::StreamExpectation {
                label: label.to_string(),
                action: match expectation.action {
                    ExpectAction::Fail => oxidant_streaming::ExpectationAction::Fail,
                    _ => oxidant_streaming::ExpectationAction::Warn,
                },
                check: expectation.check.clone(),
            },
        )
        .collect();

    // The source's own schema, then the table's `sql:` planned against it. Without a `sql:` the
    // source's rows go to the sink unchanged — which for Kafka means the seven raw columns, and
    // is almost never what a user wants, but is exactly what Spark does.
    //
    // A table with no `sql:` but with `drop` expectations still needs a query to hang the filter
    // on, so one is synthesized. Without this the expectations parse, validate, and are then
    // silently never applied — the rows they promise to drop all land in the table.
    let declared_sql = table
        .sql
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let synthesized = declared_sql
        .is_none()
        .then(|| {
            expectations::has_drops(&table.expect).then(|| format!("SELECT * FROM {STREAM_ALIAS}"))
        })
        .flatten();
    let pipeline = match declared_sql.or(synthesized.as_deref()) {
        Some(sql) => {
            let schema = oxidant_streaming::source_schema(&config)?;
            let input = Arc::new(MicroBatchInput::new(STREAM_ALIAS, schema)?);
            // Register under the fixed alias so the table's SQL can name it. Deregistered
            // afterwards: the plan captured the provider by `Arc`, so it keeps working, and
            // leaving the name bound would let a *derived* table read a streaming table's raw
            // micro-batch instead of its materialized contents.
            engine
                .ctx()
                .register_table(STREAM_ALIAS, input.provider())
                .map_err(|e| Error::Plan(format!("register streaming input: {e}")))?;
            let sql = expectations::apply_drops(sql, &table.expect);
            let plan_result = engine.logical_plan(&sql).await;
            engine.deregister_table(STREAM_ALIAS);
            Some(MicroBatchPipeline {
                input,
                plan: plan_result?,
            })
        }
        None => None,
    };

    let checkpoint = format!("{}/{name}", plan.pipeline.checkpoints.trim_end_matches('/'));
    let id = manager
        .start_with_config(
            engine,
            name.to_string(),
            checkpoint,
            // The pipeline's own loop drives the batches, so the query is registered with a
            // trigger it never fires on its own.
            StreamTrigger::AvailableNow,
            config,
            StartOptions {
                pipeline,
                current_catalog: plan.pipeline.catalog.clone(),
                current_namespace: vec![plan.pipeline.schema.clone()],
            },
        )
        .await?;
    Ok(id.id)
}

/// Run every table once, in order.
async fn one_pass(
    engine: &Engine,
    plan: &Plan<'_>,
    nodes: &[Node],
    streams: &mut StreamState,
    state: &mut PipelineState,
    drain: bool,
) -> Vec<TableOutcome> {
    let mut outcomes: Vec<TableOutcome> = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    // Tables whose contents actually moved this pass. A derived table reading only tables that
    // did not move has nothing new to compute.
    let mut changed: Vec<String> = Vec::new();

    for node in nodes {
        // A table whose upstream failed this pass would be computed from stale or missing data.
        // Skipping it — and letting the rest of the graph run — keeps a broken gold table from
        // stopping bronze ingestion.
        if node.depends_on.iter().any(|d| failed.contains(d)) {
            outcomes.push(TableOutcome {
                name: node.name.clone(),
                rows: 0,
                elapsed: Duration::ZERO,
                error: None,
                skipped: true,
                unchanged: false,
            });
            failed.push(node.name.clone());
            continue;
        }
        let Some(table) = plan.table(&node.name) else {
            continue;
        };
        // Skip a recompute that cannot produce anything new. A derived table is O(whole table)
        // per update and rewrites every file in one commit, so an idle pipeline on a 30-second
        // trigger would otherwise rebuild every gold table 2,880 times a day — churning the
        // version history and producing a fresh set of small files each time.
        //
        // Three conditions have to hold before skipping is safe, and each covers a real way to
        // serve stale numbers: the table must have been built before, nothing it reads inside
        // the pipeline may have moved, and it must not read anything *outside* the pipeline —
        // an external lake table can be rewritten by anyone, so its freshness is unknowable.
        let definition = definition_fingerprint(table);
        if table.kind() == TableKind::Derived
            && state.built_as(&node.name, &definition)
            && !node.reads_outside_pipeline
            && !node.depends_on.iter().any(|d| changed.contains(d))
        {
            // Still bind the bare name. Downstream SQL says `FROM orders_silver`, and that
            // resolves through a temporary view — so a table left alone this pass must keep its
            // alias, or skipping it breaks every table that reads it.
            if let Err(e) = bind_bare_name(engine, plan, &node.name).await {
                eprintln!(
                    "[oxidant] {:<24} warning: could not alias its bare name ({e})",
                    node.name
                );
            }
            outcomes.push(TableOutcome {
                name: node.name.clone(),
                rows: 0,
                elapsed: Duration::ZERO,
                error: None,
                skipped: false,
                unchanged: true,
            });
            continue;
        }

        let started = Instant::now();
        let result = match table.kind() {
            TableKind::Streaming => advance_stream(engine, streams, &node.name, drain).await,
            TableKind::Derived => recompute(engine, plan, table, state).await,
        };
        match result {
            Ok(rows) => {
                if rows > 0 || table.kind() == TableKind::Derived {
                    changed.push(node.name.clone());
                }
                if table.kind() == TableKind::Derived {
                    state.mark_built(&node.name, &definition);
                }
                // Make the freshly-written table visible to everything downstream, by its bare
                // name and with its new contents. Both halves are needed and neither is
                // obvious: without the refresh a derived table reads whatever the catalog cache
                // held at plan time, and without the alias `SELECT * FROM orders_bronze` does
                // not resolve at all.
                if let Err(e) = bind_bare_name(engine, plan, &node.name).await {
                    eprintln!(
                        "[oxidant] {:<24} warning: could not alias its bare name ({e}); \
                         downstream tables must use the fully-qualified name",
                        node.name
                    );
                }
                outcomes.push(TableOutcome {
                    name: node.name.clone(),
                    rows,
                    elapsed: started.elapsed(),
                    error: None,
                    skipped: false,
                    unchanged: false,
                });
            }
            Err(e) => {
                failed.push(node.name.clone());
                outcomes.push(TableOutcome {
                    name: node.name.clone(),
                    rows: 0,
                    elapsed: started.elapsed(),
                    error: Some(e.to_string()),
                    skipped: false,
                    unchanged: false,
                });
            }
        }
    }
    outcomes
}

/// Make `name` resolvable as a bare identifier, pointing at its materialized contents.
///
/// A pipeline's tables live at `{catalog}.{schema}.{name}`, but the natural way to write a
/// derived table is `SELECT * FROM orders_bronze`. Session catalog pointers do not help here:
/// they steer the catalog SPI, while DataFusion resolves a bare table reference against the
/// session's own default catalog and schema. A view bridges the two without rewriting the
/// user's SQL — which is the alternative, and a far more fragile one.
///
/// The table is refreshed first so the view sees the version just written rather than whatever
/// the catalog cache holds.
async fn bind_bare_name(engine: &Engine, plan: &Plan<'_>, name: &str) -> Result<()> {
    let target = plan.target_of(name);
    // Best-effort: a table that was never cached has nothing to refresh, which is not an error.
    let _ = engine.refresh_table(&target).await;
    engine
        .sql(&format!(
            "CREATE OR REPLACE TEMPORARY VIEW {name} AS SELECT * FROM {target}"
        ))
        .await
        .map(|_| ())
}

/// Advance one streaming table by a micro-batch, or drain it entirely.
async fn advance_stream(
    engine: &Engine,
    streams: &StreamState,
    name: &str,
    drain: bool,
) -> Result<u64> {
    let Some(id) = streams.queries.get(name) else {
        return Ok(0);
    };
    if drain {
        streams.manager.process_all_available(id, engine).await
    } else {
        streams.manager.run_batch(id, engine).await
    }
}

/// Recompute a derived table and replace its contents.
async fn recompute(
    engine: &Engine,
    plan: &Plan<'_>,
    table: &TableConfig,
    state: &mut PipelineState,
) -> Result<u64> {
    let name = table.name.trim();
    let sql = table
        .sql
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Plan(format!("derived table `{name}` has no `sql:`")))?;

    // `fail` and `warn` expectations are evaluated against the query *before* any dropping, so
    // a row that a `drop` would remove still counts as a violation for a `fail` on the same
    // table. Otherwise the two would cancel out and `fail` would never fire.
    for (label, expectation) in expectations::counted(&table.expect) {
        let count_sql = expectations::violation_count_sql(sql, &expectation.check);
        let violations = scalar_count(engine, &count_sql).await?;
        if violations == 0 {
            continue;
        }
        match expectation.action {
            ExpectAction::Fail => {
                return Err(Error::Execution(format!(
                    "table `{name}` expectation `{label}` failed: {violations} record(s) do not \
                     satisfy `{}`; the table is unchanged",
                    expectation.check
                )))
            }
            ExpectAction::Warn => {
                eprintln!("[oxidant] table={name} expectation={label} failed_records={violations}")
            }
            ExpectAction::Drop => {}
        }
    }

    let effective_sql = expectations::apply_drops(sql, &table.expect);
    let batches = engine.sql(&effective_sql).await?;
    // An empty result still has to declare the table with the right columns, so fall back to
    // the plan's own output schema rather than failing. A gold table that legitimately matches
    // no rows on its first update must still exist, and be empty.
    let schema = match batches.first() {
        Some(batch) => batch.schema(),
        None => engine.schema(&effective_sql).await?,
    };

    let format = oxidant_streaming::writable_format(&plan.format_of(table))?;
    let target = LakeTarget::from_table_identifier(
        &plan.target_of(name),
        &plan.pipeline.catalog,
        std::slice::from_ref(&plan.pipeline.schema),
        format,
        plan.location_of(name),
    )?;
    let mut sink = LakeSink::open(
        engine,
        target,
        schema,
        LakeSinkOptions {
            // The table's identity across runs, so a replayed recompute is recognized.
            app_id: Some(format!("{}::{name}", plan.pipeline.name)),
            partition_columns: table.partition_by.clone(),
            publish_iceberg: table.iceberg_compat.unwrap_or(plan.pipeline.iceberg_compat),
            iceberg_table_suffix: table
                .iceberg_table_suffix
                .clone()
                .unwrap_or_else(|| oxidant_streaming::DEFAULT_ICEBERG_SUFFIX.to_string()),
            checkpoint_interval: table
                .checkpoint_interval
                .unwrap_or(oxidant_streaming::DEFAULT_CHECKPOINT_INTERVAL),
        },
    )
    .await?;

    // A strictly increasing version per recompute, so the Delta `txn` stamp distinguishes a
    // genuine update from a replayed one. Durable, so it survives a restart — and taken above
    // whatever the table already carries, so a lost or corrupt state file cannot issue a version
    // the sink would recognize as already committed and silently discard.
    let version = state
        .next_epoch(name)
        .max(sink.committed_txn_version().max(0) as u64 + 1);
    state.tables.entry(name.to_string()).or_default().epoch = version;
    sink.replace_batch(&batches, version).await
}

/// Per-table pipeline state, persisted beside the checkpoints.
///
/// Two things live here, and both exist because the process's memory is not a safe place for
/// them:
///
/// - **`epoch`** — the version stamped into a derived table's Delta `txn` action. It has to be
///   monotonic across restarts, and it used to come from the wall clock for want of anywhere to
///   keep a counter. That reintroduced the very failure it was working around: an NTP step
///   backwards puts `now` below the last committed version, the sink deduplicates every
///   recompute, and the table silently stops updating. A durable counter cannot skew.
/// - **`built`** — whether the table has ever been materialized, which is what lets an idle pass
///   skip a recompute without risking a table that was never built in the first place.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PipelineState {
    tables: BTreeMap<String, TableState>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct TableState {
    /// Highest `txn` version issued for this table.
    epoch: u64,
    /// Whether a recompute has ever succeeded.
    built: bool,
    /// Fingerprint of the table's declaration as of the last successful recompute.
    ///
    /// Skipping an idle recompute is only sound while the table still *means* the same thing.
    /// Editing its `sql:`, adding an expectation, or changing its partitioning all change the
    /// answer even though no upstream moved — and a newly added `fail` expectation that never
    /// fired because the table was skipped is precisely the silently-inert check this codebase
    /// keeps rejecting elsewhere.
    #[serde(default)]
    definition: String,
}

impl PipelineState {
    fn path(checkpoints: &str) -> PathBuf {
        Path::new(checkpoints).join("_pipeline-state.json")
    }

    /// Read the state, treating anything unreadable as absent.
    ///
    /// A missing or corrupt state file must not stop the pipeline: the worst it costs is one
    /// redundant recompute per table, and the `txn` epoch is repaired below by taking the
    /// maximum with what the sink already carries.
    fn load(checkpoints: &str) -> Self {
        std::fs::read(Self::path(checkpoints))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    fn save(&self, checkpoints: &str) -> Result<()> {
        let path = Self::path(checkpoints);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Io(format!("creating `{}`: {e}", parent.display())))?;
        }
        let text = serde_json::to_vec_pretty(self)
            .map_err(|e| Error::Io(format!("serializing pipeline state: {e}")))?;
        // Written through a temporary file and renamed, so a crash mid-write cannot leave a
        // half-parsed state that reads as "nothing was ever built".
        let staging = path.with_extension("json.tmp");
        std::fs::write(&staging, text)
            .map_err(|e| Error::Io(format!("writing `{}`: {e}", staging.display())))?;
        std::fs::rename(&staging, &path)
            .map_err(|e| Error::Io(format!("writing `{}`: {e}", path.display())))?;
        Ok(())
    }

    /// The next `txn` version for `table`.
    fn next_epoch(&mut self, table: &str) -> u64 {
        let slot = self.tables.entry(table.to_string()).or_default();
        slot.epoch += 1;
        slot.epoch
    }

    /// Whether `table` has been built before *and* still has the declaration it was built from.
    fn built_as(&self, table: &str, definition: &str) -> bool {
        self.tables
            .get(table)
            .is_some_and(|t| t.built && t.definition == definition)
    }

    fn mark_built(&mut self, table: &str, definition: &str) {
        let slot = self.tables.entry(table.to_string()).or_default();
        slot.built = true;
        slot.definition = definition.to_string();
    }
}

/// A stable fingerprint of a table's declaration.
///
/// The whole config value is hashed rather than a hand-picked subset: choosing which fields
/// matter is exactly the judgement that goes stale when a field is added later, and the cost of
/// being wrong is a table that silently stops reflecting its own definition.
fn definition_fingerprint(table: &TableConfig) -> String {
    use std::hash::{Hash, Hasher};
    let text = serde_json::to_string(table).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Run a `SELECT count(*)`-shaped query and return the scalar.
async fn scalar_count(engine: &Engine, sql: &str) -> Result<u64> {
    let batches = engine.sql(sql).await?;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(values) = batch
            .column(0)
            .as_any()
            .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
        {
            return Ok(values.value(0).max(0) as u64);
        }
    }
    Ok(0)
}

/// Print one pass's outcomes.
fn report(outcomes: &[TableOutcome]) {
    for outcome in outcomes {
        if outcome.unchanged {
            eprintln!(
                "[oxidant] {:<24} unchanged (nothing it reads moved this pass)",
                outcome.name
            );
            continue;
        }
        if outcome.skipped {
            eprintln!(
                "[oxidant] {:<24} skipped (an upstream table failed this pass)",
                outcome.name
            );
        } else if let Some(error) = &outcome.error {
            eprintln!("[oxidant] {:<24} FAILED: {error}", outcome.name);
        } else {
            eprintln!(
                "[oxidant] {:<24} {} row(s) in {:.2}s",
                outcome.name,
                outcome.rows,
                outcome.elapsed.as_secs_f64()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn subcommands_parse() {
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "validate"])).unwrap(),
            Command::Validate
        );
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "show"])).unwrap(),
            Command::Show
        );
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "run"])).unwrap(),
            Command::Run {
                tables: vec![],
                once: false
            }
        );
    }

    #[test]
    fn flags_before_the_subcommand_are_not_mistaken_for_one() {
        // `oxidant pipeline --once` used to fail with "unknown subcommand `--once`" even though a
        // bare `oxidant pipeline` is a documented alias for `run`.
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "--once"])).unwrap(),
            Command::Run {
                tables: vec![],
                once: true
            }
        );
        // A flag's *value* must not be read as the subcommand either.
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "-c", "x.yaml"])).unwrap(),
            Command::Run {
                tables: vec![],
                once: false
            }
        );
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "-c", "x.yaml", "validate"])).unwrap(),
            Command::Validate
        );
        // And a real typo still has to be caught rather than silently defaulting to `run`.
        assert!(parse_command(&args(&["oxidant", "pipeline", "-c", "x.yaml", "shwo"])).is_err());
    }

    #[test]
    fn a_bare_pipeline_defaults_to_run() {
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline"])).unwrap(),
            Command::Run {
                tables: vec![],
                once: false
            }
        );
    }

    #[test]
    fn table_selection_accepts_both_spellings_and_repeats() {
        let command = parse_command(&args(&[
            "oxidant",
            "pipeline",
            "run",
            "--table",
            "gold",
            "--table=silver",
            "--once",
        ]))
        .unwrap();
        assert_eq!(
            command,
            Command::Run {
                tables: vec!["gold".into(), "silver".into()],
                once: true
            }
        );
    }

    #[test]
    fn an_unknown_subcommand_is_rejected_rather_than_treated_as_run() {
        // Treating `oxidant pipeline vlaidate` as `run` would start building tables when the
        // user asked for a dry check.
        let err = parse_command(&args(&["oxidant", "pipeline", "vlaidate"])).expect_err("typo");
        assert!(err.to_string().contains("vlaidate"), "got: {err}");
    }
}
