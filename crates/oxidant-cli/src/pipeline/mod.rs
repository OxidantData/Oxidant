//! `oxidant pipeline` — CLI entry: argument parsing and rendering for the library runner.

use oxidant_common::{Error, Result};
use oxidant_config::{OxidantConfig, TableKind};
use oxidant_pipelines::{
    run_pipeline, set_schedule, Plan, ReconcileOptions, ReconcileSchedule, RunEvent, RunEventKind,
    DEFAULT_SAMPLE,
};

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
    /// Report drift between a `postgres_cdc` source and the tables it feeds. Reads only.
    Reconcile {
        /// Restrict to these tables — a pipeline table, or an upstream `schema.table`.
        tables: Vec<String>,
        /// Keys walked per table.
        sample: usize,
        /// `Some(expr)` registers a schedule instead of running now; `Some("off")` clears it.
        cron: Option<String>,
    },
}

/// What `--cron` takes to mean "stop running this on a schedule".
const CRON_OFF: &[&str] = &["off", "none", "clear"];

/// Flags that consume the token after them, so it is not mistaken for the subcommand.
const VALUE_FLAGS: &[&str] = &["--config", "-c", "--table", "--sample", "--cron"];

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

/// Every `--table <NAME>` / `--table=<NAME>`, in the order they were given.
fn table_flags(args: &[String]) -> Vec<String> {
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
    tables
}

/// `--flag <VALUE>` or `--flag=<VALUE>`.
fn value_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let equals = format!("{flag}=");
    args.iter().enumerate().find_map(|(i, arg)| {
        if arg == flag {
            args.get(i + 1).map(String::as_str)
        } else {
            arg.strip_prefix(&equals)
        }
    })
}

/// Parse the `pipeline` subcommand's arguments.
pub fn parse_command(args: &[String]) -> Result<Command> {
    let sub = subcommand(args);
    match sub {
        Some("validate") => Ok(Command::Validate),
        Some("show") => Ok(Command::Show),
        Some("reconcile") => {
            let sample = match value_flag(args, "--sample") {
                None => DEFAULT_SAMPLE,
                Some(text) => text
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        Error::Io(format!(
                            "`--sample {text}` is not a number of keys (a positive integer)"
                        ))
                    })?,
            };
            let cron = value_flag(args, "--cron").map(str::to_string);
            if cron.as_deref().is_some_and(|c| c.trim().is_empty()) {
                return Err(Error::Io(
                    "`--cron` needs an expression, for example `--cron '0 6 * * *'` (or \
                     `--cron off` to clear a registered schedule)"
                        .into(),
                ));
            }
            Ok(Command::Reconcile {
                tables: table_flags(args),
                sample,
                cron,
            })
        }
        Some("run") | None => Ok(Command::Run {
            tables: table_flags(args),
            once: args.iter().any(|a| a == "--once"),
        }),
        Some(other) => Err(Error::Io(format!(
            "unknown `oxidant pipeline` subcommand `{other}` (expected run, validate, show, or \
             reconcile)"
        ))),
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
        Command::Run { tables, once } => {
            let engine = crate::embedded::build_engine(Some(&config), None).await?;
            let once_tables = std::collections::HashSet::new();
            run_pipeline(
                &engine,
                &plan,
                &tables,
                once,
                &once_tables,
                &mut render_event,
            )
            .await
        }
        Command::Reconcile {
            tables,
            sample,
            cron,
        } => {
            let options = ReconcileOptions { tables, sample };
            if let Some(cron) = cron {
                return register_schedule(&plan, &cron, config.source_path.as_deref(), &options);
            }
            // Every exit from here carries a status code the docs name, so an error out of the
            // reconcile itself must not fall through to the generic handler in `main` — that
            // exits 1, which is the code reserved for "the comparison ran and found drift".
            let engine = match crate::embedded::build_engine(Some(&config), None).await {
                Ok(engine) => engine,
                Err(e) => reconcile_failed(&e),
            };
            let report = match oxidant_pipelines::reconcile(&engine, &plan, &options).await {
                Ok(report) => report,
                Err(e) => reconcile_failed(&e),
            };
            // The report goes to stdout — it is the command's output, and a cron job pipes it.
            print!("{}", report.render());
            // Exit here rather than returning an error, so drift is reported as a full report
            // with a status code and not as `oxidant: <message>` on stderr. `run` cannot express
            // "succeeded, and the answer is no" any other way.
            let code = report.exit_code();
            if code != oxidant_pipelines::EXIT_IN_SYNC {
                std::process::exit(code);
            }
            Ok(())
        }
    }
}

/// A reconcile that could not run: `EXIT_FAILED`, never the exit code drift owns.
///
/// `docs/cli.md` and `docs/pipelines.md` both publish this split, and a CI step written as
/// `reconcile || page_the_data_team` is the reason it matters: a network blip should not read the
/// same way as a target that stopped saying what the source says.
fn reconcile_failed(error: &Error) -> ! {
    eprintln!("oxidant: {error}");
    std::process::exit(oxidant_pipelines::EXIT_FAILED)
}

/// `reconcile --cron <EXPR>`: persist the schedule (or clear it) and say what changed.
fn register_schedule(
    plan: &Plan<'_>,
    cron: &str,
    config_path: Option<&std::path::Path>,
    options: &ReconcileOptions,
) -> Result<()> {
    let checkpoints = plan.pipeline.checkpoints.as_str();
    if CRON_OFF.contains(&cron.trim().to_ascii_lowercase().as_str()) {
        if ReconcileSchedule::remove(checkpoints)? {
            println!(
                "reconcile schedule removed from {}",
                ReconcileSchedule::path_in(checkpoints).display()
            );
        } else {
            println!(
                "pipeline `{}` had no reconcile schedule",
                plan.pipeline.name
            );
        }
        return Ok(());
    }
    let schedule = set_schedule(plan, cron, config_path, options)?;
    println!(
        "reconcile scheduled for pipeline `{}`: `{}`",
        plan.pipeline.name, schedule.cron
    );
    println!(
        "  written to {}",
        ReconcileSchedule::path_in(checkpoints).display()
    );
    println!(
        "  next: {}",
        oxidant_pipelines::Cron::parse(&schedule.cron)
            .ok()
            .and_then(|c| c.next_after(schedule.anchor()))
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_else(|| "never".into())
    );
    // `pipeline run` is what ticks it; saying so here is cheaper than an operator discovering
    // that a schedule registered on a laptop never fires.
    println!(
        "  ticked by `oxidant pipeline run` between triggers; `oxidant pipeline reconcile` \
         remains the on-demand path"
    );
    Ok(())
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
            TableKind::AutoCdc => "auto_cdc",
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
    print_schedule(plan);
}

/// The registered `reconcile --cron` schedule, when there is one.
///
/// Printed by `show` because the schedule lives in the checkpoint directory rather than in the
/// config file: without this, the only way to find out whether a pipeline reconciles itself is to
/// go looking for a JSON file, and "I thought it was scheduled" is the failure that matters.
fn print_schedule(plan: &Plan<'_>) {
    let checkpoints = plan.pipeline.checkpoints.as_str();
    let Some(schedule) = ReconcileSchedule::load(checkpoints) else {
        return;
    };
    println!();
    println!("reconcile: `{}`", schedule.cron);
    if !schedule.tables.is_empty() {
        println!("      tables: {}", schedule.tables.join(", "));
    }
    println!("      sample: {} key(s)", schedule.sample);
    println!(
        "      last:   {}",
        match (&schedule.last_run, &schedule.last_result) {
            (Some(at), Some(result)) => format!("{at} — {result}"),
            (Some(at), None) => at.clone(),
            _ => format!("never (registered {})", schedule.created),
        }
    );
    println!(
        "      next:   {}",
        oxidant_pipelines::Cron::parse(&schedule.cron)
            .ok()
            .and_then(|cron| cron.next_after(schedule.anchor()))
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_else(|| "never — the expression no longer parses".into())
    );
    println!(
        "      from:   {}",
        ReconcileSchedule::path_in(checkpoints).display()
    );
}

/// Render pipeline events to stderr — byte-identical to the pre-extraction runner.
fn render_event(event: RunEvent) {
    match event.kind {
        RunEventKind::PipelineStarted {
            name,
            table_count,
            order,
        } => {
            eprintln!("[oxidant] pipeline `{name}`: {table_count} table(s), order: {order}");
        }
        RunEventKind::TableStarted { .. } | RunEventKind::PassComplete { .. } => {}
        RunEventKind::TableUpdated {
            name,
            rows,
            elapsed,
        } => {
            eprintln!(
                "[oxidant] {:<24} {} row(s) in {:.2}s",
                name,
                rows,
                elapsed.as_secs_f64()
            );
        }
        RunEventKind::TableUnchanged { name } => {
            eprintln!(
                "[oxidant] {:<24} unchanged (nothing it reads moved this pass)",
                name
            );
        }
        RunEventKind::TableSkipped { name } => {
            eprintln!(
                "[oxidant] {:<24} skipped (an upstream table failed this pass)",
                name
            );
        }
        RunEventKind::OnceFlowSkipped { name } => {
            eprintln!(
                "[oxidant] {:<24} skipped (once flow already completed)",
                name
            );
        }
        RunEventKind::ExpectationViolation {
            table,
            label,
            failed_records,
        } => {
            eprintln!(
                "[oxidant] table={table} expectation={label} failed_records={failed_records}"
            );
        }
        RunEventKind::BareNameWarning {
            table,
            error,
            downstream_hint,
        } => {
            if downstream_hint {
                eprintln!(
                    "[oxidant] {:<24} warning: could not alias its bare name ({error}); \
                     downstream tables must use the fully-qualified name",
                    table
                );
            } else {
                eprintln!(
                    "[oxidant] {:<24} warning: could not alias its bare name ({error})",
                    table
                );
            }
        }
        RunEventKind::TableFailed { name, error, .. } => {
            eprintln!("[oxidant] {:<24} FAILED: {error}", name);
        }
        RunEventKind::SinkWithoutCommitProtocol {
            table,
            path,
            format,
        } => {
            eprintln!(
                "[oxidant] {:<24} warning: `{format}` sink at {path} has no commit protocol — \
                 a reader can observe a partially written run, a replayed batch is appended \
                 rather than deduplicated, and the sink cannot be replaced atomically; use \
                 `delta` for transactional writes",
                table
            );
        }
        RunEventKind::StatePersistFailed { error } => {
            eprintln!("[oxidant] could not persist pipeline state: {error}");
        }
        RunEventKind::ReconcileFinished {
            cron,
            drifted,
            tables,
            report,
        } => {
            eprintln!(
                "[oxidant] scheduled reconcile (`{cron}`): {} of {tables} table(s) drifted",
                drifted
            );
            // The whole report, not a count: nobody is watching this terminal, and a summary
            // would only send whoever reads the log later back to run the command by hand.
            for line in report.lines() {
                eprintln!("[oxidant]   {line}");
            }
        }
        RunEventKind::ReconcileFailed { cron, error } => {
            eprintln!(
                "[oxidant] scheduled reconcile (`{cron}`) could not run: {error} — the pipeline \
                 keeps replicating"
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
        assert_eq!(
            parse_command(&args(&["oxidant", "pipeline", "--once"])).unwrap(),
            Command::Run {
                tables: vec![],
                once: true
            }
        );
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
        let err = parse_command(&args(&["oxidant", "pipeline", "vlaidate"])).expect_err("typo");
        assert!(err.to_string().contains("vlaidate"), "got: {err}");
    }

    fn reconcile(list: &[&str]) -> Command {
        parse_command(&args(list)).expect("parses")
    }

    #[test]
    fn reconcile_defaults_to_every_table_and_the_documented_sample() {
        assert_eq!(
            reconcile(&["oxidant", "pipeline", "reconcile", "-c", "x.yaml"]),
            Command::Reconcile {
                tables: vec![],
                sample: DEFAULT_SAMPLE,
                cron: None,
            }
        );
    }

    #[test]
    fn reconcile_scopes_to_named_tables_in_either_spelling() {
        assert_eq!(
            reconcile(&[
                "oxidant",
                "pipeline",
                "reconcile",
                "--table",
                "public.sales_suppliers",
                "--table=sales_customers",
            ]),
            Command::Reconcile {
                tables: vec!["public.sales_suppliers".into(), "sales_customers".into()],
                sample: DEFAULT_SAMPLE,
                cron: None,
            }
        );
    }

    #[test]
    fn the_sample_widens_and_is_rejected_when_it_is_not_a_count() {
        assert_eq!(
            reconcile(&["oxidant", "pipeline", "reconcile", "--sample", "50000"]),
            Command::Reconcile {
                tables: vec![],
                sample: 50_000,
                cron: None,
            }
        );
        assert_eq!(
            reconcile(&["oxidant", "pipeline", "reconcile", "--sample=250"]),
            Command::Reconcile {
                tables: vec![],
                sample: 250,
                cron: None,
            }
        );
        // A zero sample would walk nothing and report every table in sync, which is worse than
        // an error by exactly the amount an operator trusts it.
        for bad in ["0", "lots", "-1"] {
            let err = parse_command(&args(&[
                "oxidant",
                "pipeline",
                "reconcile",
                "--sample",
                bad,
            ]))
            .expect_err(bad)
            .to_string();
            assert!(err.contains("--sample"), "got: {err}");
        }
    }

    #[test]
    fn a_cron_expression_is_carried_through_and_off_is_a_value_not_a_subcommand() {
        assert_eq!(
            reconcile(&["oxidant", "pipeline", "reconcile", "--cron", "0 6 * * *"]),
            Command::Reconcile {
                tables: vec![],
                sample: DEFAULT_SAMPLE,
                cron: Some("0 6 * * *".into()),
            }
        );
        // `--cron` is in VALUE_FLAGS, so `off` is its value; without that, `off` would be read as
        // the subcommand and rejected as a typo.
        assert_eq!(
            reconcile(&["oxidant", "pipeline", "reconcile", "--cron", "off"]),
            Command::Reconcile {
                tables: vec![],
                sample: DEFAULT_SAMPLE,
                cron: Some("off".into()),
            }
        );
    }

    #[test]
    fn a_sample_value_is_never_mistaken_for_the_subcommand() {
        // `--sample 500 reconcile` and `reconcile --sample 500` must parse the same way.
        assert_eq!(
            reconcile(&["oxidant", "pipeline", "--sample", "500", "reconcile"]),
            Command::Reconcile {
                tables: vec![],
                sample: 500,
                cron: None,
            }
        );
    }

    #[test]
    fn render_event_prints_a_scheduled_reconcile_and_survives_a_failed_one() {
        use std::time::SystemTime;
        render_event(RunEvent {
            at: SystemTime::now(),
            kind: RunEventKind::ReconcileFinished {
                cron: "0 6 * * *".into(),
                drifted: 1,
                tables: 2,
                report: "summary: DRIFT — 1 of 2 table(s) differ\n".into(),
            },
        });
        render_event(RunEvent {
            at: SystemTime::now(),
            kind: RunEventKind::ReconcileFailed {
                cron: "0 6 * * *".into(),
                error: "connection refused".into(),
            },
        });
    }

    #[test]
    fn render_event_ignores_table_started_and_pass_complete() {
        use std::time::SystemTime;
        render_event(RunEvent {
            at: SystemTime::now(),
            kind: RunEventKind::TableStarted { name: "t".into() },
        });
        render_event(RunEvent {
            at: SystemTime::now(),
            kind: RunEventKind::PassComplete { outcomes: vec![] },
        });
    }
}
