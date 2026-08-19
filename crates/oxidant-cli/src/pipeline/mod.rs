//! `oxidant pipeline` — CLI entry: argument parsing and rendering for the library runner.

use oxidant_common::{Error, Result};
use oxidant_config::{OxidantConfig, TableKind};
use oxidant_pipelines::{run_pipeline, Plan, RunEvent};

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
            run_pipeline(&engine, &plan, &tables, once, &mut render_event).await
        }
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

/// Render pipeline events to stderr — byte-identical to the pre-extraction runner.
fn render_event(event: RunEvent) {
    match event {
        RunEvent::PipelineStarted {
            name,
            table_count,
            order,
        } => {
            eprintln!("[oxidant] pipeline `{name}`: {table_count} table(s), order: {order}");
        }
        RunEvent::TableStarted { .. } | RunEvent::PassComplete { .. } => {}
        RunEvent::TableUpdated {
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
        RunEvent::TableUnchanged { name } => {
            eprintln!(
                "[oxidant] {:<24} unchanged (nothing it reads moved this pass)",
                name
            );
        }
        RunEvent::TableSkipped { name } => {
            eprintln!(
                "[oxidant] {:<24} skipped (an upstream table failed this pass)",
                name
            );
        }
        RunEvent::ExpectationViolation {
            table,
            label,
            failed_records,
        } => {
            eprintln!(
                "[oxidant] table={table} expectation={label} failed_records={failed_records}"
            );
        }
        RunEvent::BareNameWarning {
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
        RunEvent::TableFailed { name, error, .. } => {
            eprintln!("[oxidant] {:<24} FAILED: {error}", name);
        }
        RunEvent::StatePersistFailed { error } => {
            eprintln!("[oxidant] could not persist pipeline state: {error}");
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

    #[test]
    fn render_event_ignores_table_started_and_pass_complete() {
        render_event(RunEvent::TableStarted { name: "t".into() });
        render_event(RunEvent::PassComplete { outcomes: vec![] });
    }
}
