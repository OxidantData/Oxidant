//! `oxidant pipeline reconcile` against a real PostgreSQL server and a real Delta target.
//!
//! The diff walker's own unit tests prove the classification; what only a live pair of systems can
//! answer is whether the two sides *line up* — whether a `bigint` key orders the same way in
//! Postgres and in DataFusion, whether a `numeric` written through the connector renders back to
//! the same string, and whether a target seeded by an actual pipeline run reads as in sync rather
//! than as drift in every row. That is the failure this suite exists to catch, because it is
//! silent: a reconcile that always reports drift is indistinguishable from a pipeline that is
//! always broken.
//!
//! Skipped silently unless `OXIDANT_PG_TEST_DSN` names a server with `wal_level = logical`:
//!
//! ```text
//! OXIDANT_PG_TEST_DSN=postgres://postgres@127.0.0.1:5433/postgres \
//!   cargo test -p oxidant-pipelines --test reconcile_pg
//! ```

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use oxidant_config::OxidantConfig;
use oxidant_loom::Engine;
use oxidant_pipelines::{
    reconcile, run_pipeline, set_schedule, Plan, ReconcileOptions, ReconcileReport,
    ReconcileSchedule, RunEventKind,
};
use oxidant_streaming::pg_replication::{ControlConnection, PgConnectConfig, TlsMode};

/// The table, publication and slot this suite owns, all named the same.
const TABLE: &str = "ox_reconcile";

/// The connection the test runs against, or `None` when the gate is not set.
///
/// Parsed by hand, as the sibling suite does: the only shape this gate ever holds is
/// `postgres://[user[:password]@]host[:port]/database`.
fn dsn() -> Option<PgConnectConfig> {
    let dsn = std::env::var("OXIDANT_PG_TEST_DSN")
        .ok()
        .filter(|d| !d.is_empty())?;
    let rest = dsn
        .strip_prefix("postgres://")
        .or_else(|| dsn.strip_prefix("postgresql://"))
        .unwrap_or_else(|| panic!("OXIDANT_PG_TEST_DSN `{dsn}` is not a Postgres URL"));
    let (authority, database) = rest.split_once('/').unwrap_or((rest, "postgres"));
    let (credentials, host_port) = match authority.rsplit_once('@') {
        Some((credentials, host_port)) => (credentials, host_port),
        None => ("postgres", authority),
    };
    let (user, password) = match credentials.split_once(':') {
        Some((user, password)) => (user, Some(password.to_string())),
        None => (credentials, None),
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => (host, port.parse().unwrap_or(5432)),
        None => (host_port, 5432),
    };
    Some(PgConnectConfig {
        host: host.to_string(),
        port,
        database: database.split('?').next().unwrap_or("postgres").to_string(),
        user: user.to_string(),
        password,
        // A scratch cluster on loopback; TLS is covered by the source's own tests.
        tls: TlsMode::Disable,
        tls_ca: None,
    })
}

async fn control(connect: &PgConnectConfig) -> ControlConnection {
    connect
        .connect_control()
        .await
        .expect("the test server accepts connections")
}

async fn sql(connect: &PgConnectConfig, statement: &str) {
    control(connect)
        .await
        .execute(statement)
        .await
        .unwrap_or_else(|e| panic!("`{statement}`: {e}"));
}

/// Best-effort teardown, run before a test as well as after it: a leaked slot pins WAL on the
/// server until someone drops it.
async fn drop_fixtures(connect: &PgConnectConfig, name: &str) {
    let conn = control(connect).await;
    // An active slot cannot be dropped, and the walsender takes a moment to notice its client is
    // gone — so this retries rather than failing the run that comes after it.
    for _ in 0..20 {
        let dropped = conn
            .execute(&format!(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = '{name}'"
            ))
            .await;
        if dropped.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = conn
        .execute(&format!("DROP PUBLICATION IF EXISTS {name}"))
        .await;
    let _ = conn
        .execute(&format!("DROP TABLE IF EXISTS public.{name}"))
        .await;
}

/// The `oxidant.yaml` this suite runs, rooted under a temp directory.
///
/// Written as YAML rather than assembled as structs on purpose: `reconcile` reads the source's
/// own `options:` block, and the thing worth testing is that it reads *the config file's* block
/// rather than a second config surface invented for it.
fn config(connect: &PgConnectConfig, root: &std::path::Path, name: &str) -> OxidantConfig {
    OxidantConfig::parse(&config_yaml(connect, root, name)).expect("the fixture config parses")
}

/// [`config`] before it is parsed, so a test can vary one line of it.
fn config_yaml(connect: &PgConnectConfig, root: &std::path::Path, name: &str) -> String {
    config_yaml_keyed(connect, root, name, "supplierid")
}

/// [`config_yaml`] for a table whose row identity is not `supplierid`.
fn config_yaml_keyed(
    connect: &PgConnectConfig,
    root: &std::path::Path,
    name: &str,
    key: &str,
) -> String {
    let root = root.display();
    format!(
        "catalogs:
  local:
    type: local
    warehouse: {root}/warehouse
pipeline:
  name: reconcile-test
  catalog: local
  schema: live
  checkpoints: {root}/checkpoints
  storage: {root}/warehouse/live
  trigger: once
tables:
  - name: {name}
    source:
      format: postgres_cdc
      options:
        host: {host}
        port: \"{port}\"
        database: {database}
        user: {user}
        tls: disable
        publication: {name}
        slot: {name}
        tables: public.{name}
    auto_cdc:
      source: {name}_changes
      keys: [{key}]
      sequence_by: __oxidant_lsn
      apply_as_deletes: \"__oxidant_op = 'd'\"
      apply_as_truncates: \"__oxidant_op = 't'\"
      except_column_list: [__oxidant_op, __oxidant_ts]
",
        name = name,
        key = key,
        host = connect.host,
        port = connect.port,
        database = connect.database,
        user = connect.user,
    )
}

/// An engine with the fixture's `local` catalog bridged in, as the CLI's `build_engine` does.
async fn engine_for(root: &std::path::Path) -> Arc<Engine> {
    let engine = Arc::new(Engine::new());
    let catalog = oxidant_catalog_local::LocalCatalog::new(
        "local",
        format!("{}/warehouse", root.display()),
        Default::default(),
        vec![],
        vec![],
    )
    .await
    .expect("the temp warehouse builds a catalog");
    engine.register_catalog("local", Arc::new(catalog));
    engine
}

/// One `once` pass: snapshot the source into the Delta target, exactly as `pipeline run` would.
async fn seed_target(engine: &Engine, config: &OxidantConfig) {
    let plan = Plan::build(config).expect("plans");
    run_pipeline(engine, &plan, &[], true, &HashSet::new(), &mut |event| {
        eprintln!("{:?}", event.kind)
    })
    .await
    .expect("the pipeline snapshots the source into the target");
}

async fn run_reconcile(
    engine: &Engine,
    config: &OxidantConfig,
    options: &ReconcileOptions,
) -> ReconcileReport {
    let plan = Plan::build(config).expect("plans");
    reconcile(engine, &plan, options)
        .await
        .expect("the reconcile itself runs")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_freshly_snapshotted_target_is_in_sync_and_every_upstream_change_is_a_named_drift_class()
{
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    drop_fixtures(&connect, TABLE).await;
    sql(
        &connect,
        &format!(
            "CREATE TABLE public.{TABLE} (\
               supplierid bigint primary key, name text, continent text, rating numeric(10,2))"
        ),
    )
    .await;
    sql(
        &connect,
        &format!(
            "INSERT INTO public.{TABLE} VALUES \
               (1, 'Acme', 'EU', 4.50), (2, 'Globex', 'NA', 3.25), (3, 'Initech', 'AS', 5.00)"
        ),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = config(&connect, root.path(), TABLE);
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;

    // The baseline, and the assertion the whole feature rests on: a target the pipeline just
    // wrote must read as clean. A false positive here is worse than no reconcile at all.
    let clean = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    assert_eq!(
        clean.exit_code(),
        0,
        "a freshly snapshotted target must be in sync:\n{}",
        clean.render()
    );
    let table = &clean.tables[0];
    assert_eq!(table.verdicts(), vec!["in_sync"], "{}", clean.render());
    assert_eq!(table.source_rows, 3);
    assert_eq!(table.target_rows, Some(3));
    assert_eq!(table.upstream, vec![format!("public.{TABLE}")]);
    assert_eq!(table.target, format!("local.live.{TABLE}"));
    assert_eq!(table.diff.compared, 3, "every key was content-compared");

    // Now drift the source *without* running the pipeline — which is what a dropped slot, a
    // recycled WAL segment or a restored backup looks like from the lakehouse's side.
    sql(
        &connect,
        &format!("INSERT INTO public.{TABLE} VALUES (4, 'Umbrella', 'EU', 2.00)"),
    )
    .await;
    sql(
        &connect,
        &format!("DELETE FROM public.{TABLE} WHERE supplierid = 2"),
    )
    .await;
    sql(
        &connect,
        &format!("UPDATE public.{TABLE} SET name = 'Initech Ltd' WHERE supplierid = 3"),
    )
    .await;

    let drifted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = drifted.render();
    assert_eq!(drifted.exit_code(), 1, "drift must exit 1:\n{rendered}");
    let table = &drifted.tables[0];
    assert_eq!(
        table.diff.missing_in_target,
        vec!["4"],
        "the insert the stream never carried:\n{rendered}"
    );
    assert_eq!(
        table.diff.missing_in_source,
        vec!["2"],
        "the deleted row is a phantom in the target:\n{rendered}"
    );
    assert_eq!(
        table.diff.hash_mismatches,
        vec!["3"],
        "the updated row's contents differ:\n{rendered}"
    );
    // One row went in and one went out, so the counts are equal — the key walk is the only thing
    // that sees it, which is exactly why the report has both.
    assert_eq!(table.source_rows, 3);
    assert_eq!(table.target_rows, Some(3));
    assert_eq!(table.row_count_drift(), 0);
    assert_eq!(table.verdicts(), vec!["key_drift"], "{rendered}");

    // A row-count drift on its own, once the counts stop cancelling out.
    sql(
        &connect,
        &format!("INSERT INTO public.{TABLE} VALUES (5, 'Soylent', 'NA', 1.00)"),
    )
    .await;
    let counted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    assert_eq!(counted.tables[0].row_count_drift(), 1);
    assert_eq!(
        counted.tables[0].verdicts(),
        vec!["row_count_drift", "key_drift"],
        "{}",
        counted.render()
    );

    // The connector log carries the verdict, where §6 says it will.
    let log = root
        .path()
        .join("checkpoints")
        .join("logs")
        .join(format!("{TABLE}.jsonl"));
    let events: Vec<serde_json::Value> = std::fs::read_to_string(&log)
        .unwrap_or_else(|e| panic!("`{}`: {e}", log.display()))
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|e| e["event"] == "reconcile")
        .collect();
    assert_eq!(events.len(), 3, "one line per reconcile run");
    assert_eq!(events[0]["verdict"], "in_sync");
    assert_eq!(events[1]["verdict"], "key_drift");
    assert_eq!(events[1]["missing_in_target"], 1);
    assert_eq!(events[1]["missing_in_source"], 1);
    assert_eq!(events[1]["hash_mismatches"], 1);

    drop_fixtures(&connect, TABLE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_text_key_walks_in_the_same_order_on_both_sides_and_a_short_sample_bounds_it() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // The riskiest assumption in the whole design: that two engines agree on the order of a text
    // key. They do not by default — under an `en_US` database collation Postgres sorts `a` before
    // `B`, and DataFusion sorts bytes, so `B` comes first. If the two walks disagree the merge
    // interleaves wrongly and reports every row as drift, which is a false alarm indistinguishable
    // from a real one. `COLLATE "C"` on the source's ORDER BY is what makes them agree, and this
    // is the test that would fail if it were dropped.
    let table = format!("{TABLE}_text");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!("CREATE TABLE public.{table} (supplierid text primary key, name text)"),
    )
    .await;
    sql(
        &connect,
        &format!(
            "INSERT INTO public.{table} VALUES \
               ('a', 'lower a'), ('B', 'upper B'), ('c', 'lower c'), ('D', 'upper D')"
        ),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = config(&connect, root.path(), &table);
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;

    let clean = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    assert_eq!(
        clean.exit_code(),
        0,
        "mixed-case text keys must line up:\n{}",
        clean.render()
    );
    assert_eq!(clean.tables[0].diff.compared, 4);

    // A sample shorter than the table: both walks stop at the same key, so the tail is reported
    // as unexamined rather than as drift.
    let sampled = run_reconcile(
        &engine,
        &config,
        &ReconcileOptions {
            tables: vec![],
            sample: 2,
        },
    )
    .await;
    let rendered = sampled.render();
    assert_eq!(
        sampled.exit_code(),
        0,
        "a short sample finds no drift:\n{rendered}"
    );
    assert_eq!(sampled.tables[0].source_sampled, 2);
    assert_eq!(sampled.tables[0].target_sampled, 2);
    assert_eq!(
        sampled.tables[0].diff.window_end.as_deref(),
        Some("D"),
        "byte order puts the upper-case keys first, and the walk stops at the second:\n{rendered}"
    );
    // The count still covers the whole table, which is what makes a short sample useful at all.
    assert_eq!(sampled.tables[0].source_rows, 4);
    assert_eq!(sampled.tables[0].target_rows, Some(4));

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_registered_cron_schedule_fires_between_triggers_and_records_what_it_found() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // The scheduler lives inside `pipeline run`, so the only way to prove it ticks is to run a
    // pipeline. `--cron` writing a file and `pipeline run` reading one are two halves of the same
    // feature, and testing either alone leaves the seam untested.
    let table = format!("{TABLE}_cron");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!("CREATE TABLE public.{table} (supplierid bigint primary key, name text)"),
    )
    .await;
    sql(
        &connect,
        &format!("INSERT INTO public.{table} VALUES (1, 'Acme')"),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    // A repeating trigger, so the run loop reaches the point between two passes where the
    // schedule is evaluated. `once` exits after one pass and never gets there — which is the
    // documented behaviour, not an oversight.
    let config = OxidantConfig::parse(
        &config_yaml(&connect, root.path(), &table).replace("trigger: once", "trigger: 200ms"),
    )
    .expect("the fixture config parses");
    let engine = engine_for(root.path()).await;
    let plan = Plan::build(&config).expect("plans");

    // Every minute, anchored in the past, so it is due on the first pass rather than in an hour.
    let schedule = set_schedule(&plan, "* * * * *", None, &ReconcileOptions::default())
        .expect("registers the schedule");
    assert_eq!(schedule.cron, "* * * * *");
    let mut backdated = schedule;
    backdated.created = "2020-01-01T00:00:00Z".into();
    backdated
        .save(&plan.pipeline.checkpoints)
        .expect("backdates the anchor");

    // A shared counter rather than a captured local: the run loop holds `&mut` on the callback
    // for as long as the future lives, and the future is only dropped by the timeout below.
    let reconciled = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&reconciled);
    let once_tables = HashSet::new();
    let mut on_event = move |event: oxidant_pipelines::RunEvent| {
        if matches!(event.kind, RunEventKind::ReconcileFinished { .. }) {
            counted.fetch_add(1, Ordering::Relaxed);
        }
    };
    let run = run_pipeline(&engine, &plan, &[], false, &once_tables, &mut on_event);
    // The loop never returns on its own; a bounded wait is the whole test.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(8), run).await;
    assert!(
        reconciled.load(Ordering::Relaxed) >= 1,
        "the schedule should have fired at least once"
    );

    let recorded = ReconcileSchedule::load(&plan.pipeline.checkpoints).expect("the file survives");
    assert_eq!(
        recorded.last_result.as_deref(),
        Some("in_sync"),
        "a scheduled run records its verdict for `pipeline show`"
    );
    assert!(
        recorded.last_run.is_some(),
        "the run stamps the anchor, so the next tick is measured from it"
    );

    let events: Vec<serde_json::Value> = std::fs::read_to_string(
        root.path()
            .join("checkpoints")
            .join("logs")
            .join(format!("{table}.jsonl")),
    )
    .expect("the connector log exists")
    .lines()
    .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
    .filter(|e| e["event"] == "reconcile")
    .collect();
    assert_eq!(
        events.first().map(|e| e["scheduled"].as_str()),
        Some(Some("* * * * *")),
        "registering the schedule is itself a connector-log line"
    );
    assert!(
        events.iter().any(|e| e["verdict"] == "in_sync"),
        "and so is each run's verdict: {events:?}"
    );

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_filter_scopes_the_report_and_a_name_that_matches_nothing_is_an_error() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // A table of its own, so the two tests can run concurrently against one server.
    let table = format!("{TABLE}_filter");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!("CREATE TABLE public.{table} (supplierid bigint primary key, name text)"),
    )
    .await;
    sql(
        &connect,
        &format!("INSERT INTO public.{table} VALUES (1, 'Acme')"),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = config(&connect, root.path(), &table);
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;
    let plan = Plan::build(&config).expect("plans");

    // The upstream `schema.table` spelling the docs use.
    let scoped = reconcile(
        &engine,
        &plan,
        &ReconcileOptions {
            tables: vec![format!("public.{table}")],
            sample: 10,
        },
    )
    .await
    .expect("scopes to the named upstream table");
    assert_eq!(scoped.tables.len(), 1);
    assert_eq!(scoped.tables[0].sample, 10);
    assert_eq!(scoped.exit_code(), 0);

    // The pipeline table's own name works too.
    assert_eq!(
        reconcile(
            &engine,
            &plan,
            &ReconcileOptions {
                tables: vec![table.clone()],
                ..Default::default()
            },
        )
        .await
        .expect("scopes to the pipeline table")
        .tables
        .len(),
        1
    );

    // A name that matches nothing is an error rather than an empty, clean-looking report — which
    // would read as "in sync" to a CI step that only checks the exit code.
    let err = reconcile(
        &engine,
        &plan,
        &ReconcileOptions {
            tables: vec!["public.not_a_table".into()],
            ..Default::default()
        },
    )
    .await
    .expect_err("an unmatched filter is refused");
    assert!(err.to_string().contains("public.not_a_table"), "got: {err}");

    // And a typo *among* good names is refused too: reconciling the rest and reporting in sync
    // would answer a question the operator did not ask.
    let err = reconcile(
        &engine,
        &plan,
        &ReconcileOptions {
            tables: vec![table.clone(), "public.not_a_table".into()],
            ..Default::default()
        },
    )
    .await
    .expect_err("a partially matched filter is refused");
    assert!(err.to_string().contains("public.not_a_table"), "got: {err}");
    assert!(
        err.to_string().contains(&table),
        "the error lists what the pipeline does have: {err}"
    );

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_boolean_key_is_spelled_the_same_way_by_both_engines_and_drifts_one_row_at_a_time() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // A boolean has two text forms in Postgres and only one of them matches the target: the output
    // function (`t`/`f` — what pgoutput carries and `psql` prints) and the cast (`true`/`false`).
    // The walk reads the cast on both sides. If it ever stopped doing so the two walks would
    // interleave `f < false < t < true` and report every row of a healthy table as *both* missing
    // from the target and a phantom in it — a permanent false alarm on a table nobody has touched.
    // Only a live server can say which form a query actually returns, so this is where that is
    // pinned; `a_boolean_key_is_read_through_the_cast…` holds the query shape it depends on.
    let table = format!("{TABLE}_bool");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!("CREATE TABLE public.{table} (active boolean primary key, name text)"),
    )
    .await;
    sql(
        &connect,
        &format!("INSERT INTO public.{table} VALUES (true, 'on'), (false, 'off')"),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = OxidantConfig::parse(&config_yaml_keyed(&connect, root.path(), &table, "active"))
        .expect("the fixture config parses");
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;

    let clean = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    assert_eq!(
        clean.exit_code(),
        0,
        "a boolean key must not report the whole table as drifted:\n{}",
        clean.render()
    );
    assert_eq!(clean.tables[0].diff.compared, 2, "{}", clean.render());
    assert_eq!(clean.tables[0].keys, vec!["active".to_string()]);

    // Drift one of the two keys upstream. Exactly one class, naming the key in the engine's
    // spelling — not `t`, and not both rows.
    sql(
        &connect,
        &format!("DELETE FROM public.{table} WHERE active = true"),
    )
    .await;
    let drifted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = drifted.render();
    assert_eq!(drifted.exit_code(), 1, "{rendered}");
    assert_eq!(
        drifted.tables[0].diff.missing_in_source,
        vec!["true"],
        "the deleted row is a phantom, named as the target spells it:\n{rendered}"
    );
    assert!(
        drifted.tables[0].diff.missing_in_target.is_empty(),
        "the surviving row is in sync, not missing:\n{rendered}"
    );
    assert_eq!(
        drifted.tables[0].diff.compared, 1,
        "`false` was compared on both sides:\n{rendered}"
    );

    drop_fixtures(&connect, &table).await;
}
