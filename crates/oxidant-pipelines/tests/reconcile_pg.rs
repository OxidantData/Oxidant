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
//! `#[ignore]`d unless `OXIDANT_PG_TEST_DSN` names a server with `wal_level = logical` — the
//! attribute is conditional (`build.rs`), so a run with the variable set runs them and a run
//! without it reports them as ignored rather than as passed:
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

/// Serializes the phases of this suite that hold a walsender connection.
///
/// A running pipeline holds one, and a server allows `max_wal_senders` (8 by default) at once —
/// fewer than this suite has tests. Without this the failure is
/// `number of requested standby connections exceeds "max_wal_senders"`, which reads like a broken
/// diff rather than like two tests wanting the same fixed resource. The reconciles themselves are
/// read-only and use an ordinary connection, so only the pipeline runs are serialized.
static SNAPSHOTTING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// One `once` pass: snapshot the source into the Delta target, exactly as `pipeline run` would.
async fn seed_target(engine: &Engine, config: &OxidantConfig) {
    let plan = Plan::build(config).expect("plans");
    let _slot = SNAPSHOTTING.lock().await;
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
    assert_eq!(table.source_rows, Some(3));
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
    assert_eq!(table.source_rows, Some(3));
    assert_eq!(table.target_rows, Some(3));
    assert_eq!(table.row_count_drift(), Some(0));
    assert_eq!(table.verdicts(), vec!["key_drift"], "{rendered}");

    // A row-count drift on its own, once the counts stop cancelling out.
    sql(
        &connect,
        &format!("INSERT INTO public.{TABLE} VALUES (5, 'Soylent', 'NA', 1.00)"),
    )
    .await;
    let counted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    assert_eq!(counted.tables[0].row_count_drift(), Some(1));
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
    assert_eq!(sampled.tables[0].source_rows, Some(4));
    assert_eq!(sampled.tables[0].target_rows, Some(4));

    // A sample that lands exactly on the table's size walked the whole table, so nothing is
    // bounded and there is no `window_end` — the walk read one key past the sample to know that,
    // rather than inferring truncation from a window that came back full.
    let exact = run_reconcile(
        &engine,
        &config,
        &ReconcileOptions {
            tables: vec![],
            sample: 4,
        },
    )
    .await;
    let rendered = exact.render();
    assert_eq!(exact.exit_code(), 0, "{rendered}");
    assert_eq!(exact.tables[0].source_sampled, 4);
    assert_eq!(
        exact.tables[0].diff.window_end, None,
        "four keys out of four is a complete walk:\n{rendered}"
    );
    assert_eq!(exact.tables[0].diff.compared, 4);

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
    let _slot = SNAPSHOTTING.lock().await;
    let run = run_pipeline(&engine, &plan, &[], false, &once_tables, &mut on_event);
    // The loop never returns on its own; a bounded wait is the whole test.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(8), run).await;
    drop(_slot);
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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

    // Both documented spellings of the same table at once, which is what an operator writes when
    // they are not sure which one the command wants. The pipeline-table entry decides the scope;
    // the upstream entry has to be counted as explained too, or the run fails with an error that
    // lists the very name it says does not exist.
    let both = reconcile(
        &engine,
        &plan,
        &ReconcileOptions {
            tables: vec![table.clone(), format!("public.{table}")],
            ..Default::default()
        },
    )
    .await
    .expect("both spellings name something, so neither is unmatched");
    assert_eq!(both.tables.len(), 1, "and they name the same one table");
    assert_eq!(both.exit_code(), 0);

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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn a_column_the_merge_is_configured_to_drop_is_not_reported_as_schema_drift() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // `auto_cdc` projects the stream on its way into the target, so the target's column set is not
    // the source's. Comparing the two directly reports the excluded column as `missing_columns` →
    // `schema_drift` → exit 1, on every run, forever, for a pipeline doing exactly what its config
    // says. That is the kind of red a CI step gets muted for.
    let table = format!("{TABLE}_except");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!(
            "CREATE TABLE public.{table} \
               (supplierid bigint primary key, name text, notes text)"
        ),
    )
    .await;
    sql(
        &connect,
        &format!(
            "INSERT INTO public.{table} VALUES (1, 'Acme', 'internal only'), (2, 'Globex', NULL)"
        ),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = OxidantConfig::parse(&config_yaml(&connect, root.path(), &table).replace(
        "except_column_list: [__oxidant_op, __oxidant_ts]",
        "except_column_list: [__oxidant_op, __oxidant_ts, notes]",
    ))
    .expect("the fixture config parses");
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;

    let clean = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = clean.render();
    assert_eq!(
        clean.exit_code(),
        0,
        "an excluded column is not drift:\n{rendered}"
    );
    assert_eq!(clean.tables[0].verdicts(), vec!["in_sync"], "{rendered}");
    assert!(
        clean.tables[0].missing_columns.is_empty(),
        "`notes` is absent on purpose:\n{rendered}"
    );
    assert_eq!(
        clean.tables[0].excluded_columns,
        vec!["notes"],
        "and the report says it was never compared rather than that it matched:\n{rendered}"
    );
    assert!(rendered.contains("not compared"), "{rendered}");
    assert_eq!(clean.tables[0].diff.compared, 2);

    // The columns the merge *does* project are still compared: change one upstream and it lands
    // as a content mismatch rather than being swallowed with `notes`.
    sql(
        &connect,
        &format!("UPDATE public.{table} SET name = 'Acme Ltd' WHERE supplierid = 1"),
    )
    .await;
    let drifted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    assert_eq!(
        drifted.tables[0].diff.hash_mismatches,
        vec!["1"],
        "{}",
        drifted.render()
    );

    // And a column the target genuinely does not have is still `schema_drift`: the fix narrows the
    // check to what `auto_cdc` projects, it does not switch it off. A column added upstream that
    // the stream has not carried into the target yet is exactly that case — and unlike `notes`,
    // nothing in the config says the target should be without it.
    sql(
        &connect,
        &format!("ALTER TABLE public.{table} ADD COLUMN region text"),
    )
    .await;
    let lost = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = lost.render();
    assert_eq!(lost.tables[0].missing_columns, vec!["region"], "{rendered}");
    assert_eq!(
        lost.tables[0].excluded_columns,
        vec!["notes"],
        "the two reasons a column is uncompared stay apart:\n{rendered}"
    );
    assert!(
        lost.tables[0].verdicts().contains(&"schema_drift"),
        "{rendered}"
    );
    assert_eq!(lost.exit_code(), 1, "{rendered}");

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn two_upstream_tables_reconcile_as_one_target_until_their_key_spaces_overlap() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // One source, two upstream tables, one target. The connector requires only that they share a
    // *shape*, so two tables each keyed `supplierid bigint primary key` from 1 is the ordinary
    // case. While the key spaces are disjoint the union is a fair reading of the target; the
    // moment they overlap the merge holds one row per key and the union holds two, and every
    // comparison here would report drift that is not there.
    let pipeline_table = format!("{TABLE}_multi");
    let (a, b) = (format!("{pipeline_table}_a"), format!("{pipeline_table}_b"));
    for name in [&pipeline_table, &a, &b] {
        drop_fixtures(&connect, name).await;
    }
    for name in [&a, &b] {
        sql(
            &connect,
            &format!("CREATE TABLE public.{name} (supplierid bigint primary key, name text)"),
        )
        .await;
    }
    sql(
        &connect,
        &format!("INSERT INTO public.{a} VALUES (1, 'Acme'), (2, 'Globex')"),
    )
    .await;
    sql(
        &connect,
        &format!("INSERT INTO public.{b} VALUES (3, 'Initech'), (4, 'Umbrella')"),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = OxidantConfig::parse(
        &config_yaml(&connect, root.path(), &pipeline_table).replace(
            &format!("tables: public.{pipeline_table}"),
            &format!("tables: public.{a}, public.{b}"),
        ),
    )
    .expect("the fixture config parses");
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;
    let plan = Plan::build(&config).expect("plans");

    let clean = reconcile(&engine, &plan, &ReconcileOptions::default())
        .await
        .expect("disjoint key spaces reconcile");
    let rendered = clean.render();
    assert_eq!(clean.exit_code(), 0, "{rendered}");
    assert_eq!(
        clean.tables[0].source_rows,
        Some(4),
        "both tables are counted"
    );
    assert_eq!(clean.tables[0].diff.compared, 4, "{rendered}");

    // Now give the two tables a key in common — a row `b` has and `a` has too.
    sql(
        &connect,
        &format!("INSERT INTO public.{b} VALUES (1, 'Acme (b)')"),
    )
    .await;
    let refused = reconcile(&engine, &plan, &ReconcileOptions::default())
        .await
        .expect("the refusal is this table's own, not the whole run's");
    let rendered = refused.render();
    assert_eq!(
        refused.tables[0].verdicts(),
        vec!["source_error"],
        "an overlapping key space is refused, not reported as drift:\n{rendered}"
    );
    assert_eq!(
        refused.exit_code(),
        oxidant_pipelines::EXIT_FAILED,
        "and it is `could not run`, not `drifted`:\n{rendered}"
    );
    let err = refused.tables[0]
        .source_error
        .clone()
        .expect("names why it stopped");
    assert!(err.contains(&a) && err.contains(&b), "got: {err}");
    assert!(err.contains('1'), "it names the key it found: {err}");
    assert!(
        err.contains("one `postgres_cdc` source per upstream table"),
        "and what to do about it: {err}"
    );

    for name in [&pipeline_table, &a, &b] {
        drop_fixtures(&connect, name).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn an_unreachable_publisher_for_one_table_does_not_discard_the_others_report() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // The command's premise is "run this across N tables in CI". Propagating one table's error
    // threw away every other table's result with it — so a single unreachable publisher turned a
    // drift report into no report, and the drift stayed unreported until someone noticed.
    let good = format!("{TABLE}_partial");
    drop_fixtures(&connect, &good).await;
    sql(
        &connect,
        &format!("CREATE TABLE public.{good} (supplierid bigint primary key, name text)"),
    )
    .await;
    sql(
        &connect,
        &format!("INSERT INTO public.{good} VALUES (1, 'Acme')"),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let engine = engine_for(root.path()).await;
    // Seed the reachable table on its own; the second source has no server to snapshot from.
    seed_target(&engine, &config(&connect, root.path(), &good)).await;

    // Now the same pipeline plus a table whose publisher is a closed port.
    let dead = format!("{TABLE}_partial_dead");
    let config = OxidantConfig::parse(&format!(
        "{}  - name: {dead}
    source:
      format: postgres_cdc
      options:
        host: 127.0.0.1
        port: \"1\"
        database: {database}
        user: {user}
        tls: disable
        publication: {dead}
        slot: {dead}
        tables: public.{dead}
    auto_cdc:
      source: {dead}_changes
      keys: [supplierid]
      sequence_by: __oxidant_lsn
",
        config_yaml(&connect, root.path(), &good),
        database = connect.database,
        user = connect.user,
    ))
    .expect("the fixture config parses");
    let plan = Plan::build(&config).expect("plans");

    let partial = reconcile(&engine, &plan, &ReconcileOptions::default())
        .await
        .expect("one unreachable source is reported, not propagated");
    let rendered = partial.render();
    assert_eq!(
        partial.tables.len(),
        2,
        "both tables are in the report:\n{rendered}"
    );
    let reachable = &partial.tables[0];
    assert_eq!(reachable.table, good);
    assert_eq!(
        reachable.verdicts(),
        vec!["in_sync"],
        "the table that could be read is still answered:\n{rendered}"
    );
    let unreachable = &partial.tables[1];
    assert_eq!(unreachable.table, dead);
    assert_eq!(unreachable.verdicts(), vec!["source_error"], "{rendered}");
    assert!(unreachable.source_error.is_some(), "{rendered}");
    assert_eq!(
        unreachable.source_rows, None,
        "an unread count is unknown, not zero:\n{rendered}"
    );
    // And the exit code says "could not run", not "drifted".
    assert_eq!(
        partial.exit_code(),
        oxidant_pipelines::EXIT_FAILED,
        "{rendered}"
    );
    assert!(rendered.contains("summary: FAILED"), "{rendered}");

    // Asking for the broken table by name is the same answer, and not an "unmatched --table"
    // complaint about a name the pipeline plainly has.
    let scoped = reconcile(
        &engine,
        &plan,
        &ReconcileOptions {
            tables: vec![dead.clone()],
            ..Default::default()
        },
    )
    .await
    .expect("a named table that cannot be read is a report, not an unmatched name");
    assert_eq!(scoped.tables.len(), 1);
    assert_eq!(scoped.exit_code(), oxidant_pipelines::EXIT_FAILED);

    drop_fixtures(&connect, &good).await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn a_null_in_the_row_identity_is_refused_rather_than_walked() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // A primary key is `NOT NULL`, so only a `keys:` override can put a NULL in the row identity.
    // A NULL is not a key: two NULL-keyed rows render to one string and one of them silently
    // disappears from a report whose entire output is per-key, and neither `NULLS FIRST` nor
    // `NULLS LAST` makes the sentinel's position agree with both engines for every key type.
    let table = format!("{TABLE}_nullkey");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!("CREATE TABLE public.{table} (supplierid bigint primary key, name text)"),
    )
    .await;
    // A non-PK identity needs the whole old row on a delete, which the connector insists on
    // before it will accept a `keys:` override at all.
    sql(
        &connect,
        &format!("ALTER TABLE public.{table} REPLICA IDENTITY FULL"),
    )
    .await;
    sql(
        &connect,
        &format!("INSERT INTO public.{table} VALUES (1, 'Acme'), (2, NULL)"),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    // `keys: name` — a nullable column as the row identity, which is legal and usually harmless.
    let yaml = config_yaml_keyed(&connect, root.path(), &table, "name").replace(
        &format!("tables: public.{table}"),
        &format!("tables: public.{table}\n        keys: name"),
    );
    let config = OxidantConfig::parse(&yaml).expect("the fixture config parses");
    let engine = engine_for(root.path()).await;
    let plan = Plan::build(&config).expect("plans");

    let refused = reconcile(&engine, &plan, &ReconcileOptions::default())
        .await
        .expect("the refusal is this table's own");
    let rendered = refused.render();
    assert_eq!(
        refused.tables[0].verdicts(),
        vec!["source_error"],
        "{rendered}"
    );
    assert_eq!(refused.exit_code(), oxidant_pipelines::EXIT_FAILED);
    let err = refused.tables[0].source_error.clone().expect("says why");
    assert!(err.contains("NULL"), "got: {err}");
    assert!(err.contains("name"), "it names the column: {err}");
    assert!(err.contains("keys:"), "and what to name instead: {err}");

    // The same column with no NULL in it is a perfectly good identity, and is not refused: the
    // check is on the values, not on the column being nullable.
    sql(
        &connect,
        &format!("UPDATE public.{table} SET name = 'Globex' WHERE supplierid = 2"),
    )
    .await;
    let allowed = reconcile(&engine, &plan, &ReconcileOptions::default())
        .await
        .expect("runs");
    assert!(
        !allowed.tables[0].errored(),
        "a nullable key holding no NULLs is fine: {}",
        allowed.render()
    );

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn an_empty_target_is_still_schema_checked() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // The schema comparison used to run only when the target had rows, which skipped it for the
    // one case where the schema is *all* there is to compare: a target that exists, holds nothing,
    // and is missing a column. Both counts agree at zero, so nothing else would have caught it.
    let table = format!("{TABLE}_empty");
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

    // Empty the table through the pipeline rather than starting empty: a pipeline with nothing to
    // write does not create the target at all, and `target_missing` is a different verdict with
    // its own test. The delete goes upstream and a second pass carries it into the target, which
    // leaves exactly what this test needs — a target that exists and holds nothing.
    sql(&connect, &format!("DELETE FROM public.{table}")).await;
    seed_target(&engine, &config).await;

    let empty = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = empty.render();
    assert_eq!(
        empty.tables[0].target_rows,
        Some(0),
        "the pipeline created the target even with nothing to put in it:\n{rendered}"
    );
    assert_eq!(
        empty.exit_code(),
        0,
        "empty on both sides is in sync:\n{rendered}"
    );

    // Now add a column upstream. Both counts are still zero and the key walk still has nothing to
    // walk; the schema is the only thing that differs, and it has to be reported.
    sql(
        &connect,
        &format!("ALTER TABLE public.{table} ADD COLUMN region text"),
    )
    .await;
    let drifted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = drifted.render();
    assert_eq!(drifted.tables[0].source_rows, Some(0), "{rendered}");
    assert_eq!(drifted.tables[0].target_rows, Some(0), "{rendered}");
    assert_eq!(
        drifted.tables[0].missing_columns,
        vec!["region"],
        "{rendered}"
    );
    assert_eq!(
        drifted.tables[0].verdicts(),
        vec!["schema_drift"],
        "{rendered}"
    );
    assert_eq!(drifted.exit_code(), 1, "{rendered}");

    drop_fixtures(&connect, &table).await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn a_composite_key_over_every_walkable_type_reconciles_clean_and_names_its_drift() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    // The unit-level cross-engine tests pin each type's two spellings against a fixture; this is
    // the same claim against a real server and a real Delta table, with a *composite* key — where
    // the encoding, the per-column ordering and the two spellings all have to agree at once.
    let table = format!("{TABLE}_types");
    drop_fixtures(&connect, &table).await;
    sql(
        &connect,
        &format!(
            "CREATE TABLE public.{table} (\
               supplierid bigint, signed_on date, active boolean, quantity int4, \
               name text, rating numeric(38,2), PRIMARY KEY (supplierid, signed_on))"
        ),
    )
    .await;
    sql(
        &connect,
        &format!(
            "INSERT INTO public.{table} VALUES \
               (1, '2026-08-23', true, 42, 'Acme', 4.50), \
               (1, '1969-12-31', false, -7, '  spaced ', -0.05), \
               (9007199254740993, '2026-01-01', NULL, 0, NULL, 0.00)"
        ),
    )
    .await;

    let root = tempfile::TempDir::new().expect("temp dir");
    let config = OxidantConfig::parse(&config_yaml_keyed(
        &connect,
        root.path(),
        &table,
        "supplierid, signed_on",
    ))
    .expect("the fixture config parses");
    let engine = engine_for(root.path()).await;
    seed_target(&engine, &config).await;

    let clean = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = clean.render();
    assert_eq!(
        clean.exit_code(),
        0,
        "every walkable type must line up on both sides:\n{rendered}"
    );
    assert_eq!(
        clean.tables[0].keys,
        vec!["supplierid".to_string(), "signed_on".to_string()]
    );
    assert_eq!(clean.tables[0].diff.compared, 3, "{rendered}");

    // A change of one hundredth of a unit is a content mismatch, named by its composite key —
    // `4.5` and `4.50` are the same number and different strings, so this also proves the two
    // sides render a `numeric` the same way rather than merely comparing equal by luck.
    sql(
        &connect,
        &format!(
            "UPDATE public.{table} SET rating = 4.51 \
             WHERE supplierid = 1 AND signed_on = '2026-08-23'"
        ),
    )
    .await;
    let drifted = run_reconcile(&engine, &config, &ReconcileOptions::default()).await;
    let rendered = drifted.render();
    assert_eq!(
        drifted.tables[0].diff.hash_mismatches.len(),
        1,
        "only the row that changed:\n{rendered}"
    );
    assert!(
        rendered.contains("1 | 2026-08-23"),
        "the composite key is readable in the report:\n{rendered}"
    );
    assert_eq!(drifted.tables[0].diff.compared, 3, "{rendered}");

    drop_fixtures(&connect, &table).await;
}
