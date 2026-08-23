//! End-to-end tests for the `postgres_cdc` source against a real PostgreSQL server.
//!
//! Skipped silently unless `OXIDANT_PG_TEST_DSN` names a server with `wal_level = logical` and a
//! role that may create replication slots — for example:
//!
//! ```text
//! OXIDANT_PG_TEST_DSN=postgres://postgres@127.0.0.1:5433/postgres \
//!   cargo test -p oxidant-streaming --test postgres_cdc
//! ```
//!
//! A fake wire covers the sequencing in `postgres_cdc.rs`'s own tests. What only a real server
//! can answer is whether this connector's idea of the protocol matches Postgres': the exact bytes
//! pgoutput emits, what `USE_SNAPSHOT` hands back, what a slot retains, and — the one that
//! matters most — whether a batch that never committed can still be re-read afterwards.
//!
//! Every test owns its own table, publication and slot, so they run concurrently and a failure
//! leaves nothing behind that would wedge the next run: each one drops its fixtures up front as
//! well as at the end. A leaked *slot* is the expensive kind of leftover — it pins WAL on the
//! server until someone drops it — so cleanup runs even when the assertions fail.

use std::collections::HashMap;

use oxidant_loom::arrow::array::{Array, Int64Array, StringArray};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_streaming::pg_replication::{PgConnectConfig, TlsMode};
use oxidant_streaming::postgres_cdc::{PostgresCdcSource, LSN_COLUMN, OP_COLUMN};
use oxidant_streaming::{BatchRange, Source};

/// The connection the tests run against, or `None` when the gate is not set.
fn dsn() -> Option<PgConnectConfig> {
    let dsn = std::env::var("OXIDANT_PG_TEST_DSN")
        .ok()
        .filter(|d| !d.is_empty())?;
    let parsed: tokio_postgres::Config = dsn
        .parse()
        .unwrap_or_else(|e| panic!("OXIDANT_PG_TEST_DSN `{dsn}` is not a Postgres URL: {e}"));
    let host = match parsed.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
        _ => "127.0.0.1".to_string(),
    };
    Some(PgConnectConfig {
        host,
        port: parsed.get_ports().first().copied().unwrap_or(5432),
        database: parsed.get_dbname().unwrap_or("postgres").to_string(),
        user: parsed.get_user().unwrap_or("postgres").to_string(),
        password: parsed
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned()),
        // A scratch cluster on loopback; the TLS modes are exercised by unit tests, not here.
        tls: TlsMode::Disable,
        tls_ca: None,
    })
}

/// Skip the body when the gate is unset, so `cargo test` stays green on a machine with no server.
macro_rules! gated {
    ($connect:ident) => {
        let Some($connect) = dsn() else {
            eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
            return;
        };
    };
}

/// One test's fixtures: a table, a publication and a slot, all named after it.
struct Fixture {
    connect: PgConnectConfig,
    name: String,
}

impl Fixture {
    async fn new(connect: &PgConnectConfig, name: &str, columns: &str) -> Self {
        let fixture = Self {
            connect: connect.clone(),
            name: name.to_string(),
        };
        fixture.drop_all().await;
        fixture
            .sql(&format!(
                "CREATE TABLE public.{name} ({columns})",
                name = fixture.name
            ))
            .await;
        fixture
    }

    async fn control(&self) -> oxidant_streaming::pg_replication::ControlConnection {
        self.connect
            .connect_control()
            .await
            .expect("the test server accepts connections")
    }

    async fn sql(&self, statement: &str) {
        self.control()
            .await
            .execute(statement)
            .await
            .unwrap_or_else(|e| panic!("`{statement}`: {e}"));
    }

    /// Best-effort teardown. A slot outlives the process that made it and pins WAL until it is
    /// dropped, so this runs at the start of a test as well as at the end.
    async fn drop_all(&self) {
        let control = self.control().await;
        // An active slot cannot be dropped; the source's connection is gone by now, but the
        // walsender can take a moment to notice.
        for attempt in 0..20 {
            let dropped = control
                .execute(&format!(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = '{}'",
                    self.name
                ))
                .await;
            if dropped.is_ok() {
                break;
            }
            if attempt == 19 {
                eprintln!("warning: could not drop replication slot `{}`", self.name);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let _ = control
            .execute(&format!("DROP PUBLICATION IF EXISTS {}", self.name))
            .await;
        let _ = control
            .execute(&format!("DROP TABLE IF EXISTS public.{}", self.name))
            .await;
    }

    fn options(&self) -> HashMap<String, String> {
        [
            ("host", self.connect.host.clone()),
            ("port", self.connect.port.to_string()),
            ("database", self.connect.database.clone()),
            ("user", self.connect.user.clone()),
            ("tls", "disable".to_string()),
            ("publication", self.name.clone()),
            ("slot", self.name.clone()),
            ("tables", format!("public.{}", self.name)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    fn source(&self) -> PostgresCdcSource {
        PostgresCdcSource::from_options(&self.options()).expect("the source validates and builds")
    }
}

/// The message a source that refuses to build produces. Written out rather than `expect_err`
/// because the source itself is not `Debug` — it owns a live connection, not a value.
fn build_err(options: &HashMap<String, String>, why: &str) -> String {
    match PostgresCdcSource::from_options(options) {
        Ok(_) => panic!("expected a failure: {why}"),
        Err(e) => e.to_string(),
    }
}

/// Plan and read one micro-batch, returning the range it covered and its rows.
async fn one_batch(
    source: &mut PostgresCdcSource,
    engine: &Engine,
) -> (BatchRange, Vec<RecordBatch>) {
    let range = source.plan_batch(engine).await.expect("plans");
    if range.is_empty() {
        return (range, vec![]);
    }
    let batches = source.poll_range(engine, &range).await.expect("polls");
    (range, batches)
}

/// Drain batches until one comes back empty, marking each durable as the scheduler would.
async fn drain(source: &mut PostgresCdcSource, engine: &Engine) -> Vec<RecordBatch> {
    let mut all = Vec::new();
    for _ in 0..32 {
        let (range, batches) = one_batch(source, engine).await;
        if range.is_empty() {
            break;
        }
        source.mark_durable(engine).await.expect("confirms");
        all.extend(batches);
    }
    all
}

fn strings(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for batch in batches {
        let index = batch.schema().index_of(column).expect("column exists");
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a utf8 column");
        for i in 0..array.len() {
            out.push((!array.is_null(i)).then(|| array.value(i).to_string()));
        }
    }
    out
}

fn i64s(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for batch in batches {
        let index = batch.schema().index_of(column).expect("column exists");
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an int64 column");
        for i in 0..array.len() {
            out.push((!array.is_null(i)).then(|| array.value(i)));
        }
    }
    out
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_hands_over_to_the_stream_with_no_gap_and_no_overlap() {
    gated!(connect);
    let fixture = Fixture::new(
        &connect,
        "ox_cdc_handoff",
        "id bigint primary key, name text",
    )
    .await;
    fixture
        .sql("INSERT INTO public.ox_cdc_handoff VALUES (1, 'one'), (2, 'two')")
        .await;

    let engine = Engine::new();
    let mut source = fixture.source();

    // The snapshot: every row as of the slot's consistent point, as upserts.
    let snapshot = drain(&mut source, &engine).await;
    assert_eq!(rows(&snapshot), 2, "the two rows that existed at slot time");
    assert_eq!(strings(&snapshot, OP_COLUMN), vec![Some("s".into()); 2]);
    assert_eq!(
        strings(&snapshot, "name"),
        vec![Some("one".into()), Some("two".into())]
    );

    // Everything after it arrives on the stream — and the rows already snapshotted do not.
    fixture
        .sql("INSERT INTO public.ox_cdc_handoff VALUES (3, 'three')")
        .await;
    fixture
        .sql("UPDATE public.ox_cdc_handoff SET name = 'ONE' WHERE id = 1")
        .await;
    fixture
        .sql("DELETE FROM public.ox_cdc_handoff WHERE id = 2")
        .await;
    fixture.sql("TRUNCATE public.ox_cdc_handoff").await;

    let stream = drain(&mut source, &engine).await;
    assert_eq!(
        strings(&stream, OP_COLUMN),
        ["i", "u", "d", "t"].map(|op| Some(op.to_string())).to_vec()
    );
    assert_eq!(
        strings(&stream, "name"),
        vec![
            Some("three".into()),
            Some("ONE".into()),
            // Under REPLICA IDENTITY DEFAULT a delete carries only its key…
            None,
            // …and a truncate names no row at all.
            None,
        ]
    );
    assert_eq!(
        i64s(&stream, "id"),
        vec![Some(3), Some(1), Some(2), None],
        "a delete still identifies the row it removed"
    );

    // `__oxidant_lsn` is what AUTO CDC orders by, so it has to be strictly increasing across the
    // whole stream — otherwise an older change could win over a newer one for the same key.
    let lsns: Vec<i64> = i64s(&stream, LSN_COLUMN).into_iter().flatten().collect();
    assert!(
        lsns.windows(2).all(|w| w[0] < w[1]),
        "LSNs must increase: {lsns:?}"
    );

    // Before the teardown: an active slot cannot be dropped, and the source holds the session.
    drop(source);
    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_that_never_committed_replays_identically_after_a_restart() {
    gated!(connect);
    let fixture = Fixture::new(
        &connect,
        "ox_cdc_replay",
        "id bigint primary key, name text",
    )
    .await;

    let engine = Engine::new();
    let mut source = fixture.source();
    drain(&mut source, &engine).await; // empty snapshot, then caught up

    let resume = source
        .committed_offsets()
        .expect("a position to resume from");
    fixture
        .sql("INSERT INTO public.ox_cdc_replay VALUES (1, 'a'), (2, 'b')")
        .await;

    // Read a batch and then lose the process before the sink commits: `mark_durable` is never
    // called, so nothing was confirmed and the slot still holds every byte of the range.
    let (range, first) = one_batch(&mut source, &engine).await;
    assert_eq!(rows(&first), 2);
    drop(source);

    // A change lands between the two attempts, exactly as it would in production.
    fixture
        .sql("INSERT INTO public.ox_cdc_replay VALUES (3, 'c')")
        .await;

    let mut resumed = fixture.source();
    resumed.restore_offsets(&resume);
    let replay = resumed
        .poll_range(&engine, &range)
        .await
        .expect("the slot still holds the range");
    assert_eq!(
        first, replay,
        "a replayed range reproduces the batch exactly — the newcomer is not in it"
    );

    // And the newcomer is still there for the next batch.
    let next = drain(&mut resumed, &engine).await;
    assert_eq!(strings(&next, "name"), vec![Some("c".into())]);

    drop(resumed);
    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_added_column_is_reported_and_the_stream_keeps_running() {
    gated!(connect);
    let fixture = Fixture::new(
        &connect,
        "ox_cdc_addcol",
        "id bigint primary key, name text",
    )
    .await;
    let logs = tempfile::TempDir::new().unwrap();

    let engine = Engine::new();
    let mut options = fixture.options();
    options.insert(
        "oxidant.connector.log_dir".into(),
        logs.path().to_string_lossy().into_owned(),
    );
    options.insert("oxidant.connector.name".into(), "addcol".into());
    let mut source = PostgresCdcSource::from_options(&options).expect("builds");
    drain(&mut source, &engine).await;

    // An additive change on the publisher, then a row that carries it.
    fixture
        .sql("ALTER TABLE public.ox_cdc_addcol ADD COLUMN region text DEFAULT 'EU'")
        .await;
    fixture
        .sql("INSERT INTO public.ox_cdc_addcol VALUES (1, 'a', 'US')")
        .await;

    let batches = drain(&mut source, &engine).await;
    assert_eq!(rows(&batches), 1, "ingestion continues across the change");
    assert_eq!(strings(&batches, "name"), vec![Some("a".into())]);
    assert!(
        batches[0].schema().index_of("region").is_err(),
        "the column the query was planned without is not in the batch"
    );

    let log = std::fs::read_to_string(logs.path().join("addcol.jsonl")).expect("a connector log");
    assert!(
        log.contains("\"event\":\"schema_change\"") && log.contains("region"),
        "the change is reported so an operator knows to restart: {log}"
    );

    // Restarting is what propagates it: the source re-introspects at construction.
    drop(source);
    let restarted = PostgresCdcSource::from_options(&options).expect("builds");
    assert!(
        restarted.schema().index_of("region").is_ok(),
        "the new column is in the schema after a restart"
    );
    drop(restarted);

    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_connector_log_records_the_snapshot_the_batches_and_the_slot() {
    gated!(connect);
    let fixture = Fixture::new(&connect, "ox_cdc_log", "id bigint primary key, name text").await;
    fixture
        .sql("INSERT INTO public.ox_cdc_log VALUES (1, 'a')")
        .await;
    let logs = tempfile::TempDir::new().unwrap();

    let engine = Engine::new();
    let mut options = fixture.options();
    options.insert(
        "oxidant.connector.log_dir".into(),
        logs.path().to_string_lossy().into_owned(),
    );
    options.insert("oxidant.connector.name".into(), "ox_cdc_log".into());
    let mut source = PostgresCdcSource::from_options(&options).expect("builds");
    drain(&mut source, &engine).await;
    fixture
        .sql("INSERT INTO public.ox_cdc_log VALUES (2, 'b')")
        .await;
    drain(&mut source, &engine).await;
    drop(source);

    let log = std::fs::read_to_string(logs.path().join("ox_cdc_log.jsonl")).expect("a log");
    let events: Vec<serde_json::Value> = log
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("one JSON object per line"))
        .collect();
    let kinds: Vec<&str> = events.iter().filter_map(|e| e["event"].as_str()).collect();
    for expected in [
        "snapshot_start",
        "snapshot_done",
        "batch",
        "commit",
        "slot_metrics",
    ] {
        assert!(
            kinds.contains(&expected),
            "missing `{expected}` in {kinds:?}"
        );
    }
    let snapshot = events
        .iter()
        .find(|e| e["event"] == "snapshot_done")
        .expect("a snapshot_done event");
    assert_eq!(snapshot["rows"], 1);
    assert_eq!(snapshot["table"], "public.ox_cdc_log");

    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_that_cannot_be_replicated_says_exactly_how_to_fix_it() {
    gated!(connect);
    // No primary key and REPLICA IDENTITY NOTHING: Postgres writes no old row image, so an
    // UPDATE or DELETE could never be matched to a row.
    let fixture = Fixture::new(&connect, "ox_cdc_norep", "id bigint, name text").await;
    fixture
        .sql("ALTER TABLE public.ox_cdc_norep REPLICA IDENTITY NOTHING")
        .await;

    let err = build_err(
        &fixture.options(),
        "a table with no row identity cannot be replicated",
    );
    assert!(err.contains("ox_cdc_norep"), "names the table: {err}");
    assert!(
        err.contains("REPLICA IDENTITY FULL"),
        "and the remediation SQL: {err}"
    );

    // With an identity it builds.
    fixture
        .sql("ALTER TABLE public.ox_cdc_norep REPLICA IDENTITY FULL")
        .await;
    let source = fixture.source();
    assert_eq!(source.schema().fields().len(), 5, "two columns plus three");
    drop(source);

    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_that_does_not_exist_is_refused_before_a_slot_is_created() {
    gated!(connect);
    let mut options: HashMap<String, String> = [
        ("host", connect.host.clone()),
        ("port", connect.port.to_string()),
        ("database", connect.database.clone()),
        ("user", connect.user.clone()),
        ("tls", "disable".to_string()),
        ("publication", "ox_cdc_missing".to_string()),
        ("slot", "ox_cdc_missing".to_string()),
        ("tables", "public.ox_cdc_no_such_table".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let err = build_err(&options, "the table does not exist");
    assert!(err.contains("ox_cdc_no_such_table"), "got: {err}");

    // And nothing was created on the way to finding out.
    let control = connect.connect_control().await.expect("connects");
    let slots = control
        .query(
            "SELECT slot_name::text FROM pg_replication_slots WHERE slot_name::text = $1",
            &["ox_cdc_missing"],
        )
        .await
        .expect("queries");
    assert!(
        slots.is_empty(),
        "a failed validation leaves no slot behind"
    );

    options.remove("tables");
    let err = build_err(&options, "`tables:` is required");
    assert!(err.contains("tables"), "got: {err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_thousand_row_fixture_snapshots_every_row_exactly_once() {
    gated!(connect);
    // `public.sales_suppliers` is the standing fixture on the test cluster. It is read-only here
    // — the test creates only its own publication and slot — so it can run beside the others.
    let control = connect.connect_control().await.expect("connects");
    let exists = control
        .query(
            "SELECT 1::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname::text = 'public' AND c.relname::text = 'sales_suppliers'",
            &[],
        )
        .await
        .expect("queries");
    if exists.is_empty() {
        eprintln!("skipping: public.sales_suppliers is not on this server");
        return;
    }
    for statement in [
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = 'ox_cdc_suppliers'",
        "DROP PUBLICATION IF EXISTS ox_cdc_suppliers",
    ] {
        let _ = control.execute(statement).await;
    }

    let options: HashMap<String, String> = [
        ("host", connect.host.clone()),
        ("port", connect.port.to_string()),
        ("database", connect.database.clone()),
        ("user", connect.user.clone()),
        ("tls", "disable".to_string()),
        ("publication", "ox_cdc_suppliers".to_string()),
        ("slot", "ox_cdc_suppliers".to_string()),
        ("tables", "public.sales_suppliers".to_string()),
        ("exclude_columns", "city".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let engine = Engine::new();
    let mut source = PostgresCdcSource::from_options(&options).expect("builds");
    // `exclude_columns` keeps a column out of the stream entirely.
    assert!(source.schema().index_of("city").is_err());
    assert!(source.schema().index_of("supplierid").is_ok());

    let snapshot = drain(&mut source, &engine).await;
    assert_eq!(rows(&snapshot), 1000);
    assert!(strings(&snapshot, OP_COLUMN)
        .iter()
        .all(|op| op.as_deref() == Some("s")));
    let ids: std::collections::BTreeSet<i64> = i64s(&snapshot, "supplierid")
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(ids.len(), 1000, "every row exactly once");
    drop(source);

    for statement in [
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = 'ox_cdc_suppliers'",
        "DROP PUBLICATION IF EXISTS ox_cdc_suppliers",
    ] {
        let _ = control.execute(statement).await;
    }
}
