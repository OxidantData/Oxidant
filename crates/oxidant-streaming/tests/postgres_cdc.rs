//! End-to-end tests for the `postgres_cdc` source against a real PostgreSQL server.
//!
//! `#[ignore]`d unless `OXIDANT_PG_TEST_DSN` names a server with `wal_level = logical` and a
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

/// Replication sessions this suite may hold at once.
///
/// `max_wal_senders` (8 by default) bounds replication connections *server-wide*, and these tests
/// run concurrently by design — so without a gate the suite fails intermittently with "number of
/// requested standby connections exceeds max_wal_senders", which says nothing about the code under
/// test. Four leaves room for whatever else is pointed at the same scratch cluster.
static WAL_SENDERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// Skip the body when the gate is unset, so `cargo test` stays green on a machine with no server.
///
/// Also takes this test's share of the server's replication capacity, held for its whole body.
macro_rules! gated {
    ($connect:ident) => {
        let Some($connect) = dsn() else {
            eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
            return;
        };
        let _wal_sender = WAL_SENDERS
            .acquire()
            .await
            .expect("the gate is never closed");
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
        PostgresCdcSource::from_options(None, &self.options())
            .expect("the source validates and builds")
    }
}

/// The message a source that refuses to build produces. Written out rather than `expect_err`
/// because the source itself is not `Debug` — it owns a live connection, not a value.
fn build_err(options: &HashMap<String, String>, why: &str) -> String {
    match PostgresCdcSource::from_options(None, options) {
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
    let mut source = PostgresCdcSource::from_options(None, &options).expect("builds");
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
    let restarted = PostgresCdcSource::from_options(None, &options).expect("builds");
    assert!(
        restarted.schema().index_of("region").is_ok(),
        "the new column is in the schema after a restart"
    );
    drop(restarted);

    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
    let mut source = PostgresCdcSource::from_options(None, &options).expect("builds");
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

    // The resolved row identity is in the log too: it is what a delete is matched on, and an
    // operator debugging a merge should not have to guess which columns the connector picked.
    let start = events
        .iter()
        .find(|e| e["event"] == "snapshot_start")
        .expect("a snapshot_start event");
    assert_eq!(start["tables"][0]["table"], "public.ox_cdc_log");
    assert_eq!(start["tables"][0]["keys"][0], "id");
    assert_eq!(start["tables"][0]["replica_identity"], "d");

    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn legal_values_this_mapping_cannot_hold_arrive_as_null_and_the_pipeline_keeps_running() {
    gated!(connect);
    // Every value here is something Postgres prints for an ordinary column — `'infinity'` on a
    // `date` is the canonical "no end date" sentinel. They used to raise `Error::Execution`,
    // which is not retryable: the batch failed, its recorded range stayed on disk, and every
    // later trigger replayed it and failed on the same row, forever. The fix has to be checked
    // against a real server because it turns on exactly how Postgres spells them.
    let fixture = Fixture::new(
        &connect,
        "ox_cdc_special",
        "id bigint primary key, hired_on date, seen_at timestamp, seen_tz timestamptz, \
         amount numeric(20,2), shift time",
    )
    .await;
    fixture
        .sql(
            "INSERT INTO public.ox_cdc_special VALUES \
             (1, 'infinity', 'infinity', 'infinity', 'NaN', '24:00:00'), \
             (2, '-infinity', '-infinity', '-infinity', 12.34, '00:00:00'), \
             (3, '0044-03-15 BC', '0044-03-15 10:11:12 BC', '2024-05-06 07:08:09+00', \
              -1.5, '01:02:03'), \
             (4, '2024-05-06', '2024-05-06 07:08:09', '2024-05-06 07:08:09+00', 7, '23:59:59')",
        )
        .await;

    let engine = Engine::new();
    let mut source = fixture.source();
    let snapshot = drain(&mut source, &engine).await;
    assert_eq!(rows(&snapshot), 4, "no row stops the batch");
    assert_eq!(
        i64s(&snapshot, "id"),
        vec![Some(1), Some(2), Some(3), Some(4)]
    );

    // The unrepresentable values are NULL; the ordinary ones on the same columns are not.
    for (column, unrepresentable) in [
        ("hired_on", vec![0, 1, 2]),
        ("seen_at", vec![0, 1, 2]),
        ("seen_tz", vec![0, 1]),
        ("amount", vec![0]),
        ("shift", vec![0]),
    ] {
        let batch = &snapshot[0];
        let array = batch.column(batch.schema().index_of(column).expect("column"));
        for row in 0..4 {
            assert_eq!(
                array.is_null(row),
                unrepresentable.contains(&row),
                "`{column}` row {row}"
            );
        }
    }

    // And the stream carries the same values the same way.
    fixture
        .sql("INSERT INTO public.ox_cdc_special VALUES (5, 'infinity', NULL, NULL, NULL, NULL)")
        .await;
    let stream = drain(&mut source, &engine).await;
    assert_eq!(rows(&stream), 1);
    assert_eq!(strings(&stream, OP_COLUMN), vec![Some("i".into())]);

    drop(source);
    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn a_source_column_may_not_be_named_like_a_metadata_column() {
    gated!(connect);
    let fixture = Fixture::new(
        &connect,
        "ox_cdc_collide",
        "id bigint primary key, __oxidant_lsn text",
    )
    .await;
    let err = build_err(
        &fixture.options(),
        "the column collides with `__oxidant_lsn`",
    );
    assert!(err.contains("__oxidant_lsn"), "got: {err}");
    assert!(err.contains("exclude_columns"), "and the way out: {err}");

    // Excluding it is the way out, and then the table replicates.
    let mut options = fixture.options();
    options.insert("exclude_columns".into(), "__oxidant_lsn".into());
    let source =
        PostgresCdcSource::from_options(None, &options).expect("builds once the column is out");
    assert_eq!(source.schema().fields().len(), 4, "id plus the three");
    drop(source);

    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn a_slot_someone_else_is_holding_is_a_diagnosis_and_not_a_hang() {
    gated!(connect);
    // `DROP_REPLICATION_SLOT … WAIT` blocks until the slot goes inactive, with no timeout at any
    // layer — so a pipeline started twice onto one `slot:`, or restarted before the previous
    // process exited, hung at start with no log line and nothing to diagnose from. The wait is
    // bounded and the error names the backend holding it.
    //
    // `OXIDANT_PG_CDC_SLOT_WAIT_MS` shortens the bound so this takes a moment rather than half a
    // minute. Safe to set process-wide: every other test drops a slot nobody holds, and that path
    // does not wait at all.
    std::env::set_var("OXIDANT_PG_CDC_SLOT_WAIT_MS", "750");
    let fixture = Fixture::new(&connect, "ox_cdc_held", "id bigint primary key, name text").await;

    let engine = Engine::new();
    // The holder: a source that has finished its snapshot and is streaming, which is what puts
    // an `active_pid` on the slot.
    let mut holder = fixture.source();
    drain(&mut holder, &engine).await;

    // A second pipeline pointed at the same slot. It needs a *fresh* slot for its own snapshot,
    // so it has to drop this one — which it cannot, because the first source is on it.
    let mut intruder = fixture.source();
    let err = intruder
        .plan_batch(&engine)
        .await
        .expect_err("the slot is held; recreating it cannot succeed")
        .to_string();
    assert!(err.contains("ox_cdc_held"), "names the slot: {err}");
    assert!(
        err.contains("pg_terminate_backend("),
        "and the backend holding it, with the way out: {err}"
    );

    // Once the holder lets go, the same call succeeds — the bound is a bound, not a refusal.
    // Back to the real wait for this half: the walsender takes a moment to notice the socket
    // closed, and that moment is exactly what the wait is for.
    std::env::remove_var("OXIDANT_PG_CDC_SLOT_WAIT_MS");
    drop(holder);
    drop(intruder);
    let mut resumed = fixture.source();
    let (range, _) = one_batch(&mut resumed, &engine).await;
    assert!(range.start.contains_key("snapshot"));

    drop(resumed);
    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn a_whole_schema_resolves_a_partitioned_table_once_and_not_once_per_partition() {
    gated!(connect);
    // `relkind IN ('r','p')` matches the partitioned parent *and* every leaf partition, and a
    // partition shares its parent's columns — so nothing complained. The snapshot then read the
    // parent, which returns every row across every partition, and read each partition again:
    // every row twice, 2x the memory and 2x the time, silently.
    let control = connect.connect_control().await.expect("connects");
    let _ = control
        .execute("DROP SCHEMA IF EXISTS ox_cdc_parts CASCADE")
        .await;
    for statement in [
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = 'ox_cdc_parts'",
        "DROP PUBLICATION IF EXISTS ox_cdc_parts",
    ] {
        let _ = control.execute(statement).await;
    }
    for statement in [
        "CREATE SCHEMA ox_cdc_parts",
        "CREATE TABLE ox_cdc_parts.sales (id bigint, sold_on date, PRIMARY KEY (id, sold_on)) \
         PARTITION BY RANGE (sold_on)",
        "CREATE TABLE ox_cdc_parts.sales_2024 PARTITION OF ox_cdc_parts.sales \
         FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        "CREATE TABLE ox_cdc_parts.sales_2025 PARTITION OF ox_cdc_parts.sales \
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
        "INSERT INTO ox_cdc_parts.sales VALUES (1, '2024-03-04'), (2, '2025-06-07')",
    ] {
        control
            .execute(statement)
            .await
            .unwrap_or_else(|e| panic!("`{statement}`: {e}"));
    }

    let options: HashMap<String, String> = [
        ("host", connect.host.clone()),
        ("port", connect.port.to_string()),
        ("database", connect.database.clone()),
        ("user", connect.user.clone()),
        ("tls", "disable".to_string()),
        ("publication", "ox_cdc_parts".to_string()),
        ("slot", "ox_cdc_parts".to_string()),
        ("tables", "ox_cdc_parts.*".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let engine = Engine::new();
    let mut source = PostgresCdcSource::from_options(None, &options).expect("builds");
    assert!(
        source.description().contains("ox_cdc_parts.sales@"),
        "the parent is the replication unit: {}",
        source.description()
    );
    assert!(
        !source.description().contains("sales_2024"),
        "and a leaf partition is not a second source table: {}",
        source.description()
    );

    let snapshot = drain(&mut source, &engine).await;
    assert_eq!(
        i64s(&snapshot, "id"),
        vec![Some(1), Some(2)],
        "every row exactly once, not once per partition and again for the parent"
    );

    drop(source);
    for statement in [
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = 'ox_cdc_parts'",
        "DROP PUBLICATION IF EXISTS ox_cdc_parts",
        "DROP SCHEMA IF EXISTS ox_cdc_parts CASCADE",
    ] {
        let _ = control.execute(statement).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
async fn an_excluded_column_does_not_raise_a_schema_change_alarm() {
    gated!(connect);
    // The shipped integration configuration's own shape: `exclude_columns: city` used to write a
    // `schema_change` event naming `city` as *added*, and print "restart the pipeline to
    // propagate it" to stderr, on a correctly configured, unchanged pipeline. Restarting, as
    // instructed, reproduced it.
    let fixture = Fixture::new(
        &connect,
        "ox_cdc_excluded",
        "id bigint primary key, name text, city text",
    )
    .await;
    fixture
        .sql("INSERT INTO public.ox_cdc_excluded VALUES (1, 'a', 'Berlin')")
        .await;
    let logs = tempfile::TempDir::new().unwrap();

    let engine = Engine::new();
    let mut options = fixture.options();
    options.insert("exclude_columns".into(), "city".into());
    options.insert(
        "oxidant.connector.log_dir".into(),
        logs.path().to_string_lossy().into_owned(),
    );
    options.insert("oxidant.connector.name".into(), "excluded".into());
    let mut source = PostgresCdcSource::from_options(None, &options).expect("builds");
    drain(&mut source, &engine).await;

    // A change on the stream is what makes the publisher send a `Relation` message at all.
    fixture
        .sql("INSERT INTO public.ox_cdc_excluded VALUES (2, 'b', 'Lisbon')")
        .await;
    let stream = drain(&mut source, &engine).await;
    assert_eq!(rows(&stream), 1);
    assert!(stream[0].schema().index_of("city").is_err(), "kept out");

    // `schema_change` also carries the startup advisories (REPLICA IDENTITY DEFAULT here), so
    // the assertion is on the drift report specifically: the `added_columns` list.
    let added = |log: &str| -> Vec<String> {
        log.lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|e| e["added_columns"].as_array().cloned())
            .flatten()
            .filter_map(|c| c.as_str().map(str::to_string))
            .collect()
    };
    let log = std::fs::read_to_string(logs.path().join("excluded.jsonl")).expect("a log");
    assert!(
        added(&log).is_empty(),
        "nothing changed on the publisher: {log}"
    );

    // A real `ADD COLUMN` still gets through — the alarm is worth reading again.
    fixture
        .sql("ALTER TABLE public.ox_cdc_excluded ADD COLUMN region text")
        .await;
    fixture
        .sql("INSERT INTO public.ox_cdc_excluded VALUES (3, 'c', 'Oslo', 'EU')")
        .await;
    drain(&mut source, &engine).await;
    let log = std::fs::read_to_string(logs.path().join("excluded.jsonl")).expect("a log");
    assert_eq!(
        added(&log),
        vec!["region".to_string()],
        "the real change is reported, and `city` still is not: {log}"
    );

    drop(source);
    fixture.drop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
#[cfg_attr(
    not(pg_live),
    ignore = "set OXIDANT_PG_TEST_DSN to run this against a real server"
)]
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
        // Small enough that the thousand rows have to be fetched in slices: the snapshot walks a
        // server-side cursor, so this bounds the read itself and not just the Arrow output.
        ("snapshot_batch_rows", "128".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let engine = Engine::new();
    let mut source = PostgresCdcSource::from_options(None, &options).expect("builds");
    // `exclude_columns` keeps a column out of the stream entirely.
    assert!(source.schema().index_of("city").is_err());
    assert!(source.schema().index_of("supplierid").is_ok());

    let snapshot = drain(&mut source, &engine).await;
    assert_eq!(rows(&snapshot), 1000);
    assert_eq!(
        snapshot
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![128, 128, 128, 128, 128, 128, 128, 104],
        "one Arrow batch per FETCH, so the whole table is never in the heap at once"
    );
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
