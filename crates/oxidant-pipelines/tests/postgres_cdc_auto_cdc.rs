//! What a `postgres_cdc` change stream does once AUTO CDC has merged it, against a real server.
//!
//! The source's own integration suite proves the stream is right; this proves the *target* is,
//! which is a different question and the one an operator actually asks. Skipped silently unless
//! `OXIDANT_PG_TEST_DSN` names a server with `wal_level = logical`:
//!
//! ```text
//! OXIDANT_PG_TEST_DSN=postgres://postgres@127.0.0.1:5433/postgres \
//!   cargo test -p oxidant-pipelines --test postgres_cdc_auto_cdc
//! ```

use std::collections::HashMap;

use oxidant_config::AutoCdcConfig;
use oxidant_loom::arrow::array::{Array, Int64Array, StringArray};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_pipelines::auto_cdc::CdcMerge;
use oxidant_streaming::pg_replication::{ControlConnection, PgConnectConfig, TlsMode};
use oxidant_streaming::postgres_cdc::PostgresCdcSource;
use oxidant_streaming::Source;

/// The connection the test runs against, or `None` when the gate is not set.
///
/// Parsed by hand rather than through a client: the only shape this gate ever holds is
/// `postgres://[user[:password]@]host[:port]/database`, and a dev-dependency on a Postgres client
/// to read six fields out of it is not worth the build.
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

const TABLE: &str = "ox_cdc_keymove";

async fn control(connect: &PgConnectConfig) -> ControlConnection {
    connect
        .connect_control()
        .await
        .expect("the test server accepts connections")
}

/// Best-effort teardown, run before the test as well as after it: a leaked slot pins WAL on the
/// server until someone drops it.
async fn drop_all(connect: &PgConnectConfig) {
    let control = control(connect).await;
    for _ in 0..20 {
        let dropped = control
            .execute(&format!(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = '{TABLE}'"
            ))
            .await;
        if dropped.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = control
        .execute(&format!("DROP PUBLICATION IF EXISTS {TABLE}"))
        .await;
    let _ = control
        .execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await;
}

fn options(connect: &PgConnectConfig) -> HashMap<String, String> {
    [
        ("host", connect.host.clone()),
        ("port", connect.port.to_string()),
        ("database", connect.database.clone()),
        ("user", connect.user.clone()),
        ("tls", "disable".to_string()),
        ("publication", TABLE.to_string()),
        ("slot", TABLE.to_string()),
        ("tables", format!("public.{TABLE}")),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// The `auto_cdc:` block `examples/postgres-cdc.yaml` ships, keyed on this table.
fn cdc_config() -> AutoCdcConfig {
    AutoCdcConfig {
        source: format!("{TABLE}_changes"),
        keys: vec!["supplierid".into()],
        sequence_by: "__oxidant_lsn".into(),
        apply_as_deletes: Some("__oxidant_op = 'd'".into()),
        apply_as_truncates: Some("__oxidant_op = 't'".into()),
        column_list: None,
        except_column_list: Some(vec!["__oxidant_op".into(), "__oxidant_ts".into()]),
        ignore_null_updates_columns: None,
        ignore_null_updates_except: None,
    }
}

/// Drain every available batch through the merge, returning the new target contents.
async fn drain_into(
    source: &mut PostgresCdcSource,
    merge: &CdcMerge,
    engine: &Engine,
    target: Vec<RecordBatch>,
) -> Vec<RecordBatch> {
    let mut target = target;
    for _ in 0..32 {
        let range = source.plan_batch(engine).await.expect("plans");
        if range.is_empty() {
            break;
        }
        let batches = source.poll_range(engine, &range).await.expect("polls");
        target = merge
            .apply(engine, &batches, &target)
            .await
            .expect("merges");
        source.mark_durable(engine).await.expect("confirms");
    }
    target
}

/// `(supplierid, name)` in the merged target.
async fn contents(engine: &Engine, target: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    if target.is_empty() {
        return Vec::new();
    }
    engine
        .register_batches("__probe", target.to_vec())
        .expect("register");
    let out = engine
        .sql("SELECT supplierid, name FROM __probe ORDER BY supplierid")
        .await
        .expect("probe");
    engine.deregister_table("__probe");
    let mut rows = Vec::new();
    for batch in out {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("supplierid");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name");
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                (!names.is_null(i)).then(|| names.value(i).to_string()),
            ));
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_that_moves_the_primary_key_leaves_no_orphan_in_the_target() {
    let Some(connect) = dsn() else {
        eprintln!("skipping: OXIDANT_PG_TEST_DSN is not set");
        return;
    };
    drop_all(&connect).await;
    let control = control(&connect).await;
    control
        .execute(&format!(
            "CREATE TABLE public.{TABLE} (supplierid bigint primary key, name text)"
        ))
        .await
        .expect("creates the fixture");
    control
        .execute(&format!("INSERT INTO public.{TABLE} VALUES (7, 'Acme')"))
        .await
        .expect("seeds a row");

    let engine = Engine::new();
    let mut source = PostgresCdcSource::from_options(None, &options(&connect)).expect("builds");
    let merge = CdcMerge::new(&cdc_config(), &source.schema(), TABLE).expect("plans the merge");

    let target = drain_into(&mut source, &merge, &engine, Vec::new()).await;
    assert_eq!(
        contents(&engine, &target).await,
        vec![(7, Some("Acme".into()))],
        "the snapshot lands as an upsert"
    );

    // The change under review: the row keeps its data and changes its identity. pgoutput sends
    // one `'u'` keyed 9; merged by key alone, that inserts a second row and leaves 7 behind —
    // a supplier that exists in the lakehouse and not in Postgres, forever.
    control
        .execute(&format!(
            "UPDATE public.{TABLE} SET supplierid = 9 WHERE supplierid = 7"
        ))
        .await
        .expect("moves the key");

    let target = drain_into(&mut source, &merge, &engine, target).await;
    assert_eq!(
        contents(&engine, &target).await,
        vec![(9, Some("Acme".into()))],
        "the old key is gone and the new one is there — not both"
    );

    // An ordinary update still moves nothing: only the row that changed identity is deleted.
    control
        .execute(&format!(
            "UPDATE public.{TABLE} SET name = 'Acme Ltd' WHERE supplierid = 9"
        ))
        .await
        .expect("updates a column");
    let target = drain_into(&mut source, &merge, &engine, target).await;
    assert_eq!(
        contents(&engine, &target).await,
        vec![(9, Some("Acme Ltd".into()))]
    );

    // The source holds the replication session; the slot cannot be dropped until it lets go.
    drop(source);
    drop_all(&connect).await;
}
