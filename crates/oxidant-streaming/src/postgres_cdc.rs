//! Postgres CDC micro-batch source: snapshot the tables, then stream the WAL.
//!
//! The engine speaks the Postgres wire protocol itself — no Kafka, no Debezium, no JVM — so a
//! pipeline that declares `format: postgres_cdc` needs nothing running beside it. What arrives
//! is a *change stream*: source columns plus `__oxidant_op` / `__oxidant_lsn` / `__oxidant_ts`,
//! which AUTO CDC merges into the target by key (SCD Type 1). See `docs/postgres-cdc.md`.
//!
//! Three things about this source are worth reading before the code.
//!
//! **The slot is only ever advanced by a durable batch.** Postgres keeps WAL until a consumer
//! confirms it, and confirming is the one irreversible act here: WAL past `confirmed_flush_lsn`
//! is gone. So the position a standby status update carries is always `self.position` — the last
//! batch the sink *and* the checkpoint both hold — and never one byte past it. Everything a batch
//! read is therefore still on the server if the batch dies, which is what makes a replayed range
//! reproducible and turns the offset log into exactly-once. Reading ahead of the confirmed
//! position costs nothing but memory; confirming ahead of the checkpoint costs data.
//!
//! Two calls send that message, and they send the same number. [`Source::mark_durable`] sends it
//! after a batch commits, which is what moves the slot. A keepalive answer sends it when the
//! publisher asks for a reply, or on a timer when the source has been silent for
//! `status_interval` — a walsender hangs up on a standby that says nothing for
//! `wal_sender_timeout`, so an idle pipeline has to speak, and re-sending an unchanged position
//! grants the server nothing.
//!
//! **`plan_batch` reads, and consumes nothing.** A WAL range's extent is not knowable without
//! decoding it: the byte budget is spent on *change* bytes, and the range must end on a commit
//! boundary. So planning decodes forward into memory and reports the range it covered, holding
//! the events for the poll that follows. Nothing durable moved, so a plan that is never polled
//! is simply re-planned — the same contract the file and Kafka sources keep.
//!
//! **The snapshot and the stream meet exactly once.** `CREATE_REPLICATION_SLOT … USE_SNAPSHOT`
//! hands back a `consistent_point` and a transaction that sees the database as of it. Rows read
//! in that transaction are emitted as `__oxidant_op='s'` and the stream then starts at the same
//! LSN, so no change is both in the snapshot and in the stream, and none falls between them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxidant_common::{Error, Result};
use oxidant_loom::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde_json::json;

use crate::connector_log::ConnectorLog;
use crate::pg_replication::{
    decode_logical, quote_identifier, quote_literal, ControlConnection, LogicalMessage, Lsn,
    PgConnectConfig, Relation, ReplicationConnection, TlsMode, TupleData, WireMessage,
    PG_EPOCH_UNIX_MICROS,
};
use crate::source::{BatchRange, Source, SourceOffsets};

/// Names this source in every batch range and checkpoint it writes, so a position recorded by
/// one source is never read by another.
pub const SOURCE_NAME: &str = "postgres_cdc";

/// How many source tables have been snapshotted. A span, so an interrupted snapshot resumes at
/// the table it stopped on.
const SNAPSHOT_KEY: &str = "snapshot";
/// The WAL position, as an `i64`.
const LSN_KEY: &str = "lsn";
/// The consistent point of the slot a snapshot range was planned against.
///
/// Carried in the range itself because the scheduler replays a *recorded* range without asking
/// the source to plan again — that is the whole point of the offset log — and a snapshot index on
/// its own does not say which database it means. A slot recreated after an interrupted snapshot
/// hands back a later point, and reading "source table 2" against it would splice two moments in
/// time together and lose every change between them.
const CONSISTENT_POINT_KEY: &str = "consistent_point";

/// `'s'` snapshot, `'i'` insert, `'u'` update, `'d'` delete, `'t'` truncate.
pub const OP_COLUMN: &str = "__oxidant_op";
/// The change's own WAL position — monotone, and what AUTO CDC orders by.
pub const LSN_COLUMN: &str = "__oxidant_lsn";
/// The publisher's commit timestamp. NULL for a snapshot row, which has no commit.
pub const TS_COLUMN: &str = "__oxidant_ts";

/// Slot retention past which the source refuses to run, in bytes.
///
/// A logical slot holds WAL until its consumer confirms, so a stopped or wedged pipeline is a
/// slow-motion outage on the *source* database's disk — the single most common operational
/// failure of logical replication. Ten gibibytes is large enough to ride out a long outage and
/// small enough to leave room on any real volume.
const DEFAULT_MAX_SLOT_BYTES: u64 = 10 * 1024 * 1024 * 1024;
/// Decoded change bytes one micro-batch may cover before it stops at the next commit boundary.
const DEFAULT_MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Rows per emitted Arrow batch. Named for the snapshot because that is where it matters — a
/// whole source table arrives as one micro-batch — but a WAL batch is chunked by it too.
const DEFAULT_SNAPSHOT_BATCH_ROWS: usize = 65_536;
/// How often an otherwise silent source sends a standby status update.
///
/// Postgres' walsender asks for a reply once it has not heard from the standby for
/// `wal_sender_timeout / 2` and hangs up at `wal_sender_timeout` — 60 seconds by default. A
/// pipeline over a quiet table would otherwise be killed overnight for having nothing to say, so
/// it speaks on a timer well inside that window even when it is completely caught up. The
/// message carries the position already committed, so it grants the server nothing.
const DEFAULT_STATUS_MS: u64 = 20_000;
/// How long planning waits for more WAL before deciding the publisher is caught up.
///
/// Only ever paid when the batch has *not* yet covered the flush LSN it is aiming at, so an idle
/// stream does not pay it: planning against a caught-up slot returns before the first read.
const DEFAULT_IDLE_MS: u64 = 500;

/// Type OIDs whose text form this source decodes into a non-string Arrow type.
mod oids {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const OID: u32 = 26;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const XID8: u32 = 5069;
}

/// The change kinds this source emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Snapshot,
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Op {
    pub fn code(self) -> &'static str {
        match self {
            Op::Snapshot => "s",
            Op::Insert => "i",
            Op::Update => "u",
            Op::Delete => "d",
            Op::Truncate => "t",
        }
    }

    /// The name this op has in a publication's `publish` list. A snapshot is a read, not a
    /// replicated operation, so it never appears in one.
    fn publication_name(self) -> Option<&'static str> {
        match self {
            Op::Insert => Some("insert"),
            Op::Update => Some("update"),
            Op::Delete => Some("delete"),
            Op::Truncate => Some("truncate"),
            Op::Snapshot => None,
        }
    }

    fn parse(text: &str) -> Option<Op> {
        match text.trim().to_ascii_lowercase().as_str() {
            "insert" | "i" => Some(Op::Insert),
            "update" | "u" => Some(Op::Update),
            "delete" | "d" => Some(Op::Delete),
            "truncate" | "t" => Some(Op::Truncate),
            _ => None,
        }
    }
}

/// A source column, as introspected from the publisher's catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub data_type: DataType,
    pub nullable: bool,
    /// What the mapping to `data_type` gives up, when it gives up anything. Logged once at
    /// startup rather than per batch.
    pub warning: Option<String>,
}

/// One replicated table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub schema: String,
    pub table: String,
    /// Emitted columns, in the publisher's column order, minus `exclude_columns`.
    pub columns: Vec<ColumnSchema>,
    /// The row's identity: the primary key, or the `keys:` override.
    pub keys: Vec<String>,
    /// `d` default (PK), `n` nothing, `f` full, `i` a named unique index.
    pub replica_identity: char,
}

impl TableSchema {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// The column list a snapshot `SELECT` projects — never `*`, which would pick up a column
    /// `exclude_columns` named or one added after introspection.
    fn projection(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Parsed `format: postgres_cdc` options.
#[derive(Debug, Clone)]
pub struct PostgresCdcOptions {
    pub connect: PgConnectConfig,
    pub publication: String,
    pub slot: String,
    /// `schema.table` entries, or `schema.*` for a whole schema.
    pub tables: Vec<String>,
    pub exclude_columns: BTreeSet<String>,
    /// Overrides the primary key as the row's identity.
    pub keys: Vec<String>,
    pub publish_ops: BTreeSet<Op>,
    pub max_slot_bytes: u64,
    pub max_batch_bytes: usize,
    pub snapshot_batch_rows: usize,
    pub idle: Duration,
    /// How often to send a standby status update when there is nothing else to say.
    pub status_interval: Duration,
    /// Where the connector's JSONL log goes, injected by the pipeline runner.
    pub log_dir: Option<PathBuf>,
    /// The pipeline table this source feeds, used to name the log file.
    pub name: String,
}

/// Options the pipeline runner injects; they are not part of the YAML surface.
const LOG_DIR_OPTION: &str = "oxidant.connector.log_dir";
const NAME_OPTION: &str = "oxidant.connector.name";

impl PostgresCdcOptions {
    /// Parse the `options:` block.
    ///
    /// Unknown keys are rejected rather than ignored. A misspelled `table:` would otherwise leave
    /// the source with no tables and a perfectly healthy-looking empty stream, and a misspelled
    /// `exclude_column:` would publish the column the author meant to keep out — both are silent,
    /// and both are worth one error at load time.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self> {
        const KNOWN: &[&str] = &[
            "host",
            "port",
            "database",
            "user",
            "password_env",
            "tls",
            "tls_ca",
            "publication",
            "slot",
            "tables",
            "exclude_columns",
            "keys",
            "publish_ops",
            "max_slot_bytes",
            "max_batch_bytes",
            "snapshot_batch_rows",
        ];
        for key in options.keys() {
            if !KNOWN.contains(&key.as_str()) && !key.starts_with("oxidant.") {
                return Err(Error::Plan(format!(
                    "postgres_cdc: `{key}` is not a source option (known: {})",
                    KNOWN.join(", ")
                )));
            }
        }
        let get = |key: &str| options.get(key).map(|v| v.trim()).filter(|v| !v.is_empty());
        let required = |key: &str| -> Result<String> {
            get(key).map(str::to_string).ok_or_else(|| {
                Error::Plan(format!("postgres_cdc: `{key}` is required in `options:`"))
            })
        };
        let number = |key: &str, default: u64| -> Result<u64> {
            match get(key) {
                None => Ok(default),
                Some(text) => text.parse::<u64>().map_err(|_| {
                    Error::Plan(format!(
                        "postgres_cdc: `{key}: {text}` is not a number of bytes"
                    ))
                }),
            }
        };

        let list = |key: &str| -> Vec<String> {
            get(key)
                .map(|v| {
                    v.split(',')
                        .map(|item| item.trim().to_string())
                        .filter(|item| !item.is_empty())
                        .collect()
                })
                .unwrap_or_default()
        };

        let tables: Vec<String> = list("tables")
            .into_iter()
            .map(|t| qualify(&t))
            .collect::<Result<Vec<_>>>()?;
        if tables.is_empty() {
            return Err(Error::Plan(
                "postgres_cdc: `tables:` must name at least one `schema.table` (or `schema.*`)"
                    .into(),
            ));
        }

        // The password is read from the environment by name, never from the file: an
        // `oxidant.yaml` is checked into a repository, and a literal secret in one is a leak
        // that outlives the pipeline.
        let password = match get("password_env") {
            Some(variable) => Some(std::env::var(variable).map_err(|_| {
                Error::Plan(format!(
                    "postgres_cdc: `password_env: {variable}` names an environment variable that \
                     is not set"
                ))
            })?),
            None => None,
        };

        let publish_ops = match get("publish_ops") {
            None => [Op::Insert, Op::Update, Op::Delete, Op::Truncate]
                .into_iter()
                .collect(),
            Some(_) => {
                let mut ops = BTreeSet::new();
                for item in list("publish_ops") {
                    ops.insert(Op::parse(&item).ok_or_else(|| {
                        Error::Plan(format!(
                            "postgres_cdc: `publish_ops` has `{item}` (expected any of insert, \
                             update, delete, truncate)"
                        ))
                    })?);
                }
                if ops.is_empty() {
                    return Err(Error::Plan(
                        "postgres_cdc: `publish_ops:` is empty — the stream would carry nothing"
                            .into(),
                    ));
                }
                ops
            }
        };

        let port = match get("port") {
            None => 5432,
            Some(text) => text.parse::<u16>().map_err(|_| {
                Error::Plan(format!("postgres_cdc: `port: {text}` is not a port number"))
            })?,
        };

        Ok(Self {
            connect: PgConnectConfig {
                host: required("host")?,
                port,
                database: required("database")?,
                user: required("user")?,
                password,
                tls: TlsMode::parse(get("tls").unwrap_or("verify-full"))?,
                tls_ca: get("tls_ca").map(PathBuf::from),
            },
            publication: required("publication")?,
            slot: required("slot")?,
            tables,
            exclude_columns: list("exclude_columns")
                .into_iter()
                .map(|c| c.to_ascii_lowercase())
                .collect(),
            keys: list("keys"),
            publish_ops,
            max_slot_bytes: number("max_slot_bytes", DEFAULT_MAX_SLOT_BYTES)?,
            max_batch_bytes: number("max_batch_bytes", DEFAULT_MAX_BATCH_BYTES as u64)? as usize,
            snapshot_batch_rows: number("snapshot_batch_rows", DEFAULT_SNAPSHOT_BATCH_ROWS as u64)?
                .max(1) as usize,
            idle: Duration::from_millis(
                std::env::var("OXIDANT_PG_CDC_IDLE_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_IDLE_MS),
            ),
            // Not a YAML option: the only right value is "comfortably under the publisher's
            // `wal_sender_timeout`", which is not something a pipeline author should have to
            // reason about. The environment variable exists so a test can make it fire at once.
            status_interval: Duration::from_millis(
                std::env::var("OXIDANT_PG_CDC_STATUS_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_STATUS_MS),
            ),
            log_dir: options.get(LOG_DIR_OPTION).map(PathBuf::from),
            name: options
                .get(NAME_OPTION)
                .cloned()
                .unwrap_or_else(|| SOURCE_NAME.to_string()),
        })
    }

    /// The `publish` list for `CREATE PUBLICATION`.
    fn publish_list(&self) -> String {
        self.publish_ops
            .iter()
            .filter_map(|op| op.publication_name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Normalize a `tables:` entry to `schema.table`, defaulting the schema to `public`.
fn qualify(entry: &str) -> Result<String> {
    let entry = entry.trim();
    let (schema, table) = match entry.split_once('.') {
        Some((schema, table)) => (schema.trim(), table.trim()),
        None => ("public", entry),
    };
    if schema.is_empty() || table.is_empty() || table.contains('.') {
        return Err(Error::Plan(format!(
            "postgres_cdc: `tables:` entry `{entry}` is not a `schema.table` name"
        )));
    }
    // Postgres folds unquoted identifiers to lower case, and every catalog lookup below compares
    // against the folded spelling.
    Ok(format!(
        "{}.{}",
        schema.to_ascii_lowercase(),
        table.to_ascii_lowercase()
    ))
}

// ---------------------------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------------------------

/// Map a Postgres type to the Arrow type this source emits for it, per `docs/postgres-cdc.md` §3.
///
/// Returns the type and, when the mapping loses something, the warning to log. Anything not
/// recognized falls back to `Utf8` holding the type's own text form, which is lossless and
/// queryable — the alternative, refusing to start because a column is an `hstore`, would make
/// one exotic column block a table of ordinary ones.
pub fn arrow_type_for(
    type_oid: u32,
    type_modifier: i32,
    type_name: &str,
    type_category: char,
) -> (DataType, Option<String>) {
    match type_oid {
        oids::BOOL => (DataType::Boolean, None),
        oids::INT2 | oids::INT4 => (DataType::Int32, None),
        oids::INT8 | oids::OID | oids::XID8 => (DataType::Int64, None),
        oids::FLOAT4 => (DataType::Float32, None),
        oids::FLOAT8 => (DataType::Float64, None),
        oids::BYTEA => (DataType::Binary, None),
        oids::DATE => (DataType::Date32, None),
        oids::TIME => (DataType::Time64(TimeUnit::Microsecond), None),
        oids::TIMESTAMP => (DataType::Timestamp(TimeUnit::Microsecond, None), None),
        oids::TIMESTAMPTZ => (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            None,
        ),
        oids::NUMERIC => match decimal_scale(type_modifier) {
            Some(scale) => (DataType::Decimal128(38, scale), None),
            // An unconstrained `numeric` has no precision or scale at all, and can hold values
            // no fixed-point type can. Text keeps every digit; a Decimal128(38, 9) would
            // silently round the first invoice that needed a tenth decimal place.
            None => (
                DataType::Utf8,
                Some(
                    "unconstrained `numeric` has no scale to map to a decimal; emitting its text \
                     form. Declare `numeric(p,s)` on the source column for a typed value."
                        .into(),
                ),
            ),
        },
        _ if type_category == 'A' => (
            DataType::Utf8,
            Some(format!(
                "array type `{type_name}` is emitted as its Postgres text form (`{{a,b}}`)"
            )),
        ),
        _ => (DataType::Utf8, None),
    }
}

/// `atttypmod` for `numeric(p,s)` is `((p << 16) | s) + 4`; `-1` means unconstrained.
fn decimal_scale(type_modifier: i32) -> Option<i8> {
    if type_modifier < 4 {
        return None;
    }
    let packed = type_modifier - 4;
    let precision = (packed >> 16) & 0xFFFF;
    let scale = packed & 0xFFFF;
    // Arrow's Decimal128 tops out at 38 digits, and a scale wider than the precision is not a
    // thing Postgres produces.
    if precision > 38 || scale > precision {
        return None;
    }
    Some(scale as i8)
}

/// The schema this source emits: the table's columns, then the three metadata columns.
pub fn emitted_schema(columns: &[ColumnSchema]) -> SchemaRef {
    let mut fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            // Every column is nullable in the emitted schema whatever the source says: a delete
            // under `REPLICA IDENTITY DEFAULT` carries only the key, and an unchanged TOAST
            // column carries nothing at all.
            Field::new(c.name.clone(), c.data_type.clone(), true)
        })
        .collect();
    fields.push(Field::new(OP_COLUMN, DataType::Utf8, false));
    fields.push(Field::new(LSN_COLUMN, DataType::Int64, false));
    fields.push(Field::new(
        TS_COLUMN,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    ));
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------------------------

/// Build one Arrow array from the text form of every value in a column.
fn build_array(data_type: &DataType, column: &str, values: &[Option<&str>]) -> Result<ArrayRef> {
    fn bad(column: &str, value: &str, what: &str) -> Error {
        Error::Execution(format!(
            "postgres_cdc: column `{column}`: `{value}` is not {what}. Exclude the column with \
             `exclude_columns:` if the source can hold values this mapping cannot represent."
        ))
    }

    Ok(match data_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    // Postgres' boolean output is `t`/`f`; the longer spellings appear only when
                    // a client cast one to text, which a `keys:` expression can do.
                    Some("t" | "true" | "y" | "yes" | "on" | "1") => b.append_value(true),
                    Some("f" | "false" | "n" | "no" | "off" | "0") => b.append_value(false),
                    Some(other) => return Err(bad(column, other, "a boolean")),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(
                        text.parse::<i32>()
                            .map_err(|_| bad(column, text, "a 32-bit integer"))?,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(
                        text.parse::<i64>()
                            .map_err(|_| bad(column, text, "a 64-bit integer"))?,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(
                        parse_float(text).map_err(|_| bad(column, text, "a number"))? as f32,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(
                        parse_float(text).map_err(|_| bad(column, text, "a number"))?,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(precision, scale) => {
            let mut b = Decimal128Builder::with_capacity(values.len())
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| Error::Execution(format!("postgres_cdc: column `{column}`: {e}")))?;
            for value in values {
                match value {
                    None => b.append_null(),
                    // `numeric` alone among the numeric types has a NaN, and no decimal does.
                    Some("NaN") => b.append_null(),
                    Some(text) => b.append_value(
                        parse_decimal(text, *scale)
                            .ok_or_else(|| bad(column, text, "a decimal number"))?,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => {
                        let hex = text.strip_prefix("\\x").ok_or_else(|| {
                            bad(column, text, "a hex-encoded bytea (`bytea_output = 'hex'`)")
                        })?;
                        b.append_value(
                            hex::decode(hex)
                                .map_err(|_| bad(column, text, "a hex-encoded bytea"))?,
                        );
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => {
                        b.append_value(parse_date(text).ok_or_else(|| bad(column, text, "a date"))?)
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(
                        parse_time_micros(text).ok_or_else(|| bad(column, text, "a time"))?,
                    ),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(
                        parse_timestamp_micros(text)
                            .ok_or_else(|| bad(column, text, "a timestamp"))?,
                    ),
                }
            }
            let array = match zone {
                Some(zone) => b.finish().with_timezone(zone.clone()),
                None => b.finish(),
            };
            Arc::new(array)
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(text) => b.append_value(text),
                }
            }
            Arc::new(b.finish())
        }
        other => {
            return Err(Error::Unsupported(format!(
                "postgres_cdc: column `{column}` maps to `{other}`, which this source does not \
                 build"
            )))
        }
    })
}

/// Postgres prints the three special floats as words; `str::parse` reads two of the three.
fn parse_float(text: &str) -> std::result::Result<f64, ()> {
    match text {
        "NaN" => Ok(f64::NAN),
        "Infinity" => Ok(f64::INFINITY),
        "-Infinity" => Ok(f64::NEG_INFINITY),
        other => other.parse::<f64>().map_err(|_| ()),
    }
}

/// Parse `-123.45` into the unscaled integer a `Decimal128(_, scale)` holds.
///
/// Done digit by digit rather than through `f64`: a `numeric(38,10)` does not survive a round
/// trip through a double, and the whole point of mapping it to a decimal is that it does not
/// have to.
fn parse_decimal(text: &str, scale: i8) -> Option<i128> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = match digits.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (digits, ""),
    };
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let scale = scale.max(0) as usize;
    let mut unscaled: i128 = 0;
    for byte in whole.bytes().chain(fraction.bytes().take(scale)) {
        unscaled = unscaled
            .checked_mul(10)?
            .checked_add((byte - b'0') as i128)?;
    }
    // A value with fewer fractional digits than the declared scale still has to land on it.
    for _ in fraction.len().min(scale)..scale {
        unscaled = unscaled.checked_mul(10)?;
    }
    Some(if negative { -unscaled } else { unscaled })
}

fn parse_date(text: &str) -> Option<i32> {
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    Some(
        date.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?)
            .num_days() as i32,
    )
}

fn parse_time_micros(text: &str) -> Option<i64> {
    let time = chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f").ok()?;
    time.signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0)?)
        .num_microseconds()
}

/// Parse `2024-05-06 07:08:09.123456` (and the `+00` a `timestamptz` carries).
///
/// The trailing offset is always `+00`: both connections pin `TIME ZONE 'UTC'` at startup, so a
/// `timestamptz` is printed in UTC whatever the database's own setting is.
fn parse_timestamp_micros(text: &str) -> Option<i64> {
    let text = text.trim();
    let naive = text
        .strip_suffix("+00")
        .or_else(|| text.strip_suffix("Z"))
        .unwrap_or(text)
        .trim();
    let parsed = chrono::NaiveDateTime::parse_from_str(naive, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(naive, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()?;
    Some(parsed.and_utc().timestamp_micros())
}

// ---------------------------------------------------------------------------------------------
// The wire the source drives
// ---------------------------------------------------------------------------------------------

/// What opening the slot produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotOpen {
    /// Where the stream must start. For a freshly created slot this is also the LSN the snapshot
    /// transaction sees the database as of.
    pub consistent_point: Lsn,
    pub existed: bool,
    /// True while the slot's snapshot transaction is open and its rows are still readable.
    pub snapshot_open: bool,
}

/// What the server says about the slot right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotMetrics {
    /// WAL the slot is keeping alive, in bytes.
    pub retained_bytes: u64,
    /// The publisher's current flush LSN, which bounds what a batch can cover.
    pub server_flush: Lsn,
    pub confirmed_flush: Option<Lsn>,
}

/// Everything the source needs from a Postgres server, behind a trait so the sequencing above it
/// is testable without one.
#[async_trait::async_trait]
pub(crate) trait CdcWire: Send + Sync {
    /// Find or create the replication slot. `create` drops and recreates it inside a snapshot
    /// transaction; without it an absent slot is reported, not conjured.
    async fn open_slot(&mut self, create: bool) -> Result<SlotOpen>;
    /// Read `table` inside the slot's snapshot transaction.
    async fn snapshot_rows(&mut self, table: &TableSchema) -> Result<Vec<Vec<Option<String>>>>;
    /// Commit the snapshot transaction.
    async fn end_snapshot(&mut self) -> Result<()>;
    /// (Re)start the replication stream at `start`.
    async fn start_stream(&mut self, start: Lsn) -> Result<()>;
    /// The next frame, or `None` when the publisher stayed quiet for `idle`.
    async fn next_wire(&mut self, idle: Duration) -> Result<Option<WireMessage>>;
    /// Tell the server it may recycle WAL up to `flushed`.
    ///
    /// Returns whether a standby status update was actually sent. There is no session to send
    /// one on during the snapshot, and none after a connection failure, and a caller that logs
    /// "confirmed" for a message that never left the process gives an operator a connector log
    /// that disagrees with `pg_replication_slots` for no visible reason.
    async fn confirm(&mut self, written: Lsn, flushed: Lsn) -> Result<bool>;
    async fn slot_metrics(&mut self) -> Result<SlotMetrics>;
}

/// The real wire: an ordinary SQL connection for control, and a replication session for the slot,
/// the snapshot and the stream.
pub(crate) struct PgWire {
    connect: PgConnectConfig,
    slot: String,
    publication: String,
    control: Option<ControlConnection>,
    replication: Option<ReplicationConnection>,
}

impl PgWire {
    pub(crate) fn new(options: &PostgresCdcOptions) -> Self {
        Self {
            connect: options.connect.clone(),
            slot: options.slot.clone(),
            publication: options.publication.clone(),
            control: None,
            replication: None,
        }
    }

    async fn control(&mut self) -> Result<&ControlConnection> {
        if self.control.is_none() {
            self.control = Some(self.connect.connect_control().await?);
        }
        Ok(self.control.as_ref().expect("just connected"))
    }

    /// The replication session, reconnecting if there is none.
    ///
    /// Reconnecting is also how the stream is re-seeked: a CopyBoth session cannot be rewound,
    /// and a fresh socket that issues `START_REPLICATION` at an earlier LSN is both simpler and
    /// exactly what a restarted process would do anyway.
    async fn replication(&mut self) -> Result<&mut ReplicationConnection> {
        if self.replication.is_none() {
            self.replication = Some(ReplicationConnection::connect(&self.connect).await?);
        }
        Ok(self.replication.as_mut().expect("just connected"))
    }

    /// Forget the replication session when a call on it failed.
    ///
    /// A dead socket is only ever discovered by using it, and every caller here is one retry
    /// away from asking again. Dropping the session is what makes the next attempt dial a fresh
    /// one and re-issue `START_REPLICATION` from the checkpointed position — without it, a
    /// publisher restart or an idle `wal_sender_timeout` answers every retry with the same error
    /// until the process is restarted. Cheap and idempotent: reconnecting costs one handshake,
    /// and the slot holds the WAL either way.
    fn forget_on_error<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.replication = None;
        }
        result
    }

    /// Drop the slot, waiting a bounded time for whoever holds it to let go.
    ///
    /// `DROP_REPLICATION_SLOT … WAIT` blocks until the slot goes inactive, and there is no
    /// statement timeout on a replication session to bound it — one can be set, but it would
    /// then also apply to the snapshot `FETCH`es, which are allowed to take as long as the table
    /// is big. So the wait is done here instead: poll `pg_replication_slots.active_pid` until it
    /// clears, then drop a slot that is already inactive. A pipeline started twice onto one
    /// `slot:` gets a diagnosis naming the process holding it rather than a start that hangs
    /// forever with nothing in the log.
    async fn drop_slot(&mut self, slot: &str) -> Result<()> {
        const WAIT: Duration = Duration::from_secs(30);
        let deadline = Instant::now() + WAIT;
        loop {
            let held = self
                .control()
                .await?
                .query(
                    "SELECT active_pid::text FROM pg_replication_slots \
                     WHERE slot_name::text = $1 AND active_pid IS NOT NULL",
                    &[slot],
                )
                .await?;
            let Some(pid) = held
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.clone())
            else {
                break;
            };
            if Instant::now() >= deadline {
                return Err(Error::Execution(format!(
                    "postgres_cdc: replication slot `{slot}` has to be recreated to restart an \
                     interrupted snapshot, but backend pid {pid} has held it for {}s. That is \
                     usually a previous run of this pipeline that has not exited, or a second \
                     pipeline pointed at the same `slot:`. Stop it (or give this pipeline a slot \
                     of its own); if the holder is gone, clear it with \
                     `SELECT pg_terminate_backend({pid});`.",
                    WAIT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let dropped = self
            .replication()
            .await?
            .execute(&format!("DROP_REPLICATION_SLOT {}", quote_identifier(slot)))
            .await;
        self.forget_on_error(dropped)
    }
}

#[async_trait::async_trait]
impl CdcWire for PgWire {
    async fn open_slot(&mut self, create: bool) -> Result<SlotOpen> {
        let slot = self.slot.clone();
        let existing = self
            .control()
            .await?
            .query(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name::text = $1",
                &[slot.as_str()],
            )
            .await?;
        // Presence is the row, not the value: a slot that has never confirmed anything has a
        // NULL `confirmed_flush_lsn` and is still very much there.
        let existed = !existing.is_empty();

        if !create {
            let confirmed = existing.into_iter().next().and_then(|mut row| {
                if row.is_empty() {
                    None
                } else {
                    row.swap_remove(0)
                }
            });
            return Ok(SlotOpen {
                consistent_point: match confirmed {
                    Some(text) => Lsn::parse(&text)?,
                    None => Lsn(0),
                },
                existed,
                snapshot_open: false,
            });
        }

        if existed {
            // Nothing durable depends on the old slot: `create` is only ever asked for when the
            // snapshot has not completed, so no batch has been committed against it.
            self.drop_slot(&slot).await?;
        }
        // `CREATE_REPLICATION_SLOT … USE_SNAPSHOT` must be the first command of its transaction,
        // and the transaction has to outlive the command — that is what keeps the snapshot
        // readable for the COPY that follows.
        let connection = self.replication().await?;
        let begun = connection
            .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
            .await;
        self.forget_on_error(begun)?;
        let connection = self.replication().await?;
        let created = connection
            .simple_query(&format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput USE_SNAPSHOT",
                quote_identifier(&slot)
            ))
            .await;
        let created = self.forget_on_error(created)?;
        let consistent_point = created.first("consistent_point").ok_or_else(|| {
            Error::Io(format!(
                "postgres_cdc: CREATE_REPLICATION_SLOT `{slot}` returned no consistent_point"
            ))
        })?;
        let consistent_point = Lsn::parse(consistent_point)?;
        Ok(SlotOpen {
            consistent_point,
            existed,
            snapshot_open: true,
        })
    }

    async fn snapshot_rows(&mut self, table: &TableSchema) -> Result<Vec<Vec<Option<String>>>> {
        let sql = format!(
            "SELECT {} FROM {}.{}",
            table.projection(),
            quote_identifier(&table.schema),
            quote_identifier(&table.table)
        );
        let rows = self.replication().await?.simple_query(&sql).await;
        Ok(self.forget_on_error(rows)?.rows)
    }

    async fn end_snapshot(&mut self) -> Result<()> {
        let done = self.replication().await?.execute("COMMIT").await;
        self.forget_on_error(done)
    }

    async fn start_stream(&mut self, start: Lsn) -> Result<()> {
        if self
            .replication
            .as_ref()
            .is_some_and(ReplicationConnection::is_streaming)
        {
            // A streaming session cannot take a new start position; drop it and dial again.
            self.replication = None;
        }
        let (slot, publication) = (self.slot.clone(), self.publication.clone());
        let started = self
            .replication()
            .await?
            .start_replication(&slot, &publication, start)
            .await;
        self.forget_on_error(started)
    }

    async fn next_wire(&mut self, idle: Duration) -> Result<Option<WireMessage>> {
        let next = self.replication().await?.next_wire(idle).await;
        self.forget_on_error(next)
    }

    async fn confirm(&mut self, written: Lsn, flushed: Lsn) -> Result<bool> {
        match self.replication.as_mut() {
            Some(connection) if connection.is_streaming() => {
                let sent = connection.send_standby(written, flushed).await;
                self.forget_on_error(sent)?;
                Ok(true)
            }
            // Nothing to confirm against: the next `START_REPLICATION` will begin from the
            // checkpointed position anyway, and the slot keeps the WAL until then. The caller
            // reports the `false` so the connector log never claims a confirmation the server
            // was never told about.
            _ => Ok(false),
        }
    }

    async fn slot_metrics(&mut self) -> Result<SlotMetrics> {
        let slot = self.slot.clone();
        let row = self
            .control()
            .await?
            .query(
                "SELECT pg_current_wal_flush_lsn()::text, \
                        s.confirmed_flush_lsn::text, \
                        COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn), 0)::int8::text \
                 FROM (SELECT 1) AS one \
                 LEFT JOIN pg_replication_slots s ON s.slot_name::text = $1",
                &[slot.as_str()],
            )
            .await?;
        let row = row.first().ok_or_else(|| {
            Error::Io("postgres_cdc: the server did not report its flush LSN".into())
        })?;
        Ok(SlotMetrics {
            server_flush: match row.first().and_then(|v| v.as_deref()) {
                Some(text) => Lsn::parse(text)?,
                None => Lsn(0),
            },
            confirmed_flush: match row.get(1).and_then(|v| v.as_deref()) {
                Some(text) => Some(Lsn::parse(text)?),
                None => None,
            },
            retained_bytes: row
                .get(2)
                .and_then(|v| v.as_deref())
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0)
                .max(0) as u64,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------------------------

/// One row of the change stream, before it becomes Arrow.
#[derive(Debug, Clone, PartialEq)]
struct ChangeEvent {
    op: Op,
    /// The change's own WAL position, which is monotone across the whole stream.
    lsn: i64,
    /// The publisher's commit timestamp, in microseconds since the Unix epoch.
    ts_micros: Option<i64>,
    /// One entry per emitted source column.
    values: Vec<Option<String>>,
}

/// The `Source` implementation.
pub struct PostgresCdcSource {
    options: PostgresCdcOptions,
    tables: Vec<TableSchema>,
    schema: SchemaRef,
    wire: Box<dyn CdcWire>,
    log: ConnectorLog,
    /// Relation shapes as the publisher last described them, keyed by OID.
    relations: HashMap<u32, Relation>,
    /// How each relation's columns map onto the emitted schema.
    projections: HashMap<u32, Vec<Option<usize>>>,
    /// Relations already reported as having drifted from the introspected schema.
    reported_drift: BTreeSet<u32>,
    /// Source tables whose snapshot has been committed.
    snapshot_done: usize,
    /// The last committed WAL position — what a replay resumes from, and the only LSN ever
    /// confirmed to the server.
    position: Lsn,
    /// Set once the slot has been found or created this run.
    opened: bool,
    /// True while the slot's snapshot transaction is still open.
    snapshot_open: bool,
    /// Where the open replication stream has been consumed to, if one is open.
    stream_at: Option<Lsn>,
    /// The events `plan_batch` decoded, held for the poll of the range it described.
    planned: Option<(BatchRange, Vec<ChangeEvent>)>,
    /// When a standby status update was last sent, so an idle stream still answers the walsender
    /// before `wal_sender_timeout` runs out.
    last_status: Instant,
}

impl PostgresCdcSource {
    /// Build the source: validate the server's setup, introspect the tables, and fix the schema.
    ///
    /// Blocking, on a thread of its own, because `build_source` is synchronous — the streaming
    /// DataFrame is planned against `schema()` before any batch runs, and for this source that
    /// answer only exists after a round trip to the publisher. One connection at query start is
    /// worth paying to have `wal_level`, privileges, the publication and every REPLICA IDENTITY
    /// checked before a pipeline claims to be replicating.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self> {
        let parsed = PostgresCdcOptions::from_options(options)?;
        let log = ConnectorLog::new(parsed.log_dir.as_deref(), &parsed.name);
        let tables = match bootstrap_blocking(&parsed) {
            Ok(tables) => tables,
            Err(e) => {
                log.error(&e.to_string(), false);
                return Err(e);
            }
        };
        for warning in schema_warnings(&tables) {
            log.event("schema_change", json!({ "warning": warning }));
            eprintln!("[oxidant] postgres_cdc {}: {warning}", parsed.name);
        }
        let wire = Box::new(PgWire::new(&parsed));
        Ok(Self::with_wire(parsed, tables, wire, log))
    }

    /// Assemble a source over an already-introspected schema and a caller-supplied wire.
    pub(crate) fn with_wire(
        options: PostgresCdcOptions,
        tables: Vec<TableSchema>,
        wire: Box<dyn CdcWire>,
        log: ConnectorLog,
    ) -> Self {
        let schema = emitted_schema(tables.first().map(|t| t.columns.as_slice()).unwrap_or(&[]));
        Self {
            options,
            tables,
            schema,
            wire,
            log,
            relations: HashMap::new(),
            projections: HashMap::new(),
            reported_drift: BTreeSet::new(),
            snapshot_done: 0,
            position: Lsn(0),
            opened: false,
            snapshot_open: false,
            stream_at: None,
            planned: None,
            // A source that has just started has nothing to keep alive yet, and the first
            // interval is measured from here rather than from the epoch.
            last_status: Instant::now(),
        }
    }

    /// Find or create the slot, once per run.
    async fn ensure_open(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        // An interrupted snapshot cannot be resumed: the transaction that held it ended with the
        // process, and a fresh one would see a *later* database. Re-snapshotting every table is
        // the only consistent answer, and it is a cheap one — snapshot rows are upserts merged
        // by key, so re-emitting the tables already loaded changes nothing but the clock.
        let restored_complete = !self.tables.is_empty() && self.snapshot_done >= self.tables.len();
        if !restored_complete && self.snapshot_done > 0 {
            self.log.event(
                "snapshot_start",
                json!({
                    "reason": "an earlier snapshot was interrupted; restarting it from the first \
                               table",
                    "tables_already_loaded": self.snapshot_done,
                }),
            );
        }

        let opened = self.wire.open_slot(!restored_complete).await?;
        if restored_complete {
            if !opened.existed {
                // Losing the slot means the publisher may have recycled the WAL this pipeline
                // has not read. Carrying on would skip changes and say nothing, so it stops.
                return Err(Error::Execution(format!(
                    "postgres_cdc: replication slot `{}` no longer exists, but this pipeline has \
                     committed up to LSN {}. WAL written since then may already be recycled, so \
                     resuming would silently skip changes. Re-snapshot by deleting this table's \
                     checkpoint directory, then restart the pipeline.",
                    self.options.slot, self.position
                )));
            }
        } else {
            self.snapshot_done = 0;
            self.position = opened.consistent_point;
            self.snapshot_open = opened.snapshot_open;
            self.stream_at = None;
            self.log.event(
                "snapshot_start",
                json!({
                    "slot": self.options.slot,
                    "consistent_point": self.position.to_string(),
                    "tables": self
                        .tables
                        .iter()
                        .map(|t| json!({
                            "table": t.qualified(),
                            // The identity a delete is matched on, and the REPLICA IDENTITY that
                            // decides how much of the old row arrives with one.
                            "keys": t.keys,
                            "replica_identity": t.replica_identity.to_string(),
                        }))
                        .collect::<Vec<_>>(),
                }),
            );
        }
        self.opened = true;
        Ok(())
    }

    /// Refuse to run while the slot is holding more WAL than the operator allowed.
    ///
    /// A slot nobody confirms grows without bound and takes the *source* database's disk with
    /// it. Stopping with a loud error is the only outcome that leaves the operator a database to
    /// fix; carrying on would make this pipeline the cause of an outage somewhere else.
    async fn guard_slot(&mut self) -> Result<SlotMetrics> {
        let metrics = self.wire.slot_metrics().await?;
        self.log.event(
            "slot_metrics",
            json!({
                "slot": self.options.slot,
                "retained_bytes": metrics.retained_bytes,
                "server_flush_lsn": metrics.server_flush.to_string(),
                "confirmed_flush_lsn": metrics.confirmed_flush.map(|l| l.to_string()),
                "position": self.position.to_string(),
                "lag_bytes": metrics.server_flush.0.saturating_sub(self.position.0),
            }),
        );
        if metrics.retained_bytes > self.options.max_slot_bytes {
            let message = format!(
                "postgres_cdc: replication slot `{}` is holding {} of WAL, over the \
                 `max_slot_bytes` limit of {}. The source database's disk fills next, so this \
                 pipeline stops rather than let it. Drain the backlog (or raise \
                 `max_slot_bytes:`); if the pipeline is being retired, drop the slot with \
                 `SELECT pg_drop_replication_slot({});`.",
                self.options.slot,
                bytes(metrics.retained_bytes),
                bytes(self.options.max_slot_bytes),
                quote_literal(&self.options.slot),
            );
            self.log.error(&message, false);
            return Err(Error::Execution(message));
        }
        Ok(metrics)
    }

    /// Send a standby status update carrying the committed position, and nothing past it.
    ///
    /// `flushed` is the number that lets Postgres recycle WAL, so it is always `self.position` —
    /// what a restart would resume from. Re-sending an unchanged position is free: it moves the
    /// slot nowhere and tells the walsender the standby is alive, which is the whole point of
    /// answering a keepalive.
    async fn confirm_position(&mut self) -> Result<bool> {
        let sent = self.wire.confirm(self.position, self.position).await?;
        self.last_status = Instant::now();
        Ok(sent)
    }

    /// Speak up if the source has been silent for `status_interval`.
    async fn heartbeat(&mut self) -> Result<()> {
        if self.last_status.elapsed() < self.options.status_interval {
            return Ok(());
        }
        let sent = self.confirm_position().await?;
        self.log.event(
            "standby_status",
            json!({
                "reason": "keepalive",
                "confirmed_flush_lsn": self.position.to_string(),
                "sent": sent,
            }),
        );
        Ok(())
    }

    /// Point the replication stream at `start`, restarting it when it is somewhere else.
    ///
    /// `stream_at` is a claim about a socket, so it is only ever true while that socket is
    /// healthy: any failure forgets it, and the next attempt dials again and re-issues
    /// `START_REPLICATION` from the checkpointed position. Believing it after an I/O error is
    /// what turns a publisher restart into a permanently wedged pipeline.
    async fn ensure_stream(&mut self, start: Lsn) -> Result<()> {
        // `START_REPLICATION` cannot be issued inside the snapshot's transaction, so the stream
        // is also the backstop for closing it: ordinarily `mark_durable` already has, but a plan
        // that follows a snapshot batch nothing marked durable would otherwise find it open.
        self.finish_snapshot().await?;
        if self.stream_at == Some(start) {
            return Ok(());
        }
        if let Err(e) = self.wire.start_stream(start).await {
            self.stream_at = None;
            return Err(e);
        }
        self.stream_at = Some(start);
        Ok(())
    }

    /// Decode forward from `self.position`, stopping at a commit boundary.
    ///
    /// Returns the events and the LSN the range ends at (exclusive of nothing — it *is* a commit
    /// boundary, so a range ending there covers whole transactions only).
    async fn read_forward(
        &mut self,
        from: Lsn,
        stop_at: Lsn,
        byte_budget: usize,
    ) -> Result<(Vec<ChangeEvent>, Lsn)> {
        self.ensure_stream(from).await?;

        let mut committed: Vec<ChangeEvent> = Vec::new();
        let mut open_txn: Vec<ChangeEvent> = Vec::new();
        let mut bytes_seen = 0usize;
        let mut end = from;
        let mut observed = from;
        // Whether the stream is between a `Begin` and its `Commit`, which is *not* the same
        // question as whether `open_txn` holds anything: a transaction touching only tables this
        // source does not replicate, or carrying only ops `publish_ops` excludes, leaves
        // `open_txn` empty while the stream is very much mid-transaction. Reading the one for the
        // other is what let an earlier version end — and confirm — a range inside an open
        // transaction.
        let mut in_txn = false;

        loop {
            // A batch that has already covered whole transactions stops as soon as it has
            // reached either the flush LSN it was aiming at or its byte budget. Both tests are
            // made between transactions so a batch never ends inside one.
            if !in_txn && (observed >= stop_at || bytes_seen >= byte_budget) {
                break;
            }
            let next = match self.wire.next_wire(self.options.idle).await {
                Ok(next) => next,
                Err(e) => {
                    // The socket is gone. Forget where the stream was so the next attempt dials
                    // a fresh one rather than reading from a dead one forever.
                    self.stream_at = None;
                    return Err(e);
                }
            };
            let Some(message) = next else {
                // The publisher went quiet. Whatever it has sent is what this batch covers.
                break;
            };
            match message {
                WireMessage::Keepalive {
                    wal_end,
                    reply_requested,
                    ..
                } => {
                    observed = observed.max(wal_end);
                    // "Answer me or I will hang up": the walsender sets this once it has not
                    // heard from the standby for `wal_sender_timeout / 2`, and closes the socket
                    // at `wal_sender_timeout`. The answer carries the position already
                    // committed — never one past it — so replying costs the durability contract
                    // nothing and costs the pipeline its connection if it is skipped.
                    if reply_requested {
                        let sent = self.confirm_position().await?;
                        self.log.event(
                            "standby_status",
                            json!({
                                "reason": "reply_requested",
                                "confirmed_flush_lsn": self.position.to_string(),
                                "sent": sent,
                            }),
                        );
                    }
                }
                WireMessage::XLogData {
                    wal_start,
                    wal_end,
                    data,
                    ..
                } => {
                    observed = observed.max(wal_end);
                    bytes_seen += data.len();
                    match decode_logical(&data)? {
                        LogicalMessage::Relation(relation) => self.remember(relation),
                        LogicalMessage::Begin { .. } => {
                            in_txn = true;
                            open_txn.clear();
                        }
                        LogicalMessage::Commit {
                            end_lsn,
                            commit_time,
                            ..
                        } => {
                            observed = observed.max(end_lsn);
                            in_txn = false;
                            // Postgres restarts a stream at the oldest *unconfirmed*
                            // transaction, which can be older than the LSN asked for. A
                            // transaction that ended at or before `from` is already downstream,
                            // so re-emitting it would widen the batch past its recorded range.
                            if end_lsn <= from {
                                open_txn.clear();
                                continue;
                            }
                            let ts = Some(commit_time + PG_EPOCH_UNIX_MICROS);
                            for mut event in open_txn.drain(..) {
                                event.ts_micros = ts;
                                committed.push(event);
                            }
                            end = end_lsn;
                        }
                        LogicalMessage::Insert { relation, new } => {
                            self.push_change(&mut open_txn, relation, Op::Insert, wal_start, &new)
                        }
                        LogicalMessage::Update {
                            relation,
                            key,
                            old,
                            new,
                        } => {
                            // An UPDATE that moved the row's identity is two changes downstream:
                            // the row under the old key is gone, and a row under the new one
                            // exists. The delete goes first — they name different keys, so the
                            // shared LSN cannot make them race — and without it the old key
                            // stays in the target forever.
                            if let Some(was) =
                                self.moved_key(relation, key.as_deref(), old.as_deref(), &new)
                            {
                                self.push_row(&mut open_txn, relation, Op::Delete, wal_start, was);
                            }
                            self.push_change(&mut open_txn, relation, Op::Update, wal_start, &new)
                        }
                        LogicalMessage::Delete { relation, key, old } => {
                            // The old image is whatever REPLICA IDENTITY made available: the key
                            // alone by default, the whole row under FULL.
                            let image = old.or(key).unwrap_or_default();
                            self.push_change(&mut open_txn, relation, Op::Delete, wal_start, &image)
                        }
                        LogicalMessage::Truncate { relations, .. } => {
                            for relation in relations {
                                self.push_change(
                                    &mut open_txn,
                                    relation,
                                    Op::Truncate,
                                    wal_start,
                                    &[],
                                );
                            }
                        }
                        // A type announcement carries no rows, and an origin only matters to a
                        // bidirectional setup this connector does not build.
                        LogicalMessage::Type { .. } | LogicalMessage::Origin { .. } => {}
                    }
                }
            }
        }

        // Where the socket now sits. A batch that ended *inside* a transaction — the publisher
        // went quiet mid-stream — left the connection past the range's end with the rest of that
        // transaction still to come, and reading on from there would drop it: its `Begin` and
        // its earlier rows are already consumed. Forgetting the position forces the next read to
        // re-seek from the committed one, which replays the whole transaction.
        self.stream_at = if in_txn { None } else { Some(end.max(from)) };
        if committed.is_empty() && !in_txn {
            // No change in this stretch of WAL belongs to the publication, and the stream is
            // between transactions — so `observed` is a commit boundary and covering up to it
            // loses nothing. That is what stops a slot on a busy server from growing forever
            // while a quiet table has nothing to say. The `!in_txn` test is load bearing: an
            // *open* transaction whose changes were all filtered out also leaves `committed`
            // empty, and treating that as an empty stretch would carry the range's end into the
            // middle of a transaction the publisher has not finished sending.
            end = observed.max(from);
            self.stream_at = Some(end);
        }
        Ok((committed, end))
    }

    /// Record a relation's shape, and report the first time it stops matching the schema this
    /// source emits.
    fn remember(&mut self, relation: Relation) {
        let emitted: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .filter(|name| ![OP_COLUMN, LSN_COLUMN, TS_COLUMN].contains(name))
            .collect();
        let projection: Vec<Option<usize>> = relation
            .columns
            .iter()
            .map(|column| emitted.iter().position(|name| *name == column.name))
            .collect();

        let added: Vec<&str> = relation
            .columns
            .iter()
            .zip(&projection)
            .filter(|(_, mapped)| mapped.is_none())
            .map(|(column, _)| column.name.as_str())
            .collect();
        let dropped: Vec<&str> = emitted
            .iter()
            .filter(|name| !relation.columns.iter().any(|c| c.name == **name))
            .copied()
            .collect();
        if (!added.is_empty() || !dropped.is_empty()) && self.reported_drift.insert(relation.oid) {
            // Additive changes are carried, not fatal: a column this stream does not know is
            // dropped and one the publisher no longer sends is NULL-filled, so ingestion keeps
            // running. Picking the new column up means restarting the pipeline, which
            // re-introspects — see `docs/postgres-cdc.md` §10.
            self.log.event(
                "schema_change",
                json!({
                    "table": relation.qualified(),
                    "added_columns": added,
                    "removed_columns": dropped,
                    "action": "the stream continues on the schema it started with; restart the \
                               pipeline to pick the change up",
                }),
            );
            eprintln!(
                "[oxidant] postgres_cdc {}: `{}` changed shape (added: [{}], removed: [{}]); \
                 restart the pipeline to propagate it",
                self.options.name,
                relation.qualified(),
                added.join(", "),
                dropped.join(", ")
            );
        }
        self.projections.insert(relation.oid, projection);
        self.relations.insert(relation.oid, relation);
    }

    /// The emitted row a pgoutput tuple projects to, or `None` when the relation is not one this
    /// source replicates.
    fn project(&self, relation_oid: u32, tuple: &[TupleData]) -> Option<Vec<Option<String>>> {
        // A change for a relation whose shape was never announced cannot be decoded. pgoutput
        // always sends the Relation first, so this only happens if a stream is joined mid
        // transaction — which a replay from a commit boundary never is.
        let relation = self.relations.get(&relation_oid)?;
        if !self
            .tables
            .iter()
            .any(|t| t.qualified() == relation.qualified())
        {
            return None;
        }
        let projection = self
            .projections
            .get(&relation_oid)
            .cloned()
            .unwrap_or_default();
        let mut values = vec![None; self.emitted_columns()];
        for (index, datum) in tuple.iter().enumerate() {
            let Some(Some(target)) = projection.get(index) else {
                continue;
            };
            values[*target] = match datum {
                TupleData::Text(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                // A binary datum only appears when a subscriber asked for binary output; this
                // one never does, so re-encoding it in bytea's text form keeps one decode path.
                TupleData::Binary(bytes) => Some(format!("\\x{}", hex::encode(bytes))),
                // Null and unchanged-TOAST are both emitted as NULL. They are not the same
                // thing — see `docs/postgres-cdc.md` §10 for the AUTO CDC setting that keeps an
                // unchanged TOAST column from overwriting the target's value.
                TupleData::Null | TupleData::UnchangedToast => None,
            };
        }
        Some(values)
    }

    /// Turn one pgoutput tuple into a change event, dropping it when the op is not published or
    /// the relation is not one of ours.
    fn push_change(
        &self,
        into: &mut Vec<ChangeEvent>,
        relation_oid: u32,
        op: Op,
        lsn: Lsn,
        tuple: &[TupleData],
    ) {
        if !self.options.publish_ops.contains(&op) {
            return;
        }
        self.push_row(into, relation_oid, op, lsn, tuple);
    }

    /// Emit a row whatever `publish_ops` says.
    ///
    /// Only used for the delete half of an identity-changing UPDATE, which is not a source DELETE
    /// the operator chose to exclude — it is half of how an UPDATE that moved a row is
    /// represented, and dropping it would leave the orphan behind.
    fn push_row(
        &self,
        into: &mut Vec<ChangeEvent>,
        relation_oid: u32,
        op: Op,
        lsn: Lsn,
        tuple: &[TupleData],
    ) {
        let Some(values) = self.project(relation_oid, tuple) else {
            return;
        };
        into.push(ChangeEvent {
            op,
            lsn: lsn.as_i64(),
            ts_micros: None,
            values,
        });
    }

    /// The old image of a row whose identity an UPDATE moved, if it moved one.
    ///
    /// AUTO CDC merges by key, so an UPDATE that changes the key inserts a row under the new key
    /// and leaves the old one in the target forever — a phantom that exists in the lakehouse and
    /// not in Postgres, with nothing to detect it. Debezium and PeerDB both answer this by
    /// emitting a delete of the old key alongside the new image, and so does this.
    ///
    /// pgoutput gives two different signals. Under REPLICA IDENTITY DEFAULT or USING INDEX the
    /// old image (`K`) is written *only* when a replica-identity column changed, so its presence
    /// is the answer. Under FULL there is no `K` at all — the whole old row arrives as `O` on
    /// every update — so the identity has to be compared column by column.
    fn moved_key<'a>(
        &self,
        relation_oid: u32,
        key: Option<&'a [TupleData]>,
        old: Option<&'a [TupleData]>,
        new: &[TupleData],
    ) -> Option<&'a [TupleData]> {
        if let Some(key) = key {
            return Some(key);
        }
        let old = old?;
        let positions = self.key_positions(relation_oid);
        if positions.is_empty() {
            return None;
        }
        let (was, now) = (
            self.project(relation_oid, old)?,
            self.project(relation_oid, new)?,
        );
        positions
            .iter()
            .any(|index| was.get(*index) != now.get(*index))
            .then_some(old)
    }

    /// Where the row identity's columns sit in the emitted schema.
    fn key_positions(&self, relation_oid: u32) -> Vec<usize> {
        let Some(relation) = self.relations.get(&relation_oid) else {
            return Vec::new();
        };
        let Some(table) = self
            .tables
            .iter()
            .find(|t| t.qualified() == relation.qualified())
        else {
            return Vec::new();
        };
        let emitted: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .take(self.emitted_columns())
            .map(|f| f.name().as_str())
            .collect();
        table
            .keys
            .iter()
            .filter_map(|key| {
                emitted
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(key))
            })
            .collect()
    }

    fn emitted_columns(&self) -> usize {
        self.schema.fields().len().saturating_sub(3)
    }

    /// Build the Arrow batches for a run of change events, chunked so one batch never holds an
    /// unbounded number of rows.
    fn to_batches(&self, events: &[ChangeEvent]) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for chunk in events.chunks(self.options.snapshot_batch_rows.max(1)) {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
            for (index, field) in self
                .schema
                .fields()
                .iter()
                .take(self.emitted_columns())
                .enumerate()
            {
                let values: Vec<Option<&str>> = chunk
                    .iter()
                    .map(|event| event.values.get(index).and_then(|v| v.as_deref()))
                    .collect();
                columns.push(build_array(field.data_type(), field.name(), &values)?);
            }
            let mut op = StringBuilder::with_capacity(chunk.len(), chunk.len());
            let mut lsn = Int64Builder::with_capacity(chunk.len());
            let mut ts = TimestampMicrosecondBuilder::with_capacity(chunk.len());
            for event in chunk {
                op.append_value(event.op.code());
                lsn.append_value(event.lsn);
                match event.ts_micros {
                    Some(micros) => ts.append_value(micros),
                    None => ts.append_null(),
                }
            }
            columns.push(Arc::new(op.finish()));
            columns.push(Arc::new(lsn.finish()));
            columns.push(Arc::new(ts.finish()));
            batches.push(
                RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
                    Error::Execution(format!("postgres_cdc: build record batch: {e}"))
                })?,
            );
        }
        Ok(batches)
    }

    /// Read one source table inside the slot's snapshot transaction.
    ///
    /// `planned_at` is the consistent point the range was planned against, which is what makes
    /// the read honest across a restart: the scheduler replays a *recorded* range without
    /// planning again, so without it "source table 2" would be read against whatever slot happens
    /// to be open now. See [`Self::restart_snapshot`].
    async fn poll_snapshot(
        &mut self,
        table_index: usize,
        planned_at: Option<i64>,
    ) -> Result<Vec<RecordBatch>> {
        let Some(table) = self.tables.get(table_index).cloned() else {
            return Err(Error::Plan(format!(
                "postgres_cdc: this batch was recorded against source table {table_index}, and \
                 the source now has {} — the `tables:` option changed under a live checkpoint",
                self.tables.len()
            )));
        };
        // The range names a slot this source is no longer reading. `ensure_open` has already
        // dropped and recreated the slot, so honouring the index as written would emit this table
        // as of the *new* consistent point and then declare the snapshot complete — splicing the
        // tables already loaded at the old point onto a stream that starts after the new one, and
        // losing every change in between. The pass restarts from the first table instead.
        let honourable = planned_at == Some(self.position.as_i64());
        if !honourable {
            self.restart_snapshot(&table, planned_at);
        }
        if !self.snapshot_open {
            return Err(Error::Execution(format!(
                "postgres_cdc: the snapshot of `{}` was planned, but the slot's snapshot \
                 transaction is no longer open. This batch is already durable — the transaction \
                 is committed once the checkpoint holds it — so it should never be replayed; if \
                 the pipeline is stuck here, delete this table's checkpoint directory to \
                 re-snapshot.",
                table.qualified()
            )));
        }
        let started = Instant::now();
        let rows = match self.wire.snapshot_rows(&table).await {
            Ok(rows) => rows,
            Err(e) => {
                // The snapshot transaction died with the connection. Re-opening the slot is the
                // only way back to a consistent point, and that is what `ensure_open` does — so
                // forget that this run ever opened one.
                self.opened = false;
                self.snapshot_open = false;
                self.stream_at = None;
                return Err(e);
            }
        };
        let events: Vec<ChangeEvent> = rows
            .into_iter()
            .map(|values| ChangeEvent {
                op: Op::Snapshot,
                // Every snapshot row is as of the slot's consistent point, so ordering them
                // against each other is meaningless and ordering them *before* the stream is
                // exactly right. One byte *below* the consistent point, not on it: the stream
                // starts at that LSN, and on a quiet server the very next WAL record begins
                // exactly there — `CREATE_REPLICATION_SLOT` leaves the consistent point at the
                // end of the last record written. AUTO CDC compares an incoming change against
                // the stored sequence with a strict `>`, so a snapshot row stamped *on* the
                // consistent point ties with the first change after it and silently wins. This
                // is what makes "a change to the same key arrives with a strictly larger
                // `__oxidant_lsn`" true rather than nearly true.
                lsn: self.position.as_i64().saturating_sub(1),
                // A snapshot row has no commit, so it has no commit timestamp. AUTO CDC orders
                // by `__oxidant_lsn`, not by this.
                ts_micros: None,
                values,
            })
            .collect();
        let batches = self.to_batches(&events)?;

        // Only a range planned against *this* slot counts toward the pass. A range from the slot
        // that died is still answered — the batch has to carry something the sink can commit
        // under its id, and re-reading a table is an upsert either way — but the pass itself
        // begins again at table 0, so no table is left behind at an older consistent point.
        if honourable {
            self.snapshot_done = table_index + 1;
        }
        self.log.event(
            "snapshot_done",
            json!({
                "table": table.qualified(),
                "rows": events.len(),
                "duration_ms": started.elapsed().as_millis() as u64,
                "consistent_point": self.position.to_string(),
                "counted": honourable,
            }),
        );
        Ok(batches)
    }

    /// Report that a recorded snapshot range cannot be honoured, and reset the pass.
    ///
    /// Loud on purpose: the alternative outcome — the one this replaces — was a pipeline that
    /// reported a healthy, complete snapshot while silently dropping every change written between
    /// the two consistent points.
    fn restart_snapshot(&mut self, table: &TableSchema, planned_at: Option<i64>) {
        self.snapshot_done = 0;
        let planned_at = planned_at
            .map(|lsn| Lsn::from_i64(lsn).to_string())
            .unwrap_or_else(|| "unrecorded".to_string());
        let message = format!(
            "the snapshot of `{}` was planned against consistent point {planned_at}, and the \
             slot now sits at {}. An interrupted snapshot cannot be resumed — the transaction \
             that held it ended with the process — so re-snapshot required: every source table \
             is read again from the first, against the new point.",
            table.qualified(),
            self.position
        );
        self.log.event(
            "snapshot_start",
            json!({
                "reason": message,
                "planned_at": planned_at,
                "consistent_point": self.position.to_string(),
            }),
        );
        eprintln!("[oxidant] postgres_cdc {}: {message}", self.options.name);
    }

    /// Commit the slot's snapshot transaction once every table has been read *and* the batch
    /// that read the last one is durable.
    ///
    /// Deliberately not at the end of the last read: `poll_range` is required to be
    /// deterministic, and a transaction closed at read time turns the ordinary retry after a
    /// failed sink write — an object-store 503, a full disk — into a permanently wedged query,
    /// because the recorded range can never be read a second time. Holding it open until
    /// `mark_durable` costs the publisher one pinned xmin for the length of one batch.
    async fn finish_snapshot(&mut self) -> Result<()> {
        if !self.snapshot_open || self.tables.is_empty() || self.snapshot_done < self.tables.len() {
            return Ok(());
        }
        // Holding it any longer would pin the publisher's oldest xmin for the life of the
        // pipeline.
        self.wire.end_snapshot().await?;
        self.snapshot_open = false;
        Ok(())
    }
}

/// A byte count an operator can read at a glance.
fn bytes(count: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = count as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{count} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[async_trait::async_trait]
impl Source for PostgresCdcSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn description(&self) -> String {
        format!(
            "PostgresCDC[{}@{}:{}/{} slot={}]",
            self.tables
                .iter()
                .map(TableSchema::qualified)
                .collect::<Vec<_>>()
                .join(", "),
            self.options.connect.host,
            self.options.connect.port,
            self.options.connect.database,
            self.options.slot
        )
    }

    async fn plan_batch(&mut self, _engine: &Engine) -> Result<BatchRange> {
        self.ensure_open().await?;
        let metrics = self.guard_slot().await?;

        // The snapshot comes first and one table at a time: each is a batch the sink commits on
        // its own, so a large table does not have to land in one transaction with the next.
        if self.snapshot_done < self.tables.len() {
            return Ok(BatchRange {
                source: SOURCE_NAME.into(),
                start: [
                    (SNAPSHOT_KEY.to_string(), self.snapshot_done as i64),
                    (CONSISTENT_POINT_KEY.to_string(), self.position.as_i64()),
                ]
                .into(),
                end: [(SNAPSHOT_KEY.to_string(), self.snapshot_done as i64 + 1)].into(),
                items: vec![],
            });
        }

        let from = self.position;
        let (events, end) = self
            .read_forward(from, metrics.server_flush, self.options.max_batch_bytes)
            .await?;
        if events.is_empty() {
            // Nothing for this publication in the WAL just read, and `read_forward` only reports
            // such a stretch when the stream is between transactions — so the position moves
            // over it, since there is nothing in it to lose. Planning does *not* confirm it: the
            // standby status update is sent by `mark_durable` and by the keepalive below, and
            // nowhere else. Confirming from a read path is how a slot ends up ahead of the
            // checkpoint.
            if end > self.position {
                self.position = end;
            }
            self.heartbeat().await?;
            return Ok(BatchRange::default());
        }
        let range = BatchRange {
            source: SOURCE_NAME.into(),
            start: [(LSN_KEY.to_string(), from.as_i64())].into(),
            end: [(LSN_KEY.to_string(), end.as_i64())].into(),
            items: vec![],
        };
        // Held for the poll that follows. Nothing durable moved to decode them, so a plan that is
        // never polled is simply re-planned — from `self.position`, which has not changed.
        self.planned = Some((range.clone(), events));
        Ok(range)
    }

    async fn poll_range(
        &mut self,
        _engine: &Engine,
        range: &BatchRange,
    ) -> Result<Vec<RecordBatch>> {
        if range.is_empty() {
            return Ok(vec![]);
        }
        if range.source != SOURCE_NAME {
            return Err(Error::Plan(format!(
                "postgres_cdc: batch range was planned by `{}`, not by this source",
                range.source
            )));
        }
        self.ensure_open().await?;

        if let Some(index) = range.start.get(SNAPSHOT_KEY) {
            let planned_at = range.start.get(CONSISTENT_POINT_KEY).copied();
            return self
                .poll_snapshot((*index).max(0) as usize, planned_at)
                .await;
        }

        let from = Lsn::from_i64(range.start.get(LSN_KEY).copied().unwrap_or(0));
        let end = Lsn::from_i64(range.end.get(LSN_KEY).copied().unwrap_or(0));
        let started = Instant::now();
        let events = match self.planned.take() {
            // The ordinary path: this is the range planning just decoded.
            Some((planned, events)) if planned == *range => events,
            // A replay. The slot still holds everything from `from` — nothing confirmed past it
            // — so re-reading the same range reproduces the same events.
            _ => {
                self.log.event(
                    "batch",
                    json!({
                        "replay": true,
                        "start_lsn": from.to_string(),
                        "end_lsn": end.to_string(),
                    }),
                );
                self.ensure_stream(from).await?;
                let (events, reached) = self.read_forward(from, end, usize::MAX).await?;
                if reached < end {
                    return Err(Error::Execution(format!(
                        "postgres_cdc: replaying batch range [{from}, {end}) reached only \
                         {reached} — the publisher no longer holds the WAL this batch covered"
                    )));
                }
                events
                    .into_iter()
                    .filter(|e| e.lsn < end.as_i64())
                    .collect()
            }
        };
        let batches = self.to_batches(&events)?;
        self.position = end;
        self.log.event(
            "batch",
            json!({
                "start_lsn": from.to_string(),
                "end_lsn": end.to_string(),
                "rows": events.len(),
                "duration_ms": started.elapsed().as_millis() as u64,
            }),
        );
        Ok(batches)
    }

    fn committed_offsets(&self) -> Option<SourceOffsets> {
        Some(SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [
                (SNAPSHOT_KEY.to_string(), self.snapshot_done as i64),
                (LSN_KEY.to_string(), self.position.as_i64()),
            ]
            .into(),
        })
    }

    fn restore_offsets(&mut self, offsets: &SourceOffsets) {
        if offsets.source != SOURCE_NAME {
            return;
        }
        self.snapshot_done = offsets
            .entries
            .get(SNAPSHOT_KEY)
            .copied()
            .unwrap_or(0)
            .max(0) as usize;
        self.position = Lsn::from_i64(offsets.entries.get(LSN_KEY).copied().unwrap_or(0));
    }

    async fn available_end(&mut self, _engine: &Engine) -> Result<Option<BTreeMap<String, i64>>> {
        self.ensure_open().await?;
        let metrics = self.wire.slot_metrics().await?;
        Ok(Some(
            [
                (SNAPSHOT_KEY.to_string(), self.tables.len() as i64),
                (LSN_KEY.to_string(), metrics.server_flush.as_i64()),
            ]
            .into(),
        ))
    }

    /// Confirm the batch to Postgres — the one call that lets it recycle WAL.
    ///
    /// Deliberately here rather than after the sink write: the checkpoint is what a restart
    /// resumes from, so WAL confirmed before the checkpoint is durable would be gone by the time
    /// anything asked for it again.
    async fn mark_durable(&mut self, _engine: &Engine) -> Result<()> {
        if !self.opened {
            return Ok(());
        }
        // The snapshot batch that read the last table is durable now, so the range it covered
        // will never be replayed and the transaction that made it re-readable can go.
        self.finish_snapshot().await?;
        let sent = self.confirm_position().await?;
        self.log.event(
            "commit",
            json!({
                "confirmed_flush_lsn": self.position.to_string(),
                "snapshot_tables_done": self.snapshot_done,
                // False while the snapshot is still running — there is no replication session to
                // send a standby status update on yet — and after a connection failure. Recorded
                // so the connector log never shows a `confirmed_flush_lsn` the server was never
                // told about, which would look like an unexplained disagreement with
                // `pg_replication_slots` to anyone diagnosing slot growth.
                "sent": sent,
            }),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Setup validation and introspection
// ---------------------------------------------------------------------------------------------

/// Run the setup checks and introspection on a runtime of their own.
///
/// A thread rather than `block_on` on the caller's runtime, which would panic: `build_source` is
/// called from inside the pipeline's async startup.
fn bootstrap_blocking(options: &PostgresCdcOptions) -> Result<Vec<TableSchema>> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Error::Io(format!("postgres_cdc: start a setup runtime: {e}")))?;
                runtime.block_on(validate_and_introspect(options))
            })
            .join()
            .map_err(|_| Error::Io("postgres_cdc: the setup thread panicked".into()))?
    })
}

/// Everything `docs/postgres-cdc.md` §5 requires, each failure carrying its own fix.
async fn validate_and_introspect(options: &PostgresCdcOptions) -> Result<Vec<TableSchema>> {
    let control = options.connect.connect_control().await?;

    let version: i32 = control
        .scalar("SELECT current_setting('server_version_num')", &[])
        .await?
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if version < 120000 {
        return Err(Error::Unsupported(format!(
            "postgres_cdc: the server reports version {version}; logical replication with the \
             `pgoutput` plugin needs PostgreSQL 12 or newer"
        )));
    }

    let wal_level = control
        .scalar("SELECT current_setting('wal_level')", &[])
        .await?
        .unwrap_or_default();
    if wal_level != "logical" {
        return Err(Error::Execution(format!(
            "postgres_cdc: `wal_level` is `{wal_level}`, and logical replication needs `logical`. \
             Fix it with:\n  ALTER SYSTEM SET wal_level = logical;\nand restart the server. On \
             RDS/Aurora set the parameter-group value `rds.logical_replication = 1` and reboot \
             the instance."
        )));
    }

    let may_replicate = control
        .scalar(
            "SELECT (r.rolsuper OR r.rolreplication OR EXISTS ( \
                 SELECT 1 FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid \
                 WHERE m.member = r.oid AND g.rolname = 'rds_replication'))::text \
             FROM pg_roles r WHERE r.rolname = current_user",
            &[],
        )
        .await?
        .unwrap_or_default();
    if may_replicate != "true" {
        return Err(Error::Execution(format!(
            "postgres_cdc: `{}` cannot create a replication slot. Fix it with:\n  ALTER ROLE {} \
             WITH REPLICATION;\nOn RDS/Aurora instead run:\n  GRANT rds_replication TO {};",
            options.connect.user,
            quote_identifier(&options.connect.user),
            quote_identifier(&options.connect.user),
        )));
    }

    let tables = resolve_tables(&control, options).await?;
    ensure_publication(&control, options, &tables).await?;
    Ok(tables)
}

/// Expand `schema.*` entries, then introspect every table's columns, key and replica identity.
async fn resolve_tables(
    control: &ControlConnection,
    options: &PostgresCdcOptions,
) -> Result<Vec<TableSchema>> {
    let mut names: Vec<String> = Vec::new();
    for entry in &options.tables {
        let (schema, table) = entry.split_once('.').expect("qualified at parse time");
        if table != "*" {
            names.push(entry.clone());
            continue;
        }
        let rows = control
            .query(
                "SELECT c.relname::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname::text = $1 AND c.relkind IN ('r', 'p') \
                 ORDER BY c.relname",
                &[schema],
            )
            .await?;
        if rows.is_empty() {
            return Err(Error::Plan(format!(
                "postgres_cdc: `tables: {entry}` matches no table in schema `{schema}`"
            )));
        }
        for row in rows {
            if let Some(Some(name)) = row.first() {
                names.push(format!("{schema}.{name}"));
            }
        }
    }

    let mut tables = Vec::with_capacity(names.len());
    for name in &names {
        tables.push(introspect(control, options, name).await?);
    }

    // Multiple source tables share one stream only if they share one shape; otherwise the
    // emitted schema would fit the first table and silently mangle the rest.
    if let Some((first, rest)) = tables.split_first() {
        for other in rest {
            let shape = |t: &TableSchema| -> Vec<(String, DataType)> {
                t.columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect()
            };
            if shape(first) != shape(other) {
                return Err(Error::Plan(format!(
                    "postgres_cdc: `{}` and `{}` are in one source but do not have the same \
                     columns, so they cannot share one change stream. Declare one \
                     `postgres_cdc` source per table.",
                    first.qualified(),
                    other.qualified()
                )));
            }
        }
    }
    Ok(tables)
}

async fn introspect(
    control: &ControlConnection,
    options: &PostgresCdcOptions,
    qualified: &str,
) -> Result<TableSchema> {
    let (schema, table) = qualified.split_once('.').expect("qualified at parse time");

    let identity = control
        .scalar(
            "SELECT c.relreplident::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname::text = $1 AND c.relname::text = $2 AND c.relkind IN ('r', 'p')",
            &[schema, table],
        )
        .await?
        .ok_or_else(|| {
            Error::Plan(format!(
                "postgres_cdc: table `{qualified}` does not exist, or `{}` cannot see it",
                options.connect.user
            ))
        })?;
    let replica_identity = identity.chars().next().unwrap_or('d');

    let (quoted_schema, quoted_table) = (quote_identifier(schema), quote_identifier(table));
    let readable = control
        .scalar(
            "SELECT has_table_privilege(current_user, ($1 || '.' || $2)::regclass, 'SELECT')::text",
            &[quoted_schema.as_str(), quoted_table.as_str()],
        )
        .await?
        .unwrap_or_default();
    if readable != "true" {
        return Err(Error::Execution(format!(
            "postgres_cdc: `{}` cannot SELECT from `{qualified}`, which the initial snapshot \
             needs. Fix it with:\n  GRANT SELECT ON {}.{} TO {};",
            options.connect.user,
            quote_identifier(schema),
            quote_identifier(table),
            quote_identifier(&options.connect.user)
        )));
    }

    let pk: Vec<String> = control
        .query(
            "SELECT a.attname::text FROM pg_index i \
             JOIN pg_class c ON c.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE n.nspname::text = $1 AND c.relname::text = $2 AND i.indisprimary \
             ORDER BY a.attnum",
            &[schema, table],
        )
        .await?
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect();

    // Updates and deletes are only identifiable when the publisher writes an old image, and it
    // only does that when the table has a primary key or an explicit replica identity.
    if (pk.is_empty() && replica_identity == 'd') || replica_identity == 'n' {
        return Err(Error::Execution(format!(
            "postgres_cdc: `{qualified}` has no primary key and no REPLICA IDENTITY, so Postgres \
             writes no old row image and an UPDATE or DELETE could not be matched to a row. Fix \
             it with either:\n  ALTER TABLE {}.{} ADD PRIMARY KEY (…);\nor, if the table has a \
             unique index or none at all:\n  ALTER TABLE {}.{} REPLICA IDENTITY FULL;",
            quote_identifier(schema),
            quote_identifier(table),
            quote_identifier(schema),
            quote_identifier(table)
        )));
    }

    let rows = control
        .query(
            "SELECT a.attname::text, a.atttypid::text, a.atttypmod::text, \
                    (NOT a.attnotnull)::text, t.typname::text, t.typcategory::text \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE n.nspname::text = $1 AND c.relname::text = $2 AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[schema, table],
        )
        .await?;

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let text = |index: usize| row.get(index).and_then(|v| v.as_deref()).unwrap_or("");
        let name = text(0).to_string();
        if options.exclude_columns.contains(&name.to_ascii_lowercase()) {
            continue;
        }
        let type_oid: u32 = text(1).parse().unwrap_or(0);
        let type_modifier: i32 = text(2).parse().unwrap_or(-1);
        let (data_type, warning) = arrow_type_for(
            type_oid,
            type_modifier,
            text(4),
            text(5).chars().next().unwrap_or('X'),
        );
        columns.push(ColumnSchema {
            name,
            type_oid,
            type_modifier,
            data_type,
            nullable: text(3) == "true",
            warning,
        });
    }
    if columns.is_empty() {
        return Err(Error::Plan(format!(
            "postgres_cdc: every column of `{qualified}` is in `exclude_columns:`"
        )));
    }
    // The three metadata columns are appended to the emitted schema, so a source column of the
    // same name would produce two fields with one name — and AUTO CDC's `sequence_by:
    // __oxidant_lsn` would resolve to whichever came first.
    for column in &columns {
        if [OP_COLUMN, LSN_COLUMN, TS_COLUMN].contains(&column.name.as_str()) {
            return Err(Error::Plan(format!(
                "postgres_cdc: `{qualified}` has a column named `{}`, which is the name this \
                 source gives one of its own metadata columns. Keep it out with \
                 `exclude_columns: {}`, or rename it on the source.",
                column.name, column.name
            )));
        }
    }

    // `keys:` overrides the primary key as the row's identity, so it has to name columns that
    // are actually emitted — and that the publisher's old image will carry.
    let keys = if options.keys.is_empty() {
        pk.clone()
    } else {
        for key in &options.keys {
            if !columns.iter().any(|c| c.name.eq_ignore_ascii_case(key)) {
                return Err(Error::Plan(format!(
                    "postgres_cdc: `keys: {key}` is not an emitted column of `{qualified}` \
                     (emitted: {})",
                    columns
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            if replica_identity == 'd' && !pk.iter().any(|c| c.eq_ignore_ascii_case(key)) {
                return Err(Error::Execution(format!(
                    "postgres_cdc: `keys: {key}` is not part of the primary key of \
                     `{qualified}`, and under REPLICA IDENTITY DEFAULT a DELETE carries only the \
                     primary key — so a delete could never be matched on it. Fix it with:\n  \
                     ALTER TABLE {}.{} REPLICA IDENTITY FULL;",
                    quote_identifier(schema),
                    quote_identifier(table)
                )));
            }
        }
        options.keys.clone()
    };

    Ok(TableSchema {
        schema: schema.to_string(),
        table: table.to_string(),
        columns,
        keys,
        replica_identity,
    })
}

/// Make sure the publication exists and covers every table, creating it when it does not.
///
/// Never `FOR ALL TABLES`: that quietly enrols every table in the database, present and future,
/// in WAL retention — the same guidance ClickPipes gives.
async fn ensure_publication(
    control: &ControlConnection,
    options: &PostgresCdcOptions,
    tables: &[TableSchema],
) -> Result<()> {
    let publication = options.publication.clone();
    let exists = control
        .scalar(
            "SELECT puballtables::text FROM pg_publication WHERE pubname::text = $1",
            &[publication.as_str()],
        )
        .await?;

    let table_list = tables
        .iter()
        .map(|t| {
            format!(
                "{}.{}",
                quote_identifier(&t.schema),
                quote_identifier(&t.table)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let Some(all_tables) = exists else {
        // Whole-schema entries become `FOR TABLES IN SCHEMA`, which keeps a table added to the
        // schema later inside the publication without another DDL round.
        let whole_schemas: BTreeSet<&str> = options
            .tables
            .iter()
            .filter(|entry| entry.ends_with(".*"))
            .filter_map(|entry| entry.split_once('.').map(|(schema, _)| schema))
            .collect();
        let target = if whole_schemas.is_empty() {
            format!("FOR TABLE {table_list}")
        } else {
            format!(
                "FOR TABLES IN SCHEMA {}",
                whole_schemas
                    .iter()
                    .map(|s| quote_identifier(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let sql = format!(
            "CREATE PUBLICATION {} {target} WITH (publish = {})",
            quote_identifier(&publication),
            quote_literal(&options.publish_list())
        );
        return control.execute(&sql).await.map_err(|e| {
            Error::Execution(format!(
                "{e}\npostgres_cdc: the publication does not exist and could not be created. \
                 Create it as a superuser (or the tables' owner) with:\n  {sql};"
            ))
        });
    };

    if all_tables == "true" {
        // A `FOR ALL TABLES` publication the operator already made is theirs to keep; it covers
        // everything by definition, so there is nothing to add.
        return Ok(());
    }

    let covered: BTreeSet<String> = control
        .query(
            "SELECT (schemaname || '.' || tablename)::text FROM pg_publication_tables \
             WHERE pubname::text = $1",
            &[publication.as_str()],
        )
        .await?
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect();

    let missing: Vec<&TableSchema> = tables
        .iter()
        .filter(|t| !covered.contains(&t.qualified()))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let add = missing
        .iter()
        .map(|t| {
            format!(
                "{}.{}",
                quote_identifier(&t.schema),
                quote_identifier(&t.table)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "ALTER PUBLICATION {} ADD TABLE {add}",
        quote_identifier(&publication)
    );
    control.execute(&sql).await.map_err(|e| {
        Error::Execution(format!(
            "{e}\npostgres_cdc: publication `{publication}` does not cover {}. Add them as the \
             tables' owner with:\n  {sql};",
            missing
                .iter()
                .map(|t| t.qualified())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })
}

/// Warnings worth logging once at startup: they are not errors, and each one changes what a
/// delete row looks like or what a column holds.
fn schema_warnings(tables: &[TableSchema]) -> Vec<String> {
    let mut warnings = Vec::new();
    for table in tables {
        if table.replica_identity == 'd' || table.replica_identity == 'i' {
            warnings.push(format!(
                "`{}` has REPLICA IDENTITY {}, so a DELETE carries only its key columns and \
                 every other column of a delete row is NULL. `ALTER TABLE {}.{} REPLICA IDENTITY \
                 FULL;` if the merge needs the whole old row.",
                table.qualified(),
                if table.replica_identity == 'd' {
                    "DEFAULT"
                } else {
                    "USING INDEX"
                },
                quote_identifier(&table.schema),
                quote_identifier(&table.table)
            ));
        }
        for column in &table.columns {
            if let Some(warning) = &column.warning {
                warnings.push(format!(
                    "`{}`.`{}`: {warning}",
                    table.qualified(),
                    column.name
                ));
            }
        }
    }
    warnings
}

/// Options the pipeline runner injects so the source can find its log directory and name it.
///
/// Not part of the YAML surface, and not settable there: they are derived from
/// `pipeline.checkpoints` and the table's own name, so an author cannot point one connector's
/// log at another's file.
pub fn postgres_cdc_pipeline_options(
    options: &mut BTreeMap<String, String>,
    checkpoints: &Path,
    name: &str,
) {
    options.insert(
        LOG_DIR_OPTION.to_string(),
        checkpoints.join("logs").to_string_lossy().into_owned(),
    );
    options.insert(NAME_OPTION.to_string(), name.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };

    // -----------------------------------------------------------------------------------------
    // pgoutput frame builders — the wire the fake publisher speaks
    // -----------------------------------------------------------------------------------------

    fn cstr(out: &mut Vec<u8>, text: &str) {
        out.extend_from_slice(text.as_bytes());
        out.push(0);
    }

    fn tuple(out: &mut Vec<u8>, values: &[Option<&str>]) {
        out.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for value in values {
            match value {
                // `~` marks an unchanged TOAST column, which is not the same thing as NULL.
                Some("~") => out.push(b'u'),
                Some(text) => {
                    out.push(b't');
                    out.extend_from_slice(&(text.len() as i32).to_be_bytes());
                    out.extend_from_slice(text.as_bytes());
                }
                None => out.push(b'n'),
            }
        }
    }

    fn relation_msg(
        oid: u32,
        namespace: &str,
        name: &str,
        columns: &[(bool, &str, u32)],
    ) -> Vec<u8> {
        let mut out = vec![b'R'];
        out.extend_from_slice(&oid.to_be_bytes());
        cstr(&mut out, namespace);
        cstr(&mut out, name);
        out.push(b'd');
        out.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (key, name, type_oid) in columns {
            out.push(u8::from(*key));
            cstr(&mut out, name);
            out.extend_from_slice(&type_oid.to_be_bytes());
            out.extend_from_slice(&(-1i32).to_be_bytes());
        }
        out
    }

    fn begin_msg(final_lsn: u64, ts: i64) -> Vec<u8> {
        let mut out = vec![b'B'];
        out.extend_from_slice(&final_lsn.to_be_bytes());
        out.extend_from_slice(&ts.to_be_bytes());
        out.extend_from_slice(&7u32.to_be_bytes());
        out
    }

    fn commit_msg(commit_lsn: u64, end_lsn: u64, ts: i64) -> Vec<u8> {
        let mut out = vec![b'C', 0];
        out.extend_from_slice(&commit_lsn.to_be_bytes());
        out.extend_from_slice(&end_lsn.to_be_bytes());
        out.extend_from_slice(&ts.to_be_bytes());
        out
    }

    fn change_msg(tag: u8, oid: u32, section: u8, values: &[Option<&str>]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&oid.to_be_bytes());
        out.push(section);
        tuple(&mut out, values);
        out
    }

    /// An `U` message with both sections: the old identity (`K`) and the new row (`N`), which is
    /// exactly what pgoutput writes when an UPDATE changes a replica-identity column.
    fn keyed_update_msg(oid: u32, was: &[Option<&str>], now: &[Option<&str>]) -> Vec<u8> {
        let mut out = vec![b'U'];
        out.extend_from_slice(&oid.to_be_bytes());
        out.push(b'K');
        tuple(&mut out, was);
        out.push(b'N');
        tuple(&mut out, now);
        out
    }

    fn truncate_msg(oids: &[u32]) -> Vec<u8> {
        let mut out = vec![b'T'];
        out.extend_from_slice(&(oids.len() as i32).to_be_bytes());
        out.push(0);
        for oid in oids {
            out.extend_from_slice(&oid.to_be_bytes());
        }
        out
    }

    fn xlog(wal_start: u64, payload: Vec<u8>) -> WireMessage {
        WireMessage::XLogData {
            wal_start: Lsn(wal_start),
            wal_end: Lsn(wal_start + payload.len() as u64),
            clock: 0,
            data: payload,
        }
    }

    /// Postgres' own epoch, so `__oxidant_ts` lands on 2024-05-06T07:08:09Z.
    const COMMIT_TIME: i64 = 767_171_289_000_000;

    // -----------------------------------------------------------------------------------------
    // The fake publisher
    // -----------------------------------------------------------------------------------------

    #[derive(Default)]
    struct FakeWire {
        /// Every frame the slot holds, oldest first.
        stream: Vec<WireMessage>,
        /// How far into `stream` the open session has read.
        cursor: usize,
        snapshot: Vec<Vec<Option<String>>>,
        /// Per-table rows, for the multi-table snapshot tests. Falls back to `snapshot`.
        snapshots: HashMap<String, Vec<Vec<Option<String>>>>,
        /// Every call made, in order — this is what the sequencing tests assert on.
        calls: Vec<String>,
        consistent_point: Lsn,
        slot_existed: bool,
        retained_bytes: u64,
        server_flush: Lsn,
        confirmed: Option<Lsn>,
        /// Whether `START_REPLICATION` has been issued. A standby status update has nowhere to go
        /// until it has, exactly as on a real session.
        streaming: bool,
        /// Every wire call, including the ones `calls` leaves out.
        call_count: usize,
        /// Fail the Nth wire call (1-based) the way a dead socket does, then behave again.
        ///
        /// Without this the fake cannot reproduce the failure that matters most — a publisher
        /// restart, a failover, a `wal_sender_timeout` — and every recovery path stays untested.
        fail_nth_call: Option<usize>,
    }

    impl FakeWire {
        fn new(stream: Vec<WireMessage>) -> Self {
            Self {
                stream,
                consistent_point: Lsn(0x100),
                server_flush: Lsn(0xFFFF),
                ..Default::default()
            }
        }

        /// Count this call, and blow up on it if the test asked for that.
        fn tick(&mut self) -> Result<()> {
            self.call_count += 1;
            if self.fail_nth_call == Some(self.call_count) {
                return Err(Error::Io(
                    "postgres_cdc: the server closed the replication connection".into(),
                ));
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl CdcWire for FakeWire {
        async fn open_slot(&mut self, create: bool) -> Result<SlotOpen> {
            self.tick()?;
            self.calls.push(format!("open_slot(create={create})"));
            let existed = self.slot_existed;
            if create && existed {
                // Dropping and recreating a slot hands back a *later* consistent point, which is
                // the whole reason an interrupted snapshot cannot be resumed table by table.
                self.consistent_point = Lsn(self.consistent_point.0 + 0x1000);
                self.streaming = false;
            }
            if create {
                self.slot_existed = true;
            }
            Ok(SlotOpen {
                consistent_point: self.consistent_point,
                existed,
                snapshot_open: create,
            })
        }

        async fn snapshot_rows(&mut self, table: &TableSchema) -> Result<Vec<Vec<Option<String>>>> {
            self.tick()?;
            self.calls
                .push(format!("snapshot_rows({})", table.qualified()));
            Ok(self
                .snapshots
                .get(&table.qualified())
                .cloned()
                .unwrap_or_else(|| self.snapshot.clone()))
        }

        async fn end_snapshot(&mut self) -> Result<()> {
            self.tick()?;
            self.calls.push("end_snapshot".into());
            Ok(())
        }

        async fn start_stream(&mut self, start: Lsn) -> Result<()> {
            self.tick()?;
            self.calls.push(format!("start_stream({start})"));
            self.streaming = true;
            // A real publisher resumes from the oldest *unconfirmed* transaction, which can be
            // older than the LSN asked for. Rewinding to the beginning here is what makes the
            // source's "drop transactions that ended at or before the range start" filter load
            // bearing rather than decorative.
            self.cursor = 0;
            Ok(())
        }

        async fn next_wire(&mut self, _idle: Duration) -> Result<Option<WireMessage>> {
            self.tick()?;
            let next = self.stream.get(self.cursor).cloned();
            if next.is_some() {
                self.cursor += 1;
            }
            Ok(next)
        }

        async fn confirm(&mut self, _written: Lsn, flushed: Lsn) -> Result<bool> {
            self.tick()?;
            if !self.streaming {
                self.calls.push("confirm(not streaming)".into());
                return Ok(false);
            }
            self.calls.push(format!("confirm({flushed})"));
            self.confirmed = Some(flushed);
            Ok(true)
        }

        async fn slot_metrics(&mut self) -> Result<SlotMetrics> {
            self.tick()?;
            Ok(SlotMetrics {
                retained_bytes: self.retained_bytes,
                server_flush: self.server_flush,
                confirmed_flush: self.confirmed,
            })
        }
    }

    /// A wire whose call log the test can read back after the source has taken ownership.
    ///
    /// `tokio::sync::Mutex` rather than the standard one: `async_trait` boxes these futures as
    /// `Send`, and a `std` guard held across an await is not.
    struct Spy(std::sync::Arc<tokio::sync::Mutex<FakeWire>>);

    #[async_trait::async_trait]
    impl CdcWire for Spy {
        async fn open_slot(&mut self, create: bool) -> Result<SlotOpen> {
            self.0.lock().await.open_slot(create).await
        }
        async fn snapshot_rows(&mut self, table: &TableSchema) -> Result<Vec<Vec<Option<String>>>> {
            self.0.lock().await.snapshot_rows(table).await
        }
        async fn end_snapshot(&mut self) -> Result<()> {
            self.0.lock().await.end_snapshot().await
        }
        async fn start_stream(&mut self, start: Lsn) -> Result<()> {
            self.0.lock().await.start_stream(start).await
        }
        async fn next_wire(&mut self, idle: Duration) -> Result<Option<WireMessage>> {
            self.0.lock().await.next_wire(idle).await
        }
        async fn confirm(&mut self, written: Lsn, flushed: Lsn) -> Result<bool> {
            self.0.lock().await.confirm(written, flushed).await
        }
        async fn slot_metrics(&mut self) -> Result<SlotMetrics> {
            self.0.lock().await.slot_metrics().await
        }
    }

    // -----------------------------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------------------------

    const RELATION_OID: u32 = 16385;

    fn suppliers() -> TableSchema {
        table_named("sales_suppliers")
    }

    fn table_named(name: &str) -> TableSchema {
        TableSchema {
            schema: "public".into(),
            table: name.into(),
            columns: vec![
                ColumnSchema {
                    name: "supplierid".into(),
                    type_oid: oids::INT8,
                    type_modifier: -1,
                    data_type: DataType::Int64,
                    nullable: false,
                    warning: None,
                },
                ColumnSchema {
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    data_type: DataType::Utf8,
                    nullable: true,
                    warning: None,
                },
            ],
            keys: vec!["supplierid".into()],
            replica_identity: 'd',
        }
    }

    fn options() -> PostgresCdcOptions {
        PostgresCdcOptions {
            connect: PgConnectConfig {
                host: "db.internal".into(),
                port: 5432,
                database: "sales".into(),
                user: "oxidant_cdc".into(),
                password: None,
                tls: TlsMode::Disable,
                tls_ca: None,
            },
            publication: "oxidant_sales".into(),
            slot: "oxidant_sales_suppliers".into(),
            tables: vec!["public.sales_suppliers".into()],
            exclude_columns: BTreeSet::new(),
            keys: vec![],
            publish_ops: [Op::Insert, Op::Update, Op::Delete, Op::Truncate]
                .into_iter()
                .collect(),
            max_slot_bytes: DEFAULT_MAX_SLOT_BYTES,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            snapshot_batch_rows: DEFAULT_SNAPSHOT_BATCH_ROWS,
            idle: Duration::from_millis(1),
            // Long enough that no test sees a heartbeat it did not ask for; the tests that want
            // one set it to zero.
            status_interval: Duration::from_secs(3600),
            log_dir: None,
            name: "sales_suppliers".into(),
        }
    }

    /// One transaction inserting `Acme`, updating it with an unchanged TOAST column, and
    /// deleting it — every shape the change stream carries, over one relation.
    fn a_transaction() -> Vec<WireMessage> {
        vec![
            xlog(
                0x100,
                relation_msg(
                    RELATION_OID,
                    "public",
                    "sales_suppliers",
                    &[(true, "supplierid", oids::INT8), (false, "name", 25)],
                ),
            ),
            xlog(0x110, begin_msg(0x200, COMMIT_TIME)),
            xlog(
                0x120,
                change_msg(b'I', RELATION_OID, b'N', &[Some("7"), Some("Acme")]),
            ),
            xlog(
                0x130,
                change_msg(b'U', RELATION_OID, b'N', &[Some("7"), Some("~")]),
            ),
            xlog(
                0x140,
                change_msg(b'D', RELATION_OID, b'K', &[Some("7"), None]),
            ),
            xlog(0x150, commit_msg(0x200, 0x200, COMMIT_TIME)),
        ]
    }

    fn source_with(
        stream: Vec<WireMessage>,
    ) -> (
        PostgresCdcSource,
        std::sync::Arc<tokio::sync::Mutex<FakeWire>>,
    ) {
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(stream)));
        let source = PostgresCdcSource::with_wire(
            options(),
            vec![suppliers()],
            Box::new(Spy(wire.clone())),
            ConnectorLog::default(),
        );
        (source, wire)
    }

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a dyn Array {
        batch
            .column(batch.schema().index_of(name).expect("column exists"))
            .as_ref()
    }

    fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let array = column(batch, name)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i).to_string()))
            .collect()
    }

    // -----------------------------------------------------------------------------------------
    // Options
    // -----------------------------------------------------------------------------------------

    fn parse(pairs: &[(&str, &str)]) -> Result<PostgresCdcOptions> {
        PostgresCdcOptions::from_options(
            &pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn minimal() -> Vec<(&'static str, &'static str)> {
        vec![
            ("host", "db.internal"),
            ("database", "sales"),
            ("user", "oxidant_cdc"),
            ("publication", "oxidant_sales"),
            ("slot", "oxidant_sales_suppliers"),
            ("tables", "public.sales_suppliers"),
        ]
    }

    #[test]
    fn the_documented_option_surface_parses() {
        let mut pairs = minimal();
        pairs.extend([
            ("port", "5433"),
            ("tls", "verify-ca"),
            ("tls_ca", "/etc/oxidant/pg-ca.pem"),
            ("exclude_columns", "secret, Notes"),
            ("keys", "supplierID"),
            ("publish_ops", "insert,update,delete"),
            ("max_slot_bytes", "1024"),
            ("max_batch_bytes", "4096"),
        ]);
        let options = parse(&pairs).expect("parses");
        assert_eq!(options.connect.port, 5433);
        assert_eq!(options.connect.tls, TlsMode::VerifyCa);
        assert_eq!(
            options.connect.tls_ca.as_deref(),
            Some(Path::new("/etc/oxidant/pg-ca.pem"))
        );
        assert_eq!(options.tables, vec!["public.sales_suppliers".to_string()]);
        // Excluded columns are matched case-insensitively, because Postgres folds the names.
        assert!(options.exclude_columns.contains("notes"));
        assert_eq!(options.keys, vec!["supplierID".to_string()]);
        assert_eq!(options.publish_list(), "insert, update, delete");
        assert_eq!(options.max_slot_bytes, 1024);
        assert_eq!(options.max_batch_bytes, 4096);
    }

    #[test]
    fn a_table_with_no_schema_defaults_to_public_and_folds_case() {
        let mut pairs = minimal();
        pairs.retain(|(k, _)| *k != "tables");
        pairs.push(("tables", "Sales_Suppliers, Inventory.Stock"));
        let options = parse(&pairs).expect("parses");
        assert_eq!(
            options.tables,
            vec![
                "public.sales_suppliers".to_string(),
                "inventory.stock".to_string()
            ]
        );
    }

    #[test]
    fn an_unknown_option_is_an_error_rather_than_a_silent_default() {
        // `table:` for `tables:` would otherwise leave the source with nothing to replicate and
        // a perfectly healthy-looking empty stream.
        let mut pairs = minimal();
        pairs.push(("table", "public.sales_suppliers"));
        let err = parse(&pairs).unwrap_err().to_string();
        assert!(err.contains("`table` is not a source option"), "got: {err}");
        assert!(
            err.contains("tables"),
            "the error lists the real names: {err}"
        );
    }

    #[test]
    fn every_required_option_is_named_when_it_is_missing() {
        for missing in ["host", "database", "user", "publication", "slot", "tables"] {
            let pairs: Vec<_> = minimal()
                .into_iter()
                .filter(|(k, _)| *k != missing)
                .collect();
            let err = parse(&pairs).unwrap_err().to_string();
            assert!(err.contains(missing), "expected `{missing}` in: {err}");
        }
    }

    #[test]
    fn malformed_values_are_rejected_with_the_expected_spelling() {
        let check = |key: &'static str, value: &'static str, expect: &str| {
            let mut pairs = minimal();
            pairs.retain(|(k, _)| *k != key);
            pairs.push((key, value));
            let err = parse(&pairs).unwrap_err().to_string();
            assert!(err.contains(expect), "for `{key}: {value}` got: {err}");
        };
        check("port", "not-a-port", "is not a port number");
        check("tls", "prefer", "verify-full");
        check("publish_ops", "upsert", "expected any of insert");
        check("max_slot_bytes", "10GiB", "is not a number of bytes");
        check("tables", "a.b.c", "is not a `schema.table` name");
    }

    #[test]
    fn a_password_is_read_from_the_environment_and_never_from_the_file() {
        let mut pairs = minimal();
        pairs.push(("password_env", "OXIDANT_TEST_PG_PASSWORD_UNSET"));
        let err = parse(&pairs).unwrap_err().to_string();
        assert!(err.contains("is not set"), "got: {err}");

        // There is no `password:` option at all — a literal secret in an `oxidant.yaml` outlives
        // the pipeline that leaked it.
        let mut pairs = minimal();
        pairs.push(("password", "hunter2"));
        assert!(parse(&pairs)
            .unwrap_err()
            .to_string()
            .contains("`password` is not a source option"));
    }

    // -----------------------------------------------------------------------------------------
    // Type mapping and value decoding
    // -----------------------------------------------------------------------------------------

    #[test]
    fn the_type_map_matches_the_documented_table() {
        let map = |oid: u32| arrow_type_for(oid, -1, "", 'N').0;
        assert_eq!(map(oids::BOOL), DataType::Boolean);
        assert_eq!(map(oids::INT2), DataType::Int32);
        assert_eq!(map(oids::INT4), DataType::Int32);
        assert_eq!(map(oids::INT8), DataType::Int64);
        assert_eq!(map(oids::OID), DataType::Int64);
        assert_eq!(map(oids::XID8), DataType::Int64);
        assert_eq!(map(oids::FLOAT4), DataType::Float32);
        assert_eq!(map(oids::FLOAT8), DataType::Float64);
        assert_eq!(map(oids::BYTEA), DataType::Binary);
        assert_eq!(map(oids::DATE), DataType::Date32);
        assert_eq!(map(oids::TIME), DataType::Time64(TimeUnit::Microsecond));
        assert_eq!(
            map(oids::TIMESTAMP),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            map(oids::TIMESTAMPTZ),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        // text / varchar / name / char / bpchar / uuid / json / jsonb / xml / inet / cidr /
        // macaddr / interval / an enum / an unknown extension type: all Utf8.
        for oid in [
            25, 1043, 19, 18, 1042, 2950, 114, 3802, 142, 869, 650, 829, 1186, 987654,
        ] {
            assert_eq!(map(oid), DataType::Utf8, "oid {oid}");
        }
        // An array is its text form, with a warning.
        let (array_type, warning) = arrow_type_for(1007, -1, "_int4", 'A');
        assert_eq!(array_type, DataType::Utf8);
        assert!(warning.unwrap().contains("text form"));
    }

    #[test]
    fn numeric_maps_to_a_decimal_only_when_the_source_declared_a_scale() {
        // `numeric(12,4)`: atttypmod is ((12 << 16) | 4) + 4.
        let modifier = ((12 << 16) | 4) + 4;
        assert_eq!(
            arrow_type_for(oids::NUMERIC, modifier, "numeric", 'N').0,
            DataType::Decimal128(38, 4)
        );
        // Unconstrained `numeric` holds values no fixed-point type can, so it stays text rather
        // than being silently rounded.
        let (unconstrained, warning) = arrow_type_for(oids::NUMERIC, -1, "numeric", 'N');
        assert_eq!(unconstrained, DataType::Utf8);
        assert!(warning.unwrap().contains("no scale"));
        // Wider than Decimal128 can hold.
        assert_eq!(
            arrow_type_for(oids::NUMERIC, ((60 << 16) | 2) + 4, "numeric", 'N').0,
            DataType::Utf8
        );
    }

    #[test]
    fn values_decode_from_their_postgres_text_form() {
        let one = |data_type: DataType, text: &str| {
            build_array(&data_type, "c", &[Some(text), None]).expect("decodes")
        };

        let b = one(DataType::Boolean, "t");
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(b.value(0) && b.is_null(1));

        let i = one(DataType::Int32, "-42");
        assert_eq!(
            i.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            -42
        );
        let i = one(DataType::Int64, "9223372036854775807");
        assert_eq!(
            i.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            i64::MAX
        );

        let bytes = one(DataType::Binary, "\\xdeadbeef");
        assert_eq!(
            bytes
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[0xDE, 0xAD, 0xBE, 0xEF]
        );

        let d = one(DataType::Date32, "1970-01-02");
        assert_eq!(
            d.as_any().downcast_ref::<Date32Array>().unwrap().value(0),
            1
        );

        let ts = one(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "2024-05-06 07:08:09.123456",
        );
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            ts.value(0),
            chrono::DateTime::parse_from_rfc3339("2024-05-06T07:08:09.123456Z")
                .unwrap()
                .timestamp_micros()
        );

        // A `timestamptz` prints with a `+00` offset because both sessions pin TIME ZONE UTC.
        let tz = build_array(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "c",
            &[Some("2024-05-06 07:08:09+00")],
        )
        .expect("decodes");
        assert_eq!(
            tz.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );

        let time = one(DataType::Time64(TimeUnit::Microsecond), "01:02:03.5");
        assert_eq!(
            time.as_any()
                .downcast_ref::<oxidant_loom::arrow::array::Time64MicrosecondArray>()
                .unwrap()
                .value(0),
            3_723_500_000
        );
    }

    #[test]
    fn a_decimal_keeps_every_digit_the_source_declared() {
        // The reason this is not a detour through f64: these two survive, and would not.
        assert_eq!(parse_decimal("123.45", 2), Some(12345));
        assert_eq!(parse_decimal("-0.0001", 4), Some(-1));
        assert_eq!(parse_decimal("7", 3), Some(7000));
        assert_eq!(parse_decimal("+7.5", 3), Some(7500));
        assert_eq!(
            parse_decimal("99999999999999999999.9999999999", 10),
            Some(999_999_999_999_999_999_999_999_999_999i128)
        );
        assert_eq!(parse_decimal("", 2), None);
        assert_eq!(parse_decimal("1e5", 2), None);

        // `numeric` has a NaN and no decimal type does, so it becomes NULL rather than an error
        // that would stop the pipeline on one row.
        let array = build_array(&DataType::Decimal128(38, 2), "c", &[Some("NaN")]).unwrap();
        assert!(array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn a_value_the_mapping_cannot_represent_names_the_column_and_the_fix() {
        let err = build_array(&DataType::Date32, "hired_on", &[Some("infinity")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("hired_on"), "got: {err}");
        assert!(err.contains("exclude_columns"), "got: {err}");
    }

    #[test]
    fn the_emitted_schema_is_the_source_columns_plus_three() {
        let schema = emitted_schema(&suppliers().columns);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["supplierid", "name", OP_COLUMN, LSN_COLUMN, TS_COLUMN]
        );
        // Every source column is nullable however the source declared it: a delete under
        // REPLICA IDENTITY DEFAULT carries only the key.
        assert!(schema.field(0).is_nullable());
        assert!(!schema.field(2).is_nullable(), "an op is always present");
        assert!(!schema.field(3).is_nullable(), "an LSN is always present");
        assert!(
            schema.field(4).is_nullable(),
            "a snapshot row has no commit"
        );
    }

    // -----------------------------------------------------------------------------------------
    // Snapshot ⇄ stream sequencing
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn the_snapshot_is_read_in_the_slot_transaction_and_the_stream_starts_after_it() {
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.snapshot = vec![
            vec![Some("1".into()), Some("Acme".into())],
            vec![Some("2".into()), None],
        ];

        // Batch 1: the snapshot of the single source table.
        let range = source.plan_batch(&engine).await.unwrap();
        assert_eq!(range.start[SNAPSHOT_KEY], 0);
        assert_eq!(range.end[SNAPSHOT_KEY], 1);
        let batches = source.poll_range(&engine, &range).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(
            strings(&batches[0], OP_COLUMN),
            vec![Some("s".into()), Some("s".into())],
            "snapshot rows are upserts, so AUTO CDC merges them by key"
        );
        // Every snapshot row is as of the consistent point, so a later change to the same key
        // has a strictly larger `__oxidant_lsn` and wins.
        let lsn = column(&batches[0], LSN_COLUMN)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            lsn, 0xFF,
            "one below the consistent point, so the first change *at* it still wins"
        );
        assert!(
            column(&batches[0], TS_COLUMN).is_null(0),
            "no commit, no commit time"
        );

        // Batch 2: the WAL after it.
        let range = source.plan_batch(&engine).await.unwrap();
        assert_eq!(range.start[LSN_KEY], 0x100);
        assert_eq!(range.end[LSN_KEY], 0x200);

        let calls = wire.lock().await.calls.clone();
        assert_eq!(
            calls,
            vec![
                "open_slot(create=true)",
                "snapshot_rows(public.sales_suppliers)",
                // The transaction is committed as soon as the last table is read, rather than
                // held for the life of the pipeline pinning the publisher's oldest xmin.
                "end_snapshot",
                "start_stream(0/100)",
            ]
        );
    }

    #[tokio::test]
    async fn a_restored_snapshot_is_not_read_again_and_the_slot_is_not_recreated() {
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.slot_existed = true;
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });

        let range = source.plan_batch(&engine).await.unwrap();
        assert_eq!(range.start[LSN_KEY], 0x100, "straight into the stream");
        let calls = wire.lock().await.calls.clone();
        assert_eq!(
            calls,
            vec!["open_slot(create=false)", "start_stream(0/100)"]
        );
    }

    #[tokio::test]
    async fn losing_the_slot_after_a_committed_batch_stops_rather_than_skips() {
        // WAL written since the last commit may already be recycled, so resuming would drop
        // changes and say nothing about it.
        let engine = Engine::new();
        let (mut source, _wire) = source_with(vec![]);
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });
        let err = source.plan_batch(&engine).await.unwrap_err().to_string();
        assert!(err.contains("no longer exists"), "got: {err}");
        assert!(err.contains("checkpoint directory"), "got: {err}");
    }

    #[tokio::test]
    async fn a_snapshot_range_can_be_read_again_after_the_sink_refuses_it() {
        // `poll_range` is required to be deterministic, and a snapshot batch is the first batch
        // of every new pipeline — so the first transient sink error (an object-store 503, a full
        // disk, a failed expectation) lands here. Closing the slot's snapshot transaction at read
        // time made the recorded range unreadable ever after, and every later trigger failed
        // identically until someone restarted the process.
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.snapshot = vec![
            vec![Some("1".into()), Some("Acme".into())],
            vec![Some("2".into()), None],
        ];

        let range = source.plan_batch(&engine).await.unwrap();
        let first = source.poll_range(&engine, &range).await.unwrap();
        assert_eq!(first[0].num_rows(), 2);
        assert!(
            !wire.lock().await.calls.iter().any(|c| c == "end_snapshot"),
            "the transaction stays open until the batch is durable: {:?}",
            wire.lock().await.calls
        );

        // The sink was down. The scheduler keeps the recorded range and polls it again.
        let replay = source.poll_range(&engine, &range).await.unwrap();
        assert_eq!(first, replay, "the same range yields the same records");

        // Only once it is durable does the transaction go — holding it any longer would pin the
        // publisher's oldest xmin for the life of the pipeline.
        source.mark_durable(&engine).await.unwrap();
        assert!(
            wire.lock().await.calls.iter().any(|c| c == "end_snapshot"),
            "got: {:?}",
            wire.lock().await.calls
        );
    }

    #[tokio::test]
    async fn an_interrupted_snapshot_restarts_from_the_first_table() {
        // The transaction that held the old snapshot died with the process, and a new one sees a
        // *later* database — so resuming table by table would splice two points in time.
        let engine = Engine::new();
        let mut wire = FakeWire::new(a_transaction());
        wire.slot_existed = true;
        let shared = std::sync::Arc::new(tokio::sync::Mutex::new(wire));
        let mut source = PostgresCdcSource::with_wire(
            options(),
            vec![suppliers(), suppliers()],
            Box::new(Spy(shared.clone())),
            ConnectorLog::default(),
        );
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x50)].into(),
        });

        let range = source.plan_batch(&engine).await.unwrap();
        assert_eq!(range.start[SNAPSHOT_KEY], 0, "back to the first table");
        assert_eq!(
            shared.lock().await.calls[0],
            "open_slot(create=true)",
            "and the slot is recreated so the new snapshot has a consistent point"
        );
    }

    /// What a [`RecordingSink`] kept: the batch id it was handed, and the rows in it.
    type SinkWrites = std::sync::Arc<std::sync::Mutex<Vec<(u64, Vec<RecordBatch>)>>>;

    /// A sink that records what it was handed, and refuses one nominated batch.
    struct RecordingSink {
        fail_on: Option<u64>,
        writes: SinkWrites,
    }

    #[async_trait::async_trait]
    impl crate::sink::Sink for RecordingSink {
        async fn write_batch(&mut self, batches: &[RecordBatch], batch_id: u64) -> Result<u64> {
            if self.fail_on == Some(batch_id) {
                return Err(Error::Execution("sink is down".into()));
            }
            self.writes
                .lock()
                .expect("poisoned")
                .push((batch_id, batches.to_vec()));
            Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
        }
    }

    #[tokio::test]
    async fn a_snapshot_interrupted_by_a_crash_re_reads_every_table_at_the_new_consistent_point() {
        // The review's blocker, driven the way production drives it. The scheduler replays a
        // *recorded* range rather than replanning (`load_planned`), so the snapshot reset that
        // lives in `plan_batch` is skipped entirely on the recovery path. What used to happen:
        // tables `a` and `b` kept their P0 image, table `c` was read at P1, the snapshot was
        // declared complete and the stream started at P1 — so every change to `a` and `b` in
        // [P0, P1) was in neither, with no error and no log line.
        let engine = Engine::new();
        let checkpoints = tempfile::TempDir::new().unwrap();
        let writes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(vec![])));
        let tables = vec![
            table_named("ox_a"),
            table_named("ox_b"),
            table_named("ox_c"),
        ];
        {
            let mut wire = wire.lock().await;
            for (name, id) in [("ox_a", "1"), ("ox_b", "2"), ("ox_c", "3")] {
                wire.snapshots.insert(
                    format!("public.{name}"),
                    vec![vec![Some(id.into()), Some(name.into())]],
                );
            }
        }
        let build = |fail_on: Option<u64>| {
            let mut options = options();
            options.tables = tables.iter().map(TableSchema::qualified).collect();
            (
                Box::new(PostgresCdcSource::with_wire(
                    options,
                    tables.clone(),
                    Box::new(Spy(wire.clone())),
                    ConnectorLog::default(),
                )) as Box<dyn Source>,
                Box::new(RecordingSink {
                    fail_on,
                    writes: writes.clone(),
                }) as Box<dyn crate::sink::Sink>,
            )
        };

        let mgr = crate::scheduler::StreamingQueryManager::new();
        let (source, sink) = build(Some(3));
        let id =
            crate::scheduler::test_harness::register(&mgr, checkpoints.path(), source, sink).await;

        // Batches 1 and 2 snapshot `ox_a` and `ox_b` at the slot's consistent point and commit.
        assert_eq!(mgr.run_batch(&id, &engine).await.unwrap(), 1);
        assert_eq!(mgr.run_batch(&id, &engine).await.unwrap(), 1);
        // Batch 3 records its range, reads `ox_c` — and the sink write never returns.
        mgr.run_batch(&id, &engine).await.unwrap_err();

        // The process dies. A row lands in `ox_a` before the new one starts: this is the change
        // in [P0, P1) that the old code lost, being in neither the snapshot nor the stream.
        wire.lock().await.snapshots.insert(
            "public.ox_a".into(),
            vec![
                vec![Some("1".into()), Some("ox_a".into())],
                vec![
                    Some("99".into()),
                    Some("written between the two points".into()),
                ],
            ],
        );
        let before_restart = writes.lock().unwrap().len();
        let restart_at = wire.lock().await.calls.len();

        // Restart. `register` recovers from the log exactly as `start_with_config` does, so the
        // planned range for batch 3 is still on disk and `plan_batch` is not consulted.
        let (source, sink) = build(None);
        let id =
            crate::scheduler::test_harness::register(&mgr, checkpoints.path(), source, sink).await;
        for _ in 0..8 {
            if mgr.run_batch(&id, &engine).await.unwrap() == 0 {
                break;
            }
        }

        let calls = wire.lock().await.calls[restart_at..].to_vec();
        assert_eq!(
            calls.first().map(String::as_str),
            Some("open_slot(create=true)"),
            "an interrupted snapshot recreates the slot: {calls:?}"
        );
        let stream_at = calls
            .iter()
            .position(|c| c.starts_with("start_stream"))
            .expect("the stream eventually starts");
        for table in ["ox_a", "ox_b", "ox_c"] {
            assert!(
                calls[..stream_at].contains(&format!("snapshot_rows(public.{table})")),
                "`{table}` must be re-read before the stream starts, or its P0 image is spliced \
                 onto a stream beginning at P1: {calls:?}"
            );
        }
        assert_eq!(
            calls[stream_at], "start_stream(0/1100)",
            "and the stream starts at the *new* consistent point: {calls:?}"
        );

        // Every row written after the restart is as of the new consistent point. A single P0 row
        // here would be one half of the splice.
        let written = writes.lock().unwrap().clone();
        let mut ids = BTreeSet::new();
        for (_, batches) in &written[before_restart..] {
            for batch in batches {
                let lsn = column(batch, LSN_COLUMN)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    assert_eq!(lsn.value(i), 0x10FF, "a row from the abandoned snapshot");
                }
                let key = column(batch, "supplierid")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    ids.insert(key.value(i));
                }
            }
        }
        assert!(
            ids.contains(&99),
            "the row written between the two consistent points reached the target: {ids:?}"
        );
        assert_eq!(
            ids,
            [1, 2, 3, 99].into_iter().collect::<BTreeSet<i64>>(),
            "and every source table was read again"
        );
    }

    // -----------------------------------------------------------------------------------------
    // Change decoding
    // -----------------------------------------------------------------------------------------

    async fn stream_once(source: &mut PostgresCdcSource, engine: &Engine) -> Vec<RecordBatch> {
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0)].into(),
        });
        let range = source.plan_batch(engine).await.unwrap();
        source.poll_range(engine, &range).await.unwrap()
    }

    #[tokio::test]
    async fn insert_update_and_delete_carry_their_op_lsn_and_commit_time() {
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.slot_existed = true;
        let batches = stream_once(&mut source, &engine).await;

        let batch = &batches[0];
        assert_eq!(
            strings(batch, OP_COLUMN),
            ["i", "u", "d"].map(|s| Some(s.into()))
        );
        let lsn = column(batch, LSN_COLUMN)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Each change carries its own WAL position, not the transaction's, so two changes to one
        // key inside a transaction still order against each other.
        assert_eq!(
            (lsn.value(0), lsn.value(1), lsn.value(2)),
            (0x120, 0x130, 0x140)
        );
        let ts = column(batch, TS_COLUMN)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), COMMIT_TIME + PG_EPOCH_UNIX_MICROS);

        // The update's unchanged-TOAST column and the delete's non-key columns are both NULL.
        assert_eq!(
            strings(batch, "name"),
            vec![Some("Acme".into()), None, None]
        );
    }

    #[tokio::test]
    async fn an_update_that_moves_the_key_deletes_the_row_it_left_behind() {
        // AUTO CDC merges by key, so an UPDATE that changes the key inserts a row under the new
        // one and leaves the old one in the target forever — a supplier that exists in the
        // lakehouse and not in Postgres, with no error and nothing to detect it. pgoutput writes
        // the `K` section on an UPDATE only when a replica-identity column changed, so its
        // presence is an unambiguous "this row changed identity".
        let engine = Engine::new();
        let (mut source, wire) = source_with(vec![
            xlog(
                0x100,
                relation_msg(
                    RELATION_OID,
                    "public",
                    "sales_suppliers",
                    &[(true, "supplierid", oids::INT8), (false, "name", 25)],
                ),
            ),
            xlog(0x110, begin_msg(0x200, COMMIT_TIME)),
            xlog(
                0x120,
                keyed_update_msg(RELATION_OID, &[Some("7"), None], &[Some("9"), Some("Acme")]),
            ),
            xlog(0x150, commit_msg(0x200, 0x200, COMMIT_TIME)),
        ]);
        wire.lock().await.slot_existed = true;
        let batches = stream_once(&mut source, &engine).await;

        let batch = &batches[0];
        assert_eq!(
            strings(batch, OP_COLUMN),
            ["d", "u"].map(|s| Some(s.into())),
            "the old key is removed, then the new image lands"
        );
        let ids = column(batch, "supplierid")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((ids.value(0), ids.value(1)), (7, 9));
        // Both halves are the same change, and they name different keys — so the shared LSN
        // cannot make them race in the merge.
        let lsn = column(batch, LSN_COLUMN)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((lsn.value(0), lsn.value(1)), (0x120, 0x120));
    }

    #[tokio::test]
    async fn a_full_replica_identity_update_only_deletes_when_the_key_actually_moved() {
        // Under REPLICA IDENTITY FULL there is no `K` — the whole old row arrives as `O` on
        // *every* update — so presence proves nothing and the identity has to be compared. An
        // ordinary column edit must stay a single `'u'`.
        let engine = Engine::new();
        let relation = xlog(
            0x100,
            relation_msg(
                RELATION_OID,
                "public",
                "sales_suppliers",
                &[(true, "supplierid", oids::INT8), (true, "name", 25)],
            ),
        );
        let full_update = |was: &[Option<&str>], now: &[Option<&str>]| {
            let mut out = vec![b'U'];
            out.extend_from_slice(&RELATION_OID.to_be_bytes());
            out.push(b'O');
            tuple(&mut out, was);
            out.push(b'N');
            tuple(&mut out, now);
            out
        };

        let (mut source, wire) = source_with(vec![
            relation.clone(),
            xlog(0x110, begin_msg(0x200, COMMIT_TIME)),
            xlog(
                0x120,
                full_update(&[Some("7"), Some("Acme")], &[Some("7"), Some("Acme Ltd")]),
            ),
            xlog(
                0x130,
                full_update(
                    &[Some("7"), Some("Acme Ltd")],
                    &[Some("8"), Some("Acme Ltd")],
                ),
            ),
            xlog(0x150, commit_msg(0x200, 0x200, COMMIT_TIME)),
        ]);
        wire.lock().await.slot_existed = true;
        let batches = stream_once(&mut source, &engine).await;

        assert_eq!(
            strings(&batches[0], OP_COLUMN),
            ["u", "d", "u"].map(|s| Some(s.into())),
            "one plain update, then the one that moved the key"
        );
        let ids = column(&batches[0], "supplierid")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((ids.value(0), ids.value(1), ids.value(2)), (7, 7, 8));
    }

    #[tokio::test]
    async fn a_truncate_becomes_one_row_the_merge_can_recognize() {
        let engine = Engine::new();
        let stream = vec![
            xlog(
                0x100,
                relation_msg(
                    RELATION_OID,
                    "public",
                    "sales_suppliers",
                    &[(true, "supplierid", oids::INT8), (false, "name", 25)],
                ),
            ),
            xlog(0x110, begin_msg(0x200, COMMIT_TIME)),
            xlog(0x120, truncate_msg(&[RELATION_OID])),
            xlog(0x130, commit_msg(0x200, 0x200, COMMIT_TIME)),
        ];
        let (mut source, wire) = source_with(stream);
        wire.lock().await.slot_existed = true;
        let batches = stream_once(&mut source, &engine).await;
        assert_eq!(strings(&batches[0], OP_COLUMN), vec![Some("t".into())]);
        assert!(column(&batches[0], "supplierid").is_null(0));
    }

    #[tokio::test]
    async fn publish_ops_drops_the_operations_it_does_not_name() {
        let engine = Engine::new();
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(a_transaction())));
        wire.lock().await.slot_existed = true;
        let mut options = options();
        // Append-only history: deletes never reach the target.
        options.publish_ops = [Op::Insert, Op::Update].into_iter().collect();
        let mut source = PostgresCdcSource::with_wire(
            options,
            vec![suppliers()],
            Box::new(Spy(wire.clone())),
            ConnectorLog::default(),
        );
        let batches = stream_once(&mut source, &engine).await;
        assert_eq!(
            strings(&batches[0], OP_COLUMN),
            ["i", "u"].map(|s| Some(s.into()))
        );
    }

    #[tokio::test]
    async fn changes_to_a_table_the_source_does_not_replicate_are_dropped() {
        let engine = Engine::new();
        let mut stream = a_transaction();
        stream.insert(
            1,
            xlog(
                0x105,
                relation_msg(999, "public", "audit_log", &[(true, "id", oids::INT8)]),
            ),
        );
        stream.insert(3, xlog(0x125, change_msg(b'I', 999, b'N', &[Some("1")])));
        let (mut source, wire) = source_with(stream);
        wire.lock().await.slot_existed = true;
        let batches = stream_once(&mut source, &engine).await;
        assert_eq!(
            batches[0].num_rows(),
            3,
            "only the three changes to `public.sales_suppliers`"
        );
    }

    #[tokio::test]
    async fn an_added_column_is_logged_and_the_stream_keeps_running() {
        let engine = Engine::new();
        let dir = tempfile::TempDir::new().unwrap();
        let mut stream = a_transaction();
        // The publisher re-announces the relation with a column this source has never seen.
        stream[0] = xlog(
            0x100,
            relation_msg(
                RELATION_OID,
                "public",
                "sales_suppliers",
                &[
                    (true, "supplierid", oids::INT8),
                    (false, "name", 25),
                    (false, "region", 25),
                ],
            ),
        );
        stream[2] = xlog(
            0x120,
            change_msg(
                b'I',
                RELATION_OID,
                b'N',
                &[Some("7"), Some("Acme"), Some("EU")],
            ),
        );
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(stream)));
        wire.lock().await.slot_existed = true;
        let mut source = PostgresCdcSource::with_wire(
            options(),
            vec![suppliers()],
            Box::new(Spy(wire.clone())),
            ConnectorLog::new(Some(dir.path()), "sales_suppliers"),
        );
        let batches = stream_once(&mut source, &engine).await;

        // Ingestion continues on the schema the query was planned against; the new column is
        // dropped rather than breaking the batch.
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(strings(&batches[0], "name")[0], Some("Acme".into()));
        let log = std::fs::read_to_string(dir.path().join("sales_suppliers.jsonl")).unwrap();
        assert!(log.contains("\"event\":\"schema_change\""), "got: {log}");
        assert!(log.contains("region"), "the added column is named: {log}");
    }

    // -----------------------------------------------------------------------------------------
    // Ranges, replay, and the slot
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_replayed_range_reproduces_identical_batches() {
        // The property the offset log converts into exactly-once. Nothing was confirmed, so the
        // slot still holds the range, and re-reading it has to produce what the first attempt saw
        // — not "whatever the publisher has now".
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.slot_existed = true;
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0)].into(),
        });

        let range = source.plan_batch(&engine).await.unwrap();
        let first = source.poll_range(&engine, &range).await.unwrap();

        // A second transaction lands between the two attempts, exactly as it would in
        // production. The replay must not pick it up: the sink would recognize the batch id,
        // discard the whole thing, and take the newcomer with it.
        {
            let mut wire = wire.lock().await;
            wire.stream.extend(vec![
                xlog(0x210, begin_msg(0x300, COMMIT_TIME)),
                xlog(
                    0x220,
                    change_msg(b'I', RELATION_OID, b'N', &[Some("8"), Some("Beta")]),
                ),
                xlog(0x230, commit_msg(0x300, 0x300, COMMIT_TIME)),
            ]);
        }
        let replay = source.poll_range(&engine, &range).await.unwrap();
        assert_eq!(first, replay, "the same range yields the same records");

        // And the newcomer is still there for the next batch.
        let next = source.plan_batch(&engine).await.unwrap();
        assert_eq!(next.start[LSN_KEY], 0x200);
        assert_eq!(next.end[LSN_KEY], 0x300);
        let next = source.poll_range(&engine, &next).await.unwrap();
        assert_eq!(strings(&next[0], "name"), vec![Some("Beta".into())]);
    }

    #[tokio::test]
    async fn planning_twice_describes_the_same_batch_and_consumes_nothing() {
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.slot_existed = true;
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0)].into(),
        });

        let first = source.plan_batch(&engine).await.unwrap();
        let second = source.plan_batch(&engine).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(
            source.committed_offsets().unwrap().entries[LSN_KEY],
            0,
            "planning moved no committed position"
        );
        assert_eq!(
            wire.lock().await.confirmed,
            None,
            "and confirmed nothing to the server"
        );
    }

    #[tokio::test]
    async fn nothing_is_confirmed_until_the_batch_is_durable() {
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        wire.lock().await.slot_existed = true;
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0)].into(),
        });

        let range = source.plan_batch(&engine).await.unwrap();
        source.poll_range(&engine, &range).await.unwrap();
        assert_eq!(
            wire.lock().await.confirmed,
            None,
            "reading is not confirming: the batch is not in the sink yet"
        );

        source.mark_durable(&engine).await.unwrap();
        assert_eq!(wire.lock().await.confirmed, Some(Lsn(0x200)));
    }

    #[tokio::test]
    async fn a_stretch_of_wal_with_nothing_for_this_publication_advances_the_slot() {
        // A busy server whose other databases fill the WAL would otherwise make the slot grow
        // forever while this table has nothing to say. There is nothing in the stretch to lose,
        // so covering it is safe — and it is the difference between a healthy slot and a full
        // disk on the source. Planning does not *confirm* it, though: the standby status update
        // comes from the keepalive timer or from `mark_durable`, never from a read path.
        let engine = Engine::new();
        let (mut source, wire) = source_with(vec![WireMessage::Keepalive {
            wal_end: Lsn(0x900),
            clock: 0,
            reply_requested: false,
        }]);
        wire.lock().await.slot_existed = true;
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });

        let range = source.plan_batch(&engine).await.unwrap();
        assert!(range.is_empty(), "an idle trigger leaves the table alone");
        assert_eq!(source.committed_offsets().unwrap().entries[LSN_KEY], 0x900);
        assert_eq!(
            wire.lock().await.confirmed,
            None,
            "planning is a read path, and read paths do not confirm"
        );

        source.mark_durable(&engine).await.unwrap();
        assert_eq!(wire.lock().await.confirmed, Some(Lsn(0x900)));
    }

    #[tokio::test]
    async fn a_confirmation_that_was_never_sent_is_logged_as_one() {
        // There is no replication session to send a standby status update on until the snapshot
        // is over, so `mark_durable` genuinely confirms nothing. Recording it as a confirmation
        // anyway would make the connector log disagree with `pg_replication_slots` for no visible
        // reason — exactly the disagreement someone diagnosing slot growth is trying to explain.
        let engine = Engine::new();
        let dir = tempfile::TempDir::new().unwrap();
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(a_transaction())));
        let mut source = PostgresCdcSource::with_wire(
            options(),
            vec![suppliers()],
            Box::new(Spy(wire.clone())),
            ConnectorLog::new(Some(dir.path()), "sales_suppliers"),
        );

        let range = source.plan_batch(&engine).await.unwrap();
        assert_eq!(range.start[SNAPSHOT_KEY], 0, "still snapshotting");
        source.poll_range(&engine, &range).await.unwrap();
        source.mark_durable(&engine).await.unwrap();

        let log = std::fs::read_to_string(dir.path().join("sales_suppliers.jsonl")).unwrap();
        let commit = log
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|e| e["event"] == "commit")
            .expect("a commit event");
        assert_eq!(commit["sent"], false, "nothing left the process: {log}");

        // Once the stream is open the same call really does speak, and says so.
        let range = source.plan_batch(&engine).await.unwrap();
        source.poll_range(&engine, &range).await.unwrap();
        source.mark_durable(&engine).await.unwrap();
        let log = std::fs::read_to_string(dir.path().join("sales_suppliers.jsonl")).unwrap();
        let last = log
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|e| e["event"] == "commit")
            .next_back()
            .expect("a second commit event");
        assert_eq!(last["sent"], true, "got: {log}");
    }

    #[tokio::test]
    async fn a_lost_replication_connection_is_re_dialled_from_the_checkpointed_lsn() {
        // The difference between a blip and an outage. A publisher restart, a failover or an
        // idle `wal_sender_timeout` kills the socket mid-stream; the source must forget where it
        // thought the stream was, dial again, and re-issue `START_REPLICATION` from the position
        // the checkpoint holds — not answer every retry with the same error until someone
        // restarts the process.
        let engine = Engine::new();
        let (mut source, wire) = source_with(a_transaction());
        {
            let mut wire = wire.lock().await;
            wire.slot_existed = true;
            // open_slot, slot_metrics, start_stream, then the reads: die on the second one, in
            // the middle of the transaction.
            wire.fail_nth_call = Some(5);
        }
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });

        let err = source.plan_batch(&engine).await.unwrap_err().to_string();
        assert!(
            err.contains("closed the replication connection"),
            "got: {err}"
        );
        assert_eq!(
            source.committed_offsets().unwrap().entries[LSN_KEY],
            0x100,
            "a failed read commits nothing"
        );

        // The server comes back. The retry the scheduler makes must reconnect by itself.
        wire.lock().await.fail_nth_call = None;
        let range = source.plan_batch(&engine).await.unwrap();
        assert_eq!(range.start[LSN_KEY], 0x100, "resumed from the checkpoint");
        assert_eq!(range.end[LSN_KEY], 0x200);
        let batches = source.poll_range(&engine, &range).await.unwrap();
        assert_eq!(
            strings(&batches[0], OP_COLUMN),
            ["i", "u", "d"].map(|s| Some(s.into())),
            "and the transaction the dead socket cut in half arrives whole"
        );

        let started: Vec<String> = wire
            .lock()
            .await
            .calls
            .iter()
            .filter(|c| c.starts_with("start_stream"))
            .cloned()
            .collect();
        assert_eq!(
            started,
            vec!["start_stream(0/100)", "start_stream(0/100)"],
            "the stream was re-issued rather than read on from a dead socket"
        );
    }

    #[tokio::test]
    async fn an_idle_source_still_sends_a_standby_status_update() {
        // The failure this rules out: a quiet table, a trigger longer than the walsender's
        // patience, and a pipeline that dies overnight for having said nothing. The message
        // carries the committed position, so it moves the slot nowhere.
        let engine = Engine::new();
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(vec![
            WireMessage::Keepalive {
                wal_end: Lsn(0x100),
                clock: 0,
                reply_requested: false,
            },
        ])));
        wire.lock().await.slot_existed = true;
        let mut options = options();
        options.status_interval = Duration::ZERO;
        let mut source = PostgresCdcSource::with_wire(
            options,
            vec![suppliers()],
            Box::new(Spy(wire.clone())),
            ConnectorLog::default(),
        );
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });

        let range = source.plan_batch(&engine).await.unwrap();
        assert!(range.is_empty(), "caught up, nothing to read");
        assert_eq!(
            wire.lock().await.confirmed,
            Some(Lsn(0x100)),
            "and it says so, rather than waiting to be hung up on"
        );
    }

    #[tokio::test]
    async fn a_keepalive_asking_for_a_reply_gets_one_at_the_confirmed_position() {
        // `reply_requested` is Postgres saying "answer me or I will hang up" — it is set once the
        // walsender has not heard from the standby for `wal_sender_timeout / 2`. The answer must
        // carry the position already committed and never one past it: a status update ahead of
        // the checkpoint would let the server recycle WAL a replay still needs.
        let engine = Engine::new();
        let mut stream = vec![WireMessage::Keepalive {
            wal_end: Lsn(0x1000),
            clock: 0,
            reply_requested: true,
        }];
        stream.extend(a_transaction());
        let (mut source, wire) = source_with(stream);
        wire.lock().await.slot_existed = true;
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });

        source.plan_batch(&engine).await.unwrap();
        let wire = wire.lock().await;
        assert_eq!(
            wire.confirmed,
            Some(Lsn(0x100)),
            "the committed position, not the 0x1000 the keepalive reported"
        );
        assert!(
            wire.calls.contains(&"confirm(0/100)".to_string()),
            "a status update went out while the batch was still being read: {:?}",
            wire.calls
        );
    }

    #[tokio::test]
    async fn the_slot_size_guard_stops_the_pipeline_before_the_source_disk_fills() {
        let engine = Engine::new();
        let dir = tempfile::TempDir::new().unwrap();
        let wire = std::sync::Arc::new(tokio::sync::Mutex::new(FakeWire::new(vec![])));
        {
            let mut wire = wire.lock().await;
            wire.slot_existed = true;
            wire.retained_bytes = 64 * 1024 * 1024;
        }
        let mut options = options();
        options.max_slot_bytes = 1024 * 1024;
        let mut source = PostgresCdcSource::with_wire(
            options,
            vec![suppliers()],
            Box::new(Spy(wire.clone())),
            ConnectorLog::new(Some(dir.path()), "sales_suppliers"),
        );
        source.restore_offsets(&SourceOffsets {
            source: SOURCE_NAME.into(),
            entries: [(SNAPSHOT_KEY.to_string(), 1), (LSN_KEY.to_string(), 0x100)].into(),
        });

        let err = source.plan_batch(&engine).await.unwrap_err().to_string();
        assert!(err.contains("64.0 MiB"), "got: {err}");
        assert!(err.contains("max_slot_bytes"), "got: {err}");
        assert!(
            err.contains("pg_drop_replication_slot"),
            "the way out: {err}"
        );
        let log = std::fs::read_to_string(dir.path().join("sales_suppliers.jsonl")).unwrap();
        assert!(log.contains("\"event\":\"slot_metrics\""), "got: {log}");
        assert!(log.contains("\"will_retry\":false"), "got: {log}");
    }

    #[tokio::test]
    async fn a_range_planned_by_another_source_is_refused() {
        let engine = Engine::new();
        let (mut source, _wire) = source_with(vec![]);
        let err = source
            .poll_range(
                &engine,
                &BatchRange {
                    source: "kafka".into(),
                    start: [("events-0".to_string(), 0)].into(),
                    end: [("events-0".to_string(), 7)].into(),
                    items: vec![],
                },
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("planned by `kafka`"), "got: {err}");
    }

    #[test]
    fn offsets_do_not_cross_source_types() {
        let (mut source, _wire) = source_with(vec![]);
        source.restore_offsets(&SourceOffsets {
            source: "kafka".into(),
            entries: [("lsn".to_string(), 999)].into(),
        });
        assert_eq!(source.committed_offsets().unwrap().entries[LSN_KEY], 0);
    }

    #[test]
    fn the_description_names_the_tables_the_slot_and_the_server() {
        let (source, _wire) = source_with(vec![]);
        assert_eq!(
            source.description(),
            "PostgresCDC[public.sales_suppliers@db.internal:5432/sales \
             slot=oxidant_sales_suppliers]"
        );
    }

    #[test]
    fn the_pipeline_context_options_are_derived_and_not_settable_in_yaml() {
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        postgres_cdc_pipeline_options(
            &mut options,
            Path::new("/srv/checkpoints"),
            "sales_suppliers",
        );
        assert_eq!(options[LOG_DIR_OPTION], "/srv/checkpoints/logs");
        assert_eq!(options[NAME_OPTION], "sales_suppliers");
        // They start with `oxidant.`, which is the one prefix option parsing lets through — so a
        // config author cannot point one connector's log at another's file.
        assert!(LOG_DIR_OPTION.starts_with("oxidant."));
        assert!(NAME_OPTION.starts_with("oxidant."));
        let mut pairs = minimal();
        pairs.push((LOG_DIR_OPTION, "/tmp/logs"));
        assert!(parse(&pairs).is_ok());
    }

    #[test]
    fn a_byte_count_reads_like_one() {
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(1024), "1.0 KiB");
        assert_eq!(bytes(10 * 1024 * 1024 * 1024), "10.0 GiB");
    }
}
