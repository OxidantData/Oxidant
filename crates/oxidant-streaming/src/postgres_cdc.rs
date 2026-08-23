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
//! is gone. So the standby status update lives in [`Source::mark_durable`] — after the sink write
//! *and* after the checkpoint save — and nowhere else. Everything a batch read is therefore
//! still on the server if the batch dies, which is what makes a replayed range reproducible and
//! turns the offset log into exactly-once. Reading ahead of the confirmed position costs nothing
//! but memory; confirming ahead of the checkpoint costs data.
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
/// Rows per Arrow batch while a snapshot is being read.
const DEFAULT_SNAPSHOT_BATCH_ROWS: usize = 65_536;
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
    Some(
        time.signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0)?)
            .num_microseconds()?,
    )
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
    async fn confirm(&mut self, written: Lsn, flushed: Lsn) -> Result<()>;
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

        let connection = self.replication().await?;
        if existed {
            // Nothing durable depends on the old slot: `create` is only ever asked for when the
            // snapshot has not completed, so no batch has been committed against it.
            connection
                .execute(&format!(
                    "DROP_REPLICATION_SLOT {} WAIT",
                    quote_identifier(&slot)
                ))
                .await?;
        }
        // `CREATE_REPLICATION_SLOT … USE_SNAPSHOT` must be the first command of its transaction,
        // and the transaction has to outlive the command — that is what keeps the snapshot
        // readable for the COPY that follows.
        connection
            .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
            .await?;
        let created = connection
            .simple_query(&format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput USE_SNAPSHOT",
                quote_identifier(&slot)
            ))
            .await?;
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
        Ok(self.replication().await?.simple_query(&sql).await?.rows)
    }

    async fn end_snapshot(&mut self) -> Result<()> {
        self.replication().await?.execute("COMMIT").await
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
        self.replication()
            .await?
            .start_replication(&slot, &publication, start)
            .await
    }

    async fn next_wire(&mut self, idle: Duration) -> Result<Option<WireMessage>> {
        self.replication().await?.next_wire(idle).await
    }

    async fn confirm(&mut self, written: Lsn, flushed: Lsn) -> Result<()> {
        match self.replication.as_mut() {
            Some(connection) if connection.is_streaming() => {
                connection.send_standby(written, flushed).await
            }
            // Nothing to confirm against: the next `START_REPLICATION` will begin from the
            // checkpointed position anyway, and the slot keeps the WAL until then.
            _ => Ok(()),
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
                        COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn), 0)::int8 \
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
                    "tables": self.tables.iter().map(TableSchema::qualified).collect::<Vec<_>>(),
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

    /// Point the replication stream at `start`, restarting it when it is somewhere else.
    async fn ensure_stream(&mut self, start: Lsn) -> Result<()> {
        if self.stream_at == Some(start) {
            return Ok(());
        }
        self.wire.start_stream(start).await?;
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

        loop {
            // A batch that has already covered whole transactions stops as soon as it has
            // reached either the flush LSN it was aiming at or its byte budget. Both tests are
            // made between transactions so a batch never ends inside one.
            if open_txn.is_empty() && (observed >= stop_at || bytes_seen >= byte_budget) {
                break;
            }
            let Some(message) = self.wire.next_wire(self.options.idle).await? else {
                // The publisher went quiet. Whatever it has sent is what this batch covers.
                break;
            };
            match message {
                WireMessage::Keepalive { wal_end, .. } => observed = observed.max(wal_end),
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
                        LogicalMessage::Begin { .. } => open_txn.clear(),
                        LogicalMessage::Commit {
                            end_lsn,
                            commit_time,
                            ..
                        } => {
                            observed = observed.max(end_lsn);
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
                        LogicalMessage::Update { relation, new, .. } => {
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
        self.stream_at = if open_txn.is_empty() {
            Some(end.max(from))
        } else {
            None
        };
        if committed.is_empty() && open_txn.is_empty() {
            // No change in this stretch of WAL belongs to the publication. Confirming it is safe
            // — there is nothing in it to lose — and it is what stops a slot on a busy server
            // from growing forever while a quiet table has nothing to say.
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
        let Some(relation) = self.relations.get(&relation_oid) else {
            // A change for a relation whose shape was never announced cannot be decoded. pgoutput
            // always sends the Relation first, so this only happens if a stream is joined mid
            // transaction — which a replay from a commit boundary never is.
            return;
        };
        if !self
            .tables
            .iter()
            .any(|t| t.qualified() == relation.qualified())
        {
            return;
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
        into.push(ChangeEvent {
            op,
            lsn: lsn.as_i64(),
            ts_micros: None,
            values,
        });
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
    async fn poll_snapshot(&mut self, table_index: usize) -> Result<Vec<RecordBatch>> {
        let Some(table) = self.tables.get(table_index).cloned() else {
            return Err(Error::Plan(format!(
                "postgres_cdc: this batch was recorded against source table {table_index}, and \
                 the source now has {} — the `tables:` option changed under a live checkpoint",
                self.tables.len()
            )));
        };
        if !self.snapshot_open {
            return Err(Error::Execution(format!(
                "postgres_cdc: the snapshot of `{}` was planned, but the slot's snapshot \
                 transaction is no longer open",
                table.qualified()
            )));
        }
        let started = Instant::now();
        let rows = self.wire.snapshot_rows(&table).await?;
        let events: Vec<ChangeEvent> = rows
            .into_iter()
            .map(|values| ChangeEvent {
                op: Op::Snapshot,
                // Every snapshot row is as of the slot's consistent point, so ordering them
                // against each other is meaningless and ordering them *before* the stream is
                // exactly right: a change to the same key arrives with a strictly larger LSN.
                lsn: self.position.as_i64(),
                // A snapshot row has no commit, so it has no commit timestamp. AUTO CDC orders
                // by `__oxidant_lsn`, not by this.
                ts_micros: None,
                values,
            })
            .collect();
        let batches = self.to_batches(&events)?;

        self.snapshot_done = table_index + 1;
        self.log.event(
            "snapshot_done",
            json!({
                "table": table.qualified(),
                "rows": events.len(),
                "duration_ms": started.elapsed().as_millis() as u64,
                "consistent_point": self.position.to_string(),
            }),
        );
        if self.snapshot_done >= self.tables.len() {
            // The transaction has served its purpose; holding it open would pin the publisher's
            // oldest xmin for as long as the pipeline runs.
            self.wire.end_snapshot().await?;
            self.snapshot_open = false;
        }
        Ok(batches)
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
                start: [(SNAPSHOT_KEY.to_string(), self.snapshot_done as i64)].into(),
                end: [(SNAPSHOT_KEY.to_string(), self.snapshot_done as i64 + 1)].into(),
                items: vec![],
            });
        }

        let from = self.position;
        let (events, end) = self
            .read_forward(from, metrics.server_flush, self.options.max_batch_bytes)
            .await?;
        if events.is_empty() {
            // Nothing for this publication in the WAL just read. The position still moves — the
            // stretch is empty by construction — and confirming it now is what keeps the slot
            // from retaining a busy server's unrelated WAL forever.
            if end > self.position {
                self.position = end;
                self.wire.confirm(end, end).await?;
            }
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
            return self.poll_snapshot((*index).max(0) as usize).await;
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
        self.wire.confirm(self.position, self.position).await?;
        self.log.event(
            "commit",
            json!({
                "confirmed_flush_lsn": self.position.to_string(),
                "snapshot_tables_done": self.snapshot_done,
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
