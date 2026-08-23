//! Minimal Postgres logical-replication client: the wire, and nothing above it.
//!
//! Everything a CDC source needs from Postgres is either ordinary SQL — which
//! [`tokio_postgres`] already does well, and which [`ControlConnection`] uses for validation,
//! catalog introspection and slot metrics — or the *replication* protocol, which no published
//! `tokio-postgres` speaks: 0.7.18 has no way to put `replication=database` in the startup
//! packet and no CopyBoth duplex to carry the stream once `START_REPLICATION` succeeds. So that
//! one socket is opened here, on top of `postgres-protocol` (the framing and SCRAM crate
//! `tokio-postgres` is itself built on). That is protocol plumbing, not a replication
//! framework — the `pgoutput` decode below is hand-rolled, because the message set is ten
//! shapes and vendoring someone else's decoder would put a dependency between us and the
//! format we most need to be exact about.
//!
//! Two properties of this module matter more than its size:
//!
//! - **Nothing here advances the slot.** [`ReplicationConnection::send_standby`] is the only
//!   call that tells Postgres it may recycle WAL, and this module never makes it on its own —
//!   the position it carries is always chosen by the source, which sends the LSN a batch made
//!   durable in the sink *and* in the checkpoint, and never one past it. A keepalive answer
//!   re-sends that same LSN, so it moves the slot nowhere. That is what keeps a crashed batch's
//!   range re-readable — see [`crate::postgres_cdc`].
//! - **Decoding is pure.** [`decode_wire`] and [`decode_logical`] take bytes and return
//!   messages, so the whole format is testable against constructed frames without a server.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use oxidant_common::{Error, Result};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// Microseconds between the Unix epoch and Postgres' own epoch (2000-01-01 UTC). Every
/// timestamp on the replication protocol is counted from the latter.
pub const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

/// A write-ahead-log position.
///
/// Postgres prints it as two hex halves (`16/B374D848`) and puts it on the wire as a big-endian
/// `u64`; both spellings appear in one session, so the conversion lives with the type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The checkpoint stores offsets as `i64`. Real LSNs are nowhere near the sign bit — a
    /// database would have to write eight exabytes of WAL to reach it — so the cast is total.
    pub const fn as_i64(self) -> i64 {
        self.0 as i64
    }

    pub const fn from_i64(value: i64) -> Self {
        Lsn(value as u64)
    }

    /// Parse Postgres' `XXXX/XXXXXXXX` spelling.
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        let (high, low) = text.split_once('/').ok_or_else(|| {
            Error::Io(format!(
                "`{text}` is not a Postgres LSN (expected `16/B374D848`)"
            ))
        })?;
        let high = u64::from_str_radix(high, 16)
            .map_err(|_| Error::Io(format!("`{text}` is not a Postgres LSN")))?;
        let low = u64::from_str_radix(low, 16)
            .map_err(|_| Error::Io(format!("`{text}` is not a Postgres LSN")))?;
        Ok(Lsn((high << 32) | low))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// Postgres' current time in its own epoch, for the clock field of a standby status update.
pub fn now_pg_micros() -> i64 {
    chrono::Utc::now().timestamp_micros() - PG_EPOCH_UNIX_MICROS
}

// ---------------------------------------------------------------------------------------------
// pgoutput (protocol version 1)
// ---------------------------------------------------------------------------------------------

/// One column's value inside a pgoutput tuple.
///
/// `UnchangedToast` is not a value at all: it is Postgres saying "this out-of-line column was
/// not touched by the UPDATE, so I did not write it to the WAL". Collapsing it into `Null`
/// here would turn "unchanged" into "set to NULL", which is how a CDC pipeline silently erases
/// large text columns — so the distinction survives the decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleData {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

/// One column of a `Relation` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    /// Part of the relation's replica identity — the columns a delete's old image carries.
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// A relation's shape as the *publisher* sees it right now.
///
/// Re-sent whenever it changes, which is how a schema change reaches a subscriber at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    /// `d` default (PK), `n` nothing, `f` full, `i` a named unique index.
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn>,
}

impl Relation {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

/// A decoded pgoutput v1 message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalMessage {
    Begin {
        final_lsn: Lsn,
        commit_time: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_time: i64,
    },
    Origin {
        commit_lsn: Lsn,
        name: String,
    },
    Relation(Relation),
    /// A user-defined type's identity. Carried so the decoder can name the type in a warning;
    /// values still arrive in text form.
    Type {
        oid: u32,
        namespace: String,
        name: String,
    },
    Insert {
        relation: u32,
        new: Vec<TupleData>,
    },
    Update {
        relation: u32,
        /// The replica-identity columns of the old row, when the identity is an index.
        key: Option<Vec<TupleData>>,
        /// The whole old row, when the identity is `FULL`.
        old: Option<Vec<TupleData>>,
        new: Vec<TupleData>,
    },
    Delete {
        relation: u32,
        key: Option<Vec<TupleData>>,
        old: Option<Vec<TupleData>>,
    },
    Truncate {
        /// Bit 1 `CASCADE`, bit 2 `RESTART IDENTITY` — recorded, not acted on.
        options: u8,
        relations: Vec<u32>,
    },
}

/// A cursor over a message body that reports where it ran out rather than panicking.
///
/// Every `pgoutput` frame is a fixed grammar with no length prefix on the message as a whole, so
/// a truncated or unexpected frame shows up as an out-of-bounds read. Returning an error means a
/// protocol surprise stops the batch instead of taking the process down.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], what: &'static str) -> Self {
        Self { buf, pos: 0, what }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.saturating_add(n);
        if end > self.buf.len() {
            return Err(Error::Io(format!(
                "{} message is truncated: wanted {n} byte(s) at offset {}, only {} left",
                self.what,
                self.pos,
                self.buf.len().saturating_sub(self.pos)
            )));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// Everything not yet consumed — the tail of a message whose grammar ends in "and the rest".
    fn rest(&mut self) -> &'a [u8] {
        let out = &self.buf[self.pos.min(self.buf.len())..];
        self.pos = self.buf.len();
        out
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn lsn(&mut self) -> Result<Lsn> {
        Ok(Lsn(u64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        )))
    }

    fn cstr(&mut self) -> Result<String> {
        let rest = &self.buf[self.pos.min(self.buf.len())..];
        let end = rest.iter().position(|b| *b == 0).ok_or_else(|| {
            Error::Io(format!(
                "{} message ends inside a string at offset {}",
                self.what, self.pos
            ))
        })?;
        let text = String::from_utf8(rest[..end].to_vec())
            .map_err(|e| Error::Io(format!("{} message has a non-UTF-8 string: {e}", self.what)))?;
        self.pos += end + 1;
        Ok(text)
    }

    /// `Int16 columns`, then one tagged value per column.
    fn tuple(&mut self) -> Result<Vec<TupleData>> {
        let columns = self.i16()?;
        if columns < 0 {
            return Err(Error::Io(format!(
                "{} message declares {columns} columns",
                self.what
            )));
        }
        let mut out = Vec::with_capacity(columns as usize);
        for _ in 0..columns {
            out.push(match self.u8()? {
                b'n' => TupleData::Null,
                b'u' => TupleData::UnchangedToast,
                b't' => {
                    let len = self.i32()?;
                    TupleData::Text(self.take(len.max(0) as usize)?.to_vec())
                }
                b'b' => {
                    let len = self.i32()?;
                    TupleData::Binary(self.take(len.max(0) as usize)?.to_vec())
                }
                other => {
                    return Err(Error::Io(format!(
                        "{} message has tuple kind `{}`, which pgoutput v1 does not define",
                        self.what, other as char
                    )))
                }
            });
        }
        Ok(out)
    }
}

/// Decode one pgoutput v1 message.
///
/// Streaming (`proto_version 2`) message types are deliberately absent: the source asks for
/// version 1, so an in-progress transaction is never sent and every change this returns is
/// already committed on the publisher. That is where "committed transactions only" comes from —
/// it is a property of the protocol version, not something filtered later.
pub fn decode_logical(bytes: &[u8]) -> Result<LogicalMessage> {
    let mut r = Reader::new(bytes, "pgoutput");
    Ok(match r.u8()? {
        b'B' => LogicalMessage::Begin {
            final_lsn: r.lsn()?,
            commit_time: r.i64()?,
            xid: r.u32()?,
        },
        b'C' => LogicalMessage::Commit {
            flags: r.u8()?,
            commit_lsn: r.lsn()?,
            end_lsn: r.lsn()?,
            commit_time: r.i64()?,
        },
        b'O' => LogicalMessage::Origin {
            commit_lsn: r.lsn()?,
            name: r.cstr()?,
        },
        b'R' => {
            let oid = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let replica_identity = r.u8()?;
            let count = r.i16()?;
            let mut columns = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count.max(0) {
                columns.push(RelationColumn {
                    key: r.u8()? & 1 == 1,
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    type_modifier: r.i32()?,
                });
            }
            LogicalMessage::Relation(Relation {
                oid,
                // pgoutput sends an empty namespace for `pg_catalog`.
                namespace: if namespace.is_empty() {
                    "pg_catalog".to_string()
                } else {
                    namespace
                },
                name,
                replica_identity,
                columns,
            })
        }
        b'Y' => LogicalMessage::Type {
            oid: r.u32()?,
            namespace: r.cstr()?,
            name: r.cstr()?,
        },
        b'I' => {
            let relation = r.u32()?;
            expect_tag(&mut r, b'N', "Insert")?;
            LogicalMessage::Insert {
                relation,
                new: r.tuple()?,
            }
        }
        b'U' => {
            let relation = r.u32()?;
            let mut key = None;
            let mut old = None;
            let new;
            // `K`/`O` are optional and mutually exclusive; `N` always closes the message.
            loop {
                match r.u8()? {
                    b'K' => key = Some(r.tuple()?),
                    b'O' => old = Some(r.tuple()?),
                    b'N' => {
                        new = r.tuple()?;
                        break;
                    }
                    other => {
                        return Err(Error::Io(format!(
                            "pgoutput Update message has section `{}`",
                            other as char
                        )))
                    }
                }
            }
            LogicalMessage::Update {
                relation,
                key,
                old,
                new,
            }
        }
        b'D' => {
            let relation = r.u32()?;
            let (mut key, mut old) = (None, None);
            match r.u8()? {
                b'K' => key = Some(r.tuple()?),
                b'O' => old = Some(r.tuple()?),
                other => {
                    return Err(Error::Io(format!(
                        "pgoutput Delete message has section `{}`",
                        other as char
                    )))
                }
            }
            LogicalMessage::Delete { relation, key, old }
        }
        b'T' => {
            let count = r.i32()?;
            let options = r.u8()?;
            let mut relations = Vec::with_capacity(count.max(0) as usize);
            for _ in 0..count.max(0) {
                relations.push(r.u32()?);
            }
            LogicalMessage::Truncate { options, relations }
        }
        other => {
            return Err(Error::Io(format!(
                "pgoutput message type `{}` is not part of protocol version 1",
                other as char
            )))
        }
    })
}

fn expect_tag(r: &mut Reader<'_>, tag: u8, what: &str) -> Result<()> {
    let got = r.u8()?;
    if got != tag {
        return Err(Error::Io(format!(
            "pgoutput {what} message expected section `{}`, got `{}`",
            tag as char, got as char
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The CopyBoth stream
// ---------------------------------------------------------------------------------------------

/// One frame of the replication stream, before the pgoutput payload is decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    /// WAL data. `wal_end` is where the server has sent up to, which is how a consumer knows it
    /// has covered a range even when none of the WAL in it belonged to this publication.
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        clock: i64,
        data: Vec<u8>,
    },
    Keepalive {
        wal_end: Lsn,
        clock: i64,
        reply_requested: bool,
    },
}

/// Decode a CopyData payload from a `START_REPLICATION` stream.
pub fn decode_wire(bytes: &[u8]) -> Result<WireMessage> {
    let mut r = Reader::new(bytes, "replication");
    Ok(match r.u8()? {
        b'w' => WireMessage::XLogData {
            wal_start: r.lsn()?,
            wal_end: r.lsn()?,
            clock: r.i64()?,
            data: bytes[r.pos..].to_vec(),
        },
        b'k' => WireMessage::Keepalive {
            wal_end: r.lsn()?,
            clock: r.i64()?,
            reply_requested: r.u8()? != 0,
        },
        other => {
            return Err(Error::Io(format!(
                "replication stream frame `{}` is neither XLogData nor a keepalive",
                other as char
            )))
        }
    })
}

/// Build a standby status update — the message that lets Postgres discard WAL.
///
/// `flushed` is the number that matters: it becomes the slot's `confirmed_flush_lsn`, and
/// everything at or below it may be recycled. `written` may run ahead of it (we have the bytes
/// in memory) without granting the server anything.
pub fn standby_status_update(written: Lsn, flushed: Lsn, applied: Lsn, clock: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(34);
    out.push(b'r');
    out.extend_from_slice(&written.0.to_be_bytes());
    out.extend_from_slice(&flushed.0.to_be_bytes());
    out.extend_from_slice(&applied.0.to_be_bytes());
    out.extend_from_slice(&clock.to_be_bytes());
    // Never ask the server to reply: the answer would be one more keepalive to drain, and the
    // source already knows what it confirmed.
    out.push(0);
    out
}

// ---------------------------------------------------------------------------------------------
// Connection configuration
// ---------------------------------------------------------------------------------------------

/// libpq's `sslmode`, restricted to the four values the connector documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    Disable,
    /// Encrypt, but accept any certificate — protects against passive capture only.
    Require,
    /// Verify the chain against the trust store, but not the hostname.
    VerifyCa,
    /// Verify chain *and* hostname. The only mode that stops an active attacker.
    VerifyFull,
}

impl TlsMode {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "disable" => Ok(TlsMode::Disable),
            "require" => Ok(TlsMode::Require),
            "verify-ca" | "verify_ca" => Ok(TlsMode::VerifyCa),
            "verify-full" | "verify_full" => Ok(TlsMode::VerifyFull),
            other => Err(Error::Io(format!(
                "postgres_cdc `tls: {other}` is not a TLS mode (expected `disable`, `require`, \
                 `verify-ca`, or `verify-full`)"
            ))),
        }
    }

    pub fn enabled(self) -> bool {
        self != TlsMode::Disable
    }
}

impl fmt::Display for TlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TlsMode::Disable => "disable",
            TlsMode::Require => "require",
            TlsMode::VerifyCa => "verify-ca",
            TlsMode::VerifyFull => "verify-full",
        })
    }
}

/// Everything needed to open either connection to the source database.
#[derive(Debug, Clone)]
pub struct PgConnectConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    pub tls: TlsMode,
    pub tls_ca: Option<PathBuf>,
}

impl PgConnectConfig {
    /// Session settings both connections run before anything else.
    ///
    /// Every value this connector reads — snapshot rows and WAL tuples alike — arrives as the
    /// output of a Postgres type's text function, and three of those functions consult session
    /// GUCs. Left at the server's defaults, the same `timestamptz` decodes differently depending
    /// on the *database's* time zone, and `bytea` could arrive in the pre-9.0 escape format. So
    /// the session is pinned rather than the parser made tolerant.
    pub const SESSION_SETUP: &'static [&'static str] = &[
        "SET TIME ZONE 'UTC'",
        "SET datestyle = 'ISO, MDY'",
        "SET bytea_output = 'hex'",
        "SET extra_float_digits = 3",
    ];

    fn tls_client_config(&self) -> Result<Arc<rustls::ClientConfig>> {
        use rustls_pki_types::pem::PemObject;

        let mut roots = rustls::RootCertStore::empty();
        match &self.tls_ca {
            Some(path) => {
                let certs: std::result::Result<Vec<_>, _> =
                    rustls_pki_types::CertificateDer::pem_file_iter(path)
                        .map_err(|e| {
                            Error::Io(format!("postgres_cdc `tls_ca: {}`: {e}", path.display()))
                        })?
                        .collect();
                let certs = certs.map_err(|e| {
                    Error::Io(format!("postgres_cdc `tls_ca: {}`: {e}", path.display()))
                })?;
                if certs.is_empty() {
                    return Err(Error::Io(format!(
                        "postgres_cdc `tls_ca: {}` holds no certificates",
                        path.display()
                    )));
                }
                for cert in certs {
                    roots.add(cert).map_err(|e| {
                        Error::Io(format!("postgres_cdc `tls_ca: {}`: {e}", path.display()))
                    })?;
                }
            }
            // No bundle named: trust what the machine trusts, which is what an operator who
            // has already installed their CA expects. `require` never consults the store.
            None if self.tls != TlsMode::Require => {
                let native = rustls_native_certs::load_native_certs();
                if native.certs.is_empty() {
                    return Err(Error::Io(format!(
                        "postgres_cdc `tls: {}` needs trust anchors, and this machine's \
                         certificate store yielded none ({}). Point `tls_ca:` at the server's \
                         CA bundle — for RDS, the regional `rds-ca-*.pem`.",
                        self.tls,
                        native
                            .errors
                            .first()
                            .map(|e| e.to_string())
                            .unwrap_or_else(|| "the store is empty".into())
                    )));
                }
                for cert in native.certs {
                    roots.add(cert).map_err(|e| {
                        Error::Io(format!("postgres_cdc: platform certificate store: {e}"))
                    })?;
                }
            }
            None => {}
        }

        // The provider is named rather than taken from the process default: the binary links
        // both `ring` and `aws-lc-rs` (through reqwest), and `ClientConfig::builder()` panics
        // when it has to choose.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::Io(format!("postgres_cdc: TLS configuration: {e}")))?;
        let config = match self.tls {
            TlsMode::Disable => unreachable!("callers check `enabled()` first"),
            TlsMode::Require => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate::new(provider)))
                .with_no_client_auth(),
            TlsMode::VerifyCa => {
                let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()
                .map_err(|e| Error::Io(format!("postgres_cdc: TLS configuration: {e}")))?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(SkipHostnameVerifier(inner)))
                    .with_no_client_auth()
            }
            TlsMode::VerifyFull => builder.with_root_certificates(roots).with_no_client_auth(),
        };
        Ok(Arc::new(config))
    }

    /// Open the ordinary SQL connection used for validation, introspection and slot metrics.
    ///
    /// Separate from the replication socket because that one spends its life in CopyBoth: once
    /// `START_REPLICATION` is running, no query can be issued on it, and slot metrics are
    /// exactly what an operator wants *while* the stream is running.
    pub async fn connect_control(&self) -> Result<ControlConnection> {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .application_name("oxidant-postgres-cdc");
        if let Some(password) = &self.password {
            config.password(password);
        }
        let client = if self.tls.enabled() {
            let tls =
                tokio_postgres_rustls::MakeRustlsConnect::new((*self.tls_client_config()?).clone());
            let (client, connection) = config.connect(tls).await.map_err(|e| self.dial_error(e))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        } else {
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| self.dial_error(e))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        };
        for setup in Self::SESSION_SETUP {
            client
                .simple_query(setup)
                .await
                .map_err(|e| Error::Io(format!("postgres_cdc: `{setup}`: {e}")))?;
        }
        Ok(ControlConnection { client })
    }

    fn dial_error(&self, e: tokio_postgres::Error) -> Error {
        Error::Io(format!(
            "postgres_cdc: connect to {}:{}/{} as `{}` (tls={}): {e}",
            self.host, self.port, self.database, self.user, self.tls
        ))
    }
}

/// `tls: require` — encrypt, verify nothing.
///
/// This is libpq's `require`, and it is in the connector for the same reason libpq has it: a
/// managed database whose certificate is signed by a private CA the operator has not yet wired
/// up is otherwise unreachable. It stops passive capture and nothing else, which is why the
/// setup doc points at `verify-full`.
#[derive(Debug)]
struct AcceptAnyCertificate(Arc<rustls::crypto::CryptoProvider>);

impl AcceptAnyCertificate {
    fn new(provider: Arc<rustls::crypto::CryptoProvider>) -> Self {
        Self(provider)
    }
}

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `tls: verify-ca` — verify the chain, ignore the name.
///
/// Wraps the real verifier instead of replacing it, so a bad chain is still a bad chain; only
/// the name check is dropped. That is the one difference between libpq's `verify-ca` and
/// `verify-full`, and it exists for databases reached through an address the certificate does
/// not carry.
#[derive(Debug)]
struct SkipHostnameVerifier(Arc<rustls::client::WebPkiServerVerifier>);

impl rustls::client::danger::ServerCertVerifier for SkipHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls_pki_types::CertificateDer<'_>,
        intermediates: &[rustls_pki_types::CertificateDer<'_>],
        server_name: &rustls_pki_types::ServerName<'_>,
        ocsp: &[u8],
        now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(rustls::client::danger::ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// The ordinary SQL connection.
pub struct ControlConnection {
    client: tokio_postgres::Client,
}

impl ControlConnection {
    /// Run a query and return every column of every row as text.
    ///
    /// Text for the same reason the replication path decodes text: one conversion table for the
    /// whole connector, rather than a binary path that can disagree with the WAL path about what
    /// a `numeric` is.
    pub async fn query(&self, sql: &str, params: &[&str]) -> Result<Vec<Vec<Option<String>>>> {
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows = self
            .client
            .query(sql, &params)
            .await
            .map_err(|e| Error::Io(format!("postgres_cdc: {}: {e}", one_line(sql))))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mut values = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                // Every query in this connector selects `::text`, so the cast is the query's
                // job and a mismatch here is a bug worth reporting rather than a NULL.
                values.push(row.try_get::<_, Option<String>>(i).map_err(|e| {
                    Error::Io(format!(
                        "postgres_cdc: {}: column {i} is not text: {e}",
                        one_line(sql)
                    ))
                })?);
            }
            out.push(values);
        }
        Ok(out)
    }

    /// Run a statement, ignoring its result.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.client
            .simple_query(sql)
            .await
            .map(|_| ())
            .map_err(|e| Error::Io(format!("postgres_cdc: {}: {e}", one_line(sql))))
    }

    /// The first column of the first row, or `None` when the query returned nothing.
    pub async fn scalar(&self, sql: &str, params: &[&str]) -> Result<Option<String>> {
        Ok(self
            .query(sql, params)
            .await?
            .into_iter()
            .next()
            .and_then(|mut row| {
                if row.is_empty() {
                    None
                } else {
                    row.swap_remove(0)
                }
            }))
    }
}

fn one_line(sql: &str) -> String {
    let flat: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.len() > 120 {
        format!("{}…", &flat[..120])
    } else {
        flat
    }
}

// ---------------------------------------------------------------------------------------------
// The replication connection
// ---------------------------------------------------------------------------------------------

/// Either half of the socket a replication session runs over.
///
/// Boxed rather than an enum so the TLS and plaintext paths share every line below the
/// handshake; `Box<dyn T>` already forwards `AsyncRead`/`AsyncWrite`.
trait PgIo: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> PgIo for T {}

/// One result set from the simple query protocol.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl QueryResult {
    /// The value of `column` in the first row.
    pub fn first(&self, column: &str) -> Option<&str> {
        let index = self.columns.iter().position(|c| c == column)?;
        self.rows.first()?.get(index)?.as_deref()
    }
}

/// One backend message, still undecoded.
///
/// The frame reader is written here rather than taken from `postgres-protocol` because that
/// crate's `Message::parse` rejects `CopyBothResponse` outright — `tokio-postgres` never opens a
/// replication session, so the one tag this module exists to see is the one tag it cannot read.
struct Frame {
    tag: u8,
    body: Bytes,
}

impl Frame {
    fn reader(&self, what: &'static str) -> Reader<'_> {
        Reader::new(&self.body, what)
    }

    /// The name Postgres' protocol documentation gives this tag, for error messages.
    fn describe(&self) -> String {
        let name = match self.tag {
            b'R' => "Authentication",
            b'K' => "BackendKeyData",
            b'C' => "CommandComplete",
            b'd' => "CopyData",
            b'c' => "CopyDone",
            b'G' => "CopyInResponse",
            b'H' => "CopyOutResponse",
            b'W' => "CopyBothResponse",
            b'D' => "DataRow",
            b'I' => "EmptyQueryResponse",
            b'E' => "ErrorResponse",
            b'N' => "NoticeResponse",
            b'A' => "NotificationResponse",
            b'S' => "ParameterStatus",
            b'Z' => "ReadyForQuery",
            b'T' => "RowDescription",
            _ => return format!("message `{}`", self.tag as char),
        };
        name.to_string()
    }
}

/// A session opened with `replication=database`.
///
/// A walsender in database mode answers ordinary SQL as well as replication commands, which is
/// what makes the snapshot handoff possible on one socket: the slot is created and its snapshot
/// read inside a single transaction on this connection, and `START_REPLICATION` follows on the
/// same one.
pub struct ReplicationConnection {
    io: Box<dyn PgIo>,
    /// Bytes read from the socket that have not yet formed a whole message.
    buf: BytesMut,
    streaming: bool,
}

impl ReplicationConnection {
    pub async fn connect(config: &PgConnectConfig) -> Result<Self> {
        let address = format!("{}:{}", config.host, config.port);
        let stream = TcpStream::connect(&address)
            .await
            .map_err(|e| Error::Io(format!("postgres_cdc: connect to {address}: {e}")))?;
        // Nagle would add a round trip to every standby status update, which is one per batch.
        let _ = stream.set_nodelay(true);

        let io: Box<dyn PgIo> = if config.tls.enabled() {
            Box::new(negotiate_tls(stream, config).await?)
        } else {
            Box::new(stream)
        };

        let mut conn = Self {
            io,
            buf: BytesMut::with_capacity(16 * 1024),
            streaming: false,
        };
        conn.startup(config).await?;
        for setup in PgConnectConfig::SESSION_SETUP {
            conn.execute(setup).await?;
        }
        Ok(conn)
    }

    /// Startup packet, authentication, and the wait for `ReadyForQuery`.
    ///
    /// `replication=database` is the whole reason this handshake is written out here: it is a
    /// startup-packet parameter, not a GUC, so it cannot be smuggled through a client that does
    /// not know about it — no `options=-c ...`, no `SET`.
    async fn startup(&mut self, config: &PgConnectConfig) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::startup_message(
            [
                ("user", config.user.as_str()),
                ("database", config.database.as_str()),
                ("client_encoding", "UTF8"),
                ("application_name", "oxidant-postgres-cdc"),
                ("replication", "database"),
            ],
            &mut buf,
        )
        .map_err(|e| Error::Io(format!("postgres_cdc: startup message: {e}")))?;
        self.write(&buf).await?;
        self.authenticate(config).await?;

        loop {
            let frame = self.read_frame().await?;
            match frame.tag {
                b'Z' => return Ok(()),
                b'K' | b'S' | b'N' => {}
                b'E' => return Err(server_error(&frame, "opening a replication session")),
                _ => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: unexpected {} while opening a replication session",
                        frame.describe()
                    )))
                }
            }
        }
    }

    async fn authenticate(&mut self, config: &PgConnectConfig) -> Result<()> {
        loop {
            let frame = self.read_frame().await?;
            match frame.tag {
                b'E' => return Err(server_error(&frame, "authentication")),
                b'N' | b'S' => continue,
                b'R' => {}
                _ => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: unexpected {} during authentication",
                        frame.describe()
                    )))
                }
            }
            let mut r = frame.reader("Authentication");
            match r.i32()? {
                0 => return Ok(()),
                3 => {
                    let password = require_password(config)?;
                    let mut buf = BytesMut::new();
                    frontend::password_message(password.as_bytes(), &mut buf)
                        .map_err(|e| Error::Io(format!("postgres_cdc: password message: {e}")))?;
                    self.write(&buf).await?;
                }
                5 => {
                    let salt: [u8; 4] = r.take(4)?.try_into().expect("4 bytes");
                    let password = require_password(config)?;
                    let hash = postgres_protocol::authentication::md5_hash(
                        config.user.as_bytes(),
                        password.as_bytes(),
                        salt,
                    );
                    let mut buf = BytesMut::new();
                    frontend::password_message(hash.as_bytes(), &mut buf)
                        .map_err(|e| Error::Io(format!("postgres_cdc: password message: {e}")))?;
                    self.write(&buf).await?;
                }
                10 => {
                    let password = require_password(config)?;
                    self.authenticate_sasl(&mut r, &password).await?;
                    return Ok(());
                }
                other => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: the server asked for authentication method {other}, which \
                         this connector does not implement (it speaks trust, password, md5 and \
                         SCRAM-SHA-256)"
                    )))
                }
            }
        }
    }

    /// SCRAM-SHA-256, the default on every modern server.
    ///
    /// Channel binding is declared unsupported rather than computed: `-PLUS` only strengthens
    /// SCRAM against an attacker who already terminates the TLS session, and claiming support
    /// without producing the endpoint hash would fail the exchange outright.
    async fn authenticate_sasl(&mut self, r: &mut Reader<'_>, password: &str) -> Result<()> {
        const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

        let mut mechanisms = Vec::new();
        loop {
            let mechanism = r.cstr()?;
            if mechanism.is_empty() {
                break;
            }
            mechanisms.push(mechanism);
        }
        if !mechanisms.iter().any(|m| m == SCRAM_SHA_256) {
            return Err(Error::Io(format!(
                "postgres_cdc: the server offers only [{}] authentication; this connector \
                 implements SCRAM-SHA-256",
                mechanisms.join(", ")
            )));
        }

        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
        let mut buf = BytesMut::new();
        frontend::sasl_initial_response(SCRAM_SHA_256, scram.message(), &mut buf)
            .map_err(|e| Error::Io(format!("postgres_cdc: SASL initial response: {e}")))?;
        self.write(&buf).await?;

        scram
            .update(&self.expect_auth(11, "a SCRAM challenge").await?)
            .map_err(|e| Error::Io(format!("postgres_cdc: SCRAM: {e}")))?;
        let mut buf = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut buf)
            .map_err(|e| Error::Io(format!("postgres_cdc: SASL response: {e}")))?;
        self.write(&buf).await?;

        scram
            .finish(&self.expect_auth(12, "the SCRAM server signature").await?)
            .map_err(|e| Error::Io(format!("postgres_cdc: SCRAM: {e}")))?;
        // The server still owes an AuthenticationOk.
        self.expect_auth(0, "AuthenticationOk").await?;
        Ok(())
    }

    /// Read the next authentication message and return its payload, insisting on `code`.
    async fn expect_auth(&mut self, code: i32, what: &str) -> Result<Vec<u8>> {
        loop {
            let frame = self.read_frame().await?;
            match frame.tag {
                b'N' | b'S' => continue,
                b'E' => return Err(server_error(&frame, "authentication")),
                b'R' => {}
                _ => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: expected {what}, got {}",
                        frame.describe()
                    )))
                }
            }
            let mut r = frame.reader("Authentication");
            let got = r.i32()?;
            if got != code {
                return Err(Error::Io(format!(
                    "postgres_cdc: expected {what} (authentication message {code}), got message \
                     {got}"
                )));
            }
            return Ok(r.rest().to_vec());
        }
    }

    /// Run a statement over the simple query protocol and collect its rows as text.
    ///
    /// The simple protocol, not the extended one, because it hands values back already in the
    /// text form the pgoutput decoder reads — so the snapshot and the stream go through one
    /// conversion table instead of two that can disagree about what a `numeric` is.
    pub async fn simple_query(&mut self, sql: &str) -> Result<QueryResult> {
        if self.streaming {
            return Err(Error::Io(
                "postgres_cdc: the replication session is streaming; a query cannot be issued on \
                 it until the stream is restarted"
                    .into(),
            ));
        }
        let mut buf = BytesMut::new();
        frontend::query(sql, &mut buf)
            .map_err(|e| Error::Io(format!("postgres_cdc: query message: {e}")))?;
        self.write(&buf).await?;

        let mut result = QueryResult::default();
        let mut failure = None;
        loop {
            let frame = self.read_frame().await?;
            match frame.tag {
                b'T' => {
                    result.columns = row_description(&frame)?;
                    result.rows.clear();
                }
                b'D' => result.rows.push(data_row(&frame, sql)?),
                b'C' | b'I' | b'N' | b'S' | b'A' => {}
                // Read to `ReadyForQuery` before returning: leaving the tail of a failed
                // statement on the socket would desynchronize every message after it.
                b'E' => failure = Some(server_error(&frame, &one_line(sql))),
                b'Z' => {
                    return match failure {
                        Some(e) => Err(e),
                        None => Ok(result),
                    }
                }
                _ => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: {}: unexpected {}",
                        one_line(sql),
                        frame.describe()
                    )))
                }
            }
        }
    }

    /// Run a statement whose result is not read.
    pub async fn execute(&mut self, sql: &str) -> Result<()> {
        self.simple_query(sql).await.map(|_| ())
    }

    /// Issue `START_REPLICATION` and leave the connection in CopyBoth.
    pub async fn start_replication(
        &mut self,
        slot: &str,
        publication: &str,
        start: Lsn,
    ) -> Result<()> {
        // `proto_version '1'` is load-bearing: version 2 and up stream *in-progress*
        // transactions, and this source's whole contract is that a batch holds only changes the
        // publisher has already committed.
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {start} (proto_version '1', publication_names {})",
            quote_identifier(slot),
            quote_literal(publication)
        );
        let mut buf = BytesMut::new();
        frontend::query(&sql, &mut buf)
            .map_err(|e| Error::Io(format!("postgres_cdc: query message: {e}")))?;
        self.write(&buf).await?;

        loop {
            let frame = self.read_frame().await?;
            match frame.tag {
                b'W' => {
                    self.streaming = true;
                    return Ok(());
                }
                b'N' | b'S' => {}
                b'E' => return Err(server_error(&frame, &one_line(&sql))),
                _ => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: START_REPLICATION returned {}",
                        frame.describe()
                    )))
                }
            }
        }
    }

    /// Whether the connection is currently carrying a replication stream.
    pub fn is_streaming(&self) -> bool {
        self.streaming
    }

    /// The next frame of the stream, or `None` when nothing arrived within `idle`.
    ///
    /// A timeout is a legitimate answer, not an error: it is how a caller learns the publisher
    /// has no more WAL to send right now, which is the ordinary state of a caught-up stream.
    /// `read_buf` is cancel-safe, so bytes already pulled off the socket survive the timeout.
    pub async fn next_wire(&mut self, idle: Duration) -> Result<Option<WireMessage>> {
        if !self.streaming {
            return Err(Error::Io(
                "postgres_cdc: the replication session is not streaming".into(),
            ));
        }
        loop {
            let frame = match tokio::time::timeout(idle, self.read_frame()).await {
                Ok(frame) => frame?,
                Err(_) => return Ok(None),
            };
            match frame.tag {
                b'd' => return decode_wire(&frame.body).map(Some),
                b'N' | b'S' | b'A' => {}
                b'c' => {
                    self.streaming = false;
                    return Ok(None);
                }
                b'E' => {
                    self.streaming = false;
                    return Err(server_error(&frame, "replication stream"));
                }
                _ => {
                    return Err(Error::Io(format!(
                        "postgres_cdc: replication stream carried {}",
                        frame.describe()
                    )))
                }
            }
        }
    }

    /// Tell Postgres it may recycle WAL up to `flushed`.
    ///
    /// The single call in this crate that loses data if it is made too early, which is why it
    /// takes an explicit argument rather than reading the connection's own position.
    pub async fn send_standby(&mut self, written: Lsn, flushed: Lsn) -> Result<()> {
        let payload = standby_status_update(written, flushed, flushed, now_pg_micros());
        let mut buf = BytesMut::with_capacity(payload.len() + 5);
        buf.put_u8(b'd');
        buf.put_i32(payload.len() as i32 + 4);
        buf.put_slice(&payload);
        self.write(&buf).await
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.io
            .write_all(bytes)
            .await
            .map_err(|e| Error::Io(format!("postgres_cdc: write to the server: {e}")))?;
        self.io
            .flush()
            .await
            .map_err(|e| Error::Io(format!("postgres_cdc: flush to the server: {e}")))
    }

    /// Pull one `tag / length / body` frame off the socket.
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            if self.buf.len() >= 5 {
                let length = u32::from_be_bytes(self.buf[1..5].try_into().expect("4 bytes"));
                if length < 4 {
                    return Err(Error::Io(format!(
                        "postgres_cdc: the server sent a frame of length {length}"
                    )));
                }
                let total = length as usize + 1;
                if self.buf.len() >= total {
                    let mut frame = self.buf.split_to(total);
                    let tag = frame[0];
                    let body = frame.split_off(5).freeze();
                    return Ok(Frame { tag, body });
                }
                self.buf.reserve(total - self.buf.len());
            }
            let read = self
                .io
                .read_buf(&mut self.buf)
                .await
                .map_err(|e| Error::Io(format!("postgres_cdc: read from the server: {e}")))?;
            if read == 0 {
                return Err(Error::Io(
                    "postgres_cdc: the server closed the replication connection".into(),
                ));
            }
        }
    }
}

fn require_password(config: &PgConnectConfig) -> Result<String> {
    config.password.clone().ok_or_else(|| {
        Error::Io(format!(
            "postgres_cdc: the server asked `{}` for a password, and none is configured — set \
             `password_env:` to the name of the environment variable holding it",
            config.user
        ))
    })
}

/// The `SSLRequest` dance, then a rustls handshake over the same socket.
async fn negotiate_tls(
    mut stream: TcpStream,
    config: &PgConnectConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut buf = BytesMut::new();
    frontend::ssl_request(&mut buf);
    stream
        .write_all(&buf)
        .await
        .map_err(|e| Error::Io(format!("postgres_cdc: SSL request: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| Error::Io(format!("postgres_cdc: SSL request: {e}")))?;

    let mut answer = [0u8; 1];
    stream
        .read_exact(&mut answer)
        .await
        .map_err(|e| Error::Io(format!("postgres_cdc: SSL request: {e}")))?;
    if answer[0] != b'S' {
        return Err(Error::Io(format!(
            "postgres_cdc: `tls: {}` was asked for but {}:{} refused TLS — either the server was \
             built without SSL support or `ssl = off` in postgresql.conf",
            config.tls, config.host, config.port
        )));
    }

    let server_name =
        rustls_pki_types::ServerName::try_from(config.host.clone()).map_err(|_| {
            Error::Io(format!(
                "postgres_cdc: `host: {}` is not a valid TLS server name",
                config.host
            ))
        })?;
    tokio_rustls::TlsConnector::from(config.tls_client_config()?)
        .connect(server_name, stream)
        .await
        .map_err(|e| {
            Error::Io(format!(
                "postgres_cdc: TLS handshake with {}:{} (tls={}): {e}",
                config.host, config.port, config.tls
            ))
        })
}

/// `RowDescription`: `Int16 count`, then per field a name and six values this connector ignores
/// (it reads everything as text).
fn row_description(frame: &Frame) -> Result<Vec<String>> {
    let mut r = frame.reader("RowDescription");
    let count = r.i16()?;
    let mut names = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count.max(0) {
        names.push(r.cstr()?);
        r.take(18)?;
    }
    Ok(names)
}

/// `DataRow`: `Int16 count`, then per column a length (`-1` for NULL) and its bytes.
fn data_row(frame: &Frame, sql: &str) -> Result<Vec<Option<String>>> {
    let mut r = frame.reader("DataRow");
    let count = r.i16()?;
    let mut values = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count.max(0) {
        let length = r.i32()?;
        values.push(if length < 0 {
            None
        } else {
            Some(
                String::from_utf8(r.take(length as usize)?.to_vec()).map_err(|e| {
                    Error::Io(format!(
                        "postgres_cdc: {}: non-UTF-8 value: {e}",
                        one_line(sql)
                    ))
                })?,
            )
        });
    }
    Ok(values)
}

/// Turn an `ErrorResponse` into a message that names the SQLSTATE and the server's own hint.
///
/// The hint is the part worth carrying: on a misconfigured server it is usually the exact fix —
/// "You must specify a database name" for a replication session opened without one — and
/// dropping it would leave the operator holding a bare five-character code.
fn server_error(frame: &Frame, context: &str) -> Error {
    let (mut message, mut code, mut hint, mut detail) =
        (String::new(), String::new(), String::new(), String::new());
    let mut r = frame.reader("ErrorResponse");
    while let Ok(kind) = r.u8() {
        if kind == 0 {
            break;
        }
        let Ok(value) = r.cstr() else { break };
        match kind {
            b'M' => message = value,
            b'C' => code = value,
            b'H' => hint = value,
            b'D' => detail = value,
            _ => {}
        }
    }
    let mut text = format!("postgres_cdc: {context}: {message}");
    if !code.is_empty() {
        text.push_str(&format!(" (SQLSTATE {code})"));
    }
    for extra in [detail, hint] {
        if !extra.is_empty() {
            text.push_str(&format!(" — {extra}"));
        }
    }
    Error::Io(text)
}

/// Quote an SQL identifier, doubling embedded quotes.
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote an SQL string literal, doubling embedded quotes.
pub fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frame builder for the constructed-bytes tests below.
    #[derive(Default)]
    struct Frame(Vec<u8>);

    impl Frame {
        fn tag(mut self, tag: u8) -> Self {
            self.0.push(tag);
            self
        }
        fn u8(mut self, v: u8) -> Self {
            self.0.push(v);
            self
        }
        fn i16(mut self, v: i16) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i32(mut self, v: i32) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn u32(mut self, v: u32) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i64(mut self, v: i64) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn lsn(mut self, v: Lsn) -> Self {
            self.0.extend_from_slice(&v.0.to_be_bytes());
            self
        }
        fn cstr(mut self, v: &str) -> Self {
            self.0.extend_from_slice(v.as_bytes());
            self.0.push(0);
            self
        }
        fn text(mut self, v: &str) -> Self {
            self.0.push(b't');
            self.0.extend_from_slice(&(v.len() as i32).to_be_bytes());
            self.0.extend_from_slice(v.as_bytes());
            self
        }
        fn null(mut self) -> Self {
            self.0.push(b'n');
            self
        }
        fn toast(mut self) -> Self {
            self.0.push(b'u');
            self
        }
        fn done(self) -> Vec<u8> {
            self.0
        }
    }

    #[test]
    fn an_lsn_round_trips_through_postgres_spelling() {
        let lsn = Lsn::parse("16/B374D848").unwrap();
        assert_eq!(lsn.0, (0x16u64 << 32) | 0xB374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(Lsn::from_i64(lsn.as_i64()), lsn);
        assert_eq!(Lsn::parse("0/0").unwrap(), Lsn(0));
        assert!(Lsn::parse("nonsense").is_err());
        assert!(Lsn::parse("16/ZZZZ").is_err());
    }

    #[test]
    fn begin_and_commit_carry_the_transaction_boundary() {
        let begin = Frame::default()
            .tag(b'B')
            .lsn(Lsn(0x2A))
            .i64(700_000_000)
            .u32(1234)
            .done();
        assert_eq!(
            decode_logical(&begin).unwrap(),
            LogicalMessage::Begin {
                final_lsn: Lsn(0x2A),
                commit_time: 700_000_000,
                xid: 1234,
            }
        );

        let commit = Frame::default()
            .tag(b'C')
            .u8(0)
            .lsn(Lsn(0x2A))
            .lsn(Lsn(0x2B))
            .i64(700_000_000)
            .done();
        assert_eq!(
            decode_logical(&commit).unwrap(),
            LogicalMessage::Commit {
                flags: 0,
                commit_lsn: Lsn(0x2A),
                end_lsn: Lsn(0x2B),
                commit_time: 700_000_000,
            }
        );
    }

    #[test]
    fn a_relation_message_names_every_column_and_its_type() {
        let relation = Frame::default()
            .tag(b'R')
            .u32(16385)
            .cstr("public")
            .cstr("sales_suppliers")
            .u8(b'd')
            .i16(2)
            .u8(1)
            .cstr("supplierid")
            .u32(20)
            .i32(-1)
            .u8(0)
            .cstr("name")
            .u32(25)
            .i32(-1)
            .done();
        let LogicalMessage::Relation(relation) = decode_logical(&relation).unwrap() else {
            panic!("expected a relation");
        };
        assert_eq!(relation.qualified(), "public.sales_suppliers");
        assert_eq!(relation.replica_identity, b'd');
        assert_eq!(relation.columns.len(), 2);
        assert!(relation.columns[0].key, "the PK column is a key column");
        assert_eq!(relation.columns[0].type_oid, 20);
        assert!(!relation.columns[1].key);
    }

    #[test]
    fn an_empty_relation_namespace_means_pg_catalog() {
        // pgoutput sends "" rather than "pg_catalog"; reading it literally would produce a
        // relation named `.foo` that never matches a configured table.
        let relation = Frame::default()
            .tag(b'R')
            .u32(1)
            .cstr("")
            .cstr("foo")
            .u8(b'd')
            .i16(0)
            .done();
        let LogicalMessage::Relation(relation) = decode_logical(&relation).unwrap() else {
            panic!("expected a relation");
        };
        assert_eq!(relation.qualified(), "pg_catalog.foo");
    }

    #[test]
    fn insert_update_and_delete_decode_their_tuples() {
        let insert = Frame::default()
            .tag(b'I')
            .u32(16385)
            .u8(b'N')
            .i16(2)
            .text("7")
            .text("Acme")
            .done();
        assert_eq!(
            decode_logical(&insert).unwrap(),
            LogicalMessage::Insert {
                relation: 16385,
                new: vec![
                    TupleData::Text(b"7".to_vec()),
                    TupleData::Text(b"Acme".to_vec())
                ],
            }
        );

        // REPLICA IDENTITY DEFAULT: the old image is only the key, and the untouched TOAST
        // column comes back as `u` rather than as a value.
        let update = Frame::default()
            .tag(b'U')
            .u32(16385)
            .u8(b'K')
            .i16(2)
            .text("7")
            .null()
            .u8(b'N')
            .i16(2)
            .text("7")
            .toast()
            .done();
        assert_eq!(
            decode_logical(&update).unwrap(),
            LogicalMessage::Update {
                relation: 16385,
                key: Some(vec![TupleData::Text(b"7".to_vec()), TupleData::Null]),
                old: None,
                new: vec![TupleData::Text(b"7".to_vec()), TupleData::UnchangedToast],
            }
        );

        // REPLICA IDENTITY FULL: `O` carries the whole old row.
        let update_full = Frame::default()
            .tag(b'U')
            .u32(16385)
            .u8(b'O')
            .i16(1)
            .text("old")
            .u8(b'N')
            .i16(1)
            .text("new")
            .done();
        let LogicalMessage::Update { old, key, .. } = decode_logical(&update_full).unwrap() else {
            panic!("expected an update");
        };
        assert_eq!(old, Some(vec![TupleData::Text(b"old".to_vec())]));
        assert_eq!(key, None);

        let delete = Frame::default()
            .tag(b'D')
            .u32(16385)
            .u8(b'K')
            .i16(2)
            .text("7")
            .null()
            .done();
        assert_eq!(
            decode_logical(&delete).unwrap(),
            LogicalMessage::Delete {
                relation: 16385,
                key: Some(vec![TupleData::Text(b"7".to_vec()), TupleData::Null]),
                old: None,
            }
        );
    }

    #[test]
    fn truncate_type_and_origin_decode() {
        let truncate = Frame::default()
            .tag(b'T')
            .i32(2)
            .u8(1)
            .u32(16385)
            .u32(16390)
            .done();
        assert_eq!(
            decode_logical(&truncate).unwrap(),
            LogicalMessage::Truncate {
                options: 1,
                relations: vec![16385, 16390],
            }
        );

        let ty = Frame::default()
            .tag(b'Y')
            .u32(99999)
            .cstr("public")
            .cstr("mood")
            .done();
        assert_eq!(
            decode_logical(&ty).unwrap(),
            LogicalMessage::Type {
                oid: 99999,
                namespace: "public".into(),
                name: "mood".into(),
            }
        );

        let origin = Frame::default().tag(b'O').lsn(Lsn(9)).cstr("node-a").done();
        assert_eq!(
            decode_logical(&origin).unwrap(),
            LogicalMessage::Origin {
                commit_lsn: Lsn(9),
                name: "node-a".into(),
            }
        );
    }

    #[test]
    fn a_truncated_frame_is_an_error_not_a_panic() {
        // A short read on the socket, a protocol version bump, a bug here — all of them arrive
        // as bytes that run out early, and none of them should take the process down.
        let insert = Frame::default()
            .tag(b'I')
            .u32(16385)
            .u8(b'N')
            .i16(2)
            .text("7")
            .done();
        let err = decode_logical(&insert).unwrap_err().to_string();
        assert!(err.contains("truncated"), "got: {err}");

        assert!(decode_logical(&[]).is_err());
        assert!(decode_logical(b"Z")
            .unwrap_err()
            .to_string()
            .contains("`Z`"));
        // A string with no terminator.
        assert!(decode_logical(&Frame::default().tag(b'Y').u32(1).done()).is_err());
    }

    #[test]
    fn xlogdata_and_keepalive_decode_from_the_copy_stream() {
        let payload = Frame::default()
            .tag(b'I')
            .u32(1)
            .u8(b'N')
            .i16(1)
            .text("x")
            .done();
        let mut frame = Frame::default()
            .tag(b'w')
            .lsn(Lsn(0x100))
            .lsn(Lsn(0x200))
            .i64(42)
            .done();
        frame.extend_from_slice(&payload);
        let WireMessage::XLogData {
            wal_start,
            wal_end,
            clock,
            data,
        } = decode_wire(&frame).unwrap()
        else {
            panic!("expected XLogData");
        };
        assert_eq!((wal_start, wal_end, clock), (Lsn(0x100), Lsn(0x200), 42));
        assert_eq!(data, payload);
        assert!(matches!(
            decode_logical(&data).unwrap(),
            LogicalMessage::Insert { .. }
        ));

        let keepalive = Frame::default()
            .tag(b'k')
            .lsn(Lsn(0x300))
            .i64(43)
            .u8(1)
            .done();
        assert_eq!(
            decode_wire(&keepalive).unwrap(),
            WireMessage::Keepalive {
                wal_end: Lsn(0x300),
                clock: 43,
                reply_requested: true,
            }
        );

        assert!(decode_wire(b"q").is_err());
    }

    #[test]
    fn a_standby_status_update_confirms_the_flushed_lsn_only() {
        // The flushed field is what becomes `confirmed_flush_lsn`. Written may run ahead — the
        // bytes are in memory — without granting the server permission to recycle them.
        let bytes = standby_status_update(Lsn(0x200), Lsn(0x100), Lsn(0x100), 7);
        assert_eq!(bytes[0], b'r');
        assert_eq!(u64::from_be_bytes(bytes[1..9].try_into().unwrap()), 0x200);
        assert_eq!(u64::from_be_bytes(bytes[9..17].try_into().unwrap()), 0x100);
        assert_eq!(u64::from_be_bytes(bytes[17..25].try_into().unwrap()), 0x100);
        assert_eq!(i64::from_be_bytes(bytes[25..33].try_into().unwrap()), 7);
        assert_eq!(bytes[33], 0, "never ask the server for a reply");
    }

    #[test]
    fn tls_modes_parse_the_documented_spellings() {
        assert_eq!(TlsMode::parse("disable").unwrap(), TlsMode::Disable);
        assert_eq!(TlsMode::parse(" Require ").unwrap(), TlsMode::Require);
        assert_eq!(TlsMode::parse("verify-ca").unwrap(), TlsMode::VerifyCa);
        assert_eq!(TlsMode::parse("verify_full").unwrap(), TlsMode::VerifyFull);
        assert!(!TlsMode::Disable.enabled());
        assert!(TlsMode::Require.enabled());
        let err = TlsMode::parse("sslmode=prefer").unwrap_err().to_string();
        assert!(err.contains("verify-full"), "got: {err}");
    }

    #[test]
    fn identifiers_and_literals_are_quoted_against_injection() {
        assert_eq!(quote_identifier("slot"), "\"slot\"");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("pub"), "'pub'");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn postgres_timestamps_are_offset_from_the_year_2000() {
        // 2000-01-01T00:00:00Z in Postgres' epoch is zero; getting this wrong shifts every
        // `__oxidant_ts` by thirty years, which is the sort of error a test catches and a
        // dashboard does not.
        let unix = chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        assert_eq!(unix, PG_EPOCH_UNIX_MICROS);
    }
}
