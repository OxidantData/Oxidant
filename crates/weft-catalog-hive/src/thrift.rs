//! A minimal Thrift **binary protocol** client over a raw TCP socket — just enough to call the
//! three Hive Metastore RPCs Weft needs (`get_all_databases`, `get_all_tables`, `get_table`).
//!
//! Hive Metastore's standalone Thrift server speaks `TBinaryProtocol` over an *unframed* transport
//! by default (`hive.metastore.thrift.framed.transport.enabled=false`). The binary protocol is
//! self-delimiting — structs are read field-by-field until a STOP marker — so no message framing is
//! needed: we write a request struct and read the reply struct straight off the stream. Fields we
//! don't care about are skipped generically, so this stays robust across Metastore versions that
//! add fields.

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use weft_catalog::{Error, Result};

// Thrift type ids (binary protocol).
const T_STOP: u8 = 0;
const T_BOOL: u8 = 2;
const T_BYTE: u8 = 3;
const T_DOUBLE: u8 = 4;
const T_I16: u8 = 6;
const T_I32: u8 = 8;
const T_I64: u8 = 10;
const T_STRING: u8 = 11;
const T_STRUCT: u8 = 12;
const T_MAP: u8 = 13;
const T_SET: u8 = 14;
const T_LIST: u8 = 15;

// Message types.
const M_CALL: u8 = 1;
const M_EXCEPTION: u8 = 3;
/// Strict-protocol version marker OR'd into the first i32 of a message.
const VERSION_1: u32 = 0x8001_0000;

/// One Hive table's read-relevant metadata, parsed selectively from the Thrift `Table` struct.
#[derive(Debug, Default, Clone)]
pub struct HiveTable {
    /// `sd.location` — the storage URI of the table root.
    pub location: Option<String>,
    /// `sd.inputFormat` — used to infer the file format (Parquet/ORC/text…).
    pub input_format: Option<String>,
    /// `sd.serdeInfo.serializationLib` — a second format hint.
    pub serde_lib: Option<String>,
    /// `tableType` (e.g. `EXTERNAL_TABLE`, `MANAGED_TABLE`, `VIRTUAL_VIEW`).
    pub table_type: Option<String>,
    /// `parameters` — table properties; `table_type=ICEBERG` / `spark.sql.sources.provider=delta`
    /// here are the most reliable format signals. Hive Metastore has no dedicated table-level
    /// comment field on the `Table` struct itself — `COMMENT '...'` on `CREATE TABLE` is stored
    /// as the `comment` key in this same map, hence [`HiveTable::comment`] below.
    pub parameters: Vec<(String, String)>,
    /// `sd.cols` — the data columns as `(name, hive_type_string)`, in declaration order. The
    /// catalog-declared schema the engine coerces files to.
    pub columns: Vec<(String, String)>,
    /// `partitionKeys` as `(name, hive_type_string)`, in declaration order. The schema appends
    /// these after the data columns (matching how Hive lays partitioned tables out on disk).
    pub partition_typed: Vec<(String, String)>,
    /// `partitionKeys` column *names* (kept for `TableMetadata::with_partition_columns`).
    pub partition_columns: Vec<String>,
}

impl HiveTable {
    /// Look up a table parameter (case-sensitive key).
    pub fn param(&self, key: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// The table-level comment, i.e. the `parameters["comment"]` entry Hive Metastore stores
    /// `CREATE TABLE ... COMMENT '...'` under (there is no separate Thrift field for it).
    pub fn comment(&self) -> Option<&str> {
        self.param("comment")
    }
}

/// A connection to a Hive Metastore Thrift endpoint. One request/response per method call.
pub struct MetastoreClient {
    stream: BufReader<TcpStream>,
    seq: i32,
}

impl MetastoreClient {
    /// Connect to `host:port`.
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let stream = TcpStream::connect((host, port))
            .await
            .map_err(|e| Error::Io(format!("connect to Hive Metastore {host}:{port}: {e}")))?;
        Ok(Self {
            stream: BufReader::new(stream),
            seq: 0,
        })
    }

    /// `get_all_databases()` → all database (namespace) names.
    pub async fn get_all_databases(&mut self) -> Result<Vec<String>> {
        let mut args = Vec::new();
        write_stop(&mut args); // no fields
        self.call("get_all_databases", &args).await?;
        self.read_string_list_result().await
    }

    /// `get_all_tables(db_name)` → table names in `db_name`.
    pub async fn get_all_tables(&mut self, db_name: &str) -> Result<Vec<String>> {
        let mut args = Vec::new();
        write_field(&mut args, T_STRING, 1);
        write_string(&mut args, db_name);
        write_stop(&mut args);
        self.call("get_all_tables", &args).await?;
        self.read_string_list_result().await
    }

    /// `get_table(dbname, tbl_name)` → the table, or a not-found error.
    pub async fn get_table(&mut self, db_name: &str, table: &str) -> Result<HiveTable> {
        let mut args = Vec::new();
        write_field(&mut args, T_STRING, 1);
        write_string(&mut args, db_name);
        write_field(&mut args, T_STRING, 2);
        write_string(&mut args, table);
        write_stop(&mut args);
        self.call("get_table", &args).await?;
        self.read_table_result(db_name, table).await
    }

    /// `create_table(tbl)` — declare a new external table. Returns `Ok(())` on Hive Metastore's
    /// `void` success reply; a declared exception (`AlreadyExistsException`, `InvalidObjectException`,
    /// `MetaException`, `NoSuchObjectException` — all delivered as a struct field in the result, not
    /// a Thrift-protocol-level exception) surfaces as `Error::Io` with its message.
    pub async fn create_table(&mut self, tbl: &NewHiveTable) -> Result<()> {
        let mut args = Vec::new();
        write_field(&mut args, T_STRUCT, 1); // the single `Table tbl` arg
        write_new_table(&mut args, tbl);
        write_stop(&mut args);
        self.call("create_table", &args).await?;
        self.read_void_result("create_table").await
    }

    /// Read a `void`-result reply: success is an empty struct (just `T_STOP`); any field present is
    /// one of the RPC's declared exceptions.
    async fn read_void_result(&mut self, method: &str) -> Result<()> {
        loop {
            let (ftype, _fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                return Ok(());
            }
            if ftype == T_STRUCT {
                let msg = self.read_struct_message().await?;
                return Err(Error::Io(format!("Hive Metastore {method} failed: {msg}")));
            }
            self.skip(ftype).await?;
        }
    }

    /// Frame and send a method call, then read + validate the reply envelope.
    async fn call(&mut self, method: &str, args: &[u8]) -> Result<()> {
        self.seq = self.seq.wrapping_add(1);
        let mut msg = Vec::new();
        write_i32(&mut msg, (VERSION_1 | M_CALL as u32) as i32);
        write_string(&mut msg, method);
        write_i32(&mut msg, self.seq);
        msg.extend_from_slice(args);
        self.stream
            .get_mut()
            .write_all(&msg)
            .await
            .map_err(|e| Error::Io(format!("send {method}: {e}")))?;
        self.read_message_begin(method).await
    }

    /// Read a reply envelope; turn a Thrift application EXCEPTION into an error.
    async fn read_message_begin(&mut self, method: &str) -> Result<()> {
        let header = self.r_i32().await?;
        let mtype = (header & 0xff) as u8; // strict protocol: type in the low byte
        let _name = self.r_string().await?;
        let _seqid = self.r_i32().await?;
        if mtype == M_EXCEPTION {
            let msg = self.read_app_exception().await?;
            return Err(Error::Io(format!("Hive Metastore {method} failed: {msg}")));
        }
        Ok(())
    }

    /// Read a `TApplicationException` struct, returning its message.
    async fn read_app_exception(&mut self) -> Result<String> {
        let mut message = String::new();
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                (1, T_STRING) => message = self.r_string().await?,
                _ => self.skip(ftype).await?,
            }
        }
        Ok(if message.is_empty() {
            "application exception".to_string()
        } else {
            message
        })
    }

    /// Read a result whose success value (field 0) is a `list<string>`.
    async fn read_string_list_result(&mut self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                (0, T_LIST) => out = self.read_string_list().await?,
                // A declared exception field (e.g. MetaException) — surface its message.
                (_, T_STRUCT) => {
                    let msg = self.read_struct_message().await?;
                    return Err(Error::Io(format!("Hive Metastore error: {msg}")));
                }
                _ => self.skip(ftype).await?,
            }
        }
        Ok(out)
    }

    /// Read a `get_table` result: field 0 is the `Table` struct; fields 1/2 are exceptions.
    async fn read_table_result(&mut self, db: &str, table: &str) -> Result<HiveTable> {
        let mut found: Option<HiveTable> = None;
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                (0, T_STRUCT) => found = Some(self.read_table_struct().await?),
                // NoSuchObjectException / MetaException → a clean "not found" (Plan) error so the
                // bridge maps it to DataFusion's table-not-found path.
                (_, T_STRUCT) => {
                    let _ = self.read_struct_message().await?;
                    return Err(Error::Plan(format!("no such table: {db}.{table}")));
                }
                _ => self.skip(ftype).await?,
            }
        }
        found.ok_or_else(|| Error::Plan(format!("no such table: {db}.{table}")))
    }

    /// Parse the Hive `Table` struct, keeping only read-relevant fields.
    async fn read_table_struct(&mut self) -> Result<HiveTable> {
        let mut t = HiveTable::default();
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                // 7: StorageDescriptor sd
                (7, T_STRUCT) => self.read_storage_descriptor(&mut t).await?,
                // 8: list<FieldSchema> partitionKeys — keep both typed pairs and bare names.
                (8, T_LIST) => {
                    let parts = self.read_field_schemas().await?;
                    t.partition_columns = parts.iter().map(|(n, _)| n.clone()).collect();
                    t.partition_typed = parts;
                }
                // 9: map<string,string> parameters
                (9, T_MAP) => t.parameters = self.read_string_map().await?,
                // 12: string tableType
                (12, T_STRING) => t.table_type = Some(self.r_string().await?),
                _ => self.skip(ftype).await?,
            }
        }
        Ok(t)
    }

    /// Parse `StorageDescriptor`, keeping location, inputFormat, and serde lib.
    async fn read_storage_descriptor(&mut self, t: &mut HiveTable) -> Result<()> {
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                // 1: list<FieldSchema> cols — the data columns (name + type).
                (1, T_LIST) => t.columns = self.read_field_schemas().await?,
                // 2: string location
                (2, T_STRING) => t.location = Some(self.r_string().await?),
                // 3: string inputFormat
                (3, T_STRING) => t.input_format = Some(self.r_string().await?),
                // 7: SerDeInfo serdeInfo → field 2: serializationLib
                (7, T_STRUCT) => t.serde_lib = self.read_serde_lib().await?,
                _ => self.skip(ftype).await?,
            }
        }
        Ok(())
    }

    /// Parse `SerDeInfo`, returning `serializationLib` (field 2).
    async fn read_serde_lib(&mut self) -> Result<Option<String>> {
        let mut lib = None;
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                (2, T_STRING) => lib = Some(self.r_string().await?),
                _ => self.skip(ftype).await?,
            }
        }
        Ok(lib)
    }

    /// Read `list<FieldSchema>` and return each schema's `(name, type)` — FieldSchema field 1
    /// (`name`, T_STRING) and field 2 (`type`, the Hive type string, T_STRING). Other FieldSchema
    /// fields (e.g. field 3 `comment`) are skipped.
    async fn read_field_schemas(&mut self) -> Result<Vec<(String, String)>> {
        let (elem_type, size) = self.read_list_header().await?;
        let mut cols = Vec::with_capacity(size.max(0) as usize);
        for _ in 0..size.max(0) {
            if elem_type != T_STRUCT {
                self.skip(elem_type).await?;
                continue;
            }
            let mut name = String::new();
            let mut ty = String::new();
            loop {
                let (ftype, fid) = self.read_field_header().await?;
                if ftype == T_STOP {
                    break;
                }
                match (fid, ftype) {
                    (1, T_STRING) => name = self.r_string().await?,
                    (2, T_STRING) => ty = self.r_string().await?,
                    _ => self.skip(ftype).await?,
                }
            }
            cols.push((name, ty));
        }
        Ok(cols)
    }

    /// Read the first string field of a struct (e.g. an exception's `message`), skipping the rest.
    async fn read_struct_message(&mut self) -> Result<String> {
        let mut message = String::new();
        loop {
            let (ftype, fid) = self.read_field_header().await?;
            if ftype == T_STOP {
                break;
            }
            match (fid, ftype) {
                (1, T_STRING) if message.is_empty() => message = self.r_string().await?,
                _ => self.skip(ftype).await?,
            }
        }
        Ok(message)
    }

    /// Read a `list<string>` value (the list header has already not been consumed).
    async fn read_string_list(&mut self) -> Result<Vec<String>> {
        let (elem_type, size) = self.read_list_header().await?;
        let mut out = Vec::with_capacity(size.max(0) as usize);
        for _ in 0..size.max(0) {
            if elem_type == T_STRING {
                out.push(self.r_string().await?);
            } else {
                self.skip(elem_type).await?;
            }
        }
        Ok(out)
    }

    /// Read a `map<string,string>` value into ordered pairs.
    async fn read_string_map(&mut self) -> Result<Vec<(String, String)>> {
        let key_type = self.r_byte().await?;
        let val_type = self.r_byte().await?;
        let size = self.r_i32().await?;
        let mut out = Vec::with_capacity(size.max(0) as usize);
        for _ in 0..size.max(0) {
            let k = if key_type == T_STRING {
                self.r_string().await?
            } else {
                self.skip(key_type).await?;
                String::new()
            };
            let v = if val_type == T_STRING {
                self.r_string().await?
            } else {
                self.skip(val_type).await?;
                String::new()
            };
            out.push((k, v));
        }
        Ok(out)
    }

    // ---- primitive readers -------------------------------------------------

    async fn read_field_header(&mut self) -> Result<(u8, i16)> {
        let ftype = self.r_byte().await?;
        if ftype == T_STOP {
            return Ok((T_STOP, 0));
        }
        let id = self.r_i16().await?;
        Ok((ftype, id))
    }

    async fn read_list_header(&mut self) -> Result<(u8, i32)> {
        let elem_type = self.r_byte().await?;
        let size = self.r_i32().await?;
        Ok((elem_type, size))
    }

    /// Skip a value of the given Thrift type (for fields we don't read).
    async fn skip(&mut self, ftype: u8) -> Result<()> {
        // Recursion across an async boundary needs boxing.
        Box::pin(self.skip_inner(ftype)).await
    }

    async fn skip_inner(&mut self, ftype: u8) -> Result<()> {
        match ftype {
            T_BOOL | T_BYTE => {
                self.r_byte().await?;
            }
            T_I16 => {
                self.r_i16().await?;
            }
            T_I32 => {
                self.r_i32().await?;
            }
            T_I64 | T_DOUBLE => {
                self.read_n(8).await?;
            }
            T_STRING => {
                let len = self.r_i32().await?.max(0) as usize;
                self.read_n(len).await?;
            }
            T_STRUCT => loop {
                let (ft, _) = self.read_field_header().await?;
                if ft == T_STOP {
                    break;
                }
                self.skip(ft).await?;
            },
            T_MAP => {
                let kt = self.r_byte().await?;
                let vt = self.r_byte().await?;
                let n = self.r_i32().await?.max(0);
                for _ in 0..n {
                    self.skip(kt).await?;
                    self.skip(vt).await?;
                }
            }
            T_LIST | T_SET => {
                let (et, n) = self.read_list_header().await?;
                for _ in 0..n.max(0) {
                    self.skip(et).await?;
                }
            }
            other => return Err(Error::Io(format!("unknown thrift type id {other}"))),
        }
        Ok(())
    }

    async fn read_n(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        self.stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| Error::Io(format!("read {n} bytes: {e}")))?;
        Ok(buf)
    }

    async fn r_byte(&mut self) -> Result<u8> {
        self.stream
            .read_u8()
            .await
            .map_err(|e| Error::Io(format!("read byte: {e}")))
    }

    async fn r_i16(&mut self) -> Result<i16> {
        self.stream
            .read_i16()
            .await
            .map_err(|e| Error::Io(format!("read i16: {e}")))
    }

    async fn r_i32(&mut self) -> Result<i32> {
        self.stream
            .read_i32()
            .await
            .map_err(|e| Error::Io(format!("read i32: {e}")))
    }

    async fn r_string(&mut self) -> Result<String> {
        let len = self.r_i32().await?.max(0) as usize;
        let bytes = self.read_n(len).await?;
        String::from_utf8(bytes).map_err(|e| Error::Io(format!("utf8: {e}")))
    }
}

// ---- writers (into a buffer) ----------------------------------------------

fn write_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn write_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_i32(buf, s.len() as i32);
    buf.extend_from_slice(s.as_bytes());
}
fn write_field(buf: &mut Vec<u8>, ftype: u8, id: i16) {
    buf.push(ftype);
    write_i16(buf, id);
}
fn write_stop(buf: &mut Vec<u8>) {
    buf.push(T_STOP);
}
/// Write a `list<T>` header (element type + count); caller writes each element after.
fn write_list_header(buf: &mut Vec<u8>, elem_type: u8, count: usize) {
    buf.push(elem_type);
    write_i32(buf, count as i32);
}
/// Write one `FieldSchema` struct: `name` (field 1) + `type` (field 2), no `comment` (field 3,
/// optional — Hive doesn't require it).
fn write_field_schema(buf: &mut Vec<u8>, name: &str, ty: &str) {
    write_field(buf, T_STRING, 1);
    write_string(buf, name);
    write_field(buf, T_STRING, 2);
    write_string(buf, ty);
    write_stop(buf);
}
/// Write a `map<string,string>` value (key/value type bytes, count, then each pair).
fn write_string_map(buf: &mut Vec<u8>, pairs: &[(String, String)]) {
    buf.push(T_STRING);
    buf.push(T_STRING);
    write_i32(buf, pairs.len() as i32);
    for (k, v) in pairs {
        write_string(buf, k);
        write_string(buf, v);
    }
}

/// Everything needed to declare a new Hive table via `create_table` — built by
/// `weft-catalog-hive`'s `HiveCatalog::create_table` from the CTAS result's schema (via
/// `weft_catalog::hive_types::schema_to_columns`/`format_serde`).
pub struct NewHiveTable {
    pub db_name: String,
    pub table_name: String,
    pub location: String,
    pub input_format: &'static str,
    pub output_format: &'static str,
    pub serde_lib: &'static str,
    pub serde_params: Vec<(String, String)>,
    /// Data columns as `(name, hive_type_string)`, in declaration order.
    pub columns: Vec<(String, String)>,
    /// Partition columns as `(name, hive_type_string)`, in declaration order.
    pub partition_columns: Vec<(String, String)>,
}

/// Write the Thrift `Table` struct (Hive Metastore's `create_table` single arg, field id 1):
/// `tableName` (1), `dbName` (2), `sd` (7, a `StorageDescriptor`), `partitionKeys` (8),
/// `parameters` (9, empty), `tableType` (12, `EXTERNAL_TABLE` — Weft never owns/deletes the data).
fn write_new_table(buf: &mut Vec<u8>, tbl: &NewHiveTable) {
    write_field(buf, T_STRING, 1); // tableName
    write_string(buf, &tbl.table_name);
    write_field(buf, T_STRING, 2); // dbName
    write_string(buf, &tbl.db_name);

    write_field(buf, T_STRUCT, 7); // sd: StorageDescriptor
    write_field(buf, T_LIST, 1); // cols
    write_list_header(buf, T_STRUCT, tbl.columns.len());
    for (name, ty) in &tbl.columns {
        write_field_schema(buf, name, ty);
    }
    write_field(buf, T_STRING, 2); // location
    write_string(buf, &tbl.location);
    write_field(buf, T_STRING, 3); // inputFormat
    write_string(buf, tbl.input_format);
    write_field(buf, T_STRING, 4); // outputFormat
    write_string(buf, tbl.output_format);
    write_field(buf, T_STRUCT, 7); // serdeInfo: SerDeInfo
    write_field(buf, T_STRING, 2); // serializationLib
    write_string(buf, tbl.serde_lib);
    write_field(buf, T_MAP, 3); // parameters
    write_string_map(buf, &tbl.serde_params);
    write_stop(buf); // end SerDeInfo
    write_stop(buf); // end StorageDescriptor

    write_field(buf, T_LIST, 8); // partitionKeys
    write_list_header(buf, T_STRUCT, tbl.partition_columns.len());
    for (name, ty) in &tbl.partition_columns {
        write_field_schema(buf, name, ty);
    }
    write_field(buf, T_MAP, 9); // parameters (table-level, empty)
    write_string_map(buf, &[]);
    write_field(buf, T_STRING, 12); // tableType
    write_string(buf, "EXTERNAL_TABLE");
    write_stop(buf); // end Table
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Encode a Thrift `Table` reply (envelope + result struct) the way the Metastore would, so we
    /// can exercise the parser end-to-end without a live server.
    fn encode_table_reply(seq: i32, location: &str, input_format: &str, iceberg: bool) -> Vec<u8> {
        let mut sd = Vec::new();
        write_field(&mut sd, T_STRING, 2); // location
        write_string(&mut sd, location);
        write_field(&mut sd, T_STRING, 3); // inputFormat
        write_string(&mut sd, input_format);
        // serdeInfo (struct) with serializationLib (field 2)
        write_field(&mut sd, T_STRUCT, 7);
        write_field(&mut sd, T_STRING, 2);
        write_string(
            &mut sd,
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        );
        write_stop(&mut sd);
        write_stop(&mut sd);

        let mut table = Vec::new();
        write_field(&mut table, T_STRING, 1); // tableName
        write_string(&mut table, "orders");
        write_field(&mut table, T_STRUCT, 7); // sd
        table.extend_from_slice(&sd);
        // parameters map (field 9)
        write_field(&mut table, T_MAP, 9);
        table.push(T_STRING);
        table.push(T_STRING);
        if iceberg {
            write_i32(&mut table, 1);
            write_string(&mut table, "table_type");
            write_string(&mut table, "ICEBERG");
        } else {
            write_i32(&mut table, 0);
        }
        write_field(&mut table, T_STRING, 12); // tableType
        write_string(&mut table, "EXTERNAL_TABLE");
        write_stop(&mut table);

        let mut result = Vec::new();
        write_field(&mut result, T_STRUCT, 0); // success
        result.extend_from_slice(&table);
        write_stop(&mut result);

        let mut msg = Vec::new();
        write_i32(&mut msg, (VERSION_1 | 2u32) as i32); // REPLY
        write_string(&mut msg, "get_table");
        write_i32(&mut msg, seq);
        msg.extend_from_slice(&result);
        msg
    }

    #[tokio::test]
    async fn parses_get_table_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = encode_table_reply(
            1,
            "file:///wh/db.db/orders",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            false,
        );
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Drain (part of) the request — we don't validate it here — then reply.
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });

        let mut client = MetastoreClient::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        let t = client.get_table("db", "orders").await.unwrap();
        assert_eq!(t.location.as_deref(), Some("file:///wh/db.db/orders"));
        assert!(t
            .input_format
            .as_deref()
            .unwrap()
            .contains("ParquetInputFormat"));
        assert!(t.serde_lib.as_deref().unwrap().contains("ParquetHiveSerDe"));
        assert_eq!(t.table_type.as_deref(), Some("EXTERNAL_TABLE"));
    }

    /// Encode one `FieldSchema` struct: name (field 1) + type (field 2).
    fn write_field_schema(buf: &mut Vec<u8>, name: &str, ty: &str) {
        write_field(buf, T_STRING, 1);
        write_string(buf, name);
        write_field(buf, T_STRING, 2);
        write_string(buf, ty);
        // A comment field (3) we don't read — proves field-skipping still works.
        write_field(buf, T_STRING, 3);
        write_string(buf, "a comment");
        write_stop(buf);
    }

    /// Encode a `Table` reply whose sd carries a `cols` list (field 1) of FieldSchemas, plus
    /// partitionKeys (Table field 8). Exercises the column/type capture path.
    fn encode_table_with_columns(seq: i32) -> Vec<u8> {
        let mut sd = Vec::new();
        // field 1: list<FieldSchema> cols
        write_field(&mut sd, T_LIST, 1);
        sd.push(T_STRUCT);
        write_i32(&mut sd, 2); // two data columns
        write_field_schema(&mut sd, "vendor_id", "int");
        write_field_schema(&mut sd, "fare", "bigint");
        // field 2: location
        write_field(&mut sd, T_STRING, 2);
        write_string(&mut sd, "file:///wh/db.db/trips");
        write_field(&mut sd, T_STRING, 3); // inputFormat
        write_string(
            &mut sd,
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        );
        write_stop(&mut sd);

        let mut table = Vec::new();
        write_field(&mut table, T_STRING, 1); // tableName
        write_string(&mut table, "trips");
        write_field(&mut table, T_STRUCT, 7); // sd
        table.extend_from_slice(&sd);
        // field 8: list<FieldSchema> partitionKeys (one column)
        write_field(&mut table, T_LIST, 8);
        table.push(T_STRUCT);
        write_i32(&mut table, 1);
        write_field_schema(&mut table, "month", "string");
        write_field(&mut table, T_STRING, 12); // tableType
        write_string(&mut table, "EXTERNAL_TABLE");
        write_stop(&mut table);

        let mut result = Vec::new();
        write_field(&mut result, T_STRUCT, 0); // success
        result.extend_from_slice(&table);
        write_stop(&mut result);

        let mut msg = Vec::new();
        write_i32(&mut msg, (VERSION_1 | 2u32) as i32); // REPLY
        write_string(&mut msg, "get_table");
        write_i32(&mut msg, seq);
        msg.extend_from_slice(&result);
        msg
    }

    #[tokio::test]
    async fn parses_columns_with_types() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = encode_table_with_columns(1);
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });

        let mut client = MetastoreClient::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        let t = client.get_table("db", "trips").await.unwrap();
        // Data columns captured in order, with types.
        assert_eq!(
            t.columns,
            vec![
                ("vendor_id".to_string(), "int".to_string()),
                ("fare".to_string(), "bigint".to_string()),
            ]
        );
        // Partition key captured both typed and as a bare name (the latter preserved for
        // `with_partition_columns`).
        assert_eq!(
            t.partition_typed,
            vec![("month".to_string(), "string".to_string())]
        );
        assert_eq!(t.partition_columns, vec!["month".to_string()]);
        assert_eq!(t.location.as_deref(), Some("file:///wh/db.db/trips"));
    }

    #[tokio::test]
    async fn parses_iceberg_table_param() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = encode_table_reply(1, "file:///wh/ice", "n/a", true);
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut scratch = [0u8; 1024];
            let _ = sock.try_read(&mut scratch);
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        let mut client = MetastoreClient::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        let t = client.get_table("db", "ice").await.unwrap();
        assert_eq!(t.param("table_type"), Some("ICEBERG"));
    }

    // `write_new_table`/`create_table` back `HiveCatalog::create_table` (CTAS write support).

    fn sample_new_table() -> NewHiveTable {
        NewHiveTable {
            db_name: "db".to_string(),
            table_name: "orders".to_string(),
            location: "s3://bucket/db/orders/".to_string(),
            input_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serde_lib: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            serde_params: vec![],
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("name".to_string(), "string".to_string()),
            ],
            partition_columns: vec![("dt".to_string(), "string".to_string())],
        }
    }

    #[test]
    fn write_new_table_encodes_expected_fields() {
        let tbl = sample_new_table();
        let mut buf = Vec::new();
        write_new_table(&mut buf, &tbl);
        // Thrift strings are length-prefixed raw UTF-8 bytes, so each value's bytes appear
        // contiguously in the buffer — a substring check is a reliable, low-effort way to confirm
        // every field made it into the wire encoding without hand-rolling a full struct decoder.
        let s = String::from_utf8_lossy(&buf);
        for expect in [
            "orders",
            "db",
            "s3://bucket/db/orders/",
            "id",
            "bigint",
            "name",
            "string",
            "dt",
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "EXTERNAL_TABLE",
        ] {
            assert!(
                s.contains(expect),
                "expected `{expect}` in encoded Table struct"
            );
        }
    }

    /// Encode a Thrift `void`-success reply envelope (empty result struct).
    fn encode_void_reply(seq: i32) -> Vec<u8> {
        let mut msg = Vec::new();
        write_i32(&mut msg, (VERSION_1 | 2u32) as i32); // REPLY
        write_string(&mut msg, "create_table");
        write_i32(&mut msg, seq);
        write_stop(&mut msg); // empty result struct = void success
        msg
    }

    /// Encode a Thrift reply carrying a declared exception (e.g. `AlreadyExistsException`) in the
    /// result struct's field 1, with a `message` (field 1 of the exception struct).
    fn encode_exception_reply(seq: i32, message: &str) -> Vec<u8> {
        let mut exc = Vec::new();
        write_field(&mut exc, T_STRING, 1);
        write_string(&mut exc, message);
        write_stop(&mut exc);

        let mut result = Vec::new();
        write_field(&mut result, T_STRUCT, 1); // o1: AlreadyExistsException
        result.extend_from_slice(&exc);
        write_stop(&mut result);

        let mut msg = Vec::new();
        write_i32(&mut msg, (VERSION_1 | 2u32) as i32); // REPLY
        write_string(&mut msg, "create_table");
        write_i32(&mut msg, seq);
        msg.extend_from_slice(&result);
        msg
    }

    #[tokio::test]
    async fn create_table_succeeds_on_void_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = encode_void_reply(1);
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut scratch = [0u8; 4096];
            let _ = sock.read(&mut scratch).await;
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        let mut client = MetastoreClient::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        client.create_table(&sample_new_table()).await.unwrap();
    }

    #[tokio::test]
    async fn create_table_surfaces_declared_exception() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = encode_exception_reply(1, "Table orders already exists");
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut scratch = [0u8; 4096];
            let _ = sock.read(&mut scratch).await;
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        let mut client = MetastoreClient::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        let err = client.create_table(&sample_new_table()).await.unwrap_err();
        match err {
            Error::Io(msg) => assert!(msg.contains("already exists")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }
}
