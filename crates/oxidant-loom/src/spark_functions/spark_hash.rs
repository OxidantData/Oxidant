//! Spark's cryptographic-digest functions.
//!
//! `docs/databricks-coverage.md` used to describe this category as "alias work, not new
//! implementation". That is wrong, and it is worth recording why: DataFusion 54's
//! `DigestAlgorithm` (`datafusion-functions-54.1.0/src/crypto/basic.rs`) offers exactly
//! `Md5, Sha224, Sha256, Sha384, Sha512, Blake2s, Blake2b, Blake3` — **no SHA-1 and no CRC-32**.
//! So `sha`/`sha1`/`crc32` have no built-in to alias onto, and `sha2` needs a dispatch on its
//! bit-length argument that no single built-in provides.
//!
//! Functions:
//! - `sha(expr)` / `sha1(expr)` — SHA-1 as a 40-character lowercase hex `string`. Two Spark
//!   spellings of one function.
//! - `sha2(expr, numBits)` — SHA-2 as lowercase hex. `numBits` selects the variant: `224`, `256`,
//!   `384`, `512`, and `0` meaning 256. Any other width returns **NULL**, which is Spark's
//!   documented behaviour ("if numBits is not one of the permitted values, the result is NULL"),
//!   not an error.
//! - `crc32(expr)` — CRC-32 (IEEE 802.3, the zlib polynomial) as a `bigint`. Spark widens the
//!   unsigned 32-bit checksum into a signed 64-bit value, so it is never negative.
//!
//! All three hash the argument's **bytes**: a `STRING` contributes its UTF-8 encoding and a
//! `BINARY` its raw bytes, so `sha1('abc')` and `sha1(CAST('abc' AS BINARY))` agree. NULL in,
//! NULL out. There is no third case — a numeric argument is **rejected at planning time**, as
//! Spark rejects it, because casting one to binary would silently hash its raw little-endian
//! bytes (`crc32(123)` hashing `[123,0,0,0]`) and no caller could tell that from a real digest.
//!
//! ## Not implemented here, deliberately
//!
//! `hash(expr, …)` and `xxhash64(expr, …)` are *not* in this file. They are not digests over the
//! argument's bytes — they hash Spark's **internal row representation**, recursing through structs
//! and arrays with a per-type encoding, and Spark's Murmur3 further uses a non-standard tail rule
//! (`Murmur3_x86_32.hashUnsafeBytes` mixes each leftover byte as its own 4-byte block, so an
//! off-the-shelf MurmurHash3 crate produces different values). Porting them faithfully needs
//! byte-level verification against a real Spark, which this file cannot provide, and shipping
//! unverified hash values would be worse than leaving the functions missing.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, Int64Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the digest functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Sha1Udf::new("sha")));
    ctx.register_udf(ScalarUDF::from(Sha1Udf::new("sha1")));
    ctx.register_udf(ScalarUDF::from(Sha2Udf::new()));
    ctx.register_udf(ScalarUDF::from(Crc32Udf::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// Accept only what Spark's digest functions accept.
///
/// Spark's `Sha1`/`Sha2`/`Crc32` declare `BinaryType` and implicitly accept `STRING`; an INT is
/// **not** castable to binary, so Spark raises at analysis time. Leaving the argument open was a
/// silent-wrong-answer trap: [`to_bytes`] would cast an INT to `Binary`, hashing its four raw
/// little-endian bytes, so `crc32(123)` returned the CRC of `[123,0,0,0]` rather than either
/// erroring or hashing `"123"`. A caller cannot tell that apart from a correct answer.
fn coerce_hashable(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    let Some(a) = arg_types.first() else {
        return plan_err!("{name} expects at least 1 argument");
    };
    match a {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Null => Ok(a.clone()),
        other => plan_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] the first parameter of `{name}` requires \
             the BINARY or STRING type, however it has the type {other}"
        ),
    }
}

/// View any argument as raw bytes: a string contributes its UTF-8 encoding, binary its own bytes.
fn to_bytes(v: &ColumnarValue, n: usize) -> Result<BinaryArray> {
    let arr = v.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&arr, &DataType::Binary)
        .map_err(arrow_err)?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("cast to Binary yields BinaryArray")
        .clone())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // `write!` into a String is infallible; a manual nibble table keeps this allocation-free
        // per byte and avoids a `hex` dependency for four call sites.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

// ---------------------------------------------------------------------------
// sha / sha1
// ---------------------------------------------------------------------------

/// `sha(expr)` / `sha1(expr)` — SHA-1 as lowercase hex.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Sha1Udf {
    name: &'static str,
    signature: Signature,
}

impl Sha1Udf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            // `user_defined` so `coerce_types` can reject numerics — see `coerce_hashable`.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sha1Udf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![coerce_hashable(self.name, arg_types)?])
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use sha1::{Digest, Sha1};
        let n = args.number_rows;
        let bytes = to_bytes(&args.args[0], n)?;
        let mut out = StringBuilder::new();
        for i in 0..n {
            if bytes.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_hex(&Sha1::digest(bytes.value(i))));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// sha2
// ---------------------------------------------------------------------------

/// `sha2(expr, numBits)` — SHA-2 as lowercase hex, variant chosen by `numBits`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Sha2Udf {
    signature: Signature,
}

impl Sha2Udf {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sha2Udf {
    fn name(&self) -> &str {
        "sha2"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, bits] = arg_types else {
            return plan_err!("sha2 expects exactly 2 arguments");
        };
        // Only the hashed value is type-restricted; `numBits` keeps its own coercion to int,
        // which is what the `sha2(a, a)` CAST_INVALID_INPUT golden exercises.
        Ok(vec![coerce_hashable("sha2", arg_types)?, bits.clone()])
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
        let n = args.number_rows;
        let bytes = to_bytes(&args.args[0], n)?;

        // Spark coerces `numBits` to int, and a non-numeric string is a cast error
        // (`CAST_INVALID_INPUT`), which is what the `select sha2(a, a) from t` golden in
        // `typeCoercion/native/stringCastAndExpressions.sql.out` pins.
        let bits_in = args.args[1].clone().into_array(n)?;
        let cast_opts = datafusion::arrow::compute::CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let bits =
            datafusion::arrow::compute::cast_with_options(&bits_in, &DataType::Int32, &cast_opts)
                .map_err(arrow_err)?;
        let bits = bits
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("cast to Int32 yields Int32Array");

        let mut out = StringBuilder::new();
        for i in 0..n {
            if bytes.is_null(i) || bits.is_null(i) {
                out.append_null();
                continue;
            }
            let data = bytes.value(i);
            // `0` means 256. An unsupported width is NULL, not an error — Spark's documented rule.
            match bits.value(i) {
                224 => out.append_value(to_hex(&Sha224::digest(data))),
                0 | 256 => out.append_value(to_hex(&Sha256::digest(data))),
                384 => out.append_value(to_hex(&Sha384::digest(data))),
                512 => out.append_value(to_hex(&Sha512::digest(data))),
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// crc32
// ---------------------------------------------------------------------------

/// `crc32(expr)` — CRC-32 as a `bigint`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Crc32Udf {
    signature: Signature,
}

impl Crc32Udf {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Crc32Udf {
    fn name(&self) -> &str {
        "crc32"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![coerce_hashable("crc32", arg_types)?])
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let bytes = to_bytes(&args.args[0], n)?;
        let mut out = Int64Array::builder(n);
        for i in 0..n {
            if bytes.is_null(i) {
                out.append_null();
            } else {
                let mut h = crc32fast::Hasher::new();
                h.update(bytes.value(i));
                // Widen the unsigned checksum, so the `bigint` is never negative.
                out.append_value(h.finalize() as i64);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    async fn row(q: &str) -> String {
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    /// Every expected value here is an independently computed reference digest of `abc`
    /// (`printf 'abc' | shasum -a N`, `python3 -c "import zlib; zlib.crc32(b'abc')"`), not a value
    /// read back out of this implementation.
    #[tokio::test]
    async fn digests_match_reference_vectors() {
        for (q, want) in [
            (
                "SELECT sha1('abc') AS x",
                "a9993e364706816aba3e25717850c26c9cd0d89d",
            ),
            (
                "SELECT sha('abc') AS x",
                "a9993e364706816aba3e25717850c26c9cd0d89d",
            ),
            (
                "SELECT sha1('') AS x",
                "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            ),
            (
                "SELECT sha2('abc', 224) AS x",
                "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7",
            ),
            (
                "SELECT sha2('abc', 256) AS x",
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            // `0` is Spark's spelling of 256.
            (
                "SELECT sha2('abc', 0) AS x",
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                "SELECT sha2('abc', 384) AS x",
                "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
            ),
            (
                "SELECT sha2('abc', 512) AS x",
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
            ),
            ("SELECT crc32('abc') AS x", "891568578"),
            ("SELECT crc32('') AS x", "0"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// An unsupported `numBits` is NULL, not an error — Spark's documented rule.
    #[tokio::test]
    async fn sha2_unsupported_width_is_null() {
        let engine = crate::Engine::new();
        let batches = engine
            .sql("SELECT sha2('abc', 100) AS x")
            .await
            .expect("sha2 with a bad width must not error");
        assert_eq!(batches[0].column(0).null_count(), 1);
    }

    #[tokio::test]
    async fn null_input_yields_null() {
        let engine = crate::Engine::new();
        for q in [
            "SELECT sha1(CAST(NULL AS STRING)) AS x",
            "SELECT sha2(CAST(NULL AS STRING), 256) AS x",
            "SELECT crc32(CAST(NULL AS STRING)) AS x",
        ] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            assert_eq!(batches[0].column(0).null_count(), 1, "{q}");
        }
    }
}
