//! Plain-C FFI to the GPU shim, plus Arrow C Data Interface result import.
//!
//! One entry point, [`exec_spec`]: serialize the [`GpuOpSpec`] to JSON, hand it to
//! `oxidant_gpu_exec` (exported by `libcudf_shim` under `--features gpu`, by
//! `csrc/mock_shim.c` otherwise), and import the returned struct array as a
//! [`RecordBatch`]. No cxx, no bindgen — the shim speaks the Arrow C Data
//! Interface directly, and arrow-rs's FFI types are the out-params.

use std::ffi::CString;
use std::os::raw::c_char;

use datafusion::arrow::array::{RecordBatch, StructArray};
use datafusion::arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};

use crate::spec::GpuOpSpec;

extern "C" {
    /// Shim entry point: execute the JSON op spec and export ONE record batch
    /// (final aggregate results) as a C Data Interface struct array + schema.
    /// Returns 0 on success and transfers ownership of the exported
    /// schema/array to the caller (their release callbacks are set); non-zero
    /// means the out-params were not touched.
    fn oxidant_gpu_exec(
        spec_json: *const c_char,
        out_schema: *mut FFI_ArrowSchema,
        out_array: *mut FFI_ArrowArray,
    ) -> i32;
}

/// Run `spec` on the GPU shim and import the final aggregate result batch.
pub fn exec_spec(spec: &GpuOpSpec) -> Result<RecordBatch> {
    let json = serde_json::to_string(spec).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let c_json = CString::new(json).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut schema = FFI_ArrowSchema::empty();
    let mut array = FFI_ArrowArray::empty();
    // SAFETY: both out-params point at valid, initialized-empty FFI structs. On
    // success the shim sets their release callbacks, transferring ownership to us;
    // on failure it leaves them empty (release == NULL), so dropping is a no-op.
    let rc = unsafe { oxidant_gpu_exec(c_json.as_ptr(), &mut schema, &mut array) };
    if rc != 0 {
        return Err(DataFusionError::Execution(format!(
            "oxidant_gpu_exec failed with code {rc}"
        )));
    }
    // SAFETY: rc == 0 is the shim's contract that the out-params hold a valid
    // C Data Interface struct array + schema. `from_ffi` takes the array by
    // value, so its release runs exactly once when the imported buffers drop;
    // `schema` is released by its Drop at the end of this function.
    let data = unsafe { from_ffi(array, &schema)? };
    Ok(RecordBatch::from(&StructArray::from(data)))
}
