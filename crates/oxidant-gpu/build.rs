//! Build script: two link modes for the GPU shim.
//!
//! - default: compile `csrc/mock_shim.c` (a tiny CPU shim returning one fixed batch)
//!   into a static library and link it, so the whole FFI path is testable without a GPU.
//! - `--features gpu`: link the real libcudf shim (`-lcudf_shim`) from the directory
//!   named by the `OXIDANT_GPU_SHIM_DIR` env var.

fn main() {
    if std::env::var_os("CARGO_FEATURE_GPU").is_some() {
        let dir = std::env::var("OXIDANT_GPU_SHIM_DIR").expect(
            "OXIDANT_GPU_SHIM_DIR must point at the directory containing libcudf_shim \
             when building oxidant-gpu with `--features gpu`",
        );
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=cudf_shim");
        println!("cargo:rerun-if-env-changed=OXIDANT_GPU_SHIM_DIR");
    } else {
        cc::Build::new()
            .file("csrc/mock_shim.c")
            .compile("oxidant_gpu_mock_shim");
        println!("cargo:rerun-if-changed=csrc/mock_shim.c");
    }
}
