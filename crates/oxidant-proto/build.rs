//! Compile the vendored Spark Connect protos with `protox` (pure-Rust, no `protoc`),
//! then hand the resulting FileDescriptorSet to `tonic-build` for server+client codegen.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from("proto");
    let dir = root.join("spark/connect");

    // Compile every vendored file so all transitively-reachable messages are generated.
    // Skip macOS AppleDouble sidecars (`._foo.proto`) that a careless tar can introduce —
    // they share the `.proto` extension but are not valid protobuf/UTF-8.
    let protos: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "proto").unwrap_or(false))
        .filter(|p| {
            !p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("._"))
                .unwrap_or(false)
        })
        .collect();

    let file_descriptors = protox::compile(&protos, [&root])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true) // client is used by the integration test
        .compile_fds(file_descriptors)?;

    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
