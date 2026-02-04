//! Build script to generate gRPC bindings from the protobuf definition.

/// Regenerates `volo` gRPC code when the protobuf changes.
///
/// Inputs: `proto/holo.proto`
/// Output: generated Rust code under Cargo's `OUT_DIR`.
fn main() {
    // Tell Cargo to rerun this build script when the proto file changes.
    println!("cargo:rerun-if-changed=proto/holo.proto");

    // Invoke the codegen pipeline for the gRPC service definition.
    volo_build::Builder::protobuf()
        .add_service("proto/holo.proto")
        .include_dirs(vec![std::path::PathBuf::from(".")])
        .write()
        .unwrap();
}
