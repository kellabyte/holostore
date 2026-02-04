fn main() {
    println!("cargo:rerun-if-changed=proto/holo.proto");

    volo_build::Builder::protobuf()
        .add_service("proto/holo.proto")
        .include_dirs(vec![std::path::PathBuf::from(".")])
        .write()
        .unwrap();
}
