fn main() {
    // no system protoc needed, use the vendored binary
    std::env::set_var(
        "PROTOC",
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc"),
    );
    tonic_build::compile_protos("proto/admin.proto").expect("compile admin.proto");
    println!("cargo:rerun-if-changed=proto/admin.proto");
}
