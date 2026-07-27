fn main() {
    prost_build::compile_protos(&["src/schema/dht.proto"], &["src/schema"]).unwrap();

    // Reuse the polkadot-sdk light-client proto directly instead of vendoring a
    // copy. This generates the `api.v1.light` module (RemoteCallRequest /
    // RemoteReadRequest / ...) used by the `/<genesis>/light/2` request-response
    // protocol. The path is overridable via LIGHT_PROTO_PATH for non-standard
    // checkouts; it defaults to the sibling polkadot-sdk under /home/miszka/parity.
    const DEFAULT_LIGHT_PROTO: &str = "/home/ubuntu/work/paritytech/polkadot-sdk/substrate/client/network/light/src/schema/light.v1.proto";
    let light_proto =
        std::env::var("LIGHT_PROTO_PATH").unwrap_or_else(|_| DEFAULT_LIGHT_PROTO.to_string());
    let light_dir = std::path::Path::new(&light_proto)
        .parent()
        .expect("light proto has a parent directory; qed")
        .to_path_buf();
    println!("cargo:rerun-if-env-changed=LIGHT_PROTO_PATH");
    println!("cargo:rerun-if-changed={light_proto}");
    prost_build::compile_protos(&[light_proto.as_str()], &[light_dir]).unwrap();
}
