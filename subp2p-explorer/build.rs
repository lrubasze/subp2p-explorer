fn main() {
    prost_build::compile_protos(&["src/schema/dht.proto"], &["src/schema"]).unwrap();

    // The light-client proto is vendored (like `dht.proto`) rather than read out
    // of a polkadot-sdk checkout, so the crate builds anywhere. It generates the
    // `api.v1.light` module (RemoteCallRequest / RemoteReadRequest / ...) used by
    // the `/<genesis>/light/2` request-response protocol.
    //
    // `src/schema/light.v1.proto` is a verbatim copy of
    // `substrate/client/network/light/src/schema/light.v1.proto`, taken from
    // polkadot-sdk 0e1812505425812c01e6ff6d4f28f6edf729678a. Refresh it with
    // LIGHT_PROTO_PATH if the upstream definition ever changes — but note the
    // wire format is deliberately stable (upstream even records which field ids
    // were retired), so drift is unlikely.
    const VENDORED_LIGHT_PROTO: &str = "src/schema/light.v1.proto";
    let light_proto =
        std::env::var("LIGHT_PROTO_PATH").unwrap_or_else(|_| VENDORED_LIGHT_PROTO.to_string());
    let light_dir = std::path::Path::new(&light_proto)
        .parent()
        .expect("light proto has a parent directory; qed")
        .to_path_buf();
    println!("cargo:rerun-if-env-changed=LIGHT_PROTO_PATH");
    println!("cargo:rerun-if-changed={light_proto}");
    prost_build::compile_protos(&[light_proto.as_str()], &[light_dir]).unwrap();
}
