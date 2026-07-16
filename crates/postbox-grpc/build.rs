fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/postbox.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])?;
    Ok(())
}