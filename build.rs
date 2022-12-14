const SERDE_PROC_MACROS: &str = "#[derive(serde::Serialize, serde::Deserialize)]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=protobuf/");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_well_known_types(true)
        .out_dir("src/protobuf")
        .compile_with_config(prost_config(), &["redis_service.proto"], &["protobuf"])?;
    Ok(())
}

fn prost_config() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.type_attribute(".", SERDE_PROC_MACROS);
    config.disable_comments(["Timestamp", "Empty"]);
    config.extern_path(".google.protobuf.Empty", "()");
    config
}
