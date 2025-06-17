fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/");
    
    // Use vendored protoc
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    
    prost_build::compile_protos(&[
        "proto/ArteryControlFormats.proto",
        "proto/WireFormats.proto",
        "proto/ContainerFormats.proto",
        "proto/SystemMessageFormats.proto",
    ], &["proto/"])?;
    
    Ok(())
}
