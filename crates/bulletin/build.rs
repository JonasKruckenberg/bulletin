//! Compiles the gRPC service contract (`proto/bulletin/v1/bulletin.proto`) into Rust server stubs.
//!
//! We compile with **protox** (a pure-Rust protobuf compiler) rather than the system `protoc`, so the
//! build has no external toolchain dependency — `cargo build` works on any box, CI or dev. protox
//! produces a `FileDescriptorSet` that we (a) hand to `tonic-prost-build` via `compile_fds` for the
//! service/message codegen and (b) write to `OUT_DIR` so the server can register it for gRPC reflection.

use std::{env, path::PathBuf};

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/bulletin/v1/bulletin.proto";
    let fds = protox::compile([proto], ["proto"])?;

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("bulletin_descriptor.bin");
    std::fs::write(&descriptor_path, fds.encode_to_vec())?;

    tonic_prost_build::configure()
        .build_server(true)
        // The binary is both the server (`bulletin api`) and a client of it: `bulletin debug` is a
        // thin gRPC client of the admin plane, so it runs against a remote engine without needing the
        // DB credential or the SMTP secret locally.
        .build_client(true)
        .compile_fds(fds)?;

    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
