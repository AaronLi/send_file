extern crate core;

use std::io;

fn main () -> io::Result<()> {
    prost_build::compile_protos(&["src/proto/messages.proto"], &["src/proto"])?;
    Ok(())
}