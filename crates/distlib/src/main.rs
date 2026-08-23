//! The `distlib` binary: wires the crates together and provides the CLI.

fn main() {
    println!("distlib {}", env!("CARGO_PKG_VERSION"));
}
