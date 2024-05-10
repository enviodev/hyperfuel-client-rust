fn main() {
    capnpc::CompilerCommand::new()
        .file("hyperfuel_net_types.capnp")
        .run()
        .expect("compiling schema");
}
