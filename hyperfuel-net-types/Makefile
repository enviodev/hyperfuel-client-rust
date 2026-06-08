# Regenerating the committed Cap'n Proto code requires two tools:
#   1. The capnp schema compiler:  apt-get install -y capnproto libcapnp-dev
#   2. The capnpc-rust plugin, whose version MUST match the `capnp` runtime
#      dependency in Cargo.toml (currently 0.19), otherwise the generated code
#      may reference internal APIs incompatible with the linked runtime:
#          cargo install capnpc --version "^0.19"
.PHONY: generate_capnp_types clean_generated_capnp_types

generate_capnp_types:
	capnp compile hyperfuel_net_types.capnp -o rust:./src/__generated__
	rustfmt src/__generated__/hyperfuel_net_types_capnp.rs

clean_generated_capnp_types:
	rm -f src/__generated__/hyperfuel_net_types_capnp.rs
