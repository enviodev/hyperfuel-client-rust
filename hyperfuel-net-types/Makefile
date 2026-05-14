.PHONY: generate_capnp_types clean_generated_capnp_types

generate_capnp_types:
	capnp compile hyperfuel_net_types.capnp -o rust:./src/__generated__
	rustfmt src/__generated__/hyperfuel_net_types_capnp.rs

clean_generated_capnp_types:
	rm -f src/__generated__/hyperfuel_net_types_capnp.rs
