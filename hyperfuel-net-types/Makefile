.PHONY: generate_capnp_types clean_generated_capnp_types

CAPNP_CODEGEN_IMAGE ?= hyperfuel-capnp-codegen

generate_capnp_types:
	docker build -f capnp-codegen.Dockerfile -t $(CAPNP_CODEGEN_IMAGE) .
	docker run --rm -u $$(id -u):$$(id -g) -v "$$(pwd)":/work $(CAPNP_CODEGEN_IMAGE) \
		sh -c "capnp compile hyperfuel_net_types.capnp -o rust:./src/__generated__ \
			&& rustfmt src/__generated__/hyperfuel_net_types_capnp.rs"

clean_generated_capnp_types:
	rm -f src/__generated__/hyperfuel_net_types_capnp.rs
