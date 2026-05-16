# hyperfuel-client-rust

[![CI](https://github.com/enviodev/hyperfuel-client-rust/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/enviodev/hyperfuel-client-rust/actions/workflows/ci.yaml)
<a href="https://crates.io/crates/hyperfuel-client">
<img src="https://img.shields.io/crates/v/hyperfuel-client.svg?style=flat-square"
    alt="Crates.io version" />
</a>

Rust crate for [Envio's](https://envio.dev/) HyperFuel client.

`$ cargo add hyperfuel-client`

[Documentation Page](https://docs.envio.dev/docs/hypersync-clients)

### Dependencies

Need to install capnproto tool in order to build the library. It can be installed like this on Ubuntu, Windows and MacOS in order:

```bash
sudo apt-get install -y capnproto libcapnp-dev
choco install capnproto
brew install capnp
```

### Quickstart example

Add [`hex-literal`](https://docs.rs/hex-literal) to your `Cargo.toml` if you use the `hex!` macro below.

```rust
use hyperfuel_client::{Client, ClientConfig};
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = ClientConfig {
        url: Some(Url::parse("https://fuel-15.hypersync.xyz").unwrap()),
        ..Default::default()
    };
    let client = Client::new(client_config).unwrap();

    let contracts = vec![hex_literal::hex!(
        "ff63ad3cdb5fde197dfa2d248330d458bffe631bda65938aa7ab7e37efa561d0"
    )];
    let from_block = 8076516;
    let to_block = Some(8076517);

    let logs = client
        .preset_query_get_logs(contracts, from_block, to_block)
        .await
        .unwrap();

    println!("logs: {:?}", logs.data);
}
```
