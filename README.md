# hyperfuel-client-rust
Rust crate for [Envio's](https://envio.dev/) HyperFuel client

`$ cargo add hyperfuel-client`

### Dependencies

Need to install capnproto tool in order to build the library. It can be installed like this on Ubuntu, Windows and MacOS in order:
```bash
sudo apt-get install -y capnproto libcapnp-dev
choco install capnproto
brew install capnp
```

# quickstart example
```rust
use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = Config {
        url: Url::parse("https://fuel-15.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
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

### more examples in `examples/`