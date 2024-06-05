use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = Config {
        url: Url::parse("https://fuel-testnet.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = Client::new(client_config).unwrap();

    // contract to get logs from
    let contracts = vec![hex_literal::hex!(
        "4a2ce054e3e94155f7092f7365b212f7f45105b74819c623744ebcc5d065c6ac"
    )];
    // start query from block 0
    let from_block = 0;
    // if to_block is not set, query runs to the end of the chain
    let to_block = Some(1627509);

    // get logs
    let logs = client
        .preset_query_get_logs(contracts, from_block, to_block)
        .await
        .unwrap();

    println!("logs: {:?}", logs.data);
}
