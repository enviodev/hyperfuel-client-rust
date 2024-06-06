use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use hyperfuel_net_types::Query;
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = Config {
        url: Url::parse("https://fuel-testnet.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = Client::new(client_config).unwrap();

    // Construct query in json.  Can also construct it as a typed struct (see predicate-root example)
    let query: Query = serde_json::from_value(serde_json::json!({
        // start query from block 0
        "from_block": 0,
        // if to_block is not set, query runs to the end of the chain
        "to_block": 1300000,
        // load inputs that have `asset_id` = 0x2a0d0ed9d2217ec7f32dcd9a1902ce2a66d68437aeff84e3a3cc8bebee0d2eea
        "inputs": [
            {
            "asset_id": ["0x2a0d0ed9d2217ec7f32dcd9a1902ce2a66d68437aeff84e3a3cc8bebee0d2eea"]
            }
        ],
        // fields we want returned from loaded inputs
        "field_selection": {
            "input": [
                "block_height",
                "tx_id",
                "owner",
                "amount",
                "asset_id"
            ]
        }
    }))
    .unwrap();

    let res = client.get_selected_data(&query).await.unwrap();

    println!("inputs: {:?}", res.data.inputs);
    println!("query took {}ms", res.total_execution_time);
}
