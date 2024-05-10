use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use hyperfuel_net_types::Query;
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = Config {
        url: Url::parse("https://fuel-15.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = Client::new(client_config).unwrap();

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 7980000,
        "to_block":   7980100,
        "inputs": [
            {
            "asset_id": ["0x0000000000000000000000000000000000000000000000000000000000000000"]
            }
        ],
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

    println!("inputs: {:?}", res.data);
}
