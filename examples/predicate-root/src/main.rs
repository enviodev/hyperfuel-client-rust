use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use hyperfuel_net_types::{FieldSelection, InputSelection, Query};
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = Config {
        url: Url::parse("https://fuel-15.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = Client::new(client_config).unwrap();

    let query = Query {
        from_block: 4105960,
        to_block: Some(4106000),
        inputs: vec![InputSelection {
            owner: vec![hex_literal::hex!(
                "48a0f31c78e1c837ff6a885785ceb7c2090f86ed93db3ed2d8821d13739fe981"
            )
            .into()],
            ..Default::default()
        }],
        field_selection: FieldSelection {
            input: maplit::btreeset! {
                "tx_id".to_owned(),
            "block_height".to_owned(),
            "input_type".to_owned(),
            "utxo_id".to_owned(),
            "owner".to_owned(),
            "amount".to_owned(),
            "asset_id".to_owned(),
            "predicate_gas_used".to_owned(),
            "predicate".to_owned(),
            "predicate_data".to_owned(),

            },
            ..Default::default()
        },
        ..Default::default()
    };

    let res = client.get_selected_data(&query).await.unwrap();

    println!("inputs: {:?}", res.data);
}
