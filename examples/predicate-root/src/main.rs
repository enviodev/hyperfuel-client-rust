use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use hyperfuel_net_types::{FieldSelection, InputSelection, Query};
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = Config {
        url: Url::parse("https://fuel-testnet.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = Client::new(client_config).unwrap();

    // Construct query as a typed struct.  Can also construct it in json (see asset-id example)
    let query = Query {
        // start query from block 0
        from_block: 0,
        // if to_block is not set, query runs to the end of the chain
        to_block: Some(1427625),
        // load inputs that have `owner` = 0x94a8e322ff02baeb1d625e83dadf5ec88870ac801da370d4b15bbd5f0af01169
        inputs: vec![InputSelection {
            owner: vec![hex_literal::hex!(
                "94a8e322ff02baeb1d625e83dadf5ec88870ac801da370d4b15bbd5f0af01169"
            )
            .into()],
            ..Default::default()
        }],
        // fields we want returned from loaded inputs
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

    println!("inputs: {:?}", res.data.inputs);
}
