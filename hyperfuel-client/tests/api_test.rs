use std::collections::BTreeSet;

use arrow2::array::UInt64Array;
use hyperfuel_client::{Client, Config};

use hyperfuel_format::FixedSizeData;
use hyperfuel_net_types::{FieldSelection, Query};

const URL: &str = "https://fuel-15.hypersync.xyz";

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_arrow_ipc() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let mut block_field_selection = BTreeSet::new();
    block_field_selection.insert("height".to_owned());
    block_field_selection.insert("id".to_owned());
    block_field_selection.insert("time".to_owned());

    let res = client
        .get_arrow_data(&Query {
            from_block: 20000,
            to_block: Some(30000),
            receipts: Vec::new(),
            include_all_blocks: true,
            field_selection: FieldSelection {
                block: block_field_selection,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();

    let num_blocks: usize = res
        .data
        .blocks
        .iter()
        .map(|batch| batch.column::<UInt64Array>("height").unwrap().len())
        .sum();

    assert!(num_blocks == 10000);
    assert!(res.next_block >= 30000);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_get_height() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let height = client.get_height().await.unwrap();

    assert!(height > 10136503)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_json_query_struct() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 10130504,
        "to_block": 10130604,
        "inputs": [{}],
        "include_all_blocks": true,
        "field_selection": {
            "block": [
                "height"
            ],
            "input": [
                "tx_id",
                "owner",
                "block_height"
            ]
        }
    }))
    .unwrap();

    let res = client.get_arrow_data(&query).await.unwrap();
    let num_blocks: usize = res
        .data
        .blocks
        .iter()
        .map(|batch| batch.column::<UInt64Array>("height").unwrap().len())
        .sum();
    let num_inputs: usize = res
        .data
        .inputs
        .iter()
        .map(|batch| batch.column::<UInt64Array>("block_height").unwrap().len())
        .sum();

    assert!(num_inputs > 0);
    assert!(num_blocks == 100);
    assert!(res.next_block >= 30000);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_arrow_ipc_ordering() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let query = Query {
        from_block: 20001,
        to_block: Some(30000),
        receipts: Vec::new(),
        field_selection: FieldSelection {
            block: maplit::btreeset! {
                "height".to_owned(), "time".to_owned(), "id".to_owned(),
            },
            input: maplit::btreeset! {
                "tx_id".to_owned(), "block_height".to_owned(), "owner".to_owned(),

            },
            ..Default::default()
        },
        ..Default::default()
    };

    let res = client.get_arrow_data(&query).await.unwrap();

    assert!(res.next_block >= 30000);

    let mut last = 0;
    for batch in res.data.inputs {
        let block_number = batch.column::<UInt64Array>("block_height").unwrap();

        for &block_number in block_number.values_iter() {
            assert!(
                last < block_number,
                "last: {:?};number: {:?};",
                last,
                block_number
            );
            last = block_number;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_get_data() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 10130503,
        "to_block": 10130604,
        "inputs": [{}],
        "include_all_blocks": true,
        "field_selection": {
            "block": [
                "height"
            ],
            "input": [
                "tx_id",
                "owner",
                "block_height"
            ]
        }
    }))
    .unwrap();

    let res = client.get_data(&query).await.unwrap();

    assert!(res.data.outputs.len() == 0);
    assert!(res.data.inputs.len() > 0);
    assert!(res.data.blocks.len() > 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_get_selected_data() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 10100503,
        "to_block": 10130604,
        "receipts": [
            {"receipt_type": [6]},
            {"receipt_type": [5]}
        ],
        "include_all_blocks": true,
        "field_selection": {
            "receipt": [
                "tx_id",
                "block_height",
                "receipt_type"
            ]
        }
    }))
    .unwrap();

    let res = client.get_selected_data(&query).await.unwrap();
    assert!(res.data.receipts.len() > 1);

    assert!(res
        .data
        .receipts
        .iter()
        .all(|receipt| receipt.receipt_type.to_u8() == 6 || receipt.receipt_type.to_u8() == 5));

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 10100503,
        "to_block": 10130604,
        "receipts": [
            {"receipt_type": [6, 5]},
        ],
        "include_all_blocks": true,
        "field_selection": {
            "receipt": [
                "tx_id",
                "block_height",
                "receipt_type"
            ]
        }
    }))
    .unwrap();

    let new_res = client.get_selected_data(&query).await.unwrap();

    assert_eq!(res.data.receipts, new_res.data.receipts);

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 8076516,
        "to_block":   8076517,
        "receipts": [
            {
                "root_contract_id": ["0xff63ad3cdb5fde197dfa2d248330d458bffe631bda65938aa7ab7e37efa561d0"],
                "receipt_type": [5, 6],
                "ra": [0],
                "rb": [0, 1, 2, 3]
            }
        ],
        "field_selection": {
            "receipt": [
                "root_contract_id",
                "receipt_type",
                "ra", 
                "rb"
            ],
            "input": [
                "tx_id",
                "owner"
            ]
        }
    }))
    .unwrap();

    let res = client.get_selected_data(&query).await.unwrap();

    let target_contract_id = Some(
        hex_literal::hex!("ff63ad3cdb5fde197dfa2d248330d458bffe631bda65938aa7ab7e37efa561d0")
            .into(),
    );

    assert!(res.data.receipts.iter().all(|receipt| {
        if !((receipt.receipt_type.to_u8() == 6 || receipt.receipt_type.to_u8() == 5)
            && receipt.root_contract_id == target_contract_id
            && vec![0, 1, 2, 3]
                .into_iter()
                .any(|tgt_rb| receipt.rb == Some(tgt_rb.into()))
            && receipt.ra == Some(0.into()))
        {
            println!("{:?}", receipt);
            false
        } else {
            true
        }
    }));

    assert!(res.data.inputs.len() > 0);

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 4105960,
                "to_block": 4106000,
        "inputs": [
            {
                "owner": ["0x48a0f31c78e1c837ff6a885785ceb7c2090f86ed93db3ed2d8821d13739fe981"]
            }
        ],
        "field_selection": {
            "input": [
                "tx_id",
                "block_height",
                "input_type",
                "utxo_id",
                "owner",
                "amount",
                "asset_id",
                "predicate_gas_used",
                "predicate",
                "predicate_data"
            ]
        }
    }))
    .unwrap();

    let res_inputs = client.get_selected_data(&query).await.unwrap();

    let target_owner = Some(
        hex_literal::hex!("48a0f31c78e1c837ff6a885785ceb7c2090f86ed93db3ed2d8821d13739fe981")
            .into(),
    );

    assert!(res_inputs.data.inputs.len() == 1);
    assert!(res_inputs
        .data
        .inputs
        .iter()
        .all(|input| input.owner == target_owner));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_preset_query_get_logs() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let contract: FixedSizeData<32> =
        hex_literal::hex!("ff63ad3cdb5fde197dfa2d248330d458bffe631bda65938aa7ab7e37efa561d0")
            .into();
    let res = client
        .preset_query_get_logs(vec![contract.clone()], 8076516, Some(8076517))
        .await
        .unwrap();

    // failed transaction with logs from the contract (https://app.fuel.network/tx/0x835d678ac1388b0893d9caad1a3a33e2177c3de6923202dc4d1a88b2ab67ade8/simple)
    let failed_txn: FixedSizeData<32> =
        hex_literal::hex!("835d678ac1388b0893d9caad1a3a33e2177c3de6923202dc4d1a88b2ab67ade8")
            .into();

    assert!(res.data.iter().all(|log| {
        (log.receipt_type.to_u8() == 5 || log.receipt_type.to_u8() == 6)
            && (log.contract_id == Some(contract.clone())
                || log.root_contract_id == Some(contract.clone()))
            && log.tx_id != failed_txn
    }));

    assert!(res.data.len() == 4)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_from_arrow_all_fields() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 10130503,
        "to_block": 10130604,
        "inputs": [{}],
        "include_all_blocks": true,
        "field_selection": {
            "block": [
                "id",
                "da_height",
                "transactions_count",
                "message_receipt_count",
                "transactions_root",
                "message_receipt_root",
                "height",
                "prev_root",
                "time",
                "application_hash"
            ],
            "transaction": [
                "block_height",
                "id",
                "input_asset_ids",
                "input_contracts",
                "input_contract_utxo_id",
                "input_contract_balance_root",
                "input_contract_state_root",
                "input_contract_tx_pointer_block_height",
                "input_contract_tx_pointer_tx_index",
                "input_contract",
                "gas_price",
                "gas_limit",
                "maturity",
                "mint_amount",
                "mint_asset_id",
                "tx_pointer_block_height",
                "tx_pointer_tx_index",
                "tx_type",
                "output_contract_input_index",
                "output_contract_balance_root",
                "output_contract_state_root",
                "witnesses",
                "receipts_root",
                "status",
                "time",
                "reason",
                "script",
                "script_data",
                "bytecode_witness_index",
                "bytecode_length",
                "salt"
            ],
            "receipt": [
                "receipt_index",
                "root_contract_id",
                "tx_id",
                "block_height",
                "pc",
                "is",
                "to",
                "to_address",
                "amount",
                "asset_id",
                "gas",
                "param1",
                "param2",
                "val",
                "ptr",
                "digest",
                "reason",
                "ra",
                "rb",
                "rc",
                "rd",
                "len",
                "receipt_type",
                "result",
                "gas_used",
                "data",
                "sender",
                "recipient",
                "nonce",
                "contract_id",
                "sub_id"
            ],
            "input": [
                "tx_id",
                "block_height",
                "input_type",
                "utxo_id",
                "owner",
                "amount",
                "asset_id",
                "tx_pointer_block_height",
                "tx_pointer_tx_index",
                "witness_index",
                "predicate_gas_used",
                "predicate",
                "predicate_data",
                "balance_root",
                "state_root",
                "contract",
                "sender",
                "recipient",
                "nonce",
                "data"
            ],
            "output": [
                "tx_id",
                "block_height",
                "output_type",
                "to",
                "amount",
                "asset_id",
                "input_index",
                "balance_root",
                "state_root",
                "contract"
            ]
        }
    }))
    .unwrap();

    let res = client.get_data(&query).await.unwrap();

    assert!(res.data.blocks.len() > 0);
    assert!(res.data.transactions.len() > 0);
    assert!(res.data.receipts.len() > 0);
    assert!(res.data.inputs.len() > 0);
    assert!(res.data.outputs.len() > 0);
}

/* TODO: decoding */
