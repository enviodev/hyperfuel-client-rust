use std::num::NonZeroU64;

use hyperfuel_client::{Client, Config};
use hyperfuel_format::Hex;
use hyperfuel_net_types::{FieldSelection, Query, ReceiptSelection};
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
    let contract = hex_literal::hex!(
        "f5b08689ada97df7fd2fbd67bee7dea6d219f117c1dc9345245da16fe4e99111" // https://app.fuel.network/account/0xf5b08689ada97df7fd2fbd67bee7dea6d219f117c1dc9345245da16fe4e99111
    );

    // Loop infinitely
    let mut from_block = 0;
    loop {
        // Update the query with the new from_block
        let query = Query {
            from_block,
            receipts: vec![ReceiptSelection {
                receipt_type: vec![6],
                root_contract_id: vec![contract.into()],
                tx_status: vec![1],
                rb: vec![
                    /*SellItem*/ 11192939610819626128,
                    /*LevelUp*/ 9956391856148830557,
                    /*NewPlayer*/ 169340015036328252,
                ],
                ..Default::default()
            }],
            field_selection: FieldSelection {
                receipt: maplit::btreeset! {
                    "tx_id".to_owned(),
                    "block_height".to_owned(),
                    "root_contract_id".to_owned(),
                    "data".to_owned(),
                    "receipt_index".to_owned(),
                    "receipt_type".to_owned(),
                    "ra".to_owned(),
                    "rb".to_owned(),
                },
                // transaction: maplit::btreeset! {
                //     "id".to_owned(),
                //     "time".to_owned(),
                // },
                ..Default::default()
            },
            ..Default::default()
        };

        // Make the query
        let res = client.get_selected_data(&query).await.unwrap();

        if from_block < res.next_block {
            // Check if there are any receipts
            if !res.data.receipts.is_empty() {
                println!("Receipts found: {}", res.data.receipts.len());
                println!("Receipt details:");
                for receipt in &res.data.receipts {
                    println!("  TX ID: {}", receipt.tx_id.encode_hex());
                    println!("  Block Height: {:?}", *receipt.block_height,);
                    println!(
                        "  Root Contract ID: {}",
                        receipt
                            .root_contract_id
                            .as_ref()
                            .map(|id| id.encode_hex())
                            .unwrap_or_default()
                    );
                    // TODO: add code to decode this data
                    println!(
                        "  Data: {}",
                        receipt
                            .data
                            .as_ref()
                            .map(|data| data.encode_hex())
                            .unwrap_or_default()
                    );
                    println!("  Receipt Index: {:?}", *receipt.receipt_index);
                    println!("  Receipt Type: {:?}", receipt.receipt_type);
                    println!("  RA: {:?}", receipt.ra.map(|ra| *ra).unwrap_or_default());
                    println!(
                        "  RB (logId): {:?}",
                        receipt
                            .rb
                            .map(|rb| if *rb == 11192939610819626128 {
                                "SellItem"
                            } else if *rb == 9956391856148830557 {
                                "LevelUp"
                            } else if *rb == 169340015036328252 {
                                "NewPlayer"
                            } else {
                                "Unknown"
                            })
                            .unwrap_or_default()
                    );
                    println!("  ---");
                }
            } else {
                println!("No receipts found in this block. {:?}", res.archive_height);
            }
        }

        // Update from_block for the next iteration
        from_block = res.next_block;

        // Sleep for 200ms
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
}
