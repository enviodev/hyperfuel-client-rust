use hyperfuel_client::{Client, ClientConfig};
use url::Url;

#[tokio::main]
async fn main() {
    let client_config = ClientConfig {
        url: Some(Url::parse("https://fuel.hypersync.xyz").unwrap()),
        ..Default::default()
    };
    let client = Client::new(client_config).unwrap();

    let contracts = vec![hex_literal::hex!(
        "4a2ce054e3e94155f7092f7365b212f7f45105b74819c623744ebcc5d065c6ac"
    )];
    let from_block = 0;
    let to_block = Some(50_000);

    let logs = client
        .preset_query_get_logs(contracts, from_block, to_block)
        .await
        .unwrap();

    println!(
        "archive_height={:?} next_block={} total_execution_time={}ms logs={}",
        logs.archive_height,
        logs.next_block,
        logs.total_execution_time,
        logs.data.len()
    );
}
