use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use arrow2::{array::Array, chunk::Chunk};

use filter::filter_out_unselected_data;
use format::{Transaction, TransactionStatus};
use from_arrow::{receipts_from_arrow_data, typed_data_from_arrow_data, FromArrow};
use hyperfuel_format::Hash;
use hyperfuel_net_types::{
    hyperfuel_net_types_capnp, ArchiveHeight, FieldSelection, Query, ReceiptSelection,
};
use reqwest::Method;

pub mod config;
mod filter;
mod from_arrow;
mod parquet_out;
mod transport_format;
mod types;

pub use config::Config;
pub use hyperfuel_format as format;
pub use transport_format::{ArrowIpc, TransportFormat};
pub use types::{
    ArrowBatch, LogContext, LogResponse, QueryResponse, QueryResponseData, QueryResponseDataTyped,
    QueryResponseTyped,
};

pub type ArrowChunk = Chunk<Box<dyn Array>>;

pub struct Client {
    http_client: reqwest::Client,
    cfg: Config,
}

impl Client {
    /// Create a new client with given config
    pub fn new(cfg: Config) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .no_gzip()
            .http1_only()
            .timeout(Duration::from_millis(cfg.http_req_timeout_millis.get()))
            .tcp_keepalive(Duration::from_secs(7200))
            .connect_timeout(Duration::from_millis(cfg.http_req_timeout_millis.get()))
            .build()
            .unwrap();

        Ok(Self { http_client, cfg })
    }

    /// Create a parquet file by executing a query.
    ///
    /// If the query can't be finished in a single request, this function will
    /// keep on making requests using the pagination mechanism (next_block) until
    /// it reaches the end. It will stream data into the parquet file as it comes from
    /// the server.
    ///
    /// Path should point to a folder that will contain the parquet files in the end.
    pub async fn create_parquet_folder(&self, query: Query, path: String) -> Result<()> {
        parquet_out::create_parquet_folder(self, query, path).await
    }

    /// Get the height of the source hypersync instance
    pub async fn get_height(&self) -> Result<u64> {
        let mut url = self.cfg.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("height");
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::GET, url);

        if let Some(bearer_token) = &self.cfg.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

        let res = req.send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            return Err(anyhow!("http response status code {}", status));
        }

        let height: ArchiveHeight = res.json().await.context("read response body json")?;

        Ok(height.height.unwrap_or(0))
    }

    /// Get the height of the source hypersync instance
    /// Internally calls get_height.
    /// On an error from the source hypersync instance, sleeps for
    /// 1 second (increasing by 1 each failure up to max of 5 seconds)
    /// and retries query until success.
    pub async fn get_height_with_retry(&self) -> Result<u64> {
        let mut base = 1;

        loop {
            match self.get_height().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!("failed to send request to hyperfuel server: {:?}", e);
                }
            }

            let secs = Duration::from_secs(base);
            let millis = Duration::from_millis(fastrange_rs::fastrange_64(rand::random(), 1000));

            tokio::time::sleep(secs + millis).await;

            base = std::cmp::min(base + 1, 5);
        }
    }

    /// Send a query request to the source hypersync instance.
    ///
    /// Returns a query response which contains typed data.
    ///
    /// NOTE: this query returns loads all transactions that your match your receipt, input, or output selections
    /// and applies the field selection to all these loaded transactions.  So your query will return the data you
    /// want plus additional data from the loaded transactions.  This functionality is in case you want to associate
    /// receipts, inputs, or outputs with eachother.
    pub async fn get_data(&self, query: &Query) -> Result<QueryResponseTyped> {
        let res = self.get_arrow_data(query).await.context("get arrow data")?;

        let typed_data =
            typed_data_from_arrow_data(res.data).context("convert arrow data to typed response")?;

        Ok(QueryResponseTyped {
            archive_height: res.archive_height,
            next_block: res.next_block,
            total_execution_time: res.total_execution_time,
            data: typed_data,
        })
    }

    /// Send a query request to the source hypersync instance.
    ///
    /// Returns a query response that which contains structured data that doesn't include any inputs, outputs,
    /// and receipts that don't exactly match the query's input, outout, or receipt selection.
    pub async fn get_selected_data(&self, query: &Query) -> Result<QueryResponseTyped> {
        let query = add_selections_to_field_selection(&mut query.clone());

        let res = self
            .get_arrow_data(&query)
            .await
            .context("get arrow data")?;

        let filtered_data =
            filter_out_unselected_data(res.data, &query).context("filter out unselected data")?;

        let typed_data = typed_data_from_arrow_data(filtered_data)
            .context("convert arrow data to typed response")?;

        Ok(QueryResponseTyped {
            archive_height: res.archive_height,
            next_block: res.next_block,
            total_execution_time: res.total_execution_time,
            data: typed_data,
        })
    }

    /// Send a query request to the source hypersync instance.
    ///
    /// Returns all log and logdata receipts of logs emitted by any of the specified contracts
    /// within the block range.
    /// If no 'to_block' is specified, query will run to the head of the chain.
    /// Returned data contains all the data needed to decode Fuel Log or LogData
    /// receipts as well as some extra data for context.  This query doesn't return any logs that
    /// were a part of a failed transaction.
    ///
    /// NOTE: this function is experimental and might be removed in future versions.
    pub async fn preset_query_get_logs<H: Into<Hash>>(
        &self,
        emitting_contracts: Vec<H>,
        from_block: u64,
        to_block: Option<u64>,
    ) -> Result<LogResponse> {
        let mut transaction_field_selection = BTreeSet::new();
        transaction_field_selection.insert("id".to_owned());
        transaction_field_selection.insert("status".to_owned());

        let mut receipt_field_selection = BTreeSet::new();
        receipt_field_selection.insert("block_height".to_owned());
        receipt_field_selection.insert("tx_id".to_owned());
        receipt_field_selection.insert("receipt_index".to_owned());
        receipt_field_selection.insert("receipt_type".to_owned());
        receipt_field_selection.insert("contract_id".to_owned());
        receipt_field_selection.insert("root_contract_id".to_owned());
        receipt_field_selection.insert("ra".to_owned());
        receipt_field_selection.insert("rb".to_owned());
        receipt_field_selection.insert("rc".to_owned());
        receipt_field_selection.insert("rd".to_owned());
        receipt_field_selection.insert("pc".to_owned());
        receipt_field_selection.insert("is".to_owned());
        receipt_field_selection.insert("ptr".to_owned());
        receipt_field_selection.insert("len".to_owned());
        receipt_field_selection.insert("digest".to_owned());
        receipt_field_selection.insert("data".to_owned());

        let emitting_contracts: Vec<Hash> =
            emitting_contracts.into_iter().map(|c| c.into()).collect();
        let query = Query {
            from_block,
            to_block,
            receipts: vec![
                ReceiptSelection {
                    root_contract_id: emitting_contracts.clone(),
                    receipt_type: vec![5, 6],
                    ..Default::default()
                },
                ReceiptSelection {
                    contract_id: emitting_contracts,
                    receipt_type: vec![5, 6],
                    ..Default::default()
                },
            ],
            field_selection: FieldSelection {
                transaction: transaction_field_selection,
                receipt: receipt_field_selection,
                ..Default::default()
            },
            ..Default::default()
        };

        let res = self
            .get_arrow_data(&query)
            .await
            .context("get arrow data")?;

        let filtered_data = filter_out_unselected_data(res.data, &query)
            .context("filter out unselected receipts")?;

        let typed_receipts = receipts_from_arrow_data(&filtered_data.receipts)
            .context("convert arrow data to receipt response")?;

        let mut failed_txns = HashSet::new();
        for batch in filtered_data.transactions.iter() {
            let data = Transaction::from_arrow(batch).context("transaction from arrow")?;
            for transaction in data {
                if transaction.status == TransactionStatus::Failure {
                    failed_txns.insert(transaction.id);
                }
            }
        }

        let successful_logs: Vec<LogContext> = typed_receipts
            .into_iter()
            .filter_map(|receipt| {
                if !failed_txns.contains(&receipt.tx_id) {
                    Some(receipt.into())
                } else {
                    None
                }
            })
            .collect();

        Ok(LogResponse {
            archive_height: res.archive_height,
            next_block: res.next_block,
            total_execution_time: res.total_execution_time,
            data: successful_logs,
        })
    }

    /// Send a query request to the source hypersync instance.
    ///
    /// Returns a query response which contains arrow data.
    ///
    /// NOTE: this query returns loads all transactions that your match your receipt, input, or output selections
    /// and applies the field selection to all these loaded transactions.  So your query will return the data you
    /// want plus additional data from the loaded transactions.  This functionality is in case you want to associate
    /// receipts, inputs, or outputs with eachother.
    pub async fn get_arrow_data(&self, query: &Query) -> Result<QueryResponse> {
        let mut url = self.cfg.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("query");
        segments.push(ArrowIpc::path());
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::POST, url);

        if let Some(bearer_token) = &self.cfg.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

        log::trace!("sending req to hyperfuel");
        let res = req.json(&query).send().await.context("execute http req")?;
        log::trace!("got req response");

        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.context("read text to see error")?;

            return Err(anyhow!(
                "http response status code {}, err body: {}",
                status,
                text
            ));
        }

        log::trace!("starting to get response body bytes");

        let bytes = res.bytes().await.context("read response body bytes")?;

        log::trace!("starting to parse query response");

        let res = tokio::task::block_in_place(|| {
            self.parse_query_response::<ArrowIpc>(&bytes)
                .context("parse query response")
        })?;

        log::trace!("got data from hyperfuel");

        Ok(res)
    }

    /// Send a query request to the source hypersync instance.
    /// Internally calls send.
    /// On an error from the source hypersync instance, sleeps for
    /// 1 second (increasing by 1 each failure up to max of 5 seconds)
    /// and retries query until success.
    ///
    /// Returns a query response which contains arrow data.
    ///
    /// NOTE: this query returns loads all transactions that your match your receipt, input, or output selections
    /// and applies the field selection to all these loaded transactions.  So your query will return the data you
    /// want plus additional data from the loaded transactions.  This functionality is in case you want to associate
    /// receipts, inputs, or outputs with eachother.
    /// Format can be ArrowIpc.
    pub async fn get_arrow_data_with_retry(&self, query: &Query) -> Result<QueryResponse> {
        let mut base = 1;

        loop {
            match self.get_arrow_data(query).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!("failed to send request to hyperfuel server: {:?}", e);
                }
            }

            let secs = Duration::from_secs(base);
            let millis = Duration::from_millis(fastrange_rs::fastrange_64(rand::random(), 1000));

            tokio::time::sleep(secs + millis).await;

            base = std::cmp::min(base + 1, 5);
        }
    }

    fn parse_query_response<Format: TransportFormat>(&self, bytes: &[u8]) -> Result<QueryResponse> {
        let mut opts = capnp::message::ReaderOptions::new();
        opts.nesting_limit(i32::MAX).traversal_limit_in_words(None);
        let message_reader =
            capnp::serialize_packed::read_message(bytes, opts).context("create message reader")?;

        let query_response = message_reader
            .get_root::<hyperfuel_net_types_capnp::query_response::Reader>()
            .context("get root")?;

        let archive_height = match query_response.get_archive_height() {
            -1 => None,
            h => Some(
                h.try_into()
                    .context("invalid archive height returned from server")?,
            ),
        };

        let data = query_response.get_data().context("read data")?;

        let blocks = Format::read_chunks(data.get_blocks().context("get data")?)
            .context("parse block data")?;
        let transactions = Format::read_chunks(data.get_transactions().context("get data")?)
            .context("parse tx data")?;
        let receipts = Format::read_chunks(data.get_receipts().context("get data")?)
            .context("parse receipt data")?;
        let inputs = Format::read_chunks(data.get_inputs().context("get data")?)
            .context("parse input data")?;
        let outputs = Format::read_chunks(data.get_outputs().context("get data")?)
            .context("parse output data")?;

        Ok(QueryResponse {
            archive_height,
            next_block: query_response.get_next_block(),
            total_execution_time: query_response.get_total_execution_time(),
            data: QueryResponseData {
                blocks,
                transactions,
                receipts,
                inputs,
                outputs,
            },
        })
    }
}

// receipt, input, and output selections must have the associated query fields in
// field_selection or else we can't do client-side filtering via comparison
fn add_selections_to_field_selection(query: &mut Query) -> Query {
    query.receipts.iter_mut().for_each(|selection| {
        if !selection.root_contract_id.is_empty() {
            query
                .field_selection
                .receipt
                .insert("root_contract_id".into());
        }
        if !selection.to_address.is_empty() {
            query.field_selection.receipt.insert("to_address".into());
        }
        if !selection.asset_id.is_empty() {
            query.field_selection.receipt.insert("asset_id".into());
        }
        if !selection.receipt_type.is_empty() {
            query.field_selection.receipt.insert("receipt_type".into());
        }
        if !selection.sender.is_empty() {
            query.field_selection.receipt.insert("sender".into());
        }
        if !selection.recipient.is_empty() {
            query.field_selection.receipt.insert("recipient".into());
        }
        if !selection.contract_id.is_empty() {
            query.field_selection.receipt.insert("contract_id".into());
        }
        if !selection.ra.is_empty() {
            query.field_selection.receipt.insert("ra".into());
        }
        if !selection.rb.is_empty() {
            query.field_selection.receipt.insert("rb".into());
        }
        if !selection.rc.is_empty() {
            query.field_selection.receipt.insert("rc".into());
        }
        if !selection.rd.is_empty() {
            query.field_selection.receipt.insert("rd".into());
        }
    });

    query.inputs.iter_mut().for_each(|selection| {
        if !selection.owner.is_empty() {
            query.field_selection.input.insert("owner".into());
        }
        if !selection.asset_id.is_empty() {
            query.field_selection.input.insert("asset_id".into());
        }
        if !selection.contract.is_empty() {
            query.field_selection.input.insert("contract".into());
        }
        if !selection.sender.is_empty() {
            query.field_selection.input.insert("sender".into());
        }
        if !selection.recipient.is_empty() {
            query.field_selection.input.insert("recipient".into());
        }
        if !selection.input_type.is_empty() {
            query.field_selection.input.insert("input_type".into());
        }
    });

    query.outputs.iter_mut().for_each(|selection| {
        if !selection.to.is_empty() {
            query.field_selection.output.insert("to".into());
        }
        if !selection.asset_id.is_empty() {
            query.field_selection.output.insert("asset_id".into());
        }
        if !selection.contract.is_empty() {
            query.field_selection.output.insert("contract".into());
        }
        if !selection.output_type.is_empty() {
            query.field_selection.output.insert("output_type".into());
        }
    });

    query.clone()
}
