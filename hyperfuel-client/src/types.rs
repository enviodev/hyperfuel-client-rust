use std::sync::Arc;

use crate::{ArrowChunk, FromArrow};
use anyhow::{anyhow, Context, Result};
use hyperfuel_format::{
    BlockHeader, Data, Hash, Input, Output, Receipt, ReceiptType, Transaction, UInt,
};
use polars_arrow::datatypes::SchemaRef;

/// Query response in Arrow format
#[derive(Default, Debug, Clone)]
pub struct ArrowResponseData {
    /// Query blocks response
    pub blocks: Vec<ArrowBatch>,
    /// Query transactions response
    pub transactions: Vec<ArrowBatch>,
    /// Query receipts response
    pub receipts: Vec<ArrowBatch>,
    /// Query inputs response
    pub inputs: Vec<ArrowBatch>,
    /// Query outputs response
    pub outputs: Vec<ArrowBatch>,
}

/// Query response data in Rust native format
#[derive(Default, Debug, Clone)]
pub struct ResponseData {
    /// Query blocks response
    pub blocks: Vec<Vec<BlockHeader>>,
    /// Query transactions response
    pub transactions: Vec<Vec<Transaction>>,
    /// Query receipts response
    pub receipts: Vec<Vec<Receipt>>,
    /// Query inputs response
    pub inputs: Vec<Vec<Input>>,
    /// Query outputs response
    pub outputs: Vec<Vec<Output>>,
}

impl From<&'_ ArrowResponse> for QueryResponse {
    fn from(arrow_response: &ArrowResponse) -> Self {
        let blocks = arrow_response
            .data
            .blocks
            .iter()
            .map(BlockHeader::from_arrow)
            .collect();
        let transactions = arrow_response
            .data
            .transactions
            .iter()
            .map(Transaction::from_arrow)
            .collect();
        let receipts = arrow_response
            .data
            .receipts
            .iter()
            .map(Receipt::from_arrow)
            .collect();
        let inputs = arrow_response
            .data
            .inputs
            .iter()
            .map(Input::from_arrow)
            .collect();
        let outputs = arrow_response
            .data
            .outputs
            .iter()
            .map(Output::from_arrow)
            .collect();

        QueryResponse {
            archive_height: arrow_response.archive_height,
            next_block: arrow_response.next_block,
            total_execution_time: arrow_response.total_execution_time,
            data: ResponseData {
                blocks,
                transactions,
                receipts,
                inputs,
                outputs,
            },
            // rollback_guard: arrow_response.rollback_guard.clone(),
        }
    }
}

/// Query response from a HyperFuel server.
/// Contain next_block field in case query didn't process all the block range
#[derive(Debug, Clone)]
pub struct QueryResponse<T = ResponseData> {
    /// Current height of the source HyperFuel server.
    pub archive_height: Option<u64>,
    /// Next block to query for, the responses are paginated so
    /// the caller should continue the query from this block if they
    /// didn't get responses up to the to_block they specified in the Query.
    pub next_block: u64,
    /// Total time it took the HyperFuel server to execute the query.
    pub total_execution_time: u64,
    /// Response data
    pub data: T,
    // /// Rollback guard
    // pub rollback_guard: Option<RollbackGuard>,
}

/// Alias for Arrow Query response
pub type ArrowResponse = QueryResponse<ArrowResponseData>;

/// Arrow chunk with schema
#[derive(Debug, Clone)]
pub struct ArrowBatch {
    /// Reference to array chunk
    pub chunk: Arc<ArrowChunk>,
    /// Schema reference for the chunk
    pub schema: SchemaRef,
}

impl ArrowBatch {
    /// Extract column from chunk by name
    pub fn column<T: 'static>(&self, name: &str) -> Result<&T> {
        match self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == name)
        {
            Some((idx, _)) => {
                let col = self
                    .chunk
                    .columns()
                    .get(idx)
                    .context("get column using index")?;
                let col = col.as_any().downcast_ref::<T>().with_context(|| {
                    anyhow!(
                        "cast type of column '{}', it was {:?}",
                        name,
                        col.data_type()
                    )
                })?;
                Ok(col)
            }
            None => Err(anyhow!("field {} not found in schema", name)),
        }
    }
}

/// Typed response from [`Client::preset_query_get_logs`](crate::Client::preset_query_get_logs).
#[derive(Debug, Clone)]
pub struct LogResponse {
    /// Current archive height of the HyperFuel server, if reported.
    pub archive_height: Option<u64>,
    /// Next block to query when paginating.
    pub next_block: u64,
    /// Server-side query execution time in milliseconds.
    pub total_execution_time: u64,
    /// Matching receipt rows, suitable for decoding Log / LogData payloads.
    pub data: Vec<LogContext>,
}

/// Receipt columns returned by the preset logs query (subset of [`Receipt`]).
#[derive(Debug, Clone)]
pub struct LogContext {
    /// Block height containing this receipt.
    pub block_height: UInt,
    /// Transaction id for this receipt.
    pub tx_id: Hash,
    /// Index of the receipt within the transaction.
    pub receipt_index: UInt,
    /// Receipt discriminant (for example Log vs LogData).
    pub receipt_type: ReceiptType,
    /// Contract id when applicable.
    pub contract_id: Option<Hash>,
    /// Root contract id when applicable.
    pub root_contract_id: Option<Hash>,
    /// Register `ra`.
    pub ra: Option<UInt>,
    /// Register `rb`.
    pub rb: Option<UInt>,
    /// Register `rc`.
    pub rc: Option<UInt>,
    /// Register `rd`.
    pub rd: Option<UInt>,
    /// Program counter.
    pub pc: Option<UInt>,
    /// Instruction start.
    pub is: Option<UInt>,
    /// Pointer field used by LogData-style receipts.
    pub ptr: Option<UInt>,
    /// Length field used by LogData-style receipts.
    pub len: Option<UInt>,
    /// Digest field when present.
    pub digest: Option<Hash>,
    /// Raw payload bytes when present.
    pub data: Option<Data>,
}

impl From<Receipt> for LogContext {
    fn from(value: Receipt) -> Self {
        Self {
            block_height: value.block_height,
            tx_id: value.tx_id,
            receipt_index: value.receipt_index,
            receipt_type: value.receipt_type,
            contract_id: value.contract_id,
            root_contract_id: value.root_contract_id,
            ra: value.ra,
            rb: value.rb,
            rc: value.rc,
            rd: value.rd,
            pc: value.pc,
            is: value.is,
            ptr: value.ptr,
            len: value.len,
            digest: value.digest,
            data: value.data,
        }
    }
}
