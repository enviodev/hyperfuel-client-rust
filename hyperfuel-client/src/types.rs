use crate::ArrowChunk;
use anyhow::{anyhow, Result};
use arrow2::datatypes::SchemaRef;
use hyperfuel_format::{
    BlockHeader, Data, Hash, Input, Output, Receipt, ReceiptType, Transaction, UInt,
};

#[derive(Debug, Clone)]
pub struct QueryResponseData {
    pub blocks: Vec<ArrowBatch>,
    pub transactions: Vec<ArrowBatch>,
    pub receipts: Vec<ArrowBatch>,
    pub inputs: Vec<ArrowBatch>,
    pub outputs: Vec<ArrowBatch>,
}

#[derive(Debug, Clone)]
pub struct QueryResponseDataTyped {
    pub blocks: Vec<BlockHeader>,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<Receipt>,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    /// Current height of the source hypersync instance
    pub archive_height: Option<u64>,
    /// Next block to query for, the responses are paginated so
    /// the caller should continue the query from this block if they
    /// didn't get responses up to the to_block they specified in the Query.
    pub next_block: u64,
    /// Total time it took the hypersync instance to execute the query.
    pub total_execution_time: u64,
    /// Response data
    pub data: QueryResponseData,
}

#[derive(Debug, Clone)]
pub struct QueryResponseTyped {
    /// Current height of the source hypersync instance
    pub archive_height: Option<u64>,
    /// Next block to query for, the responses are paginated so
    /// the caller should continue the query from this block if they
    /// didn't get responses up to the to_block they specified in the Query.
    pub next_block: u64,
    /// Total time it took the hypersync instance to execute the query.
    pub total_execution_time: u64,
    /// Response data
    pub data: QueryResponseDataTyped,
}

#[derive(Debug, Clone)]
pub struct ArrowBatch {
    pub chunk: ArrowChunk,
    pub schema: SchemaRef,
}

impl ArrowBatch {
    pub fn column<T: 'static>(&self, name: &str) -> Result<&T> {
        match self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == name)
        {
            Some((idx, _)) => {
                let col = self.chunk.columns()[idx]
                    .as_any()
                    .downcast_ref::<T>()
                    .unwrap();
                Ok(col)
            }
            None => Err(anyhow!("field {} not found in schema", name)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogResponse {
    /// Current height of the source hypersync instance
    pub archive_height: Option<u64>,
    /// Next block to query for, the responses are paginated so
    /// the caller should continue the query from this block if they
    /// didn't get responses up to the to_block they specified in the Query.
    pub next_block: u64,
    /// Total time it took the hypersync instance to execute the query.
    pub total_execution_time: u64,
    /// Response data
    pub data: Vec<LogContext>,
}

/// Contains all the fields needed for decoding plus some additional fields
/// for context.
#[derive(Debug, Clone)]
pub struct LogContext {
    pub block_height: UInt,
    pub tx_id: Hash,
    pub receipt_index: UInt,
    pub receipt_type: ReceiptType,
    pub contract_id: Option<Hash>,
    pub root_contract_id: Option<Hash>,
    pub ra: Option<UInt>,
    pub rb: Option<UInt>,
    pub rc: Option<UInt>,
    pub rd: Option<UInt>,
    pub pc: Option<UInt>,
    pub is: Option<UInt>,
    pub ptr: Option<UInt>,
    pub len: Option<UInt>,
    pub digest: Option<Hash>,
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
