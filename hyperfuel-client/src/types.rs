use std::sync::Arc;

use crate::{ArrowChunk, FromArrow};
use anyhow::{anyhow, Context, Result};
use hyperfuel_format::{BlockHeader, Input, Output, Receipt, Transaction};
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

/// Query response from hypersync instance.
/// Contain next_block field in case query didn't process all the block range
#[derive(Debug, Clone)]
pub struct QueryResponse<T = ResponseData> {
    /// Current height of the source hypersync instance
    pub archive_height: Option<u64>,
    /// Next block to query for, the responses are paginated so
    /// the caller should continue the query from this block if they
    /// didn't get responses up to the to_block they specified in the Query.
    pub next_block: u64,
    /// Total time it took the hypersync instance to execute the query.
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
