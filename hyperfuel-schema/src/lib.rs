use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow2::array::{new_empty_array, Array};
use arrow2::chunk::Chunk;
use arrow2::compute;
use arrow2::datatypes::{DataType, Field, Schema, SchemaRef};

mod util;

pub use util::project_schema;

pub type ArrowChunk = Chunk<Box<dyn Array>>;

pub fn block_header() -> SchemaRef {
    Schema::from(vec![
        Field::new("id", DataType::Binary, false),
        Field::new("da_height", DataType::UInt64, false),
        Field::new("consensus_parameters_version", DataType::UInt64, false), // new
        Field::new("state_transition_bytecode_version", DataType::UInt64, false), // new
        Field::new("transactions_count", DataType::UInt64, false),
        Field::new("message_receipt_count", DataType::UInt64, false),
        Field::new("transactions_root", DataType::Binary, false),
        Field::new("message_outbox_root", DataType::Binary, false), // renamed
        Field::new("event_inbox_root", DataType::Binary, false),    // new
        Field::new("height", DataType::UInt64, false),
        Field::new("prev_root", DataType::Binary, false),
        Field::new("time", DataType::Int64, false),
        Field::new("application_hash", DataType::Binary, false),
    ])
    .into()
}

pub fn transaction() -> SchemaRef {
    Schema::from(vec![
        // block number
        Field::new("block_height", DataType::UInt64, false),
        Field::new("id", DataType::Binary, false),
        // vec
        Field::new("input_asset_ids", DataType::Binary, true),
        // vec
        Field::new("input_contracts", DataType::Binary, true),
        Field::new("input_contract_utxo_id", DataType::Binary, true),
        Field::new("input_contract_balance_root", DataType::Binary, true),
        Field::new("input_contract_state_root", DataType::Binary, true),
        Field::new(
            "input_contract_tx_pointer_block_height",
            DataType::UInt64,
            true,
        ),
        Field::new("input_contract_tx_pointer_tx_index", DataType::UInt64, true),
        Field::new("input_contract", DataType::Binary, true),
        // Field::new("gas_price", DataType::UInt64, true), // removed
        Field::new("policies_tip", DataType::UInt64, true), // new
        Field::new("policies_witness_limit", DataType::UInt64, true), // new
        Field::new("policies_maturity", DataType::UInt64, true), // new
        Field::new("policies_max_fee", DataType::UInt64, true), // new
        Field::new("script_gas_limit", DataType::UInt64, true), // renamed
        Field::new("maturity", DataType::UInt64, true),
        Field::new("mint_amount", DataType::UInt64, true),
        Field::new("mint_asset_id", DataType::Binary, true),
        Field::new("mint_gas_price", DataType::UInt64, true), // new
        Field::new("tx_pointer_block_height", DataType::UInt64, true),
        Field::new("tx_pointer_tx_index", DataType::UInt64, true),
        Field::new("tx_type", DataType::UInt8, false), // not changes, but new tx_types (upgrade, upload)
        Field::new("output_contract_input_index", DataType::UInt64, true),
        Field::new("output_contract_balance_root", DataType::Binary, true),
        Field::new("output_contract_state_root", DataType::Binary, true),
        // vec
        Field::new("witnesses", DataType::Binary, true),
        Field::new("receipts_root", DataType::Binary, true),
        Field::new("status", DataType::UInt8, true),
        Field::new("time", DataType::Int64, false),
        Field::new("reason", DataType::Utf8, true),
        Field::new("script", DataType::Binary, true),
        Field::new("script_data", DataType::Binary, true),
        Field::new("bytecode_witness_index", DataType::UInt64, true),
        // Field::new("bytecode_length", DataType::UInt64, true), // removed
        Field::new("bytecode_root", DataType::Binary, true), // new
        Field::new("subsection_index", DataType::UInt64, true), // new
        Field::new("subsections_number", DataType::UInt64, true), // new
        // vec
        // Field::new("storage_slots", DataType::Binary, true), // new
        // vec
        Field::new("proof_set", DataType::Binary, true), // new
        Field::new(
            "consensus_parameters_upgrade_purpose_witness_index",
            DataType::UInt64,
            true,
        ), // new
        Field::new(
            "consensus_parameters_upgrade_purpose_checksum",
            DataType::Binary,
            true,
        ), // new
        Field::new(
            "state_transition_upgrade_purpose_root",
            DataType::Binary,
            true,
        ), // new
        Field::new("salt", DataType::Binary, true),
    ])
    .into()
}

pub fn receipt() -> SchemaRef {
    Schema::from(vec![
        // receipt index is unique per block
        Field::new("receipt_index", DataType::UInt64, false),
        Field::new("root_contract_id", DataType::Binary, true),
        Field::new("tx_id", DataType::Binary, false),
        Field::new("block_height", DataType::UInt64, false),
        Field::new("pc", DataType::UInt64, true),
        Field::new("is", DataType::UInt64, true),
        Field::new("to", DataType::Binary, true),
        Field::new("to_address", DataType::Binary, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("asset_id", DataType::Binary, true),
        Field::new("gas", DataType::UInt64, true),
        Field::new("param1", DataType::UInt64, true),
        Field::new("param2", DataType::UInt64, true),
        Field::new("val", DataType::UInt64, true),
        Field::new("ptr", DataType::UInt64, true),
        Field::new("digest", DataType::Binary, true),
        Field::new("reason", DataType::UInt64, true),
        Field::new("ra", DataType::UInt64, true),
        Field::new("rb", DataType::UInt64, true),
        Field::new("rc", DataType::UInt64, true),
        Field::new("rd", DataType::UInt64, true),
        Field::new("len", DataType::UInt64, true),
        Field::new("receipt_type", DataType::UInt8, false),
        Field::new("result", DataType::UInt64, true),
        Field::new("gas_used", DataType::UInt64, true),
        Field::new("data", DataType::Binary, true),
        Field::new("sender", DataType::Binary, true),
        Field::new("recipient", DataType::Binary, true),
        Field::new("nonce", DataType::Binary, true),
        Field::new("contract_id", DataType::Binary, true),
        Field::new("sub_id", DataType::Binary, true),
    ])
    .into()
}

pub fn input() -> SchemaRef {
    Schema::from(vec![
        // for mapping
        Field::new("tx_id", DataType::Binary, false),
        Field::new("block_height", DataType::UInt64, false),
        Field::new("input_type", DataType::UInt8, false),
        Field::new("utxo_id", DataType::Binary, true),
        Field::new("owner", DataType::Binary, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("asset_id", DataType::Binary, true),
        Field::new("tx_pointer_block_height", DataType::UInt64, true),
        Field::new("tx_pointer_tx_index", DataType::UInt64, true),
        Field::new("witness_index", DataType::UInt64, true),
        Field::new("predicate_gas_used", DataType::UInt64, true),
        Field::new("predicate", DataType::Binary, true),
        Field::new("predicate_data", DataType::Binary, true),
        Field::new("balance_root", DataType::Binary, true),
        Field::new("state_root", DataType::Binary, true),
        Field::new("contract", DataType::Binary, true),
        Field::new("sender", DataType::Binary, true),
        Field::new("recipient", DataType::Binary, true),
        Field::new("nonce", DataType::Binary, true),
        Field::new("data", DataType::Binary, true),
    ])
    .into()
}

pub fn output() -> SchemaRef {
    Schema::from(vec![
        // for mapping
        Field::new("tx_id", DataType::Binary, false),
        Field::new("block_height", DataType::UInt64, false),
        Field::new("output_type", DataType::UInt8, false),
        Field::new("to", DataType::Binary, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("asset_id", DataType::Binary, true),
        Field::new("input_index", DataType::UInt64, true),
        Field::new("balance_root", DataType::Binary, true),
        Field::new("state_root", DataType::Binary, true),
        Field::new("contract", DataType::Binary, true),
    ])
    .into()
}

/*
pub fn block_header() -> SchemaRef {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", hash_dt(), false),
        Field::new("parent_hash", hash_dt(), false),
        Field::new("nonce", DataType::Binary, true),
        Field::new("sha3_uncles", hash_dt(), false),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("transactions_root", hash_dt(), false),
        Field::new("state_root", hash_dt(), false),
        Field::new("receipts_root", hash_dt(), false),
        Field::new("miner", addr_dt(), false),
        Field::new("difficulty", quantity_dt(), true),
        Field::new("total_difficulty", quantity_dt(), true),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("size", quantity_dt(), false),
        Field::new("gas_limit", quantity_dt(), false),
        Field::new("gas_used", quantity_dt(), false),
        Field::new("timestamp", quantity_dt(), false),
        Field::new("uncles", DataType::Binary, true),
        Field::new("base_fee_per_gas", quantity_dt(), true),
    ])
    .into()
}

pub fn transaction() -> SchemaRef {
    Schema::from(vec![
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", addr_dt(), true),
        Field::new("gas", quantity_dt(), false),
        Field::new("gas_price", quantity_dt(), true),
        Field::new("hash", hash_dt(), false),
        Field::new("input", DataType::Binary, false),
        Field::new("nonce", quantity_dt(), false),
        Field::new("to", addr_dt(), true),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("value", quantity_dt(), false),
        Field::new("v", quantity_dt(), true),
        Field::new("r", quantity_dt(), true),
        Field::new("s", quantity_dt(), true),
        Field::new("max_priority_fee_per_gas", quantity_dt(), true),
        Field::new("max_fee_per_gas", quantity_dt(), true),
        Field::new("chain_id", quantity_dt(), true),
        Field::new("cumulative_gas_used", quantity_dt(), false),
        Field::new("effective_gas_price", quantity_dt(), false),
        Field::new("gas_used", quantity_dt(), false),
        Field::new("contract_address", addr_dt(), true),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("type", DataType::UInt8, true),
        Field::new("root", hash_dt(), true),
        Field::new("status", DataType::UInt8, true),
        Field::new("sighash", DataType::Binary, true),
    ])
    .into()
}

pub fn log() -> SchemaRef {
    Schema::from(vec![
        Field::new("removed", DataType::Boolean, true),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("transaction_hash", hash_dt(), false),
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("address", addr_dt(), false),
        Field::new("data", DataType::Binary, false),
        Field::new("topic0", DataType::Binary, true),
        Field::new("topic1", DataType::Binary, true),
        Field::new("topic2", DataType::Binary, true),
        Field::new("topic3", DataType::Binary, true),
    ])
    .into()
}
*/

pub fn concat_chunks(chunks: &[Arc<ArrowChunk>]) -> Result<ArrowChunk> {
    if chunks.is_empty() {
        return Err(anyhow!("can't concat 0 chunks"));
    }

    let num_cols = chunks[0].columns().len();

    let cols = (0..num_cols)
        .map(|col| {
            let arrs = chunks
                .iter()
                .map(|chunk| {
                    chunk
                        .columns()
                        .get(col)
                        .map(|col| col.as_ref())
                        .context("get column")
                })
                .collect::<Result<Vec<_>>>()?;
            compute::concatenate::concatenate(&arrs).context("concat arrays")
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowChunk::new(cols))
}

pub fn empty_chunk(schema: &Schema) -> ArrowChunk {
    let mut cols = Vec::new();
    for field in schema.fields.iter() {
        cols.push(new_empty_array(field.data_type().clone()));
    }
    ArrowChunk::new(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test_schema_constructors() {
        block_header();
        transaction();
        receipt();
        input();
        output();
    }
}
