use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use polars_arrow::array::{new_empty_array, Array, BinaryArray, Utf8Array};
use polars_arrow::compute;
use polars_arrow::datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field, SchemaRef};
use polars_arrow::record_batch::RecordBatchT as Chunk;

mod util;

pub use util::{project_schema, try_project_schema};

pub type ArrowChunk = Chunk<Box<dyn Array>>;

pub fn block_header() -> SchemaRef {
    Schema::from(vec![
        Field::new("id", DataType::BinaryView, false),
        Field::new("da_height", DataType::UInt64, false),
        Field::new("consensus_parameters_version", DataType::UInt64, false), // new
        Field::new("state_transition_bytecode_version", DataType::UInt64, false), // new
        Field::new("transactions_count", DataType::UInt64, false),
        Field::new("message_receipt_count", DataType::UInt64, false),
        Field::new("transactions_root", DataType::BinaryView, false),
        Field::new("message_outbox_root", DataType::BinaryView, false), // renamed
        Field::new("event_inbox_root", DataType::BinaryView, false),    // new
        Field::new("height", DataType::UInt64, false),
        Field::new("prev_root", DataType::BinaryView, false),
        Field::new("time", DataType::Int64, false),
        Field::new("application_hash", DataType::BinaryView, false),
    ])
    .into()
}

pub fn transaction() -> SchemaRef {
    Schema::from(vec![
        // block number
        Field::new("block_height", DataType::UInt64, false),
        Field::new("id", DataType::BinaryView, false),
        // vec
        Field::new("input_asset_ids", DataType::BinaryView, true),
        // vec
        Field::new("input_contracts", DataType::BinaryView, true),
        Field::new("input_contract_utxo_id", DataType::BinaryView, true),
        Field::new("input_contract_balance_root", DataType::BinaryView, true),
        Field::new("input_contract_state_root", DataType::BinaryView, true),
        Field::new(
            "input_contract_tx_pointer_block_height",
            DataType::UInt64,
            true,
        ),
        Field::new("input_contract_tx_pointer_tx_index", DataType::UInt64, true),
        Field::new("input_contract", DataType::BinaryView, true),
        // Field::new("gas_price", DataType::UInt64, true), // removed
        Field::new("policies_tip", DataType::UInt64, true), // new
        Field::new("policies_witness_limit", DataType::UInt64, true), // new
        Field::new("policies_maturity", DataType::UInt64, true), // new
        Field::new("policies_max_fee", DataType::UInt64, true), // new
        Field::new("script_gas_limit", DataType::UInt64, true), // renamed
        Field::new("maturity", DataType::UInt64, true),
        Field::new("mint_amount", DataType::UInt64, true),
        Field::new("mint_asset_id", DataType::BinaryView, true),
        Field::new("mint_gas_price", DataType::UInt64, true), // new
        Field::new("tx_pointer_block_height", DataType::UInt64, true),
        Field::new("tx_pointer_tx_index", DataType::UInt64, true),
        Field::new("tx_type", DataType::UInt8, false), // not changes, but new tx_types (upgrade, upload)
        Field::new("output_contract_input_index", DataType::UInt64, true),
        Field::new("output_contract_balance_root", DataType::BinaryView, true),
        Field::new("output_contract_state_root", DataType::BinaryView, true),
        // vec
        Field::new("witnesses", DataType::BinaryView, true),
        Field::new("receipts_root", DataType::BinaryView, true),
        Field::new("status", DataType::UInt8, false),
        Field::new("time", DataType::Int64, false),
        Field::new("reason", DataType::Utf8View, true),
        Field::new("script", DataType::BinaryView, true),
        Field::new("script_data", DataType::BinaryView, true),
        Field::new("bytecode_witness_index", DataType::UInt64, true),
        // Field::new("bytecode_length", DataType::UInt64, true), // removed
        Field::new("bytecode_root", DataType::BinaryView, true), // new
        Field::new("subsection_index", DataType::UInt64, true),  // new
        Field::new("subsections_number", DataType::UInt64, true), // new
        // vec
        // Field::new("storage_slots", DataType::Binary, true), // new
        // vec
        Field::new("proof_set", DataType::BinaryView, true), // new
        Field::new(
            "consensus_parameters_upgrade_purpose_witness_index",
            DataType::UInt64,
            true,
        ), // new
        Field::new(
            "consensus_parameters_upgrade_purpose_checksum",
            DataType::BinaryView,
            true,
        ), // new
        Field::new(
            "state_transition_upgrade_purpose_root",
            DataType::BinaryView,
            true,
        ), // new
        Field::new("salt", DataType::BinaryView, true),
    ])
    .into()
}

pub fn receipt() -> SchemaRef {
    Schema::from(vec![
        // receipt index is unique per block
        Field::new("receipt_index", DataType::UInt64, false),
        Field::new("root_contract_id", DataType::BinaryView, true),
        Field::new("tx_id", DataType::BinaryView, false),
        Field::new("tx_status", DataType::UInt8, false), // new
        Field::new("tx_type", DataType::UInt8, false),   // new
        Field::new("block_height", DataType::UInt64, false),
        Field::new("pc", DataType::UInt64, true),
        Field::new("is", DataType::UInt64, true),
        Field::new("to", DataType::BinaryView, true),
        Field::new("to_address", DataType::BinaryView, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("asset_id", DataType::BinaryView, true),
        Field::new("gas", DataType::UInt64, true),
        Field::new("param1", DataType::UInt64, true),
        Field::new("param2", DataType::UInt64, true),
        Field::new("val", DataType::UInt64, true),
        Field::new("ptr", DataType::UInt64, true),
        Field::new("digest", DataType::BinaryView, true),
        Field::new("reason", DataType::UInt64, true),
        Field::new("ra", DataType::UInt64, true),
        Field::new("rb", DataType::UInt64, true),
        Field::new("rc", DataType::UInt64, true),
        Field::new("rd", DataType::UInt64, true),
        Field::new("len", DataType::UInt64, true),
        Field::new("receipt_type", DataType::UInt8, false),
        Field::new("result", DataType::UInt64, true),
        Field::new("gas_used", DataType::UInt64, true),
        Field::new("data", DataType::BinaryView, true),
        Field::new("sender", DataType::BinaryView, true),
        Field::new("recipient", DataType::BinaryView, true),
        Field::new("nonce", DataType::BinaryView, true),
        Field::new("contract_id", DataType::BinaryView, true),
        Field::new("sub_id", DataType::BinaryView, true),
    ])
    .into()
}

pub fn input() -> SchemaRef {
    Schema::from(vec![
        // for mapping
        Field::new("tx_id", DataType::BinaryView, false),
        Field::new("tx_status", DataType::UInt8, false), // new
        Field::new("tx_type", DataType::UInt8, false),   // new
        Field::new("block_height", DataType::UInt64, false),
        Field::new("input_type", DataType::UInt8, false),
        Field::new("utxo_id", DataType::BinaryView, true),
        Field::new("owner", DataType::BinaryView, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("asset_id", DataType::BinaryView, true),
        Field::new("tx_pointer_block_height", DataType::UInt64, true),
        Field::new("tx_pointer_tx_index", DataType::UInt64, true),
        Field::new("witness_index", DataType::UInt64, true),
        Field::new("predicate_gas_used", DataType::UInt64, true),
        Field::new("predicate", DataType::BinaryView, true),
        Field::new("predicate_data", DataType::BinaryView, true),
        Field::new("balance_root", DataType::BinaryView, true),
        Field::new("state_root", DataType::BinaryView, true),
        Field::new("contract", DataType::BinaryView, true),
        Field::new("sender", DataType::BinaryView, true),
        Field::new("recipient", DataType::BinaryView, true),
        Field::new("nonce", DataType::BinaryView, true),
        Field::new("data", DataType::BinaryView, true),
    ])
    .into()
}

pub fn output() -> SchemaRef {
    Schema::from(vec![
        // for mapping
        Field::new("tx_id", DataType::BinaryView, false),
        Field::new("tx_status", DataType::UInt8, false), // new
        Field::new("tx_type", DataType::UInt8, false),   // new
        Field::new("block_height", DataType::UInt64, false),
        Field::new("output_type", DataType::UInt8, false),
        Field::new("to", DataType::BinaryView, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("asset_id", DataType::BinaryView, true),
        Field::new("input_index", DataType::UInt64, true),
        Field::new("balance_root", DataType::BinaryView, true),
        Field::new("state_root", DataType::BinaryView, true),
        Field::new("contract", DataType::BinaryView, true),
    ])
    .into()
}

pub fn concat_chunks(chunks: &[Arc<ArrowChunk>]) -> Result<ArrowChunk> {
    if chunks.is_empty() {
        return Err(anyhow!("can't concat 0 chunks"));
    }

    let num_cols = chunks[0].columns().len();

    let cols = (0..num_cols)
        .map(|col| {
            let mut is_utf8 = false;
            let arrs = chunks
                .iter()
                .map(|chunk| {
                    let col = chunk
                        .columns()
                        .get(col)
                        .map(|col| col.as_ref())
                        .context("get column")?;
                    is_utf8 = col.data_type() == &DataType::Utf8;
                    Ok(col)
                })
                .collect::<Result<Vec<_>>>()?;
            if !is_utf8 {
                compute::concatenate::concatenate(&arrs).context("concat arrays")
            } else {
                let arrs = arrs
                    .into_iter()
                    .map(|a| {
                        a.as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap()
                            .to_binary()
                            .boxed()
                    })
                    .collect::<Vec<_>>();
                let arrs = arrs.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                let arr =
                    compute::concatenate::concatenate(arrs.as_slice()).context("concat arrays")?;

                Ok(compute::cast::binary_to_utf8(
                    arr.as_any().downcast_ref::<BinaryArray<i32>>().unwrap(),
                    DataType::Utf8,
                )
                .unwrap()
                .boxed())
            }
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

    #[test]
    fn test_concat_utf8() {
        let chunks = [
            Arc::new(Chunk::new(vec![Utf8Array::<i32>::from(&[Some(
                "hello".to_owned(),
            )])
            .boxed()])),
            Arc::new(Chunk::new(vec![Utf8Array::<i32>::from(&[Some(
                "world".to_owned(),
            )])
            .boxed()])),
        ];

        let out = concat_chunks(&chunks).unwrap();

        assert_eq!(
            out,
            ArrowChunk::new(vec![Utf8Array::<i32>::from(&[
                Some("hello".to_owned()),
                Some("world".to_owned())
            ])
            .boxed(),])
        )
    }
}
