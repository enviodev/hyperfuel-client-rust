use anyhow::{Context, Result};
use arrow2::{
    array::{BinaryArray, BooleanArray, MutableBooleanArray, UInt64Array, UInt8Array},
    bitmap::Bitmap,
    compute::{self, filter::filter_chunk},
    datatypes::DataType,
};
use hyperfuel_net_types::{InputSelection, OutputSelection, Query, ReceiptSelection};
use std::collections::HashSet as StdHashSet;
use xxhash_rust::xxh3::Xxh3Builder;

use crate::{ArrowBatch, QueryResponseData};

pub type FastSet<T> = StdHashSet<T, Xxh3Builder>;

// filters out receipts, inputs, and outputs if the don't match the selection
// doesn't modify blocks or transactions
pub fn filter_out_unselected_data(
    data: QueryResponseData,
    query: &Query,
) -> Result<QueryResponseData> {
    let filtered_receipts = if !query.receipts.is_empty() {
        let mut filtered_receipts = vec![];
        for batch in data.receipts {
            let filter = receipt_selections_to_filter(&batch, &query.receipts)
                .context("build receipt selections filter")?;

            let filtered_chunk =
                filter_chunk(&batch.chunk, &filter).context("apply filter to chunk")?;

            filtered_receipts.push(ArrowBatch {
                chunk: filtered_chunk,
                schema: batch.schema.clone(),
            })
        }
        filtered_receipts
    } else {
        data.receipts
    };

    let filtered_inputs = if !query.inputs.is_empty() {
        let mut filtered_inputs = vec![];
        for batch in data.inputs {
            let filter = input_selections_to_filter(&batch, &query.inputs)
                .context("build input selections filter")?;

            let filtered_chunk =
                filter_chunk(&batch.chunk, &filter).context("apply filter to chunk")?;

            filtered_inputs.push(ArrowBatch {
                chunk: filtered_chunk,
                schema: batch.schema.clone(),
            })
        }
        filtered_inputs
    } else {
        data.inputs
    };

    let filtered_outputs = if !query.outputs.is_empty() {
        let mut filtered_outputs = vec![];
        for batch in data.outputs {
            let filter = output_selections_to_filter(&batch, &query.outputs)
                .context("build output selections filter")?;

            let filtered_chunk =
                filter_chunk(&batch.chunk, &filter).context("apply filter to chunk")?;

            filtered_outputs.push(ArrowBatch {
                chunk: filtered_chunk,
                schema: batch.schema.clone(),
            })
        }
        filtered_outputs
    } else {
        data.outputs
    };

    Ok(QueryResponseData {
        blocks: data.blocks,
        transactions: data.transactions,
        receipts: filtered_receipts,
        inputs: filtered_inputs,
        outputs: filtered_outputs,
    })
}

fn receipt_selections_to_filter(
    batch: &ArrowBatch,
    selections: &[ReceiptSelection],
) -> Result<BooleanArray> {
    let root_contract_id = batch.column::<BinaryArray<i32>>("root_contract_id").ok();
    let to_address = batch.column::<BinaryArray<i32>>("to_address").ok();
    let asset_id = batch.column::<BinaryArray<i32>>("asset_id").ok();
    let receipt_type = batch.column::<UInt8Array>("receipt_type").ok();
    let sender = batch.column::<BinaryArray<i32>>("sender").ok();
    let recipient = batch.column::<BinaryArray<i32>>("recipient").ok();
    let contract_id = batch.column::<BinaryArray<i32>>("contract_id").ok();
    let ra = batch.column::<UInt64Array>("ra").ok();
    let rb = batch.column::<UInt64Array>("rb").ok();
    let rc = batch.column::<UInt64Array>("rc").ok();
    let rd = batch.column::<UInt64Array>("rd").ok();

    let chunk_len = batch.chunk.len();
    let mut filter = unset_bool_array(chunk_len);

    for selection in selections.iter() {
        let selection = receipt_selection_to_filter(
            root_contract_id,
            to_address,
            asset_id,
            receipt_type,
            sender,
            recipient,
            contract_id,
            ra,
            rb,
            rc,
            rd,
            selection,
            chunk_len,
        );
        filter = compute::boolean::or(&filter, &selection);
    }

    Ok(filter)
}

#[allow(clippy::too_many_arguments)]
fn receipt_selection_to_filter(
    root_contract_id: Option<&BinaryArray<i32>>,
    to_address: Option<&BinaryArray<i32>>,
    asset_id: Option<&BinaryArray<i32>>,
    receipt_type: Option<&UInt8Array>,
    sender: Option<&BinaryArray<i32>>,
    recipient: Option<&BinaryArray<i32>>,
    contract_id: Option<&BinaryArray<i32>>,
    ra: Option<&UInt64Array>,
    rb: Option<&UInt64Array>,
    rc: Option<&UInt64Array>,
    rd: Option<&UInt64Array>,
    selection: &ReceiptSelection,
    chunk_len: usize,
) -> BooleanArray {
    let mut filter = set_bool_array(chunk_len);

    if !selection.root_contract_id.is_empty() && root_contract_id.is_some() {
        let set = selection
            .root_contract_id
            .iter()
            .map(|t| t.as_slice())
            .collect();
        filter = compute::boolean::and(&filter, &in_set_binary(root_contract_id.unwrap(), &set));
    }

    if !selection.to_address.is_empty() && to_address.is_some() {
        let set = selection.to_address.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(to_address.unwrap(), &set));
    }

    if !selection.asset_id.is_empty() && asset_id.is_some() {
        let set = selection.asset_id.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(asset_id.unwrap(), &set));
    }

    if !selection.receipt_type.is_empty() && receipt_type.is_some() {
        let set = selection.receipt_type.to_vec();
        filter = compute::boolean::and(&filter, &in_set_u8(receipt_type.unwrap(), &set));
    }

    if !selection.sender.is_empty() && sender.is_some() {
        let set = selection.sender.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(sender.unwrap(), &set));
    }

    if !selection.recipient.is_empty() && recipient.is_some() {
        let set = selection.recipient.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(recipient.unwrap(), &set));
    }

    if !selection.contract_id.is_empty() && contract_id.is_some() {
        let set = selection.contract_id.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(contract_id.unwrap(), &set));
    }

    if !selection.ra.is_empty() && ra.is_some() {
        let set = selection.ra.iter().copied().collect();
        filter = compute::boolean::and(&filter, &in_set_u64(ra.unwrap(), &set));
    }

    if !selection.rb.is_empty() && rb.is_some() {
        let set = selection.rb.iter().copied().collect();
        filter = compute::boolean::and(&filter, &in_set_u64(rb.unwrap(), &set));
    }

    if !selection.rc.is_empty() && rc.is_some() {
        let set = selection.rc.iter().copied().collect();
        filter = compute::boolean::and(&filter, &in_set_u64(rc.unwrap(), &set));
    }

    if !selection.rd.is_empty() && rd.is_some() {
        let set = selection.rd.iter().copied().collect();
        filter = compute::boolean::and(&filter, &in_set_u64(rd.unwrap(), &set));
    }

    filter
}

fn unset_bool_array(len: usize) -> BooleanArray {
    BooleanArray::new(DataType::Boolean, Bitmap::new_zeroed(len), None)
}

fn set_bool_array(len: usize) -> BooleanArray {
    let num_bytes = (len + 7) / 8 * 8;
    let ones = vec![0xffu8; num_bytes];

    BooleanArray::new(DataType::Boolean, Bitmap::from_u8_vec(ones, len), None)
}

fn in_set_u64(data: &UInt64Array, set: &FastSet<u64>) -> BooleanArray {
    let mut bools = MutableBooleanArray::with_capacity(data.len());

    for val in data.iter() {
        bools.push(val.map(|v| set.contains(v)));
    }

    bools.into()
}

fn in_set_u8(data: &UInt8Array, set: &[u8]) -> BooleanArray {
    let mut bools = MutableBooleanArray::with_capacity(data.len());

    for val in data.iter() {
        bools.push(val.map(|v| set.contains(v)));
    }

    bools.into()
}

fn in_set_binary(data: &BinaryArray<i32>, set: &FastSet<&[u8]>) -> BooleanArray {
    let mut bools = MutableBooleanArray::with_capacity(data.len());

    for val in data.iter() {
        bools.push(val.map(|v| set.contains(v)));
    }

    bools.into()
}

fn input_selections_to_filter(
    batch: &ArrowBatch,
    selections: &[InputSelection],
) -> Result<BooleanArray> {
    let owner = batch.column::<BinaryArray<i32>>("owner").ok();
    let asset_id = batch.column::<BinaryArray<i32>>("asset_id").ok();
    let contract = batch.column::<BinaryArray<i32>>("contract").ok();
    let sender = batch.column::<BinaryArray<i32>>("sender").ok();
    let recipient = batch.column::<BinaryArray<i32>>("recipient").ok();
    let input_type = batch.column::<UInt8Array>("input_type").ok();

    let chunk_len = batch.chunk.len();
    let mut filter = unset_bool_array(chunk_len);

    for selection in selections.iter() {
        let selection = input_selection_to_filter(
            owner, asset_id, contract, sender, recipient, input_type, selection, chunk_len,
        );
        filter = compute::boolean::or(&filter, &selection);
    }

    Ok(filter)
}

#[allow(clippy::too_many_arguments)]
fn input_selection_to_filter(
    owner: Option<&BinaryArray<i32>>,
    asset_id: Option<&BinaryArray<i32>>,
    contract: Option<&BinaryArray<i32>>,
    sender: Option<&BinaryArray<i32>>,
    recipient: Option<&BinaryArray<i32>>,
    input_type: Option<&UInt8Array>,
    selection: &InputSelection,
    chunk_len: usize,
) -> BooleanArray {
    let mut filter = set_bool_array(chunk_len);

    if !selection.owner.is_empty() && owner.is_some() {
        let set = selection.owner.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(owner.unwrap(), &set));
    }

    if !selection.asset_id.is_empty() && asset_id.is_some() {
        let set = selection.asset_id.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(asset_id.unwrap(), &set));
    }

    if !selection.sender.is_empty() && sender.is_some() {
        let set = selection.sender.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(sender.unwrap(), &set));
    }

    if !selection.recipient.is_empty() && recipient.is_some() {
        let set = selection.recipient.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(recipient.unwrap(), &set));
    }

    if !selection.contract.is_empty() && contract.is_some() {
        let set = selection.contract.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(contract.unwrap(), &set));
    }

    if !selection.input_type.is_empty() && input_type.is_some() {
        let set = selection.input_type.to_vec();
        filter = compute::boolean::and(&filter, &in_set_u8(input_type.unwrap(), &set));
    }

    filter
}

fn output_selections_to_filter(
    batch: &ArrowBatch,
    selections: &[OutputSelection],
) -> Result<BooleanArray> {
    let to = batch.column::<BinaryArray<i32>>("to").ok();
    let asset_id = batch.column::<BinaryArray<i32>>("asset_id").ok();
    let contract = batch.column::<BinaryArray<i32>>("contract").ok();
    let output_type = batch.column::<UInt8Array>("output_type").ok();

    let chunk_len = batch.chunk.len();
    let mut filter = unset_bool_array(chunk_len);

    for selection in selections.iter() {
        let selection =
            output_selection_to_filter(to, asset_id, contract, output_type, selection, chunk_len);
        filter = compute::boolean::or(&filter, &selection);
    }

    Ok(filter)
}

fn output_selection_to_filter(
    to: Option<&BinaryArray<i32>>,
    asset_id: Option<&BinaryArray<i32>>,
    contract: Option<&BinaryArray<i32>>,
    output_type: Option<&UInt8Array>,
    selection: &OutputSelection,
    chunk_len: usize,
) -> BooleanArray {
    let mut filter = set_bool_array(chunk_len);

    if !selection.to.is_empty() && to.is_some() {
        let set = selection.to.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(to.unwrap(), &set));
    }

    if !selection.asset_id.is_empty() && asset_id.is_some() {
        let set = selection.asset_id.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(asset_id.unwrap(), &set));
    }

    if !selection.contract.is_empty() && contract.is_some() {
        let set = selection.contract.iter().map(|t| t.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(contract.unwrap(), &set));
    }

    if !selection.output_type.is_empty() && output_type.is_some() {
        let set = selection.output_type.to_vec();
        filter = compute::boolean::and(&filter, &in_set_u8(output_type.unwrap(), &set));
    }

    filter
}
