use crate::{types::QueryResponseDataTyped, ArrowBatch, QueryResponseData};

use anyhow::{Context, Result};
use arrow2::array::{BinaryArray, Int64Array, UInt64Array, UInt8Array, Utf8Array};
use hyperfuel_format::{
    BlockHeader, Input, InputType, Output, OutputType, Receipt, ReceiptType, Transaction,
    TransactionStatus, TransactionType,
};

pub fn receipts_from_arrow_data(receipts: &[ArrowBatch]) -> Result<Vec<Receipt>> {
    let mut typed_receipts: Vec<Receipt> = vec![];
    for batch in receipts {
        let data = Receipt::from_arrow(batch).context("receipt from arrow")?;
        for receipt in data {
            typed_receipts.push(receipt);
        }
    }

    Ok(typed_receipts)
}

pub fn typed_data_from_arrow_data(data: QueryResponseData) -> Result<QueryResponseDataTyped> {
    let mut blocks = vec![];
    for batch in data.blocks.iter() {
        let data = BlockHeader::from_arrow(batch).context("map blocks from arrow")?;
        for block in data {
            blocks.push(block)
        }
    }

    let mut transactions = vec![];
    for batch in data.transactions.iter() {
        let data = Transaction::from_arrow(batch).context("transaction from arrow")?;
        for transaction in data {
            transactions.push(transaction);
        }
    }

    let mut receipts = vec![];
    for batch in data.receipts.iter() {
        let data = Receipt::from_arrow(batch).context("map receipts from arrow")?;
        for receipt in data {
            receipts.push(receipt);
        }
    }

    let mut inputs = vec![];
    for batch in data.inputs.iter() {
        let data = Input::from_arrow(batch).context("map inputs from arrow")?;
        for input in data {
            inputs.push(input);
        }
    }

    let mut outputs = vec![];
    for batch in data.outputs.iter() {
        let data = Output::from_arrow(batch).context("map outputs from arrow")?;
        for output in data {
            outputs.push(output);
        }
    }

    Ok(QueryResponseDataTyped {
        blocks,
        transactions,
        receipts,
        inputs,
        outputs,
    })
}

pub trait FromArrow: Sized {
    fn from_arrow(batch: &ArrowBatch) -> Result<Vec<Self>>;
}

impl FromArrow for BlockHeader {
    fn from_arrow(batch: &ArrowBatch) -> Result<Vec<Self>> {
        let mut out: Vec<Self> = vec![Default::default(); batch.chunk.len()];

        if let Ok(col) = batch.column::<BinaryArray<i32>>("id") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.id = val.try_into().unwrap();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("da_height") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.da_height = val.into();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("transactions_count") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.transactions_count = val.to_be_bytes().into();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("message_receipt_count") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.message_receipt_count = val.to_be_bytes().into();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("transactions_root") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.transactions_root = val.try_into().unwrap();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("message_receipt_root") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.message_receipt_root = val.try_into().unwrap();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("height") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.height = val.into();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("prev_root") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.prev_root = val.try_into().unwrap();
            }
        }

        if let Ok(col) = batch.column::<Int64Array>("time") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.time = (val as u64).into();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("application_hash") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.application_hash = val.try_into().unwrap();
            }
        }

        Ok(out)
    }
}

impl FromArrow for Transaction {
    fn from_arrow(batch: &ArrowBatch) -> Result<Vec<Self>> {
        let mut out: Vec<Self> = vec![Default::default(); batch.chunk.len()];

        if let Ok(col) = batch.column::<UInt64Array>("block_height") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.block_height = val.into();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("id") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.id = val.try_into().unwrap();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("input_asset_ids") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_asset_ids = val.map(|val| {
                    val.chunks(32)
                        .map(|chunk| chunk.try_into().unwrap())
                        .collect()
                });
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("input_contracts") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contracts = val.map(|val| {
                    val.chunks(32)
                        .map(|chunk| chunk.try_into().unwrap())
                        .collect()
                });
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("input_contract_utxo_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contract_utxo_id = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("input_contract_balance_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contract_balance_root = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("input_contract_state_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contract_state_root = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("input_contract_tx_pointer_block_height") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contract_tx_pointer_block_height = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("input_contract_tx_pointer_tx_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contract_tx_pointer_tx_index = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("input_contract") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_contract = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("gas_price") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.gas_price = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("gas_limit") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.gas_limit = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("maturity") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.maturity = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("mint_amount") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.mint_amount = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("mint_asset_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.mint_asset_id = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("tx_pointer_block_height") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.tx_pointer_block_height = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("tx_pointer_tx_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.tx_pointer_tx_index = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<UInt8Array>("tx_type") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.tx_type = TransactionType::from_u8(*val).unwrap();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("output_contract_input_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.output_contract_input_index = val.copied().map(|n| n.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("output_contract_balance_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.output_contract_balance_root = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("output_contract_state_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.output_contract_state_root = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("witnesses") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.witnesses = val.map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("receipts_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.receipts_root = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<UInt8Array>("status") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.status = TransactionStatus::from_u8(*val).unwrap();
            }
        }

        if let Ok(col) = batch.column::<Int64Array>("time") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.time = (val as u64).into();
            }
        }

        if let Ok(col) = batch.column::<Utf8Array<i32>>("reason") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.reason = val.map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("script") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.script = val.map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("script_data") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.script_data = val.map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("bytecode_witness_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.bytecode_witness_index = val.copied().map(|t| t.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("bytecode_length") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.bytecode_length = val.copied().map(|t| t.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("salt") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.salt = val.map(|v| v.into());
            }
        }

        Ok(out)
    }
}

impl FromArrow for Receipt {
    fn from_arrow(batch: &ArrowBatch) -> Result<Vec<Self>> {
        let mut out: Vec<Self> = vec![Default::default(); batch.chunk.len()];

        if let Ok(col) = batch.column::<UInt64Array>("receipt_index") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.receipt_index = val.into();
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("root_contract_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.root_contract_id = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("tx_id") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.tx_id = val.try_into().unwrap();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("block_height") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.block_height = val.into();
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("pc") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.pc = val.copied().map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("is") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.is = val.copied().map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("to") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.to = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<BinaryArray<i32>>("to_address") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.to_address = val.map(|v| v.try_into().unwrap());
            }
        }

        // UInt64 cols
        if let Ok(col) = batch.column::<UInt64Array>("amount") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.amount = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("gas") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.gas = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("param1") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.param1 = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("param2") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.param2 = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("val") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.val = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("ptr") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.ptr = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("reason") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.reason = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("ra") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.ra = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("rb") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.rb = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("rc") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.rc = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("rd") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.rd = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("len") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.len = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("result") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.result = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("gas_used") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.gas_used = val.copied().map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<UInt8Array>("receipt_type") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.receipt_type = ReceiptType::from_u8(val).unwrap();
            }
        }

        // binary cols
        if let Ok(col) = batch.column::<BinaryArray<i32>>("asset_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.asset_id = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("digest") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.digest = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("data") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.data = val.map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("sender") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.sender = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("recipient") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.recipient = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("nonce") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.nonce = val.map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("contract_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.contract_id = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("sub_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.sub_id = val.map(|v| v.try_into().unwrap());
            }
        }

        Ok(out)
    }
}

impl FromArrow for Input {
    fn from_arrow(batch: &ArrowBatch) -> Result<Vec<Self>> {
        let mut out: Vec<Self> = vec![Default::default(); batch.chunk.len()];

        if let Ok(col) = batch.column::<BinaryArray<i32>>("tx_id") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.tx_id = val.try_into().unwrap();
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("utxo_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.utxo_id = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("owner") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.owner = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("asset_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.asset_id = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("predicate") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.predicate = val.map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("predicate_data") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.predicate_data = val.map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("balance_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.balance_root = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("state_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.state_root = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("contract") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.contract = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("sender") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.sender = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("recipient") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.recipient = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("nonce") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.nonce = val.map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("data") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.data = val.map(|v| v.into());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("block_height") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.block_height = val.into();
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("amount") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.amount = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("tx_pointer_block_height") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.tx_pointer_block_height = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("tx_pointer_tx_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.tx_pointer_tx_index = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("witness_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.witness_index = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("predicate_gas_used") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.predicate_gas_used = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt8Array>("input_type") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.input_type = InputType::from_u8(val).unwrap();
            }
        }

        Ok(out)
    }
}

impl FromArrow for Output {
    fn from_arrow(batch: &ArrowBatch) -> Result<Vec<Self>> {
        let mut out: Vec<Self> = vec![Default::default(); batch.chunk.len()];
        if let Ok(col) = batch.column::<BinaryArray<i32>>("tx_id") {
            for (target, val) in out.iter_mut().zip(col.values_iter()) {
                target.tx_id = val.try_into().unwrap();
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("to") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.to = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("asset_id") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.asset_id = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("balance_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.balance_root = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("state_root") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.state_root = val.map(|v| v.try_into().unwrap());
            }
        }
        if let Ok(col) = batch.column::<BinaryArray<i32>>("contract") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.contract = val.map(|v| v.try_into().unwrap());
            }
        }

        if let Ok(col) = batch.column::<UInt64Array>("block_height") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.block_height = val.into();
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("amount") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.amount = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt64Array>("input_index") {
            for (target, val) in out.iter_mut().zip(col.iter()) {
                target.input_index = val.copied().map(|v| v.into());
            }
        }
        if let Ok(col) = batch.column::<UInt8Array>("output_type") {
            for (target, &val) in out.iter_mut().zip(col.values_iter()) {
                target.output_type = OutputType::from_u8(val).unwrap();
            }
        }
        Ok(out)
    }
}
