use polars_arrow::array::{BinaryArray, StaticArray, UInt64Array, UInt8Array, Utf8Array};

use crate::{
    // simple_types::{Block, Log, Trace, Transaction},
    ArrowBatch,
};

use hyperfuel_format::{
    BlockHeader, Input, InputType, Output, OutputType, Receipt, ReceiptType, Transaction,
    TransactionStatus, TransactionType,
};

/// Used to do ArrowBatch-Native Rust type conversions while consuming the input value.
pub trait FromArrow: Sized {
    /// Converts to the Vector type from the ArrowBatch type.
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self>;
}

fn map_binary<'a, T>(i: usize, arr: Option<&'a BinaryArray<i32>>) -> Option<T>
where
    T: TryFrom<&'a [u8]>,
    <T as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    arr.and_then(|arr| arr.get(i).map(|v| v.try_into().unwrap()))
}

// Some unwraps etc that should be improved

impl FromArrow for BlockHeader {
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self> {
        let id = batch.column::<BinaryArray<i32>>("id").ok();
        let da_height = batch.column::<UInt64Array>("da_height").ok();
        let transactions_count = batch.column::<UInt64Array>("transactions_count").ok();
        let consensus_parameters_version = batch
            .column::<UInt64Array>("consensus_parameters_version")
            .ok();
        let state_transition_bytecode_version = batch
            .column::<UInt64Array>("state_transition_bytecode_version")
            .ok();
        let message_receipt_count = batch.column::<UInt64Array>("message_receipt_count").ok();
        let transactions_root = batch.column::<BinaryArray<i32>>("transactions_root").ok();
        let message_outbox_root = batch.column::<BinaryArray<i32>>("message_outbox_root").ok();
        let event_inbox_root = batch.column::<BinaryArray<i32>>("event_inbox_root").ok();
        let height = batch.column::<UInt64Array>("height").ok();
        let prev_root = batch.column::<BinaryArray<i32>>("prev_root").ok();
        let time = batch.column::<UInt64Array>("time").ok();
        let application_hash = batch.column::<BinaryArray<i32>>("application_hash").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                id: map_binary(idx, id).unwrap(),
                da_height: da_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow da_height"),
                transactions_count: transactions_count
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow transactions_count"),
                consensus_parameters_version: consensus_parameters_version
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow da_height"),
                state_transition_bytecode_version: state_transition_bytecode_version
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow state_transition_bytecode_version"),
                message_receipt_count: message_receipt_count
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow message_receipt_count"),
                transactions_root: map_binary(idx, transactions_root)
                    .expect("Construct from_arrow transactions_root"),
                message_outbox_root: map_binary(idx, message_outbox_root)
                    .expect("Construct from_arrow message_outbox_root"),
                event_inbox_root: map_binary(idx, event_inbox_root)
                    .expect("Construct from_arrow event_inbox_root"),
                height: height
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow height"),
                prev_root: map_binary(idx, prev_root).expect("Construct from_arrow prev_root"),
                time: time
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow time"),
                application_hash: map_binary(idx, application_hash)
                    .expect("Construct from_arrow application_hash"),
            })
            .collect()
    }
}

impl FromArrow for Transaction {
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self> {
        let block_height = batch.column::<UInt64Array>("block_height").ok();
        let id = batch.column::<BinaryArray<i32>>("id").ok();
        let input_asset_ids = batch.column::<BinaryArray<i32>>("input_asset_ids").ok();
        let input_contracts = batch.column::<BinaryArray<i32>>("input_contracts").ok();
        let input_contract_utxo_id = batch
            .column::<BinaryArray<i32>>("input_contract_utxo_id")
            .ok();
        let input_contract_balance_root = batch
            .column::<BinaryArray<i32>>("input_contract_balance_root")
            .ok();
        let input_contract_state_root = batch
            .column::<BinaryArray<i32>>("input_contract_state_root")
            .ok();
        let input_contract_tx_pointer_block_height = batch
            .column::<UInt64Array>("input_contract_tx_pointer_block_height")
            .ok();
        let input_contract_tx_pointer_tx_index = batch
            .column::<UInt64Array>("input_contract_tx_pointer_tx_index")
            .ok();
        let input_contract = batch.column::<BinaryArray<i32>>("input_contract").ok();
        let policies_tip = batch.column::<UInt64Array>("policies_tip").ok();
        let policies_witness_limit = batch.column::<UInt64Array>("policies_witness_limit").ok();
        let policies_maturity = batch.column::<UInt64Array>("policies_maturity").ok();
        let policies_max_fee = batch.column::<UInt64Array>("policies_max_fee").ok();
        let script_gas_limit = batch.column::<UInt64Array>("script_gas_limit").ok();
        let maturity = batch.column::<UInt64Array>("maturity").ok();
        let mint_amount = batch.column::<UInt64Array>("mint_amount").ok();
        let mint_asset_id = batch.column::<BinaryArray<i32>>("mint_asset_id").ok();
        let mint_gas_price = batch.column::<UInt64Array>("mint_gas_price").ok();
        let tx_pointer_block_height = batch.column::<UInt64Array>("tx_pointer_block_height").ok();
        let tx_pointer_tx_index = batch.column::<UInt64Array>("tx_pointer_tx_index").ok();
        let tx_type = batch.column::<UInt8Array>("tx_type").ok();
        let output_contract_input_index = batch
            .column::<UInt64Array>("output_contract_input_index")
            .ok();
        let output_contract_balance_root = batch
            .column::<BinaryArray<i32>>("output_contract_balance_root")
            .ok();
        let output_contract_state_root = batch
            .column::<BinaryArray<i32>>("output_contract_state_root")
            .ok();
        let witnesses = batch.column::<BinaryArray<i32>>("witnesses").ok();
        let receipts_root = batch.column::<BinaryArray<i32>>("receipts_root").ok();
        let status = batch.column::<UInt8Array>("status").ok();
        let time = batch.column::<UInt64Array>("time").ok();
        let reason = batch.column::<Utf8Array<i32>>("reason").ok();
        let script = batch.column::<BinaryArray<i32>>("script").ok();
        let script_data = batch.column::<BinaryArray<i32>>("script_data").ok();
        let bytecode_witness_index = batch.column::<UInt64Array>("bytecode_witness_index").ok();
        let bytecode_root = batch.column::<BinaryArray<i32>>("bytecode_root").ok();
        let subsection_index = batch.column::<UInt64Array>("subsection_index").ok();
        let subsections_number = batch.column::<UInt64Array>("subsections_number").ok();
        let proof_set = batch.column::<BinaryArray<i32>>("proof_set").ok();
        let consensus_parameters_upgrade_purpose_witness_index = batch
            .column::<UInt64Array>("consensus_parameters_upgrade_purpose_witness_index")
            .ok();
        let consensus_parameters_upgrade_purpose_checksum = batch
            .column::<BinaryArray<i32>>("consensus_parameters_upgrade_purpose_checksum")
            .ok();
        let state_transition_upgrade_purpose_root = batch
            .column::<BinaryArray<i32>>("state_transition_upgrade_purpose_root")
            .ok();
        let salt = batch.column::<BinaryArray<i32>>("salt").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                block_height: block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow block_height"),
                id: map_binary(idx, id).unwrap(),
                input_asset_ids: input_asset_ids
                    .and_then(|arr| arr.get(idx).map(|v| bincode::deserialize(v).unwrap())),
                input_contracts: input_contracts
                    .and_then(|arr| arr.get(idx).map(|v| bincode::deserialize(v).unwrap())),
                input_contract_utxo_id: map_binary(idx, input_contract_utxo_id),
                input_contract_balance_root: map_binary(idx, input_contract_balance_root),
                input_contract_state_root: map_binary(idx, input_contract_state_root),
                input_contract_tx_pointer_block_height: input_contract_tx_pointer_block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                input_contract_tx_pointer_tx_index: input_contract_tx_pointer_tx_index
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                input_contract: map_binary(idx, input_contract),
                policies_tip: policies_tip.and_then(|arr| arr.get(idx).map(|v| v.into())),
                policies_witness_limit: policies_witness_limit
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                policies_maturity: policies_maturity.and_then(|arr| arr.get(idx).map(|v| v.into())),
                policies_max_fee: policies_max_fee.and_then(|arr| arr.get(idx).map(|v| v.into())),
                script_gas_limit: script_gas_limit.and_then(|arr| arr.get(idx).map(|v| v.into())),
                maturity: maturity.and_then(|arr| arr.get(idx).map(|v| v.into())),
                mint_amount: mint_amount.and_then(|arr| arr.get(idx).map(|v| v.into())),
                mint_asset_id: map_binary(idx, mint_asset_id),
                mint_gas_price: mint_gas_price.and_then(|arr| arr.get(idx).map(|v| v.into())),
                tx_pointer_block_height: tx_pointer_block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                tx_pointer_tx_index: tx_pointer_tx_index
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                tx_type: tx_type
                    .and_then(|arr| arr.get(idx).map(TransactionType::from))
                    .expect("Construct from_arrow tx_type"),
                output_contract_input_index: output_contract_input_index
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                output_contract_balance_root: map_binary(idx, output_contract_balance_root),
                output_contract_state_root: map_binary(idx, output_contract_state_root),
                witnesses: map_binary(idx, witnesses),
                receipts_root: map_binary(idx, receipts_root),
                status: status
                    .and_then(|arr| arr.get(idx).map(|v| TransactionStatus::from_u8(v).unwrap()))
                    .expect("Construct from_arrow status"),
                time: time
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow time"),
                reason: reason.and_then(|arr| arr.get(idx).map(|v| v.to_owned())),
                script: map_binary(idx, script),
                script_data: map_binary(idx, script_data),
                bytecode_witness_index: bytecode_witness_index
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                bytecode_root: map_binary(idx, bytecode_root),
                subsection_index: subsection_index.and_then(|arr| arr.get(idx).map(|v| v.into())),
                subsections_number: subsections_number
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                proof_set: map_binary(idx, proof_set),
                consensus_parameters_upgrade_purpose_witness_index:
                    consensus_parameters_upgrade_purpose_witness_index
                        .and_then(|arr| arr.get(idx).map(|v| v.into())),
                consensus_parameters_upgrade_purpose_checksum: map_binary(
                    idx,
                    consensus_parameters_upgrade_purpose_checksum,
                ),
                state_transition_upgrade_purpose_root: map_binary(
                    idx,
                    state_transition_upgrade_purpose_root,
                ),
                salt: map_binary(idx, salt),
            })
            .collect()
    }
}

impl FromArrow for Receipt {
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self> {
        let receipt_index = batch.column::<UInt64Array>("receipt_index").ok();
        let root_contract_id = batch.column::<BinaryArray<i32>>("root_contract_id").ok();
        let tx_id = batch.column::<BinaryArray<i32>>("tx_id").ok();
        let tx_status = batch.column::<UInt8Array>("tx_status").ok();
        let tx_type = batch.column::<UInt8Array>("tx_type").ok();
        let block_height = batch.column::<UInt64Array>("block_height").ok();
        let pc = batch.column::<UInt64Array>("pc").ok();
        let is = batch.column::<UInt64Array>("is").ok();
        let to = batch.column::<BinaryArray<i32>>("to").ok();
        let to_address = batch.column::<BinaryArray<i32>>("to_address").ok();
        let amount = batch.column::<UInt64Array>("amount").ok();
        let asset_id = batch.column::<BinaryArray<i32>>("asset_id").ok();
        let gas = batch.column::<UInt64Array>("gas").ok();
        let param1 = batch.column::<UInt64Array>("param1").ok();
        let param2 = batch.column::<UInt64Array>("param2").ok();
        let val = batch.column::<UInt64Array>("val").ok();
        let ptr = batch.column::<UInt64Array>("ptr").ok();
        let digest = batch.column::<BinaryArray<i32>>("digest").ok();
        let reason = batch.column::<UInt64Array>("reason").ok();
        let ra = batch.column::<UInt64Array>("ra").ok();
        let rb = batch.column::<UInt64Array>("rb").ok();
        let rc = batch.column::<UInt64Array>("rc").ok();
        let rd = batch.column::<UInt64Array>("rd").ok();
        let len = batch.column::<UInt64Array>("len").ok();
        let receipt_type = batch.column::<UInt8Array>("receipt_type").ok();
        let result = batch.column::<UInt64Array>("result").ok();
        let gas_used = batch.column::<UInt64Array>("gas_used").ok();
        let data = batch.column::<BinaryArray<i32>>("data").ok();
        let sender = batch.column::<BinaryArray<i32>>("sender").ok();
        let recipient = batch.column::<BinaryArray<i32>>("recipient").ok();
        let nonce = batch.column::<UInt64Array>("nonce").ok();
        let contract_id = batch.column::<BinaryArray<i32>>("contract_id").ok();
        let sub_id = batch.column::<BinaryArray<i32>>("sub_id").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                receipt_index: receipt_index
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow receipt_index"),
                root_contract_id: map_binary(idx, root_contract_id),
                tx_id: map_binary(idx, tx_id).expect("Construct from_arrow tx_id"),
                tx_status: tx_status
                    .and_then(|arr| arr.get(idx).map(|v| TransactionStatus::from_u8(v).unwrap()))
                    .expect("Construct from_arrow tx_status"),
                tx_type: tx_type
                    .and_then(|arr| arr.get(idx).map(TransactionType))
                    .expect("Construct from_arrow tx_type"),
                block_height: block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow block_height"),
                pc: pc.and_then(|arr| arr.get(idx).map(|v| v.into())),
                is: is.and_then(|arr| arr.get(idx).map(|v| v.into())),
                to: map_binary(idx, to),
                to_address: map_binary(idx, to_address),
                amount: amount.and_then(|arr| arr.get(idx).map(|v| v.into())),
                asset_id: map_binary(idx, asset_id),
                gas: gas.and_then(|arr| arr.get(idx).map(|v| v.into())),
                param1: param1.and_then(|arr| arr.get(idx).map(|v| v.into())),
                param2: param2.and_then(|arr| arr.get(idx).map(|v| v.into())),
                val: val.and_then(|arr| arr.get(idx).map(|v| v.into())),
                ptr: ptr.and_then(|arr| arr.get(idx).map(|v| v.into())),
                digest: map_binary(idx, digest),
                reason: reason.and_then(|arr| arr.get(idx).map(|v| v.into())),
                ra: ra.and_then(|arr| arr.get(idx).map(|v| v.into())),
                rb: rb.and_then(|arr| arr.get(idx).map(|v| v.into())),
                rc: rc.and_then(|arr| arr.get(idx).map(|v| v.into())),
                rd: rd.and_then(|arr| arr.get(idx).map(|v| v.into())),
                len: len.and_then(|arr| arr.get(idx).map(|v| v.into())),
                receipt_type: receipt_type
                    .and_then(|arr| arr.get(idx).map(|v| ReceiptType::from_u8(v).unwrap()))
                    .expect("Construct from_arrow receipt_type"),
                result: result.and_then(|arr| arr.get(idx).map(|v| v.into())),
                gas_used: gas_used.and_then(|arr| arr.get(idx).map(|v| v.into())),
                data: map_binary(idx, data),
                sender: map_binary(idx, sender),
                recipient: map_binary(idx, recipient),
                nonce: nonce.and_then(|arr| arr.get(idx).map(|v| v.into())),
                contract_id: map_binary(idx, contract_id),
                sub_id: map_binary(idx, sub_id),
            })
            .collect()
    }
}

impl FromArrow for Input {
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self> {
        let tx_id = batch.column::<BinaryArray<i32>>("tx_id").ok();
        let tx_status = batch.column::<UInt8Array>("tx_status").ok();
        let tx_type = batch.column::<UInt8Array>("tx_type").ok();
        let block_height = batch.column::<UInt64Array>("block_height").ok();
        let input_type = batch.column::<UInt8Array>("input_type").ok();
        let utxo_id = batch.column::<BinaryArray<i32>>("utxo_id").ok();
        let owner = batch.column::<BinaryArray<i32>>("owner").ok();
        let amount = batch.column::<UInt64Array>("amount").ok();
        let asset_id = batch.column::<BinaryArray<i32>>("asset_id").ok();
        let tx_pointer_block_height = batch.column::<UInt64Array>("tx_pointer_block_height").ok();
        let tx_pointer_tx_index = batch.column::<UInt64Array>("tx_pointer_tx_index").ok();
        let witness_index = batch.column::<UInt64Array>("witness_index").ok();
        let predicate_gas_used = batch.column::<UInt64Array>("predicate_gas_used").ok();
        let predicate = batch.column::<BinaryArray<i32>>("predicate").ok();
        let predicate_data = batch.column::<BinaryArray<i32>>("predicate_data").ok();
        let balance_root = batch.column::<BinaryArray<i32>>("balance_root").ok();
        let state_root = batch.column::<BinaryArray<i32>>("state_root").ok();
        let contract = batch.column::<BinaryArray<i32>>("contract").ok();
        let sender = batch.column::<BinaryArray<i32>>("sender").ok();
        let recipient = batch.column::<BinaryArray<i32>>("recipient").ok();
        let nonce = batch.column::<BinaryArray<i32>>("nonce").ok();
        let data = batch.column::<BinaryArray<i32>>("data").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                tx_id: map_binary(idx, tx_id).unwrap(),
                tx_status: tx_status
                    .and_then(|arr| arr.get(idx).map(|v| TransactionStatus::from_u8(v).unwrap()))
                    .expect("Construct from_arrow tx_status"),
                tx_type: tx_type
                    .and_then(|arr| arr.get(idx).map(TransactionType))
                    .expect("Construct from_arrow tx_type"),
                block_height: block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow block_height"),
                input_type: input_type
                    .and_then(|arr| arr.get(idx).map(|v| InputType::from_u8(v).unwrap()))
                    .expect("Construct from_arrow input_type"),
                utxo_id: map_binary(idx, utxo_id),
                owner: map_binary(idx, owner),
                amount: amount.and_then(|arr| arr.get(idx).map(|v| v.into())),
                asset_id: map_binary(idx, asset_id),
                tx_pointer_block_height: tx_pointer_block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                tx_pointer_tx_index: tx_pointer_tx_index
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                witness_index: witness_index.and_then(|arr| arr.get(idx).map(|v| v.into())),
                predicate_gas_used: predicate_gas_used
                    .and_then(|arr| arr.get(idx).map(|v| v.into())),
                predicate: map_binary(idx, predicate),
                predicate_data: map_binary(idx, predicate_data),
                balance_root: map_binary(idx, balance_root),
                state_root: map_binary(idx, state_root),
                contract: map_binary(idx, contract),
                sender: map_binary(idx, sender),
                recipient: map_binary(idx, recipient),
                nonce: map_binary(idx, nonce),
                data: map_binary(idx, data),
            })
            .collect()
    }
}

impl FromArrow for Output {
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self> {
        let tx_id = batch.column::<BinaryArray<i32>>("tx_id").ok();
        let tx_status = batch.column::<UInt8Array>("tx_status").ok();
        let tx_type = batch.column::<UInt8Array>("tx_type").ok();
        let block_height = batch.column::<UInt64Array>("block_height").ok();
        let output_type = batch.column::<UInt8Array>("output_type").ok();
        let to = batch.column::<BinaryArray<i32>>("to").ok();
        let amount = batch.column::<UInt64Array>("amount").ok();
        let asset_id = batch.column::<BinaryArray<i32>>("asset_id").ok();
        let input_index = batch.column::<UInt64Array>("input_index").ok();
        let balance_root = batch.column::<BinaryArray<i32>>("balance_root").ok();
        let state_root = batch.column::<BinaryArray<i32>>("state_root").ok();
        let contract = batch.column::<BinaryArray<i32>>("contract").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                tx_id: map_binary(idx, tx_id).unwrap(),
                tx_status: tx_status
                    .and_then(|arr| arr.get(idx).map(|v| TransactionStatus::from_u8(v).unwrap()))
                    .expect("Construct from_arrow tx_status"),
                tx_type: tx_type
                    .and_then(|arr| arr.get(idx).map(TransactionType))
                    .expect("Construct from_arrow tx_type"),
                block_height: block_height
                    .and_then(|arr| arr.get(idx).map(|v| v.into()))
                    .expect("Construct from_arrow block_height"),
                output_type: output_type
                    .and_then(|arr| arr.get(idx).map(|v| OutputType::from_u8(v).unwrap()))
                    .expect("Construct from_arrow output_type"),
                to: map_binary(idx, to),
                amount: amount.and_then(|arr| arr.get(idx).map(|v| v.into())),
                asset_id: map_binary(idx, asset_id),
                input_index: input_index.and_then(|arr| arr.get(idx).map(|v| v.into())),
                balance_root: map_binary(idx, balance_root),
                state_root: map_binary(idx, state_root),
                contract: map_binary(idx, contract),
            })
            .collect()
    }
}
