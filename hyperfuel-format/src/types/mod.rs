use serde::{Deserialize, Serialize};

mod data;
mod fixed_size_data;
mod hex;
mod input_type;
mod output_type;
mod quantity;
mod receipt_type;
mod transaction_status;
mod transaction_type;
mod uint;
mod util;
mod withdrawal;

pub use data::Data;
pub use fixed_size_data::FixedSizeData;
pub use hex::Hex;
pub use input_type::InputType;
pub use output_type::OutputType;
pub use quantity::Quantity;
pub use receipt_type::ReceiptType;
pub use transaction_status::TransactionStatus;
pub use transaction_type::TransactionType;
pub use uint::UInt;

/// The header contains metadata about a certain block.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    /// Hash of the header
    pub id: Hash,
    /// The block height for the data availability layer up to which (inclusive) input messages are processed.
    pub da_height: UInt,
    /// The number of transactions in the block.
    pub transactions_count: Quantity,
    /// version of consensus
    pub consensus_parameters_version: UInt,
    /// version of the state transition
    pub state_transition_bytecode_version: UInt,
    /// The number of receipt messages in the block.
    pub message_receipt_count: Quantity,
    /// The merkle root of the transactions in the block.
    pub transactions_root: Hash,
    /// The merkle root of the messages in the block.
    pub message_outbox_root: Hash,
    pub event_inbox_root: Hash,
    /// The block height.
    pub height: UInt,
    /// The merkle root of all previous consensus header hashes (not including this block).
    pub prev_root: Hash,
    /// The timestamp for the block.
    pub time: UInt,
    /// The hash of the serialized application header for this block.
    pub application_hash: Hash,
}

/// An object containing information about a transaction.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_height: UInt,
    pub id: Hash,
    pub input_asset_ids: Option<Vec<Hash>>,
    pub input_contracts: Option<Vec<ContractId>>,
    pub input_contract_utxo_id: Option<Hash>,
    pub input_contract_balance_root: Option<Hash>,
    pub input_contract_state_root: Option<Hash>,
    pub input_contract_tx_pointer_block_height: Option<UInt>,
    pub input_contract_tx_pointer_tx_index: Option<UInt>,
    pub input_contract: Option<ContractId>,
    pub policies_tip: Option<UInt>,
    pub policies_witness_limit: Option<UInt>,
    pub policies_maturity: Option<UInt>,
    pub policies_max_fee: Option<UInt>,
    pub script_gas_limit: Option<UInt>,
    pub maturity: Option<UInt>,
    pub mint_amount: Option<UInt>,
    pub mint_asset_id: Option<Hash>,
    pub mint_gas_price: Option<UInt>,
    pub tx_pointer_block_height: Option<UInt>,
    pub tx_pointer_tx_index: Option<UInt>,
    pub tx_type: TransactionType,
    pub output_contract_input_index: Option<UInt>,
    pub output_contract_balance_root: Option<Hash>,
    pub output_contract_state_root: Option<Hash>,
    pub witnesses: Option<Data>,
    pub receipts_root: Option<Hash>,
    pub status: TransactionStatus,
    pub time: UInt,
    pub reason: Option<String>,
    pub script: Option<Data>,
    pub script_data: Option<Data>,
    pub bytecode_witness_index: Option<UInt>,
    pub bytecode_root: Option<Hash>,
    pub subsection_index: Option<UInt>,
    pub subsections_number: Option<UInt>,
    pub proof_set: Option<Data>,
    pub consensus_parameters_upgrade_purpose_witness_index: Option<UInt>,
    pub consensus_parameters_upgrade_purpose_checksum: Option<Data>,
    pub state_transition_upgrade_purpose_root: Option<Hash>,
    pub salt: Option<Data>,
}

/// An object representing all possible types of receipts.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    pub receipt_index: UInt,
    pub root_contract_id: Option<ContractId>,
    pub tx_id: Hash,
    pub tx_status: TransactionStatus,
    pub tx_type: TransactionType,
    pub block_height: UInt,
    pub pc: Option<UInt>,
    pub is: Option<UInt>,
    pub to: Option<ContractId>,
    pub to_address: Option<Address>,
    pub amount: Option<UInt>,
    pub asset_id: Option<Hash>,
    pub gas: Option<UInt>,
    pub param1: Option<UInt>,
    pub param2: Option<UInt>,
    pub val: Option<UInt>,
    pub ptr: Option<UInt>,
    pub digest: Option<Hash>,
    pub reason: Option<UInt>,
    pub ra: Option<UInt>,
    pub rb: Option<UInt>,
    pub rc: Option<UInt>,
    pub rd: Option<UInt>,
    pub len: Option<UInt>,
    pub receipt_type: ReceiptType,
    pub result: Option<UInt>,
    pub gas_used: Option<UInt>,
    pub data: Option<Data>,
    pub sender: Option<Address>,
    pub recipient: Option<Address>,
    pub nonce: Option<Quantity>,
    pub contract_id: Option<ContractId>,
    pub sub_id: Option<Hash>,
}

/// An object representing all possible types of inputs. InputCoin, InputContract, InputMessage
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Input {
    pub tx_id: Hash,
    pub tx_status: TransactionStatus,
    pub tx_type: TransactionType,
    pub block_height: UInt,
    pub input_type: InputType,
    pub utxo_id: Option<Hash>,
    pub owner: Option<Address>,
    pub amount: Option<UInt>,
    pub asset_id: Option<Hash>,
    pub tx_pointer_block_height: Option<UInt>,
    pub tx_pointer_tx_index: Option<UInt>,
    pub witness_index: Option<UInt>,
    pub predicate_gas_used: Option<UInt>,
    pub predicate: Option<Data>,
    pub predicate_data: Option<Data>,
    pub balance_root: Option<Hash>,
    pub state_root: Option<Hash>,
    pub contract: Option<ContractId>,
    pub sender: Option<Address>,
    pub recipient: Option<Address>,
    pub nonce: Option<Data>,
    pub data: Option<Data>,
}

/// An object representing all possible types of Outputs. CoinOutput, ContractOutput, ChangeOutput, VariableOutput, ContractCreated
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Output {
    pub tx_id: Hash,
    pub tx_status: TransactionStatus,
    pub tx_type: TransactionType,
    pub block_height: UInt,
    pub output_type: OutputType,
    pub to: Option<Address>,
    pub amount: Option<UInt>,
    pub asset_id: Option<Hash>,
    pub input_index: Option<UInt>,
    pub balance_root: Option<Hash>,
    pub state_root: Option<Hash>,
    pub contract: Option<ContractId>,
}

/// hash is 32 bytes of data
pub type Hash = FixedSizeData<32>;

/// An address of an externally owned account identified by a 32 byte string prefixed by 0x.
pub type Address = FixedSizeData<32>;

/// contract id is also a 32 byte hash
pub type ContractId = FixedSizeData<32>;
