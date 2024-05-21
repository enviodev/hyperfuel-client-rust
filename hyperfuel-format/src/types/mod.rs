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

// referencing https://docs.fuel.network/docs/graphql/reference/objects/#header

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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block<Tx> {
    #[serde(flatten)]
    pub header: BlockHeader,
    pub transactions: Vec<Tx>,
}

/// An object containing information about a transaction.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    /// block the transaction is in.
    pub block_height: UInt,
    /// A unique transaction id.
    pub id: Hash,
    /// An array of asset ids used for the transaction inputs.
    pub input_asset_ids: Option<Vec<Hash>>,
    // Contract object -> bincode into schema
    /// An array of contracts used for the transaction inputs.
    pub input_contracts: Option<Vec<ContractId>>,
    /// A contract used for the transaction input.
    /// A unique 32 byte identifier for the UTXO for a contract used for the transaction input.
    pub input_contract_utxo_id: Option<Hash>,
    /// The root of amount of coins owned by contract before transaction execution for a contract used for the transaction input.
    pub input_contract_balance_root: Option<Hash>,
    /// The state root of contract before transaction execution for a contract used for the transaction input.
    pub input_contract_state_root: Option<Hash>,
    /// A pointer to the TX whose output is being spent for a contract used for the transaction input.
    pub input_contract_tx_pointer_block_height: Option<UInt>,
    /// A pointer to the TX whose output is being spent for a contract used for the transaction input.
    pub input_contract_tx_pointer_tx_index: Option<UInt>,
    /// The contract id for a contract used for the transaction input.
    pub input_contract_id: Option<ContractId>,
    pub policies_tip: Option<UInt>,
    pub policies_witness_limit: Option<UInt>,
    pub policies_maturity: Option<UInt>,
    pub policies_max_fee: Option<UInt>,
    /// The gas limit for the script.
    pub script_gas_limit: Option<UInt>,
    /// The minimum block height that the transaction can be included at.
    pub maturity: Option<UInt>,
    /// The amount minted in the transaction.
    pub mint_amount: Option<UInt>,
    /// The asset ID for coins minted in the transaction.
    pub mint_asset_id: Option<Hash>,
    pub mint_gas_price: Option<UInt>,
    /// The location of the transaction in the block.
    pub tx_pointer_block_height: Option<UInt>,
    pub tx_pointer_tx_index: Option<UInt>,
    /// Script, creating a new contract, or minting new coins
    pub tx_type: TransactionType,
    /// The index of the input from a transaction that changed the state of a contract.
    pub output_contract_input_index: Option<UInt>,
    /// The root of amount of coins owned by contract after transaction execution from a transaction that changed the state of a contract.
    pub output_contract_balance_root: Option<Hash>,
    /// The state root of contract after transaction execution from a transaction that changed the state of a contract.
    pub output_contract_state_root: Option<Hash>,
    /// An array of witnesses.
    pub witnesses: Option<Data>,
    /// The root of the receipts.
    pub receipts_root: Option<Hash>,
    /// The status type of the transaction.
    pub status: TransactionStatus,
    /// for SubmittedStatus, SuccessStatus, and FailureStatus, the time a transaction was submitted, successful, or failed
    pub time: UInt,
    /// for SuccessStatus, the state of the program execution
    // pub program_state: Option<ProgramState>
    /// for SqueezedOutStatus & FailureStatus, the reason the transaction was squeezed out or failed
    pub reason: Option<String>,
    /// The script to execute.
    pub script: Option<Data>,
    /// The script input parameters.
    pub script_data: Option<Data>,
    /// The witness index of contract bytecode.
    pub bytecode_witness_index: Option<UInt>,
    pub bytecode_root: Option<Hash>,
    pub subsection_index: Option<UInt>,
    pub subsection_number: Option<UInt>,
    pub proof_set: Option<Vec<Data>>,
    pub consensus_parameters_upgrade_purpose_witness_index: Option<UInt>,
    pub consensus_parameters_upgrade_purpose_checksum: Option<Data>,
    pub state_transition_upgrade_purpose_root: Option<Hash>,
    /// The salt value for the transaction.
    pub salt: Option<Data>,
}

/// An object representing all possible types of receipts.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    /// Index of the receipt in the block
    pub receipt_index: UInt,
    /// Contract that produced the receipt
    pub root_contract_id: Option<ContractId>,
    /// transaction that this receipt originated from
    pub tx_id: Hash,
    /// block that the receipt originated in
    pub block_height: UInt,
    /// The value of the program counter register $pc, which is the memory address of the current instruction.
    pub pc: Option<UInt>,
    /// The value of register $is, which is the pointer to the start of the currently-executing code.
    pub is: Option<UInt>,
    /// The recipient contract
    pub to: Option<ContractId>,
    /// The recipient address
    pub to_address: Option<Address>,
    /// The amount of coins transferred.
    pub amount: Option<UInt>,
    /// The asset id of the coins transferred.
    pub asset_id: Option<Hash>,
    /// The gas used for the transaction.
    pub gas: Option<UInt>,
    /// The first parameter for a CALL receipt type, holds the function selector.
    pub param1: Option<UInt>,
    /// The second parameter for a CALL receipt type, typically used for the user-specified input to the ABI function being selected.
    pub param2: Option<UInt>,
    /// The value of registers at the end of execution, used for debugging.
    pub val: Option<UInt>,
    /// The value of the pointer register, used for debugging.
    pub ptr: Option<UInt>,
    /// A 32-byte hash of MEM[$rC, $rD]. The syntax MEM[x, y] means the memory range starting at byte x, of length y bytes.
    pub digest: Option<Hash>,
    /// The decimal string representation of an 8-bit unsigned integer for the panic reason. Only returned if the receipt type is PANIC.
    pub reason: Option<UInt>,
    /// The value of register $rA.
    pub ra: Option<UInt>,
    /// The value of register $rB.
    pub rb: Option<UInt>,
    /// The value of register $rC.
    pub rc: Option<UInt>,
    /// The value of register $rD.
    pub rd: Option<UInt>,
    /// The length of the receipt.
    pub len: Option<UInt>,
    /// The type of receipt.
    pub receipt_type: ReceiptType,
    /// 0 if script exited successfully, any otherwise.
    pub result: Option<UInt>,
    /// The amount of gas consumed by the script.
    pub gas_used: Option<UInt>,
    /// The receipt data.
    pub data: Option<Data>,
    /// The address of the message sender.
    pub sender: Option<Address>,
    /// The address of the message recipient.
    pub recipient: Option<Address>,
    /// The nonce value for a message.
    pub nonce: Option<Quantity>,
    /// Current context if in an internal context. null otherwise
    pub contract_id: Option<ContractId>,
    /// The sub id.
    pub sub_id: Option<Hash>,
}

/// An object representing all possible types of inputs.  InputCoin, InputContract, InputMessage
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Input {
    /// transaction that this input originated from
    pub tx_id: Hash,
    /// block that the input originated in
    pub block_height: UInt,
    /// InputCoin, InputContract, or InputMessage
    pub input_type: InputType,
    /// A unique 32 byte identifier for the UTXO.
    pub utxo_id: Option<Hash>,
    /// The owning address or predicate root.
    pub owner: Option<Address>,
    /// for InputCoin type: The amount of coins.
    /// for InputMessage type: The amount sent in the message.
    pub amount: Option<UInt>,
    /// The asset ID of the coins.
    pub asset_id: Option<Hash>,
    /// A pointer to the transaction whose output is being spent.
    pub tx_pointer_block_height: Option<UInt>,
    pub tx_pointer_tx_index: Option<UInt>,
    /// The index of the witness that authorizes spending the coin.
    pub witness_index: Option<UInt>,
    /// The amount of gas used in the predicate transaction.
    pub predicate_gas_used: Option<UInt>,
    /// The predicate bytecode.
    pub predicate: Option<Data>,
    /// The predicate input parameters.
    pub predicate_data: Option<Data>,
    /// The root of amount of coins owned by contract before transaction execution.
    pub balance_root: Option<Hash>,
    /// The state root of contract before transaction execution.
    pub state_root: Option<Hash>,
    /// The input contract.
    pub contract: Option<ContractId>,
    /// The sender address of the message.
    pub sender: Option<Address>,
    /// The recipient address of the message.
    pub recipient: Option<Address>,
    /// A nonce value for the message input, which is determined by the sending system and is published at the time the message is sent.
    pub nonce: Option<Data>,
    /// The message data.
    pub data: Option<Data>,
}

/// An object representing all possible types of Outputs. CoinOutput, ContractOutput, ChangeOutput, VariableOutput, ContractCreated
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Output {
    /// transaction that this out originated from
    pub tx_id: Hash,
    /// block that the output originated in
    pub block_height: UInt,
    /// CoinOutput, ContractOutput, ChangeOutput, VariableOutput, or ContractCreated
    pub output_type: OutputType,
    /// The address the coins were sent to.
    pub to: Option<Address>,
    /// The amount of coins in the output.
    pub amount: Option<UInt>,
    /// The asset id for the coins sent.
    pub asset_id: Option<Hash>,
    /// The index of the input.
    pub input_index: Option<UInt>,
    /// The root of amount of coins owned by contract after transaction execution.
    pub balance_root: Option<Hash>,
    /// for ContractedCreated type: The initial state root of contract.
    /// for ContractOutput type: The state root of contract after transaction execution.
    pub state_root: Option<Hash>,
    /// for ContractCreated type: The contract that was created.
    pub contract: Option<ContractId>,
}

/// hash is 32 bytes of data
pub type Hash = FixedSizeData<32>;

/// An address of an externally owned account identified by a 32 byte string prefixed by 0x.
pub type Address = FixedSizeData<32>;

// contract id is also a 32 byte hash
pub type ContractId = FixedSizeData<32>;
