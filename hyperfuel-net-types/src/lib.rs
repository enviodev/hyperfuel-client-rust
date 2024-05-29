use std::collections::BTreeSet;

use hyperfuel_format::{FixedSizeData, Hash};
use serde::{Deserialize, Serialize};

pub type Sighash = FixedSizeData<4>;

pub mod hyperfuel_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/hyperfuel_net_types_capnp.rs"));
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct ReceiptSelection {
    #[serde(default)]
    pub root_contract_id: Vec<Hash>,
    #[serde(default)]
    pub to_address: Vec<Hash>,
    #[serde(default)]
    pub asset_id: Vec<Hash>,
    #[serde(default)]
    pub receipt_type: Vec<u8>,
    #[serde(default)]
    pub sender: Vec<Hash>,
    #[serde(default)]
    pub recipient: Vec<Hash>,
    #[serde(default)]
    pub contract_id: Vec<Hash>,
    #[serde(default)]
    pub ra: Vec<u64>,
    #[serde(default)]
    pub rb: Vec<u64>,
    #[serde(default)]
    pub rc: Vec<u64>,
    #[serde(default)]
    pub rd: Vec<u64>,
    #[serde(default)]
    pub tx_status: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct InputSelection {
    #[serde(default)]
    pub owner: Vec<Hash>,
    #[serde(default)]
    pub asset_id: Vec<Hash>,
    #[serde(default)]
    pub contract: Vec<Hash>,
    #[serde(default)]
    pub sender: Vec<Hash>,
    #[serde(default)]
    pub recipient: Vec<Hash>,
    #[serde(default)]
    pub input_type: Vec<u8>,
    #[serde(default)]
    pub tx_status: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct OutputSelection {
    #[serde(default)]
    pub to: Vec<Hash>,
    #[serde(default)]
    pub asset_id: Vec<Hash>,
    #[serde(default)]
    pub contract: Vec<Hash>,
    #[serde(default)]
    pub output_type: Vec<u8>,
    #[serde(default)]
    pub tx_status: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct Query {
    /// The block to start the query from
    pub from_block: u64,
    /// The block to end the query at. If not specified, the query will go until the
    ///  end of data. Exclusive, the returned range will be [from_block..to_block).
    ///
    /// The query will return before it reaches this target block if it hits the time limit
    ///  configured on the server. The user should continue their query by putting the
    ///  next_block field in the response into from_block field of their next query. This implements
    ///  pagination.
    pub to_block: Option<u64>,
    /// List of receipt selections, the query will return receipts that match any of these selections and
    ///  it will return receipts that are related to the returned objects.
    #[serde(default)]
    pub receipts: Vec<ReceiptSelection>,
    /// List of input selections, the query will return inputs that match any of these selections and
    ///  it will return inputs that are related to the returned objects.
    #[serde(default)]
    pub inputs: Vec<InputSelection>,
    /// List of output selections, the query will return outputs that match any of these selections and
    ///  it will return outputs that are related to the returned objects.
    #[serde(default)]
    pub outputs: Vec<OutputSelection>,
    /// Weather to include all blocks regardless of if they are related to a returned transaction or log. Normally
    ///  the server will return only the blocks that are related to the transaction or logs in the response. But if this
    ///  is set to true, the server will return data for all blocks in the requested range [from_block, to_block).
    #[serde(default)]
    pub include_all_blocks: bool,
    /// Field selection. The user can select which fields they are interested in, requesting less fields will improve
    ///  query execution time and reduce the payload size so the user should always use a minimal number of fields.
    #[serde(default)]
    pub field_selection: FieldSelection,
    /// Maximum number of blocks that should be returned, the server might return more blocks than this number but
    ///  it won't overshoot by too much.
    #[serde(default)]
    pub max_num_blocks: Option<usize>,
    /// Maximum number of transactions that should be returned, the server might return more transactions than this number but
    ///  it won't overshoot by too much.
    #[serde(default)]
    pub max_num_transactions: Option<usize>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct FieldSelection {
    #[serde(default)]
    pub block: BTreeSet<String>,
    #[serde(default)]
    pub transaction: BTreeSet<String>,
    #[serde(default)]
    pub receipt: BTreeSet<String>,
    #[serde(default)]
    pub input: BTreeSet<String>,
    #[serde(default)]
    pub output: BTreeSet<String>,
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug)]
pub struct ArchiveHeight {
    pub height: Option<u64>,
}
