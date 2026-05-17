use std::collections::BTreeSet;

use hyperfuel_format::{FixedSizeData, Hash};
use serde::{Deserialize, Serialize};

pub type Sighash = FixedSizeData<4>;

pub mod hyperfuel_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/hyperfuel_net_types_capnp.rs"));
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptSelection {
    #[serde(default)]
    pub root_contract_id: Vec<Hash>,
    #[serde(default)]
    pub to: Vec<Hash>,
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
    #[serde(default)]
    pub tx_type: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub tx_type: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub tx_type: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// The block to start the query from.
    pub from_block: u64,
    /// The block to end the query at. If not specified, the query will go until the
    /// end of data. Exclusive: the returned range will be `[from_block..to_block)`.
    ///
    /// The query will return before it reaches this target block if it hits the time limit
    /// configured on the server. The user should continue their query by putting the
    /// `next_block` field from the response into `from_block` on the next query (pagination).
    pub to_block: Option<u64>,
    /// List of receipt selections: the query returns receipts that match any of these selections,
    /// and receipts that are related to the returned objects.
    #[serde(default)]
    pub receipts: Vec<ReceiptSelection>,
    /// List of input selections: the query returns inputs that match any of these selections,
    /// and inputs that are related to the returned objects.
    #[serde(default)]
    pub inputs: Vec<InputSelection>,
    /// List of output selections: the query returns outputs that match any of these selections,
    /// and outputs that are related to the returned objects.
    #[serde(default)]
    pub outputs: Vec<OutputSelection>,
    /// Whether to include all blocks regardless of whether they are related to a returned
    /// transaction or receipt. Normally the server returns only blocks tied to rows in the
    /// response. If this is `true`, the server returns data for every block in the requested
    /// range `[from_block, to_block)`.
    #[serde(default)]
    pub include_all_blocks: bool,
    /// Field selection: choose which columns to fetch. Requesting fewer fields speeds up queries
    /// and reduces payload size, so prefer a minimal selection.
    #[serde(default)]
    pub field_selection: FieldSelection,
    /// Maximum number of blocks to return. The server may return slightly more than this cap.
    #[serde(default)]
    pub max_num_blocks: Option<usize>,
    /// Maximum number of transactions to return. The server may return slightly more than this cap.
    #[serde(default)]
    pub max_num_transactions: Option<usize>,
    /// Maximum number of receipts to return. The server may return slightly more than this cap.
    #[serde(default)]
    pub max_num_receipts: Option<usize>,
    /// Maximum number of inputs to return. The server may return slightly more than this cap.
    #[serde(default)]
    pub max_num_inputs: Option<usize>,
    /// Maximum number of outputs to return. The server may return slightly more than this cap.
    #[serde(default)]
    pub max_num_outputs: Option<usize>,
    /// How the server joins related rows when resolving the query (for example transactions
    /// linked to matching receipts). `Default` uses the server's default join behavior.
    #[serde(default)]
    pub join_mode: JoinMode,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum JoinMode {
    #[default]
    Default,
    JoinAll,
    JoinNothing,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Copy, Deserialize, Serialize, Debug)]
pub struct ChainId {
    pub chain_id: u64,
}
