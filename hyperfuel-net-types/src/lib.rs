#![allow(clippy::needless_lifetimes)]
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
    pub from_block: u64,
    pub to_block: Option<u64>,
    #[serde(default)]
    pub receipts: Vec<ReceiptSelection>,
    #[serde(default)]
    pub inputs: Vec<InputSelection>,
    #[serde(default)]
    pub outputs: Vec<OutputSelection>,
    #[serde(default)]
    pub include_all_blocks: bool,
    #[serde(default)]
    pub field_selection: FieldSelection,
    #[serde(default)]
    pub max_num_blocks: Option<usize>,
    #[serde(default)]
    pub max_num_transactions: Option<usize>,
    #[serde(default)]
    // The below 4 fields were not in the original fuel-client
    pub max_num_receipts: Option<usize>,
    #[serde(default)]
    pub max_num_inputs: Option<usize>,
    #[serde(default)]
    pub max_num_outputs: Option<usize>,
    #[serde(default)]
    pub join_mode: JoinMode,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum JoinMode {
    Default,
    JoinAll,
    JoinNothing,
}

impl Default for JoinMode {
    fn default() -> Self {
        Self::Default
    }
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
