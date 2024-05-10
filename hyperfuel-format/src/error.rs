use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Unexpected length. Expected {expected} got {got}.")]
    UnexpectedLength { expected: usize, got: usize },
    #[error("Failed to decode hex string:\n{0}")]
    DecodeHex(faster_hex::Error),
    #[error("Invalid hex prefix. Hex string doesn't start with \"0x\". Value was: \"{0}\"")]
    InvalidHexPrefix(String),
    #[error("Unknown transaction status: {0}")]
    UnknownTransactionStatus(String),
    #[error("Unknown transaction type: {0}")]
    UnknownTransactionType(String),
    #[error("Unknown receipt type: {0}")]
    UnknownReceiptType(String),
    #[error("Unknown input type: {0}")]
    UnknownInputType(String),
    #[error("Unknown output type: {0}")]
    UnknownOutputType(String),
    #[error("Unexpected quantity. Value was: {0}")]
    UnexpectedQuantity(String),
    #[error("Invalid Number from Hex. {0}")]
    DecodeNumberFromHex(String),
}

pub type Result<T> = StdResult<T, Error>;
