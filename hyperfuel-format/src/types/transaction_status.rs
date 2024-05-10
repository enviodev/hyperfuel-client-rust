use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use super::Hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatus {
    #[default]
    Submitted,
    Success,
    SqueezedOut,
    Failure,
}

impl TransactionStatus {
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0 => Ok(Self::Submitted),
            1 => Ok(Self::Success),
            2 => Ok(Self::SqueezedOut),
            3 => Ok(Self::Failure),
            _ => Err(Error::UnknownTransactionStatus(val.to_string())),
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Self::Submitted => 0,
            Self::Success => 1,
            Self::SqueezedOut => 2,
            Self::Failure => 3,
        }
    }
}

impl FromStr for TransactionStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0x0" => Ok(Self::Submitted),
            "0x1" => Ok(Self::Success),
            "0x2" => Ok(Self::SqueezedOut),
            "0x3" => Ok(Self::Failure),
            _ => Err(Error::UnknownTransactionStatus(s.to_owned())),
        }
    }
}

impl TransactionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Submitted => "0x0",
            Self::Success => "0x1",
            Self::SqueezedOut => "0x2",
            Self::Failure => "0x3",
        }
    }
}

struct TransactionStatusVisitor;

impl<'de> Visitor<'de> for TransactionStatusVisitor {
    type Value = TransactionStatus;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for transaction status")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        TransactionStatus::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for TransactionStatus {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TransactionStatusVisitor)
    }
}

impl Serialize for TransactionStatus {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Hex for TransactionStatus {
    fn encode_hex(&self) -> String {
        self.as_str().to_owned()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        Self::from_str(hex)
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionStatus;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde() {
        assert_tokens(&TransactionStatus::Submitted, &[Token::Str("0x0")]);
        assert_tokens(&TransactionStatus::Success, &[Token::Str("0x1")]);
        assert_tokens(&TransactionStatus::SqueezedOut, &[Token::Str("0x2")]);
        assert_tokens(&TransactionStatus::Failure, &[Token::Str("0x3")]);
    }

    #[test]
    #[should_panic]
    fn test_de_unknown() {
        assert_de_tokens(&TransactionStatus::Submitted, &[Token::Str("0x4")]);
    }
}
