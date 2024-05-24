use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use super::Hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum TransactionType {
    #[default]
    Script,
    Create,
    Mint,
    Upgrade,
    Upload,
}

impl TransactionType {
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0 => Ok(Self::Script),
            1 => Ok(Self::Create),
            2 => Ok(Self::Mint),
            3 => Ok(Self::Upgrade),
            4 => Ok(Self::Upload),
            _ => Err(Error::UnknownTransactionType(val.to_string())),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Script => 0,
            Self::Create => 1,
            Self::Mint => 2,
            Self::Upgrade => 3,
            Self::Upload => 4,
        }
    }
}

impl FromStr for TransactionType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0x0" => Ok(Self::Script),
            "0x1" => Ok(Self::Create),
            "0x2" => Ok(Self::Mint),
            "0x3" => Ok(Self::Upgrade),
            "0x4" => Ok(Self::Upload),
            _ => Err(Error::UnknownTransactionType(s.to_owned())),
        }
    }
}

impl TransactionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Script => "0x0",
            Self::Create => "0x1",
            Self::Mint => "0x2",
            Self::Upgrade => "0x3",
            Self::Upload => "0x4",
        }
    }
}

struct TransactionTypeVisitor;

impl<'de> Visitor<'de> for TransactionTypeVisitor {
    type Value = TransactionType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for transaction status")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        TransactionType::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for TransactionType {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TransactionTypeVisitor)
    }
}

impl Serialize for TransactionType {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Hex for TransactionType {
    fn encode_hex(&self) -> String {
        self.as_str().to_owned()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        Self::from_str(hex)
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionType;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde() {
        assert_tokens(&TransactionType::Script, &[Token::Str("0x0")]);
        assert_tokens(&TransactionType::Create, &[Token::Str("0x1")]);
        assert_tokens(&TransactionType::Mint, &[Token::Str("0x2")]);
        assert_tokens(&TransactionType::Upgrade, &[Token::Str("0x3")]);
        assert_tokens(&TransactionType::Upload, &[Token::Str("0x4")]);
    }

    #[test]
    #[should_panic]
    fn test_de_unknown() {
        assert_de_tokens(&TransactionType::Script, &[Token::Str("0x3")]);
    }
}
