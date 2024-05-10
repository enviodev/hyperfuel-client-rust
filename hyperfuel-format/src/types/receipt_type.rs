use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use super::Hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReceiptType {
    #[default]
    Call,
    Return,
    ReturnData,
    Panic,
    Revert,
    Log,
    LogData,
    Transfer,
    TransferOut,
    ScriptResult,
    MessageOut,
    Mint,
    Burn,
}

impl ReceiptType {
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0 => Ok(Self::Call),
            1 => Ok(Self::Return),
            2 => Ok(Self::ReturnData),
            3 => Ok(Self::Panic),
            4 => Ok(Self::Revert),
            5 => Ok(Self::Log),
            6 => Ok(Self::LogData),
            7 => Ok(Self::Transfer),
            8 => Ok(Self::TransferOut),
            9 => Ok(Self::ScriptResult),
            10 => Ok(Self::MessageOut),
            11 => Ok(Self::Mint),
            12 => Ok(Self::Burn),
            _ => Err(Error::UnknownReceiptType(val.to_string())),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Call => 0,
            Self::Return => 1,
            Self::ReturnData => 2,
            Self::Panic => 3,
            Self::Revert => 4,
            Self::Log => 5,
            Self::LogData => 6,
            Self::Transfer => 7,
            Self::TransferOut => 8,
            Self::ScriptResult => 9,
            Self::MessageOut => 10,
            Self::Mint => 11,
            Self::Burn => 12,
        }
    }
}

impl FromStr for ReceiptType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0x0" => Ok(Self::Call),
            "0x1" => Ok(Self::Return),
            "0x2" => Ok(Self::ReturnData),
            "0x3" => Ok(Self::Panic),
            "0x4" => Ok(Self::Revert),
            "0x5" => Ok(Self::Log),
            "0x6" => Ok(Self::LogData),
            "0x7" => Ok(Self::Transfer),
            "0x8" => Ok(Self::TransferOut),
            "0x9" => Ok(Self::ScriptResult),
            "0x10" => Ok(Self::MessageOut),
            "0x11" => Ok(Self::Mint),
            "0x12" => Ok(Self::Burn),
            _ => Err(Error::UnknownReceiptType(s.to_owned())),
        }
    }
}

impl ReceiptType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Call => "0x0",
            Self::Return => "0x1",
            Self::ReturnData => "0x2",
            Self::Panic => "0x3",
            Self::Revert => "0x4",
            Self::Log => "0x5",
            Self::LogData => "0x6",
            Self::Transfer => "0x7",
            Self::TransferOut => "0x8",
            Self::ScriptResult => "0x9",
            Self::MessageOut => "0x10",
            Self::Mint => "0x11",
            Self::Burn => "0x12",
        }
    }
}

struct ReceiptTypeVisitor;

impl<'de> Visitor<'de> for ReceiptTypeVisitor {
    type Value = ReceiptType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for transaction status")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        ReceiptType::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for ReceiptType {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ReceiptTypeVisitor)
    }
}

impl Serialize for ReceiptType {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Hex for ReceiptType {
    fn encode_hex(&self) -> String {
        self.as_str().to_owned()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        Self::from_str(hex)
    }
}

#[cfg(test)]
mod tests {
    use super::ReceiptType;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde() {
        assert_tokens(&ReceiptType::Call, &[Token::Str("0x0")]);
        assert_tokens(&ReceiptType::Return, &[Token::Str("0x1")]);
        assert_tokens(&ReceiptType::ReturnData, &[Token::Str("0x2")]);
        assert_tokens(&ReceiptType::Panic, &[Token::Str("0x3")]);
        assert_tokens(&ReceiptType::Revert, &[Token::Str("0x4")]);
        assert_tokens(&ReceiptType::Log, &[Token::Str("0x5")]);
        assert_tokens(&ReceiptType::LogData, &[Token::Str("0x6")]);
        assert_tokens(&ReceiptType::Transfer, &[Token::Str("0x7")]);
        assert_tokens(&ReceiptType::TransferOut, &[Token::Str("0x8")]);
        assert_tokens(&ReceiptType::ScriptResult, &[Token::Str("0x9")]);
        assert_tokens(&ReceiptType::MessageOut, &[Token::Str("0x10")]);
        assert_tokens(&ReceiptType::Mint, &[Token::Str("0x11")]);
        assert_tokens(&ReceiptType::Burn, &[Token::Str("0x12")]);
    }

    #[test]
    #[should_panic]
    fn test_de_unknown() {
        assert_de_tokens(&ReceiptType::Call, &[Token::Str("0x13")]);
    }
}
