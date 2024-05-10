use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use super::Hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputType {
    #[default]
    CoinOutput,
    ContractOutput,
    ChangeOutput,
    VariableOutput,
    ContractCreated,
}

impl FromStr for OutputType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0x0" => Ok(Self::CoinOutput),
            "0x1" => Ok(Self::ContractOutput),
            "0x2" => Ok(Self::ChangeOutput),
            "0x3" => Ok(Self::VariableOutput),
            "0x4" => Ok(Self::ContractCreated),
            _ => Err(Error::UnknownOutputType(s.to_owned())),
        }
    }
}

impl OutputType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CoinOutput => "0x0",
            Self::ContractOutput => "0x1",
            Self::ChangeOutput => "0x2",
            Self::VariableOutput => "0x3",
            Self::ContractCreated => "0x4",
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Self::CoinOutput => 0,
            Self::ContractOutput => 1,
            Self::ChangeOutput => 2,
            Self::VariableOutput => 3,
            Self::ContractCreated => 4,
        }
    }

    pub fn from_u8(n: u8) -> Result<Self> {
        match n {
            0 => Ok(Self::CoinOutput),
            1 => Ok(Self::ContractOutput),
            2 => Ok(Self::ChangeOutput),
            3 => Ok(Self::VariableOutput),
            4 => Ok(Self::ContractCreated),
            _ => Err(Error::UnknownOutputType(n.to_string())),
        }
    }
}

struct OutputTypeVisitor;

impl<'de> Visitor<'de> for OutputTypeVisitor {
    type Value = OutputType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for transaction status")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        OutputType::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for OutputType {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(OutputTypeVisitor)
    }
}

impl Serialize for OutputType {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Hex for OutputType {
    fn encode_hex(&self) -> String {
        self.as_str().to_owned()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        Self::from_str(hex)
    }
}

#[cfg(test)]
mod tests {
    use super::OutputType;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde() {
        assert_tokens(&OutputType::CoinOutput, &[Token::Str("0x0")]);
        assert_tokens(&OutputType::ContractOutput, &[Token::Str("0x1")]);
        assert_tokens(&OutputType::ChangeOutput, &[Token::Str("0x2")]);
        assert_tokens(&OutputType::VariableOutput, &[Token::Str("0x3")]);
        assert_tokens(&OutputType::ContractCreated, &[Token::Str("0x4")]);
    }

    #[test]
    #[should_panic]
    fn test_de_unknown() {
        assert_de_tokens(&OutputType::CoinOutput, &[Token::Str("0x5")]);
    }
}
