use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use super::Hex;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InputType {
    #[default]
    InputCoin,
    InputContract,
    InputMessage,
}

impl FromStr for InputType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0x0" => Ok(Self::InputCoin),
            "0x1" => Ok(Self::InputContract),
            "0x2" => Ok(Self::InputMessage),
            _ => Err(Error::UnknownInputType(s.to_owned())),
        }
    }
}

impl InputType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InputCoin => "0x0",
            Self::InputContract => "0x1",
            Self::InputMessage => "0x2",
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Self::InputCoin => 0,
            Self::InputContract => 1,
            Self::InputMessage => 2,
        }
    }

    pub fn from_u8(n: u8) -> Result<Self> {
        match n {
            0 => Ok(Self::InputCoin),
            1 => Ok(Self::InputContract),
            2 => Ok(Self::InputMessage),
            _ => Err(Error::UnknownInputType(n.to_string())),
        }
    }
}

struct InputTypeVisitor;

impl<'de> Visitor<'de> for InputTypeVisitor {
    type Value = InputType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for transaction status")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        InputType::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for InputType {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(InputTypeVisitor)
    }
}

impl Serialize for InputType {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Hex for InputType {
    fn encode_hex(&self) -> String {
        self.as_str().to_owned()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        Self::from_str(hex)
    }
}

#[cfg(test)]
mod tests {
    use super::InputType;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde() {
        assert_tokens(&InputType::InputCoin, &[Token::Str("0x0")]);
        assert_tokens(&InputType::InputContract, &[Token::Str("0x1")]);
        assert_tokens(&InputType::InputMessage, &[Token::Str("0x2")]);
    }

    #[test]
    #[should_panic]
    fn test_de_unknown() {
        assert_de_tokens(&InputType::InputCoin, &[Token::Str("0x4")]);
    }
}
