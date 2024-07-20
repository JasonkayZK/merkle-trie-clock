use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub timestamp: String,
    pub dataset: String,
    pub row: String,
    pub column: String,
    pub value_type: ValueType,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueType {
    None,
    Number,
    String,
}

impl From<String> for ValueType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "None" => ValueType::None,
            "Number" => ValueType::Number,
            _ => ValueType::String,
        }
    }
}

impl Display for ValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let printable = match self {
            ValueType::None => "None",
            ValueType::Number => "Number",
            ValueType::String => "String",
        };
        write!(f, "{}", printable)
    }
}

#[derive(Debug, Clone)]
pub struct RowParam {
    pub id: Option<String>,
    pub column: String,
    pub value_type: ValueType,
    pub value: String,
}
