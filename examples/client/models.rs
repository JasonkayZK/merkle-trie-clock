use std::fmt::{Display, Formatter, Result};

use anyhow::bail;
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
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
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

pub const TODO_TABLE: &str = "todos";

#[derive(Debug, Default)]
pub struct Todo {
    pub id: String,
    pub content: String,
    pub todo_type: String,
    pub tombstone: i8,
}

impl Todo {
    pub fn new(id: String) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn handle_message(&mut self, message: &Message) -> anyhow::Result<()> {
        if message.dataset.ne(TODO_TABLE) {
            bail!("Wrong table: {}", message.dataset);
        }
        if message.row.ne(&self.id) {
            bail!("Wrong row: {}", message.row);
        }

        let todo_param: TodoParam = message
            .column
            .as_str()
            .try_into()
            .map_err(anyhow::Error::msg)?;
        match todo_param {
            TodoParam::Content => {
                self.content = message.value.clone();
            }
            TodoParam::TodoType => {
                self.todo_type = message.value.clone();
            }
            TodoParam::Tombstone => {
                self.tombstone = message.value.parse::<i8>()?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum TodoParam {
    Content,
    TodoType,
    Tombstone,
}

impl TryFrom<&str> for TodoParam {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "content" => Ok(TodoParam::Content),
            "todo_type" => Ok(TodoParam::TodoType),
            "tombstone" => Ok(TodoParam::Tombstone),
            _ => Err(format!("Unknown type: {}", value)),
        }
    }
}

impl Display for TodoParam {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            TodoParam::Content => {
                write!(f, "content")
            }
            TodoParam::TodoType => {
                write!(f, "todo_type")
            }
            TodoParam::Tombstone => {
                write!(f, "tombstone")
            }
        }
    }
}
