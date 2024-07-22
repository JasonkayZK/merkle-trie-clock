use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;

use merkle_trie_clock::clock::MerkleClock;
use merkle_trie_clock::models::Message;

pub trait Store<Item: DeserializeOwned + Serialize + Debug, const MERKLE_BASE: usize> {
    fn apply_messages(
        &mut self,
        clock: &mut MerkleClock<MERKLE_BASE>,
        messages: &mut Vec<Message>,
    ) -> anyhow::Result<()>;

    fn items(&self) -> &HashMap<String, Item>;

    fn applied_messages(&self) -> &HashSet<String>;
}

pub trait MessageHandler: Sized {
    fn from_message(message: &Message) -> Self;

    fn handle_message(&mut self, message: &Message) -> anyhow::Result<()>;

    fn table_name() -> String;
}
