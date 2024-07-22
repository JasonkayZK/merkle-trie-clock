use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;

use merkle_trie_clock::clock::MerkleClock;
use merkle_trie_clock::models::Message;
use merkle_trie_clock::timestamp::Timestamp;

use crate::storage::{MessageHandler, Store};

pub const MERKLE_BASE_CONST: usize = 3;

pub struct MemStorage<
    Item: MessageHandler + DeserializeOwned + Serialize + Debug,
    const MERKLE_BASE: usize,
> {
    table_name: String,
    items: HashMap<String, Item>,
    applied_messages: HashSet<String>,
}

impl<Item: MessageHandler + DeserializeOwned + Serialize + Debug, const MERKLE_BASE: usize>
    Store<Item, MERKLE_BASE> for MemStorage<Item, MERKLE_BASE>
{
    fn apply_messages(
        &mut self,
        clock: &mut MerkleClock<MERKLE_BASE>,
        messages: &mut Vec<Message>,
    ) -> anyhow::Result<()> {
        // Sort the whole messages
        messages.sort_by(|a, b| {
            let timestamp_a = &a.timestamp;
            let timestamp_b = &b.timestamp;
            timestamp_a.cmp(timestamp_b)
        });

        // Look at each incoming message. If it's new to us (i.e., we don't have it in
        // our local store), or is newer than the message we have for the same field
        // (i.e., dataset + row + column), then apply it to our local data store and
        // insert it into our local collection of messages and merkle tree (which is
        // basically a specialized index of those messages).
        for message in messages {
            if message.dataset.as_str().eq(self.table_name.as_str()) {
                (*self).apply_item_table(clock, message)?;
            } else {
                log::warn!("Unknown dataset, message: {:?}", message);
                continue;
            }
        }

        Ok(())
    }

    fn items(&self) -> &HashMap<String, Item> {
        &self.items
    }

    fn applied_messages(&self) -> &HashSet<String> {
        &self.applied_messages
    }
}

impl<Item: MessageHandler + DeserializeOwned + Serialize + Debug, const MERKLE_BASE: usize> Default
    for MemStorage<Item, MERKLE_BASE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Item: MessageHandler + DeserializeOwned + Serialize + Debug, const MERKLE_BASE: usize>
    MemStorage<Item, MERKLE_BASE>
{
    pub fn new() -> Self {
        Self {
            table_name: Item::table_name(),
            items: HashMap::new(),
            applied_messages: HashSet::new(),
        }
    }

    /// Apply the data operation contained in a message to our local data store
    /// (i.e., set a new property value for a secified dataset/table/row/column).
    fn apply_item_table(
        &mut self,
        clock: &mut MerkleClock<MERKLE_BASE>,
        incoming_message: &Message,
    ) -> anyhow::Result<()> {
        debug!("About to be applied message: {:?}", incoming_message);

        // If there is no corresponding local message (i.e., this is a "new" /
        // unknown incoming message), OR the incoming message is "newer" than the
        // one we have, apply the incoming message to our local data store.
        //
        // If this is a new message that we don't have locally (i.e., we didn't find
        // a corresponding local message for the same dataset/row/column OR we did,
        // but it has a different timestamp than ours), we need to add it to our
        // array of local messages and update the merkle tree.
        if !self.applied_messages.contains(&incoming_message.timestamp) {
            match self.items.get_mut(&incoming_message.row) {
                // We don't have the data yet, insert;
                None => {
                    let mut new_item = Item::from_message(incoming_message);
                    new_item.handle_message(incoming_message)?;
                    self.items.insert(incoming_message.row.clone(), new_item);
                }
                // We have the data
                Some(item) => {
                    item.handle_message(incoming_message)?;
                }
            }
            clock
                .merkle_mut()
                .insert(&Timestamp::parse(&incoming_message.timestamp)?);
            self.applied_messages
                .insert(incoming_message.timestamp.clone());
        };

        Ok(())
    }
}
