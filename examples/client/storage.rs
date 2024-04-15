use log::debug;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use merkle_trie_clock::clock::MerkleClock;
use merkle_trie_clock::timestamp::Timestamp;

use crate::models::{Message, Todo, TODO_TABLE};

pub const MERKLE_BASE: usize = 3;

static STORAGE: OnceLock<Mutex<Storage>> = OnceLock::new();

pub struct Storage {
    todos: HashMap<String, Todo>,
    applied_messages: HashSet<String>,
}

impl Storage {
    pub fn global() -> &'static Mutex<Storage> {
        STORAGE.get_or_init(|| {
            Mutex::new(Storage {
                todos: HashMap::new(),
                applied_messages: HashSet::new(),
            })
        })
    }

    pub fn apply_messages(
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
            match message.dataset.as_str() {
                TODO_TABLE => self.apply_todo_table(clock, message)?,
                _ => {
                    log::warn!("Unknown dataset, message: {:?}", message);
                    continue;
                }
            };
        }

        Ok(())
    }

    /// Apply the data operation contained in a message to our local data store
    /// (i.e., set a new property value for a secified dataset/table/row/column).
    fn apply_todo_table(
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
            match self.todos.get_mut(&incoming_message.row) {
                // We don't have the data yet, insert;
                None => {
                    let mut new_todo = Todo::new(incoming_message.row.clone());
                    new_todo.handle_message(incoming_message)?;
                    self.todos.insert(incoming_message.row.clone(), new_todo);
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

    pub fn todos(&self) -> &HashMap<String, Todo> {
        &self.todos
    }

    pub fn debug(&self) {
        debug!("Current storage: {:#?}", self.todos);
        debug!("Current applied_messages: {:#?}", self.applied_messages);
    }
}
