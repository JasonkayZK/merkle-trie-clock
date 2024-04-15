use std::env;
use std::sync::{Mutex, OnceLock};

use anyhow::bail;
use log::debug;
use serde::{Deserialize, Serialize};

use merkle_trie_clock::clock::MerkleClock;
use merkle_trie_clock::merkle::MerkleTrie;
use merkle_trie_clock::timestamp::Timestamp;

use crate::models::{Message, RowParam, ValueType};
use crate::storage::{Storage, MERKLE_BASE};

const DEFAULT_NODE_NAME: &str = "CLIENT";

const ENDPOINT: &str = "http://localhost:8006";

pub static SYNCER: OnceLock<Mutex<Syncer>> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize)]
struct SyncRequest {
    group_id: String,
    client_id: String,
    messages: Vec<Message>,
    merkle: MerkleTrie<MERKLE_BASE>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncResponse {
    messages: Vec<Message>,
    merkle: MerkleTrie<MERKLE_BASE>,
}

pub struct Syncer {
    node_name: String,
    merkle_clock: MerkleClock<MERKLE_BASE>,
    sync_enabled: bool,
}

impl Syncer {
    pub fn global() -> &'static Mutex<Self> {
        SYNCER.get_or_init(|| {
            let node_name = env::var("CLIENT").unwrap_or(DEFAULT_NODE_NAME.to_string());
            let t = Timestamp::new(0, 0, node_name.clone());
            let c = MerkleClock::new(t, MerkleTrie::<MERKLE_BASE>::new());
            Mutex::new(Syncer {
                node_name,
                merkle_clock: c,
                sync_enabled: true,
            })
        })
    }

    pub fn insert(
        &mut self,
        group_id: &str,
        table: &str,
        row_params: Vec<RowParam>,
    ) -> anyhow::Result<String> {
        // This is roughly comparable to assigning a primary key value to the row if
        // it were in a RDBMS.
        let id = uuid::Uuid::new_v4().to_string();

        // Because we're going to generate a "change" message for every field in the
        // object that is being "inserted" (i.e., there)
        let mut messages = vec![];
        for x in row_params {
            // Here we update the timestamp, but not update the merkle tree
            // Update merkle tree will be operated when sync called, and
            // data exactly executed!
            let next_time = self.merkle_clock.timer_mut().send()?;

            messages.push(Message {
                // Note that every message we create/send gets its own, globally-unique
                // timestamp. In effect, there is a 1-1 relationship between the timestamp
                // and this specific message.
                timestamp: next_time.to_string(),
                dataset: table.to_string(),
                row: x.id.unwrap_or(id.clone()),
                column: x.column,
                value_type: x.value_type,
                value: x.value,
            })
        }

        self.send_messages(group_id, messages)?;

        Ok(id)
    }

    pub fn update(
        &mut self,
        group_id: &str,
        table: &str,
        row_params: Vec<RowParam>,
    ) -> anyhow::Result<()> {
        let mut messages = vec![];
        for x in row_params {
            if let Some(id) = x.id {
                let next_time = self.merkle_clock.timer_mut().send()?;
                messages.push(Message {
                    // Note that every message we create/send gets its own, globally-unique
                    // timestamp. In effect, there is a 1-1 relationship between the timestamp
                    // and this specific message.
                    timestamp: next_time.to_string(),
                    dataset: table.to_string(),
                    row: id,
                    column: x.column,
                    value_type: x.value_type,
                    value: x.value,
                })
            }
        }
        self.send_messages(group_id, messages)?;

        Ok(())
    }

    pub fn delete(&mut self, group_id: &str, table: &str, id: &str) -> anyhow::Result<()> {
        let next_time = self.merkle_clock.timer_mut().send()?;
        self.send_messages(
            group_id,
            vec![Message {
                timestamp: next_time.to_string(),
                dataset: table.to_string(),
                row: id.to_string(),
                column: "tombstone".to_string(),
                value_type: ValueType::Number,
                value: "1".to_string(),
            }],
        )?;
        Ok(())
    }

    pub fn sync(
        &mut self,
        group_id: &str,
        initial_messages: Vec<Message>,
        since: Option<i64>,
    ) -> anyhow::Result<Option<Vec<Message>>> {
        if !self.sync_enabled {
            return Ok(None);
        }

        let mut messages = initial_messages;

        if let Some(since) = since {
            let since = Timestamp::new(since, 0, "".to_string()).to_string();
            messages.retain(|msg| msg.timestamp >= since);
        }

        let client = reqwest::blocking::Client::new();
        let endpoint = format!("{}/sync", ENDPOINT);

        let diff_time = {
            let body = serde_json::to_string(&SyncRequest {
                group_id: group_id.to_string(),
                client_id: self.node_name.clone(),
                messages,
                merkle: self.merkle_clock.merkle().clone(),
            })?;

            let res = client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .body(body)
                .send()?
                .json::<SyncResponse>()?;
            debug!("Got synced response: {:#?}", res);

            if !res.messages.is_empty() {
                // handle received messages
                debug!("{:#?}", res.messages);
                self.receive_messages(res.messages)?;
            }

            self.merkle_clock.merkle_mut().diff(&res.merkle)
        };

        if let Some(diff_time) = diff_time {
            if diff_time > 0 {
                if let Some(since) = since {
                    if since == diff_time {
                        bail!(
                            "A bug happened while syncing and the client \
                        was unable to get in sync with the server. \
                        This is an internal error that shouldn't happen"
                        );
                    }
                }
                self.sync(group_id, vec![], Some(diff_time))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn send_messages(
        &mut self,
        group_id: &str,
        mut messages: Vec<Message>,
    ) -> anyhow::Result<()> {
        Storage::global()
            .lock()
            .unwrap()
            .apply_messages(&mut self.merkle_clock, &mut messages)?;
        self.sync(group_id, messages, None)?;
        Ok(())
    }

    fn receive_messages(&mut self, mut messages: Vec<Message>) -> anyhow::Result<()> {
        for msg in &messages {
            match Timestamp::parse(&msg.timestamp) {
                Ok(timestamp) => {
                    self.merkle_clock.timer_mut().recv(&timestamp)?;
                }
                _ => {
                    log::warn!("Parse timestamp failed: {:?}", msg);
                }
            }
        }

        Storage::global()
            .lock()
            .unwrap()
            .apply_messages(&mut self.merkle_clock, &mut messages)?;
        Ok(())
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn debug(&self) {
        debug!(
            "Current time: {:?}, current merkle trie: {:?}",
            self.merkle_clock.timer(),
            self.merkle_clock.merkle()
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Storage;
    use crate::syncer::Syncer;

    #[test]
    fn sync_test() {
        let mut s = Syncer::global().lock().unwrap();

        let res = s.sync("todo-app", vec![], None);
        println!("{:#?}", res);

        Storage::global().lock().unwrap().debug();
    }

    #[test]
    fn deadlock_test() {
        {
            let c = Syncer::global().lock().unwrap();
            println!("{}", c.node_name);
        }

        let c2 = Syncer::global().lock().unwrap();
    }
}
