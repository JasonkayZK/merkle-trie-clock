use std::sync::{Mutex, OnceLock};

use anyhow::bail;
use rusqlite::{params, Connection};

use merkle_trie_clock::merkle::MerkleTrie;
use merkle_trie_clock::timestamp::Timestamp;

use crate::models::Message;

pub const MERKLE_BASE: usize = 3;

const DB_FILE: &str = "db.sqlite";

static DB: OnceLock<Mutex<Connection>> = OnceLock::new();

pub struct Db;

impl Db {
    fn global() -> &'static Mutex<Connection> {
        DB.get_or_init(|| {
            let c = Connection::open(DB_FILE).unwrap();

            c.execute(
                "CREATE TABLE IF NOT EXISTS messages (
                        timestamp  TEXT,
                        group_id   TEXT,
                        dataset    TEXT,
                        row        TEXT,
                        column     TEXT,
                        value_type TEXT,
                        value      TEXT,
                        PRIMARY KEY (timestamp, group_id)
                    )",
                [],
            )
            .unwrap();

            c.execute(
                "CREATE TABLE IF NOT EXISTS messages_merkles (
                        group_id TEXT PRIMARY KEY,
                        merkle   TEXT,
                        merkle_base INT
                    )",
                [],
            )
            .unwrap();

            Mutex::new(c)
        })
    }
}

pub fn get_merkle(group_id: &str) -> anyhow::Result<MerkleTrie<MERKLE_BASE>> {
    let conn = Db::global().lock().unwrap();
    let mut stmt = conn.prepare("SELECT merkle, merkle_base FROM messages_merkles WHERE group_id = ?")?;

    let mut rows = stmt.query_map([group_id], |row| {
        let merkle: String = row.get(0)?;
        let merkle_base: usize = row.get(1)?;
        Ok((merkle, merkle_base))
    })?;

    match rows.next() {
        Some(Ok(merkle_item)) => {
            let merkle_str = merkle_item.0;
            let merkle_base = merkle_item.1;
            if merkle_base != MERKLE_BASE {
                bail!(
                    "Wrong merkle base, got: {}, expected: {}",
                    merkle_base,
                    MERKLE_BASE
                );
            }
            let trie: MerkleTrie<MERKLE_BASE> = serde_json::from_str(&merkle_str)?;
            Ok(trie)
        }
        _ => Ok(MerkleTrie::<MERKLE_BASE>::new()),
    }
}

pub fn add_messages(
    group_id: &str,
    messages: &[Message],
) -> anyhow::Result<MerkleTrie<MERKLE_BASE>> {
    let mut trie = get_merkle(group_id)?;

    let mut conn = Db::global().lock().unwrap();
    let tx = conn.transaction()?;
    let mut changed = false;

    for message in messages {
        let res = tx.execute(
            "INSERT OR IGNORE INTO messages (timestamp, group_id, dataset, row, column, value_type, value) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            params![
                message.timestamp,
                group_id,
                message.dataset,
                message.row,
                message.column,
                message.value_type.to_string(),
                message.value,
            ],
        )?;

        if res == 1 {
            // Update the merkle trie
            if let Some(time) = Timestamp::parse(&message.timestamp) {
                trie.insert(&time);
                changed = true;
            } else {
                log::error!("Failed to parse timestamp: {}", message.timestamp);
            }
        }
    }

    if changed {
        tx.execute(
            "INSERT OR REPLACE INTO messages_merkles (group_id, merkle, merkle_base) VALUES (?, ?, ?)",
            params![group_id, serde_json::to_string(&trie)?, MERKLE_BASE],
        )?;
    }

    tx.commit()?;

    Ok(trie)
}

pub fn find_late_messages(
    group_id: &str,
    client_id: &str,
    timestamp: &str,
) -> anyhow::Result<Vec<Message>> {
    let conn = Db::global().lock().unwrap();

    let mut stmt = conn.prepare("SELECT dataset, row, column, value_type, value, timestamp FROM messages WHERE group_id = ? AND timestamp > ? AND timestamp NOT LIKE '%' || ? ORDER BY timestamp").unwrap();
    let new_messages_result = stmt.query_map(params![group_id, timestamp, client_id], |row| {
        Ok(Message {
            dataset: row.get(0)?,
            row: row.get(1)?,
            column: row.get(2)?,
            value_type: row.get::<usize, String>(3)?.into(),
            value: row.get(4)?,
            timestamp: row.get(5)?,
        })
    })?;

    let mut new_messages = vec![];
    for msg in new_messages_result {
        let msg = msg.unwrap();
        new_messages.push(msg);
    }

    Ok(new_messages)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use merkle_trie_clock::timestamp::Timestamp;

    use crate::db::{add_messages, get_merkle, Db};
    use crate::models::{Message, ValueType};

    #[test]
    fn db_test() {
        let c = Db::global().lock().unwrap();
        assert!(!c.is_busy())
    }

    #[test]
    fn get_merkle_test() {
        let r = get_merkle("undefined").unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn add_messages_test() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let id = Timestamp::generate_short_uuid();
        let t = Timestamp::new(timestamp as i64, 0, id.to_string());

        let message = Message {
            timestamp: t.to_string(),
            dataset: "abc".to_string(),
            row: "ae37814d-4201-432b-a9a2-f277224cd730".to_string(),
            column: "name".to_string(),
            value_type: ValueType::String,
            value: "Jack".to_string(),
        };
        let trie = add_messages("test-group", &[message]).unwrap();

        assert!(!trie.is_empty());
        trie.debug();
    }
}
