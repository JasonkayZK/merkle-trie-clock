use std::cmp::max;
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use std::usize;

use anyhow::{bail, Result};
use chrono::DateTime;
use murmurhash32::murmurhash3;
use uuid::Uuid;

/// Maximum physical clock drift allowed, in ms. In other words, if we
/// receive a message from another node and that node's time differs from
/// ours by more than this many milliseconds, throw an error.
const MAX_DRIFT: i64 = 60000;

const MAX_COUNTER: usize = 65535;

#[derive(Debug, Clone)]
pub struct Timestamp {
    millis: i64,
    counter: usize,
    node: String,
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let date = Self::millis_to_datetime(self.millis);
        let counter = format!("{:04X}", self.counter);
        let node = format!("{:016}", self.node);

        write!(f, "{}-{}-{}", date, counter, node)
    }
}

impl Timestamp {
    pub fn new(millis: i64, counter: usize, node: String) -> Self {
        Self {
            millis,
            counter,
            node,
        }
    }

    pub fn hash(&self) -> u64 {
        murmurhash3(self.to_string().as_bytes()) as u64
    }

    /// Timestamp send. Generates a unique, monotonic timestamp suitable
    /// for transmission to another system in string format
    pub fn send(&mut self) -> Result<Timestamp> {
        // Retrieve the local wall time
        let phys = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

        // Unpack the clock.timestamp logical time and counter
        let l_old = self.millis;
        let c_old = self.counter;

        // Calculate the next logical time and counter
        // * ensure that the logical time never goes backward
        // * increment the counter if phys time does not advance
        let l_new = max(l_old, phys);
        let c_new = match l_old == l_new {
            true => c_old + 1,
            false => 0,
        };

        // Check the result for drift and counter overflow
        if l_new - phys > MAX_DRIFT {
            bail!("ClockDriftError: {}, {}, {}", l_new, phys, MAX_DRIFT)
        }
        // Check counter overflow
        if c_new > MAX_COUNTER {
            // We don't support counters greater than 65535 because we need to ensure
            // that, when converted to a hex string, it doesn't use more than 4 chars
            // (see Timestamp.toString). For example:
            //   (65533).toString(16) -> fffd
            //   (65534).toString(16) -> fffe
            //   (65535).toString(16) -> ffff
            //   (65536).toString(16) -> 10000 -- oops, this is 5 chars
            // It's not that a larger counter couldn't be used--that would just mean
            // increasing the expected length of the counterpart of the timestamp
            // and updating the code that parses/generates that string. Some sort of
            // length needs to be picked, and therefore there is going to be some sort
            // of limit to how big the counter can be.
            bail!("OverflowError");
        }

        // Repack the logical time/counter
        self.millis = l_new;
        self.counter = c_new;

        Ok(self.clone())
    }

    /// Timestamp receive. Parses and merges a timestamp from a remote
    /// system with the local time global uniqueness and monotonicity are
    /// preserved
    pub fn recv(&mut self, other_timestamp: &Timestamp) -> Result<()> {
        let phys = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

        // Unpack the message wall time/counter
        let l_msg = other_timestamp.millis;
        let c_msg = other_timestamp.counter;

        // Assert the node id and remote clock drift
        if other_timestamp.node == self.node {
            // Whoops, looks like the message came from the same node ID as ours!
            bail!("DuplicateNodeError: {}", self.node);
        }

        if l_msg - phys > MAX_DRIFT {
            // Whoops, the other node's physical time differs from ours by more than
            // the configured limit (e.g., 1 minute).
            bail!("ClockDriftError");
        }

        // Unpack the clock.timestamp logical time and counter
        let l_old = self.millis;
        let c_old = self.counter;

        // Calculate the next logical time and counter.
        // Ensure that the logical time never goes backward;
        // * if all logical clocks are equal, increment the max counter,
        // * if max = old > message, increment local counter,
        // * if max = message > old, increment message counter,
        // * otherwise, clocks are monotonic, reset counter
        let l_new = max(max(l_old, phys), l_msg);
        let c_new = if l_new == l_old && l_new == l_msg {
            max(c_old, c_msg) + 1
        } else if l_new == l_old {
            c_old + 1
        } else if l_new == l_msg {
            c_msg + 1
        } else {
            0
        };

        // Check the result for drift and counter overflow
        if l_new - phys > MAX_DRIFT {
            bail!("ClockDriftError");
        }
        if c_new > MAX_COUNTER {
            bail!("OverflowError");
        }

        // Repack the logical time/counter
        self.millis = l_new;
        self.counter = c_new;

        Ok(())
    }

    /// Converts a fixed-length string timestamp to the structured value
    pub fn parse(timestamp: &str) -> Result<Timestamp> {
        let parts = timestamp.split('-').collect::<Vec<_>>();

        if parts.len() == 5 {
            if let Ok(millis) = chrono::DateTime::parse_from_rfc3339(&parts[0..3].join("-")) {
                if let Ok(counter) = usize::from_str_radix(parts[3], 16) {
                    return Ok(Timestamp {
                        millis: millis.timestamp_millis(),
                        counter,
                        node: parts[4].to_string(),
                    });
                }
            }
        };

        bail!("Parse timestamp failed: {}", timestamp);
    }

    pub fn since(iso_string: &str) -> String {
        format!("{}-0000-0000000000000000", iso_string)
    }

    pub fn generate_short_uuid() -> String {
        let uuid = Uuid::new_v4().simple().to_string();
        uuid.replace('-', "")
            .chars()
            .rev()
            .take(16)
            .collect::<String>()
    }

    pub fn millis(&self) -> i64 {
        self.millis
    }
    pub fn counter(&self) -> usize {
        self.counter
    }
    pub fn node(&self) -> &str {
        &self.node
    }

    fn millis_to_datetime(millis: i64) -> String {
        let datetime = DateTime::from_timestamp_millis(millis).unwrap_or_default();
        datetime.to_rfc3339()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::timestamp::Timestamp;

    #[test]
    fn new_test() {
        let now = SystemTime::now();
        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let id = Timestamp::generate_short_uuid();
        let t = Timestamp::new(timestamp as i64, 0, id.to_string());
        println!("{}", t);
    }

    #[test]
    fn hash_test() {
        let now = SystemTime::now();
        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let id = Timestamp::generate_short_uuid();
        let t = Timestamp::new(timestamp as i64, 0, id.to_string());
        println!("{}", t.hash());
    }

    #[test]
    fn millis_to_datetime_test() {
        let now = SystemTime::now();
        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        println!("Current timestamp: {}", timestamp);
        println!("{}", Timestamp::millis_to_datetime(timestamp as i64));
    }

    #[test]
    fn generate_short_uuid_test() {
        let uuid = Timestamp::generate_short_uuid();
        assert_eq!(uuid.len(), 16);
        println!("{}", uuid);
    }

    #[test]
    fn parse_test() {
        let serialized = "2024-04-12T05:13:20.831+00:00-0000-5ef35ca3375b14c8";
        let t = Timestamp::parse(serialized).unwrap();

        assert_eq!(t.millis, 1712898800831);
        assert_eq!(t.node, "5ef35ca3375b14c8");
        assert_eq!(t.counter, 0);
    }

    #[test]
    fn parse_test2() {
        let serialized = "2024-04-12T05:13:20.831+00:00-0001-5ef35ca3375b14c8";
        let t = Timestamp::parse(serialized).unwrap();

        assert_eq!(t.millis, 1712898800831);
        assert_eq!(t.node, "5ef35ca3375b14c8");
        assert_eq!(t.counter, 1);
    }

    #[test]
    fn send_test() {
        // Old timestamp
        let mut local_t = Timestamp::new(1712898800831, 0, "local".to_string());
        local_t.send().unwrap();
        println!("phys: {}, local_t: {:?}", 1712898800831i64, local_t);
        assert!(local_t.millis > 1712898800831);
        assert_eq!(local_t.counter, 0);

        // Concurrent timestamp
        let phys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut local_t = Timestamp::new(phys, 0, "local".to_string());

        local_t.send().unwrap();
        println!("phys: {}, local_t: {:?}", phys, local_t);

        assert!(local_t.millis >= phys);
        if local_t.millis == phys {
            assert_eq!(local_t.counter, 1);
        } else {
            assert_eq!(local_t.counter, 0);
        }
        assert_eq!(local_t.node, "local")
    }

    #[test]
    fn recv_local_old_test() {
        let phys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut local_t = Timestamp::new(1712898800831, 4, "local".to_string());
        let remote_t = Timestamp::new(phys, 5, "remote".to_string());

        local_t.recv(&remote_t).unwrap();

        assert!(local_t.millis >= phys);
        if local_t.millis == phys {
            assert_eq!(local_t.counter, 6);
        } else {
            assert_eq!(local_t.counter, 0);
        }
    }

    #[test]
    fn recv_remote_old_test() {
        let phys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut local_t = Timestamp::new(phys, 4, "local".to_string());
        let remote_t = Timestamp::new(1712898800831, 5, "remote".to_string());

        local_t.recv(&remote_t).unwrap();

        assert!(local_t.millis >= phys);
        if local_t.millis == phys {
            assert_eq!(local_t.counter, 5);
        } else {
            assert_eq!(local_t.counter, 0);
        }
    }

    #[test]
    fn recv_concurrent_test() {
        let phys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut local_t = Timestamp::new(phys, 4, "local".to_string());
        let remote_t = Timestamp::new(phys, 5, "remote".to_string());

        local_t.recv(&remote_t).unwrap();

        assert!(local_t.millis >= phys);
        if local_t.millis == phys {
            assert_eq!(local_t.counter, 6);
        } else {
            assert_eq!(local_t.counter, 0);
        }
    }
}
