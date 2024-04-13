use anyhow::Result;

use crate::merkle::MerkleTrie;
use crate::timestamp::Timestamp;

pub struct MerkleClock<const BASE: usize = 3> {
    timer: Timestamp,
    merkle: MerkleTrie<BASE>,
}

unsafe impl<const BASE: usize> Send for MerkleClock<BASE> {}

unsafe impl<const BASE: usize> Sync for MerkleClock<BASE> {}

impl<const BASE: usize> MerkleClock<BASE> {
    pub fn new(timer: Timestamp, merkle: MerkleTrie<BASE>) -> Self {
        Self { timer, merkle }
    }

    pub fn send(&mut self) -> Result<()> {
        // Update timer
        self.timer.send()?;

        // Insert into merkle trie
        self.merkle.insert(&self.timer);

        Ok(())
    }

    pub fn recv(&mut self, other_clock: &MerkleClock<BASE>) -> Result<()> {
        // Update timer
        self.timer.recv(&other_clock.timer)?;

        // Insert into merkle trie
        // Insert into merkle trie
        self.merkle.insert(&self.timer);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::clock::MerkleClock;
    use crate::merkle::MerkleTrie;
    use crate::timestamp::Timestamp;

    #[test]
    fn send_test() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let id = Timestamp::generate_short_uuid();
        let t = Timestamp::new(timestamp as i64, 0, id.to_string());

        let mut c = MerkleClock::new(t, MerkleTrie::<100>::new());

        c.send().unwrap();
        println!("Timer: {}", c.timer);
        println!("Merkle Trie:");
        c.merkle.debug();
        println!();
        assert_eq!(c.merkle.length(), 1);

        c.send().unwrap();
        println!("Timer: {}", c.timer);
        println!("Merkle Trie:");
        c.merkle.debug();
        println!();
        assert_eq!(c.merkle.length(), 2);
    }
}
