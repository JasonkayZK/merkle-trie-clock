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

    pub fn timer(&self) -> &Timestamp {
        &self.timer
    }

    pub fn timer_mut(&mut self) -> &mut Timestamp {
        &mut self.timer
    }

    pub fn merkle(&self) -> &MerkleTrie<BASE> {
        &self.merkle
    }

    pub fn merkle_mut(&mut self) -> &mut MerkleTrie<BASE> {
        &mut self.merkle
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

        // Update timer
        c.timer.send().unwrap();
        // Insert into merkle trie
        c.merkle.insert(&c.timer);
        println!("Timer: {}", c.timer);
        println!("Merkle Trie:");
        c.merkle.debug();
        println!();
        assert_eq!(c.merkle.length(), 1);

        // Update timer
        c.timer.send().unwrap();
        // Insert into merkle trie
        c.merkle.insert(&c.timer);
        println!("Timer: {}", c.timer);
        println!("Merkle Trie:");
        c.merkle.debug();
        println!();
        assert_eq!(c.merkle.length(), 2);
    }
}
