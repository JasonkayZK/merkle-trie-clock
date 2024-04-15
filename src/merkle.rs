#![allow(clippy::only_used_in_recursion)]

use std::cmp::min;
use std::collections::BTreeMap;
use std::i64;
use std::ptr::NonNull;

use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::timestamp::Timestamp;

#[derive(Debug, Clone)]
struct MerkleTrieNode<const BASE: usize = 3> {
    /// The children of this trie
    children: Option<BTreeMap<usize, NonNull<MerkleTrieNode<BASE>>>>,

    /// The hash of the data
    hash: u64,

    /// Whether this node stored the corresponding data
    stored: bool,
}

unsafe impl<const BASE: usize> Send for MerkleTrieNode<BASE> {}

unsafe impl<const BASE: usize> Sync for MerkleTrieNode<BASE> {}

impl<const BASE: usize> Default for MerkleTrieNode<BASE> {
    fn default() -> Self {
        Self {
            children: None,
            hash: 0,
            stored: false,
        }
    }
}

impl<const BASE: usize> Serialize for MerkleTrieNode<BASE> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MerkleTrieNode", 3)?;

        // Serialize the hash
        state.serialize_field("hash", &self.hash)?;

        // Serialize the stored flag
        state.serialize_field("stored", &self.stored)?;

        // Serialize the children recursively
        if let Some(children) = &self.children {
            let mut serialized_children = BTreeMap::new();
            unsafe {
                for (k, v) in children {
                    let boxed_node = Box::new(v.as_ref());
                    serialized_children.insert(*k, boxed_node);
                }
            }
            state.serialize_field("children", &serialized_children)?;
        } else {
            state.serialize_field(
                "children",
                &None::<BTreeMap<usize, Box<MerkleTrieNode<BASE>>>>,
            )?;
        }

        state.end()
    }
}

impl<'de, const BASE: usize> Deserialize<'de> for MerkleTrieNode<BASE> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct NodeData<const BASE: usize = 3> {
            hash: u64,
            stored: bool,
            children: Option<BTreeMap<usize, Box<MerkleTrieNode<BASE>>>>,
        }

        let node_data = NodeData::deserialize(deserializer)?;

        // Convert Boxed nodes back to NonNull
        let mut children: Option<BTreeMap<usize, NonNull<MerkleTrieNode<BASE>>>> = None;
        if let Some(map) = node_data.children {
            let mut new_map = BTreeMap::new();
            for (key, value) in map {
                let non_null_value =
                    NonNull::new(Box::into_raw(value)).expect("Failed to create NonNull");
                new_map.insert(key, non_null_value);
            }
            children = Some(new_map);
        }

        Ok(MerkleTrieNode {
            hash: node_data.hash,
            stored: node_data.stored,
            children,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MerkleTrie<const BASE: usize = 3> {
    /// The root of this trie
    root: NonNull<MerkleTrieNode<BASE>>,

    /// The size of the trie
    length: u64,
}

unsafe impl<const BASE: usize> Send for MerkleTrie<BASE> {}

unsafe impl<const BASE: usize> Sync for MerkleTrie<BASE> {}

impl<const BASE: usize> Default for MerkleTrie<BASE> {
    fn default() -> Self {
        let m = MerkleTrieNode {
            children: None,
            hash: 0,
            stored: false,
        };

        Self {
            root: NonNull::new(Box::leak(Box::new(m))).unwrap(),
            length: 0,
        }
    }
}

impl<const BASE: usize> MerkleTrie<BASE> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn root_hash(&self) -> u64 {
        unsafe { (*self.root.as_ptr()).hash }
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn insert(&mut self, timestamp: &Timestamp) {
        let hash = timestamp.hash();

        // Convert the timestamp's logical time (i.e., its "milliseconds since
        // 1970") to minutes, then convert that to a BASE STRING.
        // For example, Base 3 meaning:
        // 0 => '0', 1 => '1', 2 => '2', 3 => '10', 2938 => '11000211'.
        //
        // This string will be used as a path to navigate the merkle tree: each
        // character is a step in the path used to navigate to the next node in the
        // trie. In other words, the logical time becomes the "key" that can be used
        // to get/set a value (the timestamp's hash) in the merkle tree.
        //
        // Since we're using base-3, each char in the path will either be '0',
        // '1', or '2'. This means that the trie will consist of nodes that have, at
        // most, 3 child nodes.
        //
        // Note the use of the bitwise OR operator (`... | 0`). This is a quick way
        // of converting the floating-point value to an integer (in a nutshell: the
        // bitwise operators only work on 32-bit integers, so it causes the 64-bit
        // float to be converted to an integer). For example, this causes:
        // "1211121022121110.11221000121012222" to become "1211121022121110".
        let key = self.timestamp_to_key(timestamp);

        // Create a new object that has the same tree and a NEW root hash. Note that
        // "bitwise hashing" is being used here to make a new hash. Bitwise XOR
        // treats both operands as a sequence of 32 bits. It returns a new sequence
        // of 32 bits where each bit is the result of combining the corresponding
        // pair of bits (i.e., bits in the same position) from the operands. It
        // returns a 1 in each bit position for which the corresponding bits of
        // either but not both operands are 1s.
        unsafe {
            let root_node = self.root.as_ptr();
            (*root_node).hash ^= hash;

            let new_root = self.insert_key(root_node.as_mut().unwrap(), &key, hash);
            self.root = NonNull::new(Box::leak(Box::new(new_root))).unwrap();
        }

        self.length += 1;
    }

    fn insert_key(
        &mut self,
        current_trie: &MerkleTrieNode<BASE>,
        key: &[usize],
        timestamp_hash: u64,
    ) -> MerkleTrieNode<BASE> {
        if key.is_empty() {
            return current_trie.clone();
        }

        let child_key = key[0];

        let curr_child = current_trie
            .children
            .as_ref()
            .and_then(|map| map.get(&child_key))
            .map_or_else(
                || MerkleTrieNode {
                    children: None,
                    hash: 0,
                    stored: key.len() == 1,
                },
                |child_ptr| unsafe { child_ptr.as_ref().clone() },
            );

        let new_child = MerkleTrieNode {
            children: self
                .insert_key(&curr_child, &key[1..], timestamp_hash)
                .children,
            hash: curr_child.hash ^ timestamp_hash,
            stored: curr_child.stored || key.len() == 1,
        };

        let mut new_children = current_trie.children.clone().unwrap_or_default();
        new_children.insert(
            child_key,
            NonNull::new(Box::into_raw(Box::new(new_child))).unwrap(),
        );

        MerkleTrieNode {
            children: Some(new_children),
            hash: current_trie.hash,
            stored: key.len() == 1,
        }
    }

    /// Find the first diff element in the merkle tree
    pub fn diff(&self, other: &MerkleTrie<BASE>) -> Option<i64> {
        if self.is_empty() && other.is_empty() {
            return None;
        }
        if self.is_empty() || other.is_empty() {
            return Some(0);
        }
        if other.is_empty() {
            unsafe { return Some(self.find_first_key_by_prefix(Some(other.root.as_ref()), &[])) }
        }

        if self.root_hash() == other.root_hash() {
            return None;
        }

        unsafe {
            // Find the prefix
            let mut node1 = Some(self.root.as_ref());
            let mut node2 = Some(other.root.as_ref());
            let mut node1_prev_stored = false;
            let mut node2_prev_stored = false;
            let mut key_diff_prefix = vec![];

            loop {
                let key_diff: Option<usize>;

                match (node1, node2) {
                    (Some(node1), Some(node2)) => {
                        let mut keyset: Vec<usize> = Vec::new();
                        node1_prev_stored = node1.stored;
                        node2_prev_stored = node2.stored;

                        // We reached to the leaf node, stop!
                        if node1.children.as_ref().map_or(true, |c| c.is_empty())
                            || node2.children.as_ref().map_or(true, |c| c.is_empty())
                        {
                            break;
                        }

                        node1.children.as_ref().and_then(|children| {
                            keyset.extend(children.keys());
                            None::<()>
                        });
                        node2.children.as_ref().and_then(|children| {
                            keyset.extend(children.keys());
                            None::<()>
                        });
                        keyset.sort();

                        key_diff = keyset.into_iter().find(|k| {
                            match (node1.children.as_ref(), node2.children.as_ref()) {
                                (Some(children1), Some(children2)) => {
                                    let child_node1_hash = children1
                                        .get(k)
                                        .map(|node| node.as_ref().hash)
                                        .unwrap_or(0);
                                    let child_node2_hash = children2
                                        .get(k)
                                        .map(|node| node.as_ref().hash)
                                        .unwrap_or(0);
                                    child_node1_hash != child_node2_hash
                                }
                                (None, None) => false,
                                _ => true,
                            };
                            true
                        });
                    }
                    (Some(_), None) => {
                        break;
                    }
                    (None, Some(_)) => {
                        break;
                    }
                    (None, None) => {
                        break;
                    }
                };

                match key_diff {
                    None => {
                        break;
                    }
                    Some(key_diff) => {
                        key_diff_prefix.push(key_diff);
                        node1 = node1.and_then(|node| {
                            node.children.as_ref().and_then(|children| {
                                children.get(&key_diff).map(|node| node.as_ref())
                            })
                        });
                        node2 = node2.and_then(|node| {
                            node.children.as_ref().and_then(|children| {
                                children.get(&key_diff).map(|node| node.as_ref())
                            })
                        });
                    }
                };
            }
            assert!(!key_diff_prefix.is_empty());

            // If the path is already a store node, then the minimum key is the prefix key!
            if node1_prev_stored || node2_prev_stored {
                return Some(self.key_to_timestamp_millis(key_diff_prefix));
            }
            // Continue to find the first diff node that stores the data
            match (node1, node2) {
                (Some(node1), None) => {
                    Some(self.find_first_key_by_prefix(Some(node1), &key_diff_prefix))
                }
                (None, Some(node2)) => {
                    Some(self.find_first_key_by_prefix(Some(node2), &key_diff_prefix))
                }
                (None, None) => {
                    // Only the last node is different!
                    Some(self.key_to_timestamp_millis(key_diff_prefix))
                }
                (Some(node1), Some(node2)) => {
                    // There can be no circumstances for both not none!
                    Some(min(
                        self.find_first_key_by_prefix(Some(node1), &key_diff_prefix),
                        self.find_first_key_by_prefix(Some(node2), &key_diff_prefix),
                    ))
                }
            }
        }
    }

    fn find_first_key_by_prefix(
        &self,
        mut tree: Option<&MerkleTrieNode<{ BASE }>>,
        key_prefix: &[usize],
    ) -> i64 {
        let mut key = Vec::from(key_prefix);

        if tree.is_none() {
            return i64::MAX;
        }

        while let Some(node) = tree {
            if node.stored {
                return self.key_to_timestamp_millis(key);
            };

            // Leaf node must be a store node!
            assert!(node.children.is_some());

            unsafe {
                tree = node.children.as_ref().and_then(|children| {
                    children.first_key_value().map(|kv| {
                        key.push(*kv.0);
                        kv.1.as_ref()
                    })
                });
            }
        }

        self.key_to_timestamp_millis(key)
    }

    pub fn key_to_timestamp_millis(&self, mut key: Vec<usize>) -> i64 {
        let mut base = 1;
        let mut current = 0;
        key.reverse();
        for x in key {
            current += x * base;
            base *= BASE;
        }

        current as i64
    }

    pub fn timestamp_to_key(&self, timestamp: &Timestamp) -> Vec<usize> {
        let mut v: Vec<usize> = vec![];
        let mut current = timestamp.millis() as usize;
        let mut res: usize;
        while current != 0 {
            res = current % BASE;
            v.push(res);
            current /= BASE;
        }
        v.reverse();

        v
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn debug(&self) {
        self.print_node_recursive(unsafe { &*self.root.as_ptr() }, 0);
    }

    #[allow(clippy::only_used_in_recursion)]
    fn print_node_recursive(&self, node: &MerkleTrieNode<BASE>, ident: usize) {
        println!("{}Node Hash: {}", " ".repeat(ident), node.hash);

        if let Some(children) = &node.children {
            let ident = ident + 2;
            for (key, child_ptr) in children {
                unsafe {
                    let child = child_ptr.as_ptr();
                    println!(
                        "{}Child Key: {}, Child Hash: {}, Stored: {}",
                        " ".repeat(ident),
                        key,
                        (*child).hash,
                        (*child).stored
                    );
                    self.print_node_recursive(&*child, ident);
                }
            }
        }
    }
}

impl<const BASE: usize> Serialize for MerkleTrie<BASE> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MerkleTrie", 2)?;

        // Serialize the root
        let root_node = unsafe { self.root.as_ref() };
        state.serialize_field("root", &root_node)?;

        // Serialize the length
        state.serialize_field("length", &self.length)?;

        state.end()
    }
}

impl<'de, const BASE: usize> Deserialize<'de> for MerkleTrie<BASE> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TrieData<const BASE: usize = 3> {
            root: Box<MerkleTrieNode<BASE>>,
            length: u64,
        }

        let trie_data = TrieData::deserialize(deserializer)?;

        // Convert Boxed root node to NonNull
        let root = NonNull::new(Box::into_raw(trie_data.root)).expect("Failed to create NonNull");

        Ok(MerkleTrie {
            root,
            length: trie_data.length,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::merkle::MerkleTrie;
    use crate::timestamp::Timestamp;

    #[test]
    fn debug_test() {
        let m: MerkleTrie<3> = MerkleTrie::new();
        m.debug();
    }

    #[test]
    fn key_to_timestamp_base3_test() {
        let m: MerkleTrie<3> = MerkleTrie::new();
        let cur = m.key_to_timestamp_millis(vec![]);
        assert_eq!(cur, 0);

        let cur = m.key_to_timestamp_millis(vec![1, 1, 0, 0, 0, 2, 1, 1]);
        assert_eq!(cur, 2938);
    }

    #[test]
    fn key_to_timestamp_base10_test() {
        let m: MerkleTrie<10> = MerkleTrie::new();
        let cur = m.key_to_timestamp_millis(vec![9, 2, 4, 7]);
        assert_eq!(cur, 9247);
    }

    #[test]
    fn timestamp_to_key_base3_test() {
        let m: MerkleTrie = MerkleTrie::new();
        let t1 = Timestamp::new(0, 0, String::from("1"));
        let cur = m.timestamp_to_key(&t1);
        assert!(cur.is_empty());

        let t2 = Timestamp::new(2938, 0, String::from("1"));
        let cur = m.timestamp_to_key(&t2);
        assert_eq!(cur, vec![1, 1, 0, 0, 0, 2, 1, 1]);
    }

    #[test]
    fn timestamp_to_key_base10_test() {
        let m: MerkleTrie<10> = MerkleTrie::new();
        let t = Timestamp::new(9247, 0, String::from("1"));
        let cur = m.timestamp_to_key(&t);
        assert_eq!(cur, vec![9, 2, 4, 7]);
    }

    #[test]
    fn insert_test() {
        let mut m: MerkleTrie<10> = MerkleTrie::new();
        m.insert(&Timestamp::new(1, 0, String::from("local")));
        m.insert(&Timestamp::new(2, 0, String::from("local")));
        m.insert(&Timestamp::new(3, 0, String::from("local")));
        m.insert(&Timestamp::new(44, 0, String::from("local")));
        m.insert(&Timestamp::new(127, 0, String::from("local")));

        assert_eq!(m.length, 5);

        m.debug()
    }

    #[test]
    fn diff_test1() {
        let mut m1: MerkleTrie<10> = MerkleTrie::new();
        m1.insert(&Timestamp::new(12788, 0, String::from("local")));

        let mut m2: MerkleTrie<10> = MerkleTrie::new();
        m2.insert(&Timestamp::new(12768, 0, String::from("remote")));

        assert_eq!(m1.diff(&m2), Some(12768));
        assert_eq!(m1.diff(&m2), m2.diff(&m1));
    }

    #[test]
    fn diff_test2() {
        let mut m1: MerkleTrie<10> = MerkleTrie::new();
        m1.insert(&Timestamp::new(12786, 0, String::from("local")));

        let mut m2: MerkleTrie<10> = MerkleTrie::new();
        m2.insert(&Timestamp::new(12787, 0, String::from("remote")));

        assert_eq!(m1.diff(&m2), Some(12786));
        assert_eq!(m1.diff(&m2), m2.diff(&m1));
    }

    #[test]
    fn diff_test3() {
        let m1: MerkleTrie<10> = MerkleTrie::new();

        let mut m2: MerkleTrie<10> = MerkleTrie::new();
        m2.insert(&Timestamp::new(12787, 0, String::from("remote")));

        m1.debug();
        println!();
        m2.debug();

        assert_eq!(m1.diff(&m2), Some(0));
        assert_eq!(m1.diff(&m2), m2.diff(&m1));
    }

    #[test]
    fn diff_test4() {
        let mut m1: MerkleTrie<10> = MerkleTrie::new();
        m1.insert(&Timestamp::new(127, 0, String::from("local")));

        let mut m2: MerkleTrie<10> = MerkleTrie::new();
        m2.insert(&Timestamp::new(12787, 0, String::from("remote")));

        assert_eq!(m1.diff(&m2), Some(127));
        assert_eq!(m1.diff(&m2), m2.diff(&m1));
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut m: MerkleTrie<10> = MerkleTrie::new();
        m.insert(&Timestamp::new(1, 0, String::from("local")));
        m.insert(&Timestamp::new(2, 0, String::from("local")));
        m.insert(&Timestamp::new(3, 0, String::from("local")));
        m.insert(&Timestamp::new(44, 0, String::from("local")));
        m.insert(&Timestamp::new(127, 0, String::from("local")));
        println!("Before serializing: ");
        m.debug();
        println!();

        // Serialize the MerkleTrie instance to a JSON string
        let serialized = serde_json::to_string(&m).unwrap();

        println!("Serialized: ");
        println!("{:#?}", serialized);
        println!();

        // Deserialize the JSON string back to a SerdeMerkleTrie instance
        let deserialized: MerkleTrie<10> = serde_json::from_str(&serialized).unwrap();

        println!("Deserialized: ");
        deserialized.debug();
        assert_eq!(deserialized.length, 5);
    }

    #[test]
    fn test_serialize_deserialize2() {
        let m: MerkleTrie<10> = MerkleTrie::new();
        println!("Before serializing: ");
        m.debug();
        println!();

        // Serialize the MerkleTrie instance to a JSON string
        let serialized = serde_json::to_string(&m).unwrap();

        println!("Serialized: ");
        println!("{:#?}", serialized);
        println!();

        // Deserialize the JSON string back to a SerdeMerkleTrie instance
        let deserialized: MerkleTrie<10> = serde_json::from_str(&serialized).unwrap();

        println!("Deserialized: ");
        deserialized.debug();
    }
}
