use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    io::{Read, Write},
};

use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone)]
pub struct RingBufferMap<K, V> {
    map: HashMap<K, V>,
    order: VecDeque<K>,
    max_size: Option<usize>,
}

impl<K, V> Default for RingBufferMap<K, V> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            max_size: None,
        }
    }
}

impl<K, V> RingBufferMap<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            max_size: Some(max_size),
        }
    }

    pub fn set_max_size(&mut self, max_size: Option<usize>) {
        self.max_size = max_size;
        self.enforce_limit();
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.map.contains_key(&key) {
            return self.map.insert(key, value);
        }
        self.order.push_back(key.clone());
        let prev = self.map.insert(key, value);
        self.enforce_limit();
        prev
    }

    pub fn pop_front(&mut self) -> Option<(K, V)> {
        while let Some(key) = self.order.pop_front() {
            if let Some(value) = self.map.remove(&key) {
                return Some((key, value));
            }
        }
        None
    }

    fn enforce_limit(&mut self) {
        let Some(max_size) = self.max_size else {
            return;
        };
        while self.map.len() > max_size {
            if self.pop_front().is_none() {
                break;
            }
        }
    }
}

impl<K, V> BorshSerialize for RingBufferMap<K, V>
where
    K: BorshSerialize + Eq + Hash,
    V: BorshSerialize,
{
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let mut vec = Vec::with_capacity(self.map.len());
        for key in &self.order {
            if let Some(value) = self.map.get(key) {
                vec.push((key, value));
            }
        }
        vec.serialize(writer)
    }
}

impl<K, V> BorshDeserialize for RingBufferMap<K, V>
where
    K: BorshDeserialize + Eq + Hash + Clone,
    V: BorshDeserialize,
{
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let vec: Vec<(K, V)> = Vec::deserialize_reader(reader)?;
        let mut map = HashMap::with_capacity(vec.len());
        let mut order = VecDeque::with_capacity(vec.len());
        for (key, value) in vec {
            if map.contains_key(&key) {
                map.insert(key, value);
                continue;
            }
            order.push_back(key.clone());
            map.insert(key, value);
        }
        Ok(Self {
            map,
            order,
            max_size: None,
        })
    }
}
