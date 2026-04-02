#[cfg(not(feature = "turmoil"))]
pub use std::collections::HashMap;

#[cfg(feature = "turmoil")]
mod deterministic {
    use std::ops::{Deref, DerefMut};

    use borsh::{BorshDeserialize, BorshSerialize};

    // HashMap with deterministic hashing in turmoil tests.
    pub struct HashMap<K, V>(std::collections::HashMap<K, V, DeterministicBuildHasher>);

    impl<K, V> HashMap<K, V> {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_capacity(capacity: usize) -> Self {
            Self(std::collections::HashMap::with_capacity_and_hasher(
                capacity,
                DeterministicBuildHasher::default(),
            ))
        }
    }

    impl<K, V> Default for HashMap<K, V> {
        fn default() -> Self {
            Self(std::collections::HashMap::with_hasher(
                DeterministicBuildHasher::default(),
            ))
        }
    }

    impl<K, V> Deref for HashMap<K, V> {
        type Target = std::collections::HashMap<K, V, DeterministicBuildHasher>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<K, V> DerefMut for HashMap<K, V> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl<K, V> FromIterator<(K, V)> for HashMap<K, V>
    where
        K: std::hash::Hash + Eq,
    {
        fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
            let mut map = Self::default();
            map.extend(iter);
            map
        }
    }

    impl<K, V> Extend<(K, V)> for HashMap<K, V>
    where
        K: std::hash::Hash + Eq,
    {
        fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
            self.0.extend(iter);
        }
    }

    impl<K, V> IntoIterator for HashMap<K, V> {
        type Item = (K, V);
        type IntoIter = std::collections::hash_map::IntoIter<K, V>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.into_iter()
        }
    }

    impl<'a, K, V> IntoIterator for &'a HashMap<K, V> {
        type Item = (&'a K, &'a V);
        type IntoIter = std::collections::hash_map::Iter<'a, K, V>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.iter()
        }
    }

    impl<'a, K, V> IntoIterator for &'a mut HashMap<K, V> {
        type Item = (&'a K, &'a mut V);
        type IntoIter = std::collections::hash_map::IterMut<'a, K, V>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.iter_mut()
        }
    }

    impl<K, V> Clone for HashMap<K, V>
    where
        K: Clone,
        V: Clone,
    {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<K, V> std::fmt::Debug for HashMap<K, V>
    where
        K: std::fmt::Debug,
        V: std::fmt::Debug,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl<K, V> BorshSerialize for HashMap<K, V>
    where
        K: BorshSerialize + std::hash::Hash + Eq,
        V: BorshSerialize,
    {
        fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
            let items: Vec<(&K, &V)> = self.0.iter().collect();
            items.serialize(writer)
        }
    }

    impl<K, V> BorshDeserialize for HashMap<K, V>
    where
        K: BorshDeserialize + std::hash::Hash + Eq,
        V: BorshDeserialize,
    {
        fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
            let items: Vec<(K, V)> = Vec::deserialize_reader(reader)?;
            let mut map = HashMap::with_capacity(items.len());
            map.extend(items);
            Ok(map)
        }
    }

    #[derive(Clone)]
    pub struct DeterministicBuildHasher {
        seed: u64,
    }

    impl Default for DeterministicBuildHasher {
        fn default() -> Self {
            use rand::RngCore;

            let mut rng = crate::rng::deterministic_rng();
            Self {
                seed: rng.next_u64(),
            }
        }
    }

    impl std::hash::BuildHasher for DeterministicBuildHasher {
        type Hasher = DeterministicHasher;

        fn build_hasher(&self) -> Self::Hasher {
            DeterministicHasher::new(self.seed)
        }
    }

    pub struct DeterministicHasher {
        state: u64,
    }

    impl DeterministicHasher {
        fn new(seed: u64) -> Self {
            Self {
                state: seed ^ 0xcbf29ce484222325,
            }
        }
    }

    impl std::hash::Hasher for DeterministicHasher {
        fn finish(&self) -> u64 {
            self.state
        }

        fn write(&mut self, bytes: &[u8]) {
            const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

            for byte in bytes {
                self.state ^= u64::from(*byte);
                self.state = self.state.wrapping_mul(FNV_PRIME);
            }
        }
    }
}

#[cfg(feature = "turmoil")]
pub use deterministic::HashMap;

#[cfg(feature = "turmoil")]
#[derive(Clone)]
pub struct DeterministicBuildHasher {
    seed: u64,
}

#[cfg(feature = "turmoil")]
impl Default for DeterministicBuildHasher {
    fn default() -> Self {
        use rand::RngCore;

        let mut rng = crate::rng::deterministic_rng();
        Self {
            seed: rng.next_u64(),
        }
    }
}

#[cfg(feature = "turmoil")]
impl std::hash::BuildHasher for DeterministicBuildHasher {
    type Hasher = DeterministicHasher;

    fn build_hasher(&self) -> Self::Hasher {
        DeterministicHasher::new(self.seed)
    }
}

#[cfg(feature = "turmoil")]
pub struct DeterministicHasher {
    state: u64,
}

#[cfg(feature = "turmoil")]
impl DeterministicHasher {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0xcbf29ce484222325,
        }
    }
}

#[cfg(feature = "turmoil")]
impl std::hash::Hasher for DeterministicHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }
}
