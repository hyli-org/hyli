// HashMap with deterministic hashing in turmoil tests, RandomState in production.
pub type HashMap<K, V> = std::collections::HashMap<K, V, HyliBuildHasher>;

#[cfg(feature = "turmoil")]
pub type HyliBuildHasher = DeterministicBuildHasher;

#[cfg(not(feature = "turmoil"))]
pub type HyliBuildHasher = std::collections::hash_map::RandomState;

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
