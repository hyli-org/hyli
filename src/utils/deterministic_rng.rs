use rand::{rngs::StdRng, SeedableRng};
#[cfg(feature = "turmoil")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "turmoil")]
const SEED_ENV: &str = "HYLI_TURMOIL_SEED";
#[cfg(feature = "turmoil")]
static SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a reproducible RNG when `HYLI_TURMOIL_SEED` is set, otherwise a fresh RNG from entropy.
pub fn deterministic_rng() -> StdRng {
    #[cfg(feature = "turmoil")]
    {
        if let Ok(seed) = std::env::var(SEED_ENV) {
            if let Ok(seed) = seed.parse::<u64>() {
                let offset = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
                return StdRng::seed_from_u64(seed.wrapping_add(offset));
            }
        }
    }

    StdRng::from_entropy()
}
