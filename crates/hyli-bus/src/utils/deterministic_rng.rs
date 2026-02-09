use rand::{SeedableRng, rng, rngs::StdRng};
#[cfg(feature = "turmoil")]
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};

#[cfg(feature = "turmoil")]
const SEED_ENV: &str = "HYLI_TURMOIL_SEED";
#[cfg(feature = "turmoil")]
static BASE_SEED: OnceLock<Option<u64>> = OnceLock::new();
#[cfg(feature = "turmoil")]
static SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a reproducible RNG when `HYLI_TURMOIL_SEED` is set, otherwise a fresh RNG.
pub fn deterministic_rng() -> StdRng {
    #[cfg(feature = "turmoil")]
    {
        if let Some(seed) = *BASE_SEED.get_or_init(|| {
            std::env::var(SEED_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
        }) {
            let offset = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
            return StdRng::seed_from_u64(seed.wrapping_add(offset));
        }
    }

    // Fall back to OS randomness for non-deterministic runs.
    let mut rng = rng();
    StdRng::from_rng(&mut rng)
}
