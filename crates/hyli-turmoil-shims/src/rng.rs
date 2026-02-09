use rand::{SeedableRng, rng, rngs::StdRng};

#[cfg(feature = "turmoil")]
mod turmoil_seed {
    use rand::{SeedableRng, rngs::StdRng};
    use std::cell::{Cell, RefCell};
    use std::sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    };

    const SEED_ENV: &str = "HYLI_TURMOIL_SEED";
    static BASE_SEED: OnceLock<Option<u64>> = OnceLock::new();
    static SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

    thread_local! {
        // Per-thread seed override to keep parallel tests isolated.
        static LOCAL_SEED: RefCell<Option<u64>> = const { RefCell::new(None) };
        // Per-thread counter so repeated RNG requests stay deterministic but distinct.
        static LOCAL_COUNTER: Cell<u64> = const { Cell::new(0) };
    }

    #[derive(Clone)]
    pub struct DeterministicSeedGuard {
        _inner: Arc<SeedGuardInner>,
    }

    struct SeedGuardInner {
        // Preserve previous thread-local state so nested guards restore correctly.
        previous_seed: Option<u64>,
        previous_counter: u64,
    }

    impl Drop for SeedGuardInner {
        fn drop(&mut self) {
            LOCAL_SEED.with(|slot| {
                *slot.borrow_mut() = self.previous_seed;
            });
            LOCAL_COUNTER.with(|counter| {
                counter.set(self.previous_counter);
            });
        }
    }

    /// Set a deterministic seed for the current thread. Restores the previous seed on drop.
    pub fn set_deterministic_seed(seed: u64) -> DeterministicSeedGuard {
        let previous_seed = LOCAL_SEED.with(|slot| *slot.borrow());
        let previous_counter = LOCAL_COUNTER.with(|counter| counter.get());

        LOCAL_SEED.with(|slot| {
            *slot.borrow_mut() = Some(seed);
        });
        LOCAL_COUNTER.with(|counter| {
            counter.set(0);
        });

        DeterministicSeedGuard {
            _inner: Arc::new(SeedGuardInner {
                previous_seed,
                previous_counter,
            }),
        }
    }

    pub(super) fn rng_from_local_or_env() -> Option<StdRng> {
        // Prefer the thread-local override when a test sets it explicitly.
        if let Some(seed) = LOCAL_SEED.with(|slot| *slot.borrow()) {
            let offset = LOCAL_COUNTER.with(|counter| {
                let current = counter.get();
                counter.set(current.wrapping_add(1));
                current
            });
            return Some(StdRng::seed_from_u64(seed.wrapping_add(offset)));
        }

        // Fall back to env-based determinism when no local seed is set.
        if let Some(seed) = *BASE_SEED.get_or_init(|| {
            std::env::var(SEED_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
        }) {
            let offset = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
            return Some(StdRng::seed_from_u64(seed.wrapping_add(offset)));
        }

        None
    }
}

#[cfg(feature = "turmoil")]
pub use turmoil_seed::{DeterministicSeedGuard, set_deterministic_seed};

/// Returns a reproducible RNG when `HYLI_TURMOIL_SEED` is set, otherwise a fresh RNG.
pub fn deterministic_rng() -> StdRng {
    #[cfg(feature = "turmoil")]
    if let Some(rng) = turmoil_seed::rng_from_local_or_env() {
        return rng;
    }

    // Fall back to OS randomness for non-deterministic runs.
    let mut rng = rng();
    StdRng::from_rng(&mut rng)
}
