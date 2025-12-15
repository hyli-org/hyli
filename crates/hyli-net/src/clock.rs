use sdk::hyli_model_utils::TimestampMs;

pub struct TimestampMsClock;

#[cfg(not(feature = "turmoil"))]
impl TimestampMsClock {
    pub fn now() -> TimestampMs {
        use std::time::{SystemTime, UNIX_EPOCH};

        TimestampMs(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backward")
                .as_millis(),
        )
    }
}

#[cfg(feature = "turmoil")]
mod turmoil_time {
    pub use std::sync::atomic::{AtomicU64, Ordering};
    pub static ORIGIN_MS: AtomicU64 = AtomicU64::new(0);
    pub static LAST_ELAPSED_MS: AtomicU64 = AtomicU64::new(0);

    pub fn init_origin_ms() -> u64 {
        if let Ok(from_env) = std::env::var("HYLI_TURMOIL_ORIGIN_MS") {
            if let Ok(parsed) = from_env.parse::<u64>() {
                return parsed;
            }
        }
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[cfg(feature = "turmoil")]
impl TimestampMsClock {
    pub fn now() -> TimestampMs {
        let mut origin = turmoil_time::ORIGIN_MS.load(turmoil_time::Ordering::Relaxed);
        if origin == 0 {
            let _ = turmoil_time::ORIGIN_MS.compare_exchange(
                0,
                turmoil_time::init_origin_ms(),
                turmoil_time::Ordering::SeqCst,
                turmoil_time::Ordering::Relaxed,
            );
            origin = turmoil_time::ORIGIN_MS.load(turmoil_time::Ordering::Relaxed);
        }

        let elapsed_ms = turmoil::sim_elapsed()
            .map(|d| {
                let ms = d.as_millis() as u128;
                turmoil_time::LAST_ELAPSED_MS.store(ms as u64, turmoil_time::Ordering::Relaxed);
                ms
            })
            .unwrap_or_else(|| {
                turmoil_time::LAST_ELAPSED_MS.load(turmoil_time::Ordering::Relaxed) as u128
            });

        TimestampMs(origin as u128 + elapsed_ms)
    }

    /// Update the cached simulated elapsed time from the current Turmoil world.
    /// Useful for threads that cannot access `turmoil::sim_elapsed` directly.
    pub fn refresh_sim_elapsed() {
        if let Some(elapsed) = turmoil::sim_elapsed() {
            turmoil_time::LAST_ELAPSED_MS
                .store(elapsed.as_millis() as u64, turmoil_time::Ordering::Relaxed);
        }
    }

    pub fn set_origin_ms(origin_ms: u128) {
        turmoil_time::ORIGIN_MS.store(origin_ms as u64, turmoil_time::Ordering::SeqCst);
    }

    pub fn reset_origin_and_elapsed(origin_ms: u128) {
        turmoil_time::ORIGIN_MS.store(origin_ms as u64, turmoil_time::Ordering::SeqCst);
        turmoil_time::LAST_ELAPSED_MS.store(0, turmoil_time::Ordering::SeqCst);
    }
}
