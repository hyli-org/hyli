use sdk::hyli_model_utils::TimestampMs;

pub struct TimestampMsClock;

impl TimestampMsClock {
    #[cfg(not(feature = "turmoil"))]
    pub fn now() -> TimestampMs {
        use std::time::{SystemTime, UNIX_EPOCH};

        TimestampMs(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backward")
                .as_millis(),
        )
    }

    #[cfg(feature = "turmoil")]
    pub fn now() -> TimestampMs {
        TimestampMs(turmoil::elapsed().as_millis())
    }
}
