use std::{
    ops::{Add, Sub},
    time::Duration,
};

use borsh::{BorshDeserialize, BorshSerialize};
use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    PartialOrd,
    Ord,
    Default,
    Display,
    Hash,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct TimestampMs(pub u128);

impl TimestampMs {
    pub const ZERO: TimestampMs = TimestampMs(0);

    /// Truncates the timestamp to the nearest day in milliseconds.
    pub fn current_day_ms(&self) -> u64 {
        let millis_in_day = 86_400_000; // 86,400,000 ms in a day
        self.0.div_euclid(millis_in_day) as u64
    }
}

impl Add<Duration> for TimestampMs {
    type Output = TimestampMs;

    fn add(self, rhs: Duration) -> Self::Output {
        TimestampMs(self.0 + rhs.as_millis())
    }
}

impl Sub<TimestampMs> for TimestampMs {
    type Output = Duration;

    fn sub(self, rhs: TimestampMs) -> Duration {
        Duration::from_millis((self.0 - rhs.0) as u64)
    }
}

impl Sub<Duration> for TimestampMs {
    type Output = TimestampMs;

    fn sub(self, rhs: Duration) -> TimestampMs {
        TimestampMs(self.0 - rhs.as_millis())
    }
}
