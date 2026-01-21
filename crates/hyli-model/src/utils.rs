use std::{
    ops::{Add, Sub},
    time::Duration,
};

use borsh::{BorshDeserialize, BorshSerialize};
use derive_more::Display;
use serde::{Deserialize, Serialize};

pub mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let normalized = super::normalize_hex_string(&s);
        hex::decode(&normalized).map_err(serde::de::Error::custom)
    }
}

fn normalize_hex_string(s: &str) -> String {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    if trimmed.len() % 2 == 1 {
        format!("0{trimmed}")
    } else {
        trimmed.to_string()
    }
}

pub fn decode_hex_string_checked(s: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let normalized = normalize_hex_string(s);
    hex::decode(&normalized)
}

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
