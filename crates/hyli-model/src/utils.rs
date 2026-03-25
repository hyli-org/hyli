use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::{
    fmt,
    ops::{Add, Sub},
    time::Duration,
};

use borsh::{BorshDeserialize, BorshSerialize};
use derive_more::Display;
use serde::{Deserialize, Serialize};

#[cfg(feature = "full")]
use std::{ops::Deref, sync::Arc};

pub mod hex_bytes {
    use alloc::{string::String, vec::Vec};
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

#[cfg(feature = "full")]
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct ArcBorsh<T>(Arc<T>);

#[cfg(feature = "full")]
impl<T> ArcBorsh<T> {
    pub fn new(value: Arc<T>) -> Self {
        Self(value)
    }

    pub fn arc(&self) -> Arc<T> {
        Arc::clone(&self.0)
    }
}

#[cfg(feature = "full")]
impl<T> fmt::Debug for ArcBorsh<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(feature = "full")]
impl<T> Deref for ArcBorsh<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[cfg(feature = "full")]
impl<T> From<Arc<T>> for ArcBorsh<T> {
    fn from(value: Arc<T>) -> Self {
        Self::new(value)
    }
}

#[cfg(feature = "full")]
impl<T> From<T> for ArcBorsh<T> {
    fn from(value: T) -> Self {
        Self::new(Arc::new(value))
    }
}

#[cfg(feature = "full")]
impl<T> PartialEq<T> for ArcBorsh<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        self.0.as_ref() == other
    }
}

#[cfg(feature = "full")]
impl<T> BorshSerialize for ArcBorsh<T>
where
    T: BorshSerialize,
{
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> Result<(), borsh::io::Error> {
        self.0.serialize(writer)
    }
}

#[cfg(feature = "full")]
impl<T> BorshDeserialize for ArcBorsh<T>
where
    T: BorshDeserialize,
{
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> Result<Self, borsh::io::Error> {
        Ok(Self(Arc::new(T::deserialize_reader(reader)?)))
    }
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
