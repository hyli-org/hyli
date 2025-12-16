use borsh::{BorshDeserialize, BorshSerialize};
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};

use crate::*;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RewardsClaim {
    block_heights: Vec<BlockHeight>,
}

/// Enum representing the actions that can be performed by the Staking contract.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum StakingAction {
    Stake {
        amount: u128,
    },
    Delegate {
        validator: ValidatorPublicKey,
    },
    Distribute {
        claim: RewardsClaim,
    },

    /// Fees are deposited by the validators to be distributed to the bonded validators
    DepositForFees {
        holder: ValidatorPublicKey,
        amount: u128,
    },
}

impl ContractAction for StakingAction {
    fn as_blob(
        &self,
        contract_name: ContractName,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        Blob {
            contract_name,
            data: BlobData::from(StructuredBlobData {
                caller,
                callees,
                parameters: self.clone(),
            }),
        }
    }
}

#[derive(
    Clone, BorshSerialize, BorshDeserialize, Default, Eq, PartialEq, Hash, PartialOrd, Ord,
)]
pub struct ValidatorPublicKey(pub Vec<u8>);

impl ValidatorPublicKey {
    pub fn new_for_tests(str: &str) -> Self {
        Self(str.as_bytes().to_vec())
    }
}

#[cfg(feature = "full")]
impl utoipa::PartialSchema for ValidatorPublicKey {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        String::schema()
    }
}

#[cfg(feature = "full")]
impl utoipa::ToSchema for ValidatorPublicKey {}

impl Serialize for ValidatorPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(hex::encode(&self.0).as_str())
    }
}

impl<'de> Deserialize<'de> for ValidatorPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValidatorPublicKeyVisitor;

        impl Visitor<'_> for ValidatorPublicKeyVisitor {
            type Value = ValidatorPublicKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a hex string representing a ValidatorPublicKey")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let bytes = hex::decode(value).map_err(de::Error::custom)?;
                Ok(ValidatorPublicKey(bytes))
            }
        }

        deserializer.deserialize_str(ValidatorPublicKeyVisitor)
    }
}

impl std::fmt::Debug for ValidatorPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ValidatorPubK")
            .field(&hex::encode(
                self.0.get(..HASH_DISPLAY_SIZE).unwrap_or(&self.0),
            ))
            .finish()
    }
}

impl std::fmt::Display for ValidatorPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            &hex::encode(self.0.get(..HASH_DISPLAY_SIZE).unwrap_or(&self.0),)
        )
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct LaneId(pub ValidatorPublicKey);

// Cumulative size of the lane from the beginning
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct LaneBytesSize(pub u64); // 16M Terabytes, is it enough ?

impl std::ops::Add<usize> for LaneBytesSize {
    type Output = Self;
    fn add(self, other: usize) -> Self {
        LaneBytesSize(self.0 + other as u64)
    }
}

impl std::fmt::Display for LaneBytesSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 < 1024 {
            write!(f, "{} B", self.0)
        } else if self.0 < 1024 * 1024 {
            write!(f, "{} KB", self.0 / 1024)
        } else if self.0 < 1024 * 1024 * 1024 {
            write!(f, "{} MB", self.0 / (1024 * 1024))
        } else if self.0 < 1024 * 1024 * 1024 * 1024 {
            write!(f, "{} GB", self.0 / (1024 * 1024 * 1024))
        } else {
            write!(f, "{} TB", self.0 / (1024 * 1024 * 1024 * 1024))
        }
    }
}
