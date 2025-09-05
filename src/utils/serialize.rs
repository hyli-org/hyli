use std::{
    io::{Read, Write},
    ops::{Deref, DerefMut},
};

use borsh::{BorshDeserialize, BorshSerialize};
use indexmap::IndexMap;

/// from <https://users.rust-lang.org/t/how-to-serialize-deserialize-an-async-std-rwlock-t-where-t-serialize-deserialize/37407>
pub mod arc_rwlock_serde {
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, RwLock};

    pub fn serialize<S, T>(val: &Arc<RwLock<T>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        #[allow(
            clippy::unwrap_used,
            reason = "Cannot panic in sync code unless poisoned where panic is OK"
        )]
        T::serialize(&*val.read().unwrap(), s)
    }

    pub fn deserialize<'de, D, T>(d: D) -> Result<Arc<RwLock<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Arc::new(RwLock::new(T::deserialize(d)?)))
    }
}

pub mod arc_rwlock_borsh {
    use std::sync::{Arc, RwLock};

    pub fn serialize<T: borsh::ser::BorshSerialize, W: borsh::io::Write>(
        obj: &Arc<RwLock<T>>,
        writer: &mut W,
    ) -> ::core::result::Result<(), borsh::io::Error> {
        #[allow(
            clippy::unwrap_used,
            reason = "Cannot panic in sync code unless poisoned where panic is OK"
        )]
        borsh::BorshSerialize::serialize(&*obj.read().unwrap(), writer)?;
        Ok(())
    }

    pub fn deserialize<R: borsh::io::Read, T: borsh::de::BorshDeserialize>(
        reader: &mut R,
    ) -> ::core::result::Result<Arc<RwLock<T>>, borsh::io::Error> {
        Ok(Arc::new(RwLock::new(T::deserialize_reader(reader)?)))
    }
}

#[derive(Default)]
pub struct BorshableIndexMap<K, V>(pub IndexMap<K, V>);

impl<K, V> DerefMut for BorshableIndexMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Deref for BorshableIndexMap<K, V> {
    type Target = IndexMap<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> BorshSerialize for BorshableIndexMap<K, V>
where
    K: BorshSerialize,
    V: BorshSerialize,
{
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let vec: Vec<(&K, &V)> = self.0.iter().collect();
        vec.serialize(writer)
    }
}

impl<K, V> BorshDeserialize for BorshableIndexMap<K, V>
where
    K: BorshDeserialize + Eq + std::hash::Hash,
    V: BorshDeserialize,
{
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let vec: Vec<(K, V)> = Vec::deserialize_reader(reader)?;
        let map: IndexMap<K, V> = vec.into_iter().collect();
        Ok(BorshableIndexMap(map))
    }
}
