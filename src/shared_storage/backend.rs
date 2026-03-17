use anyhow::Result;
use std::sync::Arc;

pub trait KvEncode: Send + Sync + 'static {
    fn encode(&self) -> Result<Vec<u8>>;
}

impl<T> KvEncode for T
where
    T: borsh::BorshSerialize + Send + Sync + 'static,
{
    fn encode(&self) -> Result<Vec<u8>> {
        borsh::to_vec(self).map_err(Into::into)
    }
}

pub trait KvBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: Arc<dyn KvEncode>) -> Result<()>;
    fn contains_key(&self, key: &[u8]) -> Result<bool>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn persist(&self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedStorageCommand {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl hyli_modules::bus::BusMessage for SharedStorageCommand {}
