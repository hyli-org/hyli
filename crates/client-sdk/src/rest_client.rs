use std::{
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    time::Duration,
};

use anyhow::{Context, Result};
use hyle_net::http::HttpClient;
use sdk::{
    api::{
        APIBlob, APIBlock, APIContract, APIRegisterContract, APIStaking, APITransaction, NodeInfo,
        TransactionWithBlobs,
    },
    BlobIndex, BlobTransaction, BlockHash, BlockHeight, ConsensusInfo, Contract, ContractName,
    ProofTransaction, TxHash, UnsettledBlobTransaction, ValidatorPublicKey,
};

#[derive(Clone)]
pub struct IndexerApiHttpClient {
    pub client: HttpClient,
}

impl IndexerApiHttpClient {
    pub fn new(url: String) -> Result<Self> {
        Ok(IndexerApiHttpClient {
            client: HttpClient {
                url: url.parse()?,
                api_key: None,
                retry: None,
            },
        })
    }
    pub async fn list_contracts(&self) -> Result<Vec<APIContract>> {
        self.get("v1/indexer/contracts")
            .await
            .context("listing contracts")
    }

    pub async fn get_indexer_contract(&self, contract_name: &ContractName) -> Result<APIContract> {
        self.get(&format!("v1/indexer/contract/{contract_name}"))
            .await
            .context(format!("getting contract {contract_name}"))
    }

    pub async fn fetch_current_state<State>(&self, contract_name: &ContractName) -> Result<State>
    where
        State: serde::de::DeserializeOwned,
    {
        self.get::<State>(&format!("v1/indexer/contract/{contract_name}/state"))
            .await
            .context(format!("getting contract {contract_name} state"))
    }

    pub async fn get_block_height(&self) -> Result<BlockHeight> {
        let block: APIBlock = self.get_last_block().await?;
        Ok(BlockHeight(block.height))
    }

    pub async fn get_blocks(&self) -> Result<Vec<APIBlock>> {
        self.get("v1/indexer/blocks")
            .await
            .context("getting blocks")
    }

    pub async fn get_last_block(&self) -> Result<APIBlock> {
        self.get("v1/indexer/block/last")
            .await
            .context("getting last block")
    }

    pub async fn get_block_by_height(&self, height: &BlockHeight) -> Result<APIBlock> {
        self.get(&format!("v1/indexer/block/height/{height}"))
            .await
            .context(format!("getting block with height {height}"))
    }

    pub async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<APIBlock> {
        self.get(&format!("v1/indexer/block/hash/{hash}"))
            .await
            .context(format!("getting block with hash {hash}"))
    }

    pub async fn get_transactions(&self) -> Result<Vec<APITransaction>> {
        self.get("v1/indexer/transactions")
            .await
            .context("getting transactions")
    }

    pub async fn get_transactions_by_height(
        &self,
        height: &BlockHeight,
    ) -> Result<Vec<APITransaction>> {
        self.get(&format!("v1/indexer/transactions/block/{height}"))
            .await
            .context(format!("getting transactions for block height {height}"))
    }

    pub async fn get_transactions_by_contract(
        &self,
        contract_name: &ContractName,
    ) -> Result<Vec<APITransaction>> {
        self.get(&format!("v1/indexer/transactions/contract/{contract_name}"))
            .await
            .context(format!("getting transactions for contract {contract_name}"))
    }

    pub async fn get_transaction_with_hash(&self, tx_hash: &TxHash) -> Result<APITransaction> {
        self.get(&format!("v1/indexer/transaction/hash/{tx_hash}"))
            .await
            .context(format!("getting transaction with hash {tx_hash}"))
    }

    pub async fn get_blob_transactions_by_contract(
        &self,
        contract_name: &ContractName,
    ) -> Result<Vec<TransactionWithBlobs>> {
        self.get(&format!(
            "v1/indexer/blob_transactions/contract/{contract_name}"
        ))
        .await
        .context(format!(
            "getting blob transactions for contract {contract_name}"
        ))
    }

    pub async fn get_blobs_by_tx_hash(&self, tx_hash: &TxHash) -> Result<Vec<APIBlob>> {
        self.get(&format!("v1/indexer/blobs/hash/{tx_hash}"))
            .await
            .context(format!("getting blob by transaction hash {tx_hash}"))
    }

    pub async fn get_blob(&self, tx_hash: &TxHash, blob_index: BlobIndex) -> Result<APIBlob> {
        self.get(&format!(
            "v1/indexer/blob/hash/{tx_hash}/index/{blob_index}"
        ))
        .await
        .context(format!(
            "getting blob with hash {tx_hash} and index {blob_index}"
        ))
    }
}

impl Deref for IndexerApiHttpClient {
    type Target = HttpClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Clone)]
pub struct NodeApiHttpClient {
    pub client: HttpClient,
}

pub trait NodeApiClient {
    fn register_contract(
        &self,
        tx: APIRegisterContract,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>>;

    fn send_tx_blob(
        &self,
        tx: BlobTransaction,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>>;

    fn send_tx_proof(
        &self,
        tx: ProofTransaction,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>>;

    fn get_consensus_info(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConsensusInfo>> + Send + '_>>;

    fn get_consensus_staking_state(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<APIStaking>> + Send + '_>>;

    fn get_node_info(&self) -> Pin<Box<dyn Future<Output = Result<NodeInfo>> + Send + '_>>;

    fn metrics(&self) -> Pin<Box<dyn Future<Output = Result<String>> + Send + '_>>;

    fn get_block_height(&self) -> Pin<Box<dyn Future<Output = Result<BlockHeight>> + Send + '_>>;

    fn get_contract(
        &self,
        contract_name: ContractName,
    ) -> Pin<Box<dyn Future<Output = Result<Contract>> + Send + '_>>;

    fn get_settled_height(
        &self,
        contract_name: ContractName,
    ) -> Pin<Box<dyn Future<Output = Result<BlockHeight>> + Send + '_>>;

    fn get_unsettled_tx(
        &self,
        blob_tx_hash: TxHash,
    ) -> Pin<Box<dyn Future<Output = Result<UnsettledBlobTransaction>> + Send + '_>>;
}

impl NodeApiHttpClient {
    pub fn new(url: String) -> Result<Self> {
        Ok(NodeApiHttpClient {
            client: HttpClient {
                url: url.parse()?,
                api_key: None,
                retry: None,
            },
        })
    }

    /// Create a new client with a retry configuration (retrying `n` times, waiting `duration` before each retry)
    pub fn with_retry(&self, n: usize, duration: Duration) -> Self {
        let mut cloned = self.clone();
        cloned.retry = Some((n, duration));
        cloned
    }

    /// Create a client with the retry configuration (n=3, duration=1000ms)
    pub fn retry_15times_1000ms(&self) -> Self {
        self.with_retry(8, Duration::from_millis(4000))
    }
}

impl NodeApiClient for NodeApiHttpClient {
    fn register_contract(
        &self,
        tx: APIRegisterContract,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>> {
        Box::pin(async move {
            self.post_json("v1/contract/register", &tx)
                .await
                .context("Registering contract")
        })
    }

    fn send_tx_blob(
        &self,
        tx: BlobTransaction,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>> {
        Box::pin(async move {
            self.post_json("v1/tx/send/blob", &tx)
                .await
                .context("Sending tx blob")
        })
    }

    fn send_tx_proof(
        &self,
        tx: ProofTransaction,
    ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>> {
        Box::pin(async move {
            self.post_json("v1/tx/send/proof", &tx)
                .await
                .context("Sending tx proof")
        })
    }

    fn get_consensus_info(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConsensusInfo>> + Send + '_>> {
        Box::pin(async move {
            self.get("v1/consensus/info")
                .await
                .context("getting consensus info")
        })
    }

    fn get_consensus_staking_state(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<APIStaking>> + Send + '_>> {
        Box::pin(async move {
            self.get("v1/consensus/staking_state")
                .await
                .context("getting consensus staking state")
        })
    }

    fn get_node_info(&self) -> Pin<Box<dyn Future<Output = Result<NodeInfo>> + Send + '_>> {
        Box::pin(async move { self.get("v1/info").await.context("getting node info") })
    }

    fn metrics(&self) -> Pin<Box<dyn Future<Output = Result<String>> + Send + '_>> {
        Box::pin(async move {
            self.get_str("v1/metrics")
                .await
                .context("getting node metrics")
        })
    }

    fn get_block_height(&self) -> Pin<Box<dyn Future<Output = Result<BlockHeight>> + Send + '_>> {
        Box::pin(async move {
            self.get("v1/da/block/height")
                .await
                .context("getting block height")
        })
    }

    fn get_contract(
        &self,
        contract_name: ContractName,
    ) -> Pin<Box<dyn Future<Output = Result<Contract>> + Send + '_>> {
        Box::pin(async move {
            self.get(&format!("v1/contract/{}", contract_name))
                .await
                .context(format!("getting contract {}", contract_name))
        })
    }

    fn get_unsettled_tx(
        &self,
        blob_tx_hash: TxHash,
    ) -> Pin<Box<dyn Future<Output = Result<UnsettledBlobTransaction>> + Send + '_>> {
        Box::pin(async move {
            self.get(&format!("v1/unsettled_tx/{blob_tx_hash}"))
                .await
                .context(format!("getting tx {}", blob_tx_hash))
        })
    }

    fn get_settled_height(
        &self,
        contract_name: ContractName,
    ) -> Pin<Box<dyn Future<Output = Result<BlockHeight>> + Send + '_>> {
        Box::pin(async move {
            self.get(&format!("v1/contract/{contract_name}/settled_height"))
                .await
                .context(format!(
                    "getting earliest unsettled height for contract {}",
                    contract_name
                ))
        })
    }
}
impl Deref for NodeApiHttpClient {
    type Target = HttpClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
impl DerefMut for NodeApiHttpClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

pub mod test {
    use sdk::Hashed;

    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    pub struct NodeApiMockClient {
        pub block_height: Arc<Mutex<BlockHeight>>,
        pub settled_height: Arc<Mutex<BlockHeight>>,
        pub consensus_info: Arc<Mutex<ConsensusInfo>>,
        pub node_info: Arc<Mutex<NodeInfo>>,
        pub staking_state: Arc<Mutex<APIStaking>>,
        pub contracts: Arc<Mutex<std::collections::HashMap<ContractName, Contract>>>,
        pub unsettled_txs: Arc<Mutex<std::collections::HashMap<TxHash, UnsettledBlobTransaction>>>,
        pub pending_proofs: Arc<Mutex<Vec<ProofTransaction>>>,
        pub pending_blobs: Arc<Mutex<Vec<BlobTransaction>>>,
    }

    impl NodeApiMockClient {
        pub fn new() -> Self {
            Self {
                block_height: Arc::new(Mutex::new(BlockHeight(0))),
                settled_height: Arc::new(Mutex::new(BlockHeight(0))),
                consensus_info: Arc::new(Mutex::new(ConsensusInfo {
                    slot: 0,
                    view: 0,
                    round_leader: ValidatorPublicKey::default(),
                    validators: vec![],
                })),
                node_info: Arc::new(Mutex::new(NodeInfo {
                    id: "mock_node_id".to_string(),
                    pubkey: Some(ValidatorPublicKey::default()),
                    da_address: "mock_da_address".to_string(),
                })),
                staking_state: Arc::new(Mutex::new(APIStaking::default())),
                contracts: Arc::new(Mutex::new(std::collections::HashMap::new())),
                unsettled_txs: Arc::new(Mutex::new(std::collections::HashMap::new())),
                pending_proofs: Arc::new(Mutex::new(vec![])),
                pending_blobs: Arc::new(Mutex::new(vec![])),
            }
        }

        pub fn set_block_height(&self, height: BlockHeight) {
            *self.block_height.lock().unwrap() = height;
        }

        pub fn set_settled_height(&self, height: BlockHeight) {
            *self.settled_height.lock().unwrap() = height;
        }

        pub fn set_consensus_info(&self, info: ConsensusInfo) {
            *self.consensus_info.lock().unwrap() = info;
        }

        pub fn set_node_info(&self, info: NodeInfo) {
            *self.node_info.lock().unwrap() = info;
        }

        pub fn set_staking_state(&self, state: APIStaking) {
            *self.staking_state.lock().unwrap() = state;
        }

        pub fn add_contract(&self, contract: Contract) {
            self.contracts
                .lock()
                .unwrap()
                .insert(contract.name.clone(), contract);
        }

        pub fn add_unsettled_tx(&self, tx_hash: TxHash, tx: UnsettledBlobTransaction) {
            self.unsettled_txs.lock().unwrap().insert(tx_hash, tx);
        }
    }

    impl Default for NodeApiMockClient {
        fn default() -> Self {
            Self::new()
        }
    }

    impl NodeApiClient for NodeApiMockClient {
        fn register_contract(
            &self,
            tx: APIRegisterContract,
        ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>> {
            Box::pin(async move { Ok(BlobTransaction::from(tx).hashed()) })
        }

        fn send_tx_blob(
            &self,
            tx: BlobTransaction,
        ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>> {
            self.pending_blobs.lock().unwrap().push(tx.clone());
            Box::pin(async move { Ok(tx.hashed()) })
        }

        fn send_tx_proof(
            &self,
            tx: ProofTransaction,
        ) -> Pin<Box<dyn Future<Output = Result<TxHash>> + Send + '_>> {
            self.pending_proofs.lock().unwrap().push(tx.clone());
            Box::pin(async move { Ok(tx.hashed()) })
        }

        fn get_consensus_info(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<ConsensusInfo>> + Send + '_>> {
            Box::pin(async move { Ok(self.consensus_info.lock().unwrap().clone()) })
        }

        fn get_consensus_staking_state(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<APIStaking>> + Send + '_>> {
            Box::pin(async move { Ok(self.staking_state.lock().unwrap().clone()) })
        }

        fn get_node_info(&self) -> Pin<Box<dyn Future<Output = Result<NodeInfo>> + Send + '_>> {
            Box::pin(async move { Ok(self.node_info.lock().unwrap().clone()) })
        }

        fn metrics(&self) -> Pin<Box<dyn Future<Output = Result<String>> + Send + '_>> {
            Box::pin(async move { Ok("mock metrics".to_string()) })
        }

        fn get_block_height(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<BlockHeight>> + Send + '_>> {
            Box::pin(async move { Ok(*self.block_height.lock().unwrap()) })
        }

        fn get_contract(
            &self,
            contract_name: ContractName,
        ) -> Pin<Box<dyn Future<Output = Result<Contract>> + Send + '_>> {
            Box::pin(async move {
                self.contracts
                    .lock()
                    .unwrap()
                    .get(&contract_name)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Contract not found"))
            })
        }

        fn get_unsettled_tx(
            &self,
            blob_tx_hash: TxHash,
        ) -> Pin<Box<dyn Future<Output = Result<UnsettledBlobTransaction>> + Send + '_>> {
            Box::pin(async move {
                self.unsettled_txs
                    .lock()
                    .unwrap()
                    .get(&blob_tx_hash)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Unsettled transaction not found"))
            })
        }

        fn get_settled_height(
            &self,
            _contract_name: ContractName,
        ) -> Pin<Box<dyn Future<Output = Result<BlockHeight>> + Send + '_>> {
            Box::pin(async move { Ok(*self.settled_height.lock().unwrap()) })
        }
    }
}
