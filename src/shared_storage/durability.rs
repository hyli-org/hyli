use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use anyhow::{bail, Result};
use borsh::BorshSerialize;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use futures::FutureExt;
use hyli_model::LaneId;
use hyli_modules::node_state::module::load_current_chain_timestamp;
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::model::{DataProposal, DataProposalHash, Hashed};

pub trait DurabilityBackend: Send + Sync {
    fn upload_data_proposal(
        &self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        payload: Vec<u8>,
        current_chain_timestamp: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}

pub struct NullDurabilityBackend;

impl DurabilityBackend for NullDurabilityBackend {
    fn upload_data_proposal(
        &self,
        _lane_id: LaneId,
        _dp_hash: DataProposalHash,
        _payload: Vec<u8>,
        _current_chain_timestamp: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(async { Ok(()) })
    }
}

type ProposalKey = (LaneId, DataProposalHash);
type SharedPersistenceStateStore = Arc<dyn PersistenceStateStore>;

static SHARED_PERSISTENCE_STORES: OnceLock<Mutex<HashMap<PathBuf, SharedPersistenceStateStore>>> =
    OnceLock::new();

trait PersistenceStateStore: Send + Sync {
    fn mark_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()>;
    fn is_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<bool>;
    fn persist(&self) -> Result<()>;
}

#[cfg(test)]
#[derive(Default)]
struct InMemoryPersistenceStateStore {
    persisted: RwLock<HashMap<ProposalKey, ()>>,
}

#[cfg(test)]
impl PersistenceStateStore for InMemoryPersistenceStateStore {
    fn mark_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        self.persisted
            .write()
            .unwrap()
            .insert((lane_id.clone(), dp_hash.clone()), ());
        Ok(())
    }

    fn is_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<bool> {
        Ok(self
            .persisted
            .read()
            .unwrap()
            .contains_key(&(lane_id.clone(), dp_hash.clone())))
    }

    fn persist(&self) -> Result<()> {
        Ok(())
    }
}

struct FjallPersistenceStateStore {
    db: Database,
    persisted: Keyspace,
}

impl FjallPersistenceStateStore {
    fn shared(path: &Path) -> Result<SharedPersistenceStateStore> {
        let registry = SHARED_PERSISTENCE_STORES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(existing) = guard.get(path) {
            return Ok(Arc::clone(existing));
        }

        let store: SharedPersistenceStateStore = Arc::new(Self::new(path)?);
        guard.insert(path.to_path_buf(), Arc::clone(&store));
        Ok(store)
    }

    fn new(path: &Path) -> Result<Self> {
        let db = Database::builder(path.join("data_proposal_durability.db"))
            .cache_size(32 * 1024 * 1024)
            .max_journaling_size(64 * 1024 * 1024)
            .open()?;
        let persisted = db.keyspace("dp_persisted", KeyspaceCreateOptions::default)?;
        Ok(Self { db, persisted })
    }
}

impl PersistenceStateStore for FjallPersistenceStateStore {
    fn mark_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        self.persisted
            .insert(&persisted_key(lane_id, dp_hash)?, borsh::to_vec(&())?)?;
        Ok(())
    }

    fn is_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<bool> {
        Ok(self
            .persisted
            .contains_key(&persisted_key(lane_id, dp_hash)?)?)
    }

    fn persist(&self) -> Result<()> {
        self.db.persist(fjall::PersistMode::Buffer)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum PersistenceState {
    NotStarted,
    InFlight,
    Succeeded,
    Failed(String),
}

#[derive(Debug)]
struct PersistenceTracker {
    state: Mutex<PersistenceState>,
    notify: Notify,
}

impl Default for PersistenceTracker {
    fn default() -> Self {
        Self {
            state: Mutex::new(PersistenceState::NotStarted),
            notify: Notify::new(),
        }
    }
}

#[derive(Clone)]
pub struct DataProposalDurability {
    backend: Arc<dyn DurabilityBackend>,
    current_chain_timestamp: Arc<RwLock<Option<String>>>,
    persistence_state: SharedPersistenceStateStore,
    in_flight: Arc<RwLock<HashMap<ProposalKey, Arc<PersistenceTracker>>>>,
}

impl DataProposalDurability {
    pub fn new(backend: Arc<dyn DurabilityBackend>, data_directory: &Path) -> Result<Self> {
        Ok(Self {
            backend,
            current_chain_timestamp: Arc::new(RwLock::new(
                load_current_chain_timestamp(data_directory).ok(),
            )),
            persistence_state: FjallPersistenceStateStore::shared(data_directory)?,
            in_flight: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    #[cfg(test)]
    pub fn new_in_memory(backend: Arc<dyn DurabilityBackend>) -> Self {
        Self {
            backend,
            current_chain_timestamp: Arc::new(RwLock::new(None)),
            persistence_state: Arc::new(InMemoryPersistenceStateStore::default()),
            in_flight: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_current_chain_timestamp(&self, current_chain_timestamp: String) {
        let mut guard = self
            .current_chain_timestamp
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(current_chain_timestamp);
    }

    pub fn has_current_chain_timestamp(&self) -> bool {
        self.current_chain_timestamp
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    pub fn prime_persistence(&self, lane_id: LaneId, data_proposal: &DataProposal) -> Result<()> {
        let mut canonical = data_proposal.clone();
        canonical.remove_proofs();
        let dp_hash = canonical.hashed();
        if self.persistence_state.is_persisted(&lane_id, &dp_hash)? {
            return Ok(());
        }
        let key = (lane_id.clone(), dp_hash.clone());
        let tracker = self.persistence_tracker(key.clone());

        {
            let mut state = tracker
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &*state {
                PersistenceState::InFlight | PersistenceState::Succeeded => return Ok(()),
                PersistenceState::NotStarted | PersistenceState::Failed(_) => {
                    *state = PersistenceState::InFlight
                }
            }
        }

        let payload = borsh::to_vec(&canonical)?;
        let backend = Arc::clone(&self.backend);
        let current_chain_timestamp = self
            .current_chain_timestamp
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let persistence_state = Arc::clone(&self.persistence_state);
        let in_flight = Arc::clone(&self.in_flight);
        let tracker = Arc::clone(&tracker);
        let mut upload = backend.upload_data_proposal(
            lane_id.clone(),
            dp_hash.clone(),
            payload,
            current_chain_timestamp,
        );

        if let Some(result) = upload.as_mut().now_or_never() {
            Self::finish_persistence_attempt(
                &tracker,
                &in_flight,
                &persistence_state,
                &lane_id,
                &dp_hash,
                result,
            );
            return Ok(());
        }

        let handle = tokio::runtime::Handle::try_current()?;

        // DP persistence is currently fire-and-forget per hash with no dedicated limiter.
        // Revisit concurrency control once the retry policy and operational envelope settle.
        handle.spawn(async move {
            let result = upload.await;
            Self::finish_persistence_attempt(
                &tracker,
                &in_flight,
                &persistence_state,
                &lane_id,
                &dp_hash,
                result,
            );
        });

        Ok(())
    }

    pub async fn wait_until_persisted(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<()> {
        if self.persistence_state.is_persisted(lane_id, dp_hash)? {
            return Ok(());
        }

        let tracker = self.persistence_tracker((lane_id.clone(), dp_hash.clone()));
        loop {
            let notified = tracker.notify.notified();
            let state = tracker
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            match state {
                PersistenceState::Succeeded => return Ok(()),
                PersistenceState::Failed(err) => {
                    bail!(
                        "Data proposal {} on lane {} was not persisted: {}",
                        dp_hash,
                        lane_id,
                        err
                    );
                }
                PersistenceState::NotStarted => {
                    bail!(
                        "Data proposal {} on lane {} was never scheduled for persistence",
                        dp_hash,
                        lane_id
                    );
                }
                PersistenceState::InFlight => notified.await,
            }
        }
    }

    pub fn is_persisted(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        self.persistence_state
            .is_persisted(lane_id, dp_hash)
            .unwrap_or(false)
            || self
                .in_flight
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&(lane_id.clone(), dp_hash.clone()))
                .cloned()
                .is_some_and(|tracker| {
                    matches!(
                        *tracker
                            .state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()),
                        PersistenceState::Succeeded
                    )
                })
    }

    pub fn persist(&self) -> Result<()> {
        self.persistence_state.persist()
    }

    fn persistence_tracker(&self, key: ProposalKey) -> Arc<PersistenceTracker> {
        if let Some(existing) = self
            .in_flight
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .cloned()
        {
            return existing;
        }

        let mut guard = self
            .in_flight
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(PersistenceTracker::default()))
            .clone()
    }

    fn finish_persistence_attempt(
        tracker: &Arc<PersistenceTracker>,
        in_flight: &Arc<RwLock<HashMap<ProposalKey, Arc<PersistenceTracker>>>>,
        persistence_state: &SharedPersistenceStateStore,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        result: Result<()>,
    ) {
        let next_state = match result {
            Ok(()) => {
                if let Err(err) = persistence_state.mark_persisted(lane_id, dp_hash) {
                    warn!(
                        "Persisted data proposal {} on lane {} but failed to mark it persisted locally: {err:#}",
                        dp_hash, lane_id
                    );
                    PersistenceState::Failed(err.to_string())
                } else {
                    debug!("Persisted data proposal {} on lane {}", dp_hash, lane_id);
                    PersistenceState::Succeeded
                }
            }
            Err(err) => {
                warn!(
                    "Failed to persist data proposal {} on lane {}: {err:#}",
                    dp_hash, lane_id
                );
                PersistenceState::Failed(err.to_string())
            }
        };

        *tracker
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next_state;
        tracker.notify.notify_waiters();
        if matches!(
            *tracker
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            PersistenceState::Succeeded
        ) {
            in_flight
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&(lane_id.clone(), dp_hash.clone()));
        }
    }
}

fn persisted_key(lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<Vec<u8>> {
    namespaced_key(b"persisted:", &(lane_id, dp_hash))
}

fn namespaced_key<K: BorshSerialize>(prefix: &[u8], key: &K) -> Result<Vec<u8>> {
    let mut encoded = prefix.to_vec();
    encoded.extend(borsh::to_vec(key)?);
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mempool::tests::make_register_contract_tx,
        shared_storage::gcs::{DpGcsRuntime, GcsDurabilityBackend},
        utils::conf::DataProposalDurabilityConf,
    };
    use axum::{
        extract::{Path, Request},
        response::IntoResponse,
        routing::post,
        Json, Router,
    };
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_storage::client::Storage;
    use hyli_crypto::BlstCrypto;
    use hyli_model::ContractName;
    use serde_json::json;
    use std::sync::{Arc, OnceLock};
    use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};

    static GCS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn gcs_test_lock() -> &'static Mutex<()> {
        GCS_TEST_LOCK.get_or_init(|| Mutex::const_new(()))
    }

    struct TestGcsServer {
        task: JoinHandle<()>,
    }

    impl Drop for TestGcsServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn build_test_gcs_runtime(endpoint: String) -> anyhow::Result<DpGcsRuntime> {
        let client = Storage::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        Ok(DpGcsRuntime {
            client,
            conf: DataProposalDurabilityConf {
                gcs_bucket: "test-bucket".to_string(),
                gcs_prefix: "test-prefix".to_string(),
                save_data_proposals: true,
            },
        })
    }

    async fn start_test_gcs_server() -> anyhow::Result<(String, TestGcsServer)> {
        async fn upload(Path(bucket): Path<String>) -> impl IntoResponse {
            Json(json!({
                "bucket": format!("projects/_/buckets/{bucket}"),
                "name": "stored-object",
                "size": 1,
            }))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let app = Router::new().route("/upload/storage/v1/b/{bucket}/o", post(upload));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok((endpoint, TestGcsServer { task }))
    }

    async fn start_failing_test_gcs_server() -> anyhow::Result<(String, TestGcsServer)> {
        async fn upload() -> impl IntoResponse {
            (axum::http::StatusCode::BAD_REQUEST, "upload rejected")
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let app = Router::new().route("/upload/storage/v1/b/{bucket}/o", post(upload));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok((endpoint, TestGcsServer { task }))
    }

    async fn start_recording_test_gcs_server(
        recorded_queries: Arc<Mutex<Vec<String>>>,
    ) -> anyhow::Result<(String, TestGcsServer)> {
        async fn upload(
            Path(bucket): Path<String>,
            request: Request,
            recorded_queries: Arc<Mutex<Vec<String>>>,
        ) -> impl IntoResponse {
            recorded_queries
                .lock()
                .await
                .push(request.uri().query().unwrap_or_default().to_string());
            Json(json!({
                "bucket": format!("projects/_/buckets/{bucket}"),
                "name": "stored-object",
                "size": 1,
            }))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let app = Router::new().route(
            "/upload/storage/v1/b/{bucket}/o",
            post({
                let recorded_queries = Arc::clone(&recorded_queries);
                move |path, request| upload(path, request, Arc::clone(&recorded_queries))
            }),
        );
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok((endpoint, TestGcsServer { task }))
    }

    #[test_log::test(tokio::test)]
    async fn test_wait_until_persisted_succeeds_with_mock_gcs() -> Result<()> {
        let _guard = gcs_test_lock().lock().await;
        let (endpoint, _server) = start_test_gcs_server().await?;
        let durability = DataProposalDurability::new_in_memory(Arc::new(
            GcsDurabilityBackend::with_runtime(build_test_gcs_runtime(endpoint.clone()).await?),
        ));
        durability.set_current_chain_timestamp("2026-03-30T14-00-00Z".to_string());

        let crypto = BlstCrypto::new("persistence-success").unwrap();
        let lane_id = LaneId::new(crypto.validator_pubkey().clone());
        let data_proposal = DataProposal::new_root(
            lane_id.clone(),
            vec![make_register_contract_tx(ContractName::new(
                "test-persisted",
            ))],
        );
        let hash = data_proposal.hashed();

        durability.prime_persistence(lane_id.clone(), &data_proposal)?;
        durability.wait_until_persisted(&lane_id, &hash).await?;

        assert!(durability.is_persisted(&lane_id, &hash));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_wait_until_persisted_fails_when_gcs_upload_fails() -> Result<()> {
        let _guard = gcs_test_lock().lock().await;
        let (endpoint, _server) = start_failing_test_gcs_server().await?;
        let durability = DataProposalDurability::new_in_memory(Arc::new(
            GcsDurabilityBackend::with_runtime(build_test_gcs_runtime(endpoint.clone()).await?),
        ));
        durability.set_current_chain_timestamp("2026-03-30T14-00-00Z".to_string());

        let crypto = BlstCrypto::new("persistence-failure").unwrap();
        let lane_id = LaneId::new(crypto.validator_pubkey().clone());
        let data_proposal = DataProposal::new_root(
            lane_id.clone(),
            vec![make_register_contract_tx(ContractName::new(
                "test-persist-fail",
            ))],
        );
        let hash = data_proposal.hashed();

        durability.prime_persistence(lane_id.clone(), &data_proposal)?;
        let err = durability
            .wait_until_persisted(&lane_id, &hash)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("was not persisted"), "{err:#}");
        assert!(!durability.is_persisted(&lane_id, &hash));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_persisted_state_is_reused_by_new_durability_instance() -> Result<()> {
        let _guard = gcs_test_lock().lock().await;
        let (endpoint, _server) = start_test_gcs_server().await?;
        let dir = tempfile::tempdir()?;
        let durability = DataProposalDurability::new(
            Arc::new(GcsDurabilityBackend::with_runtime(
                build_test_gcs_runtime(endpoint.clone()).await?,
            )),
            dir.path(),
        )?;
        durability.set_current_chain_timestamp("2026-03-30T14-00-00Z".to_string());

        let crypto = BlstCrypto::new("persistence-reused").unwrap();
        let lane_id = LaneId::new(crypto.validator_pubkey().clone());
        let data_proposal = DataProposal::new_root(
            lane_id.clone(),
            vec![make_register_contract_tx(ContractName::new(
                "test-persisted-reused",
            ))],
        );
        let hash = data_proposal.hashed();

        durability.prime_persistence(lane_id.clone(), &data_proposal)?;
        durability.wait_until_persisted(&lane_id, &hash).await?;

        let fresh = DataProposalDurability::new(
            Arc::new(GcsDurabilityBackend::with_runtime(
                build_test_gcs_runtime(endpoint).await?,
            )),
            dir.path(),
        )?;
        assert!(fresh.is_persisted(&lane_id, &hash));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_gcs_upload_prefix_includes_genesis_timestamp_folder() -> Result<()> {
        let _guard = gcs_test_lock().lock().await;
        let recorded_queries = Arc::new(Mutex::new(Vec::new()));
        let (endpoint, _server) =
            start_recording_test_gcs_server(Arc::clone(&recorded_queries)).await?;
        let durability = DataProposalDurability::new_in_memory(Arc::new(
            GcsDurabilityBackend::with_runtime(DpGcsRuntime {
                client: Storage::builder()
                    .with_endpoint(endpoint)
                    .with_credentials(Anonymous::new().build())
                    .build()
                    .await?,
                conf: DataProposalDurabilityConf {
                    gcs_bucket: "test-bucket".to_string(),
                    gcs_prefix: "test-prefix".to_string(),
                    save_data_proposals: true,
                },
            }),
        ));
        durability.set_current_chain_timestamp("2026-03-30T14-00-00Z".to_string());

        let crypto = BlstCrypto::new("persistence-prefix").unwrap();
        let lane_id = LaneId::new(crypto.validator_pubkey().clone());
        let data_proposal = DataProposal::new_root(
            lane_id.clone(),
            vec![make_register_contract_tx(ContractName::new(
                "test-prefix-dp",
            ))],
        );
        let hash = data_proposal.hashed();

        durability.prime_persistence(lane_id.clone(), &data_proposal)?;
        durability.wait_until_persisted(&lane_id, &hash).await?;

        let queries = recorded_queries.lock().await;
        assert!(queries.iter().any(|query| {
            query.contains("name=test-prefix%2F2026-03-30T14-00-00Z%2Fdata_proposals%2F")
        }));
        Ok(())
    }
}
