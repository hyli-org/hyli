mod config;
mod metrics;
mod storage;
mod store;

pub use config::{GCSConf, GcsUploaderCtx};
pub use metrics::GcsUploaderMetrics;
pub use storage::{GcsStorageBackend, LocalStorageBackend, StorageBackend};

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use sdk::{BlockHeight, DataEvent, DataProposalHash, SignedBlock, TxHash};
use tracing::debug;

use crate::bus::{BusMessage, SharedMessageBus};
use crate::modules::Module;
use crate::{log_error, module_bus_client, module_handle_messages};

use self::store::GcsUploaderStore;

// ============================================================================
// Bus Messages
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GCSRequest {
    ProofUpload {
        proof: Vec<u8>,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
    },
}

impl BusMessage for GCSRequest {}

module_bus_client! {
    #[derive(Debug)]
    struct GcsUploaderBusClient {
        receiver(DataEvent),
        receiver(GCSRequest),
    }
}

// ============================================================================
// Main GCS Uploader Module
// ============================================================================

pub struct GcsUploader {
    ctx: GcsUploaderCtx,
    bus: GcsUploaderBusClient,
    storage: Box<dyn StorageBackend>,
    store: GcsUploaderStore,
    metrics: GcsUploaderMetrics,
    buffered_blocks: Vec<(BlockHeight, SignedBlock)>,
    last_batch_time: Instant,
}

impl Module for GcsUploader {
    type Context = GcsUploaderCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = GcsUploaderBusClient::new_from_bus(bus.new_handle()).await;

        // Créer le storage backend selon la config
        let storage: Box<dyn StorageBackend> = match ctx.gcs_config.storage_backend.as_str() {
            "local" => {
                tracing::info!("Using local storage backend for testing");
                Box::new(LocalStorageBackend::new(
                    ctx.data_directory.join("gcs_storage"),
                )?)
            }
            "gcs" => {
                tracing::info!("Using GCS storage backend");
                Box::new(
                    GcsStorageBackend::new(
                        ctx.gcs_config.gcs_bucket.clone(),
                        ctx.gcs_config.gcs_prefix.clone(),
                    )
                    .await?,
                )
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown storage backend: {}",
                    ctx.gcs_config.storage_backend
                ));
            }
        };

        // Charger le store depuis le disque
        let store = Self::load_from_disk_or_default::<GcsUploaderStore>(
            ctx.data_directory.join("gcs_uploader_store.bin").as_path(),
        );

        let metrics = GcsUploaderMetrics::global("gcs_uploader".to_string());

        // Initialiser les métriques avec l'état actuel
        metrics.record_last_uploaded_height(store.last_uploaded_height.0);
        metrics.record_retry_queue_size(store.retry_queue.len() as u64);

        tracing::info!(
            "GCS Uploader initialized - last uploaded height: {}, retry queue size: {}",
            store.last_uploaded_height.0,
            store.retry_queue.len()
        );

        Ok(GcsUploader {
            ctx,
            bus,
            storage,
            store,
            metrics,
            buffered_blocks: Vec::new(),
            last_batch_time: Instant::now(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }

    async fn persist(&mut self) -> Result<()> {
        // Flusher le batch en attente
        if !self.buffered_blocks.is_empty() {
            tracing::info!(
                "Flushing {} buffered blocks before shutdown",
                self.buffered_blocks.len()
            );
            _ = log_error!(self.upload_batch().await, "Flushing pending batch");
        }

        // Sauvegarder le store
        log_error!(
            Self::save_on_disk::<GcsUploaderStore>(
                self.ctx
                    .data_directory
                    .join("gcs_uploader_store.bin")
                    .as_path(),
                &self.store,
            ),
            "Saving GCS uploader state on shutdown"
        )
    }
}

impl GcsUploader {
    pub fn read_last_uploaded_height(data_directory: &Path) -> BlockHeight {
        let store = Self::load_from_disk_or_default::<GcsUploaderStore>(
            data_directory.join("gcs_uploader_store.bin").as_path(),
        );

        store.last_uploaded_height
    }

    pub async fn start(&mut self) -> Result<()> {
        // Traiter la retry queue au démarrage
        log_error!(
            self.process_retry_queue().await,
            "Processing retry queue on startup"
        )?;

        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                if !self.ctx.gcs_config.save_blocks {
                    debug!("Skipping block upload as save_blocks is disabled");
                    return Ok(());
                }
                self.handle_data_availability_event(event).await?;

                // Vérifier le trigger temporel
                if self.last_batch_time.elapsed()
                    >= std::time::Duration::from_secs(self.ctx.gcs_config.batch_timeout_secs)
                    && !self.buffered_blocks.is_empty()
                {
                    debug!("Time trigger reached, uploading batch");
                    log_error!(self.upload_batch().await, "Time-triggered batch upload")?;
                }
            },
            listen<GCSRequest> request => {
                if !self.ctx.gcs_config.save_proofs {
                    debug!("Skipping GCS request as save_proofs is disabled");
                    return Ok(());
                }
                debug!("Received GCS request: {:?}", request);
                match request {
                    GCSRequest::ProofUpload { proof, tx_hash, parent_data_proposal_hash } => {
                        debug!("Processing proof upload of size: {}", proof.len());
                        let start = Instant::now();
                        match self.storage.upload_proof(tx_hash, parent_data_proposal_hash, proof.clone()).await {
                            Ok(bytes) => {
                                let latency = start.elapsed().as_secs_f64();
                                self.metrics.record_proof_uploaded(bytes as u64, latency);
                            }
                            Err(e) => {
                                self.metrics.record_block_failed("proof");
                                tracing::error!("Proof upload failed: {}", e);
                            }
                        }
                    }
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                // Toutes les 5 minutes, traiter la retry queue
                log_error!(self.process_retry_queue().await, "Periodic retry queue processing")?;
            }
        };
        Ok(())
    }

    async fn handle_data_availability_event(&mut self, event: DataEvent) -> Result<()> {
        let DataEvent::OrderedSignedBlock(block) = event;
        let block_height = block.height();

        // Skip si déjà uploadé
        if block_height <= self.store.last_uploaded_height {
            debug!("Skipping already uploaded block {}", block_height);
            return Ok(());
        }

        // Ajouter au buffer
        self.buffered_blocks.push((block_height, block));
        self.metrics
            .record_queue_size(self.buffered_blocks.len() as u64);

        // Vérifier les triggers de batch
        let count_trigger = self.buffered_blocks.len() >= self.ctx.gcs_config.batch_size;
        let time_trigger = self.last_batch_time.elapsed()
            >= std::time::Duration::from_secs(self.ctx.gcs_config.batch_timeout_secs);

        if count_trigger || time_trigger {
            log_error!(self.upload_batch().await, "Uploading batch")?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    async fn upload_batch(&mut self) -> Result<()> {
        if self.buffered_blocks.is_empty() {
            return Ok(());
        }

        let blocks = std::mem::take(&mut self.buffered_blocks);
        let start_height = blocks.first().unwrap().0;
        let end_height = blocks.last().unwrap().0;

        tracing::info!(
            "Starting batch upload of {} blocks (height {}-{})",
            blocks.len(),
            start_height,
            end_height
        );

        // Extraire juste les SignedBlocks
        let signed_blocks: Vec<SignedBlock> = blocks.iter().map(|(_, b)| b.clone()).collect();

        // Upload avec retry
        let start_time = Instant::now();
        match self
            .upload_batch_with_retry(start_height, signed_blocks.clone())
            .await
        {
            Ok(bytes_uploaded) => {
                let latency = start_time.elapsed().as_secs_f64();
                tracing::info!(
                    "Successfully uploaded batch block_{}-{}.bin ({} blocks, {} bytes, {:.2}s)",
                    start_height.0,
                    end_height.0,
                    signed_blocks.len(),
                    bytes_uploaded,
                    latency
                );

                // Métriques
                self.metrics
                    .record_block_uploaded("block_batch", bytes_uploaded as u64, latency);

                // Mettre à jour le checkpoint
                self.store.last_uploaded_height = end_height;
                self.metrics.record_last_uploaded_height(end_height.0);

                // Sauvegarder le checkpoint
                log_error!(
                    Self::save_on_disk::<GcsUploaderStore>(
                        self.ctx
                            .data_directory
                            .join("gcs_uploader_store.bin")
                            .as_path(),
                        &self.store,
                    ),
                    "Saving checkpoint"
                )?;
            }
            Err(e) => {
                tracing::error!("Batch upload failed after all retries: {}", e);
                self.metrics.record_block_failed("block_batch");

                // Ajouter tous les blocs à la retry queue
                for (height, block) in blocks {
                    self.store.retry_queue.insert(height, (block, 0));
                }
                self.metrics
                    .record_retry_queue_size(self.store.retry_queue.len() as u64);

                // Sauvegarder la retry queue
                log_error!(
                    Self::save_on_disk::<GcsUploaderStore>(
                        self.ctx
                            .data_directory
                            .join("gcs_uploader_store.bin")
                            .as_path(),
                        &self.store,
                    ),
                    "Saving retry queue"
                )?;
            }
        }

        self.last_batch_time = Instant::now();
        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, blocks)))]
    async fn upload_batch_with_retry(
        &self,
        start_height: BlockHeight,
        blocks: Vec<SignedBlock>,
    ) -> Result<usize> {
        let mut retry_count = 0;
        let max_retries = self.ctx.gcs_config.retry_attempts;

        loop {
            match self
                .storage
                .upload_block_batch(start_height, blocks.clone())
                .await
            {
                Ok(bytes_uploaded) => {
                    return Ok(bytes_uploaded);
                }
                Err(e) if retry_count < max_retries => {
                    let delay_secs = 2u64.pow(retry_count as u32);
                    tracing::warn!(
                        "Batch upload failed, retry {}/{} after {}s: {}",
                        retry_count + 1,
                        max_retries,
                        delay_secs,
                        e
                    );
                    self.metrics.record_retry();
                    tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                    retry_count += 1;
                }
                Err(e) => {
                    tracing::error!("Batch upload failed after {} retries: {}", max_retries, e);
                    return Err(e);
                }
            }
        }
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    async fn process_retry_queue(&mut self) -> Result<()> {
        if self.store.retry_queue.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "Processing retry queue with {} items",
            self.store.retry_queue.len()
        );

        // Regrouper les blocs consécutifs pour batch upload
        let mut current_batch = Vec::new();
        let mut current_start_height: Option<BlockHeight> = None;
        let mut to_remove = Vec::new();

        for (&height, (block, _retry_count)) in self.store.retry_queue.iter() {
            // Si le bloc est consécutif au dernier, l'ajouter au batch
            if let Some(start) = current_start_height {
                let expected_next = BlockHeight(start.0 + current_batch.len() as u64);
                if height == expected_next && current_batch.len() < self.ctx.gcs_config.batch_size {
                    current_batch.push((height, block.clone()));
                    continue;
                } else {
                    // Batch terminé, uploader
                    if !current_batch.is_empty() {
                        let start = current_batch.first().unwrap().0;
                        let blocks: Vec<_> = current_batch.iter().map(|(_, b)| b.clone()).collect();

                        match self.upload_batch_with_retry(start, blocks).await {
                            Ok(_) => {
                                for (h, _) in &current_batch {
                                    to_remove.push(*h);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to upload retry batch starting at {}: {}",
                                    start,
                                    e
                                );
                            }
                        }
                    }
                    current_batch.clear();
                }
            }

            // Commencer nouveau batch
            current_start_height = Some(height);
            current_batch.push((height, block.clone()));
        }

        // Uploader le dernier batch
        if !current_batch.is_empty() {
            let start = current_batch.first().unwrap().0;
            let blocks: Vec<_> = current_batch.iter().map(|(_, b)| b.clone()).collect();

            match self.upload_batch_with_retry(start, blocks).await {
                Ok(_) => {
                    for (h, _) in &current_batch {
                        to_remove.push(*h);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to upload retry batch starting at {}: {}", start, e);
                }
            }
        }

        // Retirer les blocs uploadés avec succès
        for height in to_remove {
            self.store.retry_queue.remove(&height);
        }

        self.metrics
            .record_retry_queue_size(self.store.retry_queue.len() as u64);

        // Sauvegarder la queue mise à jour
        log_error!(
            Self::save_on_disk::<GcsUploaderStore>(
                self.ctx
                    .data_directory
                    .join("gcs_uploader_store.bin")
                    .as_path(),
                &self.store,
            ),
            "Saving retry queue"
        )?;

        Ok(())
    }
}
