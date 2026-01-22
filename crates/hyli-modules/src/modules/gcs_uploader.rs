use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::Bytes;
use google_cloud_storage::client::{Storage, StorageControl};
use sdk::{DataEvent, DataProposalHash, SignedBlock, TxHash};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::bus::{BusMessage, SharedMessageBus};
use crate::modules::{gcs_uploader_metrics::GcsUploaderMetrics, Module};
use crate::{log_error, module_bus_client, module_handle_messages};

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

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GCSConf {
    // GCS uploader options
    pub gcs_bucket: String,
    pub gcs_prefix: String,

    pub save_proofs: bool,
    pub save_blocks: bool,

    pub start_block: u64,
    pub max_concurrent_uploads: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GcsUploaderCtx {
    pub gcs_config: GCSConf,
    pub data_directory: PathBuf,
}

pub struct GcsUploader {
    ctx: GcsUploaderCtx,
    bus: GcsUploaderBusClient,
    gcs_client: Storage,
    last_uploaded_height: u64,
    metrics: GcsUploaderMetrics,
    upload_tasks: JoinSet<(u64, Result<(), String>)>,
    upload_semaphore: Arc<Semaphore>,
}

impl Module for GcsUploader {
    type Context = GcsUploaderCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = GcsUploaderBusClient::new_from_bus(bus.new_handle()).await;
        let gcs_client = Storage::builder().build().await?;

        // If start_block is configured, use it to override (takes precedence)
        let last_uploaded_height = if ctx.gcs_config.start_block > 0 {
            info!(
                "Using configured start_block: {} (overrides GCS query)",
                ctx.gcs_config.start_block
            );
            ctx.gcs_config.start_block.saturating_sub(1)
        } else {
            info!("Querying GCS to find last uploaded block...");

            let gcs_control = StorageControl::builder().build().await?;
            let bucket_path = Self::gcs_bucket_path(&ctx.gcs_config.gcs_bucket);
            match Self::find_last_uploaded_block(
                &gcs_control,
                &bucket_path,
                &ctx.gcs_config.gcs_prefix,
            )
            .await
            {
                Ok(Some(last_height)) => {
                    info!("Found last uploaded block in GCS: {}", last_height);
                    last_height
                }
                Ok(None) => {
                    info!("No blocks found in GCS, starting from beginning");
                    0
                }
                Err(e) => {
                    warn!("Failed to query GCS for last block: {}.", e);
                    bail!(e);
                }
            }
        };

        // Initialize metrics
        let metrics = GcsUploaderMetrics::global("hyle-node".to_string(), "gcs_uploader");
        metrics.record_success(last_uploaded_height);

        // Initialize semaphore with configured max (default to 100 if 0)
        let max_concurrent = if ctx.gcs_config.max_concurrent_uploads == 0 {
            100
        } else {
            ctx.gcs_config.max_concurrent_uploads
        };

        Ok(GcsUploader {
            ctx,
            bus,
            gcs_client,
            last_uploaded_height,
            metrics,
            upload_tasks: JoinSet::new(),
            upload_semaphore: Arc::new(Semaphore::new(max_concurrent)),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }
}

impl GcsUploader {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                // FIXME: this implies that every SignedBlock will be in GCSUploader's Bus channel
                if !self.ctx.gcs_config.save_blocks {
                    debug!("Skipping block upload as save_blocks is disabled");
                    return Ok(());
                }
                self.handle_data_availability_event(event).await?;
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
                        _ = log_error!(
                            self.upload_proof(proof, tx_hash, parent_data_proposal_hash).await,
                            "Uploading proof to GCS"
                        );
                    }
                }
            },
            Some(result) = self.upload_tasks.join_next() => {
                self.handle_upload_result(result);
            }
        };
        Ok(())
    }

    async fn handle_data_availability_event(&mut self, event: DataEvent) -> Result<()> {
        let DataEvent::OrderedSignedBlock(block) = event;
        let block_height = block.height().0;

        if block_height <= self.last_uploaded_height {
            debug!(
                "Block {} already uploaded (last uploaded: {}), skipping",
                block_height, self.last_uploaded_height
            );
            return Ok(());
        }

        self.upload_block_parallel(block_height, block);

        self.last_uploaded_height = block_height;

        Ok(())
    }

    fn upload_block_parallel(&mut self, block_height: u64, block: SignedBlock) {
        let gcs_client = self.gcs_client.clone();
        let bucket_path = Self::gcs_bucket_path(&self.ctx.gcs_config.gcs_bucket);
        let prefix = self.ctx.gcs_config.gcs_prefix.clone();
        let semaphore = self.upload_semaphore.clone();

        // Serialize block before spawning to avoid holding SignedBlock across await
        let data = borsh::to_vec(&block).expect("Failed to serialize SignedBlock");

        self.upload_tasks.spawn(async move {
            // Acquire permit - this will wait if at capacity
            let _permit = semaphore.acquire().await.expect("Semaphore closed");

            let object_name = format!("{prefix}/block_{block_height}.bin");

            match gcs_client
                .write_object(bucket_path, object_name.clone(), Bytes::from(data))
                .set_if_generation_match(0_i64)
                .send_unbuffered()
                .await
            {
                Ok(_) => (block_height, Ok(())),
                Err(e) => (block_height, Err(e.to_string())),
            }
            // _permit is dropped here, releasing the semaphore slot
        });
    }

    fn handle_upload_result(
        &self,
        result: Result<(u64, Result<(), String>), tokio::task::JoinError>,
    ) {
        match result {
            Ok((height, Ok(()))) => {
                info!("Successfully uploaded block {} to GCS bucket", height);
                self.metrics.record_success(height);
            }
            Ok((height, Err(e))) => {
                warn!("Upload task for block {} failed: {}", height, e);
                self.metrics.record_failure();
            }
            Err(e) => {
                warn!("Upload task panicked: {}", e);
                self.metrics.record_failure();
            }
        }
    }

    pub async fn upload_proof(
        &self,
        proof: Vec<u8>,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
    ) -> Result<()> {
        // Upload to GCS if client is configured
        let prefix = &self.ctx.gcs_config.gcs_prefix;
        let object_name = format!(
            "{}/proofs/{}/{}.bin",
            prefix, parent_data_proposal_hash.0, tx_hash.0
        );

        match self
            .gcs_client
            .write_object(
                Self::gcs_bucket_path(&self.ctx.gcs_config.gcs_bucket),
                object_name.clone(),
                Bytes::from(proof),
            )
            .set_if_generation_match(0_i64)
            .send_buffered()
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Successfully uploaded proof {} to GCS bucket {}",
                    tx_hash,
                    self.ctx.gcs_config.gcs_bucket
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to upload proof {} to GCS bucket {}: {}",
                    tx_hash,
                    self.ctx.gcs_config.gcs_bucket,
                    e
                );
            }
        }
        Ok(())
    }

    fn gcs_bucket_path(bucket: &str) -> String {
        if bucket.starts_with("projects/") {
            bucket.to_string()
        } else {
            format!("projects/_/buckets/{bucket}")
        }
    }

    async fn find_last_uploaded_block(
        gcs_control: &StorageControl,
        bucket_path: &str,
        prefix: &str,
    ) -> Result<Option<u64>> {
        use google_cloud_storage::model::ListObjectsRequest;

        // List objects with the block prefix
        let block_prefix = format!("{}/block_", prefix);
        let mut heights: Vec<u64> = Vec::new();
        let mut page_token: Option<String> = None;

        // Paginate through all results
        loop {
            let mut request = ListObjectsRequest::new()
                .set_parent(bucket_path)
                .set_prefix(&block_prefix);

            if let Some(token) = &page_token {
                request = request.set_page_token(token);
            }

            let response = gcs_control
                .list_objects()
                .with_request(request)
                .send()
                .await?;

            // Parse block heights from object names
            for object in response.objects {
                // Object name format: "{prefix}/block_{height}.bin"
                let object_name: String = object.name;
                if let Some(name) = object_name.as_str().strip_prefix(&block_prefix) {
                    if let Some(height_str) = name.strip_suffix(".bin") {
                        if let Ok(height) = height_str.parse::<u64>() {
                            heights.push(height);
                        }
                    }
                }
            }

            // Check if there are more pages
            if response.next_page_token.is_empty() {
                break;
            }

            page_token = Some(response.next_page_token);
        }

        if heights.is_empty() {
            return Ok(None);
        }

        heights.sort_unstable();
        heights.dedup();

        let min_height = heights[0];
        let max_height = *heights.last().expect("heights is non-empty");

        let mut gaps: Vec<(u64, u64)> = Vec::new();
        let mut prev = min_height;
        for &height in heights.iter().skip(1) {
            if height > prev.saturating_add(1) {
                gaps.push((prev.saturating_add(1), height.saturating_sub(1)));
            }
            prev = height;
        }

        if gaps.is_empty() {
            return Ok(Some(max_height));
        }

        for (start, end) in &gaps {
            if start == end {
                warn!(
                    "Detected missing block {} in GCS (between uploaded blocks)",
                    start
                );
            } else {
                warn!(
                    "Detected missing blocks {}..{} in GCS (between uploaded blocks)",
                    start, end
                );
            }
        }

        let first_missing = gaps[0].0;
        warn!(
            "Detected {} missing block range(s) in GCS. Will resume uploads from block {}.",
            gaps.len(),
            first_missing
        );

        Ok(Some(first_missing.saturating_sub(1)))
    }
}
