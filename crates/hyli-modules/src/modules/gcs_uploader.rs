use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use chrono;
use google_cloud_storage::client::{Storage, StorageControl};
use sdk::{BlockHeight, DataEvent, DataProposalHash, SignedBlock, TxHash};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

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

    pub max_concurrent_uploads: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GcsUploaderCtx {
    pub gcs_config: GCSConf,
    pub data_directory: PathBuf,
    pub node_name: String,
    pub last_uploaded_height: BlockHeight,
    pub genesis_timestamp_folder: String,
}

pub struct GcsUploader {
    ctx: GcsUploaderCtx,
    bus: GcsUploaderBusClient,
    gcs_client: Storage,
    metrics: GcsUploaderMetrics,
    upload_tasks: JoinSet<(BlockHeight, Result<(), google_cloud_storage::Error>)>,
    upload_semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct GenesisTimestampStore {
    timestamp_folder: String,
}

#[derive(Debug, Clone)]
pub struct GcsUploadStart {
    pub last_uploaded_height: BlockHeight,
    pub genesis_timestamp_folder: String,
    pub start_height: BlockHeight,
}

const GENESIS_TIMESTAMP_FILE: &str = "gcs_genesis_timestamp.bin";

impl Module for GcsUploader {
    type Context = GcsUploaderCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = GcsUploaderBusClient::new_from_bus(bus.new_handle()).await;
        let gcs_client = Storage::builder().build().await?;

        // Initialize metrics
        let metrics = GcsUploaderMetrics::global(ctx.node_name.clone(), "gcs_uploader");
        metrics.record_success(ctx.last_uploaded_height);

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
                // Drain any other completed tasks while we're here
                self.drain_completed_tasks();
            }
        };
        Ok(())
    }

    async fn handle_data_availability_event(&mut self, event: DataEvent) -> Result<()> {
        let DataEvent::OrderedSignedBlock(block) = event;
        let block_height = block.height();
        let is_genesis = block_height.0 == 0;

        // Check if this is genesis block (height 0)
        if is_genesis {
            let timestamp_ms = block.consensus_proposal.timestamp.0;
            let timestamp_folder = Self::timestamp_to_folder_name(timestamp_ms);

            info!(
                "Genesis block detected (height 0) with timestamp {} ms, using folder: {}",
                timestamp_ms, timestamp_folder
            );

            self.ctx.genesis_timestamp_folder = timestamp_folder;

            if let Err(e) = Self::save_genesis_timestamp_folder(
                &self.ctx.data_directory,
                &self.ctx.genesis_timestamp_folder,
            ) {
                warn!("Failed to persist genesis timestamp folder: {}", e);
            }

            // Check that no blocks have been uploaded yet for this timestamp
            if let Some(last_uploaded) = Self::find_last_uploaded_block_for_timestamp(
                &Self::gcs_bucket_path(&self.ctx.gcs_config.gcs_bucket),
                &self.ctx.gcs_config.gcs_prefix,
                &self.ctx.genesis_timestamp_folder,
            )
            .await?
            {
                if last_uploaded > 0 {
                    info!(
                        "Blocks for timestamp folder {} already exists, skipping uploads; will resume at {}",
                        self.ctx.genesis_timestamp_folder,
                        last_uploaded + 1
                    );
                    self.ctx.last_uploaded_height = BlockHeight(last_uploaded);
                    return Ok(());
                }
            }
        }

        if block_height <= self.ctx.last_uploaded_height {
            debug!(
                "Block {} already uploaded (last uploaded: {}), skipping",
                block_height, self.ctx.last_uploaded_height
            );
            return Ok(());
        }

        self.upload_block_parallel(block_height, block);

        self.ctx.last_uploaded_height = block_height;

        Ok(())
    }

    fn upload_block_parallel(&mut self, block_height: BlockHeight, block: SignedBlock) {
        let gcs_client = self.gcs_client.clone();
        let bucket_path = Self::gcs_bucket_path(&self.ctx.gcs_config.gcs_bucket);
        let prefix = self.ctx.gcs_config.gcs_prefix.clone();
        let semaphore = self.upload_semaphore.clone();
        let timestamp_folder = self.ctx.genesis_timestamp_folder.clone();

        self.upload_tasks.spawn(async move {
            // Acquire permit - this will wait if at capacity
            let _permit = semaphore.acquire().await.expect("Semaphore closed");

            let data = borsh::to_vec(&block).expect("Failed to serialize SignedBlock");

            // Build object name with timestamp folder if available
            let object_name = format!("{}/{}/block_{}.bin", prefix, timestamp_folder, block_height);

            let now = std::time::Instant::now();

            match gcs_client
                .write_object(bucket_path, object_name.clone(), Bytes::from(data))
                .set_if_generation_match(0_i64)
                .send_unbuffered()
                .await
            {
                Ok(_) => {
                    let elapsed = now.elapsed();
                    if block_height.0.is_multiple_of(1000) {
                        info!(
                            "Successfully uploaded block {} to GCS in {:.2?}",
                            block_height, elapsed
                        );
                    } else {
                        debug!(
                            "Successfully uploaded block {} to GCS in {:.2?}",
                            block_height, elapsed
                        );
                    }

                    (block_height, Ok(()))
                }
                Err(e) => (block_height, Err(e)),
            }
            // _permit is dropped here, releasing the semaphore slot
        });

        // Drain completed tasks to prevent memory buildup
        self.drain_completed_tasks();
    }

    fn handle_upload_result(
        &self,
        result: Result<
            (BlockHeight, Result<(), google_cloud_storage::Error>),
            tokio::task::JoinError,
        >,
    ) {
        match result {
            Ok((height, Ok(()))) => {
                self.metrics.record_success(height);
            }
            Ok((height, Err(e))) => {
                error!("Upload task for block {} failed: {:#}", height, e);
                self.metrics.record_failure();
            }
            Err(e) => {
                error!("Upload task panicked: {}", e);
                self.metrics.record_failure();
            }
        }
    }

    fn drain_completed_tasks(&mut self) {
        // Drain all completed tasks without blocking to prevent memory buildup
        while let Some(result) = self.upload_tasks.try_join_next() {
            self.handle_upload_result(result);
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
            "{}/{}/proofs/{}/{}.bin",
            prefix, self.ctx.genesis_timestamp_folder, parent_data_proposal_hash, tx_hash
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

    pub async fn get_last_uploaded_block(
        conf: &GCSConf,
        data_directory: &Path,
    ) -> Result<GcsUploadStart> {
        let GCSConf {
            gcs_prefix,
            gcs_bucket,
            ..
        } = conf;
        let Some(genesis_timestamp_folder) = Self::load_genesis_timestamp_folder(data_directory)?
        else {
            info!("No genesis timestamp on disk, starting from block 0");
            return Ok(GcsUploadStart {
                last_uploaded_height: BlockHeight(0),
                genesis_timestamp_folder: "none".to_string(),
                start_height: BlockHeight(0),
            });
        };

        let bucket_path = Self::gcs_bucket_path(gcs_bucket);
        match Self::find_last_uploaded_block_for_timestamp(
            &bucket_path,
            gcs_prefix,
            &genesis_timestamp_folder,
        )
        .await
        {
            Ok(Some(last_height)) => {
                info!("Found last uploaded block in GCS: {}", last_height);
                let last_uploaded_height = BlockHeight(last_height);
                Ok(GcsUploadStart {
                    last_uploaded_height,
                    genesis_timestamp_folder,
                    start_height: last_uploaded_height + 1,
                })
            }
            Ok(None) => {
                info!(
                    "No blocks found in GCS for genesis timestamp {}, starting from block 0",
                    genesis_timestamp_folder
                );
                Ok(GcsUploadStart {
                    last_uploaded_height: BlockHeight(0),
                    genesis_timestamp_folder,
                    start_height: BlockHeight(0),
                })
            }
            Err(e) => {
                warn!("Failed to query GCS for last block: {}.", e);
                bail!(e);
            }
        }
    }

    async fn find_last_uploaded_block_for_timestamp(
        bucket_path: &str,
        prefix: &str,
        timestamp_folder: &str,
    ) -> Result<Option<u64>> {
        let gcs_control = StorageControl::builder().build().await?;
        let block_prefix = format!("{}/{}/block_", prefix, timestamp_folder);
        Self::find_last_uploaded_block_with_prefix(&gcs_control, bucket_path, &block_prefix).await
    }

    async fn find_last_uploaded_block_with_prefix(
        gcs_control: &StorageControl,
        bucket_path: &str,
        block_prefix: &str,
    ) -> Result<Option<u64>> {
        use google_cloud_storage::model::ListObjectsRequest;

        let mut heights: Vec<u64> = Vec::new();
        let mut page_token: Option<String> = None;

        // Paginate through all results
        loop {
            let mut request = ListObjectsRequest::new()
                .set_parent(bucket_path)
                .set_prefix(block_prefix);

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
                // Object name format: "{prefix}/{timestamp}/block_{height}.bin"
                let object_name: String = object.name;
                if let Some(name) = object_name.as_str().strip_prefix(block_prefix) {
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

    fn timestamp_to_folder_name(timestamp_ms: u128) -> String {
        let secs = (timestamp_ms / 1000) as i64;
        let datetime =
            chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).expect("Invalid timestamp");
        datetime.format("%Y-%m-%dT%H-%M-%SZ").to_string()
    }

    fn load_genesis_timestamp_folder(data_directory: &Path) -> Result<Option<String>> {
        let file = PathBuf::from(GENESIS_TIMESTAMP_FILE);
        let full_path = data_directory.join(&file);
        let mut handle = match File::open(&full_path) {
            Ok(handle) => handle,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).context("Opening genesis timestamp file"),
        };

        let store: GenesisTimestampStore =
            borsh::from_reader(&mut handle).context("Deserializing genesis timestamp file")?;

        // Validate the loaded timestamp_folder
        chrono::NaiveDateTime::parse_from_str(&store.timestamp_folder, "%Y-%m-%dT%H-%M-%SZ")
            .context("Parsing genesis timestamp")?;

        Ok(Some(store.timestamp_folder))
    }

    fn save_genesis_timestamp_folder(data_directory: &Path, timestamp_folder: &str) -> Result<u32> {
        let file = PathBuf::from(GENESIS_TIMESTAMP_FILE);
        let store = GenesisTimestampStore {
            timestamp_folder: timestamp_folder.to_string(),
        };
        Self::save_on_disk(data_directory, &file, &store)
    }
}
