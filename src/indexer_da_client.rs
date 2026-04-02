use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use google_cloud_storage::client::Storage as GcsStorageClient;
use hyli_bus::modules::ModulePersistOutput;
use hyli_model::{
    BlockHeight, DataAvailabilityEvent, DataAvailabilityRequest, DataProposal, Hashed, LaneId,
    SignedBlock, StoredSignedBlock,
};
use hyli_modules::{
    bus::{BusClientSender, SharedMessageBus},
    module_bus_client, module_handle_messages,
    modules::Module,
    node_state::{
        module::{load_current_chain_timestamp, persist_current_chain_timestamp, NodeStateModule},
        NodeStateStore,
    },
    utils::da_codec::DataAvailabilityClient,
};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::{
    model::{DataEvent, MempoolStatusEvent},
    shared_storage::gcs::timestamp_to_folder_name,
    utils::conf::DataProposalDurabilityConf,
};

pub struct StoredDaIndexerClientCtx {
    pub data_directory: PathBuf,
    pub da_read_from: String,
    pub da_fallback_addresses: Vec<String>,
    pub timeout_client_secs: u64,
    pub gcs_conf: DataProposalDurabilityConf,
}

pub struct GcsEventIndexerClientCtx {
    pub data_directory: PathBuf,
    pub timeout_client_secs: u64,
    pub gcs_conf: DataProposalDurabilityConf,
    pub subscription: String,
}

module_bus_client! {
#[derive(Debug)]
struct StoredDaIndexerClientBus {
    sender(DataEvent),
    sender(MempoolStatusEvent),
}
}

pub struct StoredDaIndexerClient {
    bus: StoredDaIndexerClientBus,
    config: StoredDaIndexerClientCtx,
    da_client: Option<DataAvailabilityClient>,
    gcs_client: GcsStorageClient,
    current_block: BlockHeight,
    current_chain_timestamp: Option<String>,
    block_buffer: BTreeMap<BlockHeight, StoredSignedBlock>,
    deadline: Instant,
}

pub struct GcsEventIndexerClient {
    bus: StoredDaIndexerClientBus,
    config: GcsEventIndexerClientCtx,
    gcs_client: GcsStorageClient,
    http_client: HttpClient,
    access_token_credentials: AccessTokenCredentials,
    current_block: BlockHeight,
    current_chain_timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PubsubPullResponse {
    #[serde(default)]
    received_messages: Vec<PubsubReceivedMessage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PubsubReceivedMessage {
    ack_id: String,
    message: PubsubMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PubsubMessage {
    #[serde(default)]
    data: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PubsubPullRequest {
    max_messages: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PubsubAcknowledgeRequest {
    ack_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsObjectNotification {
    #[serde(default)]
    event_type: String,
    #[serde(default, alias = "name")]
    object_id: String,
}

impl Module for StoredDaIndexerClient {
    type Context = StoredDaIndexerClientCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let start_block_in_file = match NodeStateModule::load_from_disk::<BlockHeight>(
            &ctx.data_directory,
            "da_start_height.bin".as_ref(),
        )? {
            Some(height) => height,
            None => {
                warn!("Starting StoredDaIndexerClient from default block height.");
                BlockHeight(0)
            }
        };

        let bus = StoredDaIndexerClientBus::new_from_bus(bus.new_handle()).await;
        let gcs_client = GcsStorageClient::builder().build().await?;
        let current_chain_timestamp = load_current_chain_timestamp(&ctx.data_directory).ok();

        Ok(Self {
            bus,
            config: ctx,
            da_client: None,
            gcs_client,
            current_block: start_block_in_file,
            current_chain_timestamp,
            block_buffer: BTreeMap::new(),
            deadline: Instant::now(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        persist_indexer_progress(
            &self.config.data_directory,
            self.current_block,
            self.current_chain_timestamp.clone(),
        )
    }
}

impl Module for GcsEventIndexerClient {
    type Context = GcsEventIndexerClientCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let start_block_in_file = match NodeStateModule::load_from_disk::<BlockHeight>(
            &ctx.data_directory,
            "da_start_height.bin".as_ref(),
        )? {
            Some(height) => height,
            None => {
                warn!("Starting GcsEventIndexerClient from default block height.");
                BlockHeight(0)
            }
        };

        let bus = StoredDaIndexerClientBus::new_from_bus(bus.new_handle()).await;
        let gcs_client = GcsStorageClient::builder().build().await?;
        let access_token_credentials = Builder::default()
            .with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
            .build_access_token_credentials()?;

        Ok(Self {
            bus,
            current_block: start_block_in_file,
            current_chain_timestamp: load_current_chain_timestamp(&ctx.data_directory).ok(),
            config: ctx,
            gcs_client,
            http_client: HttpClient::builder().build()?,
            access_token_credentials,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        persist_indexer_progress(
            &self.config.data_directory,
            self.current_block,
            self.current_chain_timestamp.clone(),
        )
    }
}

impl StoredDaIndexerClient {
    async fn start(&mut self) -> Result<()> {
        self.connect().await?;

        info!(
            "Starting DA client for stored signed blocks at block {}",
            self.current_block
        );

        module_handle_messages! {
            on_self self,
            recv = self.recv_next() => {
                match recv? {
                    Some(DataAvailabilityEvent::StoredSignedBlock(block)) => {
                        self.handle_stored_signed_block(block).await?;
                    }
                    Some(DataAvailabilityEvent::MempoolStatusEvent(status)) => {
                        self.bus.send_waiting_if_full(status).await?;
                    }
                    Some(DataAvailabilityEvent::BlockNotFound(height)) => {
                        return Err(anyhow!("Stored block {height} was not found by DA server"));
                    }
                    Some(DataAvailabilityEvent::SignedBlock(_)) => {
                        debug!("Ignoring signed block on stored block client");
                    }
                    None => {
                        warn!("Stored DA stream closed. Reconnecting after sleeping 1s...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        self.connect().await?;
                    }
                }
            }
        };
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        let mut addresses = vec![self.config.da_read_from.clone()];
        addresses.extend(self.config.da_fallback_addresses.clone());

        for address in addresses {
            match DataAvailabilityClient::connect_with_opts(
                "stored_da_indexer",
                Some(1024 * 1024 * 1024),
                address.clone(),
            )
            .await
            {
                Ok(mut client) => {
                    client
                        .send(DataAvailabilityRequest::StreamStoredFromHeight(
                            self.current_block,
                        ))
                        .await?;
                    self.deadline =
                        Instant::now() + Duration::from_secs(self.config.timeout_client_secs);
                    self.da_client = Some(client);
                    return Ok(());
                }
                Err(err) => {
                    warn!("Failed to connect to DA server {}: {}", address, err);
                }
            }
        }

        Err(anyhow!("Failed to connect to any DA server"))
    }

    async fn recv_next(&mut self) -> Result<Option<DataAvailabilityEvent>> {
        let Some(client) = self.da_client.as_mut() else {
            return Ok(None);
        };

        match tokio::time::timeout(
            self.deadline.saturating_duration_since(Instant::now()),
            client.recv(),
        )
        .await
        {
            Ok(event) => {
                self.deadline =
                    Instant::now() + Duration::from_secs(self.config.timeout_client_secs);
                Ok(event)
            }
            Err(_) => {
                warn!("Stored DA client timed out. Reconnecting.");
                self.connect().await?;
                Ok(None)
            }
        }
    }

    async fn handle_stored_signed_block(&mut self, block: StoredSignedBlock) -> Result<()> {
        let block_height = block.height();

        match block_height.cmp(&self.current_block) {
            std::cmp::Ordering::Less => {
                warn!("Ignoring past stored block {}", block_height);
            }
            std::cmp::Ordering::Equal => {
                self.emit_reconstructed_block(block).await?;
                self.current_block = block_height + 1;
                self.process_buffered_blocks().await?;
            }
            std::cmp::Ordering::Greater => {
                for missing_height in self.current_block.0..block_height.0 {
                    self.request_specific_block(BlockHeight(missing_height))
                        .await?;
                }
                self.block_buffer.insert(block_height, block);
            }
        }

        Ok(())
    }

    async fn process_buffered_blocks(&mut self) -> Result<()> {
        while let Some(block) = self.block_buffer.remove(&self.current_block) {
            let height = block.height();
            self.emit_reconstructed_block(block).await?;
            self.current_block = height + 1;
        }
        Ok(())
    }

    async fn request_specific_block(&mut self, height: BlockHeight) -> Result<()> {
        let Some(client) = self.da_client.as_mut() else {
            return Err(anyhow!("DA client not initialized"));
        };
        client
            .send(DataAvailabilityRequest::StoredBlockRequest(height))
            .await?;
        Ok(())
    }

    async fn emit_reconstructed_block(&mut self, block: StoredSignedBlock) -> Result<()> {
        let block_height = block.height();
        let signed_block = self
            .reconstruct_signed_block(block)
            .await
            .with_context(|| format!("Reconstructing stored signed block {block_height}"))?;
        self.bus
            .send_waiting_if_full(DataEvent::OrderedSignedBlock(signed_block))
            .await
    }

    async fn reconstruct_signed_block(&mut self, block: StoredSignedBlock) -> Result<SignedBlock> {
        let current_chain_timestamp = self
            .resolve_current_chain_timestamp(&block)
            .await?
            .context("Current chain timestamp is required to fetch data proposals from GCS")?;

        let mut data_proposals = Vec::with_capacity(block.data_proposals.len());
        for (lane_id, hashes) in &block.data_proposals {
            let mut proposals = Vec::with_capacity(hashes.len());
            for dp_hash in hashes {
                proposals.push(
                    self.fetch_data_proposal(
                        block.height(),
                        &current_chain_timestamp,
                        lane_id,
                        dp_hash,
                    )
                    .await?,
                );
            }
            data_proposals.push((lane_id.clone(), proposals));
        }

        Ok(SignedBlock {
            data_proposals,
            consensus_proposal: block.consensus_proposal,
            certificate: block.certificate,
        })
    }

    async fn resolve_current_chain_timestamp(
        &mut self,
        block: &StoredSignedBlock,
    ) -> Result<Option<String>> {
        if let Some(current_chain_timestamp) = &self.current_chain_timestamp {
            return Ok(Some(current_chain_timestamp.clone()));
        }

        if block.height() != BlockHeight(0) {
            return Ok(None);
        }

        let current_chain_timestamp =
            timestamp_to_folder_name(block.consensus_proposal.timestamp.0)?;
        self.current_chain_timestamp = Some(current_chain_timestamp.clone());

        let mut store = NodeStateStore::default();
        store.current_chain_timestamp = Some(current_chain_timestamp.clone());
        let _ = persist_current_chain_timestamp(&self.config.data_directory, &store)?;

        Ok(Some(current_chain_timestamp))
    }

    async fn fetch_data_proposal(
        &self,
        block_height: BlockHeight,
        current_chain_timestamp: &str,
        lane_id: &LaneId,
        dp_hash: &hyli_model::DataProposalHash,
    ) -> Result<DataProposal> {
        let object_name = data_proposal_object_name(
            &self.config.gcs_conf.gcs_prefix,
            current_chain_timestamp,
            lane_id,
            dp_hash,
        );
        let mut reader = self
            .gcs_client
            .read_object(
                bucket_path(&self.config.gcs_conf.gcs_bucket),
                object_name.clone(),
            )
            .send()
            .await
            .with_context(|| {
                format!(
                    "Fetching data proposal for block {block_height} from GCS object {}/{} (lane {}, hash {})",
                    self.config.gcs_conf.gcs_bucket, object_name, lane_id, dp_hash
                )
            })?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await.transpose()? {
            bytes.extend_from_slice(&chunk);
        }
        let proposal = borsh::from_slice::<DataProposal>(&bytes).with_context(|| {
            format!(
                "Deserializing data proposal for block {block_height} from GCS object {}/{}",
                self.config.gcs_conf.gcs_bucket, object_name
            )
        })?;
        if proposal.hashed() != *dp_hash {
            return Err(anyhow!(
                "Fetched data proposal hash mismatch for block {} on lane {} from {}/{}",
                block_height,
                lane_id,
                self.config.gcs_conf.gcs_bucket,
                object_name
            ));
        }
        Ok(proposal)
    }
}

impl GcsEventIndexerClient {
    async fn start(&mut self) -> Result<()> {
        self.process_ready_blocks().await?;

        loop {
            let ack_ids = self.pull_notification_batch().await?;
            if !ack_ids.is_empty() {
                self.process_ready_blocks().await?;
                self.acknowledge(ack_ids).await?;
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn pull_notification_batch(&mut self) -> Result<Vec<String>> {
        let access_token = self
            .access_token_credentials
            .access_token()
            .await
            .context("Fetching Pub/Sub access token")?;
        let response = self
            .http_client
            .post(format!(
                "https://pubsub.googleapis.com/v1/{}:pull",
                self.config.subscription
            ))
            .bearer_auth(access_token.token)
            .json(&PubsubPullRequest { max_messages: 32 })
            .send()
            .await?
            .error_for_status()?
            .json::<PubsubPullResponse>()
            .await?;

        let mut ack_ids = Vec::with_capacity(response.received_messages.len());
        for received in response.received_messages {
            let Some((timestamp, _height)) = parse_notification_object_name(
                &self.config.gcs_conf.gcs_prefix,
                &received.message.data,
            )?
            else {
                ack_ids.push(received.ack_id);
                continue;
            };

            if self.current_chain_timestamp.is_none() {
                self.current_chain_timestamp = Some(timestamp.clone());
                persist_current_chain_timestamp_value(&self.config.data_directory, timestamp)?;
            }

            ack_ids.push(received.ack_id);
        }

        Ok(ack_ids)
    }

    async fn acknowledge(&self, ack_ids: Vec<String>) -> Result<()> {
        if ack_ids.is_empty() {
            return Ok(());
        }

        let access_token = self
            .access_token_credentials
            .access_token()
            .await
            .context("Fetching Pub/Sub access token for acknowledge")?;
        self.http_client
            .post(format!(
                "https://pubsub.googleapis.com/v1/{}:acknowledge",
                self.config.subscription
            ))
            .bearer_auth(access_token.token)
            .json(&PubsubAcknowledgeRequest { ack_ids })
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn process_ready_blocks(&mut self) -> Result<()> {
        loop {
            let Some(current_chain_timestamp) = self.current_chain_timestamp.clone() else {
                return Ok(());
            };

            let maybe_block = fetch_stored_signed_block_from_gcs(
                &self.gcs_client,
                &self.config.gcs_conf,
                &current_chain_timestamp,
                self.current_block,
            )
            .await?;

            let Some(block) = maybe_block else {
                return Ok(());
            };

            let signed_block = reconstruct_signed_block_from_gcs(
                &self.gcs_client,
                &self.config.gcs_conf,
                &self.config.data_directory,
                &mut self.current_chain_timestamp,
                block,
            )
            .await
            .with_context(|| {
                format!("Reconstructing stored signed block {}", self.current_block)
            })?;
            self.bus
                .send_waiting_if_full(DataEvent::OrderedSignedBlock(signed_block))
                .await?;
            self.current_block = self.current_block + 1;
        }
    }
}

fn persist_indexer_progress(
    data_directory: &std::path::Path,
    current_block: BlockHeight,
    current_chain_timestamp: Option<String>,
) -> Result<ModulePersistOutput> {
    let file = PathBuf::from("da_start_height.bin");
    let checksum = NodeStateModule::save_on_disk(data_directory, &file, &current_block)?;

    let mut persisted = vec![(data_directory.join(file), checksum)];
    if let Some(timestamp_folder) = current_chain_timestamp {
        let mut store = NodeStateStore::default();
        store.current_chain_timestamp = Some(timestamp_folder);
        if let Some(timestamp_file) = persist_current_chain_timestamp(data_directory, &store)? {
            persisted.push(timestamp_file);
        }
    }
    Ok(persisted)
}

fn persist_current_chain_timestamp_value(
    data_directory: &std::path::Path,
    current_chain_timestamp: String,
) -> Result<()> {
    let mut store = NodeStateStore::default();
    store.current_chain_timestamp = Some(current_chain_timestamp);
    let _ = persist_current_chain_timestamp(data_directory, &store)?;
    Ok(())
}

fn bucket_path(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_string()
    } else {
        format!("projects/_/buckets/{bucket}")
    }
}

fn data_proposal_object_name(
    gcs_prefix: &str,
    current_chain_timestamp: &str,
    lane_id: &LaneId,
    dp_hash: &hyli_model::DataProposalHash,
) -> String {
    format!(
        "{}/{}/data_proposals/{}/{}.bin",
        gcs_prefix, current_chain_timestamp, lane_id, dp_hash
    )
}

fn stored_signed_block_object_name(
    gcs_prefix: &str,
    current_chain_timestamp: &str,
    height: BlockHeight,
) -> String {
    format!(
        "{}/{}/stored_signed_blocks/block_{}.bin",
        gcs_prefix, current_chain_timestamp, height.0
    )
}

async fn fetch_stored_signed_block_from_gcs(
    gcs_client: &GcsStorageClient,
    gcs_conf: &DataProposalDurabilityConf,
    current_chain_timestamp: &str,
    height: BlockHeight,
) -> Result<Option<StoredSignedBlock>> {
    let object_name =
        stored_signed_block_object_name(&gcs_conf.gcs_prefix, current_chain_timestamp, height);
    let mut reader = match gcs_client
        .read_object(bucket_path(&gcs_conf.gcs_bucket), object_name.clone())
        .send()
        .await
    {
        Ok(reader) => reader,
        Err(err)
            if err.to_string().contains("404") || err.to_string().contains("No such object") =>
        {
            return Ok(None);
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "Fetching stored signed block {height} from GCS object {}/{}",
                    gcs_conf.gcs_bucket, object_name
                )
            });
        }
    };

    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        bytes.extend_from_slice(&chunk);
    }
    let block = borsh::from_slice::<StoredSignedBlock>(&bytes).with_context(|| {
        format!(
            "Deserializing stored signed block {height} from GCS object {}/{}",
            gcs_conf.gcs_bucket, object_name
        )
    })?;
    Ok(Some(block))
}

async fn reconstruct_signed_block_from_gcs(
    gcs_client: &GcsStorageClient,
    gcs_conf: &DataProposalDurabilityConf,
    data_directory: &std::path::Path,
    current_chain_timestamp: &mut Option<String>,
    block: StoredSignedBlock,
) -> Result<SignedBlock> {
    let current_chain_timestamp =
        resolve_current_chain_timestamp(data_directory, current_chain_timestamp, &block)?
            .context("Current chain timestamp is required to fetch data proposals from GCS")?;

    let mut data_proposals = Vec::with_capacity(block.data_proposals.len());
    for (lane_id, hashes) in &block.data_proposals {
        let mut proposals = Vec::with_capacity(hashes.len());
        for dp_hash in hashes {
            proposals.push(
                fetch_data_proposal_from_gcs(
                    gcs_client,
                    gcs_conf,
                    block.height(),
                    &current_chain_timestamp,
                    lane_id,
                    dp_hash,
                )
                .await?,
            );
        }
        data_proposals.push((lane_id.clone(), proposals));
    }

    Ok(SignedBlock {
        data_proposals,
        consensus_proposal: block.consensus_proposal,
        certificate: block.certificate,
    })
}

fn resolve_current_chain_timestamp(
    data_directory: &std::path::Path,
    current_chain_timestamp: &mut Option<String>,
    block: &StoredSignedBlock,
) -> Result<Option<String>> {
    if let Some(current_chain_timestamp) = current_chain_timestamp {
        return Ok(Some(current_chain_timestamp.clone()));
    }

    if block.height() != BlockHeight(0) {
        return Ok(None);
    }

    let resolved = timestamp_to_folder_name(block.consensus_proposal.timestamp.0)?;
    *current_chain_timestamp = Some(resolved.clone());
    persist_current_chain_timestamp_value(data_directory, resolved.clone())?;
    Ok(Some(resolved))
}

async fn fetch_data_proposal_from_gcs(
    gcs_client: &GcsStorageClient,
    gcs_conf: &DataProposalDurabilityConf,
    block_height: BlockHeight,
    current_chain_timestamp: &str,
    lane_id: &LaneId,
    dp_hash: &hyli_model::DataProposalHash,
) -> Result<DataProposal> {
    let object_name = data_proposal_object_name(
        &gcs_conf.gcs_prefix,
        current_chain_timestamp,
        lane_id,
        dp_hash,
    );
    let mut reader = gcs_client
        .read_object(bucket_path(&gcs_conf.gcs_bucket), object_name.clone())
        .send()
        .await
        .with_context(|| {
            format!(
                "Fetching data proposal for block {block_height} from GCS object {}/{} (lane {}, hash {})",
                gcs_conf.gcs_bucket, object_name, lane_id, dp_hash
            )
        })?;
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        bytes.extend_from_slice(&chunk);
    }
    let proposal = borsh::from_slice::<DataProposal>(&bytes).with_context(|| {
        format!(
            "Deserializing data proposal for block {block_height} from GCS object {}/{}",
            gcs_conf.gcs_bucket, object_name
        )
    })?;
    if proposal.hashed() != *dp_hash {
        return Err(anyhow!(
            "Fetched data proposal hash mismatch for block {} on lane {} from {}/{}",
            block_height,
            lane_id,
            gcs_conf.gcs_bucket,
            object_name
        ));
    }
    Ok(proposal)
}

fn parse_notification_object_name(
    gcs_prefix: &str,
    encoded_message: &str,
) -> Result<Option<(String, BlockHeight)>> {
    let message = BASE64.decode(encoded_message)?;
    let notification = serde_json::from_slice::<GcsObjectNotification>(&message)?;
    if !notification.event_type.is_empty() && notification.event_type != "OBJECT_FINALIZE" {
        return Ok(None);
    }

    let prefix = format!("{gcs_prefix}/");
    let suffix = "/stored_signed_blocks/block_";
    let object_id = notification.object_id;
    let Some(rest) = object_id.strip_prefix(&prefix) else {
        return Ok(None);
    };
    let Some((timestamp, block_suffix)) = rest.split_once(suffix) else {
        return Ok(None);
    };
    let Some(height) = block_suffix.strip_suffix(".bin") else {
        return Ok(None);
    };
    let Ok(height) = height.parse::<u64>() else {
        return Ok(None);
    };
    Ok(Some((timestamp.to_string(), BlockHeight(height))))
}

#[cfg(test)]
mod gcs_event_tests {
    use super::*;

    #[test]
    fn parse_notification_object_name_extracts_timestamp_and_height() -> Result<()> {
        let payload = serde_json::json!({
            "eventType": "OBJECT_FINALIZE",
            "objectId": "camelot/2026-04-02T13-11-49Z/stored_signed_blocks/block_42.bin"
        });
        let encoded = BASE64.encode(serde_json::to_vec(&payload)?);
        assert_eq!(
            parse_notification_object_name("camelot", &encoded)?,
            Some(("2026-04-02T13-11-49Z".to_string(), BlockHeight(42)))
        );
        Ok(())
    }

    #[test]
    fn parse_notification_object_name_ignores_non_finalize_events() -> Result<()> {
        let payload = serde_json::json!({
            "eventType": "OBJECT_DELETE",
            "objectId": "camelot/2026-04-02T13-11-49Z/stored_signed_blocks/block_42.bin"
        });
        let encoded = BASE64.encode(serde_json::to_vec(&payload)?);
        assert_eq!(parse_notification_object_name("camelot", &encoded)?, None);
        Ok(())
    }
}
