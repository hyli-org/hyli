use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use sdk::{BlockHeight, DataProposalHash, SignedBlock, TxHash};

// ============================================================================
// Audit Result Structure
// ============================================================================

#[derive(Debug, Clone)]
pub struct AuditResult {
    /// Ranges of blocks found in storage (start, end)
    pub covered_ranges: Vec<(BlockHeight, BlockHeight)>,
    /// Ranges of blocks missing from storage (start, end)
    pub missing_ranges: Vec<(BlockHeight, BlockHeight)>,
    /// Total number of blocks covered
    pub total_blocks_covered: u64,
    /// Total number of blocks missing
    pub total_blocks_missing: u64,
    /// Lowest block height found
    pub min_height: Option<BlockHeight>,
    /// Highest block height found
    pub max_height: Option<BlockHeight>,
}

impl AuditResult {
    pub fn new() -> Self {
        Self {
            covered_ranges: Vec::new(),
            missing_ranges: Vec::new(),
            total_blocks_covered: 0,
            total_blocks_missing: 0,
            min_height: None,
            max_height: None,
        }
    }

    /// Compute missing ranges from covered ranges
    pub fn compute_missing_ranges(&mut self) {
        if self.covered_ranges.is_empty() {
            return;
        }

        // Sort covered ranges by start height
        self.covered_ranges.sort_by_key(|(start, _)| start.0);

        // Find gaps between covered ranges
        for i in 0..self.covered_ranges.len() - 1 {
            let (_, current_end) = self.covered_ranges[i];
            let (next_start, _) = self.covered_ranges[i + 1];

            if next_start.0 > current_end.0 + 1 {
                let gap_start = BlockHeight(current_end.0 + 1);
                let gap_end = BlockHeight(next_start.0 - 1);
                let gap_size = gap_end.0 - gap_start.0 + 1;

                self.missing_ranges.push((gap_start, gap_end));
                self.total_blocks_missing += gap_size;
            }
        }
    }

    /// Print a formatted audit report
    pub fn print_report(&self) {
        println!("\n=== Storage Audit Report ===\n");

        if let (Some(min), Some(max)) = (self.min_height, self.max_height) {
            println!("Block Height Range: {} - {}", min.0, max.0);
        } else {
            println!("No blocks found in storage");
            return;
        }

        println!("Total Blocks Covered: {}", self.total_blocks_covered);
        println!("Total Blocks Missing: {}", self.total_blocks_missing);

        if !self.covered_ranges.is_empty() {
            println!("\nCovered Ranges ({}):", self.covered_ranges.len());
            for (start, end) in &self.covered_ranges {
                let count = end.0 - start.0 + 1;
                println!("  {} - {} ({} blocks)", start.0, end.0, count);
            }
        }

        if !self.missing_ranges.is_empty() {
            println!("\nMissing Ranges ({}):", self.missing_ranges.len());
            for (start, end) in &self.missing_ranges {
                let count = end.0 - start.0 + 1;
                println!("  {} - {} ({} blocks)", start.0, end.0, count);
            }
        } else {
            println!("\nNo missing ranges - storage is complete!");
        }

        println!();
    }
}

// ============================================================================
// Storage Backend Trait
// ============================================================================

#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Upload a batch of sequential blocks
    /// start_height: height of the first block
    /// blocks: list of blocks to upload
    /// Returns the number of bytes uploaded
    async fn upload_block_batch(
        &self,
        start_height: BlockHeight,
        blocks: Vec<SignedBlock>,
    ) -> Result<usize>;

    /// Upload an individual proof
    async fn upload_proof(
        &self,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
        proof: Vec<u8>,
    ) -> Result<usize>;

    /// Audit the storage to identify covered and missing block ranges
    async fn audit_storage(&self, prefix: &str) -> Result<AuditResult>;
}

// ============================================================================
// GCS Storage Backend
// ============================================================================

pub struct GcsStorageBackend {
    client: Storage,
    bucket: String,
    prefix: String,
}

impl GcsStorageBackend {
    pub async fn new(bucket: String, prefix: String) -> Result<Self> {
        let client = Storage::builder().build().await?;
        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    fn gcs_bucket_path(&self) -> String {
        if self.bucket.starts_with("projects/") {
            self.bucket.clone()
        } else {
            format!("projects/_/buckets/{}", self.bucket)
        }
    }
}

#[async_trait]
impl StorageBackend for GcsStorageBackend {
    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, blocks)))]
    async fn upload_block_batch(
        &self,
        start_height: BlockHeight,
        blocks: Vec<SignedBlock>,
    ) -> Result<usize> {
        if blocks.is_empty() {
            return Ok(0);
        }

        let end_height = start_height.0 + blocks.len() as u64 - 1;
        let object_name = format!(
            "{}/block_{}-{}.bin",
            self.prefix, start_height.0, end_height
        );

        // Serialize the entire batch
        let data = borsh::to_vec(&blocks)?;
        let data_len = data.len();

        self.client
            .write_object(
                self.gcs_bucket_path(),
                object_name.clone(),
                Bytes::from(data),
            )
            .set_if_generation_match(0_i64)
            .send_unbuffered()
            .await?;

        tracing::info!(
            "Successfully uploaded batch {} ({} blocks, {} bytes)",
            object_name,
            blocks.len(),
            data_len
        );

        Ok(data_len)
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, proof)))]
    async fn upload_proof(
        &self,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
        proof: Vec<u8>,
    ) -> Result<usize> {
        let object_name = format!(
            "{}/proofs/{}/{}.bin",
            self.prefix, parent_data_proposal_hash.0, tx_hash.0
        );
        let proof_len = proof.len();

        self.client
            .write_object(
                self.gcs_bucket_path(),
                object_name.clone(),
                Bytes::from(proof),
            )
            .set_if_generation_match(0_i64)
            .send_buffered()
            .await?;

        tracing::info!(
            "Successfully uploaded proof {} ({} bytes)",
            object_name,
            proof_len
        );
        Ok(proof_len)
    }

    async fn audit_storage(&self, _prefix: &str) -> Result<AuditResult> {
        // TODO: Implement GCS audit using the google-cloud-storage list objects API
        // For now, use the local storage backend for audit functionality
        Err(anyhow::anyhow!(
            "GCS audit is not yet implemented. Please use storage_backend='local' for audit functionality."
        ))
    }
}

// ============================================================================
// Local Storage Backend (for testing)
// ============================================================================

pub struct LocalStorageBackend {
    base_path: PathBuf,
}

impl LocalStorageBackend {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        // Create directories if necessary
        std::fs::create_dir_all(&base_path)?;
        std::fs::create_dir_all(base_path.join("batches"))?;
        std::fs::create_dir_all(base_path.join("proofs"))?;
        Ok(Self { base_path })
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, blocks)))]
    async fn upload_block_batch(
        &self,
        start_height: BlockHeight,
        blocks: Vec<SignedBlock>,
    ) -> Result<usize> {
        if blocks.is_empty() {
            return Ok(0);
        }

        let end_height = start_height.0 + blocks.len() as u64 - 1;
        let file_path = self
            .base_path
            .join("batches")
            .join(format!("block_{}-{}.bin", start_height.0, end_height));

        let data = borsh::to_vec(&blocks)?;
        let data_len = data.len();

        std::fs::write(&file_path, &data)?;

        tracing::info!(
            "Successfully wrote local batch {:?} ({} blocks, {} bytes)",
            file_path,
            blocks.len(),
            data_len
        );

        Ok(data_len)
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, proof)))]
    async fn upload_proof(
        &self,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
        proof: Vec<u8>,
    ) -> Result<usize> {
        let dir_path = self
            .base_path
            .join("proofs")
            .join(parent_data_proposal_hash.0);
        std::fs::create_dir_all(&dir_path)?;

        let file_path = dir_path.join(format!("{}.bin", tx_hash.0));
        let proof_len = proof.len();

        std::fs::write(&file_path, &proof)?;

        tracing::info!(
            "Successfully wrote local proof {:?} ({} bytes)",
            file_path,
            proof_len
        );
        Ok(proof_len)
    }

    async fn audit_storage(&self, _prefix: &str) -> Result<AuditResult> {
        let mut result = AuditResult::new();
        let batches_dir = self.base_path.join("batches");

        // Check if directory exists
        if !batches_dir.exists() {
            return Ok(result);
        }

        // Read all files in batches directory
        let entries = std::fs::read_dir(&batches_dir)?;

        for entry in entries {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Parse filename: block_START-END.bin
            if let Some(range_str) = filename_str.strip_prefix("block_") {
                if let Some(range_str) = range_str.strip_suffix(".bin") {
                    let parts: Vec<&str> = range_str.split('-').collect();
                    if parts.len() == 2 {
                        if let (Ok(start), Ok(end)) =
                            (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                        {
                            let start_height = BlockHeight(start);
                            let end_height = BlockHeight(end);
                            let blocks_count = end - start + 1;

                            result.covered_ranges.push((start_height, end_height));
                            result.total_blocks_covered += blocks_count;

                            // Update min/max
                            result.min_height = Some(match result.min_height {
                                Some(min) => BlockHeight(min.0.min(start)),
                                None => start_height,
                            });
                            result.max_height = Some(match result.max_height {
                                Some(max) => BlockHeight(max.0.max(end)),
                                None => end_height,
                            });
                        }
                    }
                }
            }
        }

        // Compute missing ranges
        result.compute_missing_ranges();

        Ok(result)
    }
}
