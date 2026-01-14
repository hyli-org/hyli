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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_block(height: u64) -> SignedBlock {
        use sdk::hyli_model_utils::TimestampMs;
        use sdk::{AggregateSignature, ConsensusProposal, ConsensusProposalHash, Cut};
        SignedBlock {
            data_proposals: vec![],
            consensus_proposal: ConsensusProposal {
                slot: height,
                parent_hash: ConsensusProposalHash(String::new()),
                cut: Cut::default(),
                staking_actions: vec![],
                timestamp: TimestampMs(0),
            },
            certificate: AggregateSignature::default(),
        }
    }

    #[tokio::test]
    async fn test_local_storage_backend_new() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf());

        assert!(backend.is_ok());

        // Verify directories were created
        assert!(temp_dir.path().join("batches").exists());
        assert!(temp_dir.path().join("proofs").exists());
    }

    #[tokio::test]
    async fn test_local_upload_block_batch_empty() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let result = backend.upload_block_batch(BlockHeight(0), vec![]).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_local_upload_block_batch_single() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let block = create_test_block(100);
        let result = backend
            .upload_block_batch(BlockHeight(100), vec![block.clone()])
            .await;

        assert!(result.is_ok());

        // Verify file was created
        let file_path = temp_dir.path().join("batches").join("block_100-100.bin");
        assert!(file_path.exists());

        // Verify content
        let data = std::fs::read(&file_path).unwrap();
        let deserialized: Vec<SignedBlock> = borsh::from_slice(&data).unwrap();
        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].height(), BlockHeight(100));
    }

    #[tokio::test]
    async fn test_local_upload_block_batch_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let blocks: Vec<SignedBlock> = (1000..1010).map(create_test_block).collect();

        let result = backend
            .upload_block_batch(BlockHeight(1000), blocks.clone())
            .await;

        assert!(result.is_ok());
        let bytes_written = result.unwrap();
        assert!(bytes_written > 0);

        // Verify file naming
        let file_path = temp_dir.path().join("batches").join("block_1000-1009.bin");
        assert!(file_path.exists());

        // Verify content
        let data = std::fs::read(&file_path).unwrap();
        let deserialized: Vec<SignedBlock> = borsh::from_slice(&data).unwrap();
        assert_eq!(deserialized.len(), 10);

        // Verify block heights
        for (i, block) in deserialized.iter().enumerate() {
            assert_eq!(block.height(), BlockHeight(1000 + i as u64));
        }
    }

    #[tokio::test]
    async fn test_local_upload_proof() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let tx_hash = TxHash("test_tx_hash".to_string());
        let parent_hash = DataProposalHash("parent_hash".to_string());
        let proof = vec![1, 2, 3, 4, 5];

        let result = backend
            .upload_proof(tx_hash.clone(), parent_hash.clone(), proof.clone())
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        // Verify file was created in correct directory structure
        let file_path = temp_dir
            .path()
            .join("proofs")
            .join(&parent_hash.0)
            .join(format!("{}.bin", tx_hash.0));
        assert!(file_path.exists());

        // Verify content
        let data = std::fs::read(&file_path).unwrap();
        assert_eq!(data, proof);
    }

    #[tokio::test]
    async fn test_local_audit_storage_empty() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let result = backend.audit_storage("test_prefix").await;

        assert!(result.is_ok());
        let audit = result.unwrap();
        assert_eq!(audit.covered_ranges.len(), 0);
        assert_eq!(audit.missing_ranges.len(), 0);
        assert_eq!(audit.total_blocks_covered, 0);
        assert_eq!(audit.total_blocks_missing, 0);
        assert!(audit.min_height.is_none());
        assert!(audit.max_height.is_none());
    }

    #[tokio::test]
    async fn test_local_audit_storage_single_batch() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        // Upload a batch
        let blocks: Vec<SignedBlock> = (100..110).map(create_test_block).collect();
        backend
            .upload_block_batch(BlockHeight(100), blocks)
            .await
            .unwrap();

        // Run audit
        let result = backend.audit_storage("test_prefix").await;

        assert!(result.is_ok());
        let audit = result.unwrap();
        assert_eq!(audit.covered_ranges.len(), 1);
        assert_eq!(
            audit.covered_ranges[0],
            (BlockHeight(100), BlockHeight(109))
        );
        assert_eq!(audit.total_blocks_covered, 10);
        assert_eq!(audit.missing_ranges.len(), 0);
        assert_eq!(audit.min_height, Some(BlockHeight(100)));
        assert_eq!(audit.max_height, Some(BlockHeight(109)));
    }

    #[tokio::test]
    async fn test_local_audit_storage_multiple_batches_continuous() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        // Upload multiple continuous batches
        for start in [0, 100, 200] {
            let blocks: Vec<SignedBlock> = (start..start + 100).map(create_test_block).collect();
            backend
                .upload_block_batch(BlockHeight(start), blocks)
                .await
                .unwrap();
        }

        // Run audit
        let result = backend.audit_storage("test_prefix").await;

        assert!(result.is_ok());
        let audit = result.unwrap();
        assert_eq!(audit.covered_ranges.len(), 3);
        assert_eq!(audit.total_blocks_covered, 300);
        assert_eq!(audit.missing_ranges.len(), 0); // No gaps - batches are continuous
        assert_eq!(audit.min_height, Some(BlockHeight(0)));
        assert_eq!(audit.max_height, Some(BlockHeight(299)));
    }

    #[tokio::test]
    async fn test_local_audit_storage_with_gaps() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        // Upload batches with intentional gaps
        let blocks1: Vec<SignedBlock> = (0..50).map(create_test_block).collect();
        backend
            .upload_block_batch(BlockHeight(0), blocks1)
            .await
            .unwrap();

        // Gap: 50-99 missing

        let blocks2: Vec<SignedBlock> = (100..200).map(create_test_block).collect();
        backend
            .upload_block_batch(BlockHeight(100), blocks2)
            .await
            .unwrap();

        // Gap: 200-299 missing

        let blocks3: Vec<SignedBlock> = (300..350).map(create_test_block).collect();
        backend
            .upload_block_batch(BlockHeight(300), blocks3)
            .await
            .unwrap();

        // Run audit
        let result = backend.audit_storage("test_prefix").await;

        assert!(result.is_ok());
        let audit = result.unwrap();

        // Verify covered ranges
        assert_eq!(audit.covered_ranges.len(), 3);
        assert_eq!(audit.total_blocks_covered, 200); // 50 + 100 + 50

        // Verify missing ranges
        assert_eq!(audit.missing_ranges.len(), 2);
        assert_eq!(audit.missing_ranges[0], (BlockHeight(50), BlockHeight(99)));
        assert_eq!(
            audit.missing_ranges[1],
            (BlockHeight(200), BlockHeight(299))
        );
        assert_eq!(audit.total_blocks_missing, 150); // 50 + 100
    }

    #[test]
    fn test_audit_result_compute_missing_ranges_empty() {
        let mut result = AuditResult::new();
        result.compute_missing_ranges();

        assert_eq!(result.missing_ranges.len(), 0);
        assert_eq!(result.total_blocks_missing, 0);
    }

    #[test]
    fn test_audit_result_compute_missing_ranges_no_gaps() {
        let mut result = AuditResult::new();
        result.covered_ranges = vec![
            (BlockHeight(0), BlockHeight(99)),
            (BlockHeight(100), BlockHeight(199)),
            (BlockHeight(200), BlockHeight(299)),
        ];

        result.compute_missing_ranges();

        assert_eq!(result.missing_ranges.len(), 0);
        assert_eq!(result.total_blocks_missing, 0);
    }

    #[test]
    fn test_audit_result_compute_missing_ranges_with_gaps() {
        let mut result = AuditResult::new();
        result.covered_ranges = vec![
            (BlockHeight(0), BlockHeight(49)),
            (BlockHeight(100), BlockHeight(149)),
            (BlockHeight(200), BlockHeight(249)),
        ];

        result.compute_missing_ranges();

        assert_eq!(result.missing_ranges.len(), 2);
        assert_eq!(result.missing_ranges[0], (BlockHeight(50), BlockHeight(99)));
        assert_eq!(
            result.missing_ranges[1],
            (BlockHeight(150), BlockHeight(199))
        );
        assert_eq!(result.total_blocks_missing, 100); // 50 + 50
    }

    #[test]
    fn test_audit_result_compute_missing_ranges_unsorted() {
        let mut result = AuditResult::new();
        // Add ranges out of order
        result.covered_ranges = vec![
            (BlockHeight(200), BlockHeight(249)),
            (BlockHeight(0), BlockHeight(49)),
            (BlockHeight(100), BlockHeight(149)),
        ];

        result.compute_missing_ranges();

        // Should sort first, then compute
        assert_eq!(result.missing_ranges.len(), 2);
        assert_eq!(result.missing_ranges[0], (BlockHeight(50), BlockHeight(99)));
        assert_eq!(
            result.missing_ranges[1],
            (BlockHeight(150), BlockHeight(199))
        );
    }

    #[tokio::test]
    async fn test_local_upload_multiple_proofs_same_parent() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let parent_hash = DataProposalHash("parent_hash".to_string());

        // Upload multiple proofs for the same parent
        for i in 0..5 {
            let tx_hash = TxHash(format!("tx_{}", i));
            let proof = vec![i as u8; 10];

            backend
                .upload_proof(tx_hash.clone(), parent_hash.clone(), proof.clone())
                .await
                .unwrap();
        }

        // Verify all proofs exist
        let parent_dir = temp_dir.path().join("proofs").join(&parent_hash.0);
        assert!(parent_dir.exists());

        for i in 0..5 {
            let file_path = parent_dir.join(format!("tx_{}.bin", i));
            assert!(file_path.exists());

            let data = std::fs::read(&file_path).unwrap();
            assert_eq!(data, vec![i as u8; 10]);
        }
    }

    #[tokio::test]
    async fn test_local_overwrite_protection() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().to_path_buf()).unwrap();

        let blocks1: Vec<SignedBlock> = (100..110).map(create_test_block).collect();
        backend
            .upload_block_batch(BlockHeight(100), blocks1)
            .await
            .unwrap();

        // Try to upload again (would overwrite)
        let blocks2: Vec<SignedBlock> = (100..110).map(create_test_block).collect();
        let result = backend.upload_block_batch(BlockHeight(100), blocks2).await;

        // Should succeed (overwrites in local mode, but GCS would reject with if_generation_match)
        assert!(result.is_ok());
    }
}
