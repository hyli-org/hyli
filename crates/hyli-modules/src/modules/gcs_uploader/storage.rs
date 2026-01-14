use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use sdk::{BlockHeight, DataProposalHash, SignedBlock, TxHash};

// ============================================================================
// Storage Backend Trait
// ============================================================================

#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Upload un batch de blocs séquentiels
    /// start_height: hauteur du premier bloc
    /// blocks: liste de blocs à uploader
    /// Retourne le nombre de bytes uploadés
    async fn upload_block_batch(
        &self,
        start_height: BlockHeight,
        blocks: Vec<SignedBlock>,
    ) -> Result<usize>;

    /// Upload un proof individuel
    async fn upload_proof(
        &self,
        tx_hash: TxHash,
        parent_data_proposal_hash: DataProposalHash,
        proof: Vec<u8>,
    ) -> Result<usize>;
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

        // Sérialiser le batch entier
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
}

// ============================================================================
// Local Storage Backend (for testing)
// ============================================================================

pub struct LocalStorageBackend {
    base_path: PathBuf,
}

impl LocalStorageBackend {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        // Créer les répertoires si nécessaire
        std::fs::create_dir_all(&base_path)?;
        std::fs::create_dir_all(base_path.join("batches"))?;
        std::fs::create_dir_all(base_path.join("proofs"))?;
        Ok(Self { base_path })
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
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
}
