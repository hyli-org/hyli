use std::{future::Future, path::PathBuf, pin::Pin};

use anyhow::Result;
use hyli_model::LaneId;

use crate::{model::DataProposalHash, shared_storage::durability::DurabilityBackend};

#[derive(Clone)]
pub struct FileDurabilityBackend {
    root: PathBuf,
    prefix: String,
}

impl FileDurabilityBackend {
    pub fn new(root: PathBuf, prefix: String) -> Self {
        Self { root, prefix }
    }

    fn object_path(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> PathBuf {
        self.root
            .join(&self.prefix)
            .join("data_proposals")
            .join(lane_id.to_string())
            .join(format!("{dp_hash}.bin"))
    }

    fn write(&self, lane_id: LaneId, dp_hash: DataProposalHash, payload: Vec<u8>) -> Result<()> {
        let path = self.object_path(&lane_id, &dp_hash);
        if path.exists() {
            return Ok(());
        }

        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("missing parent directory for {}", path.display()))?;
        std::fs::create_dir_all(parent)?;

        let tmp_path = path.with_extension(format!("bin.tmp-{}", std::process::id()));
        std::fs::write(&tmp_path, payload)?;
        if path.exists() {
            let _ = std::fs::remove_file(&tmp_path);
            return Ok(());
        }
        std::fs::rename(&tmp_path, &path)?;
        Ok(())
    }
}

impl DurabilityBackend for FileDurabilityBackend {
    fn upload_data_proposal(
        &self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        payload: Vec<u8>,
        _current_chain_timestamp: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let backend = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || backend.write(lane_id, dp_hash, payload)).await?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DataProposal, Hashed};

    #[test_log::test(tokio::test)]
    async fn test_file_durability_backend_writes_payload() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let backend = FileDurabilityBackend::new(dir.path().to_path_buf(), "test-prefix".into());
        let lane_id = LaneId::default();
        let dp_hash = DataProposal::new_root(lane_id.clone(), vec![]).hashed();

        backend
            .upload_data_proposal(lane_id.clone(), dp_hash.clone(), b"payload".to_vec(), None)
            .await?;

        let path = backend.object_path(&lane_id, &dp_hash);
        assert_eq!(std::fs::read(path)?, b"payload");
        Ok(())
    }
}
