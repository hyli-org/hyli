#![allow(
    clippy::expect_used,
    reason = "Local DA replay is debug-only and should fail fast on invalid input"
)]

use std::{
    future::Future,
    path::{Path, PathBuf},
};

use anyhow::Result;
use hyli_bus::modules::{ModulePersistOutput, ModulesHandler};
use hyli_model::{BlockHeight, SignedBlock};
use hyli_modules::{
    bus::SharedMessageBus,
    modules::{
        block_processor::{BlockProcessor, BusOnlyProcessor},
        da_listener::{DAListenerConf, SignedDAListener},
        Module,
    },
    node_state::module::NodeStateModule,
};
use tokio::task::yield_now;
use tracing::{debug, info, warn};

use crate::mempool::LanesStorage;

pub struct LocalDaReplayer {
    config: DAListenerConf<BusOnlyProcessor>,
    processor: BusOnlyProcessor,
    current_block: BlockHeight,
}

impl LocalDaReplayer {
    pub async fn build_bus_only_source(
        handler: &mut ModulesHandler,
        config: DAListenerConf<BusOnlyProcessor>,
    ) -> Result<()> {
        if Self::is_local_source(&config.da_read_from) {
            handler.build_module::<LocalDaReplayer>(config).await
        } else {
            handler
                .build_module::<SignedDAListener<BusOnlyProcessor>>(config)
                .await
        }
    }

    pub fn is_local_source(source: &str) -> bool {
        source.starts_with("folder:") || source.starts_with("da:")
    }

    fn load_start_block(data_directory: &Path) -> Result<BlockHeight> {
        Ok(
            match NodeStateModule::load_from_disk::<BlockHeight>(
                data_directory,
                "da_start_height.bin".as_ref(),
            )? {
                Some(height) => height,
                None => {
                    warn!("Starting LocalDaReplayer from default block height.");
                    BlockHeight(0)
                }
            },
        )
    }

    pub async fn replay_from_source<F, Fut>(source: &str, mut on_block: F) -> Result<bool>
    where
        F: FnMut(SignedBlock) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if let Some(folder) = source.strip_prefix("folder:") {
            Self::replay_from_folder(folder, &mut on_block).await?;
            return Ok(true);
        }

        if let Some(folder) = source.strip_prefix("da:") {
            Self::replay_from_da_storage(folder, &mut on_block).await?;
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn replay_listener_config<F, Fut>(
        config: &DAListenerConf<BusOnlyProcessor>,
        on_block: F,
    ) -> Result<bool>
    where
        F: FnMut(SignedBlock) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        Self::replay_from_source(&config.da_read_from, on_block).await
    }

    async fn replay_from_folder<F, Fut>(folder: &str, on_block: &mut F) -> Result<()>
    where
        F: FnMut(SignedBlock) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        info!("Reading blocks from folder {folder}");

        let mut blocks = vec![];
        let mut entries = std::fs::read_dir(folder)
            .expect("Local DA replay folder must exist and be readable")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("Local DA replay folder entries must be readable");
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            if !is_bin_file(&path) {
                yield_now().await;
                continue;
            }

            let bytes = std::fs::read(&path).expect("Local DA replay block file must be readable");
            let (block, tx_count) = borsh::from_slice::<(SignedBlock, usize)>(&bytes)
                .expect("Local DA replay block file must contain a serialized SignedBlock");
            blocks.push((block, tx_count));

            yield_now().await;
        }

        blocks.sort_by_key(|(block, _)| block.consensus_proposal.slot);

        info!("Got {} blocks from folder. Processing...", blocks.len());
        for (block, _) in blocks {
            on_block(block).await?;
        }

        Ok(())
    }

    async fn replay_from_da_storage<F, Fut>(folder: &str, on_block: &mut F) -> Result<()>
    where
        F: FnMut(SignedBlock) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        info!("Reading blocks from DA {folder}");

        let lanes = LanesStorage::new(&PathBuf::from(folder), Default::default())?;
        let block_hashes = lanes
            .block_range(BlockHeight(0), BlockHeight(u64::MAX))
            .collect::<Result<Vec<_>>>()?;

        for block_hash in block_hashes {
            let block = lanes
                .load_full_block(&block_hash)?
                .expect("DA replay storage must contain every block referenced by its index");
            on_block(block).await?;
        }

        Ok(())
    }

    async fn replay_folder_into_processor(&mut self, folder: &str) -> Result<()> {
        info!("Reading blocks from folder {folder}");

        let mut blocks = vec![];
        let mut entries = std::fs::read_dir(folder)
            .expect("Local DA replay folder must exist and be readable")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("Local DA replay folder entries must be readable");
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            if !is_bin_file(&path) {
                yield_now().await;
                continue;
            }

            let bytes = std::fs::read(&path).expect("Local DA replay block file must be readable");
            let (block, tx_count) = borsh::from_slice::<(SignedBlock, usize)>(&bytes)
                .expect("Local DA replay block file must contain a serialized SignedBlock");
            blocks.push((block, tx_count));

            yield_now().await;
        }

        blocks.sort_by_key(|(block, _)| block.consensus_proposal.slot);

        info!("Got {} blocks from folder. Processing...", blocks.len());
        for (block, _) in blocks {
            self.process_replayed_block(block).await?;
        }

        Ok(())
    }

    async fn replay_da_storage_into_processor(&mut self, folder: &str) -> Result<()> {
        info!("Reading blocks from DA {folder}");

        let lanes = LanesStorage::new(&PathBuf::from(folder), Default::default())?;
        let block_hashes = lanes
            .block_range(BlockHeight(0), BlockHeight(u64::MAX))
            .collect::<Result<Vec<_>>>()?;

        for block_hash in block_hashes {
            let block = lanes
                .load_full_block(&block_hash)?
                .expect("DA replay storage must contain every block referenced by its index");
            self.process_replayed_block(block).await?;
        }

        Ok(())
    }

    async fn process_replayed_block(&mut self, block: SignedBlock) -> Result<()> {
        let block_height = block.height();
        if block_height < self.current_block {
            return Ok(());
        }

        self.processor.process_block(block).await?;
        self.current_block = block_height + 1;
        Ok(())
    }
}

impl Module for LocalDaReplayer {
    type Context = DAListenerConf<BusOnlyProcessor>;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let start_block_in_file = Self::load_start_block(&ctx.data_directory)?;
        debug!(
            "Building LocalDaReplayer with start block from file: {:?}",
            start_block_in_file
        );

        let current_block = ctx
            .start_block
            .or(Some(start_block_in_file))
            .unwrap_or_default();
        info!(
            "LocalDaReplayer current block height set to: {}",
            current_block
        );

        let processor = BusOnlyProcessor::build(
            bus.new_handle(),
            &ctx.processor_config,
            ctx.data_directory.clone(),
        )
        .await?;

        Ok(Self {
            config: ctx,
            processor,
            current_block,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let da_read_from = self.config.da_read_from.clone();

        if let Some(folder) = da_read_from.strip_prefix("folder:") {
            self.replay_folder_into_processor(folder).await?;
            return Ok(());
        }

        if let Some(folder) = da_read_from.strip_prefix("da:") {
            self.replay_da_storage_into_processor(folder).await?;
            return Ok(());
        }

        anyhow::bail!(
            "LocalDaReplayer received non-local source {}",
            self.config.da_read_from
        );
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        self.processor.persist(self.current_block).await
    }
}

fn is_bin_file(path: &Path) -> bool {
    path.extension().map(|ext| ext == "bin").unwrap_or(false)
}
