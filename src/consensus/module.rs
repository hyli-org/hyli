use anyhow::Result;
use hyle_modules::{bus::SharedMessageBus, modules::Module};
use tracing::warn;

use crate::model::SharedRunContext;

use super::{
    api, consensus_bus_client::ConsensusBusClient, metrics::ConsensusMetrics, Consensus,
    ConsensusStore,
};

impl Module for Consensus {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let file = ctx.config.data_directory.clone().join("consensus.bin");
        let store: ConsensusStore = Self::load_from_disk_or_default(file.as_path());
        let metrics = ConsensusMetrics::global(ctx.config.id.clone());

        let api = api::api(&bus, &ctx).await;
        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/consensus", api));
            }
        }
        let bus = ConsensusBusClient::new_from_bus(bus.new_handle()).await;

        Ok(Consensus {
            metrics,
            bus,
            file: Some(file),
            store,
            config: ctx.config.clone(),
            crypto: ctx.crypto.clone(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.wait_genesis().await
    }

    async fn persist(&self) -> Result<()> {
        if let Some(file) = &self.file {
            if let Err(e) = Self::save_on_disk(file.as_path(), &self.store) {
                warn!("Failed to save consensus storage on disk: {}", e);
            }
        }

        Ok(())
    }
}
