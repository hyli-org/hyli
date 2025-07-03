use std::collections::BTreeSet;

use hyle_modules::{bus::SharedMessageBus, modules::Module};

use crate::model::SharedRunContext;

use super::{blocks_fjall::Blocks, d_a_bus_client::DABusClient, DataAvailability};

impl Module for DataAvailability {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> anyhow::Result<Self> {
        let bus = DABusClient::new_from_bus(bus.new_handle()).await;

        Ok(DataAvailability {
            config: ctx.config.clone(),
            bus,
            blocks: Blocks::new(&ctx.config.data_directory.join("data_availability.db"))?,
            buffered_signed_blocks: BTreeSet::new(),
            need_catchup: false,
            catchup_task: None,
            catchup_height: None,
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        self.start().await
    }
}
