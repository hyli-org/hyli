use anyhow::Result;
use sdk::{
    api::{APIBlock, APITransaction, TransactionTypeDb},
    Hashed, NodeStateEvent, TransactionData,
};
use serde::Serialize;

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::websocket::WsTopicMessage,
};
use crate::{module_bus_client, module_handle_messages, modules::Module};

#[derive(Debug, Clone, Serialize)]
pub enum WebsocketOutEvent {
    NodeStateEvent(NodeStateEvent),
    NewBlock(APIBlock),
    NewTx(APITransaction),
}

module_bus_client! {
#[derive(Debug)]
pub struct NodeWebsocketConnectorBusClient {
    sender(WsTopicMessage<WebsocketOutEvent>),
    receiver(NodeStateEvent),
}
}

pub struct NodeWebsocketConnector {
    bus: NodeWebsocketConnectorBusClient,
    events: Vec<String>,
}

pub struct NodeWebsocketConnectorCtx {
    pub events: Vec<String>,
}

impl Module for NodeWebsocketConnector {
    type Context = NodeWebsocketConnectorCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        Ok(Self {
            bus: NodeWebsocketConnectorBusClient::new_from_bus(bus.new_handle()).await,
            events: ctx.events,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<NodeStateEvent> msg => {
                self.handle_node_state_event(msg)?;
            },
        };
        Ok(())
    }
}

impl NodeWebsocketConnector {
    fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<()> {
        self.handle("node_state", &event, Self::handle_node_state);
        self.handle("new_block", &event, Self::handle_new_block);
        self.handle("new_tx", &event, Self::handle_new_tx);
        Ok(())
    }

    fn handle(
        &mut self,
        topic: &str,
        event: &NodeStateEvent,
        handler: fn(NodeStateEvent) -> Vec<WebsocketOutEvent>,
    ) {
        if self.events.contains(&topic.to_string()) {
            for e in handler(event.clone()) {
                let _ = self.bus.send(WsTopicMessage::new(topic, e));
            }
        }
    }

    fn handle_node_state(event: NodeStateEvent) -> Vec<WebsocketOutEvent> {
        vec![WebsocketOutEvent::NodeStateEvent(event)]
    }

    fn handle_new_block(event: NodeStateEvent) -> Vec<WebsocketOutEvent> {
        let NodeStateEvent::NewBlock(block) = event;
        let signed_block = block.signed_block;

        let block_hash = signed_block.hashed();
        let api_block = APIBlock {
            hash: block_hash,
            parent_hash: signed_block.parent_hash().clone(),
            height: signed_block.height().0,
            timestamp: signed_block.consensus_proposal.timestamp.0 as i64,
            total_txs: signed_block.count_txs() as u64,
        };

        vec![WebsocketOutEvent::NewBlock(api_block)]
    }

    fn handle_new_tx(event: NodeStateEvent) -> Vec<WebsocketOutEvent> {
        let NodeStateEvent::NewBlock(block) = event;
        let signed_block = block.signed_block;
        let mut txs = Vec::new();
        let block_hash = signed_block.hashed();
        let block_height = signed_block.height();
        let block_timestamp = signed_block.consensus_proposal.timestamp.clone();

        for (idx, (lane_id, tx_id, tx)) in signed_block.iter_txs_with_id().enumerate() {
            let transaction_type = match &tx.transaction_data {
                TransactionData::Blob(_) => TransactionTypeDb::BlobTransaction,
                TransactionData::Proof(_) => TransactionTypeDb::ProofTransaction,
                TransactionData::VerifiedProof(_) => TransactionTypeDb::ProofTransaction,
            };
            let identity = match &tx.transaction_data {
                TransactionData::Blob(tx) => Some(tx.identity.0.clone()),
                _ => None,
            };
            let transaction_status = sdk::api::TransactionStatusDb::Sequenced;
            let api_tx = APITransaction {
                tx_hash: tx_id.1.clone(),
                parent_dp_hash: tx_id.0.clone(),
                version: tx.version,
                transaction_type,
                transaction_status,
                block_hash: Some(block_hash.clone()),
                block_height: Some(block_height),
                index: Some(idx as u32),
                timestamp: Some(block_timestamp.clone()),
                lane_id: Some(lane_id.clone()),
                identity,
            };
            txs.push(WebsocketOutEvent::NewTx(api_tx));
        }
        txs
    }
}
