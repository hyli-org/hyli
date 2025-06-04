//! Networking layer

use std::collections::HashSet;

use crate::{
    bus::BusClientSender, consensus::ConsensusNetMessage, mempool::MempoolNetMessage,
    model::SharedRunContext, utils::conf::SharedConf,
};
use anyhow::{bail, Context, Error, Result};
use hyle_crypto::{BlstCrypto, SharedBlstCrypto};
use hyle_model::{BlockHeight, NodeStateBlock, ValidatorPublicKey};
use hyle_modules::{
    bus::SharedMessageBus,
    log_warn, module_handle_messages,
    modules::{module_bus_client, Module},
};
use hyle_net::{
    clock::TimestampMsClock,
    tcp::{
        p2p_server::{P2PServer, P2PServerEvent},
        Canal,
    },
};
use network::{
    IntoHeaderSignableData, MsgHeader, MsgWithHeader, NetMessage, OutboundMessage, PeerEvent,
};
use opentelemetry::{metrics::Histogram, InstrumentationScope};
use tracing::{info, trace, warn};

pub mod network;

#[derive(Debug, Clone)]
pub enum P2PCommand {
    ConnectTo { peer: String },
}
module_bus_client! {
struct P2PBusClient {
    sender(MsgWithHeader<MempoolNetMessage>),
    sender(MsgWithHeader<ConsensusNetMessage>),
    sender(PeerEvent),
    receiver(P2PCommand),
    receiver(NodeStateBlock),
    receiver(OutboundMessage),
}
}
pub struct P2P {
    config: SharedConf,
    bus: P2PBusClient,
    crypto: SharedBlstCrypto,
    // Metrics stuff
    netmessage_delay: Histogram<u64>,
}

impl Module for P2P {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus_client = P2PBusClient::new_from_bus(bus.new_handle()).await;

        let scope = InstrumentationScope::builder(ctx.config.id.clone()).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        Ok(P2P {
            config: ctx.config.clone(),
            bus: bus_client,
            crypto: ctx.crypto.clone(),
            netmessage_delay: my_meter
                .u64_histogram("netmessage_delay")
                .with_description("Reception delay in milliseconds for net messages")
                .with_unit("ms")
                .build(),
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.p2p_server()
    }
}

impl P2P {
    pub fn choose_canal(msg: &NetMessage) -> Canal {
        match msg {
            NetMessage::MempoolMessage(_) => Canal::new("mempool"),
            NetMessage::ConsensusMessage(_) => Canal::new("consensus"),
        }
    }

    pub async fn p2p_server(&mut self) -> Result<()> {
        let mut p2p_server = P2PServer::new(
            self.crypto.clone(),
            self.config.id.clone(),
            self.config.p2p.server_port,
            Some(self.config.p2p.max_frame_length),
            self.config.p2p.public_address.clone(),
            self.config.da_public_address.clone(),
            HashSet::from_iter(vec![Canal::new("mempool"), Canal::new("consensus")]),
        )
        .await?;

        info!(
            "📡  Starting P2P module, listening on {}",
            self.config.p2p.public_address
        );

        for peer_ip in self.config.p2p.peers.clone() {
            _ = p2p_server.try_start_connection(peer_ip.clone(), Canal::new("mempool"));
            _ = p2p_server.try_start_connection(peer_ip, Canal::new("consensus"));
        }

        module_handle_messages! {
            on_bus self.bus,
            listen<NodeStateBlock> b => {
                if b.0.block_height.0 > p2p_server.current_height {
                    p2p_server.current_height = b.0.block_height.0;
                }
            }
            listen<P2PCommand> cmd => {
                match cmd {
                    P2PCommand::ConnectTo { peer } => {
                        _ = p2p_server.try_start_connection(peer, Canal::new("consensus"));
                    }
                }
            }
            listen<OutboundMessage> res => {
                match res {
                    OutboundMessage::SendMessage { validator_id, msg }  => {
                        let canal = Self::choose_canal(&msg);
                        if let Err(e) = p2p_server.send(validator_id.clone(), canal, msg.clone()).await {
                            self.handle_failed_send(
                                &mut p2p_server,
                                validator_id,
                                msg,
                                e
                            ).await;
                        }
                    }
                    OutboundMessage::BroadcastMessage(message) => {
                        let canal = Self::choose_canal(&message);
                        p2p_server.broadcast(message.clone(), canal)
                    }
                    OutboundMessage::BroadcastMessageOnlyFor(only_for, message) => {
                        let canal = Self::choose_canal(&message);
                        p2p_server.broadcast_only_for(&only_for, canal, message.clone())
                    }
                };
            }

            p2p_tcp_event = p2p_server.listen_next() => {
                if let Ok(Some(p2p_server_event)) = log_warn!(p2p_server.handle_p2p_tcp_event(p2p_tcp_event).await, "Handling P2PTcpEvent") {
                    match p2p_server_event {
                        P2PServerEvent::NewPeer { name, pubkey, da_address, height } => {
                            let _ = log_warn!(self.bus.send(PeerEvent::NewPeer {
                                name,
                                pubkey,
                                da_address,
                                height: BlockHeight(height)
                            }), "Sending new peer event");
                        },
                        P2PServerEvent::P2PMessage { msg: net_message } => {
                            let _ = log_warn!(self.handle_net_message(net_message).await, "Handling P2P net message");
                        },
                    }
                }
            }
        };
        Ok(())
    }

    fn verify_msg_header<T: std::fmt::Debug + IntoHeaderSignableData>(
        msg: MsgWithHeader<T>,
    ) -> Result<()> {
        // Ignore messages that seem incorrectly timestamped (1h ahead or back)
        if msg.header.msg.timestamp.abs_diff(TimestampMsClock::now().0) > 3_600_000 {
            bail!("Message timestamp too far from current time");
        }
        let result = BlstCrypto::verify(&msg.header)?;
        if !result {
            bail!("Invalid header signature for message {:?}", msg);
        }
        // Verify the message matches the signed data
        if msg.header.msg.hash != msg.msg.to_header_signable_data() {
            bail!("Invalid signed hash for message {:?}", msg);
        }
        Ok(())
    }

    fn log_message_delay(
        &self,
        validator: &ValidatorPublicKey,
        header: &MsgHeader,
        msg_type: &'static str,
    ) {
        self.netmessage_delay.record(
            header.timestamp.abs_diff(TimestampMsClock::now().0) as u64,
            &[
                opentelemetry::KeyValue::new("msg_type", msg_type),
                opentelemetry::KeyValue::new("validator_pubkey", validator.to_string()),
            ],
        );
    }

    async fn handle_net_message(&mut self, msg: NetMessage) -> Result<(), Error> {
        trace!("RECV: {:?}", msg);
        match msg {
            NetMessage::MempoolMessage(mempool_msg) => {
                trace!("Received new mempool net message {}", mempool_msg.msg);
                Self::verify_msg_header(mempool_msg.clone())?;
                self.log_message_delay(
                    &mempool_msg.header.signature.validator,
                    &mempool_msg.header.msg,
                    "mempool",
                );
                self.bus
                    .send(mempool_msg)
                    .context("Receiving mempool net message")?;
            }
            NetMessage::ConsensusMessage(consensus_msg) => {
                trace!("Received new consensus net message {:?}", consensus_msg);
                Self::verify_msg_header(consensus_msg.clone())?;
                self.log_message_delay(
                    &consensus_msg.header.signature.validator,
                    &consensus_msg.header.msg,
                    "consensus",
                );
                self.bus
                    .send(consensus_msg)
                    .context("Receiving consensus net message")?;
            }
        }
        Ok(())
    }

    async fn handle_failed_send(
        &self,
        p2p_server: &mut P2PServer<NetMessage>,
        validator_id: ValidatorPublicKey,
        _msg: NetMessage,
        error: Error,
    ) {
        // TODO: add waiting list for failed messages
        let canal = Self::choose_canal(&_msg);
        warn!("{error}. Reconnecting to peer on canal {:?}...", canal);
        if let Some(peer_info) = p2p_server.peers.get(&validator_id) {
            p2p_server.start_connection_task(
                peer_info.node_connection_data.p2p_public_address.clone(),
                canal,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::network::{HeaderSignableData, HeaderSigner};
    use assertables::assert_err;

    #[tokio::test]
    async fn test_invalid_net_messages() -> Result<()> {
        let crypto2 = BlstCrypto::new("2").unwrap();

        // Test message with timestamp too far in future
        let mut bad_time_msg =
            crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(None, None))?;
        bad_time_msg.header.msg.timestamp = TimestampMsClock::now().0 + 7200000; // 2h in future
        assert_err!(P2P::verify_msg_header(bad_time_msg.clone()));

        // Test message with timestamp too far in past
        let mut bad_time_msg =
            crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(None, None))?;
        bad_time_msg.header.msg.timestamp = TimestampMsClock::now().0 - 7200000; // 2h in future
        assert_err!(P2P::verify_msg_header(bad_time_msg.clone()));

        // Test message with bad signature
        let mut bad_sig_msg =
            crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(None, None))?;
        bad_sig_msg.header.signature.signature.0 = vec![0, 1, 2, 3]; // Invalid signature bytes
        assert_err!(P2P::verify_msg_header(bad_sig_msg.clone()));

        // Test message with mismatched hash
        let mut bad_hash_msg =
            crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(None, None))?;
        bad_hash_msg.header.msg.hash = HeaderSignableData(vec![9, 9, 9]); // Wrong hash
        assert_err!(P2P::verify_msg_header(bad_hash_msg.clone()));

        Ok(())
    }
}
