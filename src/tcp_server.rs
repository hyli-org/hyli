use crate::bus::BusClientSender;

use anyhow::Result;
use client_sdk::tcp_client::{TcpApiServer, TcpServerMessage};
use hyli_modules::{
    bus::SharedMessageBus,
    log_error, module_handle_messages,
    modules::{Module, module_bus_client},
};
use hyli_net::tcp::TcpEvent;
use tracing::{info, warn};

module_bus_client! {
#[derive(Debug)]
struct TcpServerBusClient {
    sender(TcpServerMessage),
}
}

#[derive(Debug)]
pub struct TcpServer {
    tcp_server_port: u16,
    bus: TcpServerBusClient,
}

impl Module for TcpServer {
    type Context = u16;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = TcpServerBusClient::new_from_bus(bus.new_handle()).await;

        Ok(TcpServer {
            tcp_server_port: ctx,
            bus,
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl TcpServer {
    pub async fn start(&mut self) -> Result<()> {
        let tcp_server_port = self.tcp_server_port;

        info!(
            "📡  Starting TcpServer module, listening for stream requests on port {}",
            &tcp_server_port
        );

        let mut server = TcpApiServer::start(tcp_server_port, "TcpApiServer").await?;

        module_handle_messages! {
            on_self self,
            Some(tcp_event) = server.listen_next() => {
                match tcp_event {
                    TcpEvent::Message { socket_addr: _, data, headers: _ } => {
                        _ = log_error!(self.bus.send_waiting_if_full(data).await, "Sending message on TcpServerMessage topic from connection pool");
                    }
                    TcpEvent::Closed { socket_addr } => {
                        // Ensure we don't leak dead sockets in the underlying TcpServer.
                        server.drop_peer_stream(socket_addr);
                    }
                    TcpEvent::Error { socket_addr, error } => {
                        warn!("TcpApiServer connection error from {}: {}", socket_addr, error);
                        server.drop_peer_stream(socket_addr);
                    }
                }
            }
        };

        Ok(())
    }
}
