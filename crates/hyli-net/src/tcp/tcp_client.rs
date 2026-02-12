use std::{net::SocketAddr, time::Duration};

use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use sdk::hyli_model_utils::TimestampMs;

use crate::{clock::TimestampMsClock, metrics::TcpClientMetrics, net::TcpStream};
use anyhow::{bail, Result};
use tracing::{debug, info, trace, warn};

#[cfg(feature = "turmoil")]
use super::intercept;
use super::{
    decode_tcp_payload, framed_stream, to_tcp_message, FramedStream, TcpMessage, TcpMessageLabel,
};

type TcpSender = SplitSink<FramedStream, Bytes>;
type TcpReceiver = SplitStream<FramedStream>;

#[derive(Debug)]
pub struct TcpClient<Req, Res>
where
    Req: BorshSerialize,
    Res: BorshDeserialize,
{
    pub id: String,
    pub sender: TcpSender,
    pub receiver: TcpReceiver,
    pub last_ping: TimestampMs,
    pub socket_addr: SocketAddr,
    pub metrics: TcpClientMetrics,
    pub _marker: std::marker::PhantomData<(Req, Res)>,
}

impl<Req, Res> TcpClient<Req, Res>
where
    Req: BorshSerialize,
    Res: BorshDeserialize,
{
    pub async fn connect<
        Id: std::fmt::Display,
        A: crate::net::ToSocketAddrs + std::fmt::Display,
    >(
        id: Id,
        target: A,
    ) -> Result<TcpClient<Req, Res>> {
        Self::connect_with_opts(id, None, target).await
    }

    pub async fn connect_with_opts<
        Id: std::fmt::Display,
        A: crate::net::ToSocketAddrs + std::fmt::Display,
    >(
        id: Id,
        max_frame_length: Option<usize>,
        target: A,
    ) -> Result<TcpClient<Req, Res>> {
        Self::connect_with_opts_and_timeout(id, max_frame_length, target, Duration::from_secs(10))
            .await
    }

    pub async fn connect_with_opts_and_timeout<
        Id: std::fmt::Display,
        A: crate::net::ToSocketAddrs + std::fmt::Display,
    >(
        id: Id,
        max_frame_length: Option<usize>,
        target: A,
        timeout: Duration,
    ) -> Result<TcpClient<Req, Res>> {
        let id = id.to_string();
        let start = tokio::time::Instant::now();
        let tcp_stream = loop {
            debug!(
                "TcpClient {} - Trying to connect to {} with max_frame_len: {:?}",
                id, &target, max_frame_length
            );
            match TcpStream::connect(&target).await {
                Ok(stream) => break stream,
                Err(e) => {
                    if start.elapsed() >= timeout {
                        bail!(
                            "TcpClient {} - Failed to connect to {}: {}. Timeout reached.",
                            id,
                            &target,
                            e
                        );
                    }
                    warn!(
                        "TcpClient {} - Failed to connect to {}: {}. Retrying in 1 second...",
                        id, target, e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        };
        let addr = tcp_stream.peer_addr()?;
        info!("TcpClient {} - Connected to data stream on {}.", id, addr);

        let (sender, receiver) = framed_stream(tcp_stream, max_frame_length).split();

        Ok(TcpClient::<Req, Res> {
            id: id.clone(),
            sender,
            receiver,
            last_ping: TimestampMsClock::now(),
            socket_addr: addr,
            metrics: TcpClientMetrics::global(id),
            _marker: std::marker::PhantomData,
        })
    }

    pub async fn send(&mut self, msg: Req) -> Result<()>
    where
        Req: TcpMessageLabel,
    {
        let message_label = msg.message_label();
        let msg_bytes: Bytes = to_tcp_message(&msg)?.try_into()?;
        #[cfg(feature = "turmoil")]
        let msg_bytes = match intercept::intercept_message(&msg_bytes) {
            intercept::MessageAction::Pass => msg_bytes,
            intercept::MessageAction::Drop => {
                debug!("Dropping outbound TCP frame for client {}", self.id);
                return Ok(());
            }
            intercept::MessageAction::Replace(corrupted) => {
                debug!("Corrupting outbound TCP frame for client {}", self.id);
                corrupted
            }
        };
        let nb_bytes: usize = (&msg_bytes as &Bytes).len();
        let start = std::time::Instant::now();
        let res = self.sender.send(msg_bytes).await;
        self.metrics
            .message_send_time(start.elapsed().as_secs_f64(), message_label);
        match res {
            Ok(()) => {
                self.metrics.message_emitted();
                self.metrics.message_emitted_bytes(nb_bytes as u64);
                Ok(())
            }
            Err(e) => {
                self.metrics.message_send_error();
                Err(e.into())
            }
        }
    }
    pub async fn ping(&mut self) -> Result<()> {
        self.sender.send(TcpMessage::Ping.try_into()?).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<Res> {
        loop {
            match self.receiver.next().await {
                Some(Ok(bytes)) => {
                    if *bytes == *b"PING" {
                        trace!("Ping received for client {}", self.id);
                    } else {
                        self.metrics.message_received();
                        self.metrics.message_received_bytes(bytes.len() as u64);
                        match decode_tcp_payload(&bytes) {
                            Ok((_, data)) => return Some(data),
                            Err(io) => {
                                self.metrics.message_error();
                                warn!("Error while deserializing data: {:#}", io);
                                return None;
                            }
                        }
                    }
                }
                None => {
                    // End of stream
                    warn!("End of stream for client {}", self.id);
                    self.metrics.message_closed();
                    return None;
                }
                Some(Err(e)) => {
                    warn!("Error while streaming data from peer: {:#}", e);
                    self.metrics.message_error();
                    return None;
                }
            }
        }
    }

    pub fn split(self) -> (TcpSender, TcpReceiver) {
        (self.sender, self.receiver)
    }

    pub async fn close(mut self) -> Result<()> {
        self.sender.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::TcpClient;
    use crate::tcp::tcp_server::TcpServer;

    type TestTCPServer = TcpServer<String, String>;
    type TestTCPClient = TcpClient<String, String>;

    #[tokio::test]
    async fn test_peer_addr() -> anyhow::Result<()> {
        let mut server = TestTCPServer::start(0, "Test").await?;

        let server_socket = server.local_addr()?;

        let client_socket = tokio::spawn(async move {
            let client = TestTCPClient::connect("id", server_socket).await.unwrap();
            client.socket_addr
        });

        while server.connected_clients().len() == 0 {
            _ = tokio::time::timeout(Duration::from_millis(100), server.listen_next()).await;
        }

        let client_socket = client_socket.await?;
        assert_eq!(client_socket.port(), server_socket.port());

        let clients: Vec<String> = server.connected_clients().cloned().collect();
        assert_eq!(clients.len(), 1);
        assert_ne!(clients, vec![server_socket.to_string()]);

        Ok(())
    }
}
