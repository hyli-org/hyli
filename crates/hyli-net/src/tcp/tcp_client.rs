use std::net::SocketAddr;

use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use sdk::hyli_model_utils::TimestampMs;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{clock::TimestampMsClock, net::TcpStream};
use anyhow::{bail, Result};
use tracing::{debug, info, trace, warn};

use super::{to_tcp_message, TcpMessage};

type TcpSender = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;
type TcpReceiver = SplitStream<Framed<TcpStream, LengthDelimitedCodec>>;

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
        let timeout = std::time::Duration::from_secs(10);
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

        let mut codec = LengthDelimitedCodec::new();
        if let Some(mfl) = max_frame_length {
            codec.set_max_frame_length(mfl);
        }

        let (sender, receiver) = Framed::new(tcp_stream, codec).split();

        Ok(TcpClient::<Req, Res> {
            id: id.to_string(),
            sender,
            receiver,
            last_ping: TimestampMsClock::now(),
            socket_addr: addr,
            _marker: std::marker::PhantomData,
        })
    }

    pub async fn send(&mut self, msg: Req) -> Result<()> {
        self.sender.send(to_tcp_message(&msg)?.try_into()?).await?;
        Ok(())
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
                        match borsh::from_slice(&bytes) {
                            Ok(data) => return Some(data),
                            Err(io) => {
                                warn!("Error while deserializing data: {:#}", io);
                                return None;
                            }
                        }
                    }
                }
                None => {
                    // End of stream
                    warn!("End of stream for client {}", self.id);
                    return None;
                }
                Some(Err(e)) => {
                    warn!("Error while streaming data from peer: {:#}", e);
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

        while server.connected_clients().is_empty() {
            _ = tokio::time::timeout(Duration::from_millis(100), server.listen_next()).await;
        }

        let client_socket = client_socket.await?;
        assert_eq!(client_socket.port(), server_socket.port());

        let clients = server.connected_clients();
        assert_eq!(clients.len(), 1);
        assert_ne!(clients, vec![server_socket.to_string()]);

        Ok(())
    }
}
