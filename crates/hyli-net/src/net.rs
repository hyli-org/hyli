use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::task::Poll;

use axum::extract::ConnectInfo;
use axum::extract::connect_info::{Connected, IntoMakeServiceWithConnectInfo, ResponseFuture};
use axum::middleware::AddExtension;
use axum::serve::{IncomingStream, Listener};
#[cfg(not(feature = "turmoil"))]
pub use tokio::net::*;

#[cfg(feature = "turmoil")]
pub use turmoil::net::*;
#[cfg(feature = "turmoil")]
pub use turmoil::*;

pub async fn bind_tcp_listener(port: u16) -> anyhow::Result<HyliNetTcpListener> {
    Ok(HyliNetTcpListener(
        TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await?,
    ))
}
/// Turmoil Listener does not implement axum::Listener. Wrap it with this helper to spawn an Axum server with it.
pub struct HyliNetTcpListener(pub TcpListener);

impl Listener for HyliNetTcpListener {
    type Io = TcpStream;

    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.0.accept().await.unwrap()
    }

    fn local_addr(&self) -> tokio::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

impl From<TcpListener> for HyliNetTcpListener {
    fn from(value: TcpListener) -> Self {
        HyliNetTcpListener(value)
    }
}

impl Deref for HyliNetTcpListener {
    type Target = TcpListener;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct HyliNetIntoMakeServiceWithconnectInfo<S, C>(pub IntoMakeServiceWithConnectInfo<S, C>);

impl<'a, S, C> tower_service::Service<IncomingStream<'a, HyliNetTcpListener>>
    for HyliNetIntoMakeServiceWithconnectInfo<S, C>
where
    S: Clone,
    C: Connected<IncomingStream<'a, HyliNetTcpListener>>,
{
    type Response = AddExtension<S, ConnectInfo<C>>;

    type Error = Infallible;

    type Future = ResponseFuture<S, C>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: IncomingStream<'a, HyliNetTcpListener>) -> Self::Future {
        self.0.call(req)
    }
}

#[derive(Clone)]
pub struct HyliNetSocketAddr(pub SocketAddr);

impl Connected<IncomingStream<'_, HyliNetTcpListener>> for HyliNetSocketAddr {
    fn connect_info(stream: IncomingStream<'_, HyliNetTcpListener>) -> Self {
        HyliNetSocketAddr(*stream.remote_addr())
    }
}

impl<S, C> Deref for HyliNetIntoMakeServiceWithconnectInfo<S, C> {
    type Target = IntoMakeServiceWithConnectInfo<S, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for HyliNetSocketAddr {
    type Target = SocketAddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
