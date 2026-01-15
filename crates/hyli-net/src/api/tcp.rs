use crate::tcp::{tcp_client::TcpClient, tcp_server::TcpServer, TcpMessageLabel};
use borsh::{BorshDeserialize, BorshSerialize};
use sdk::Transaction;
use strum_macros::IntoStaticStr;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq, IntoStaticStr)]
pub enum TcpServerMessage {
    NewTx(Transaction),
}

impl TcpMessageLabel for TcpServerMessage {
    fn message_label(&self) -> &'static str {
        self.clone().into()
    }
}
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub struct TcpServerResponse;

impl TcpMessageLabel for TcpServerResponse {
    fn message_label(&self) -> &'static str {
        "response"
    }
}

pub type TcpApiServer = TcpServer<TcpServerMessage, TcpServerResponse>;
pub type TcpApiClient = TcpClient<TcpServerMessage, TcpServerResponse>;
