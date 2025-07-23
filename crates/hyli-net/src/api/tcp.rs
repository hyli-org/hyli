use crate::tcp::{tcp_client::TcpClient, tcp_server::TcpServer};
use borsh::{BorshDeserialize, BorshSerialize};
use sdk::Transaction;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub enum TcpServerMessage {
    NewTx(Transaction),
}
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub struct TcpServerResponse;

pub type TcpApiServer = TcpServer<TcpServerMessage, TcpServerResponse>;
pub type TcpApiClient = TcpClient<TcpServerMessage, TcpServerResponse>;
