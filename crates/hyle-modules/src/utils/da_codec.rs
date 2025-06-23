use borsh::{BorshDeserialize, BorshSerialize};
use hyle_net::tcp::{tcp_client::TcpClient, tcp_server::TcpServer};
use sdk::{BlockHeight, MempoolStatusEvent, SignedBlock};

// Da Listener

pub const DATA_AVAILABILITY_REQUEST_CONFIRMATION: u64 = 1000; // 1000 blocks

#[derive(BorshDeserialize, BorshSerialize, Clone, Debug, PartialEq, Eq)]
pub enum DataAvailabilityRequest {
    /// Initial request to start listening for data availability events from a specific height.
    FromHeight(BlockHeight),
    /// Periodic request to confirm the current height synchronization with the data availability source.
    /// This is used for the server to avoid sending too many blocks at once, and saturate TCP buffers
    ConfirmedHeight(BlockHeight),
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum DataAvailabilityEvent {
    SignedBlock(SignedBlock),
    MempoolStatusEvent(MempoolStatusEvent),
}

pub type DataAvailabilityServer = TcpServer<DataAvailabilityRequest, DataAvailabilityEvent>;
pub type DataAvailabilityClient = TcpClient<DataAvailabilityRequest, DataAvailabilityEvent>;
