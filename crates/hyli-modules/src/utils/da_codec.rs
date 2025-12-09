use borsh::{BorshDeserialize, BorshSerialize};
use hyli_net::tcp::{tcp_client::TcpClient, tcp_server::TcpServer};
use sdk::{BlockHeight, MempoolStatusEvent, SignedBlock};

// Da Listener
//
#[derive(BorshDeserialize, BorshSerialize, Clone, Debug, PartialEq, Eq)]
pub struct DataAvailabilityRequest(pub BlockHeight);

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum DataAvailabilityEvent {
    SignedBlock(SignedBlock),
    MempoolStatusEvent(MempoolStatusEvent),
}

pub type DataAvailabilityServer = TcpServer<DataAvailabilityRequest, DataAvailabilityEvent>;
pub type DataAvailabilityClient = TcpClient<DataAvailabilityRequest, DataAvailabilityEvent>;
