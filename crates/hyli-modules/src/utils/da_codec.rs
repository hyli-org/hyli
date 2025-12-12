use hyli_net::tcp::{tcp_client::TcpClient, tcp_server::TcpServer};
use sdk::{DataAvailabilityEvent, DataAvailabilityRequest};

pub type DataAvailabilityServer = TcpServer<DataAvailabilityRequest, DataAvailabilityEvent>;
pub type DataAvailabilityClient = TcpClient<DataAvailabilityRequest, DataAvailabilityEvent>;
