use crate::config::HylixConfig;
use crate::error::HylixResult;
use client_sdk::rest_client::NodeApiHttpClient;

/// Multi-node configuration
#[derive(Debug, Clone)]
pub struct MultiNodeConfig {
    /// Total number of validators (including local node if enabled)
    pub total_nodes: u32,
    /// Number of Docker nodes
    pub docker_nodes: u32,
    /// Whether there's a local node for debugging
    pub has_local_node: bool,
    /// Genesis timestamp shared by all nodes
    pub genesis_timestamp: u64,
}

impl MultiNodeConfig {
    pub fn new(total_nodes: u32, no_local: bool) -> Self {
        let has_local_node = !no_local;
        let docker_nodes = if has_local_node {
            total_nodes - 1
        } else {
            total_nodes
        };
        let genesis_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            total_nodes,
            docker_nodes,
            has_local_node,
            genesis_timestamp,
        }
    }
}

/// Context struct containing client and config for devnet operations
pub struct DevnetContext {
    pub client: NodeApiHttpClient,
    pub config: HylixConfig,
    pub profile: Option<String>,
    pub pull: bool,
    pub multi_node: Option<MultiNodeConfig>,
}

impl From<HylixConfig> for DevnetContext {
    fn from(config: HylixConfig) -> Self {
        DevnetContext::new(config).unwrap()
    }
}

impl DevnetContext {
    /// Create a new DevnetContext
    pub fn new(config: HylixConfig) -> HylixResult<Self> {
        let node_url = format!("http://localhost:{}", config.devnet.node_port);
        let client = NodeApiHttpClient::new(node_url)?;
        Ok(Self {
            client,
            config,
            profile: None,
            pull: true,
            multi_node: None,
        })
    }

    /// Create a new DevnetContext with a specific profile
    pub fn new_with_profile(config: HylixConfig, profile: Option<String>) -> HylixResult<Self> {
        let node_url = format!("http://localhost:{}", config.devnet.node_port);
        let client = NodeApiHttpClient::new(node_url)?;
        Ok(Self {
            client,
            config,
            profile,
            pull: true,
            multi_node: None,
        })
    }

    pub fn without_pull(&mut self) {
        self.pull = false;
    }

    pub fn with_multi_node(&mut self, multi_node: MultiNodeConfig) {
        self.multi_node = Some(multi_node);
    }
}

/// Port configuration for a node in multi-node setup
#[derive(Debug, Clone)]
pub struct NodePorts {
    pub rest: u16,
    pub da: u16,
    pub p2p: u16,
    pub admin: u16,
}

impl NodePorts {
    pub fn for_docker_node(index: u32, base_config: &crate::config::DevnetConfig) -> Self {
        Self {
            rest: base_config.node_port + (index * 1000) as u16,
            da: base_config.da_port + (index * 1010) as u16,
            p2p: 1231 + (index * 1000) as u16,
            admin: 1111 + (index * 1111) as u16,
        }
    }

    pub fn for_local_node(base_config: &crate::config::DevnetConfig) -> Self {
        // Local node uses port 0 offsets (before Docker nodes)
        Self {
            rest: base_config.node_port, // 4321
            da: base_config.da_port,     // 4141
            p2p: 1231,
            admin: 1111,
        }
    }
}
