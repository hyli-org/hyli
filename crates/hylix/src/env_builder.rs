use crate::commands::devnet::{get_node_id, NodePorts};
use crate::config::HylixConfig;
use crate::constants::{self, env_values, env_vars};
use std::collections::HashMap;
use std::ffi::OsStr;

pub struct EnvBuilder {
    vars: HashMap<String, String>,
}

impl EnvBuilder {
    pub fn new() -> Self {
        Self {
            vars: HashMap::new(),
        }
    }

    pub fn for_devnet(config: &HylixConfig) -> Self {
        Self::new()
            .risc0_dev_mode()
            .sp1_prover_mock()
            .hyli_node_url(&config.devnet.node_port)
            .hyli_indexer_url(&config.devnet.indexer_port)
            .hyli_da_read_from(&config.devnet.da_port)
            .hyli_registry_url(&config.devnet.registry_server_port)
            .hyli_registry_api_key_dev()
    }

    pub fn with_ports(self, ip: &str, ports: &NodePorts) -> Self {
        self.set(
            constants::env_vars::HYLI_P2P__ADDRESS,
            &format!("localhost:{}", ports.p2p),
        )
        .set(
            constants::env_vars::HYLI_P2P__SERVER_PORT,
            &ports.p2p.to_string(),
        )
        .set(
            constants::env_vars::HYLI_REST_SERVER_PORT,
            &ports.rest.to_string(),
        )
        .set(
            constants::env_vars::HYLI_DA_SERVER_PORT,
            &ports.da.to_string(),
        )
        .set(
            constants::env_vars::HYLI_ADMIN_SERVER_PORT,
            &ports.admin.to_string(),
        )
        .set(
            constants::env_vars::HYLI_DA_PUBLIC_ADDRESS,
            format!("{}:{}", ip, ports.da).as_str(),
        )
        .set(
            constants::env_vars::HYLI_P2P_PUBLIC_ADDRESS,
            format!("{}:{}", ip, ports.p2p).as_str(),
        )
    }

    pub fn risc0_dev_mode(self) -> Self {
        self.set(env_vars::RISC0_DEV_MODE, env_values::RISC0_DEV_MODE_ONE)
    }

    pub fn sp1_prover_mock(self) -> Self {
        self.set(env_vars::SP1_PROVER, env_values::SP1_PROVER_MOCK)
    }

    pub fn hyli_node_url(self, port: &u16) -> Self {
        self.set(
            env_vars::HYLI_NODE_URL,
            &format!("http://localhost:{}", port),
        )
    }

    pub fn hyli_indexer_url(self, port: &u16) -> Self {
        self.set(
            env_vars::HYLI_INDEXER_URL,
            &format!("http://localhost:{}", port),
        )
    }

    pub fn hyli_da_read_from(self, port: &u16) -> Self {
        self.set(env_vars::HYLI_DA_READ_FROM, &format!("localhost:{}", port))
    }

    pub fn hyli_registry_url(self, port: &u16) -> Self {
        self.set(
            env_vars::HYLI_REGISTRY_URL,
            &format!("http://localhost:{}", port),
        )
    }

    pub fn hyli_registry_api_key_dev(self) -> Self {
        self.set(
            env_vars::HYLI_REGISTRY_API_KEY,
            env_values::REGISTRY_API_KEY_DEV,
        )
    }

    pub fn hyli_database_url(self, port: &u16) -> Self {
        self.set(
            env_vars::HYLI_DATABASE_URL,
            &format!("postgresql://postgres:postgres@localhost:{}", port),
        )
    }

    pub fn rust_log(self, value: &str) -> Self {
        self.set(env_vars::RUST_LOG, value)
    }

    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.vars.insert(key.to_string(), value.to_string());
        self
    }

    pub fn genesis_stakers(mut self, total_nodes: u32, has_local_node: bool) -> Self {
        // Set individual staker env vars for all nodes in the network
        for j in 0..total_nodes {
            let staker_node_id = if has_local_node && j == 0 {
                constants::containers::NODE_LOCAL.to_string()
            } else {
                get_node_id(j)
            };
            self = self.set(
                &format!("HYLI_GENESIS__STAKERS__{}", staker_node_id),
                "1000",
            )
        }
        self
    }

    pub fn into_tokio_command<S: AsRef<OsStr>>(self, program: S) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(program);
        for (key, value) in &self.vars {
            cmd.env(key, value);
        }
        cmd
    }
    pub fn into_std_command<S: AsRef<OsStr>>(self, program: S) -> std::process::Command {
        let mut cmd = std::process::Command::new(program);
        for (key, value) in &self.vars {
            cmd.env(key, value);
        }
        cmd
    }

    pub fn to_docker_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        for (key, value) in &self.vars {
            args.push("-e".to_string());
            args.push(format!("{}={}", key, value));
        }
        args
    }
}

impl Default for EnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}
