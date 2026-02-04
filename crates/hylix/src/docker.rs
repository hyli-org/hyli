use crate::constants;
use crate::env_builder::EnvBuilder;
use crate::error::{HylixError, HylixResult};
use crate::logging::ProgressExecutor;

pub struct ContainerSpec {
    name: String,
    image: String,
    network: String,
    ports: Vec<(u16, u16)>,
    ip: Option<String>,
    env_builder: EnvBuilder,
    custom_env: Vec<String>,
    args: Vec<String>,
}

impl ContainerSpec {
    pub fn new(name: &str, image: &str) -> Self {
        Self {
            name: name.to_string(),
            image: image.to_string(),
            network: constants::networks::DEVNET.to_string(),
            ports: Vec::new(),
            env_builder: EnvBuilder::new(),
            custom_env: Vec::new(),
            args: Vec::new(),
            ip: None,
        }
    }

    pub fn port(mut self, host: u16, container: u16) -> Self {
        self.ports.push((host, container));
        self
    }

    pub fn env_builder(mut self, builder: EnvBuilder) -> Self {
        self.env_builder = builder;
        self
    }

    pub fn ip(mut self, ip: &str) -> Self {
        self.ip = Some(ip.to_string());
        self
    }

    pub fn custom_env(mut self, env_vars: Vec<String>) -> Self {
        self.custom_env = env_vars;
        self
    }

    pub fn arg(mut self, arg: String) -> Self {
        self.args.push(arg);
        self
    }

    pub fn args(mut self, args: Vec<String>) -> Self {
        self.args.extend(args);
        self
    }

    fn build_run_args(&self) -> Vec<String> {
        let mut args = vec!["run".to_string(), "-d".to_string()];

        args.push("--network".to_string());
        args.push(self.network.clone());

        args.push("--name".to_string());
        args.push(self.name.clone());

        for (host, container) in &self.ports {
            args.push("-p".to_string());
            args.push(format!("{}:{}", host, container));
        }

        if let Some(ip) = &self.ip {
            args.push("--ip".to_string());
            args.push(ip.clone());
        }

        args.extend(self.env_builder.to_docker_args());

        for env_var in &self.custom_env {
            args.push("-e".to_string());
            args.push(env_var.clone());
        }

        args.push("--add-host".to_string());
        args.push("host.docker.internal:host-gateway".to_string());

        args.push(self.image.clone());
        args.extend(self.args.clone());

        args
    }
}

pub struct ContainerManager;

impl ContainerManager {
    pub async fn start_container(
        executor: &ProgressExecutor,
        spec: ContainerSpec,
        pull: bool,
    ) -> HylixResult<()> {
        if pull {
            Self::pull_image(executor, &spec.image).await?;
        }

        let args = spec.build_run_args();
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

        let success = executor
            .execute_command(
                format!("Starting container {}...", spec.name),
                "docker",
                &args_refs,
                None,
            )
            .await?;

        if !success {
            return Err(HylixError::process(format!(
                "Failed to start container {}",
                spec.name
            )));
        }

        Ok(())
    }

    pub async fn pull_image(executor: &ProgressExecutor, image: &str) -> HylixResult<()> {
        let success = executor
            .execute_command(
                format!("Pulling Docker image {}...", image),
                "docker",
                &["pull", image],
                None,
            )
            .await?;

        if !success {
            let output = tokio::process::Command::new("docker")
                .args(["images", "-q", image])
                .output()
                .await?;

            if !output.status.success() || output.stdout.is_empty() {
                return Err(HylixError::process(
                    "Failed to pull and image doesn't exist locally".to_string(),
                ));
            }
            crate::logging::log_info(&format!("Using existing local image: {image}"));
        }

        Ok(())
    }
}

/// Find an available /24 subnet that doesn't conflict with existing Docker networks
pub async fn find_available_subnet() -> HylixResult<String> {
    use tokio::process::Command;

    // Get all existing Docker networks
    let output = Command::new("docker")
        .args(["network", "ls", "--format", "{{.ID}}"])
        .output()
        .await
        .map_err(|e| HylixError::process(format!("Failed to list Docker networks: {e}")))?;

    if !output.status.success() {
        return Err(HylixError::process(
            "Failed to list Docker networks".to_string(),
        ));
    }

    let network_ids = String::from_utf8_lossy(&output.stdout);
    let mut existing_subnets = Vec::new();

    // Get subnets for each network
    for network_id in network_ids.lines() {
        let network_id = network_id.trim();
        if network_id.is_empty() {
            continue;
        }

        let output = Command::new("docker")
            .args([
                "network",
                "inspect",
                "-f",
                "{{range .IPAM.Config}}{{.Subnet}} {{end}}",
                network_id,
            ])
            .output()
            .await
            .map_err(|e| {
                HylixError::process(format!(
                    "Failed to inspect Docker network {network_id}: {e}"
                ))
            })?;

        if output.status.success() {
            let subnets = String::from_utf8_lossy(&output.stdout);
            for subnet in subnets.split_whitespace() {
                if !subnet.is_empty() {
                    existing_subnets.push(subnet.to_string());
                }
            }
        }
    }

    // Generate candidate /24 subnets from common private IP ranges
    let mut candidates = Vec::new();

    // 10.0.0.0/8 range - try 10.89.0.0 to 10.99.255.0
    for second_octet in 89..=99 {
        for third_octet in 0..=255 {
            candidates.push(format!("10.{}.{}.0/24", second_octet, third_octet));
        }
    }

    // 172.16.0.0/12 range - try 172.18.0.0 to 172.31.255.0
    for second_octet in 18..=31 {
        for third_octet in 0..=255 {
            candidates.push(format!("172.{}.{}.0/24", second_octet, third_octet));
        }
    }

    // 192.168.0.0/16 range - try 192.168.0.0 to 192.168.255.0
    for third_octet in 0..=255 {
        candidates.push(format!("192.168.{}.0/24", third_octet));
    }

    // Find first non-conflicting subnet
    for candidate in candidates {
        if !subnet_conflicts(&candidate, &existing_subnets) {
            return Ok(candidate);
        }
    }

    Err(HylixError::devnet(
        "No available /24 subnet found. Please clean up unused Docker networks.".to_string(),
    ))
}

/// Check if a candidate subnet conflicts with any existing subnets
fn subnet_conflicts(candidate: &str, existing: &[String]) -> bool {
    // Parse candidate subnet
    let Some((candidate_ip, candidate_prefix)) = parse_subnet(candidate) else {
        return true; // If we can't parse it, consider it a conflict
    };

    for existing_subnet in existing {
        let Some((existing_ip, existing_prefix)) = parse_subnet(existing_subnet) else {
            continue;
        };

        // Check if subnets overlap
        if subnets_overlap(candidate_ip, candidate_prefix, existing_ip, existing_prefix) {
            return true;
        }
    }

    false
}

/// Parse a subnet string (e.g., "10.89.0.0/24") into (IP as u32, prefix length)
fn parse_subnet(subnet: &str) -> Option<(u32, u8)> {
    let parts: Vec<&str> = subnet.split('/').collect();
    if parts.len() != 2 {
        return None;
    }

    let ip_parts: Vec<&str> = parts[0].split('.').collect();
    if ip_parts.len() != 4 {
        return None;
    }

    let octets: Result<Vec<u8>, _> = ip_parts.iter().map(|s| s.parse::<u8>()).collect();
    let Ok(octets) = octets else {
        return None;
    };

    let ip = ((octets[0] as u32) << 24)
        | ((octets[1] as u32) << 16)
        | ((octets[2] as u32) << 8)
        | (octets[3] as u32);

    let prefix = parts[1].parse::<u8>().ok()?;

    Some((ip, prefix))
}

/// Check if two subnets overlap
fn subnets_overlap(ip1: u32, prefix1: u8, ip2: u32, prefix2: u8) -> bool {
    // Use the smaller prefix (larger network) for comparison
    let prefix = prefix1.min(prefix2);
    let mask = if prefix == 0 {
        0
    } else {
        !0u32 << (32 - prefix)
    };

    (ip1 & mask) == (ip2 & mask)
}
