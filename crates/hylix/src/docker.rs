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
