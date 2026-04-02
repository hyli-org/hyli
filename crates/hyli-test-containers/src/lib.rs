//! Minimal Docker test container support using the Docker CLI.
//! Replaces testcontainers-modules with zero heavy transitive dependencies.

use anyhow::Context;
use std::marker::PhantomData;

pub mod postgres {
    #[derive(Default)]
    pub struct Postgres {
        pub(crate) tag: Option<String>,
        pub(crate) cmd: Vec<String>,
        pub(crate) mapped_port: Option<(u16, u16)>,
    }
}

pub struct ContainerAsync<T> {
    container_id: String,
    _phantom: PhantomData<T>,
}

pub trait ImageExt: Sized {
    fn with_tag(self, tag: impl Into<String>) -> Self;
    fn with_cmd<I>(self, cmd: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>;
    fn with_mapped_port(self, host_port: u16, container_port: u16) -> Self;
}

impl ImageExt for postgres::Postgres {
    fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    fn with_cmd<I>(mut self, cmd: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        self.cmd = cmd.into_iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    fn with_mapped_port(mut self, host_port: u16, container_port: u16) -> Self {
        self.mapped_port = Some((host_port, container_port));
        self
    }
}

#[allow(async_fn_in_trait)]
pub mod runners {
    use super::{postgres, ContainerAsync};
    use anyhow::{Context, Result};
    use std::marker::PhantomData;
    use std::time::Duration;
    use tokio::process::Command;

    pub trait AsyncRunner: Sized {
        async fn start(self) -> Result<ContainerAsync<Self>>;
    }

    impl AsyncRunner for postgres::Postgres {
        async fn start(self) -> Result<ContainerAsync<postgres::Postgres>> {
            let tag = self.tag.as_deref().unwrap_or("latest");
            let image = format!("postgres:{}", tag);

            let mut args = vec![
                "run".to_string(),
                "-d".to_string(),
                "-e".to_string(),
                "POSTGRES_PASSWORD=postgres".to_string(),
                "-e".to_string(),
                "POSTGRES_USER=postgres".to_string(),
                "-e".to_string(),
                "POSTGRES_DB=postgres".to_string(),
            ];

            if let Some((host_port, container_port)) = self.mapped_port {
                args.push("-p".to_string());
                args.push(format!("{}:{}", host_port, container_port));
            } else {
                args.push("-P".to_string());
            }

            args.push(image);

            for arg in &self.cmd {
                args.push(arg.clone());
            }

            let output = Command::new("docker")
                .args(&args)
                .output()
                .await
                .context("Failed to run docker command. Is Docker running?")?;

            if !output.status.success() {
                anyhow::bail!(
                    "docker run failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            let container_id = String::from_utf8(output.stdout)
                .context("Invalid container ID from docker run")?
                .trim()
                .to_string();

            // Wait for PostgreSQL to be fully ready by watching Docker logs.
            //
            // testcontainers-modules waits for these two conditions sequentially:
            //   1. WaitFor::message_on_stderr("database system is ready to accept connections")
            //   2. WaitFor::message_on_stdout("database system is ready to accept connections")
            //
            // The Postgres Docker entrypoint runs a temporary server during initdb (writes to
            // stderr), then stops it and starts the real server (writes to stdout). Waiting for
            // both ensures we are past the restart and connected to the stable final server.
            //
            // `docker logs` sends the container's stdout to the command's stdout and the
            // container's stderr to the command's stderr, so we check each independently.
            let ready_marker = "database system is ready to accept connections";
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            loop {
                let result = Command::new("docker")
                    .args(["logs", &container_id])
                    .output()
                    .await;

                if let Ok(out) = result {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    if stderr.contains(ready_marker) && stdout.contains(ready_marker) {
                        break;
                    }
                }

                if std::time::Instant::now() >= deadline {
                    let _ = Command::new("docker")
                        .args(["rm", "-f", &container_id])
                        .output()
                        .await;
                    anyhow::bail!("Timed out waiting for postgres to be ready");
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            Ok(ContainerAsync {
                container_id,
                _phantom: PhantomData,
            })
        }
    }
}

impl<T> ContainerAsync<T> {
    pub async fn get_host_port_ipv4(&self, container_port: u16) -> anyhow::Result<u16> {
        let output = tokio::process::Command::new("docker")
            .args(["port", &self.container_id, &container_port.to_string()])
            .output()
            .await
            .context("Failed to run docker port command")?;

        if !output.status.success() {
            anyhow::bail!(
                "docker port failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let stdout = String::from_utf8(output.stdout).context("Invalid output from docker port")?;

        // Output format: "0.0.0.0:54321\n" or ":::54321\n" (possibly multiple lines for IPv4/IPv6)
        let port_str = stdout
            .lines()
            .rfind(|l: &&str| !l.is_empty())
            .context("No port mapping found in docker port output")?
            .rsplit(':')
            .next()
            .context("Unexpected docker port output format")?;

        port_str
            .trim()
            .parse::<u16>()
            .context("Invalid port number in docker port output")
    }
}

impl<T> Drop for ContainerAsync<T> {
    fn drop(&mut self) {
        let id = self.container_id.clone();
        let _ = std::process::Command::new("docker")
            .args(["stop", &id])
            .output();
        let _ = std::process::Command::new("docker")
            .args(["rm", &id])
            .output();
    }
}
