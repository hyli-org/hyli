use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use borsh::{from_slice, to_vec};
use hyli_contract_sdk::{HyliOutput, ProgramId, Verifier};
use hyli_model::{
    verifier_worker::{VerifyRequest, VerifyResponse},
    ProofData,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};
use tracing::error;

use crate::utils::conf::{VerifierWorkersBackendConf, VerifierWorkersConf};

pub struct ProofVerifierService {
    routes: HashMap<String, Arc<ProcessVerifierWorker>>,
}

impl ProofVerifierService {
    pub async fn from_config(config: &VerifierWorkersConf) -> Result<Self> {
        if !config.enabled {
            return Ok(Self {
                routes: HashMap::new(),
            });
        }

        let mut routes = HashMap::new();
        for (backend_name, backend) in &config.backends {
            if !backend.enabled {
                continue;
            }

            let worker = Arc::new(
                ProcessVerifierWorker::spawn(
                    backend_name.clone(),
                    backend.clone(),
                    config.default_request_timeout_ms,
                    config.default_restart_backoff_ms,
                )
                .await
                .with_context(|| format!("starting verifier worker backend '{backend_name}'"))?,
            );

            for verifier in &backend.verifiers {
                routes.insert(verifier.clone(), Arc::clone(&worker));
            }
        }

        Ok(Self { routes })
    }

    pub fn handles(&self, verifier: &Verifier) -> bool {
        self.routes.contains_key(&verifier.0)
    }

    pub async fn verify(
        &self,
        verifier: &Verifier,
        proof: &ProofData,
        program_id: &ProgramId,
    ) -> Result<Vec<HyliOutput>> {
        let worker = self
            .routes
            .get(&verifier.0)
            .with_context(|| format!("no verifier worker configured for {}", verifier))?;
        worker.verify(verifier, proof, program_id).await
    }
}

struct ProcessVerifierWorker {
    backend_name: String,
    command: String,
    args: Vec<String>,
    request_timeout: Duration,
    startup_timeout: Duration,
    restart_backoff: Duration,
    auto_restart: bool,
    state: Mutex<Option<WorkerProcess>>,
}

struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
}

impl ProcessVerifierWorker {
    async fn spawn(
        backend_name: String,
        backend: VerifierWorkersBackendConf,
        default_request_timeout_ms: u64,
        default_restart_backoff_ms: u64,
    ) -> Result<Self> {
        let request_timeout_ms = if backend.request_timeout_ms == 0 {
            default_request_timeout_ms
        } else {
            backend.request_timeout_ms
        };
        let restart_backoff_ms = if default_restart_backoff_ms == 0 {
            1_000
        } else {
            default_restart_backoff_ms
        };

        let mut worker = Self {
            backend_name,
            command: backend.command,
            args: backend.args,
            request_timeout: Duration::from_millis(request_timeout_ms),
            startup_timeout: Duration::from_millis(backend.startup_timeout_ms),
            restart_backoff: Duration::from_millis(restart_backoff_ms),
            auto_restart: backend.auto_restart,
            state: Mutex::new(None),
        };

        worker.ensure_started().await?;
        Ok(worker)
    }

    async fn verify(
        &self,
        verifier: &Verifier,
        proof: &ProofData,
        program_id: &ProgramId,
    ) -> Result<Vec<HyliOutput>> {
        let request = VerifyRequest {
            verifier: verifier.0.clone(),
            proof: proof.0.clone(),
            program_id: program_id.0.clone(),
        };
        let request_bytes = to_vec(&request).context("serializing verifier worker request")?;

        let mut guard = self.state.lock().await;
        self.ensure_started_locked(&mut guard).await?;

        let process = guard
            .as_mut()
            .ok_or_else(|| anyhow!("verifier worker '{}' is not running", self.backend_name))?;

        let response = match timeout(
            self.request_timeout,
            Self::send_request(process, &request_bytes),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                tracing::warn!(
                    backend = %self.backend_name,
                    error = %err,
                    "Verifier worker request failed"
                );
                self.clear_process_locked(&mut guard).await;
                if self.auto_restart {
                    self.restart_locked(&mut guard).await?;
                }
                return Err(err);
            }
            Err(_) => {
                tracing::warn!(backend = %self.backend_name, "Verifier worker request timed out");
                self.clear_process_locked(&mut guard).await;
                if self.auto_restart {
                    self.restart_locked(&mut guard).await?;
                }
                bail!("verifier worker '{}' timed out", self.backend_name);
            }
        };

        if !response.ok {
            bail!(
                "worker backend '{}' rejected proof: {}",
                self.backend_name,
                response.error
            );
        }

        from_slice::<Vec<HyliOutput>>(&response.outputs)
            .context("deserializing verifier worker outputs")
    }

    async fn send_request(process: &mut WorkerProcess, request: &[u8]) -> Result<VerifyResponse> {
        process
            .stdin
            .write_u32_le(request.len() as u32)
            .await
            .context("writing verifier worker request length")?;
        process
            .stdin
            .write_all(request)
            .await
            .context("writing verifier worker request")?;
        process
            .stdin
            .flush()
            .await
            .context("flushing verifier worker request")?;

        let response_len = process
            .stdout
            .read_u32_le()
            .await
            .context("reading verifier worker response length")?;
        let mut response = vec![0; response_len as usize];
        process
            .stdout
            .read_exact(&mut response)
            .await
            .context("reading verifier worker response body")?;

        from_slice::<VerifyResponse>(&response).context("deserializing verifier worker response")
    }

    async fn ensure_started(&mut self) -> Result<()> {
        let mut guard = self.state.lock().await;
        self.ensure_started_locked(&mut guard).await
    }

    async fn ensure_started_locked(&self, guard: &mut Option<WorkerProcess>) -> Result<()> {
        let should_start = match guard.as_mut() {
            Some(process) => match process.child.try_wait() {
                Ok(Some(status)) => {
                    tracing::warn!(
                        backend = %self.backend_name,
                        status = %status,
                        "Verifier worker exited unexpectedly"
                    );
                    true
                }
                Ok(None) => false,
                Err(err) => {
                    tracing::warn!(
                        backend = %self.backend_name,
                        error = %err,
                        "Failed to query verifier worker status"
                    );
                    true
                }
            },
            None => true,
        };

        if should_start {
            *guard = Some(self.start_process().await?);
        }

        Ok(())
    }

    async fn restart_locked(&self, guard: &mut Option<WorkerProcess>) -> Result<()> {
        tokio::time::sleep(self.restart_backoff).await;
        *guard = Some(self.start_process().await?);
        Ok(())
    }

    async fn clear_process_locked(&self, guard: &mut Option<WorkerProcess>) {
        if let Some(mut process) = guard.take() {
            if let Err(err) = process.child.start_kill() {
                tracing::debug!(
                    backend = %self.backend_name,
                    error = %err,
                    "Failed to kill verifier worker"
                );
            }
            let _ = process.child.wait().await;
        }
    }

    async fn start_process(&self) -> Result<WorkerProcess> {
        let resolved_command = resolve_worker_command(&self.command).with_context(|| {
            format!(
                "resolving command for verifier worker backend '{}'",
                self.backend_name
            )
        })?;
        tracing::info!(
            backend = %self.backend_name,
            command = %resolved_command.display(),
            "Starting verifier worker"
        );

        let mut child = timeout(self.startup_timeout, async {
            let mut command = Command::new(&resolved_command);
            command
                .args(&self.args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            command.spawn()
        })
        .await
        .context("timed out spawning verifier worker")?
        .with_context(|| format!("spawning verifier worker '{}'", self.command))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("missing stdin for verifier worker '{}'", self.backend_name))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("missing stdout for verifier worker '{}'", self.backend_name))?;

        Ok(WorkerProcess {
            child,
            stdin,
            stdout,
        })
    }
}

fn resolve_worker_command(command: &str) -> Result<PathBuf> {
    let candidate = PathBuf::from(command);
    if candidate.components().count() > 1 || candidate.is_absolute() {
        return Ok(candidate);
    }

    if let Some(path) = find_in_path(command) {
        return Ok(path);
    }

    if cfg!(debug_assertions) {
        if let Some(path) = find_in_target_dir(command) {
            tracing::info!(
                command = %command,
                resolved = %path.display(),
                "Resolved verifier worker command from Cargo target dir fallback"
            );
            return Ok(path);
        }
    }

    error!("Could not resolve worker command '{}'", command);
    error!("Searched PATH and Cargo target directories (debug mode only)");
    error!("Have you built the worker binary? If not, you can build it with `cargo build -p {command}-worker --release`");
    error!("Or make sure the command is installed and available in PATH, or specify an absolute or relative path to the executable");

    bail!("could not resolve worker command '{}'", command)
}

fn find_in_path(command: &str) -> Option<PathBuf> {
    let path_var = env::var_os("PATH")?;
    env::split_paths(&path_var)
        .map(|dir| dir.join(command))
        .find(|candidate| candidate.is_file())
}

fn find_in_target_dir(command: &str) -> Option<PathBuf> {
    let target_dir = cargo_target_directory()?;
    let candidate = target_dir.join("debug").join(command);
    candidate.is_file().then_some(candidate).or_else(|| {
        let candidate = target_dir.join("release").join(command);
        candidate.is_file().then_some(candidate)
    })
}

fn cargo_target_directory() -> Option<PathBuf> {
    if let Some(path) = env::var_os("CARGO_TARGET_DIR").map(PathBuf::from) {
        return Some(path);
    }

    cargo_metadata_target_directory()
}

fn cargo_metadata_target_directory() -> Option<PathBuf> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    metadata
        .get("target_directory")
        .and_then(|value| value.as_str())
        .map(Path::new)
        .map(Path::to_path_buf)
}
