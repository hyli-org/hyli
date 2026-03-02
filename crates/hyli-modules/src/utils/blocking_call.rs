use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tracing::warn;

#[derive(Debug, Clone, Copy)]
pub struct BlockingCallPolicy {
    pub timeout: Duration,
    pub max_retries: usize,
    pub retry_backoff: Duration,
    pub retry_on_timeout: bool,
}

impl BlockingCallPolicy {
    pub fn from_env(
        timeout_var: &str,
        max_retries_var: &str,
        retry_backoff_var: Option<&str>,
        retry_on_timeout_var: Option<&str>,
        default_retry_on_timeout: bool,
    ) -> Self {
        let timeout = std::env::var(timeout_var)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(5));
        let max_retries = std::env::var(max_retries_var)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2);
        let retry_backoff = retry_backoff_var
            .and_then(|var| std::env::var(var).ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(50));
        let retry_on_timeout = retry_on_timeout_var
            .and_then(|var| std::env::var(var).ok())
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(default_retry_on_timeout);
        Self {
            timeout,
            max_retries,
            retry_backoff,
            retry_on_timeout,
        }
    }
}

pub fn run_blocking_with_retry_sync<T, F, Op, OnOp, OnRetry, OnTimeout>(
    policy: BlockingCallPolicy,
    op: &'static str,
    keyspace: &'static str,
    mut build: F,
    mut on_op: OnOp,
    mut on_retry: OnRetry,
    mut on_timeout: OnTimeout,
) -> Result<T>
where
    T: Send + 'static,
    F: FnMut() -> Op,
    Op: FnOnce() -> Result<T> + Send + 'static,
    OnOp: FnMut(u64),
    OnRetry: FnMut(),
    OnTimeout: FnMut(),
{
    let mut attempts = 0usize;
    loop {
        let start = Instant::now();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let op_fn = build();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || {
                let _ = tx.send(op_fn());
            });
        } else {
            std::thread::spawn(move || {
                let _ = tx.send(op_fn());
            });
        }

        match rx.recv_timeout(policy.timeout) {
            Ok(result) => {
                on_op(start.elapsed().as_micros() as u64);
                if result.is_err() && attempts < policy.max_retries {
                    attempts += 1;
                    on_retry();
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = policy.max_retries,
                        "retrying failed blocking call"
                    );
                    continue;
                }
                return result;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                on_op(start.elapsed().as_micros() as u64);
                on_timeout();
                if policy.retry_on_timeout && attempts < policy.max_retries {
                    attempts += 1;
                    on_retry();
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = policy.max_retries,
                        timeout_ms = policy.timeout.as_millis(),
                        "blocking call timed out; retrying"
                    );
                    continue;
                }
                return Err(anyhow!(
                    "blocking call {} on {} exceeded timeout budget ({} ms)",
                    op,
                    keyspace,
                    policy.timeout.as_millis()
                ));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                on_op(start.elapsed().as_micros() as u64);
                if attempts < policy.max_retries {
                    attempts += 1;
                    on_retry();
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = policy.max_retries,
                        "retrying blocking call after worker disconnect"
                    );
                    continue;
                }
                return Err(anyhow!("blocking worker disconnected while running {}", op));
            }
        }
    }
}

pub async fn run_blocking_with_retry_async<T, F, Op, OnOp, OnRetry, OnTimeout>(
    policy: BlockingCallPolicy,
    op: &'static str,
    keyspace: &'static str,
    mut build: F,
    mut on_op: OnOp,
    mut on_retry: OnRetry,
    mut on_timeout: OnTimeout,
) -> Result<T>
where
    T: Send + 'static,
    F: FnMut() -> Op,
    Op: FnOnce() -> Result<T> + Send + 'static,
    OnOp: FnMut(u64),
    OnRetry: FnMut(),
    OnTimeout: FnMut(),
{
    let mut attempts = 0usize;
    loop {
        let start = Instant::now();
        let handle = tokio::task::spawn_blocking(build());
        let timed = tokio::time::timeout(policy.timeout, handle).await;
        match timed {
            Ok(joined) => match joined {
                Ok(Ok(value)) => {
                    on_op(start.elapsed().as_micros() as u64);
                    return Ok(value);
                }
                Ok(Err(err)) => {
                    on_op(start.elapsed().as_micros() as u64);
                    if attempts >= policy.max_retries {
                        return Err(err);
                    }
                    attempts += 1;
                    on_retry();
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = policy.max_retries,
                        "retrying failed blocking call"
                    );
                }
                Err(join_err) => {
                    on_op(start.elapsed().as_micros() as u64);
                    if attempts >= policy.max_retries {
                        return Err(anyhow!("blocking task join error: {}", join_err));
                    }
                    attempts += 1;
                    on_retry();
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = policy.max_retries,
                        error = %join_err,
                        "retrying blocking call after join error"
                    );
                }
            },
            Err(_) => {
                on_op(start.elapsed().as_micros() as u64);
                on_timeout();
                if policy.retry_on_timeout && attempts < policy.max_retries {
                    attempts += 1;
                    on_retry();
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = policy.max_retries,
                        timeout_ms = policy.timeout.as_millis(),
                        "blocking call timed out; retrying"
                    );
                } else {
                    return Err(anyhow!(
                        "blocking call timed out after {} ms (op={}, keyspace={})",
                        policy.timeout.as_millis(),
                        op,
                        keyspace
                    ));
                }
            }
        }
        if !policy.retry_backoff.is_zero() {
            tokio::time::sleep(policy.retry_backoff).await;
        }
    }
}
