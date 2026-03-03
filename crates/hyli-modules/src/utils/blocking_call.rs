use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tracing::error;

#[derive(Debug, Clone, Copy)]
pub struct BlockingCallPolicy {
    pub timeout: Duration,
}

impl BlockingCallPolicy {
    pub fn from_env(timeout_var: &str) -> Self {
        let timeout = std::env::var(timeout_var)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(30));
        Self { timeout }
    }

    pub fn fjall_mempool_from_env() -> Self {
        Self::from_env("HYLI_FJALL_TIMEOUT_MS")
    }

    pub fn fjall_da_from_env() -> Self {
        Self::from_env("HYLI_FJALL_ASYNC_TIMEOUT_MS")
    }
}

pub fn run_blocking_with_timeout_sync<T, F, Op, OnOp, OnTimeout>(
    policy: BlockingCallPolicy,
    op: &'static str,
    keyspace: &'static str,
    mut build: F,
    mut on_op: OnOp,
    mut on_timeout: OnTimeout,
) -> Result<T>
where
    T: Send + 'static,
    F: FnMut() -> Op,
    Op: FnOnce() -> Result<T> + Send + 'static,
    OnOp: FnMut(u64),
    OnTimeout: FnMut(),
{
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
            if let Err(ref err) = result {
                error!(op = op, keyspace = keyspace, error = %err, "blocking call failed");
            }
            result
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            on_op(start.elapsed().as_micros() as u64);
            on_timeout();
            let err = anyhow!(
                "blocking call {} on {} exceeded timeout budget ({} ms)",
                op,
                keyspace,
                policy.timeout.as_millis()
            );
            error!(op = op, keyspace = keyspace, error = %err, "blocking call timed out");
            Err(err)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            on_op(start.elapsed().as_micros() as u64);
            let err = anyhow!("blocking worker disconnected while running {}", op);
            error!(op = op, keyspace = keyspace, error = %err, "blocking worker disconnected");
            Err(err)
        }
    }
}

pub async fn run_blocking_with_timeout_async<T, F, Op, OnOp, OnTimeout>(
    policy: BlockingCallPolicy,
    op: &'static str,
    keyspace: &'static str,
    mut build: F,
    mut on_op: OnOp,
    mut on_timeout: OnTimeout,
) -> Result<T>
where
    T: Send + 'static,
    F: FnMut() -> Op,
    Op: FnOnce() -> Result<T> + Send + 'static,
    OnOp: FnMut(u64),
    OnTimeout: FnMut(),
{
    let start = Instant::now();
    let handle = tokio::task::spawn_blocking(build());
    let timed = tokio::time::timeout(policy.timeout, handle).await;
    match timed {
        Ok(joined) => match joined {
            Ok(Ok(value)) => {
                on_op(start.elapsed().as_micros() as u64);
                Ok(value)
            }
            Ok(Err(err)) => {
                on_op(start.elapsed().as_micros() as u64);
                error!(op = op, keyspace = keyspace, error = %err, "blocking call failed");
                Err(err)
            }
            Err(join_err) => {
                on_op(start.elapsed().as_micros() as u64);
                let err = anyhow!("blocking task join error: {}", join_err);
                error!(op = op, keyspace = keyspace, error = %err, "blocking task join error");
                Err(err)
            }
        },
        Err(_) => {
            on_op(start.elapsed().as_micros() as u64);
            on_timeout();
            let err = anyhow!(
                "blocking call timed out after {} ms (op={}, keyspace={})",
                policy.timeout.as_millis(),
                op,
                keyspace
            );
            error!(op = op, keyspace = keyspace, error = %err, "blocking call timed out");
            Err(err)
        }
    }
}
