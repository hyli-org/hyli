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
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let op_fn = build();
    tokio::task::spawn_blocking(move || {
        let _ = tx.send(op_fn());
    });

    let poll_interval = Duration::from_millis(25);
    let deadline = start + policy.timeout;

    loop {
        let now = Instant::now();
        if now >= deadline {
            on_op(start.elapsed().as_micros() as u64);
            on_timeout();
            let err = anyhow!(
                "blocking call timed out after {} ms (op={}, keyspace={})",
                policy.timeout.as_millis(),
                op,
                keyspace
            );
            error!(op = op, keyspace = keyspace, error = %err, "blocking call timed out");
            return Err(err);
        }

        let remaining = deadline.saturating_duration_since(now);
        let wait_for = remaining.min(poll_interval);
        match rx.recv_timeout(wait_for) {
            Ok(result) => {
                on_op(start.elapsed().as_micros() as u64);
                if let Err(ref err) = result {
                    error!(op = op, keyspace = keyspace, error = %err, "blocking call failed");
                }
                return result;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                on_op(start.elapsed().as_micros() as u64);
                let err = anyhow!("blocking worker disconnected while running {}", op);
                error!(op = op, keyspace = keyspace, error = %err, "blocking worker disconnected");
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn sync_timeout_triggers_timeout_callback() {
        let timeout_calls = Arc::new(AtomicUsize::new(0));
        let op_calls = Arc::new(AtomicUsize::new(0));

        let timeout_calls_cb = Arc::clone(&timeout_calls);
        let op_calls_cb = Arc::clone(&op_calls);
        let res = run_blocking_with_timeout_sync(
            BlockingCallPolicy {
                timeout: Duration::from_millis(10),
            },
            "test_sync",
            "test_keyspace",
            || {
                move || {
                    std::thread::sleep(Duration::from_millis(200));
                    Ok::<(), anyhow::Error>(())
                }
            },
            move |_| {
                op_calls_cb.fetch_add(1, Ordering::Relaxed);
            },
            move || {
                timeout_calls_cb.fetch_add(1, Ordering::Relaxed);
            },
        );

        assert!(res.is_err());
        assert_eq!(timeout_calls.load(Ordering::Relaxed), 1);
        assert_eq!(op_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn async_timeout_triggers_timeout_callback() {
        let timeout_calls = Arc::new(AtomicUsize::new(0));
        let op_calls = Arc::new(AtomicUsize::new(0));

        let timeout_calls_cb = Arc::clone(&timeout_calls);
        let op_calls_cb = Arc::clone(&op_calls);
        let res = run_blocking_with_timeout_async(
            BlockingCallPolicy {
                timeout: Duration::from_millis(10),
            },
            "test_async",
            "test_keyspace",
            || {
                move || {
                    std::thread::sleep(Duration::from_millis(200));
                    Ok::<(), anyhow::Error>(())
                }
            },
            move |_| {
                op_calls_cb.fetch_add(1, Ordering::Relaxed);
            },
            move || {
                timeout_calls_cb.fetch_add(1, Ordering::Relaxed);
            },
        )
        .await;

        assert!(res.is_err());
        assert_eq!(timeout_calls.load(Ordering::Relaxed), 1);
        assert_eq!(op_calls.load(Ordering::Relaxed), 1);
    }
}
