use std::{any::Any, future::Future};

use futures::FutureExt;
use tokio::task::{self, JoinHandle};

use std::{panic::AssertUnwindSafe, process};

fn log_panic_info(err: Box<dyn Any + Send>) {
    if let Some(s) = err.downcast_ref::<&'static str>() {
        tracing::error!("Task panicked with message: {}", s);
    } else if let Some(s) = err.downcast_ref::<String>() {
        tracing::error!("Task panicked with message: {}", s);
    } else {
        tracing::error!("Task panicked with non-string payload.");
    }
}

pub fn logged_task<F, T>(fut: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    task::spawn(async {
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(result) => result,
            Err(e) => {
                log_panic_info(e);
                process::abort();
            }
        }
    })
}
