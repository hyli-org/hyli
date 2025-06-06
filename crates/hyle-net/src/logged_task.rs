use std::future::Future;

use tokio::task::{self, JoinHandle};
use tracing::error;

/// Takes a task that is awaited in another task and logs a panic if it occurs.
pub fn logged_task<F>(task: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    task::spawn(async move {
        let result = task::spawn(task).await;

        match result {
            Ok(res) => res,
            Err(e) => {
                error!("Task failed to complete: {:?}", e);
                panic!("Task failed to complete: {:?}", e);
            }
        }
    })
}
