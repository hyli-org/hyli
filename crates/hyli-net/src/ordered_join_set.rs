use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// A JoinSet that maintains the order of tasks.
/// Tasks are processed in FIFO order, and only one task can be processed at a time.
pub struct OrderedJoinSet<T> {
    tasks: VecDeque<JoinHandle<T>>,
}

impl<T> OrderedJoinSet<T> {
    pub fn new() -> Self {
        Self {
            tasks: VecDeque::new(),
        }
    }

    /// Adds a new task to the set.
    /// Tasks are awaited in a FIFO order.
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_on(task, &Handle::current())
    }

    /// Adds a new task to the set, spawning it on the specified runtime handle.
    /// Tasks are awaited in a FIFO order.
    pub fn spawn_on<F>(&mut self, task: F, handle: &Handle)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = handle.spawn(task);
        self.tasks.push_back(handle);
    }

    /// Returns true if the set contains no tasks.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns the number of tasks in the set.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Waits for the next task in order to complete.
    /// Returns None if there are no tasks in the set.
    pub async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        std::future::poll_fn(|cx| self.poll_join_next(cx)).await
    }

    /// Polls for the next completed task in order.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, tokio::task::JoinError>>> {
        match self.tasks.front_mut() {
            Some(task) => {
                if let Poll::Ready(result) = Pin::new(task).poll(cx) {
                    self.tasks.pop_front();
                    return Poll::Ready(Some(result));
                }
                // Not ready, try again next time
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }

    /// Aborts all tasks in the set.
    pub fn abort_all(&mut self) {
        while let Some(task) = self.tasks.pop_front() {
            task.abort();
        }
    }
}

impl<T> Default for OrderedJoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for OrderedJoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_ordered_join_set() {
        let mut set = OrderedJoinSet::new();

        // Spawn tasks that complete in reverse order
        set.spawn(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            1
        });
        set.spawn(async { 2 });
        set.spawn(async { 3 });

        // Tasks should complete in order despite different completion times
        assert_eq!(set.join_next().await.unwrap().unwrap(), 1);
        assert_eq!(set.join_next().await.unwrap().unwrap(), 2);
        assert_eq!(set.join_next().await.unwrap().unwrap(), 3);
        assert!(set.join_next().await.is_none());
    }

    #[tokio::test]
    async fn test_empty_set() {
        let mut set = OrderedJoinSet::<i32>::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert!(set.join_next().await.is_none());
    }

    #[tokio::test]
    async fn test_abort_all() {
        let mut set = OrderedJoinSet::new();

        set.spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            1
        });
        set.spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            2
        });

        assert_eq!(set.len(), 2);
        set.abort_all();
        assert!(set.is_empty());
    }
}
