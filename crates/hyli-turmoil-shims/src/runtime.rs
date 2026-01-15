use std::{mem::ManuallyDrop, time::Duration};

use tokio::runtime::{Handle, Runtime};

enum LongTasksRuntimeInner {
    Dedicated(ManuallyDrop<Runtime>),
    #[cfg(feature = "turmoil")]
    Current(Handle),
}

pub struct LongTasksRuntime(LongTasksRuntimeInner);

impl Default for LongTasksRuntime {
    fn default() -> Self {
        #[cfg(feature = "turmoil")]
        {
            return Self(LongTasksRuntimeInner::Current(Handle::current()));
        }

        #[cfg(not(feature = "turmoil"))]
        {
            return Self::build_dedicated(DEFAULT_THREADS, DEFAULT_THREAD_NAME);
        }
    }
}

impl Drop for LongTasksRuntime {
    fn drop(&mut self) {
        match &mut self.0 {
            LongTasksRuntimeInner::Dedicated(runtime) => {
                // Shut down the hashing runtime.
                // TODO: serialize?
                // Safety: We'll manually drop the runtime below and it won't be double-dropped as we use ManuallyDrop.
                let rt = unsafe { ManuallyDrop::take(runtime) };
                // This has to be done outside the current runtime.
                tokio::task::spawn_blocking(move || {
                    #[cfg(test)]
                    rt.shutdown_timeout(Duration::from_millis(10));
                    #[cfg(not(test))]
                    rt.shutdown_timeout(Duration::from_secs(10));
                });
            }
            #[cfg(feature = "turmoil")]
            LongTasksRuntimeInner::Current(_) => {}
        }
    }
}

impl LongTasksRuntime {
    pub fn new(threads: usize, thread_name: impl Into<String>) -> Self {
        #[cfg(feature = "turmoil")]
        {
            let _ = (threads, thread_name.into());
            return Self(LongTasksRuntimeInner::Current(Handle::current()));
        }

        #[cfg(not(feature = "turmoil"))]
        {
            let thread_name = thread_name.into();
            return Self::build_dedicated(threads, &thread_name);
        }
    }

    pub fn handle(&self) -> Handle {
        match &self.0 {
            LongTasksRuntimeInner::Dedicated(runtime) => runtime.handle().clone(),
            #[cfg(feature = "turmoil")]
            LongTasksRuntimeInner::Current(handle) => handle.clone(),
        }
    }

    #[cfg(not(feature = "turmoil"))]
    fn build_dedicated(threads: usize, thread_name: &str) -> Self {
        Self(LongTasksRuntimeInner::Dedicated(ManuallyDrop::new(
            #[allow(clippy::expect_used, reason = "Fails at startup, is OK")]
            tokio::runtime::Builder::new_multi_thread()
                // Limit the number of threads arbitrarily to lower the maximal impact on the whole node
                .worker_threads(threads)
                .thread_name(thread_name)
                .build()
                .expect("Failed to create hashing runtime"),
        )))
    }
}
const DEFAULT_THREADS: usize = 3;
const DEFAULT_THREAD_NAME: &str = "mempool-hashing";
