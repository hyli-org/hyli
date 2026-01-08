//! Hooks for dropping application-level frames in tests.
//!
//! Example (requires the `turmoil` feature on `hyli-net`):
//! ```rust,ignore
//! use hyli_net::tcp::intercept::{clear_message_intercept, set_message_intercept};
//!
//! set_message_intercept(|bytes| bytes.starts_with(b"drop"));
//! // run test traffic
//! clear_message_intercept();
//! ```

use std::sync::{Mutex, OnceLock};

type MessageHook = Box<dyn FnMut(&[u8]) -> bool + Send>;

static MESSAGE_HOOK: OnceLock<Mutex<Option<MessageHook>>> = OnceLock::new();

fn hook_slot() -> &'static Mutex<Option<MessageHook>> {
    MESSAGE_HOOK.get_or_init(|| Mutex::new(None))
}

/// Install a hook used by tests to drop application-level frames.
pub fn set_message_intercept<F>(hook: F)
where
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    let mut guard = hook_slot().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = Some(Box::new(hook));
}

/// Clear the previously installed hook.
pub fn clear_message_intercept() {
    let mut guard = hook_slot().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = None;
}

pub(crate) fn should_drop(bytes: &[u8]) -> bool {
    let mut guard = hook_slot().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    match guard.as_mut() {
        Some(hook) => hook(bytes),
        None => false,
    }
}
