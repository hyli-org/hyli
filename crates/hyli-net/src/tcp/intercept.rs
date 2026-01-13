//! Hooks for dropping or corrupting application-level frames in tests.
//!
//! Example (requires the `turmoil` feature on `hyli-net`):
//! ```rust,ignore
//! use hyli_net::tcp::intercept::set_message_intercept_scoped;
//!
//! // Drop messages starting with "drop"
//! let _guard = set_message_intercept_scoped(|bytes| bytes.starts_with(b"drop"));
//! // run test traffic
//! ```
//!
//! For corruption:
//! ```rust,ignore
//! use hyli_net::tcp::intercept::set_message_corrupt_scoped;
//! use bytes::Bytes;
//!
//! // Corrupt messages by flipping bits
//! let _guard = set_message_corrupt_scoped(|bytes| {
//!     let mut corrupted = bytes.to_vec();
//!     corrupted[0] ^= 0xFF; // flip bits in first byte
//!     Some(Bytes::from(corrupted))
//! });
//! ```

use bytes::Bytes;
use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};

type MessageHook = Box<dyn FnMut(&[u8]) -> bool + Send>;
type CorruptHook = Box<dyn FnMut(&[u8]) -> Option<Bytes> + Send>;

static MESSAGE_HOOK: OnceLock<Mutex<Option<MessageHook>>> = OnceLock::new();
static CORRUPT_HOOK: OnceLock<Mutex<Option<CorruptHook>>> = OnceLock::new();
thread_local! {
    static LOCAL_MESSAGE_HOOK: RefCell<Option<MessageHook>> = const { RefCell::new(None) };
    static LOCAL_CORRUPT_HOOK: RefCell<Option<CorruptHook>> = const { RefCell::new(None) };
}

fn hook_slot() -> &'static Mutex<Option<MessageHook>> {
    MESSAGE_HOOK.get_or_init(|| Mutex::new(None))
}

fn corrupt_hook_slot() -> &'static Mutex<Option<CorruptHook>> {
    CORRUPT_HOOK.get_or_init(|| Mutex::new(None))
}

/// Install a hook used by tests to drop application-level frames.
pub fn set_message_intercept<F>(hook: F)
where
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    let mut guard = hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = Some(Box::new(hook));
}

/// Clear the previously installed hook.
pub fn clear_message_intercept() {
    let mut guard = hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = None;
}

/// Install a hook scoped to the current thread, cleared on drop.
pub fn set_message_intercept_scoped<F>(hook: F) -> MessageInterceptGuard
where
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    LOCAL_MESSAGE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    MessageInterceptGuard { _private: () }
}

/// Clear the scoped hook on the current thread.
pub fn clear_message_intercept_scoped() {
    LOCAL_MESSAGE_HOOK.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub struct MessageInterceptGuard {
    _private: (),
}

impl Drop for MessageInterceptGuard {
    fn drop(&mut self) {
        clear_message_intercept_scoped();
    }
}

/// Install a hook used by tests to corrupt application-level frames.
/// The hook receives the original bytes and returns `Some(corrupted_bytes)` to corrupt,
/// or `None` to leave the message unchanged.
pub fn set_message_corrupt<F>(hook: F)
where
    F: FnMut(&[u8]) -> Option<Bytes> + Send + 'static,
{
    let mut guard = corrupt_hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = Some(Box::new(hook));
}

/// Clear the previously installed corruption hook.
pub fn clear_message_corrupt() {
    let mut guard = corrupt_hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = None;
}

/// Install a corruption hook scoped to the current thread, cleared on drop.
pub fn set_message_corrupt_scoped<F>(hook: F) -> MessageCorruptGuard
where
    F: FnMut(&[u8]) -> Option<Bytes> + Send + 'static,
{
    LOCAL_CORRUPT_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    MessageCorruptGuard { _private: () }
}

/// Clear the scoped corruption hook on the current thread.
pub fn clear_message_corrupt_scoped() {
    LOCAL_CORRUPT_HOOK.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub struct MessageCorruptGuard {
    _private: (),
}

impl Drop for MessageCorruptGuard {
    fn drop(&mut self) {
        clear_message_corrupt_scoped();
    }
}

/// Check if a message should be corrupted. Returns `Some(corrupted_bytes)` if so,
/// or `None` to leave the message unchanged.
pub(crate) fn maybe_corrupt(bytes: &[u8]) -> Option<Bytes> {
    if let Some(result) = LOCAL_CORRUPT_HOOK.with(|slot| {
        let mut guard = slot.borrow_mut();
        guard.as_mut().and_then(|hook| hook(bytes))
    }) {
        return Some(result);
    }

    let mut guard = corrupt_hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match guard.as_mut() {
        Some(hook) => hook(bytes),
        None => None,
    }
}

pub(crate) fn should_drop(bytes: &[u8]) -> bool {
    if let Some(result) = LOCAL_MESSAGE_HOOK.with(|slot| {
        let mut guard = slot.borrow_mut();
        guard.as_mut().map(|hook| hook(bytes))
    }) {
        return result;
    }

    let mut guard = hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match guard.as_mut() {
        Some(hook) => hook(bytes),
        None => false,
    }
}
