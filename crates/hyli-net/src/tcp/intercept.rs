//! Hooks for dropping or corrupting application-level frames in tests.
//!
//! Example (requires the `turmoil` feature on `hyli-net`):
//! ```rust,ignore
//! use hyli_net::tcp::intercept::{set_message_hook_scoped, MessageAction};
//!
//! // Drop messages starting with "drop"
//! let _guard = set_message_hook_scoped(|bytes| {
//!     if bytes.starts_with(b"drop") {
//!         MessageAction::Drop
//!     } else {
//!         MessageAction::Pass
//!     }
//! });
//! // run test traffic
//! ```
//!
//! For corruption:
//! ```rust,ignore
//! use hyli_net::tcp::intercept::{set_message_hook_scoped, MessageAction};
//! use bytes::Bytes;
//!
//! // Corrupt messages by flipping bits
//! let _guard = set_message_hook_scoped(|bytes| {
//!     let mut corrupted = bytes.to_vec();
//!     corrupted[0] ^= 0xFF; // flip bits in first byte
//!     MessageAction::Replace(Bytes::from(corrupted))
//! });
//! ```

use bytes::Bytes;
use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};

/// The action to take on a message after inspection.
#[derive(Debug, Clone)]
pub enum MessageAction {
    /// Let the message through unchanged.
    Pass,
    /// Drop the message entirely.
    Drop,
    /// Replace the message with different bytes (corruption).
    Replace(Bytes),
}

pub type MessageHook = Box<dyn FnMut(&[u8]) -> MessageAction + Send>;

static MESSAGE_HOOK: OnceLock<Mutex<Option<MessageHook>>> = OnceLock::new();
thread_local! {
    static LOCAL_MESSAGE_HOOK: RefCell<Option<MessageHook>> = const { RefCell::new(None) };
}

fn hook_slot() -> &'static Mutex<Option<MessageHook>> {
    MESSAGE_HOOK.get_or_init(|| Mutex::new(None))
}

/// Install a hook used by tests to intercept application-level frames.
/// The hook can drop, corrupt, or pass through messages.
pub fn set_message_hook<F>(hook: F)
where
    F: FnMut(&[u8]) -> MessageAction + Send + 'static,
{
    let mut guard = hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = Some(Box::new(hook));
}

/// Clear the previously installed hook.
pub fn clear_message_hook() {
    let mut guard = hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = None;
}

/// Install a hook scoped to the current thread, cleared on drop.
pub fn set_message_hook_scoped<F>(hook: F) -> MessageHookGuard
where
    F: FnMut(&[u8]) -> MessageAction + Send + 'static,
{
    LOCAL_MESSAGE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    MessageHookGuard { _private: () }
}

/// Clear the scoped hook on the current thread.
pub fn clear_message_hook_scoped() {
    LOCAL_MESSAGE_HOOK.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub struct MessageHookGuard {
    _private: (),
}

impl Drop for MessageHookGuard {
    fn drop(&mut self) {
        clear_message_hook_scoped();
    }
}

/// Check what action should be taken on a message.
/// Returns the action from the hook, or `MessageAction::Pass` if no hook is set.
pub(crate) fn intercept_message(bytes: &[u8]) -> MessageAction {
    // Check thread-local hook first
    if let Some(action) = LOCAL_MESSAGE_HOOK.with(|slot| {
        let mut guard = slot.borrow_mut();
        guard.as_mut().map(|hook| hook(bytes))
    }) {
        return action;
    }

    // Fall back to global hook
    let mut guard = hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match guard.as_mut() {
        Some(hook) => hook(bytes),
        None => MessageAction::Pass,
    }
}
