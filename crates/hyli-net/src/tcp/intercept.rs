//! Hooks for dropping application-level frames in tests.
//!
//! Example (requires the `turmoil` feature on `hyli-net`):
//! ```rust,ignore
//! use hyli_net::tcp::intercept::set_message_intercept_scoped;
//!
//! let _guard = set_message_intercept_scoped(|bytes| bytes.starts_with(b"drop"));
//! // run test traffic
//! ```

use std::cell::RefCell;

type MessageHook = Box<dyn FnMut(&[u8]) -> bool + Send>;
thread_local! {
    static LOCAL_MESSAGE_HOOK: RefCell<Option<MessageHook>> = const { RefCell::new(None) };
}

/// Install a hook used by tests to drop application-level frames.
pub fn set_message_intercept<F>(hook: F)
where
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    LOCAL_MESSAGE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

/// Clear the previously installed hook.
pub fn clear_message_intercept() {
    LOCAL_MESSAGE_HOOK.with(|slot| {
        *slot.borrow_mut() = None;
    });
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

pub(crate) fn should_drop(bytes: &[u8]) -> bool {
    LOCAL_MESSAGE_HOOK.with(|slot| {
        let mut guard = slot.borrow_mut();
        guard.as_mut().map(|hook| hook(bytes)).unwrap_or(false)
    })
}
