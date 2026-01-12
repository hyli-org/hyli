//! Message bus and module system for Hyli
//!
//! This crate provides:
//! - A type-safe message bus for asynchronous communication between components
//! - A module system for managing long-running services
//! - Supporting utilities for logging, profiling, and type-safe access patterns

pub mod bus;
pub mod modules;
pub mod utils;

// Re-export commonly used types and functions
pub use bus::{
    BusClientReceiver, BusClientSender, BusEnvelope, BusMessage, BusReceiver, BusSender,
    SharedMessageBus, DEFAULT_CAPACITY, LOW_CAPACITY,
};
pub use modules::{Module, ModulesHandler, ShutdownClient};

// Macros are automatically exported at crate root via #[macro_export]
// Available macros: bus_client, handle_messages, info_span_ctx,
// log_debug, log_error, log_warn, module_bus_client, module_handle_messages
