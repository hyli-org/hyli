//! BusMessage implementations for hyli-net types
//!
//! This module provides BusMessage trait implementations for types defined in hyli-net,
//! allowing them to be used with the hyli-bus message bus system.
//!
//! This is gated behind the "bus" feature to avoid pulling in unnecessary dependencies.

use hyli_bus::BusMessage;

use crate::api::tcp::TcpServerMessage;

impl BusMessage for TcpServerMessage {}
