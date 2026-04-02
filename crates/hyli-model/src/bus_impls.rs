//! BusMessage implementations for hyli-model types
//!
//! This module provides BusMessage trait implementations for types defined in hyli-model,
//! allowing them to be used with the hyli-bus message bus system.
//!
//! This is gated behind the "bus" feature to avoid pulling in unnecessary dependencies
//! when hyli-model is used in constrained environments.

use hyli_bus::{BusMessage, LOW_CAPACITY};

use crate::{DataEvent, MempoolBlockEvent, MempoolStatusEvent, NodeStateEvent};

impl BusMessage for NodeStateEvent {
    const CAPACITY: usize = LOW_CAPACITY; // Lowered, large data type
}

impl BusMessage for DataEvent {
    const CAPACITY: usize = LOW_CAPACITY; // Lowered, large data type
}

impl BusMessage for MempoolBlockEvent {
    const CAPACITY: usize = LOW_CAPACITY; // Lowered, large data type
}

impl BusMessage for MempoolStatusEvent {
    const CAPACITY: usize = LOW_CAPACITY; // Lowered, large data type
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bus_channel_capacity() {
        // Check rust does what we think.
        assert_eq!(
            <MempoolStatusEvent as BusMessage>::CAPACITY_IF_WAITING,
            LOW_CAPACITY - 10
        );
    }
}
