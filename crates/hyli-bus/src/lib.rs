//! # hyli-bus — Modular Monolith with Microservice Advantages
//!
//! `hyli-bus` is an async, in-process message bus that lets you structure your application
//! as a set of loosely-coupled **modules** (think micro-services) while keeping them all in
//! a single binary (think monolith).
//!
//! ## Design goals
//!
//! | Goal | Approach |
//! |------|----------|
//! | Clear module boundaries | Each module declares its message contract with [`module_bus_client!`] |
//! | Simple operations | Everything runs in one binary — no network, no service discovery |
//! | Compile-time safety | Rust's type system enforces that senders and receivers agree on types |
//! | No serialisation overhead | Messages are cloned in-process, not marshalled to bytes |
//! | Easy testing | Spin up only the module(s) under test and inject typed events directly |
//!
//! ## Quick start
//!
//! ### 1. Define your message types
//!
//! Any type that implements [`BusMessage`] can travel on the bus.
//!
//! ```rust,ignore
//! #[derive(Clone)]
//! struct OrderPlaced { order_id: u64 }
//!
//! #[derive(Clone)]
//! struct QueryNextBatch;
//!
//! #[derive(Clone)]
//! struct Batch(Vec<u64>);
//!
//! impl hyli_bus::BusMessage for OrderPlaced {}
//! impl hyli_bus::BusMessage for QueryNextBatch {}
//! impl hyli_bus::BusMessage for Batch {}
//! ```
//!
//! ### 2. Declare a module's contract
//!
//! [`module_bus_client!`] generates a strongly-typed struct that owns exactly the
//! senders and receivers declared — nothing more, nothing less.
//!
//! ```rust,ignore
//! use hyli_bus::module_bus_client;
//!
//! module_bus_client! {
//!     struct ProcessorBusClient {
//!         sender(OrderPlaced),          // events this module emits
//!         receiver(OrderPlaced),        // events this module consumes
//!         query(QueryNextBatch, Batch), // synchronous request/response
//!     }
//! }
//! ```
//!
//! `ShutdownModule` and `PersistModule` receivers are added automatically by the macro.
//!
//! ### 3. Implement the module
//!
//! ```rust,ignore
//! use hyli_bus::{Module, SharedMessageBus, module_handle_messages};
//!
//! struct Processor {
//!     bus: ProcessorBusClient,
//!     // ... your state
//! }
//!
//! impl Module for Processor {
//!     type Context = (); // build-time configuration
//!
//!     async fn build(bus: SharedMessageBus, _ctx: ()) -> anyhow::Result<Self> {
//!         Ok(Self { bus: ProcessorBusClient::new_from_bus(bus).await })
//!     }
//!
//!     async fn run(&mut self) -> anyhow::Result<()> {
//!         module_handle_messages! {
//!             on_self self,
//!
//!             listen<OrderPlaced> ev => { /* handle event */ }
//!
//!             command_response<QueryNextBatch, Batch> q => {
//!                 Ok(Batch(vec![]))
//!             }
//!         }
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ### 4. Wire it all together
//!
//! ```rust,ignore
//! use hyli_bus::{SharedMessageBus, ModulesHandler, ModulesHandlerOptions};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let bus = SharedMessageBus::new();
//!     let mut handler = ModulesHandler::new(&bus, "data".into(), ModulesHandlerOptions::default())?;
//!
//!     // Build all modules before starting — guarantees no message is lost.
//!     handler.build_module::<Processor>(()).await?;
//!
//!     handler.start_modules().await?;
//!     // Run until SIGINT / SIGTERM.
//!     handler.exit_process().await
//! }
//! ```
//!
//! ## Architecture overview
//!
//! ```text
//!  ┌────────────────────────────────────────────────┐
//!  │               SharedMessageBus                 │
//!  │   Arc<Mutex<AnyMap<broadcast::Sender<M>>>>     │
//!  └─────────┬──────────────────────────┬───────────┘
//!            │ subscribe                │ subscribe
//!  ┌─────────▼──────────┐   ┌──────────▼──────────┐
//!  │   Module A         │   │   Module B           │
//!  │  (MempoolBusClient)│   │  (ConsensusBusClient)│
//!  │   sender(EventA)   │──▶│   receiver(EventA)   │
//!  └────────────────────┘   └─────────────────────-┘
//! ```
//!
//! - Each message type gets one [`tokio::sync::broadcast`] channel, created on first use.
//! - Cloning a bus handle gives access to the same underlying channels.
//! - All communication is in-process: zero network hops, zero serialisation.
//!
//! ## Modules
//!
//! - [`bus`] — Core bus, traits, [`bus_client!`], [`handle_messages!`]
//! - [`bus::command_response`] — Request/response pattern via [`Query`](bus::command_response::Query)
//! - [`modules`] — [`Module`] trait, [`ModulesHandler`], shutdown signals
//! - [`utils`] — Logging macros, profiling, static type maps, checksummed persistence

pub mod bus;
pub mod modules;
pub mod utils;

pub use opentelemetry::KeyValue;

// Re-export commonly used types and functions
pub use bus::{
    BusClientReceiver, BusClientSender, BusEnvelope, BusMessage, BusReceiver, BusSender,
    SharedMessageBus, DEFAULT_CAPACITY, LOW_CAPACITY,
};
pub use modules::{Module, ModulesHandler, ShutdownClient};

// Macros are automatically exported at crate root via #[macro_export]
// Available macros: bus_client, handle_messages, info_span_ctx,
// log_debug, log_error, log_warn, module_bus_client, module_handle_messages
