use std::{
    any::type_name,
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io::{BufWriter, ErrorKind, Write},
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use crate::{
    bus::{BusClientReceiver, BusClientSender, SharedMessageBus},
    bus_client, handle_messages, log_error, log_warn,
};
use anyhow::{bail, Context, Error, Result};
use rand::{distr::Alphanumeric, Rng};
use tracing::{debug, info, warn};

use crate::utils::checksums::{self, ChecksumReader, ChecksumWriter};
use crate::utils::deterministic_rng::deterministic_rng;

pub use crate::utils::checksums::{
    format_checksum, manifest_path, write_manifest, PersistenceError, CHECKSUMS_MANIFEST,
};

const MODULE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MODULE_ID_SEPARATOR: char = '#';
const PERSISTENCE_FAILURES_LOG: &str = "persistence_failures.log";

#[cfg(target_os = "linux")]
fn is_mount_point(path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::msg("data_dir has no parent"))?;
    let parent_meta = fs::metadata(parent)?;
    Ok(meta.dev() != parent_meta.dev())
}

#[cfg(not(target_os = "linux"))]
fn is_mount_point(_path: &Path) -> Result<bool> {
    Ok(false)
}

/// Files saved to disk by [`Module::persist`], paired with their CRC32 checksums.
///
/// The [`ModulesHandler`] collects these entries from all modules during a clean
/// shutdown and writes them to a [`CHECKSUMS_MANIFEST`] file. On the next startup,
/// [`Module::load_from_disk`] uses the manifest to verify file integrity before
/// deserialising.
pub type ModulePersistOutput = Vec<(PathBuf, u32)>;

/// A long-running component of the application.
///
/// A `Module` is the hyli-bus equivalent of a micro-service. It:
/// - has its own async event loop ([`run`](Module::run))
/// - declares which messages it sends and receives via [`module_bus_client!`](crate::module_bus_client)
/// - can optionally persist state to disk ([`persist`](Module::persist))
/// - responds to lifecycle signals ([`ShutdownModule`](signal::ShutdownModule),
///   [`PersistModule`](signal::PersistModule)) automatically via
///   [`module_handle_messages!`](crate::module_handle_messages)
///
/// # Lifecycle
///
/// ```text
///  ModulesHandler::build_module::<M>(ctx)
///       │
///       ▼
///  M::build(bus, ctx)  ← set up subscriptions, load state from disk
///       │
///       ▼  (all modules built before this point)
///  ModulesHandler::start_modules()
///       │
///       ▼
///  M::run()            ← event loop, runs until ShutdownModule received
///       │
///       ▼
///  M::persist()        ← save state, return checksums
/// ```
///
/// # Implementing a module
///
/// ```rust,ignore
/// use hyli_bus::{Module, SharedMessageBus, module_bus_client, module_handle_messages};
///
/// module_bus_client! {
///     struct MyModuleBusClient {
///         sender(MyEvent),
///         receiver(OtherEvent),
///     }
/// }
///
/// struct MyModule {
///     bus: MyModuleBusClient,
///     state: u64,
/// }
///
/// impl Module for MyModule {
///     type Context = ();
///
///     async fn build(bus: SharedMessageBus, _ctx: ()) -> anyhow::Result<Self> {
///         Ok(Self {
///             bus: MyModuleBusClient::new_from_bus(bus).await,
///             state: 0,
///         })
///     }
///
///     async fn run(&mut self) -> anyhow::Result<()> {
///         module_handle_messages! {
///             on_self self,
///             listen<OtherEvent> ev => {
///                 self.state += 1;
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait Module
where
    Self: Sized,
{
    /// Module-specific build-time configuration.
    ///
    /// This is passed to [`build`](Module::build) by [`ModulesHandler::build_module`].
    /// Use it for anything that isn't available via the bus (CLI flags, config files, …).
    type Context;

    /// Construct the module and subscribe to the bus.
    ///
    /// Called once by [`ModulesHandler::build_module`] before any module is started.
    /// This is the right place to subscribe to channels and optionally restore persisted
    /// state with [`load_from_disk`](Module::load_from_disk).
    fn build(
        bus: SharedMessageBus,
        ctx: Self::Context,
    ) -> impl futures::Future<Output = Result<Self>> + Send;

    /// Run the module's event loop until a shutdown signal is received.
    ///
    /// Implement this with [`module_handle_messages!`](crate::module_handle_messages),
    /// which automatically handles [`ShutdownModule`](signal::ShutdownModule) and
    /// [`PersistModule`](signal::PersistModule) signals.
    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send;

    /// Load data from disk with checksum verification from manifest.
    /// `file` is relative to `data_dir`.
    ///
    /// Returns:
    /// - `Ok(Some(data))` if file exists and checksum verifies (or no manifest for backwards compat)
    /// - `Ok(None)` if file doesn't exist and no manifest entry exists
    /// - `Err` with `PersistenceError::FileMissingOnDisk` if file is in manifest but missing on disk
    /// - `Err` with `PersistenceError::FileNotInManifest` if file exists but isn't in manifest
    /// - `Err` with `PersistenceError::ChecksumMismatch` if checksum verification fails
    /// - `Err` with `PersistenceError::DeserializationFailed` if data is corrupted
    fn load_from_disk<S>(data_dir: &Path, file: &Path) -> Result<Option<S>>
    where
        S: borsh::BorshDeserialize,
    {
        if file.is_absolute() {
            bail!("Persisted file path must be relative to data_dir");
        }
        let full_path = data_dir.join(file);

        let data_file = match fs::File::open(&full_path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Missing file: only acceptable if manifest doesn't list it.
                match checksums::read_checksum_from_manifest(data_dir, file) {
                    Ok(Some(_)) => {
                        return Err(PersistenceError::FileMissingOnDisk(full_path))
                            .with_context(|| format!("Module {}", type_name::<S>()));
                    }
                    Ok(None) => {
                        return Ok(None);
                    }
                    Err(PersistenceError::FileNotInManifest(_)) => {
                        // File is absent and not listed in manifest: treat as optional state.
                        return Ok(None);
                    }
                    Err(e) => {
                        return Err(e).with_context(|| format!("Module {}", type_name::<S>()))
                    }
                }
            }
            Err(e) => {
                return Err(PersistenceError::IoError(e))
                    .with_context(|| format!("Module {}", type_name::<S>()))
            }
        };

        // Deserialize while computing checksum to avoid a second read.
        let mut checksum_reader = ChecksumReader::new(data_file);
        let deserialized: S = borsh::from_reader(&mut checksum_reader)
            .with_context(|| format!("Deserializing module {}", type_name::<S>()))?;
        checksum_reader
            .drain_to_end()
            .with_context(|| format!("Module {}", type_name::<S>()))?;

        let actual_checksum = checksum_reader.checksum();
        // Manifest is required if the file exists; it is the source of truth.
        let expected_checksum = match checksums::read_checksum_from_manifest(data_dir, file) {
            Ok(Some(expected_checksum)) => expected_checksum,
            Ok(None) => {
                tracing::error!(
                    "No manifest found for {}, refusing to load persisted file",
                    full_path.to_string_lossy()
                );
                return Err(PersistenceError::FileNotInManifest(file.to_path_buf()))
                    .with_context(|| format!("Module {}", type_name::<S>()));
            }
            Err(e) => {
                return Err(e).with_context(|| format!("Module {}", type_name::<S>()));
            }
        };

        if expected_checksum != actual_checksum {
            tracing::error!(
                "Checksum mismatch for {}: expected {}, got {}",
                full_path.to_string_lossy(),
                format_checksum(expected_checksum),
                format_checksum(actual_checksum)
            );
            return Err(PersistenceError::ChecksumMismatch {
                expected: format_checksum(expected_checksum),
                actual: format_checksum(actual_checksum),
            })
            .with_context(|| format!("Module {}", type_name::<S>()));
        }
        debug!(
            "Checksum verification passed for {}",
            file.to_string_lossy()
        );

        info!("Loaded data from disk {}", full_path.to_string_lossy());
        Ok(Some(deserialized))
    }

    /// Persist module state to disk.
    ///
    /// Called automatically by [`module_handle_messages!`](crate::module_handle_messages) when a
    /// [`PersistModule`](signal::PersistModule) signal is received, and once more by the
    /// [`ModulesHandler`] after [`run`](Module::run) returns.
    ///
    /// The default implementation logs a "not implemented" message and returns an empty list.
    /// Override it and call [`save_on_disk`](Module::save_on_disk) to opt in to persistence.
    ///
    /// Returns a list of `(relative_path, crc32_checksum)` pairs for every file written.
    /// These are collected by [`ModulesHandler`] and written to the manifest.
    fn persist(&mut self) -> impl futures::Future<Output = Result<ModulePersistOutput>> + Send {
        async {
            info!(
                "Persistence is not implemented for module {}",
                type_name::<Self>()
            );
            Ok(Vec::new())
        }
    }

    /// Atomically serialize `store` to `data_dir/file` and return the CRC32 checksum.
    ///
    /// The write is atomic: data is first written to a temporary file, then renamed
    /// into place. The CRC32 checksum is computed while writing so no extra I/O pass
    /// is needed.
    ///
    /// `file` **must** be a relative path. It will be joined with `data_dir`.
    ///
    /// The returned checksum must be included in the value returned by [`persist`](Module::persist)
    /// so that [`ModulesHandler`] can write it to the manifest.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// async fn persist(&mut self) -> anyhow::Result<ModulePersistOutput> {
    ///     let file = Path::new(files::NODE_STATE_BIN);
    ///     let checksum = Self::save_on_disk(&self.data_dir, file, &self.state)?;
    ///     Ok(vec![(file.to_path_buf(), checksum)])
    /// }
    /// ```
    fn save_on_disk<S>(data_dir: &Path, file: &Path, store: &S) -> Result<u32>
    where
        S: borsh::BorshSerialize,
    {
        if file.is_absolute() {
            bail!("Persisted file path must be relative to data_dir");
        }
        let full_path = data_dir.join(file);

        // TODO/FIXME: Concurrent writes can happen, and an older state can override a newer one
        // Example:
        // State 1 starts creating a tmp file data.state1.tmp
        // State 2 starts creating a tmp file data.state2.tmp
        // rename data.state2.tmp into store (atomic override)
        // rename data.state1.tmp into

        let salt: String = deterministic_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        let tmp_data = full_path.with_extension(format!("{salt}.tmp"));

        debug!("Saving on disk in a tmp file {:?}", tmp_data.clone());

        // Write data to temp file
        let buf_writer = BufWriter::new(log_error!(
            fs::File::create(tmp_data.as_path()),
            "Create data file"
        )?);
        let mut checksum_writer = ChecksumWriter::new(buf_writer);
        log_error!(
            borsh::to_writer(&mut checksum_writer, store),
            "Serializing {}",
            type_name::<S>()
        )?;
        log_error!(
            checksum_writer.flush(),
            "Flushing Buffer writer for store {}",
            type_name::<S>()
        )?;

        let checksum = checksum_writer.checksum();

        // Atomically rename data file
        debug!("Renaming {:?} to {:?}", &tmp_data, &full_path);
        log_error!(fs::rename(&tmp_data, &full_path), "Rename data file")?;

        Ok(checksum)
    }
}

/// Well-known file names used for module persistence.
///
/// Pass these to [`Module::save_on_disk`] and [`Module::load_from_disk`].
pub mod files {
    /// Default persistence file for `NodeState`.
    pub const NODE_STATE_BIN: &str = "node_state.bin";
    /// Default persistence file for `Consensus`.
    pub const CONSENSUS_BIN: &str = "consensus.bin";
}

type ModuleStarterFuture =
    Pin<Box<dyn Future<Output = Result<ModulePersistOutput, Error>> + Send + 'static>>;

struct ModuleStarter {
    pub name: &'static str,
    starter: ModuleStarterFuture,
}

/// Lifecycle signals broadcast on the bus to coordinate module shutdown and persistence.
///
/// These are added automatically to every client generated by [`module_bus_client!`](crate::module_bus_client)
/// and handled by [`module_handle_messages!`](crate::module_handle_messages).
/// You generally don't need to use them directly.
pub mod signal {
    use std::any::TypeId;

    use crate::{bus::BusMessage, modules::ModuleShutdownStatus, utils::static_type_map::Pick};

    /// Broadcast by [`ModulesHandler`](super::ModulesHandler) to ask modules to flush
    /// state to disk without stopping.
    ///
    /// [`module_handle_messages!`](crate::module_handle_messages) calls [`Module::persist`](super::Module::persist)
    /// automatically when this signal is received.
    #[derive(Clone, Debug)]
    pub struct PersistModule {}

    /// Broadcast by [`ModulesHandler`](super::ModulesHandler) to ask a specific module to stop.
    ///
    /// Only the module whose Rust type name matches [`module`](ShutdownModule::module) will act on this;
    /// others ignore it.
    #[derive(Clone, Debug)]
    pub struct ShutdownModule {
        /// Fully-qualified Rust type name of the module to shut down
        /// (e.g. `"my_crate::node::NodeState"`).
        pub module: String,
    }

    /// Sent by each module's watchdog task after the module has exited.
    ///
    /// [`ModulesHandler::shutdown_loop`](super::ModulesHandler::shutdown_loop) listens for
    /// this signal to track shutdown progress and trigger the next module in the sequence.
    #[derive(Clone, Debug)]
    pub struct ShutdownCompleted {
        pub module: String,
        pub shutdown_status: ModuleShutdownStatus,
    }

    impl BusMessage for PersistModule {}
    impl BusMessage for ShutdownModule {}
    impl BusMessage for ShutdownCompleted {}

    /// Execute a future, cancelling it if a shutdown signal is received.
    pub async fn shutdown_aware<M: 'static, F>(
        receiver: &mut impl Pick<crate::bus::BusReceiver<crate::modules::signal::ShutdownModule>>,
        f: F,
    ) -> anyhow::Result<F::Output>
    where
        F: std::future::IntoFuture,
    {
        let mut dummy = false;
        tokio::select! {
            _ = async_receive_shutdown::<M>(
                &mut dummy,
                receiver.get_mut(),
            ) => {
                anyhow::bail!("Shutdown received");
            }
            res = f => {
                Ok(res)
            }
        }
    }

    /// Execute a future, cancelling it if a shutdown signal is received or a timeout is reached.
    pub async fn shutdown_aware_timeout<M: 'static, F>(
        receiver: &mut impl Pick<crate::bus::BusReceiver<crate::modules::signal::ShutdownModule>>,
        duration: std::time::Duration,
        f: F,
    ) -> anyhow::Result<F::Output>
    where
        F: std::future::IntoFuture,
    {
        let mut dummy = false;
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                anyhow::bail!("Timeout reached");
            }
            _ = async_receive_shutdown::<M>(
                &mut dummy,
                receiver.get_mut(),
            ) => {
                anyhow::bail!("Shutdown received");
            }
            res = f => {
                Ok(res)
            }
        }
    }

    pub async fn async_receive_shutdown<T: 'static>(
        should_shutdown: &mut bool,
        shutdown_receiver: &mut crate::bus::BusReceiver<crate::modules::signal::ShutdownModule>,
    ) -> anyhow::Result<()> {
        if *should_shutdown {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return Ok(());
        }
        while let Ok(shutdown_event) = shutdown_receiver.recv().await {
            let shutdown_event = shutdown_event.into_message();
            if TypeId::of::<T>() == TypeId::of::<()>() {
                tracing::debug!("Break signal received for any module");
                *should_shutdown = true;
                return Ok(());
            }
            if shutdown_event.module == std::any::type_name::<T>() {
                tracing::debug!(
                    "Break signal received for module {}",
                    std::any::type_name::<T>()
                );
                *should_shutdown = true;
                return Ok(());
            }
        }
        anyhow::bail!(
            "Error while shutting down module {}",
            std::any::type_name::<T>()
        );
    }
}

/// Event loop macro for modules.
///
/// A thin wrapper around [`handle_messages!`](crate::handle_messages) that automatically:
/// - Handles [`PersistModule`](signal::PersistModule) by calling `self.persist().await`
/// - Handles [`ShutdownModule`](signal::ShutdownModule) by breaking the loop
///
/// **`self.bus`** must be a bus client generated by [`module_bus_client!`](crate::module_bus_client).
///
/// # Variants
///
/// ## Standard shutdown
///
/// ```rust,ignore
/// module_handle_messages! {
///     on_self self,
///     listen<SomeEvent> ev => { /* … */ }
///     command_response<QueryFoo, Foo> q => { Ok(Foo) }
/// }
/// ```
///
/// ## Deferred shutdown (`delay_shutdown_until`)
///
/// Use this variant when the module must finish some in-progress work before it is safe
/// to stop (e.g. flush a write batch, finish a TCP handshake).
///
/// ```rust,ignore
/// module_handle_messages! {
///     on_self self,
///     delay_shutdown_until { self.pending_writes == 0 },
///     listen<WriteRequest> req => {
///         self.pending_writes += 1;
///         self.process(req).await;
///         self.pending_writes -= 1;
///     }
/// }
/// ```
///
/// The block must evaluate to `bool`. The loop breaks only when it returns `true`.
#[macro_export]
macro_rules! module_handle_messages {
    (on_self $self:expr, delay_shutdown_until  $lay_shutdow_until:block, $($rest:tt)*) => {
        {
            // Safety: this is disjoint.
            let mut shutdown_receiver = unsafe { &mut *$crate::utils::static_type_map::Pick::<$crate::bus::BusReceiver<$crate::modules::signal::ShutdownModule>>::splitting_get_mut(&mut $self.bus) };
            let mut should_shutdown = false;
            $crate::handle_messages! {
                on_bus $self.bus,
                listen<$crate::modules::signal::PersistModule> _ => {
                    _ = $self.persist().await;
                }
                $($rest)*
                Ok(_) = $crate::modules::signal::async_receive_shutdown::<Self>(&mut should_shutdown, &mut shutdown_receiver) => {
                    let res = $lay_shutdow_until;
                    if res {
                        break;
                    }
                }
            }
            tracing::info!("Event loop listening to {} has stopped", module_path!());
            should_shutdown
        }

    };
    (on_self $self:expr, $($rest:tt)*) => {
        {
            // Safety: this is disjoint.
            let mut shutdown_receiver = unsafe { &mut *$crate::utils::static_type_map::Pick::<$crate::bus::BusReceiver<$crate::modules::signal::ShutdownModule>>::splitting_get_mut(&mut $self.bus) };
            let mut should_shutdown = false;
            $crate::handle_messages! {
                on_bus $self.bus,
                listen<$crate::modules::signal::PersistModule> _ => {
                    _ = $self.persist().await;
                }
                $($rest)*
                Ok(_) = $crate::modules::signal::async_receive_shutdown::<Self>(&mut should_shutdown, &mut shutdown_receiver) => {
                    break;
                }
            }
            tracing::info!("Event loop listening in {} has stopped", module_path!());
            should_shutdown
        }
    };
}

/// Declare a typed bus client struct for a [`Module`].
///
/// Identical to [`bus_client!`](crate::bus_client) but automatically adds receivers for:
/// - [`ShutdownModule`](signal::ShutdownModule) — handled by [`module_handle_messages!`]
/// - [`PersistModule`](signal::PersistModule) — handled by [`module_handle_messages!`]
///
/// Use `query(CmdType, RespType)` to declare a synchronous request/response channel.
/// This generates a receiver for [`Query<CmdType, RespType>`](crate::bus::command_response::Query)
/// and can be handled with `command_response<CmdType, RespType>` inside the event loop.
///
/// # Example
///
/// ```rust,ignore
/// module_bus_client! {
///     pub struct MempoolBusClient {
///         sender(MempoolStatusEvent),    // this module emits these
///
///         receiver(ConsensusEvent),      // this module listens to these
///         receiver(NodeStateEvent),
///
///         query(QueryNewCut, Cut),       // synchronous request/response
///     }
/// }
/// ```
#[macro_export]
macro_rules! module_bus_client {
    (
        $(#[$meta:meta])*
        $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? {
            $(sender($sender:ty),)*
            $(receiver($receiver:ty),)*
            $(query($qtype:ty, $qresp:ty),)*
        }
    ) => {
        $crate::bus::bus_client!{
            $(#[$meta])*
            $pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? {
                $(sender($sender),)*
                $(receiver($receiver),)*
                $(receiver($crate::bus::command_response::Query<$qtype, $qresp>),)*
                receiver($crate::modules::signal::ShutdownModule),
                receiver($crate::modules::signal::PersistModule),
            }
        }
    }
}

pub use module_bus_client;

bus_client! {
    pub struct ShutdownClient {
        sender(signal::ShutdownModule),
        sender(signal::ShutdownCompleted),
        receiver(signal::ShutdownModule),
        receiver(signal::ShutdownCompleted),
    }
}

/// Manages the lifecycle of all [`Module`]s in the application.
///
/// `ModulesHandler` is the orchestrator: it builds modules, starts their event loops,
/// and coordinates a graceful, ordered shutdown.
///
/// # Shutdown order
///
/// Modules are shut down in **reverse startup order** (last started, first stopped).
/// This mirrors the typical dependency direction: if module B depends on module A,
/// you start A first and stop B first.
///
/// # Persistence and manifest integrity
///
/// After all modules have stopped cleanly, `ModulesHandler` writes a
/// [`CHECKSUMS_MANIFEST`] file. On the next startup, [`Module::load_from_disk`]
/// validates every persisted file against this manifest. If the manifest is missing
/// or empty (e.g. after a crash), the data directory is either backed up or removed
/// depending on [`ModulesHandlerOptions::backup_on_invalid_manifest`].
///
/// # Example
///
/// ```rust,ignore
/// let bus = SharedMessageBus::new();
/// let mut handler = ModulesHandler::new(&bus, "data".into(), ModulesHandlerOptions::default())?;
///
/// handler.build_module::<NodeState>(node_ctx).await?;
/// handler.build_module::<Mempool>(mempool_ctx).await?;
/// handler.build_module::<Consensus>(consensus_ctx).await?;
///
/// handler.start_modules().await?;
/// handler.exit_process().await?; // blocks until SIGINT / SIGTERM
/// ```
pub struct ModulesHandler {
    bus: SharedMessageBus,
    modules: Vec<ModuleStarter>,
    /// Data directory for manifest file
    data_dir: PathBuf,
    /// Track status for each module instance (keyed by module_id).
    modules_statuses: HashMap<String, ModuleStatus>,
    /// Startup order for shutdown sequencing (keyed by module_id).
    module_shutdown_order: Vec<String>,
}

/// Configuration for [`ModulesHandler`].
#[derive(Clone, Debug, Default)]
pub struct ModulesHandlerOptions {
    /// What to do with a non-empty data directory that has no valid manifest.
    ///
    /// - `true` — move the directory to `<data_dir>.backup_<timestamp>` (safe, keeps the data).
    /// - `false` — delete the directory and start fresh (default, simpler ops).
    ///
    /// A missing or empty manifest typically indicates a crash during a previous shutdown.
    /// The persisted files may be inconsistent, so hyli-bus refuses to load them and
    /// resets the directory instead.
    pub backup_on_invalid_manifest: bool,
}

/// Outcome of a module's shutdown sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleShutdownStatus {
    /// The module did not respond within the 5-second shutdown timeout.
    TimedOut,
    /// The module exited but [`Module::persist`] returned an error.
    PersistenceFailed { reason: String },
    /// The module exited cleanly and persisted the listed files.
    PersistedEntries(ModulePersistOutput),
}

/// Tracks whether a module is still running or has completed its shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleStatus {
    Running,
    Shutdown(ModuleShutdownStatus),
}

impl ModulesHandler {
    pub fn new(
        shared_bus: &SharedMessageBus,
        data_dir: PathBuf,
        options: ModulesHandlerOptions,
    ) -> Result<ModulesHandler> {
        let shared_message_bus = shared_bus.new_handle();
        let module_handler = ModulesHandler {
            bus: shared_message_bus,
            modules: vec![],
            data_dir: data_dir.clone(),
            modules_statuses: HashMap::new(),
            module_shutdown_order: Vec::new(),
        };

        // Try to read the directory
        let mut entries = match fs::read_dir(&data_dir) {
            Ok(it) => it,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                fs::create_dir_all(&data_dir).context("Failed to create data directory")?;
                return Ok(module_handler);
            }
            Err(e) => {
                bail!("Failed to read data_dir {}: {}", data_dir.display(), e);
            }
        };

        // Directory exists — check emptiness
        if entries.next().is_none() {
            // If empty, nothing to do
            return Ok(module_handler);
        }

        let manifest_file = manifest_path(&data_dir);

        // Decide whether the directory should be reset due to missing/invalid manifest.
        let has_invalid_manifest = if manifest_file.exists() {
            // Manifest exists -> invalid if empty or unreadable.
            match fs::read_to_string(&manifest_file) {
                Ok(content) => content.trim().is_empty(),
                Err(_) => true,
            }
        } else {
            // No manifest -> invalid.
            true
        };

        if has_invalid_manifest {
            if is_mount_point(&data_dir)? {
                bail!(
                    "Cannot reset data_dir because it is a mount point: {}. Use a subdirectory (e.g. {}), or empty the directory manually.",
                    data_dir.display(),
                    data_dir.join("node").display()
                );
            }

            if options.backup_on_invalid_manifest {
                // Generate a backup directory name using a timestamp
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let backup_dir = data_dir.with_extension(format!("backup_{timestamp}"));

                log_warn!(
                    fs::rename(&data_dir, &backup_dir),
                    "Moving data_dir to backup location"
                )
                .context("Failed to move data_dir to backup location")?;

                warn!(
                    "Moved data_dir without valid manifest to backup: {} -> {}",
                    data_dir.display(),
                    backup_dir.display()
                );
            } else {
                log_warn!(fs::remove_dir_all(&data_dir), "Removing invalid data_dir")
                    .context("Failed to remove invalid data_dir")?;
                warn!(
                    "Removed data_dir without valid manifest: {}",
                    data_dir.display()
                );
            }

            // Recreate an empty data_dir
            fs::create_dir_all(&data_dir).context("Failed to recreate data_dir")?;
        }

        Ok(module_handler)
    }

    /// Spawn all registered modules as Tokio tasks.
    ///
    /// Each module runs in its own task. A watchdog task is also spawned per module:
    /// it sends a timeout error if the module hasn't finished within
    /// 5 seconds after receiving its [`ShutdownModule`](signal::ShutdownModule)
    /// signal.
    ///
    /// **Call this only after all modules have been built.** Channels are created on
    /// first subscription, so any module built after this point might miss messages
    /// that were sent before it subscribed.
    ///
    /// Also removes the previous manifest file so that an incomplete run cannot be
    /// mistaken for a clean shutdown.
    pub async fn start_modules(&mut self) -> Result<()> {
        if self.has_running_modules() {
            bail!("Modules are already running!");
        }
        self.modules_statuses.clear();
        self.module_shutdown_order.clear();

        let mut seen_modules: HashSet<&'static str> = HashSet::new();
        let mut last_module_name: Option<&'static str> = None;
        for module in self.modules.drain(..) {
            if seen_modules.contains(&module.name) && last_module_name != Some(module.name) {
                bail!(
                    "Module {} appears multiple times but is not consecutive",
                    module.name
                );
            }
            seen_modules.insert(module.name);
            last_module_name = Some(module.name);
            let module_id = Self::build_module_id(module.name);
            self.modules_statuses
                .insert(module_id.clone(), ModuleStatus::Running);
            self.module_shutdown_order.push(module_id.clone());

            debug!(module = %module.name, module_id = %module_id, "Starting module");

            let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            let mut shutdown_client2 = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            tokio::spawn(async move {
                let module_name = module.name;
                let module_task = tokio::spawn(module.starter);
                let timeout_task = tokio::spawn(async move {
                    loop {
                        if let Ok(signal::ShutdownModule { module: modname }) =
                            shutdown_client2.recv().await
                        {
                            if modname == module_name {
                                tokio::time::sleep(MODULE_SHUTDOWN_TIMEOUT).await;
                                break;
                            }
                        }
                    }
                });

                let (res, timed_out) = tokio::select! {
                    res = module_task => (res, false),
                    _ = timeout_task => {
                        (Ok(Err(anyhow::anyhow!("Shutdown timeout reached"))), true)
                    }
                };

                let shutdown_status = if timed_out {
                    ModuleShutdownStatus::TimedOut
                } else {
                    match res {
                        Ok(Ok(entries)) => {
                            tracing::info!(
                                module = %module_name,
                                module_id = %module_id,
                                "Module {} exited and persisted {} file(s)",
                                module_name,
                                entries.len()
                            );
                            ModuleShutdownStatus::PersistedEntries(entries)
                        }
                        Ok(Err(e)) => {
                            let reason = format!("{e:#}");
                            tracing::error!(
                                module = %module_name,
                                module_id = %module_id,
                                reason = %reason,
                                "Module {} exited with error: {:?}",
                                module_name,
                                e
                            );
                            ModuleShutdownStatus::PersistenceFailed { reason }
                        }
                        Err(e) => {
                            let reason = format!("{e:#}");
                            tracing::error!(
                                module = %module_name,
                                module_id = %module_id,
                                reason = %reason,
                                "Module {} exited, error joining: {:?}",
                                module_name,
                                e
                            );
                            ModuleShutdownStatus::PersistenceFailed { reason }
                        }
                    }
                };

                _ = log_error!(
                    shutdown_client.send(signal::ShutdownCompleted {
                        module: module_id,
                        shutdown_status
                    }),
                    "Sending ShutdownCompleted message"
                );
            });
        }

        _ = log_warn!(
            checksums::remove_manifest(&self.data_dir),
            "Removing manifest file"
        );
        Ok(())
    }

    /// Setup a loop waiting for shutdown signals from modules
    pub async fn shutdown_loop(&mut self) -> Result<()> {
        if !self.has_running_modules() {
            return Ok(());
        }

        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;

        // Trigger shutdown chain when one shutdown message is received for a long running module
        handle_messages! {
            on_bus shutdown_client,
            listen<signal::ShutdownCompleted> msg => {
                let shutdown_status = msg.shutdown_status.clone();
                match shutdown_status {
                    ModuleShutdownStatus::TimedOut => {
                        tracing::warn!(
                            "Module {} timed out during shutdown, manifest will not be written",
                            msg.module
                        );
                    }
                    ModuleShutdownStatus::PersistenceFailed { ref reason } => {
                        tracing::warn!(
                            module = %msg.module,
                            reason = %reason,
                            "Module failed to persist during shutdown, manifest will not be written"
                        );
                    }
                    ModuleShutdownStatus::PersistedEntries(_) => {}
                }
                self.modules_statuses
                    .insert(msg.module.clone(), ModuleStatus::Shutdown(shutdown_status));
                if !self.has_running_modules() {
                    break;
                } else {
                    _ = self.shutdown_next_module().await;
                }
            }
        }

        info!("All modules have been shut down");
        let timed_out_modules: Vec<&str> = self
            .modules_statuses
            .iter()
            .filter_map(|(module, status)| {
                matches!(
                    status,
                    ModuleStatus::Shutdown(ModuleShutdownStatus::TimedOut)
                )
                .then_some(module.as_str())
            })
            .collect();
        let persistence_failed_modules: Vec<(&str, &str)> = self
            .modules_statuses
            .iter()
            .filter_map(|(module, status)| {
                if let ModuleStatus::Shutdown(ModuleShutdownStatus::PersistenceFailed { reason }) =
                    status
                {
                    Some((module.as_str(), reason.as_str()))
                } else {
                    None
                }
            })
            .collect();
        let has_shutdown_errors =
            !timed_out_modules.is_empty() || !persistence_failed_modules.is_empty();

        if has_shutdown_errors {
            let log_path = self.data_dir.join(PERSISTENCE_FAILURES_LOG);
            if let Ok(mut file) = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
            {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                for module in &timed_out_modules {
                    _ = writeln!(file, "{timestamp} timed_out {module}");
                }
                for (module, reason) in &persistence_failed_modules {
                    _ = writeln!(
                        file,
                        "{} persistence_failed {} reason={}",
                        timestamp,
                        module,
                        reason.replace('\n', "\\n")
                    );
                }
            } else {
                warn!(
                    "Failed to open persistence failure log at {}",
                    log_path.display()
                );
            }
        }

        let persisted_entries: ModulePersistOutput = self
            .modules_statuses
            .values()
            .flat_map(|status| match status {
                ModuleStatus::Shutdown(ModuleShutdownStatus::PersistedEntries(entries)) => {
                    entries.clone()
                }
                _ => Vec::new(),
            })
            .collect();

        // Write manifest if no shutdown errors occurred
        if !has_shutdown_errors && !persisted_entries.is_empty() {
            _ = log_error!(
                write_manifest(&self.data_dir, &persisted_entries),
                "Writing checksum manifest"
            );
        } else if has_shutdown_errors {
            tracing::warn!(
                timed_out_modules = ?timed_out_modules,
                persistence_failed_modules = ?persistence_failed_modules,
                "Skipping manifest write due to module shutdown errors"
            );
        }

        Ok(())
    }

    /// Shutdown modules in reverse order (start A, B, C, shutdown C, B, A)
    async fn shutdown_next_module(&mut self) -> Result<()> {
        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
        if let Some(module_id) = self.next_running_module_id() {
            let module_name = Self::module_name_from_id(module_id);
            _ = log_error!(
                shutdown_client.send(signal::ShutdownModule {
                    module: module_name.to_string(),
                }),
                "Shutting down module"
            );
        }

        Ok(())
    }

    /// Trigger a graceful shutdown of all running modules and wait for them to finish.
    ///
    /// Modules are stopped in reverse startup order. Equivalent to calling
    /// `shutdown_next_module` and then [`shutdown_loop`](Self::shutdown_loop).
    pub async fn shutdown_modules(&mut self) -> Result<()> {
        self.shutdown_next_module().await?;
        self.shutdown_loop().await?;

        Ok(())
    }

    /// Wait for all modules to finish (without initiating a shutdown) and then shut down any
    /// remaining ones.
    ///
    /// Useful when a module exits on its own (e.g. after processing a finite workload) and
    /// you want to cascade the shutdown to the rest.
    pub async fn exit_loop(&mut self) -> Result<()> {
        _ = log_error!(self.shutdown_loop().await, "Shutdown Loop triggered");
        _ = self.shutdown_modules().await;

        Ok(())
    }

    /// If the node is run as a process, we want to setup a proper exit loop with SIGINT/SIGTERM
    pub async fn exit_process(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            use tokio::signal::unix;
            let mut interrupt = unix::signal(unix::SignalKind::interrupt())?;
            let mut terminate = unix::signal(unix::SignalKind::terminate())?;
            tokio::select! {
                res = self.shutdown_loop() => {
                    _ = log_error!(res, "Shutdown Loop triggered");
                }
                _ = interrupt.recv() =>  {
                    info!("SIGINT received, shutting down");
                }
                _ = terminate.recv() =>  {
                    info!("SIGTERM received, shutting down");
                }
            }
            _ = self.shutdown_modules().await;
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                res = self.shutdown_loop() => {
                    _ = log_error!(res, "Shutdown Loop triggered");
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C received, shutting down");
                }
            }
            _ = self.shutdown_modules().await;
        }
        Ok(())
    }

    async fn run_module<M>(mut module: M) -> Result<ModulePersistOutput>
    where
        M: Module,
    {
        module.run().await?;
        module.persist().await
    }

    /// Build a module and register it for startup.
    ///
    /// Calls [`Module::build`] with a new bus handle and the provided context,
    /// then internally calls [`add_module`](Self::add_module).
    ///
    /// This must be called **before** [`start_modules`](Self::start_modules).
    pub async fn build_module<M>(&mut self, ctx: M::Context) -> Result<()>
    where
        M: Module + 'static + Send,
        <M as Module>::Context: std::marker::Send,
    {
        let module = M::build(self.bus.new_handle(), ctx).await?;
        self.add_module(module)
    }

    /// Register an already-built module instance for startup.
    ///
    /// Use [`build_module`](Self::build_module) when you want the handler to call
    /// [`Module::build`] for you. Use `add_module` when you need to construct the
    /// module yourself (e.g. to pass non-`Send` context or to share state between modules
    /// during setup).
    pub fn add_module<M>(&mut self, module: M) -> Result<()>
    where
        M: Module + 'static + Send,
        <M as Module>::Context: std::marker::Send,
    {
        debug!("Adding module {}", type_name::<M>());
        self.modules.push(ModuleStarter {
            name: type_name::<M>(),
            starter: Box::pin(Self::run_module(module)),
        });
        Ok(())
    }

    fn has_running_modules(&self) -> bool {
        self.modules_statuses
            .values()
            .any(|status| matches!(status, ModuleStatus::Running))
    }

    fn next_running_module_id(&self) -> Option<&String> {
        self.module_shutdown_order.iter().rev().find(|module_id| {
            matches!(
                self.modules_statuses.get(*module_id),
                Some(ModuleStatus::Running)
            )
        })
    }

    fn module_name_from_id(module_id: &str) -> &str {
        module_id
            .split_once(MODULE_ID_SEPARATOR)
            .map(|(module_name, _)| module_name)
            .unwrap_or(module_id)
    }

    fn build_module_id(module_name: &str) -> String {
        let unique_id: String = deterministic_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        format!("{module_name}{MODULE_ID_SEPARATOR}{unique_id}")
    }
}

#[cfg(test)]
mod tests;
