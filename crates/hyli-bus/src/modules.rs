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
const BACKUP_DIR_SUFFIX: &str = "backup";
const BACKUP_TIMESTAMP_FILE: &str = ".backup_timestamp_ms";

#[cfg(target_os = "linux")]
fn is_mount_point(path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(path)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_meta = fs::metadata(parent)?;
    Ok(meta.dev() != parent_meta.dev())
}

#[cfg(not(target_os = "linux"))]
fn is_mount_point(_path: &Path) -> Result<bool> {
    Ok(false)
}

pub type ModulePersistOutput = Vec<(PathBuf, u32)>;

/// Module trait to define startup dependencies
pub trait Module
where
    Self: Sized,
{
    type Context;

    fn build(
        bus: SharedMessageBus,
        ctx: Self::Context,
    ) -> impl futures::Future<Output = Result<Self>> + Send;
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
    /// Returns `Some(vec)` if data was saved, `None` if module doesn't persist.
    fn persist(&mut self) -> impl futures::Future<Output = Result<ModulePersistOutput>> + Send {
        async {
            info!(
                "Persistence is not implemented for module {}",
                type_name::<Self>()
            );
            Ok(Vec::new())
        }
    }

    /// Save data to disk and return the CRC32 checksum.
    /// `file` is relative to `data_dir`.
    /// The checksum should be collected by ModulesHandler and written to a manifest file.
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

pub mod files {
    pub const NODE_STATE_BIN: &str = "node_state.bin";
    pub const CONSENSUS_BIN: &str = "consensus.bin";
}

type ModuleStarterFuture =
    Pin<Box<dyn Future<Output = Result<ModulePersistOutput, Error>> + Send + 'static>>;

struct ModuleStarter {
    pub name: &'static str,
    starter: ModuleStarterFuture,
}

pub mod signal {
    use std::any::TypeId;

    use crate::{bus::BusMessage, modules::ModuleShutdownStatus, utils::static_type_map::Pick};

    #[derive(Clone, Debug)]
    pub struct PersistModule {}

    #[derive(Clone, Debug)]
    pub struct ShutdownModule {
        pub module: String,
    }

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

#[macro_export]
macro_rules! module_bus_client {
    (
        $(#[$meta:meta])*
        $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? {
            $(sender($sender:ty),)*
            $(receiver($receiver:ty),)*
        }
    ) => {
        $crate::bus::bus_client!{
            $(#[$meta])*
            $pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? {
                $(sender($sender),)*
                $(receiver($receiver),)*
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleShutdownStatus {
    TimedOut,
    PersistenceFailed { reason: String },
    PersistedEntries(ModulePersistOutput),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleStatus {
    Running,
    Shutdown(ModuleShutdownStatus),
}

impl ModulesHandler {
    pub fn new(shared_bus: &SharedMessageBus, data_dir: PathBuf) -> Result<ModulesHandler> {
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

        // Decide whether the directory should be backed up
        let should_backup = if manifest_file.exists() {
            // Manifest exists → invalid if empty
            match fs::read_to_string(&manifest_file) {
                Ok(content) => content.trim().is_empty(),
                Err(_) => true,
            }
        } else {
            // No manifest → back up
            true
        };

        if should_backup {
            if is_mount_point(&data_dir)? {
                bail!(
                    "Cannot back up data_dir because it is a mount point: {}. Use a subdirectory (e.g. {}), or empty the directory manually.",
                    data_dir.display(),
                    data_dir.join("node").display()
                );
            }

            // Keep a stable backup folder name and record when it was refreshed.
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let backup_dir = data_dir.with_extension(BACKUP_DIR_SUFFIX);
            if backup_dir.exists() {
                log_warn!(
                    fs::remove_dir_all(&backup_dir),
                    "Removing existing backup directory before overwrite"
                )
                .context("Failed to remove existing backup directory")?;
            }

            log_warn!(
                fs::rename(&data_dir, &backup_dir),
                "Moving data_dir to backup location"
            )
            .context("Failed to move data_dir to backup location")?;

            let backup_timestamp_file = backup_dir.join(BACKUP_TIMESTAMP_FILE);
            fs::write(&backup_timestamp_file, timestamp.to_string()).context(format!(
                "Failed to write backup timestamp metadata at {}",
                backup_timestamp_file.display()
            ))?;

            warn!(
                "Moved data_dir without valid manifest to backup: {} -> {}",
                data_dir.display(),
                backup_dir.display()
            );

            // Recreate an empty data_dir
            fs::create_dir_all(&data_dir).context("Failed to recreate data_dir")?;
        }

        Ok(module_handler)
    }

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

    pub async fn shutdown_modules(&mut self) -> Result<()> {
        self.shutdown_next_module().await?;
        self.shutdown_loop().await?;

        Ok(())
    }

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

    pub async fn build_module<M>(&mut self, ctx: M::Context) -> Result<()>
    where
        M: Module + 'static + Send,
        <M as Module>::Context: std::marker::Send,
    {
        let module = M::build(self.bus.new_handle(), ctx).await?;
        self.add_module(module)
    }

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
