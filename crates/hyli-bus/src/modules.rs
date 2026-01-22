use std::{
    any::type_name,
    collections::HashSet,
    fs,
    future::Future,
    io::{BufWriter, Write},
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

    use crate::{bus::BusMessage, utils::static_type_map::Pick};

    #[derive(Clone, Debug)]
    pub struct PersistModule {}

    #[derive(Clone, Debug)]
    pub struct ShutdownModule {
        pub module: String,
    }

    #[derive(Clone, Debug)]
    pub struct ShutdownCompleted {
        pub module: String,
        pub persisted_entries: Vec<(std::path::PathBuf, u32)>,
        pub timed_out: bool,
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
    running_modules: Vec<&'static str>,
    /// Data directory for manifest file
    data_dir: PathBuf,
    /// Collected checksums from successfully persisted modules
    persisted_checksums: Vec<(PathBuf, u32)>,
    /// Track if any module timed out during shutdown
    any_timeout: bool,
}

impl ModulesHandler {
    pub async fn new(shared_bus: &SharedMessageBus, data_dir: PathBuf) -> ModulesHandler {
        let shared_message_bus = shared_bus.new_handle();

        ModulesHandler {
            bus: shared_message_bus,
            modules: vec![],
            running_modules: vec![],
            data_dir,
            persisted_checksums: vec![],
            any_timeout: false,
        }
    }

    pub async fn start_modules(&mut self) -> Result<()> {
        if !self.running_modules.is_empty() {
            bail!("Modules are already running!");
        }

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
            self.running_modules.push(module.name);

            debug!("Starting module {}", module.name);

            let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            let mut shutdown_client2 = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            tokio::spawn(async move {
                let module_task = tokio::spawn(module.starter);
                let timeout_task = tokio::spawn(async move {
                    loop {
                        if let Ok(signal::ShutdownModule { module: modname }) =
                            shutdown_client2.recv().await
                        {
                            if modname == module.name {
                                tokio::time::sleep(MODULE_SHUTDOWN_TIMEOUT).await;
                                break;
                            }
                        }
                    }
                });

                let (res, timed_out) = tokio::select! {
                    res = module_task => {
                        (res, false)
                    },
                    _ = timeout_task => {
                        (Ok(Err(anyhow::anyhow!("Shutdown timeout reached"))), true)
                    }
                };

                let mut persisted_entries = Vec::new();
                match res {
                    Ok(Ok(entries)) => {
                        tracing::info!(
                            module =% module.name,
                            "Module {} exited and persisted {} file(s)",
                            module.name,
                            entries.len()
                        );
                        persisted_entries = entries;
                    }
                    Ok(Err(e)) => {
                        tracing::error!(module =% module.name, "Module {} exited with error: {:?}", module.name, e);
                    }
                    Err(e) => {
                        tracing::error!(module =% module.name, "Module {} exited, error joining: {:?}", module.name, e);
                    }
                }

                _ = log_error!(
                    shutdown_client.send(signal::ShutdownCompleted {
                        module: module.name.to_string(),
                        persisted_entries,
                        timed_out,
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
        if self.running_modules.is_empty() {
            return Ok(());
        }

        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;

        // Trigger shutdown chain when one shutdown message is received for a long running module
        handle_messages! {
            on_bus shutdown_client,
            listen<signal::ShutdownCompleted> msg => {
                if msg.timed_out {
                    tracing::warn!(
                        "Module {} timed out during shutdown, manifest will not be written",
                        msg.module
                    );
                    self.any_timeout = true;
                }
                for (path, checksum) in msg.persisted_entries {
                    debug!(
                        "Received persisted entries for {}: {} -> {}",
                        msg.module,
                        path.display(),
                        format_checksum(checksum)
                    );
                    self.persisted_checksums.push((path, checksum));
                }
                if let Some(idx) = self.running_modules.iter().position(|module| *module == msg.module) {
                    self.running_modules.remove(idx);
                }
                if self.running_modules.is_empty() {
                    break;
                } else {
                    _ = self.shutdown_next_module().await;
                }
            }
        }

        info!("All modules have been shut down");
        // Write manifest if no timeouts occurred
        if !self.any_timeout && !self.persisted_checksums.is_empty() {
            _ = log_error!(
                write_manifest(&self.data_dir, &self.persisted_checksums),
                "Writing checksum manifest"
            );
        } else if self.any_timeout {
            tracing::warn!("Skipping manifest write due to module timeout");
        }

        Ok(())
    }

    /// Shutdown modules in reverse order (start A, B, C, shutdown C, B, A)
    async fn shutdown_next_module(&mut self) -> Result<()> {
        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
        if let Some(&module_name) = self.running_modules.last() {
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
}

#[cfg(test)]
mod tests;
