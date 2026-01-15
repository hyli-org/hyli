use std::{
    any::type_name,
    fs,
    future::Future,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use crate::{
    bus::{BusClientReceiver, BusClientSender, SharedMessageBus},
    bus_client, handle_messages, log_error,
};
use anyhow::{bail, Error, Result};
use rand::{distr::Alphanumeric, Rng};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::utils::deterministic_rng::deterministic_rng;

const MODULE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Error type for persistence operations
#[derive(Debug)]
pub enum PersistenceError {
    /// File not found (not an error, use default)
    NotFound,
    /// Checksum verification failed (data corruption)
    ChecksumMismatch { expected: String, actual: String },
    /// Checksum file missing
    MissingChecksumFile,
    /// Checksum file format invalid
    ChecksumParseFailed(String),
    /// Deserialization failed
    DeserializationFailed(String),
    /// IO error during read/write
    IoError(std::io::Error),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "File not found"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "Data corruption detected: checksum mismatch (expected {}, got {})",
                    expected, actual
                )
            }
            Self::MissingChecksumFile => write!(f, "Checksum file missing"),
            Self::ChecksumParseFailed(msg) => {
                write!(f, "Invalid checksum file contents: {}", msg)
            }
            Self::DeserializationFailed(msg) => write!(f, "Failed to deserialize data: {}", msg),
            Self::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl From<std::io::Error> for PersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

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

    /// Load data from disk with checksum verification.
    ///
    /// Returns:
    /// - `Ok(Some(data))` if file exists and checksum verifies
    /// - `Ok(None)` if file doesn't exist
    /// - `Err(PersistenceError::MissingChecksumFile)` if checksum file is missing
    /// - `Err(PersistenceError::ChecksumMismatch)` if checksum verification fails
    /// - `Err(PersistenceError::DeserializationFailed)` if data is corrupted
    fn load_from_disk<S>(file: &Path) -> Result<Option<S>, PersistenceError>
    where
        S: borsh::BorshDeserialize,
    {
        // Read and deserialize while computing checksum
        let data_file = match fs::File::open(file) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    "File {} not found for module {} (using default)",
                    file.to_string_lossy(),
                    type_name::<S>(),
                );
                return Ok(None);
            }
            Err(e) => return Err(PersistenceError::IoError(e)),
        };

        let mut checksum_reader = ChecksumReader::new(data_file);
        let deserialized: S = borsh::from_reader(&mut checksum_reader)
            .map_err(|e| PersistenceError::DeserializationFailed(e.to_string()))?;
        checksum_reader
            .drain_to_end()
            .map_err(PersistenceError::IoError)?;

        // Check for checksum file and verify
        let expected_checksum = read_checksum_file(file)?;
        let actual_checksum = checksum_reader.checksum;
        if expected_checksum != actual_checksum {
            tracing::error!(
                "Checksum mismatch for {}: expected {}, got {}",
                file.to_string_lossy(),
                format_checksum(expected_checksum),
                format_checksum(actual_checksum)
            );
            return Err(PersistenceError::ChecksumMismatch {
                expected: format_checksum(expected_checksum),
                actual: format_checksum(actual_checksum),
            });
        }
        debug!(
            "Checksum verification passed for {}",
            file.to_string_lossy()
        );

        info!("Loaded data from disk {}", file.to_string_lossy());
        Ok(Some(deserialized))
    }

    fn persist(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        async {
            info!(
                "Persistence is not implemented for module {}",
                type_name::<Self>()
            );
            Ok(())
        }
    }

    fn load_from_disk_or_default<S>(file: &Path) -> Result<S, PersistenceError>
    where
        S: borsh::BorshDeserialize + Default,
    {
        Ok(Self::load_from_disk(file)?.unwrap_or(S::default()))
    }

    fn save_on_disk<S>(file: &Path, store: &S) -> Result<()>
    where
        S: borsh::BorshSerialize,
    {
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

        // Temp file paths for data and checksum
        let tmp_data = file.with_extension(format!("{salt}.tmp"));
        let checksum_path = checksum_file_path(file);
        let tmp_checksum = checksum_path.with_extension(format!("checksum.{salt}.tmp"));

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

        // Write checksum to temp file
        log_error!(
            fs::write(tmp_checksum.as_path(), format_checksum(checksum)),
            "Writing checksum file"
        )?;

        // Atomically rename both files
        debug!("Renaming {:?} to {:?}", &tmp_data, &file);
        log_error!(fs::rename(&tmp_data, file), "Rename data file")?;
        log_error!(
            fs::rename(&tmp_checksum, &checksum_path),
            "Rename checksum file"
        )?;

        Ok(())
    }
}

struct ChecksumWriter<W> {
    inner: W,
    checksum: u64,
}

impl<W: Write> ChecksumWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, checksum: 0 }
    }

    fn checksum(&self) -> u64 {
        self.checksum
    }
}

impl<W: Write> Write for ChecksumWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.inner.write(buf)?;
        self.checksum = self
            .checksum
            .wrapping_add(checksum_bytes(&buf[..bytes_written]));
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct ChecksumReader<R> {
    inner: R,
    checksum: u64,
}

impl<R: Read> ChecksumReader<R> {
    fn new(inner: R) -> Self {
        Self { inner, checksum: 0 }
    }

    fn drain_to_end(&mut self) -> std::io::Result<()> {
        let mut buf = [0u8; 4096];
        loop {
            let bytes_read = self.read(&mut buf)?;
            if bytes_read == 0 {
                return Ok(());
            }
        }
    }
}

impl<R: Read> Read for ChecksumReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.read(buf)?;
        self.checksum = self
            .checksum
            .wrapping_add(checksum_bytes(&buf[..bytes_read]));
        Ok(bytes_read)
    }
}

fn checksum_bytes(data: &[u8]) -> u64 {
    data.iter()
        .fold(0u64, |acc, byte| acc.wrapping_add(u64::from(*byte)))
}

fn format_checksum(checksum: u64) -> String {
    format!("{:016x}", checksum)
}

/// Get the path for the checksum file given a data file path
fn checksum_file_path(file: &Path) -> PathBuf {
    let mut path = file.as_os_str().to_owned();
    path.push(".checksum");
    PathBuf::from(path)
}

/// Read checksum from checksum file, returns error if file doesn't exist
fn read_checksum_file(file: &Path) -> Result<u64, PersistenceError> {
    match fs::read_to_string(checksum_file_path(file)) {
        Ok(contents) => {
            let trimmed = contents.trim();
            u64::from_str_radix(trimmed, 16)
                .map_err(|_| PersistenceError::ChecksumParseFailed(trimmed.to_string()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(PersistenceError::MissingChecksumFile)
        }
        Err(e) => Err(PersistenceError::IoError(e)),
    }
}

pub mod files {
    pub const NODE_STATE_BIN: &str = "node_state.bin";
    pub const CONSENSUS_BIN: &str = "consensus.bin";
}

struct ModuleStarter {
    pub name: &'static str,
    starter: Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>,
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
    started_modules: Vec<&'static str>,
    running_modules: Vec<JoinHandle<()>>,
    shut_modules: Vec<String>,
}

impl ModulesHandler {
    pub async fn new(shared_bus: &SharedMessageBus) -> ModulesHandler {
        let shared_message_bus = shared_bus.new_handle();

        ModulesHandler {
            bus: shared_message_bus,
            modules: vec![],
            started_modules: vec![],
            running_modules: vec![],
            shut_modules: vec![],
        }
    }

    fn long_running_module(module_name: &str) -> bool {
        !["hyli::genesis::Genesis"].contains(&module_name)
    }

    pub async fn start_modules(&mut self) -> Result<()> {
        if !self.running_modules.is_empty() {
            bail!("Modules are already running!");
        }

        for module in self.modules.drain(..) {
            if Self::long_running_module(module.name) {
                self.started_modules.push(module.name);
            }

            debug!("Starting module {}", module.name);

            let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            let mut shutdown_client2 = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            let task = tokio::spawn(async move {
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

                let res = tokio::select! {
                    res = module_task => {
                        res
                    },
                    _ = timeout_task => {
                        Ok(Err(anyhow::anyhow!("Shutdown timeout reached")))
                    }
                };
                match res {
                    Ok(Ok(())) => {
                        tracing::warn!(module =% module.name, "Module {} exited with no error.", module.name);
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
                    }),
                    "Sending ShutdownCompleted message"
                );
            });

            if Self::long_running_module(module.name) {
                self.running_modules.push(task);
            }
        }

        Ok(())
    }

    /// Setup a loop waiting for shutdown signals from modules
    pub async fn shutdown_loop(&mut self) -> Result<()> {
        if self.started_modules.is_empty() {
            return Ok(());
        }

        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;

        // Trigger shutdown chain when one shutdown message is received for a long running module
        handle_messages! {
            on_bus shutdown_client,
            listen<signal::ShutdownCompleted> msg => {
                if Self::long_running_module(msg.module.as_str()) && !self.shut_modules.contains(&msg.module)  {
                    self.started_modules.retain(|module| *module != msg.module.clone());
                    self.shut_modules.push(msg.module.clone());
                    if self.started_modules.is_empty() {
                        break;
                    } else {
                        _ = self.shutdown_next_module().await;
                    }
                }
            }

            // Add one second as buffer to let the module cancel itself, hopefully.
            _ = tokio::time::sleep(MODULE_SHUTDOWN_TIMEOUT + Duration::from_secs(1)) => {
                if !self.shut_modules.is_empty() {
                    _ = self.shutdown_next_module().await;
                }
            }
        }

        Ok(())
    }

    /// Shutdown modules in reverse order (start A, B, C, shutdown C, B, A)
    async fn shutdown_next_module(&mut self) -> Result<()> {
        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
        if let Some(module_name) = self.started_modules.pop() {
            // May be the shutdown message was skipped because the module failed somehow
            if !self.shut_modules.contains(&module_name.to_string()) {
                _ = log_error!(
                    shutdown_client.send(signal::ShutdownModule {
                        module: module_name.to_string(),
                    }),
                    "Shutting down module"
                );
            } else {
                tracing::debug!("Not shutting already shut module {}", module_name);
            }
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

    async fn run_module<M>(mut module: M) -> Result<()>
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
mod tests {
    use crate::bus::{dont_use_this::get_receiver, metrics::BusMetrics};

    use super::*;
    use crate::bus::SharedMessageBus;
    use signal::{ShutdownCompleted, ShutdownModule};
    use std::{fs::File, sync::Arc};
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[derive(Default, borsh::BorshSerialize, borsh::BorshDeserialize)]
    struct TestStruct {
        value: u32,
    }

    struct TestModule<T> {
        bus: TestBusClient,
        _field: T,
    }

    module_bus_client! {
        struct TestBusClient { sender(usize), }
    }

    macro_rules! test_module {
        ($bus_client:ty, $tag:ty) => {
            impl Module for TestModule<$tag> {
                type Context = ();
                async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
                    Ok(TestModule {
                        bus: <$bus_client>::new_from_bus(bus).await,
                        _field: Default::default(),
                    })
                }

                async fn run(&mut self) -> Result<()> {
                    let nb_shutdowns: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
                    let cloned = Arc::clone(&nb_shutdowns);
                    module_handle_messages! {
                        on_self self,
                        _ = async {
                            let mut guard = cloned.lock().await;
                            (*guard) += 1;
                            std::future::pending::<()>().await
                        } => { }
                    };

                    self.bus.send(*cloned.lock().await).expect(
                        "Error while sending the number of loop cancellations while shutting down",
                    );

                    Ok(())
                }
            }
        };
    }

    test_module!(TestBusClient, String);
    test_module!(TestBusClient, usize);
    test_module!(TestBusClient, bool);

    // Failing module by breaking event loop

    impl Module for TestModule<u64> {
        type Context = ();
        async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
            Ok(TestModule {
                bus: TestBusClient::new_from_bus(bus).await,
                _field: Default::default(),
            })
        }

        async fn run(&mut self) -> Result<()> {
            module_handle_messages! {
                on_self self,
                _ = async { } => {
                    break;
                }
            };

            Ok(())
        }
    }

    // Failing module by early exit (no shutdown completed event emitted)

    impl Module for TestModule<u32> {
        type Context = ();
        async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
            Ok(TestModule {
                bus: TestBusClient::new_from_bus(bus).await,
                _field: Default::default(),
            })
        }

        async fn run(&mut self) -> Result<()> {
            panic!("bruh");
        }
    }

    #[test]
    fn test_load_from_disk_or_default_valid_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");

        // Write a valid TestStruct to the file (with checksum)
        let test_struct = TestStruct { value: 42 };
        TestModule::<usize>::save_on_disk(&file_path, &test_struct).unwrap();

        // Load the struct from the file
        let loaded_struct: TestStruct =
            TestModule::<usize>::load_from_disk_or_default(&file_path).unwrap();
        assert_eq!(loaded_struct.value, 42);
    }

    #[test]
    fn test_load_from_disk_or_default_missing_checksum_file() {
        let dir = tempdir().unwrap();
        let test_struct = TestStruct { value: 42 };

        // Missing checksum file should return an error
        let non_existant_checksum_file = dir.path().join("non_existant_checksum_file");
        let mut legacy_file = File::create(&non_existant_checksum_file).unwrap();
        borsh::to_writer(&mut legacy_file, &test_struct).unwrap();
        let result = TestModule::<usize>::load_from_disk_or_default::<TestStruct>(
            &non_existant_checksum_file,
        );
        assert!(
            result.is_err(),
            "Expected error due to missing checksum file"
        );
    }

    #[test]
    fn test_load_from_disk_or_default_non_existent_file() {
        let dir = tempdir().unwrap();
        // Load from a non-existent file
        let non_existent_path = dir.path().join("non_existent_file");
        let default_struct: TestStruct =
            TestModule::<usize>::load_from_disk_or_default(&non_existent_path).unwrap();
        assert_eq!(default_struct.value, 0);
    }

    #[test_log::test]
    fn test_save_creates_checksum_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file.data");

        let test_struct = TestStruct { value: 42 };
        TestModule::<usize>::save_on_disk(&file_path, &test_struct).unwrap();

        // Verify checksum file was created
        let checksum_path = super::checksum_file_path(&file_path);
        assert!(checksum_path.exists(), "Checksum file should be created");

        // Verify checksum file content is a valid hex string
        let checksum_content = std::fs::read_to_string(&checksum_path).unwrap();
        assert_eq!(
            checksum_content.len(),
            16,
            "Checksum should be 16 hex chars"
        );
        assert!(
            checksum_content.chars().all(|c| c.is_ascii_hexdigit()),
            "Checksum should be valid hex"
        );
    }

    #[test_log::test]
    fn test_checksum_mismatch_detection() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file.data");

        let test_struct = TestStruct { value: 42 };
        TestModule::<usize>::save_on_disk(&file_path, &test_struct).unwrap();

        // Corrupt the checksum file
        let checksum_path = super::checksum_file_path(&file_path);
        std::fs::write(&checksum_path, "0000000000000001").unwrap();

        // Load should fail with checksum mismatch
        let result: Result<Option<TestStruct>, super::PersistenceError> =
            TestModule::<usize>::load_from_disk(&file_path);

        assert!(
            matches!(
                result,
                Err(super::PersistenceError::ChecksumMismatch { .. })
            ),
            "Should detect checksum mismatch"
        );
    }

    #[test_log::test]
    fn test_corrupted_data_detection() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file.data");

        let test_struct = TestStruct { value: 42 };
        TestModule::<usize>::save_on_disk(&file_path, &test_struct).unwrap();

        // Corrupt the data file (but leave checksum intact)
        let mut data = std::fs::read(&file_path).unwrap();
        data[0] ^= 0xFF; // Flip some bits
        std::fs::write(&file_path, &data).unwrap();

        // Load should fail with checksum mismatch
        let result: Result<Option<TestStruct>, super::PersistenceError> =
            TestModule::<usize>::load_from_disk(&file_path);

        assert!(
            matches!(
                result,
                Err(super::PersistenceError::ChecksumMismatch { .. })
            ),
            "Should detect corrupted data via checksum mismatch"
        );
    }

    #[test_log::test]
    fn test_missing_checksum_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");

        // Write file directly without checksum (simulating legacy)
        let mut file = File::create(&file_path).unwrap();
        let test_struct = TestStruct { value: 42 };
        borsh::to_writer(&mut file, &test_struct).unwrap();

        // Verify no checksum file exists
        let checksum_path = super::checksum_file_path(&file_path);
        assert!(
            !checksum_path.exists(),
            "Checksum file should not exist for legacy file"
        );

        // Load should fail due to missing checksum file
        let result: Result<Option<TestStruct>, super::PersistenceError> =
            TestModule::<usize>::load_from_disk(&file_path);
        assert!(
            matches!(result, Err(super::PersistenceError::MissingChecksumFile)),
            "Should error on missing checksum file"
        );
    }

    #[tokio::test]
    async fn test_build_module() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut handler = ModulesHandler::new(&shared_bus).await;
        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        assert_eq!(handler.modules.len(), 1);
    }

    #[tokio::test]
    async fn test_add_module() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut handler = ModulesHandler::new(&shared_bus).await;
        let module = TestModule {
            bus: TestBusClient::new_from_bus(shared_bus.new_handle()).await,
            _field: 2_usize,
        };

        handler.add_module(module).unwrap();
        assert_eq!(handler.modules.len(), 1);
    }

    #[tokio::test]
    async fn test_start_modules() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;
        handler.build_module::<TestModule<usize>>(()).await.unwrap();

        _ = handler.start_modules().await;
        _ = handler.shutdown_next_module().await;

        assert_eq!(
            shutdown_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }

    // When modules are started in the following order A, B, C, they should be closed in the reverse order C, B, A
    #[tokio::test]
    async fn test_start_stop_modules_in_order() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        _ = handler.start_modules().await;
        _ = handler.shutdown_modules().await;

        // Shutdown last module first
        assert_eq!(
            shutdown_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        // Then first module at last
        assert_eq!(
            shutdown_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }

    #[tokio::test]
    async fn test_shutdown_modules_exactly_once() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut cancellation_counter_receiver = get_receiver::<usize>(&shared_bus).await;
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        handler.build_module::<TestModule<bool>>(()).await.unwrap();

        _ = handler.start_modules().await;
        _ = tokio::time::sleep(Duration::from_millis(100)).await;
        _ = handler.shutdown_modules().await;

        // Shutdown last module first
        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<bool>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        // Then first module at last

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );

        assert_eq!(
            cancellation_counter_receiver
                .try_recv()
                .expect("1")
                .into_message(),
            1
        );
        assert_eq!(
            cancellation_counter_receiver
                .try_recv()
                .expect("1")
                .into_message(),
            1
        );
        assert_eq!(
            cancellation_counter_receiver
                .try_recv()
                .expect("1")
                .into_message(),
            1
        );
    }

    // in case a module fails, it will emit a shutdowncompleted event that will trigger the shutdown loop and shut all other modules
    // All other modules are shut in the right order
    #[tokio::test]
    async fn test_shutdown_all_modules_if_one_fails() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        handler.build_module::<TestModule<u64>>(()).await.unwrap();
        handler.build_module::<TestModule<bool>>(()).await.unwrap();

        _ = handler.start_modules().await;

        // Starting shutdown loop should shut all modules because one failed immediately

        _ = handler.shutdown_loop().await;

        // u64 module fails first, emits two events, one because it is the first task to end,
        // and the other because it finished to shutdown correctly
        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<u64>>().to_string()
        );

        // Shutdown last module first
        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<bool>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        // Then first module at last

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }

    // in case a module panics,
    // the module panic listener will know the task has ended, and will trigger a shutdown completed event
    // the other modules will shut in the right order
    #[tokio::test]
    async fn test_shutdown_all_modules_if_one_module_panics() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        handler.build_module::<TestModule<u32>>(()).await.unwrap();
        handler.build_module::<TestModule<bool>>(()).await.unwrap();

        _ = handler.start_modules().await;

        // Starting shutdown loop should shut all modules because one failed immediately

        _ = handler.shutdown_loop().await;

        // u32 module failed with panic, but the event should be emitted

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<u32>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<bool>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }
}
