use crate::bus::{dont_use_this::get_receiver, metrics::BusMetrics};

use super::*;
use crate::bus::SharedMessageBus;
use signal::{ShutdownCompleted, ShutdownModule};
use std::{fs::File, path::PathBuf, sync::Arc};
use tempfile::tempdir;
use tokio::sync::Mutex;

#[derive(Debug, Default, borsh::BorshSerialize, borsh::BorshDeserialize)]
struct TestStruct {
    value: u32,
}

struct TestModule<T> {
    bus: TestBusClient,
    _field: T,
}

#[derive(Clone)]
struct MultiPersistCtx {
    data_dir: PathBuf,
}

struct MultiPersistModule {
    bus: TestBusClient,
    ctx: MultiPersistCtx,
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

fn multi_persist_paths() -> (PathBuf, PathBuf) {
    (PathBuf::from("one.data"), PathBuf::from("two.data"))
}

impl Module for MultiPersistModule {
    type Context = MultiPersistCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        Ok(MultiPersistModule {
            bus: TestBusClient::new_from_bus(bus).await,
            ctx,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
        };
        Ok(())
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let (first_path, second_path) = multi_persist_paths();

        let first_data = TestStruct { value: 1 };
        let second_data = TestStruct { value: 2 };
        let first_checksum = Self::save_on_disk(&self.ctx.data_dir, &first_path, &first_data)?;
        let second_checksum = Self::save_on_disk(&self.ctx.data_dir, &second_path, &second_data)?;
        Ok(vec![
            (self.ctx.data_dir.join(first_path), first_checksum),
            (self.ctx.data_dir.join(second_path), second_checksum),
        ])
    }
}

#[test]
fn test_save_returns_checksum() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let file_path = PathBuf::from("test_file");

    let test_struct = TestStruct { value: 42 };
    let checksum = TestModule::<usize>::save_on_disk(data_dir, &file_path, &test_struct).unwrap();

    // Verify checksum is a valid u32 (non-zero for this data)
    assert!(checksum != 0, "Checksum should be non-zero for test data");
}

#[test_log::test]
fn test_load_with_manifest_verifies_checksum() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let file_path = PathBuf::from("test_file.data");

    // Save file and get checksum
    let test_struct = TestStruct { value: 42 };
    let checksum = TestModule::<usize>::save_on_disk(data_dir, &file_path, &test_struct).unwrap();

    // Write manifest with correct checksum
    let manifest_path = super::manifest_path(data_dir);
    let manifest_content = format!("{} test_file.data\n", super::format_checksum(checksum));
    std::fs::write(&manifest_path, &manifest_content).unwrap();

    // Load should succeed
    let loaded: TestStruct = TestModule::<usize>::load_from_disk(data_dir, &file_path)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.value, 42);
}

#[test_log::test]
fn test_checksum_mismatch_detection() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let file_path = PathBuf::from("test_file.data");

    let test_struct = TestStruct { value: 42 };
    let _checksum = TestModule::<usize>::save_on_disk(data_dir, &file_path, &test_struct).unwrap();

    // Write manifest with wrong checksum
    let manifest_path = super::manifest_path(data_dir);
    std::fs::write(&manifest_path, "00000001 test_file.data\n").unwrap();

    // Load should fail with checksum mismatch
    let result: Result<Option<TestStruct>> =
        TestModule::<usize>::load_from_disk(data_dir, &file_path);

    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<super::PersistenceError>()
            .is_some_and(|e| matches!(e, super::PersistenceError::ChecksumMismatch { .. })),
        "Should detect checksum mismatch"
    );
}

#[test_log::test]
fn test_corrupted_data_detection() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let file_path = PathBuf::from("test_file.data");

    let test_struct = TestStruct { value: 42 };
    let checksum = TestModule::<usize>::save_on_disk(data_dir, &file_path, &test_struct).unwrap();

    // Write manifest with original checksum
    let manifest_path = super::manifest_path(data_dir);
    let manifest_content = format!("{} test_file.data\n", super::format_checksum(checksum));
    std::fs::write(&manifest_path, &manifest_content).unwrap();

    // Corrupt the data file
    let full_path = data_dir.join(&file_path);
    let mut data = std::fs::read(&full_path).unwrap();
    data[0] ^= 0xFF; // Flip some bits
    std::fs::write(&full_path, &data).unwrap();

    // Load should fail with checksum mismatch
    let result: Result<Option<TestStruct>> =
        TestModule::<usize>::load_from_disk(data_dir, &file_path);

    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<super::PersistenceError>()
            .is_some_and(|e| matches!(e, super::PersistenceError::ChecksumMismatch { .. })),
        "Should detect corrupted data via checksum mismatch"
    );
}

#[test_log::test]
fn test_file_not_in_manifest_fails() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let file_path = PathBuf::from("test_file.data");

    // Save file
    let test_struct = TestStruct { value: 42 };
    let _checksum = TestModule::<usize>::save_on_disk(data_dir, &file_path, &test_struct).unwrap();

    // Write manifest without this file
    let manifest_path = super::manifest_path(data_dir);
    std::fs::write(&manifest_path, "00000001 other_file.data\n").unwrap();

    // Load should fail because file is not in manifest
    let result: Result<Option<TestStruct>> =
        TestModule::<usize>::load_from_disk(data_dir, &file_path);
    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<super::PersistenceError>()
            .is_some_and(|e| matches!(e, super::PersistenceError::FileNotInManifest(_))),
        "Should error when file is not in manifest"
    );
}

#[test_log::test]
fn test_no_manifest_fails_to_load() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path();
    let file_path = PathBuf::from("test_file");

    // Write file directly without manifest (simulating no previous clean shutdown)
    let full_path = data_dir.join(&file_path);
    let mut file = File::create(&full_path).unwrap();
    let test_struct = TestStruct { value: 42 };
    borsh::to_writer(&mut file, &test_struct).unwrap();

    // No manifest should fail to load (manifest is required for persisted files)
    let result: Result<Option<TestStruct>> =
        TestModule::<usize>::load_from_disk(data_dir, &file_path);
    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<super::PersistenceError>()
            .is_some_and(|e| matches!(e, super::PersistenceError::FileNotInManifest(_))),
        "Should error when file is not in manifest"
    );
}

#[tokio::test]
async fn test_shutdown_timeout_skips_manifest_and_fails_to_load() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let file_path = PathBuf::from("timeout.data");

    let test_struct = TestStruct { value: 99 };
    let _checksum = TestModule::<usize>::save_on_disk(&data_dir, &file_path, &test_struct).unwrap();

    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();
    handler.build_module::<TestModule<usize>>(()).await.unwrap();

    handler.start_modules().await.unwrap();

    let mut shutdown_client = ShutdownClient::new_from_bus(shared_bus.new_handle()).await;
    let module_id = handler
        .modules_statuses
        .keys()
        .next()
        .cloned()
        .expect("module id");
    let module_name = std::any::type_name::<TestModule<usize>>().to_string();
    let send_task = tokio::spawn(async move {
        _ = shutdown_client.send(signal::ShutdownCompleted {
            module: module_id,
            shutdown_status: ModuleShutdownStatus::TimedOut,
        });
        _ = shutdown_client.send(signal::ShutdownModule {
            module: module_name,
        });
    });

    handler.shutdown_loop().await.unwrap();
    send_task.await.unwrap();

    let manifest = super::manifest_path(&data_dir);
    assert!(
        !manifest.exists(),
        "Manifest should not be written when shutdown times out"
    );

    let result: Result<Option<TestStruct>> =
        TestModule::<usize>::load_from_disk(&data_dir, &file_path);
    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<super::PersistenceError>()
            .is_some_and(|e| matches!(e, super::PersistenceError::FileNotInManifest(_))),
        "Should error when file is not in manifest"
    );
}

#[test_log::test(tokio::test)]
async fn test_multi_file_persist_writes_manifest_and_loads_files() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let (first_path, second_path) = multi_persist_paths();

    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();
    handler
        .build_module::<MultiPersistModule>(MultiPersistCtx {
            data_dir: data_dir.clone(),
        })
        .await
        .unwrap();

    handler.start_modules().await.unwrap();
    handler.shutdown_modules().await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Log the contents of the data_dir for debugging
    let entries = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect::<Vec<_>>();
    info!("data_dir entries: {:?}", entries);

    // Log the first_path for debugging
    info!("first_path: {:?}", data_dir.join(&first_path));

    let manifest_path = super::manifest_path(&data_dir);
    let manifest_content = std::fs::read_to_string(&manifest_path).unwrap();
    assert!(manifest_content.contains("one.data"));
    assert!(manifest_content.contains("two.data"));

    let loaded_first: Option<TestStruct> =
        MultiPersistModule::load_from_disk(&data_dir, &first_path).unwrap();
    assert_eq!(loaded_first.unwrap().value, 1);
    let loaded_second: Option<TestStruct> =
        MultiPersistModule::load_from_disk(&data_dir, &second_path).unwrap();
    assert_eq!(loaded_second.unwrap().value, 2);

    let first_line = manifest_content
        .lines()
        .find(|line| line.ends_with("one.data"))
        .unwrap();
    std::fs::write(&manifest_path, format!("{first_line}\n")).unwrap();

    let missing: Result<Option<TestStruct>> =
        MultiPersistModule::load_from_disk(&data_dir, &second_path);
    let err = missing.unwrap_err();
    assert!(
        err.downcast_ref::<super::PersistenceError>()
            .is_some_and(|e| matches!(e, super::PersistenceError::FileNotInManifest(_))),
        "Should error when second file is missing from manifest"
    );
}

#[tokio::test]
async fn test_build_module() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();
    handler.build_module::<TestModule<usize>>(()).await.unwrap();
    assert_eq!(handler.modules.len(), 1);
}

#[tokio::test]
async fn test_add_module() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();
    let module = TestModule {
        bus: TestBusClient::new_from_bus(shared_bus.new_handle()).await,
        _field: 2_usize,
    };

    handler.add_module(module).unwrap();
    assert_eq!(handler.modules.len(), 1);
}

#[tokio::test]
async fn test_start_modules() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
    let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();
    handler.build_module::<TestModule<usize>>(()).await.unwrap();

    _ = handler.start_modules().await;
    _ = handler.shutdown_next_module().await;

    assert_eq!(
        shutdown_receiver.recv().await.unwrap().module,
        std::any::type_name::<TestModule<usize>>().to_string()
    );

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<usize>>()));
}

// When modules are started in the following order A, B, C, they should be closed in the reverse order C, B, A
#[tokio::test]
async fn test_start_stop_modules_in_order() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
    let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();

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

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<String>>()));

    // Then first module at last
    assert_eq!(
        shutdown_receiver.recv().await.unwrap().module,
        std::any::type_name::<TestModule<usize>>().to_string()
    );

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<usize>>()));
}

#[tokio::test]
async fn test_shutdown_duplicate_modules() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
    let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();

    handler.build_module::<TestModule<usize>>(()).await.unwrap();
    handler.build_module::<TestModule<usize>>(()).await.unwrap();

    _ = handler.start_modules().await;
    _ = handler.shutdown_modules().await;

    let module_name = std::any::type_name::<TestModule<usize>>().to_string();

    assert_eq!(shutdown_receiver.recv().await.unwrap().module, module_name);
    assert_eq!(shutdown_receiver.recv().await.unwrap().module, module_name);
    let first_completed = &shutdown_completed_receiver.recv().await.unwrap().module;
    let second_completed = &shutdown_completed_receiver.recv().await.unwrap().module;
    assert!(first_completed.starts_with(&module_name));
    assert!(second_completed.starts_with(&module_name));
    assert_ne!(first_completed, second_completed);
}

#[tokio::test]
async fn test_shutdown_modules_exactly_once() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut cancellation_counter_receiver = get_receiver::<usize>(&shared_bus).await;
    let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();

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
    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<bool>>()));

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<String>>()));

    // Then first module at last

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<usize>>()));

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
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();

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

    // u64 module fails first and emits shutdown completion
    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<u64>>()));

    // Shutdown last module first
    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<bool>>()));

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<String>>()));

    // Then first module at last

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<usize>>()));
}

// in case a module panics,
// the module panic listener will know the task has ended, and will trigger a shutdown completed event
// the other modules will shut in the right order
#[tokio::test]
async fn test_shutdown_all_modules_if_one_module_panics() {
    let dir = tempdir().unwrap();
    let shared_bus = SharedMessageBus::new(BusMetrics::global());
    let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
    let mut handler = ModulesHandler::new(&shared_bus, dir.path().to_path_buf())
        .await
        .unwrap();

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

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<u32>>()));

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<bool>>()));

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<String>>()));

    assert!(shutdown_completed_receiver
        .recv()
        .await
        .unwrap()
        .module
        .starts_with(std::any::type_name::<TestModule<usize>>()));
}

// Tests for ModulesHandler::new() backup logic

#[tokio::test]
async fn test_new_with_missing_manifest_backs_up_state_files() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Create module state files without manifest
    let node_state_path = data_dir.join(files::NODE_STATE_BIN);
    let consensus_path = data_dir.join(files::CONSENSUS_BIN);
    std::fs::write(&node_state_path, b"fake node state data").unwrap();
    std::fs::write(&consensus_path, b"fake consensus data").unwrap();

    let shared_bus = SharedMessageBus::new(BusMetrics::global());

    // This should back up the directory
    let _handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();

    // Original data_dir should exist (recreated)
    assert!(data_dir.exists());

    // State files should not exist in the new data_dir
    assert!(!node_state_path.exists());
    assert!(!consensus_path.exists());

    // A backup directory should exist
    let parent = data_dir.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data.backup_"))
        .collect();

    assert_eq!(entries.len(), 1, "Should have created exactly one backup");

    // Verify backup contains the original files
    let backup_dir = entries[0].path();
    assert!(backup_dir.join(files::NODE_STATE_BIN).exists());
    assert!(backup_dir.join(files::CONSENSUS_BIN).exists());
}

#[tokio::test]
async fn test_new_with_empty_manifest_backs_up_state_files() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Create module state files
    let node_state_path = data_dir.join(files::NODE_STATE_BIN);
    std::fs::write(&node_state_path, b"fake node state data").unwrap();

    // Create empty manifest
    let manifest_path = super::manifest_path(&data_dir);
    std::fs::write(&manifest_path, "   \n  \n").unwrap(); // Only whitespace

    let shared_bus = SharedMessageBus::new(BusMetrics::global());

    // This should back up the directory because manifest is effectively empty
    let _handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();

    // Original data_dir should exist (recreated)
    assert!(data_dir.exists());

    // State file should not exist in the new data_dir
    assert!(!node_state_path.exists());

    // A backup directory should exist
    let parent = data_dir.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data.backup_"))
        .collect();

    assert_eq!(entries.len(), 1, "Should have created exactly one backup");
}

#[tokio::test]
async fn test_new_without_state_files_does_not_backup() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Create some other file, but not the critical state files
    std::fs::write(data_dir.join("some_other_file.txt"), b"hello").unwrap();

    let shared_bus = SharedMessageBus::new(BusMetrics::global());

    // This should NOT back up because there are no module state files
    let _handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();

    // Original data_dir should still exist
    assert!(data_dir.exists());

    // Other file should still be there
    assert!(data_dir.join("some_other_file.txt").exists());

    // No backup directory should exist
    let parent = data_dir.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data.backup_"))
        .collect();

    assert_eq!(entries.len(), 0, "Should not have created any backup");
}

#[tokio::test]
async fn test_new_with_valid_manifest_does_not_backup() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Create module state files
    let node_state_path = data_dir.join(files::NODE_STATE_BIN);
    std::fs::write(&node_state_path, b"fake node state data").unwrap();

    // Create valid (non-empty) manifest
    let manifest_path = super::manifest_path(&data_dir);
    std::fs::write(&manifest_path, "12345678 node_state.bin\n").unwrap();

    let shared_bus = SharedMessageBus::new(BusMetrics::global());

    // This should NOT back up because manifest is valid
    let _handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();

    // Original data_dir should exist
    assert!(data_dir.exists());

    // State file should still be there
    assert!(node_state_path.exists());

    // Manifest should still be there
    assert!(manifest_path.exists());

    // No backup directory should exist
    let parent = data_dir.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data.backup_"))
        .collect();

    assert_eq!(entries.len(), 0, "Should not have created any backup");
}

#[tokio::test]
async fn test_new_creates_fresh_data_dir() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("fresh_data");

    // data_dir doesn't exist yet
    assert!(!data_dir.exists());

    let shared_bus = SharedMessageBus::new(BusMetrics::global());

    let _handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();

    // data_dir should now exist
    assert!(data_dir.exists());
    assert!(data_dir.is_dir());

    // No backup should have been created
    let parent = data_dir.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("backup"))
        .collect();

    assert_eq!(entries.len(), 0, "Should not have created any backup");
}

#[tokio::test]
async fn test_new_empty_manifest_without_state_files_does_not_backup() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Create empty manifest but NO state files
    let manifest_path = super::manifest_path(&data_dir);
    std::fs::write(&manifest_path, "\n").unwrap();

    let shared_bus = SharedMessageBus::new(BusMetrics::global());

    // This should NOT back up because there are no module state files to protect
    let _handler = ModulesHandler::new(&shared_bus, data_dir.clone())
        .await
        .unwrap();

    // Original data_dir should exist
    assert!(data_dir.exists());

    // No backup directory should exist
    let parent = data_dir.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data.backup_"))
        .collect();

    assert_eq!(entries.len(), 0, "Should not have created any backup");
}
