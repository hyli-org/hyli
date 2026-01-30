use super::*;
use crate::bus::metrics::BusMetrics;
use crate::modules::block_processor::BusOnlyProcessor;
use crate::node_state::test::craft_signed_block;
use crate::utils::da_codec::DataAvailabilityServer;
use sdk::{BlockHeight, DataAvailabilityEvent, SignedBlock};
use tempfile::tempdir;

/// Helper to create a test configuration
fn create_test_config(fallback_addresses: Vec<String>) -> DAListenerConf<BusOnlyProcessor> {
    create_test_config_with_read_from("test://localhost:1234".to_string(), fallback_addresses)
}

fn create_test_config_with_read_from(
    da_read_from: String,
    fallback_addresses: Vec<String>,
) -> DAListenerConf<BusOnlyProcessor> {
    let temp_dir = tempdir().unwrap();
    DAListenerConf {
        data_directory: temp_dir.path().to_path_buf(),
        da_read_from,
        start_block: Some(BlockHeight(0)),
        timeout_client_secs: 30,
        da_fallback_addresses: fallback_addresses,
        processor_config: (),
    }
}

/// Helper to create a test SignedDAListener
async fn create_test_listener(
    config: DAListenerConf<BusOnlyProcessor>,
) -> Result<SignedDAListener<BusOnlyProcessor>> {
    let bus = SharedMessageBus::new(BusMetrics::global());
    SignedDAListener::build(bus.new_handle(), config).await
}

/// Helper to create a test block at a specific height
fn create_test_block(height: u64) -> SignedBlock {
    craft_signed_block(height, vec![])
}

#[tokio::test]
async fn test_in_order_block_processing() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    for height in 0..5 {
        let block = create_test_block(height);
        listener.process_block(block).await.unwrap();
        assert_eq!(listener.current_block, BlockHeight(height + 1));
    }

    assert!(listener.block_buffer.is_empty());
}

#[tokio::test]
async fn test_out_of_order_blocks_buffered() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    let block3 = create_test_block(3);
    listener.process_block(block3.clone()).await.unwrap();

    assert_eq!(listener.current_block, BlockHeight(0));
    assert_eq!(listener.block_buffer.len(), 1);
    assert!(listener.block_buffer.contains_key(&BlockHeight(3)));
}

#[tokio::test]
async fn test_past_blocks_ignored() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    let block0 = create_test_block(0);
    listener.process_block(block0).await.unwrap();
    let block1 = create_test_block(1);
    listener.process_block(block1).await.unwrap();
    assert_eq!(listener.current_block, BlockHeight(2));

    // Past block should be ignored
    let old_block = create_test_block(0);
    listener.process_block(old_block).await.unwrap();
    assert_eq!(listener.current_block, BlockHeight(2));
}

#[tokio::test]
async fn test_buffered_block_processing() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    // Send blocks out of order
    listener.process_block(create_test_block(2)).await.unwrap();
    listener.process_block(create_test_block(3)).await.unwrap();

    assert_eq!(listener.current_block, BlockHeight(0));
    assert_eq!(listener.block_buffer.len(), 2);

    // Fill the gap
    listener.process_block(create_test_block(0)).await.unwrap();
    listener.process_block(create_test_block(1)).await.unwrap();

    assert_eq!(listener.current_block, BlockHeight(4));
    assert!(listener.block_buffer.is_empty());
}

#[tokio::test]
async fn test_gap_detection_creates_pending_requests() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    listener.process_block(create_test_block(0)).await.unwrap();
    listener.process_block(create_test_block(3)).await.unwrap();

    // Should have requested blocks 1 and 2
    assert!(listener
        .pending_block_requests
        .contains_key(&BlockHeight(1)));
    assert!(listener
        .pending_block_requests
        .contains_key(&BlockHeight(2)));
    assert_eq!(listener.pending_block_requests.len(), 2);
}

#[tokio::test]
async fn test_no_duplicate_requests() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    listener.request_specific_block(BlockHeight(5));
    listener.request_specific_block(BlockHeight(5));

    assert_eq!(listener.pending_block_requests.len(), 1);
}

#[tokio::test]
async fn test_pending_request_cleared_on_block_arrival() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    listener.process_block(create_test_block(0)).await.unwrap();
    listener.process_block(create_test_block(2)).await.unwrap();

    assert!(listener
        .pending_block_requests
        .contains_key(&BlockHeight(1)));

    listener.process_block(create_test_block(1)).await.unwrap();

    assert!(!listener
        .pending_block_requests
        .contains_key(&BlockHeight(1)));
}

#[tokio::test]
async fn test_processing_next_frame_signed_block() {
    let config = create_test_config(vec![]);
    let mut listener = create_test_listener(config).await.unwrap();

    let block = create_test_block(0);
    let event = DataAvailabilityEvent::SignedBlock(block.clone());

    listener.request_specific_block(BlockHeight(0));
    listener.processing_next_frame(event).await.unwrap();

    assert!(!listener
        .pending_block_requests
        .contains_key(&BlockHeight(0)));
    assert_eq!(listener.current_block, BlockHeight(1));
}

#[tokio::test]
async fn test_handle_block_not_found_switches_to_fallback() {
    // Use random high ports to avoid conflicts
    let main_port = 19100 + (std::process::id() % 100) as u16;
    let fallback_port = main_port + 1;

    // Start mock DA servers
    let _main_server: DataAvailabilityServer =
        DataAvailabilityServer::start(main_port, "test_main_da")
            .await
            .unwrap();

    let _fallback: DataAvailabilityServer =
        DataAvailabilityServer::start(fallback_port, "test_fallback")
            .await
            .unwrap();

    // Create listener with fallback address
    let config = create_test_config_with_read_from(
        format!("127.0.0.1:{}", main_port),
        vec![format!("127.0.0.1:{}", fallback_port)],
    );
    let mut listener = create_test_listener(config).await.unwrap();

    // Connect to main server initially
    let mut client = DataAvailabilityClient::connect_with_opts(
        "test_client".to_string(),
        Some(1024 * 1024),
        format!("127.0.0.1:{}", main_port),
    )
    .await
    .unwrap();

    // Create a pending request for block 5
    listener.request_specific_block(BlockHeight(5));
    assert!(listener
        .pending_block_requests
        .contains_key(&BlockHeight(5)));
    assert_eq!(listener.current_da_index, 0);

    // Simulate receiving BlockNotFound from main server
    // This should: remove pending request, switch to fallback, re-request the block
    listener
        .handle_block_not_found(BlockHeight(5), &mut client)
        .await
        .unwrap();

    // Verify: switched to fallback server
    assert_eq!(listener.current_da_index, 1);

    // Verify: block was re-requested (new pending request created)
    assert!(listener
        .pending_block_requests
        .contains_key(&BlockHeight(5)));
}

#[tokio::test]
async fn test_check_block_request_timeouts_switches_to_fallback() {
    use std::time::{Duration, Instant};

    // Use random high ports to avoid conflicts
    let main_port = 19200 + (std::process::id() % 100) as u16;
    let fallback_port = main_port + 1;

    // Start mock DA servers
    let _main_server: DataAvailabilityServer =
        DataAvailabilityServer::start(main_port, "test_main_da")
            .await
            .unwrap();

    let _fallback: DataAvailabilityServer =
        DataAvailabilityServer::start(fallback_port, "test_fallback")
            .await
            .unwrap();

    // Create listener with fallback address
    let config = create_test_config_with_read_from(
        format!("127.0.0.1:{}", main_port),
        vec![format!("127.0.0.1:{}", fallback_port)],
    );
    let mut listener = create_test_listener(config).await.unwrap();

    // Connect to main server initially
    let mut client = DataAvailabilityClient::connect_with_opts(
        "test_client".to_string(),
        Some(1024 * 1024),
        format!("127.0.0.1:{}", main_port),
    )
    .await
    .unwrap();

    // Create a pending request for block 5
    listener.request_specific_block(BlockHeight(5));
    assert_eq!(listener.current_da_index, 0);

    // Simulate timeout by setting request_time to the past
    if let Some(state) = listener.pending_block_requests.get_mut(&BlockHeight(5)) {
        state.request_time = Instant::now() - Duration::from_secs(100);
    }

    // Call check_block_request_timeouts - should detect timeout and switch to fallback
    listener
        .check_block_request_timeouts(&mut client)
        .await
        .unwrap();

    // Verify: switched to fallback server
    assert_eq!(listener.current_da_index, 1);

    // Verify: retry count was incremented
    let state = listener
        .pending_block_requests
        .get(&BlockHeight(5))
        .unwrap();
    assert_eq!(state.retry_count, 1);
}
