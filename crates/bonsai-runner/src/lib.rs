use std::{str::FromStr, time::Duration};

use anyhow::{bail, Result};
use bonsai_sdk::non_blocking::Client;
use borsh::BorshSerialize;
use boundless_market::{
    alloy::{
        primitives::utils::parse_ether, signers::local::PrivateKeySigner,
        transports::http::reqwest::Url,
    },
    client::ClientBuilder,
    contracts::{Input, Offer, Predicate, ProofRequestBuilder, Requirements},
    input::InputBuilder,
    storage::{BuiltinStorageProvider, StorageProvider},
};
use risc0_zkvm::{compute_image_id, default_executor, sha::Digestible, Receipt};
use tracing::info;

pub fn as_input_data<T: BorshSerialize>(data: &T) -> Result<Vec<u8>> {
    let data = borsh::to_vec(&data)?;
    let size = risc0_zkvm::serde::to_vec(&data.len())?;
    let mut input_data = bytemuck::cast_slice(&size).to_vec();
    input_data.extend(data);
    Ok(input_data)
}

pub async fn run_boundless(elf: &[u8], input_data: Vec<u8>) -> Result<Receipt> {
    let offchain = std::env::var("BOUNDLESS_OFFCHAIN").unwrap_or_default() == "true";
    let boundless_market_address = std::env::var("BOUNDLESS_MARKET_ADDRESS").unwrap_or_default();
    let order_stream_url = std::env::var("BOUNDLESS_ORDER_STREAM_URL").ok();
    let wallet_private_key = std::env::var("BOUNDLESS_WALLET_PRIVATE_KEY").unwrap_or_default();
    let rpc_url = std::env::var("BOUNDLESS_RPC_URL").unwrap_or_default();
    let set_verifier_addr = std::env::var("BOUNDLESS_SET_VERIFIER_ADDRESS").ok();

    // Creates a storage provider based on the environment variables.
    //
    // If the environment variable `RISC0_DEV_MODE` is set, a temporary file storage provider is used.
    // Otherwise, the following environment variables are checked in order:
    // - `PINATA_JWT`, `PINATA_API_URL`, `IPFS_GATEWAY_URL`: Pinata storage provider;
    // - `S3_ACCESS`, `S3_SECRET`, `S3_BUCKET`, `S3_URL`, `AWS_REGION`: S3 storage provider.
    // TODO: gcp storage provider
    let storage_provider = BuiltinStorageProvider::from_env().await?;

    let image_url = storage_provider.upload_image(elf).await?;
    info!("Uploaded image to {}", image_url);

    let boundless_market_address = boundless_market::alloy::primitives::Address::parse_checksummed(
        boundless_market_address,
        None,
    )?;
    let order_stream_url = order_stream_url.map(|url| Url::parse(&url)).transpose()?;
    let wallet_private_key = PrivateKeySigner::from_str(&wallet_private_key)?;
    let rpc_url = Url::parse(&rpc_url)?;
    let set_verifier_addr = boundless_market::alloy::primitives::Address::parse_checksummed(
        set_verifier_addr.unwrap_or_default(),
        None,
    )?;

    // Create a Boundless client from the provided parameters.
    let boundless_client = ClientBuilder::new()
        .with_rpc_url(rpc_url)
        .with_boundless_market_address(boundless_market_address)
        .with_set_verifier_address(set_verifier_addr)
        .with_order_stream_url(offchain.then_some(order_stream_url).flatten())
        .with_storage_provider(Some(storage_provider))
        .with_private_key(wallet_private_key)
        .build()
        .await?;

    let balance = boundless_client
        .boundless_market
        .balance_of(boundless_client.local_signer.as_ref().unwrap().address())
        .await?;
    info!("Wallet balance: {}", balance);
    if balance < parse_ether("0.1")? {
        info!("Wallet balance is low, depositing 0.1 ETH");
        boundless_client
            .boundless_market
            .deposit(parse_ether("0.1")?)
            .await?;
    }

    // Encode the input and upload it to the storage provider.
    let input_builder = InputBuilder::new().write_slice(&input_data);
    tracing::info!("input builder: {:?}", input_builder);

    let guest_env = input_builder.clone().build_env()?;
    let guest_env_bytes = guest_env.encode()?;

    // Dry run the ELF with the input to get the journal and cycle count.
    // This can be useful to estimate the cost of the proving request.
    // It can also be useful to ensure the guest can be executed correctly and we do not send into
    // the market unprovable proving requests. If you have a different mechanism to get the expected
    // journal and set a price, you can skip this step.
    let session_info = default_executor().execute(guest_env.try_into().unwrap(), elf)?;
    let mcycles_count = session_info
        .segments
        .iter()
        .map(|segment| 1 << segment.po2)
        .sum::<u64>()
        .div_ceil(1_000_000);
    let journal = session_info.journal;

    // Create a proof request with the image, input, requirements and offer.
    // The ELF (i.e. image) is specified by the image URL.
    // The input can be specified by an URL, as in this example, or can be posted on chain by using
    // the `with_inline` method with the input bytes.
    // The requirements are the image ID and the digest of the journal. In this way, the market can
    // verify that the proof is correct by checking both the committed image id and digest of the
    // journal. The offer specifies the price range and the timeout for the request.
    // Additionally, the offer can also specify:
    // - the bidding start time: the block number when the bidding starts;
    // - the ramp up period: the number of blocks before the price start increasing until reaches
    //   the maxPrice, starting from the the bidding start;
    // - the lockin price: the price at which the request can be locked in by a prover, if the
    //   request is not fulfilled before the timeout, the prover can be slashed.
    // If the input exceeds 2 kB, upload the input and provide its URL instead, as a rule of thumb.
    let request_input = if guest_env_bytes.len() > 2 << 10 {
        let input_url = boundless_client.upload_input(&guest_env_bytes).await?;
        tracing::info!("Uploaded input to {}", input_url);
        Input::url(input_url)
    } else {
        tracing::info!("Sending input inline with request");
        Input::inline(guest_env_bytes.clone())
    };

    let request = ProofRequestBuilder::new()
        .with_image_url(image_url.to_string())
        .with_input(request_input)
        .with_requirements(Requirements::new(
            compute_image_id(elf)?,
            Predicate::digest_match(journal.digest()),
        ))
        .with_offer(
            Offer::default()
                // The market uses a reverse Dutch auction mechanism to match requests with provers.
                // Each request has a price range that a prover can bid on. One way to set the price
                // is to choose a desired (min and max) price per million cycles and multiply it
                // by the number of cycles. Alternatively, you can use the `with_min_price` and
                // `with_max_price` methods to set the price directly.
                .with_min_price_per_mcycle(parse_ether("0.001")?, mcycles_count)
                // NOTE: If your offer is not being accepted, try increasing the max price.
                .with_max_price_per_mcycle(parse_ether("0.05")?, mcycles_count)
                // The timeout is the maximum number of blocks the request can stay
                // unfulfilled in the market before it expires. If a prover locks in
                // the request and does not fulfill it before the timeout, the prover can be
                // slashed.
                .with_timeout(1000)
                .with_lock_timeout(500)
                .with_ramp_up_period(100),
        )
        .build()
        .unwrap();

    // Send the request and wait for it to be completed.
    let (request_id, expires_at) = if offchain {
        boundless_client.submit_request_offchain(&request).await?
    } else {
        boundless_client.submit_request(&request).await?
    };
    tracing::info!("Request 0x{request_id:x} submitted");

    // Wait for the request to be fulfilled by the market, returning the journal and seal.
    tracing::info!("Waiting for 0x{request_id:x} to be fulfilled");
    let (_journal, seal) = boundless_client
        .wait_for_request_fulfillment(request_id, Duration::from_secs(5), expires_at)
        .await?;
    tracing::info!("Request 0x{request_id:x} fulfilled");

    let receipt: Receipt = bincode::deserialize(&seal)?;

    Ok(receipt)
}

pub async fn run_bonsai(elf: &[u8], input_data: Vec<u8>) -> Result<Receipt> {
    let client = Client::from_env(risc0_zkvm::VERSION)?;

    // Compute the image_id, then upload the ELF with the image_id as its key.
    let image_id = hex::encode(compute_image_id(elf)?);
    client.upload_img(&image_id, elf.to_vec()).await?;

    // Prepare input data and upload it.
    let input_id = client.upload_input(input_data).await?;

    // Add a list of assumptions
    let assumptions: Vec<String> = vec![];

    // Wether to run in execute only mode
    let execute_only = false;

    // Start a session running the prover
    let session = client
        .create_session(image_id, input_id, assumptions, execute_only)
        .await?;
    loop {
        let res = session.status(&client).await?;
        if res.status == "RUNNING" {
            info!(
                "Current status: {} - state: {} - continue polling...",
                res.status,
                res.state.unwrap_or_default()
            );
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        if res.status == "SUCCEEDED" {
            // Download the receipt, containing the output
            let receipt_url = res
                .receipt_url
                .expect("API error, missing receipt on completed session");

            let receipt_buf = client.download(&receipt_url).await?;
            let receipt: Receipt = bincode::deserialize(&receipt_buf)?;
            return Ok(receipt);
        } else {
            bail!(
                "Workflow exited: {} - | err: {}",
                res.status,
                res.error_msg.unwrap_or_default()
            );
        }
    }
}
