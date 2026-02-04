use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use hyli_crypto::BlstCrypto;
use hyli_model::{
    Blob, BlobData, BlobTransaction, DataAvailabilityRequest, Signature, SignedByValidator,
    Transaction, ValidatorPublicKey, ValidatorSignature, utils::TimestampMs,
};
use hyli_net::tcp::{Canal, Handshake, NodeConnectionData, P2PTcpMessage};
use rand::Rng;
use rand::prelude::IndexedRandom;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    time::{sleep, timeout},
};
use tracing::{info, warn};

#[derive(Clone, Debug, ValueEnum)]
enum Aggression {
    Low,
    High,
}

#[derive(Clone, Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Optional: pick a specific behavior (otherwise random)
    #[arg(long)]
    behavior: Option<String>,

    /// Min wait before executing behavior (seconds)
    #[arg(long, default_value_t = 5)]
    min_wait_secs: u64,

    /// Max wait before executing behavior (seconds)
    #[arg(long, default_value_t = 30)]
    max_wait_secs: u64,

    /// Aggression level
    #[arg(long, value_enum, default_value_t = Aggression::Low)]
    aggression: Aggression,

    /// REST base URL(s)
    #[arg(long, value_delimiter = ',', default_value = "http://localhost:4321")]
    rest_base_url: Vec<String>,

    /// TCP server address(es) (host:port)
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:1414")]
    tcp_addr: Vec<String>,

    /// P2P server address(es) (host:port)
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:1231")]
    p2p_addr: Vec<String>,

    /// DA server address(es) (host:port)
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:4141")]
    da_addr: Vec<String>,

    /// Disable REST behaviors
    #[arg(long, default_value_t = false)]
    no_rest: bool,

    /// Disable TCP behaviors
    #[arg(long, default_value_t = false)]
    no_tcp: bool,

    /// Disable P2P behaviors
    #[arg(long, default_value_t = false)]
    no_p2p: bool,

    /// Disable DA behaviors
    #[arg(long, default_value_t = false)]
    no_da: bool,

    /// Log format
    #[arg(long, default_value = "json")]
    log_format: String,

    /// If set, tries to sign a P2P handshake using HYLI_VALIDATOR_SECRET
    #[arg(long, default_value_t = false)]
    p2p_valid_handshake: bool,

    /// If set, perform a valid P2P handshake before misbehaving on P2P behaviors
    #[arg(long, default_value_t = false)]
    p2p_handshake_first: bool,
}

#[derive(Clone, Debug)]
struct AggressionProfile {
    connections: usize,
    frames: usize,
    hold_secs: u64,
    block_requests: usize,
    ping_flood_count: usize,
}

impl AggressionProfile {
    fn for_level(level: &Aggression) -> Self {
        match level {
            Aggression::Low => Self {
                connections: 10,
                frames: 5,
                hold_secs: 5,
                block_requests: 5,
                ping_flood_count: 50,
            },
            // Disclaimer: these are _aggressive_.
            Aggression::High => Self {
                connections: 2500,
                frames: 100,
                hold_secs: 10000,
                block_requests: 5000,
                ping_flood_count: 5000,
            },
        }
    }
}

#[derive(Clone, Debug)]
enum BehaviorKind {
    RestInvalidJson,
    RestNodeStateEdge,
    TcpInvalidFrame,
    TcpPartialFrame,
    TcpValidThenPartial,
    P2pInvalidHandshake,
    P2pGarbageFrame,
    P2pPartialFrame,
    P2pHandshakeThenGarbage,
    P2pHandshakeThenPartial,
    P2pHandshakeThenPingFlood,
    P2pConnectionStorm,
    DaNoRead,
    DaBlockRequestSpam,
}

#[derive(Clone, Debug)]
struct Behavior {
    name: &'static str,
    kind: BehaviorKind,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    hyli_modules::utils::logger::setup_tracing(&args.log_format, "chaos_monkey".to_string())
        .expect("Failed to set up tracing");

    if args.min_wait_secs > args.max_wait_secs {
        return Err(anyhow!(
            "min_wait_secs ({}) cannot exceed max_wait_secs ({})",
            args.min_wait_secs,
            args.max_wait_secs
        ));
    }

    let profile = AggressionProfile::for_level(&args.aggression);
    let behaviors = build_behaviors(&args);
    if behaviors.is_empty() {
        return Err(anyhow!("No behaviors enabled (all targets disabled)"));
    }

    let chosen = if let Some(name) = args.behavior.as_deref() {
        behaviors
            .iter()
            .find(|b| b.name == name)
            .cloned()
            .ok_or_else(|| {
                let available = behaviors
                    .iter()
                    .map(|b| b.name)
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow!("Unknown behavior '{name}'. Available: {available}")
            })?
    } else {
        let mut rng = rand::rng();
        behaviors
            .choose(&mut rng)
            .cloned()
            .expect("behaviors non-empty")
    };

    let wait = {
        let mut rng = rand::rng();
        rng.random_range(args.min_wait_secs..=args.max_wait_secs)
    };

    info!(
        behavior = chosen.name,
        aggression = ?args.aggression,
        wait_secs = wait,
        "Chaos monkey selected behavior"
    );
    sleep(Duration::from_secs(wait)).await;

    run_behavior(&args, &profile, &chosen).await?;
    info!(behavior = chosen.name, "Chaos monkey completed behavior");
    Ok(())
}

fn build_behaviors(args: &Args) -> Vec<Behavior> {
    let mut behaviors = Vec::new();
    if !args.no_rest {
        behaviors.push(Behavior {
            name: "rest_invalid_json",
            kind: BehaviorKind::RestInvalidJson,
        });
        behaviors.push(Behavior {
            name: "rest_node_state_edge",
            kind: BehaviorKind::RestNodeStateEdge,
        });
    }
    if !args.no_tcp {
        behaviors.push(Behavior {
            name: "tcp_invalid_frame",
            kind: BehaviorKind::TcpInvalidFrame,
        });
        behaviors.push(Behavior {
            name: "tcp_partial_frame",
            kind: BehaviorKind::TcpPartialFrame,
        });
        behaviors.push(Behavior {
            name: "tcp_valid_then_partial",
            kind: BehaviorKind::TcpValidThenPartial,
        });
    }
    if !args.no_p2p {
        behaviors.push(Behavior {
            name: "p2p_invalid_handshake",
            kind: BehaviorKind::P2pInvalidHandshake,
        });
        behaviors.push(Behavior {
            name: "p2p_garbage_frame",
            kind: BehaviorKind::P2pGarbageFrame,
        });
        behaviors.push(Behavior {
            name: "p2p_partial_frame",
            kind: BehaviorKind::P2pPartialFrame,
        });
        behaviors.push(Behavior {
            name: "p2p_handshake_then_garbage",
            kind: BehaviorKind::P2pHandshakeThenGarbage,
        });
        behaviors.push(Behavior {
            name: "p2p_handshake_then_partial",
            kind: BehaviorKind::P2pHandshakeThenPartial,
        });
        behaviors.push(Behavior {
            name: "p2p_handshake_then_ping_flood",
            kind: BehaviorKind::P2pHandshakeThenPingFlood,
        });
        behaviors.push(Behavior {
            name: "p2p_connection_storm",
            kind: BehaviorKind::P2pConnectionStorm,
        });
    }
    if !args.no_da {
        behaviors.push(Behavior {
            name: "da_no_read",
            kind: BehaviorKind::DaNoRead,
        });
        behaviors.push(Behavior {
            name: "da_block_request_spam",
            kind: BehaviorKind::DaBlockRequestSpam,
        });
    }
    behaviors
}

async fn run_behavior(args: &Args, profile: &AggressionProfile, behavior: &Behavior) -> Result<()> {
    let mut rng = rand::rng();
    let rest_base_url = choose_addr(&args.rest_base_url, &mut rng);
    let tcp_addr = choose_addr(&args.tcp_addr, &mut rng);
    let p2p_addr = choose_addr(&args.p2p_addr, &mut rng);
    let da_addr = choose_addr(&args.da_addr, &mut rng);

    match behavior.kind {
        BehaviorKind::RestInvalidJson => rest_invalid_json(rest_base_url).await,
        BehaviorKind::RestNodeStateEdge => rest_node_state_edge(rest_base_url).await,
        BehaviorKind::TcpInvalidFrame => tcp_invalid_frame(tcp_addr, profile).await,
        BehaviorKind::TcpPartialFrame => tcp_partial_frame(tcp_addr, profile).await,
        BehaviorKind::TcpValidThenPartial => tcp_valid_then_partial(tcp_addr, profile).await,
        BehaviorKind::P2pInvalidHandshake => {
            p2p_invalid_handshake(p2p_addr, args.p2p_valid_handshake).await
        }
        BehaviorKind::P2pGarbageFrame => {
            p2p_garbage_frame(p2p_addr, profile, args.p2p_handshake_first).await
        }
        BehaviorKind::P2pPartialFrame => {
            p2p_partial_frame(p2p_addr, profile, args.p2p_handshake_first).await
        }
        BehaviorKind::P2pHandshakeThenGarbage => p2p_garbage_frame(p2p_addr, profile, true).await,
        BehaviorKind::P2pHandshakeThenPartial => p2p_partial_frame(p2p_addr, profile, true).await,
        BehaviorKind::P2pHandshakeThenPingFlood => {
            p2p_handshake_then_ping_flood(p2p_addr, profile).await
        }
        BehaviorKind::P2pConnectionStorm => p2p_connection_storm(p2p_addr, profile).await,
        BehaviorKind::DaNoRead => da_no_read(da_addr, profile).await,
        BehaviorKind::DaBlockRequestSpam => da_block_request_spam(da_addr, profile).await,
    }
}

async fn rest_invalid_json(base_url: &str) -> Result<()> {
    let base_url = base_url.to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .build();
        let agent = ureq::Agent::new_with_config(config);

        let endpoints = [
            "/v1/tx/send/blob",
            "/v1/tx/send/proof",
            "/v1/contract/register",
        ];
        for path in endpoints {
            let url = format!("{base_url}{path}");
            let resp = agent.post(&url).content_type("application/json").send("{");
            match resp {
                Ok(r) => {
                    let status: u16 = r.status().as_u16();
                    info!(
                        behavior = "rest_invalid_json",
                        target = "rest",
                        addr = %base_url,
                        url,
                        status,
                        "ok"
                    );
                }
                Err(e) => {
                    warn!(
                        behavior = "rest_invalid_json",
                        target = "rest",
                        addr = %base_url,
                        url,
                        error = %e,
                        "error"
                    );
                }
            }
        }
        Ok(())
    })
    .await?
}

async fn rest_node_state_edge(base_url: &str) -> Result<()> {
    let base_url = base_url.to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .build();
        let agent = ureq::Agent::new_with_config(config);

        let random_contract = format!("chaos_{}", random_hex(6));
        let random_hash = random_hex(32);
        let endpoints = [
            format!("{base_url}/v1/contract/{random_contract}"),
            format!("{base_url}/v1/contract/{random_contract}/settled_height"),
            format!("{base_url}/v1/unsettled_tx/{random_hash}"),
            format!("{base_url}/v1/unsettled_txs_count"),
        ];

        for url in endpoints {
            let resp = agent.get(&url).call();
            match resp {
                Ok(r) => {
                    let status: u16 = r.status().as_u16();
                    info!(
                        behavior = "rest_node_state_edge",
                        target = "rest",
                        addr = %base_url,
                        url,
                        status,
                        "ok"
                    );
                }
                Err(e) => {
                    warn!(
                        behavior = "rest_node_state_edge",
                        target = "rest",
                        addr = %base_url,
                        url,
                        error = %e,
                        "error"
                    );
                }
            }
        }
        Ok(())
    })
    .await?
}

async fn tcp_invalid_frame(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    for i in 0..profile.frames {
        let payload = random_bytes(128);
        send_frame(&mut stream, &payload).await?;
        info!(
            behavior = "tcp_invalid_frame",
            target = "tcp",
            addr = %addr,
            frame_idx = i,
            bytes = payload.len(),
            "sent invalid frame"
        );
    }
    Ok(())
}

async fn tcp_partial_frame(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    let declared_len = 1024 * 1024u32;
    stream
        .write_all(&declared_len.to_be_bytes())
        .await
        .context("writing length prefix")?;
    stream
        .write_all(&random_bytes(128))
        .await
        .context("writing partial payload")?;
    info!(
        behavior = "tcp_partial_frame",
        target = "tcp",
        addr = %addr,
        declared_len = declared_len,
        "sent partial frame, holding connection"
    );
    sleep(Duration::from_secs(profile.hold_secs)).await;
    Ok(())
}

async fn tcp_valid_then_partial(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    let tx = minimal_blob_tx();
    let msg = client_sdk::tcp_client::TcpServerMessage::NewTx(Transaction::from(tx));
    let payload = borsh::to_vec(&msg).context("serializing tcp message")?;
    send_frame(&mut stream, &payload).await?;
    info!(
        behavior = "tcp_valid_then_partial",
        target = "tcp",
        addr = %addr,
        bytes = payload.len(),
        "sent valid-ish tcp message"
    );

    let declared_len = 1024 * 1024u32;
    stream
        .write_all(&declared_len.to_be_bytes())
        .await
        .context("writing length prefix")?;
    stream
        .write_all(&random_bytes(64))
        .await
        .context("writing partial payload")?;
    info!(
        behavior = "tcp_valid_then_partial",
        target = "tcp",
        addr = %addr,
        declared_len = declared_len,
        "sent partial frame after valid message"
    );
    sleep(Duration::from_secs(profile.hold_secs)).await;
    Ok(())
}

async fn p2p_connection_storm(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut handles = Vec::new();
    for i in 0..profile.connections {
        let addr = addr.to_string();
        handles.push(tokio::spawn(async move {
            let res = connect_with_timeout(&addr).await;
            match res {
                Ok(_stream) => {
                    info!(
                        behavior = "p2p_connection_storm",
                        target = "p2p",
                        addr = %addr,
                        conn_idx = i,
                        "connected"
                    );
                }
                Err(e) => {
                    warn!(
                        behavior = "p2p_connection_storm",
                        target = "p2p",
                        addr = %addr,
                        conn_idx = i,
                        error = %e,
                        "connect failed"
                    );
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

async fn p2p_garbage_frame(
    addr: &str,
    profile: &AggressionProfile,
    handshake_first: bool,
) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    if handshake_first {
        p2p_handshake_first(&mut stream, addr).await?;
    }
    for i in 0..profile.frames {
        let payload = random_bytes(64);
        send_frame(&mut stream, &payload).await?;
        info!(
            behavior = "p2p_garbage_frame",
            target = "p2p",
            addr = %addr,
            frame_idx = i,
            bytes = payload.len(),
            "sent garbage frame"
        );
    }
    Ok(())
}

async fn p2p_partial_frame(
    addr: &str,
    profile: &AggressionProfile,
    handshake_first: bool,
) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    if handshake_first {
        p2p_handshake_first(&mut stream, addr).await?;
    }
    let declared_len = 2 * 1024 * 1024u32;
    stream
        .write_all(&declared_len.to_be_bytes())
        .await
        .context("writing length prefix")?;
    stream
        .write_all(&random_bytes(64))
        .await
        .context("writing partial payload")?;
    info!(
        behavior = "p2p_partial_frame",
        target = "p2p",
        addr = %addr,
        declared_len = declared_len,
        "sent partial frame, holding connection"
    );
    sleep(Duration::from_secs(profile.hold_secs)).await;
    Ok(())
}

async fn p2p_invalid_handshake(addr: &str, valid_handshake: bool) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    let canal = Canal::new("mempool");
    let timestamp = now_timestamp_ms();

    let signed = if valid_handshake {
        let crypto = BlstCrypto::new("chaos_monkey")
            .context("initializing BlstCrypto for valid handshake")?;
        let data = NodeConnectionData {
            version: 1,
            name: "chaos_monkey".to_string(),
            current_height: 0,
            p2p_public_address: "0.0.0.0:0".to_string(),
            da_public_address: "0.0.0.0:0".to_string(),
            start_timestamp: timestamp.clone(),
        };
        crypto.sign(data)?
    } else {
        fake_signed_node_data(timestamp.clone())
    };

    let msg: P2PTcpMessage<Vec<u8>> =
        P2PTcpMessage::Handshake(Handshake::Hello((canal, signed, timestamp)));
    let payload = borsh::to_vec(&msg).context("serializing P2P handshake")?;
    send_frame(&mut stream, &payload).await?;
    info!(
        behavior = "p2p_invalid_handshake",
        target = "p2p",
        addr = %addr,
        valid_handshake,
        bytes = payload.len(),
        "sent handshake"
    );
    Ok(())
}

async fn p2p_handshake_first(stream: &mut TcpStream, addr: &str) -> Result<()> {
    let crypto =
        BlstCrypto::new("chaos_monkey").context("initializing BlstCrypto for P2P handshake")?;
    let canal = Canal::new("mempool");
    let timestamp = now_timestamp_ms();
    let data = NodeConnectionData {
        version: 1,
        name: "chaos_monkey".to_string(),
        current_height: 0,
        p2p_public_address: "0.0.0.0:0".to_string(),
        da_public_address: "0.0.0.0:0".to_string(),
        start_timestamp: timestamp.clone(),
    };
    let signed = crypto.sign(data)?;
    let msg: P2PTcpMessage<Vec<u8>> =
        P2PTcpMessage::Handshake(Handshake::Hello((canal, signed, timestamp)));
    let payload = borsh::to_vec(&msg).context("serializing P2P handshake")?;
    send_frame(stream, &payload).await?;

    match timeout(Duration::from_secs(3), read_frame(stream)).await {
        Ok(Ok(bytes)) => {
            if let Ok(parsed) = borsh::from_slice::<P2PTcpMessage<Vec<u8>>>(&bytes) {
                match parsed {
                    P2PTcpMessage::Handshake(Handshake::Verack(_)) => {
                        info!(
                            behavior = "p2p_handshake_first",
                            target = "p2p",
                            addr = %addr,
                            "received verack"
                        );
                    }
                    _ => {
                        info!(
                            behavior = "p2p_handshake_first",
                            target = "p2p",
                            addr = %addr,
                            "received non-verack response"
                        );
                    }
                }
            } else {
                warn!(
                    behavior = "p2p_handshake_first",
                    target = "p2p",
                    addr = %addr,
                    "received non-decodable response"
                );
            }
        }
        Ok(Err(e)) => {
            warn!(
                behavior = "p2p_handshake_first",
                target = "p2p",
                addr = %addr,
                error = %e,
                "failed to read handshake response"
            );
        }
        Err(_) => {
            warn!(
                behavior = "p2p_handshake_first",
                target = "p2p",
                addr = %addr,
                "handshake response timed out"
            );
        }
    }

    Ok(())
}

async fn p2p_handshake_then_ping_flood(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    p2p_handshake_first(&mut stream, addr).await?;
    for i in 0..profile.ping_flood_count {
        send_ping_frame(&mut stream).await?;
        info!(
            behavior = "p2p_handshake_then_ping_flood",
            target = "p2p",
            addr = %addr,
            ping_idx = i,
            "sent ping"
        );
    }
    Ok(())
}

async fn da_no_read(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    let req = DataAvailabilityRequest::StreamFromHeight(hyli_model::BlockHeight(0));
    let payload = borsh::to_vec(&req).context("serializing DA request")?;
    send_frame(&mut stream, &payload).await?;
    info!(
        behavior = "da_no_read",
        target = "da",
        addr = %addr,
        hold_secs = profile.hold_secs,
        "sent StreamFromHeight and holding without reading"
    );
    sleep(Duration::from_secs(profile.hold_secs)).await;
    Ok(())
}

async fn da_block_request_spam(addr: &str, profile: &AggressionProfile) -> Result<()> {
    let mut stream = connect_with_timeout(addr).await?;
    for i in 0..profile.block_requests {
        let height = random_u64(1_000_000_000);
        let req = DataAvailabilityRequest::BlockRequest(hyli_model::BlockHeight(height));
        let payload = borsh::to_vec(&req).context("serializing DA request")?;
        send_frame(&mut stream, &payload).await?;
        info!(
            behavior = "da_block_request_spam",
            target = "da",
            addr = %addr,
            request_idx = i,
            height = height,
            bytes = payload.len(),
            "sent block request"
        );
    }
    Ok(())
}

async fn connect_with_timeout(addr: &str) -> Result<TcpStream> {
    timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .context("tcp connect timeout")?
        .context("tcp connect error")
}

async fn send_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .context("writing length prefix")?;
    stream.write_all(payload).await.context("writing payload")?;
    Ok(())
}

async fn send_ping_frame(stream: &mut TcpStream) -> Result<()> {
    send_frame(stream, b"PING").await
}

async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("reading length prefix")?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .context("reading frame payload")?;
    Ok(buf)
}

fn random_hex(bytes: usize) -> String {
    hex::encode(random_bytes(bytes))
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut out = vec![0u8; len];
    rng.fill(out.as_mut_slice());
    out
}

fn random_u64(max: u64) -> u64 {
    let mut rng = rand::rng();
    rng.random_range(0..=max)
}

fn now_timestamp_ms() -> TimestampMs {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0));
    TimestampMs(now.as_millis())
}

fn choose_addr<'a>(addrs: &'a [String], rng: &mut impl Rng) -> &'a str {
    addrs
        .choose(rng)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:0")
}

fn minimal_blob_tx() -> BlobTransaction {
    let blob = Blob {
        contract_name: "a".into(),
        data: BlobData(Vec::new()),
    };
    BlobTransaction::new("a@a", vec![blob])
}

fn fake_signed_node_data(timestamp: TimestampMs) -> SignedByValidator<NodeConnectionData> {
    let mut rng = rand::rng();
    let fake_pubkey = ValidatorPublicKey(random_bytes(48));
    let fake_sig = Signature(random_bytes(96));
    let signature = ValidatorSignature {
        signature: fake_sig,
        validator: fake_pubkey,
    };
    let data = NodeConnectionData {
        version: 1,
        name: format!("chaos-{}", rng.random_range(1000..9999)),
        current_height: rng.random_range(0..1000),
        p2p_public_address: "0.0.0.0:0".to_string(),
        da_public_address: "0.0.0.0:0".to_string(),
        start_timestamp: timestamp,
    };
    SignedByValidator {
        msg: data,
        signature,
    }
}
