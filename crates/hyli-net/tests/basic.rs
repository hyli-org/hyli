#![allow(clippy::all)]
#![cfg(feature = "turmoil")]
#![cfg(test)]

use std::{collections::HashSet, error::Error, time::Duration};

use hyli_crypto::BlstCrypto;
use hyli_net::{
    net::Sim,
    tcp::{p2p_server::P2PServer, p2p_server::P2PTimeouts, Canal, P2PTcpMessage, TcpMessageLabel},
};
use rand::{rngs::StdRng, RngCore, SeedableRng};

#[derive(Clone, Debug, borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct Msg(usize);

impl TcpMessageLabel for Msg {
    fn message_label(&self) -> &'static str {
        "Msg"
    }
}

impl Into<P2PTcpMessage<Msg>> for Msg {
    fn into(self) -> P2PTcpMessage<Msg> {
        P2PTcpMessage::Data(self)
    }
}

macro_rules! turmoil_simple {
    ($seed:literal, $nb:literal, $simulation:ident) => {
        paste::paste! {
        #[test_log::test]
            fn [<turmoil_p2p_ $nb _nodes_ $simulation _ $seed >]() -> anyhow::Result<()> {
                tracing::info!("Starting test {} with seed {}", stringify!([<turmoil_ $simulation _ $seed >]), $seed);
                let mut sim = hyli_net::turmoil::Builder::new()
                    .simulation_duration(Duration::from_secs(50))
                    .tick_duration(Duration::from_millis(50))
                    .enable_tokio_io()
                    .rng_seed($seed)
                    .build();

                let mut peers = vec![];

                for i in 1..($nb+1) {
                    peers.push(format!("peer-{}", i));
                }

                $simulation(peers, &mut sim, $seed)?;

                Ok(())
            }
        }
    };

    ($seed_from:literal..=$seed_to:literal, $nb:literal, $simulation:ident) => {
        seq_macro::seq!(SEED in $seed_from..=$seed_to {
            turmoil_simple!(SEED, $nb, $simulation);
        });
    };
}

turmoil_simple!(501..=520, 4, setup_basic);
turmoil_simple!(501..=520, 10, setup_basic);
turmoil_simple!(501..=520, 4, setup_drops);
turmoil_simple!(521..=540, 10, setup_drops);

turmoil_simple!(521..=540, 10, setup_late_host_at_first_handshake);
turmoil_simple!(701..=710, 2, setup_decode_error_reconnect);
turmoil_simple!(702..=711, 2, setup_poisoned_socket_skip_send);

async fn setup_basic_host(
    peer: String,
    peers: Vec<String>,
    connect_to_others: bool,
    _seed: u64,
) -> Result<(), Box<dyn Error>> {
    let crypto = BlstCrypto::new(peer.clone().as_str())?;
    let mut p2p = P2PServer::<Msg>::new(
        std::sync::Arc::new(crypto),
        peer.clone(),
        9090,
        None,
        format!("{}:{}", peer, 9090),
        format!("{}:{}", peer, 4141),
        HashSet::from_iter(std::iter::once(Canal::new("A"))),
        P2PTimeouts::default(),
    )
    .await?;

    let all_other_peers: HashSet<String> =
        HashSet::from_iter(peers.clone().into_iter().filter(|p| p != &peer));

    tracing::info!("All other peers {:?}", all_other_peers);

    if connect_to_others {
        for peer in all_other_peers.clone() {
            let _ = p2p.try_start_connection(format!("{}:{}", peer.clone(), 9090), Canal::new("A"));
        }
    }

    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        hyli_turmoil_shims::tokio_select_biased! {
            _ = interval.tick() => {
                let peer_names = HashSet::from_iter(p2p.peers.iter().map(|(_, v)| v.node_connection_data.name.clone()));

                if peer_names == all_other_peers {
                    break {
                        tracing::info!("Breaking {:?}", peer_names);
                        Ok(())
                    };
                }
            }
            tcp_event = p2p.listen_next() => {
                _ = p2p.handle_p2p_tcp_event(tcp_event).await;
            }
        }
    }
}

pub fn setup_basic(peers: Vec<String>, sim: &mut Sim<'_>, seed: u64) -> anyhow::Result<()> {
    tracing::info!("Starting simulation with peers {:?}", peers.clone());
    for peer in peers.clone().into_iter() {
        let peer_clone = peer.clone();
        let peers_clone = peers.clone();
        sim.client(peer.clone(), async move {
            setup_basic_host(peer_clone, peers_clone, true, seed).await
        })
    }

    sim.run()
        .map_err(|e| anyhow::anyhow!("Simulation error {}", e.to_string()))?;

    Ok(())
}

pub fn setup_late_host_at_first_handshake(
    peers: Vec<String>,
    sim: &mut Sim<'_>,
    seed: u64,
) -> anyhow::Result<()> {
    tracing::info!("Starting simulation with peers {:?}", peers.clone());
    let mut rng = StdRng::seed_from_u64(seed);
    let late_host = peers.get((rng.next_u64() as usize) % peers.len()).unwrap();

    for peer in peers.clone().into_iter() {
        let late = rng.next_u64() % 12;
        let peer_clone = peer.clone();
        let peers_clone = peers.clone();
        let late_clone = late_host.clone();
        sim.client(peer.clone(), async move {
            if peer_clone == *late_clone {
                tokio::time::sleep(Duration::from_secs(15)).await;
                setup_basic_host(peer_clone, peers_clone, false, seed).await
            } else {
                tokio::time::sleep(Duration::from_secs(late)).await;
                setup_basic_host(peer_clone, peers_clone, true, seed).await
            }
        })
    }

    sim.run()
        .map_err(|e| anyhow::anyhow!("Simulation error {}", e.to_string()))?;

    Ok(())
}

async fn setup_drop_host(
    peer: String,
    peers: Vec<String>,
    duration: u64,
) -> Result<(), Box<dyn Error>> {
    let crypto = BlstCrypto::new(peer.clone().as_str())?;
    let mut p2p = P2PServer::new(
        std::sync::Arc::new(crypto),
        peer.clone(),
        9090,
        None,
        format!("{}:{}", peer, 9090),
        format!("{}:{}", peer, 4141),
        HashSet::from_iter(std::iter::once(Canal::new("A"))),
        P2PTimeouts::default(),
    )
    .await?;

    let all_other_peers: HashSet<String> =
        HashSet::from_iter(peers.clone().into_iter().filter(|p| p != &peer));

    tracing::info!("All other peers {:?}", all_other_peers);

    for peer in all_other_peers.clone() {
        let _ = p2p.try_start_connection(format!("{}:{}", peer.clone(), 9090), Canal::new("A"));
    }

    let mut interval_broadcast = tokio::time::interval(Duration::from_millis(50));
    let mut interval_start_shutdown = tokio::time::interval(Duration::from_millis(1000));
    interval_start_shutdown.tick().await;
    loop {
        hyli_turmoil_shims::tokio_select_biased! {
            _ = interval_start_shutdown.tick() => {
                if turmoil::elapsed() > Duration::from_millis(duration) {
                    tracing::error!("Current peers {:?}", p2p.peers.keys());
                    tracing::error!("Current tcp peers {:?}", p2p.tcp_server.connected_clients());

                    // Peers map should match all_other_peers
                    assert_eq!(all_other_peers.len(), p2p.peers.keys().len());
                    // All current peer sockets should be in tcp server sockets
                    let connected_tcp_clients = p2p.tcp_server.connected_clients().clone();
                    assert!(p2p.peers.values().flat_map(|t| t.canals.values()).all(|v| connected_tcp_clients.contains(&v.socket_addr)));
                }
            }
            tcp_event = p2p.listen_next() => {
                _ = p2p.handle_p2p_tcp_event(tcp_event).await;
            }
            _ = interval_broadcast.tick() => {
                p2p.broadcast(Msg(10), Canal::new("A"));
            }
        }
    }
}
async fn setup_drop_client(
    peer: String,
    peers: Vec<String>,
    duration: u64,
) -> Result<(), Box<dyn Error>> {
    let crypto = BlstCrypto::new(peer.clone().as_str())?;
    let mut p2p = P2PServer::new(
        std::sync::Arc::new(crypto),
        peer.clone(),
        9090,
        None,
        format!("{}:{}", peer, 9090),
        format!("{}:{}", peer, 4141),
        HashSet::from_iter(std::iter::once(Canal::new("A"))),
        P2PTimeouts::default(),
    )
    .await?;

    let all_other_peers: HashSet<String> =
        HashSet::from_iter(peers.clone().into_iter().filter(|p| p != &peer));

    tracing::info!("All other peers {:?}", all_other_peers);

    for peer in all_other_peers.clone() {
        let _ = p2p.try_start_connection(format!("{}:{}", peer.clone(), 9090), Canal::new("A"));
    }

    let mut interval_broadcast = tokio::time::interval(Duration::from_millis(100));
    let mut interval_start_shutdown = tokio::time::interval(Duration::from_millis(1000));
    interval_start_shutdown.tick().await;
    loop {
        hyli_turmoil_shims::tokio_select_biased! {
            _ = interval_start_shutdown.tick() => {

                if turmoil::elapsed() > Duration::from_millis(duration) {

                    // Peers map should match all_other_peers
                    assert_eq!(all_other_peers.len(), p2p.peers.keys().len());
                    // All current peer sockets should be in tcp server sockets
                    let connected_tcp_clients = p2p.tcp_server.connected_clients().clone();
                    assert!(p2p.peers.values().flat_map(|t| t.canals.values()).all(|v| connected_tcp_clients.contains(&v.socket_addr)));

                    break Ok(())
                }
            }

            tcp_event = p2p.listen_next() => {
                _ = p2p.handle_p2p_tcp_event(tcp_event).await;
            }
            _ = interval_broadcast.tick() => {
                p2p.broadcast(Msg(10), Canal::new("A"));
            }
        }
    }
}
pub fn setup_drops(peers: Vec<String>, sim: &mut Sim<'_>, seed: u64) -> anyhow::Result<()> {
    tracing::info!("Starting simulation with peers {:?}", peers.clone());

    // Nb of node kills, every second
    let nb_drops = 10;

    let sim_duration: u64 = (nb_drops * 1000 + peers.len() * 1500 + 5000) as u64;
    let mut host_peers = peers.clone();
    let client_peer = host_peers.pop().unwrap();

    // Setup hosts (long running, restarts, network issues)
    for peer in host_peers.clone().into_iter() {
        let peer_clone = peer.clone();
        let peers_clone = peers.clone();

        sim.host(peer.clone(), move || {
            let peer_clone = peer_clone.clone();
            let peers_clone = peers_clone.clone();
            async move { setup_drop_host(peer_clone, peers_clone, sim_duration).await }
        });
    }

    let cloned_client_peer = client_peer.clone();
    let cloned_peers = peers.clone();

    // Setup client (check all peers are here at the end of the simulation)
    sim.client(client_peer.clone(), async move {
        setup_drop_client(cloned_client_peer, cloned_peers, sim_duration).await
    });

    // Choose 2 random hosts
    let mut gen_hosts_couple = {
        let mut rng = StdRng::seed_from_u64(seed);
        let peers_len = host_peers.len() as u64;
        move || {
            let peerss = host_peers.clone();
            let n1 = rng.next_u64() % peers_len;

            let n2 = loop {
                let n = rng.next_u64() % peers_len;

                if n != n1 {
                    break n;
                }
            };

            (
                peerss.get(n1 as usize).unwrap().clone(),
                peerss.get(n2 as usize).unwrap().clone(),
            )
        }
    };

    let mut last_trigger = Duration::from_secs(0);
    let mut last_couple = gen_hosts_couple();

    let mut drops = 0;

    loop {
        let step = sim
            .step()
            .map_err(|e| anyhow::anyhow!("Simulation error {}", e.to_string()))?;

        if sim.elapsed().abs_diff(last_trigger) > Duration::from_secs(1) && drops < nb_drops {
            tracing::error!("Repair {} {}", last_couple.0.clone(), last_couple.1.clone());
            sim.repair(last_couple.0.clone(), last_couple.1.clone());

            // regen couple
            last_couple = gen_hosts_couple();
            if drops < nb_drops - 1 {
                sim.partition(last_couple.0.clone(), last_couple.1.clone());
                tracing::error!(
                    "Partition {} {}",
                    last_couple.0.clone(),
                    last_couple.1.clone()
                );
            }

            // bounce
            sim.bounce(last_couple.0.clone());
            tracing::error!("Bounce {}", last_couple.0.clone());

            last_trigger = sim.elapsed();
            drops += 1;
        }

        if step {
            return Ok(());
        }
    }
}

async fn setup_decode_error_host(peer: String, peers: Vec<String>) -> Result<(), Box<dyn Error>> {
    let crypto = BlstCrypto::new(peer.clone().as_str())?;
    let mut p2p = P2PServer::<Msg>::new(
        std::sync::Arc::new(crypto),
        peer.clone(),
        9090,
        None,
        format!("{}:{}", peer, 9090),
        format!("{}:{}", peer, 4141),
        HashSet::from_iter(std::iter::once(Canal::new("A"))),
        P2PTimeouts::default(),
    )
    .await?;

    let all_other_peers: HashSet<String> =
        HashSet::from_iter(peers.clone().into_iter().filter(|p| p != &peer));

    for peer in all_other_peers.clone() {
        let _ = p2p.try_start_connection(format!("{}:{}", peer.clone(), 9090), Canal::new("A"));
    }

    let mut interval = tokio::time::interval(Duration::from_millis(50));
    let start = tokio::time::Instant::now();
    let mut armed = false;
    let mut sent_error = false;
    let mut saw_disconnect = false;
    let mut reconnected = false;
    let deadline = start + Duration::from_secs(12);

    loop {
        hyli_turmoil_shims::tokio_select_biased! {
            _ = interval.tick() => {
                if !armed && start.elapsed() > Duration::from_secs(1) && p2p.peers.len() == all_other_peers.len() {
                    armed = true;
                }

                if armed && !sent_error && peer == "peer-1" {
                    if let Some(socket) = p2p.tcp_server.connected_clients().first().cloned() {
                        let errors = p2p
                            .tcp_server
                            .raw_send_parallel(vec![socket], vec![255], vec![], "raw")
                            .await;
                        assert!(errors.is_empty(), "Expected raw send to succeed");
                        sent_error = true;
                    }
                }

                if armed && reconnected && (peer != "peer-1" || sent_error) {
                    break Ok(());
                }

                if tokio::time::Instant::now() > deadline {
                    break Err("Timed out waiting for reconnection".into());
                }
            }
            tcp_event = p2p.listen_next() => {
                if armed {
                    if matches!(
                        tcp_event,
                        hyli_net::tcp::p2p_server::P2PTcpEvent::TcpEvent(
                            hyli_net::tcp::TcpEvent::Error { .. }
                                | hyli_net::tcp::TcpEvent::Closed { .. }
                        )
                    ) {
                        saw_disconnect = true;
                    }

                    if saw_disconnect
                        && matches!(
                            tcp_event,
                            hyli_net::tcp::p2p_server::P2PTcpEvent::HandShakeTcpClient(_, _, _)
                                | hyli_net::tcp::p2p_server::P2PTcpEvent::TcpEvent(
                                    hyli_net::tcp::TcpEvent::Message {
                                        data: P2PTcpMessage::Handshake(_),
                                        ..
                                    }
                                )
                        )
                    {
                        reconnected = true;
                    }
                }
                _ = p2p.handle_p2p_tcp_event(tcp_event).await;
            }
        }
    }
}

pub fn setup_decode_error_reconnect(
    peers: Vec<String>,
    sim: &mut Sim<'_>,
    _seed: u64,
) -> anyhow::Result<()> {
    tracing::info!("Starting simulation with peers {:?}", peers.clone());
    for peer in peers.clone().into_iter() {
        let peer_clone = peer.clone();
        let peers_clone = peers.clone();
        sim.client(peer.clone(), async move {
            setup_decode_error_host(peer_clone, peers_clone).await
        })
    }

    sim.run()
        .map_err(|e| anyhow::anyhow!("Simulation error {}", e.to_string()))?;

    Ok(())
}

async fn setup_poisoned_socket_host(
    peer: String,
    peers: Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let crypto = BlstCrypto::new(peer.clone().as_str())?;
    let mut p2p = P2PServer::<Msg>::new(
        std::sync::Arc::new(crypto),
        peer.clone(),
        9090,
        None,
        format!("{}:{}", peer, 9090),
        format!("{}:{}", peer, 4141),
        HashSet::from_iter(std::iter::once(Canal::new("A"))),
        P2PTimeouts::default(),
    )
    .await?;

    let all_other_peers: HashSet<String> =
        HashSet::from_iter(peers.clone().into_iter().filter(|p| p != &peer));

    for peer in all_other_peers.clone() {
        let _ = p2p.try_start_connection(format!("{}:{}", peer.clone(), 9090), Canal::new("A"));
    }

    let mut interval = tokio::time::interval(Duration::from_millis(50));
    let start = tokio::time::Instant::now();
    let mut sent_error = false;
    let mut sent_error_at = None;
    let mut verified_poison = false;
    let mut send_skipped = false;
    let deadline = start + Duration::from_secs(12);
    let mut other_pubkey = None;

    loop {
        hyli_turmoil_shims::tokio_select_biased! {
            _ = interval.tick() => {
                if !sent_error
                    && peer == "peer-1"
                    && start.elapsed() > Duration::from_millis(500)
                    && p2p.peers.len() == all_other_peers.len()
                {
                    if let Some(socket) = p2p.tcp_server.connected_clients().first().cloned() {
                        let errors = p2p
                            .tcp_server
                            .raw_send_parallel(vec![socket], vec![255], vec![], "raw")
                            .await;
                        assert!(errors.is_empty(), "Expected raw send to succeed");
                        sent_error = true;
                        sent_error_at = Some(tokio::time::Instant::now());
                    }
                }

                if peer != "peer-1" && verified_poison && send_skipped {
                    break Ok(());
                }
                if peer == "peer-1" {
                    if let Some(sent_at) = sent_error_at {
                        if tokio::time::Instant::now().duration_since(sent_at)
                            > Duration::from_millis(200)
                        {
                            break Ok(());
                        }
                    }
                }

                if tokio::time::Instant::now() > deadline {
                    break Err("Timed out waiting for poisoned socket state".into());
                }
            }
            tcp_event = p2p.listen_next() => {
                let is_disconnect = matches!(
                    tcp_event,
                    hyli_net::tcp::p2p_server::P2PTcpEvent::TcpEvent(
                        hyli_net::tcp::TcpEvent::Error { .. }
                            | hyli_net::tcp::TcpEvent::Closed { .. }
                    )
                );

                _ = p2p.handle_p2p_tcp_event(tcp_event).await;

                if is_disconnect && peer != "peer-1" && !verified_poison {
                    if p2p.peers.is_empty() {
                        continue;
                    }
                    other_pubkey = other_pubkey.or_else(|| p2p.peers.keys().next().cloned());
                    let pubkey = other_pubkey.clone().ok_or("Missing peer pubkey")?;
                    let peer_info = p2p.peers.get(&pubkey).ok_or("Missing peer info")?;
                    let socket = peer_info
                        .canals
                        .get(&Canal::new("A"))
                        .ok_or("Missing peer socket")?;

                    assert!(
                        socket.poisoned_at.is_some(),
                        "Expected socket to be poisoned after error"
                    );
                    assert!(
                        socket.poisoned_at.is_some(),
                        "Expected poisoned_at to be set after error"
                    );
                    verified_poison = true;

                    p2p.send(pubkey, Canal::new("A"), Msg(123)).await?;
                    send_skipped = true;
                }
            }
        }
    }
}

pub fn setup_poisoned_socket_skip_send(
    peers: Vec<String>,
    sim: &mut Sim<'_>,
    _seed: u64,
) -> anyhow::Result<()> {
    tracing::info!("Starting simulation with peers {:?}", peers.clone());
    for peer in peers.clone().into_iter() {
        let peer_clone = peer.clone();
        let peers_clone = peers.clone();
        sim.client(peer.clone(), async move {
            setup_poisoned_socket_host(peer_clone, peers_clone).await
        })
    }

    sim.run()
        .map_err(|e| anyhow::anyhow!("Simulation error {}", e.to_string()))?;

    Ok(())
}
