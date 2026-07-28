// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Hold many cheap fake peers against a single full node to measure how many
//! concurrent peers it accepts.
//!
//! A node gates inbound light peers with `--in-peers-light` and inbound full
//! peers with `--in-peers`. Both limits are checked when the peer opens the
//! `block-announces` substream, so a peer that opens that substream with the
//! right role byte and then does nothing is exactly what the limit counts. That
//! makes a holder far cheaper than a real smoldot client, which needs warp sync
//! and runtime execution before it becomes a peer at all.
//!
//! Each holder is an independent swarm with its own libp2p identity, running the
//! [`Notifications`] behaviour on its own — no discovery, no ping, no identify.
//! An open notification substream is enough to keep the connection alive, so the
//! holder only has to stay polled and drop whatever the node announces to it.
//!
//! The headline number is how many peers were *held* versus how many were
//! offered. Held is counted on the block-announces substream alone: the node
//! refuses that substream without closing the connection, so counting
//! connections would report peers we do not actually have.

use crate::commands::authorities::fetch_genesis_hash;
use crate::commands::light_common::{percentile_ms, Chain};
use futures::{future::join_all, FutureExt, StreamExt};
use jsonrpsee::client_transport::ws::Url;
use libp2p::{identity, swarm::SwarmEvent, Multiaddr, Swarm};
use primitive_types::H256;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subp2p_explorer::{
    notifications::{
        behavior::{Notifications, NotificationsToSwarm, ProtocolsData},
        messages::ProtocolRole,
    },
    BLOCK_ANNOUNCES_INDEX,
};
use tokio::time::Instant as TokioInstant;

/// The role a holder advertises in the block-announces handshake.
///
/// This is the only thing that decides which of the node's limits applies, so it
/// doubles as the correctness check for the tool: the same run against the same
/// node should stop at `--in-peers-light` for [`HoldRole::Light`] and at
/// `--in-peers` for [`HoldRole::Full`].
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum HoldRole {
    Light,
    Full,
    Authority,
}

impl HoldRole {
    fn protocol_role(&self) -> ProtocolRole {
        match self {
            HoldRole::Light => ProtocolRole::LightNode,
            HoldRole::Full => ProtocolRole::FullNode,
            HoldRole::Authority => ProtocolRole::Authority,
        }
    }
}

/// Lock-free counters shared by all holders, read by the orchestrator for the
/// live progress line and the final summary.
#[derive(Default)]
struct HoldMetrics {
    /// Holders that were dialed.
    dialed: AtomicU64,
    /// Dials that reached an established connection.
    connected: AtomicU64,
    /// Dials that never connected.
    dial_failed: AtomicU64,
    /// Holders currently holding the block-announces substream.
    held: AtomicU64,
    peak_held: AtomicU64,
    /// Cumulative block-announces opens (a holder re-opening counts again).
    accepted: AtomicU64,
    /// Block-announces substreams the node refused during the handshake.
    refused: AtomicU64,
    /// Block-announces substreams closed after having been held.
    evicted: AtomicU64,
    /// Block announcements received and dropped.
    announces: AtomicU64,
}

impl HoldMetrics {
    fn bump_held(&self) {
        let held = self.held.fetch_add(1, Ordering::Relaxed) + 1;
        let mut peak = self.peak_held.load(Ordering::Relaxed);
        while held > peak {
            match self.peak_held.compare_exchange_weak(
                peak,
                held,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => peak = observed,
            }
        }
    }
}

/// Build a swarm for one holder: a fresh identity and nothing but the
/// notification protocols.
async fn build_swarm(
    data: ProtocolsData,
    idle_timeout: Duration,
) -> Result<Swarm<Notifications>, Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();
    let tcp_config = libp2p::tcp::Config::new().nodelay(true);

    let swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp_config,
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .expect("Can construct TCP; qed")
        .with_dns()
        .expect("Can construct DNS; qed")
        .with_websocket(libp2p::noise::Config::new, libp2p::yamux::Config::default)
        .await
        .expect("Can construct WebSocket; qed")
        .with_behaviour(|_key| Notifications::new(data))
        .expect("Can construct behaviour; qed")
        // A refused holder keeps its connection but has no open substream. Park it
        // for the run instead of letting libp2p's short default close and churn it.
        .with_swarm_config(|config| config.with_idle_connection_timeout(idle_timeout))
        .build();

    Ok(swarm)
}

/// Run one holder: dial, then stay polled until the deadline, holding whatever
/// the node granted us. Returns how long the node took to accept us, if it did.
async fn run_peer(
    id: usize,
    addr: Multiaddr,
    data: ProtocolsData,
    idle_timeout: Duration,
    deadline: TokioInstant,
    metrics: Arc<HoldMetrics>,
) -> Option<u64> {
    let mut swarm = match build_swarm(data, idle_timeout).await {
        Ok(swarm) => swarm,
        Err(e) => {
            log::error!("peer {id}: build_swarm failed: {e}");
            metrics.dial_failed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };

    let dialed_at = Instant::now();
    if let Err(e) = swarm.dial(addr) {
        log::error!("peer {id}: dial failed: {e}");
        metrics.dial_failed.fetch_add(1, Ordering::Relaxed);
        return None;
    }

    // Set while we hold block-announces, so the shared gauge is released exactly
    // once however this holder ends.
    let mut holding = false;
    let mut time_to_hold = None;

    let sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep);

    loop {
        futures::select! {
            event = swarm.select_next_some().fuse() => match event {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    log::debug!("peer {id}: connected to {peer_id}");
                    metrics.connected.fetch_add(1, Ordering::Relaxed);
                }
                SwarmEvent::OutgoingConnectionError { error, .. } => {
                    log::warn!("peer {id}: connection error: {error}");
                    metrics.dial_failed.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                SwarmEvent::ConnectionClosed { cause, .. } => {
                    log::debug!("peer {id}: connection closed: {cause:?}");
                    if holding {
                        holding = false;
                        metrics.held.fetch_sub(1, Ordering::Relaxed);
                    }
                    break;
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::CustomProtocolOpen { index, .. })
                    if index == BLOCK_ANNOUNCES_INDEX =>
                {
                    metrics.accepted.fetch_add(1, Ordering::Relaxed);
                    if !holding {
                        holding = true;
                        metrics.bump_held();
                        time_to_hold.get_or_insert(dialed_at.elapsed().as_micros() as u64);
                    }
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::CustomProtocolRefused { index, .. })
                    if index == BLOCK_ANNOUNCES_INDEX =>
                {
                    log::debug!("peer {id}: block-announces refused");
                    metrics.refused.fetch_add(1, Ordering::Relaxed);
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::CustomProtocolClosed { index, .. })
                    if index == BLOCK_ANNOUNCES_INDEX =>
                {
                    if holding {
                        holding = false;
                        metrics.held.fetch_sub(1, Ordering::Relaxed);
                        metrics.evicted.fetch_add(1, Ordering::Relaxed);
                    }
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::Notification { index, .. })
                    if index == BLOCK_ANNOUNCES_INDEX =>
                {
                    metrics.announces.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            },
            _ = (&mut sleep).fuse() => break,
        }
    }

    if holding {
        metrics.held.fetch_sub(1, Ordering::Relaxed);
    }

    time_to_hold
}

fn print_progress(metrics: &HoldMetrics, start: Instant, opened: usize, peers: usize) {
    print!(
        "\r  t={:.0}s offered={opened}/{peers} connected={} held={}(peak {}) refused={} evicted={} announces={}   ",
        start.elapsed().as_secs_f64(),
        metrics.connected.load(Ordering::Relaxed),
        metrics.held.load(Ordering::Relaxed),
        metrics.peak_held.load(Ordering::Relaxed),
        metrics.refused.load(Ordering::Relaxed),
        metrics.evicted.load(Ordering::Relaxed),
        metrics.announces.load(Ordering::Relaxed),
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

#[allow(clippy::too_many_arguments)]
fn print_report(
    metrics: &HoldMetrics,
    hold_times: &mut [u64],
    address: &str,
    genesis: &str,
    role: HoldRole,
    peers: usize,
    ramp_ms: u64,
    steady_held: u64,
    elapsed: f64,
) {
    hold_times.sort_unstable();

    println!("\n=== hold-peers summary ===");
    println!("target:     {address}");
    println!("protocol:   /{genesis}/block-announces/1");
    println!(
        "role:       {:?} (handshake byte {})",
        role,
        role.protocol_role().encoded()
    );
    println!("offered:    {peers} peers (one every {ramp_ms} ms)");
    println!("duration:   {elapsed:.1}s");
    println!(
        "held:       peak {} | steady {steady_held} at end of run",
        metrics.peak_held.load(Ordering::Relaxed),
    );
    println!(
        "outcome:    connected={} accepted={} refused={} evicted={} dial_failed={}",
        metrics.connected.load(Ordering::Relaxed),
        metrics.accepted.load(Ordering::Relaxed),
        metrics.refused.load(Ordering::Relaxed),
        metrics.evicted.load(Ordering::Relaxed),
        metrics.dial_failed.load(Ordering::Relaxed),
    );
    println!(
        "dial->held ms: p50={:.1} p90={:.1} p99={:.1} (over {} accepted peers)",
        percentile_ms(hold_times, 50),
        percentile_ms(hold_times, 90),
        percentile_ms(hold_times, 99),
        hold_times.len(),
    );
    println!(
        "announces:  {} received and dropped",
        metrics.announces.load(Ordering::Relaxed)
    );

    let peak = metrics.peak_held.load(Ordering::Relaxed);
    if metrics.refused.load(Ordering::Relaxed) > 0 {
        println!(
            "\nThe node refused peers, so it is at its limit for this role. Its ceiling is {peak}."
        );
    } else if peak >= peers as u64 {
        println!("\nEvery offered peer was held, so the node's limit is above {peers}.");
    }
}

/// Entry point for the `hold-peers` command.
#[allow(clippy::too_many_arguments)]
pub async fn hold_peers(
    chain: Option<Chain>,
    url: Option<String>,
    address: Option<String>,
    genesis: Option<String>,
    role: HoldRole,
    peers: usize,
    ramp_ms: u64,
    duration: Duration,
    idle_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let peers = peers.max(1);

    let address = address
        .or_else(|| chain.map(|c| c.address().to_string()))
        .ok_or("provide --address or --chain")?;

    // Unlike spam-light / soak-light this needs no live chain state: the node
    // validates only the genesis hash in the handshake, so `--genesis` lets the
    // whole run happen without an RPC endpoint.
    let genesis = match genesis {
        Some(genesis) => genesis.trim_start_matches("0x").to_string(),
        None => {
            let url = url
                .or_else(|| chain.map(|c| c.rpc_url().to_string()))
                .ok_or("provide --genesis, or --url/--chain to fetch it")?;
            println!("Fetching genesis from RPC...");
            fetch_genesis_hash(Url::parse(&url)?).await?
        }
    };

    let data = ProtocolsData {
        genesis_hash: H256::from_slice(hex::decode(&genesis)?.as_slice()),
        node_role: role.protocol_role(),
    };

    println!("Address:    {address}");
    println!("Protocol:   /{genesis}/block-announces/1");
    println!(
        "Role:       {:?} (handshake byte {})",
        role,
        role.protocol_role().encoded()
    );
    println!(
        "Hold:       peers={peers} duration={:.0}s (one new peer every {ramp_ms} ms, idle timeout {:.0}s)",
        duration.as_secs_f64(),
        idle_timeout.as_secs_f64(),
    );

    let multiaddr: Multiaddr = address.parse()?;
    let metrics = Arc::new(HoldMetrics::default());
    let start = Instant::now();
    let deadline = TokioInstant::now() + duration;

    println!("Opening peers...\n");
    let mut handles = Vec::with_capacity(peers);
    let mut opened = 0usize;

    let final_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(final_sleep);
    let mut spawn_tick = tokio::time::interval(Duration::from_millis(ramp_ms.max(1)));
    let mut progress_tick = tokio::time::interval(Duration::from_secs(1));
    spawn_tick.tick().await;
    progress_tick.tick().await;

    loop {
        futures::select! {
            _ = (&mut final_sleep).fuse() => break,
            _ = spawn_tick.tick().fuse() => {
                // `--ramp-ms 0` means open everything at once; otherwise one per tick.
                let batch = if ramp_ms == 0 { peers - opened } else { 1 };
                for _ in 0..batch {
                    if opened >= peers {
                        break;
                    }
                    opened += 1;
                    metrics.dialed.fetch_add(1, Ordering::Relaxed);
                    handles.push(tokio::spawn(run_peer(
                        opened,
                        multiaddr.clone(),
                        data.clone(),
                        idle_timeout,
                        deadline,
                        metrics.clone(),
                    )));
                }
            }
            _ = progress_tick.tick().fuse() => print_progress(&metrics, start, opened, peers),
        }
    }

    // Read the gauge before the holders wind down, otherwise it is always zero.
    let steady_held = metrics.held.load(Ordering::Relaxed);
    println!("\nDuration reached, draining {} peer(s)...", handles.len());

    let mut hold_times: Vec<u64> = join_all(handles)
        .await
        .into_iter()
        .flatten()
        .flatten()
        .collect();

    print_report(
        &metrics,
        &mut hold_times,
        &address,
        &genesis,
        role,
        peers,
        ramp_ms,
        steady_held,
        start.elapsed().as_secs_f64().max(0.001),
    );

    Ok(())
}
