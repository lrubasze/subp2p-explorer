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
use codec::{Compact, Decode};
use futures::{future::join_all, FutureExt, StreamExt};
use jsonrpsee::client_transport::ws::Url;
use libp2p::{identity, swarm::SwarmEvent, Multiaddr, Swarm};
use primitive_types::H256;
use std::collections::HashMap;
use std::error::Error;
use std::hash::{Hash, Hasher};
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
use tokio::sync::{mpsc, watch};
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
    pub(crate) fn protocol_role(&self) -> ProtocolRole {
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
pub(crate) struct HoldMetrics {
    /// Holders that were dialed.
    pub dialed: AtomicU64,
    /// Dials that reached an established connection.
    pub connected: AtomicU64,
    /// Dials that never connected.
    pub dial_failed: AtomicU64,
    /// Holders currently holding the block-announces substream.
    pub held: AtomicU64,
    peak_held: AtomicU64,
    /// Cumulative block-announces opens (a holder re-opening counts again).
    accepted: AtomicU64,
    /// Block-announces substreams the node refused during the handshake.
    refused: AtomicU64,
    /// Block-announces substreams closed after having been held.
    evicted: AtomicU64,
    /// Block announcements received and dropped.
    pub announces: AtomicU64,
}

/// One block announcement as seen by one holder.
pub(crate) struct Arrival {
    /// Hash of the announcement bytes. The node builds one message per block and
    /// sends the same bytes to every peer, so this groups arrivals of the same
    /// block without having to decode the header.
    pub key: u64,
    /// Block number, when the header decoded.
    pub number: Option<u32>,
    /// Timestamped in the holder task, as close to the event as we can get.
    pub at: Instant,
    /// Peers held when this arrival happened — the coverage denominator.
    pub held: u64,
}

/// Per-block view of one announcement, accumulated across every holder.
struct BlockObs {
    number: Option<u32>,
    first: Instant,
    last: Instant,
    /// Holders that received this block.
    count: u64,
    /// Peers held when this block first reached us.
    held_at_first: u64,
}

/// A `BlockAnnounce` begins with the block header, which begins with a 32-byte
/// parent hash followed by the block number as a compact integer. That is all we
/// want, so skip the parent hash and decode the number rather than modelling the
/// whole header.
fn announced_block_number(bytes: &[u8]) -> Option<u32> {
    let mut rest = bytes.get(32..)?;
    Compact::<u32>::decode(&mut rest)
        .ok()
        .map(|number| number.0)
}

/// Group key for one announcement: a hash of its bytes.
fn announcement_key(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Fold every holder's arrivals into one observation per block. Runs as its own
/// task so holders never block on aggregation, and ends when the last holder
/// drops its sender.
async fn collect_arrivals(
    mut arrivals: mpsc::UnboundedReceiver<Arrival>,
) -> HashMap<u64, BlockObs> {
    let mut blocks: HashMap<u64, BlockObs> = HashMap::new();

    while let Some(arrival) = arrivals.recv().await {
        blocks
            .entry(arrival.key)
            // Arrivals from different holders interleave, so first and last both
            // need a comparison rather than assuming arrival order.
            .and_modify(|obs| {
                obs.count += 1;
                if arrival.at < obs.first {
                    obs.first = arrival.at;
                    obs.held_at_first = arrival.held;
                }
                if arrival.at > obs.last {
                    obs.last = arrival.at;
                }
            })
            .or_insert(BlockObs {
                number: arrival.number,
                first: arrival.at,
                last: arrival.at,
                count: 1,
                held_at_first: arrival.held,
            });
    }

    blocks
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
pub(crate) async fn build_swarm(
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
        // Only governs our side. A refused holder has no open substream, and the
        // node reaps such a connection after its own idle timeout (measured: 10.0s
        // against a dev node), so this cannot keep a refused holder connected.
        .with_swarm_config(|config| config.with_idle_connection_timeout(idle_timeout))
        .build();

    Ok(swarm)
}

/// Run one holder: dial, then stay polled until `stop` is set, holding whatever
/// the node granted us. Returns how long the node took to accept us, if it did.
pub(crate) async fn run_peer(
    id: usize,
    addr: Multiaddr,
    data: ProtocolsData,
    idle_timeout: Duration,
    mut stop: watch::Receiver<bool>,
    arrivals: mpsc::UnboundedSender<Arrival>,
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

    loop {
        if *stop.borrow() {
            break;
        }

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
                SwarmEvent::Behaviour(NotificationsToSwarm::Notification { index, message, .. })
                    if index == BLOCK_ANNOUNCES_INDEX =>
                {
                    // Stamp the time first, before any work on the message.
                    let at = Instant::now();
                    metrics.announces.fetch_add(1, Ordering::Relaxed);
                    let _ = arrivals.send(Arrival {
                        key: announcement_key(&message),
                        number: announced_block_number(&message),
                        at,
                        held: metrics.held.load(Ordering::Relaxed),
                    });
                }
                _ => {}
            },
            // The orchestrator reads the held count before setting this, so a
            // holder can never release its slot before that count is taken.
            changed = stop.changed().fuse() => if changed.is_err() {
                break;
            },
        }
    }

    if holding {
        metrics.held.fetch_sub(1, Ordering::Relaxed);
    }

    time_to_hold
}

fn print_progress(metrics: &HoldMetrics, phase: &str, elapsed: f64, opened: usize, peers: usize) {
    print!(
        "\r  [{phase}] t={elapsed:.0}s offered={opened}/{peers} connected={} held={}(peak {}) refused={} evicted={} announces={}   ",
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
    held_at_start: u64,
    steady_held: u64,
    connect_secs: f64,
    hold_secs: f64,
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
    println!("connect:    {connect_secs:.1}s to dial every peer and let the dials settle");
    println!("hold:       {hold_secs:.1}s window, timed from the end of connect");
    println!(
        "held:       peak {} | {held_at_start} at window start | {steady_held} at window end",
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
    let peak = metrics.peak_held.load(Ordering::Relaxed);
    if metrics.refused.load(Ordering::Relaxed) > 0 {
        println!(
            "\nThe node refused peers, so it is at its limit for this role. Its ceiling is {peak}."
        );
    } else if peak >= peers as u64 {
        println!("\nEvery offered peer was held, so the node's limit is above {peers}.");
    }
}

/// Report announcement fan-out: how long the node took to reach every holder
/// with the same block, and whether every holder got it at all.
///
/// The node announces a block by looping over its peers one at a time, and the
/// send is fire-and-forget — a peer whose buffer is full is skipped silently. So
/// spread grows with peer count, and coverage below 100% means dropped
/// announcements.
///
/// Only blocks first seen during the hold window are reported. Grouping is by
/// message bytes, and the node re-announces a block with identical bytes, so a
/// block first seen during the ramp merges two rounds that are seconds apart and
/// whose peer counts differ — that produced coverage above 100% and a spread of
/// seconds. During the hold window the peer set is stable, and a re-announce
/// reaches nobody new because the node tracks what each peer has already seen.
///
/// Caveat: at high peer counts these timings include our own scheduling delay,
/// since every holder shares one process. Treat them as an upper bound on what
/// the node is responsible for.
fn print_announcement_quality(blocks: &HashMap<u64, BlockObs>, hold_start: Instant) {
    let total = blocks.len();
    let blocks: Vec<&BlockObs> = blocks
        .values()
        .filter(|obs| obs.first >= hold_start)
        .collect();

    if blocks.is_empty() {
        println!(
            "announce:   nothing received during the hold window ({} before it, not comparable)",
            total
        );
        return;
    }

    let received: u64 = blocks.iter().map(|obs| obs.count).sum();
    let mut spreads: Vec<u64> = blocks
        .iter()
        .map(|obs| obs.last.duration_since(obs.first).as_micros() as u64)
        .collect();
    spreads.sort_unstable();

    println!(
        "announce:   {received} received over {} announcement(s) in the hold window ({} earlier one(s) skipped)",
        blocks.len(),
        total - blocks.len(),
    );
    println!(
        "spread ms:  p50={:.1} p90={:.1} p99={:.1} max={:.1} (first to last holder, same block)",
        percentile_ms(&spreads, 50),
        percentile_ms(&spreads, 90),
        percentile_ms(&spreads, 99),
        percentile_ms(&spreads, 100),
    );

    let mut rows: Vec<(f64, &&BlockObs)> = blocks
        .iter()
        .filter(|obs| obs.held_at_first > 0)
        .map(|obs| (obs.count as f64 / obs.held_at_first as f64, obs))
        .collect();
    if !rows.is_empty() {
        rows.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("coverage is finite; qed"));
        let mean = rows.iter().map(|(cov, _)| cov).sum::<f64>() / rows.len() as f64;
        let incomplete = rows
            .iter()
            .filter(|(_, obs)| obs.count < obs.held_at_first)
            .count();
        let (worst_cov, worst) = rows[0];
        println!(
            "coverage:   mean {:.1}% | worst {:.1}% ({} of {} holders{}) | {incomplete} of {} announcement(s) incomplete",
            mean * 100.0,
            worst_cov * 100.0,
            worst.count,
            worst.held_at_first,
            worst
                .number
                .map(|number| format!(" on #{number}"))
                .unwrap_or_default(),
            rows.len(),
        );
    }

    let mut numbers: Vec<u32> = blocks.iter().filter_map(|obs| obs.number).collect();
    numbers.sort_unstable();
    numbers.dedup();
    if let (Some(first), Some(last)) = (numbers.first(), numbers.last()) {
        let span = (last - first + 1) as usize;
        let missing = span.saturating_sub(numbers.len());
        println!(
            "blocks:     #{first}..#{last} | {} seen | {missing} number(s) never announced to us",
            numbers.len(),
        );
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
    connect_timeout: Duration,
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
        "Hold:       peers={peers} hold={:.0}s after all peers connect (one new peer every {ramp_ms} ms, idle timeout {:.0}s)",
        duration.as_secs_f64(),
        idle_timeout.as_secs_f64(),
    );

    let multiaddr: Multiaddr = address.parse()?;
    let metrics = Arc::new(HoldMetrics::default());
    // Holders run until this is set rather than until a fixed instant, so the
    // hold window can start once the last peer has connected.
    let (stop_tx, stop_rx) = watch::channel(false);
    // Announcement arrivals are aggregated off the holder path.
    let (arrivals_tx, arrivals_rx) = mpsc::unbounded_channel();
    let collector = tokio::spawn(collect_arrivals(arrivals_rx));

    // Phase 1: open every peer on the ramp, then let the dials settle. Opening
    // 10000 peers 10 ms apart takes 100 s on its own, so this has to finish
    // before the hold clock starts or a long ramp eats the whole window.
    //
    // The target peer count may never be held — the node refuses everything past
    // its limit and reaps those connections — so the exit condition is about
    // dials, not about holders: every peer dialed, and every dial resolved one
    // way or the other, with `connect_timeout` as the backstop for a dial that
    // neither connects nor fails.
    println!("Opening peers...\n");
    let connect_start = Instant::now();
    let mut handles = Vec::with_capacity(peers);
    let mut opened = 0usize;

    let mut spawn_tick = tokio::time::interval(Duration::from_millis(ramp_ms.max(1)));
    let mut progress_tick = tokio::time::interval(Duration::from_secs(1));
    let mut settle_tick = tokio::time::interval(Duration::from_millis(100));
    spawn_tick.tick().await;
    progress_tick.tick().await;
    settle_tick.tick().await;

    // A dial has resolved once it either connected or failed.
    let resolved = |m: &HoldMetrics| {
        m.connected.load(Ordering::Relaxed) + m.dial_failed.load(Ordering::Relaxed)
    };
    // Armed only once every peer has been opened, so the ramp is not on its clock.
    let mut resolve_deadline: Option<TokioInstant> = None;

    loop {
        if opened >= peers {
            if resolved(&metrics) >= peers as u64 {
                break;
            }
            let deadline =
                *resolve_deadline.get_or_insert_with(|| TokioInstant::now() + connect_timeout);
            if TokioInstant::now() >= deadline {
                println!(
                    "\n  warning: only {}/{peers} dials resolved within {:.0}s of the last one \
                     being opened; starting the hold window anyway",
                    resolved(&metrics),
                    connect_timeout.as_secs_f64(),
                );
                break;
            }
        }

        futures::select! {
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
                        stop_rx.clone(),
                        arrivals_tx.clone(),
                        metrics.clone(),
                    )));
                }
            }
            // Re-checks the loop condition while nothing else is due.
            _ = settle_tick.tick().fuse() => {}
            _ = progress_tick.tick().fuse() => print_progress(
                &metrics,
                "connect",
                connect_start.elapsed().as_secs_f64(),
                opened,
                peers,
            ),
        }
    }

    let connect_secs = connect_start.elapsed().as_secs_f64();
    // How many we actually had when the window opened, which for a target past
    // the node's limit is well below `peers`.
    let held_at_start = metrics.held.load(Ordering::Relaxed);
    println!(
        "\n\nAll {peers} peers dialed in {connect_secs:.1}s ({} connected, {} failed, {held_at_start} held). Holding for {:.0}s...\n",
        metrics.connected.load(Ordering::Relaxed),
        metrics.dial_failed.load(Ordering::Relaxed),
        duration.as_secs_f64(),
    );

    // Phase 2: the hold window proper.
    let hold_start = Instant::now();
    let hold_sleep = tokio::time::sleep_until(TokioInstant::now() + duration);
    tokio::pin!(hold_sleep);

    loop {
        futures::select! {
            _ = (&mut hold_sleep).fuse() => break,
            _ = progress_tick.tick().fuse() => print_progress(
                &metrics,
                "hold",
                hold_start.elapsed().as_secs_f64(),
                opened,
                peers,
            ),
        }
    }

    let hold_secs = hold_start.elapsed().as_secs_f64();
    // Read the count before telling the holders to stop, so none of them can
    // release its slot first.
    let steady_held = metrics.held.load(Ordering::Relaxed);
    let _ = stop_tx.send(true);
    println!("\nHold window over, draining {} peer(s)...", handles.len());

    let mut hold_times: Vec<u64> = join_all(handles)
        .await
        .into_iter()
        .flatten()
        .flatten()
        .collect();

    // Every holder has dropped its sender by now; drop ours so the collector
    // sees the channel close and returns what it aggregated.
    drop(arrivals_tx);
    let blocks = collector.await.unwrap_or_default();

    print_report(
        &metrics,
        &mut hold_times,
        &address,
        &genesis,
        role,
        peers,
        ramp_ms,
        held_at_start,
        steady_held,
        connect_secs,
        hold_secs,
    );
    print_announcement_quality(&blocks, hold_start);

    Ok(())
}
