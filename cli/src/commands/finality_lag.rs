// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Measure the finality lag a light peer sees from one full node, as a function
//! of how many light peers that node has.
//!
//! A light client (smoldot) learns that a block is final only from GRANDPA
//! *commit* messages on the `/grandpa/1` notification substream. The node's
//! gossip validator (`sc-consensus-grandpa`, `communication/gossip.rs`) does not
//! send commits to every light peer. `global_message_allowed` lets a commit go to
//! a light peer only while that peer is in `lucky_light_peers`, a set of
//! `LUCKY_PEERS = 4` light peers drawn at random by `Peers::reshuffle`. Reshuffle
//! runs on every `note_round`, and a full node runs the GRANDPA voter as a
//! non-voting participant (observer mode is compiled out, `lib.rs`
//! `observer_enabled = false`), so it notes a new round each time the validators
//! complete one — every ~4-6 s on Kusama, somewhat more often than a commit.
//!
//! The commit itself is offered to the lucky set more than once. `sc-network-gossip`
//! keeps only the node's best commit alive (`message_expired`) and re-runs
//! `propagate` for every live message every 750 ms (`tick` → `rebroadcast`).
//! For a peer that does not yet know the message that pass counts as a fresh
//! `Broadcast`, so it is gated by the lucky set *as it stands at that moment*.
//! Net effect: each round, four more light peers picked at random get the
//! current best commit; a peer not picked keeps its stale finalized head. There
//! is no fallback that helps such a peer: the "send to everyone" stage needs the
//! current round to be older than `PROPAGATION_ALL * ROUND_DURATION *
//! gossip_duration` = 15 s, which a healthy chain never reaches because every
//! round restarts that clock, and the 5-minute `REBROADCAST_AFTER` pass only
//! re-sends to peers that *already know* the message (`propagate` downgrades the
//! intent to `Broadcast` for the others, and the lucky gate applies again).
//!
//! So with `N` light peers each one is picked with probability 4/N per round,
//! and the gap between two commits reaching the same peer is geometric with mean
//!
//! ```text
//! N / 4 * T_round        (no cap; MEASURED p99 ≈ 4-5x the mean)
//! ```
//!
//! which is also, to first order, the finality lag that peer observes. At N = 120
//! and T_round ≈ 4.5 s that is ~2.3 min; the issue reports about 3 min
//! (<https://github.com/paritytech/smoldot/issues/3375>).
//!
//! This command reproduces the setup with cheap synthetic peers: each holder opens
//! block-announces (to get a slot) and `/grandpa/1` with the light role, answers
//! the node's neighbor packet with its own so the node considers it in the current
//! set (a commit is never sent to a peer whose view is in another set), and
//! advances its "finalized" height only when a commit reaches it — exactly what
//! smoldot does. After each commit it re-sends its neighbor packet with the new
//! height, as smoldot does after finalizing.
//!
//! The reference clock for "when did the node finalize block H" needs no RPC: on
//! every commit it imports the node multicasts a neighbor packet carrying its
//! `commit_finalized_height` to *all* its peers (`note_commit_finalized` →
//! `multicast_neighbor_packet`). The first time any holder sees height H in a
//! neighbor packet or a commit is when the node finalized it. A holder's lag at
//! time t is then `t - node_finalized_at(holder.finalized)`.
//!
//! Caveat: the node's *other* light peers (real ones) share the four lucky slots
//! with ours. Run a small N first — with N = 4 and no outside light peers every
//! holder is always lucky and should see every commit.

use crate::commands::authorities::fetch_genesis_hash;
use crate::commands::grandpa_follow::{
    GrandpaFollower, GrandpaMessage, NeighborPacket, LUCKY_PEERS, REBROADCAST_AFTER,
};
use crate::commands::hold_peers::{build_swarm, HoldRole, WallAnchor};
use crate::commands::light_common::Chain;
use futures::{future::join_all, FutureExt, StreamExt};
use jsonrpsee::client_transport::ws::Url;
use libp2p::{swarm::SwarmEvent, Multiaddr};
use primitive_types::H256;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subp2p_explorer::{
    notifications::behavior::{NotificationsToSwarm, ProtocolsData},
    BLOCK_ANNOUNCES_INDEX, GRANDPA_INDEX,
};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant as TokioInstant;

/// What a holder saw on its grandpa substream, timestamped in the holder task.
enum GossipEvent {
    GrandpaOpen {
        peer: usize,
    },
    GrandpaClosed {
        peer: usize,
    },
    /// The node told us its view. Carries its finalized height, so every peer
    /// learns the node's finality timeline whether or not it is lucky.
    Neighbor {
        peer: usize,
        at: Instant,
        packet: NeighborPacket,
    },
    /// A commit reached us: our finalized head moves to `target`.
    Commit {
        peer: usize,
        at: Instant,
        round: u64,
        set_id: u64,
        target: u32,
        /// Wire size of the commit message — what the fix costs per light peer.
        bytes: usize,
    },
    /// The orchestrator marks the start of the hold window.
    HoldStart {
        at: Instant,
    },
    /// The orchestrator marks the end of the hold window, before teardown.
    HoldEnd,
}

/// Lock-free counters for the progress line.
#[derive(Default)]
struct Metrics {
    connected: AtomicU64,
    dial_failed: AtomicU64,
    held: AtomicU64,
    refused: AtomicU64,
    grandpa_open: AtomicU64,
    grandpa_refused: AtomicU64,
    commits: AtomicU64,
    neighbors: AtomicU64,
    votes: AtomicU64,
    other: AtomicU64,
}

/// One holder's finality view, as the collector tracks it.
#[derive(Default)]
struct PeerState {
    grandpa_open: bool,
    /// Height the holder currently considers final, and when it learned it.
    finalized: Option<(u32, Instant)>,
    commits: u64,
    /// Commits that reached this holder inside the hold window.
    commits_in_hold: u64,
    /// The (set, round) of the last commit delivered, to spot repeats.
    last_round: Option<(u64, u64)>,
    /// Further copies of a commit for a round this holder already had.
    duplicates: u64,
    /// Gaps between consecutive commits reaching this holder, hold window only.
    gaps: Vec<Duration>,
}

/// Snapshot published once a second for the progress line.
#[derive(Clone, Default)]
struct Live {
    node_head: u32,
    peers_open: usize,
    lag_s_p50: f64,
    lag_s_p90: f64,
    lag_s_max: f64,
    lag_blocks_p50: f64,
    lag_blocks_max: f64,
    /// Share of open peers within two blocks of the node.
    fresh_pct: f64,
}

/// What the collector hands back when the run ends.
#[derive(Default)]
pub(crate) struct Summary {
    /// Distinct finalized heights the node advertised in the hold window, and
    /// the mean gap between them. `commits_delivered` is over the same window.
    node_commits: usize,
    commit_interval_s: f64,
    /// Distinct GRANDPA rounds the node went through in the window (one lucky
    /// draw each), and the mean gap between them.
    node_rounds: usize,
    round_interval_s: f64,
    peers_open_at_end: usize,
    peers_with_commit: usize,
    commits_delivered: u64,
    /// Deliveries that repeated a (set, round) the peer already had — several
    /// validators' copies of one commit, all forwarded by the node.
    duplicates: u64,
    gap_s: Percentiles,
    lag_s: Percentiles,
    lag_blocks: Percentiles,
    /// Peer-samples more than 5.5 min behind the node, out of all peer-samples
    /// (one per open peer per second). Non-zero shows that the 5-minute periodic
    /// rebroadcast does not rescue a starved peer.
    samples_over_cap: u64,
    samples: u64,
    /// Wire size of a commit message, over every delivery.
    commit_bytes: Percentiles,
}

#[derive(Default, Clone, Copy)]
struct Percentiles {
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
    min: f64,
    mean: f64,
}

fn percentiles(values: &mut [f64]) -> Percentiles {
    if values.is_empty() {
        return Percentiles::default();
    }
    values.sort_by(|a, b| a.partial_cmp(b).expect("finite; qed"));
    let at = |p: usize| values[(p * (values.len() - 1)) / 100];
    Percentiles {
        p50: at(50),
        p90: at(90),
        p99: at(99),
        max: values[values.len() - 1],
        min: values[0],
        mean: values.iter().sum::<f64>() / values.len() as f64,
    }
}

/// Aggregates every holder's gossip events, samples lag once a second, and
/// writes the CSVs. Runs as its own task so holders never block on it.
struct Collector {
    peers: Vec<PeerState>,
    /// First time any holder saw the node claim height H final. This is the
    /// node's finality timeline.
    node_finalized: BTreeMap<u32, Instant>,
    node_head: u32,
    /// First time any holder saw the node in a given (set, round). The node
    /// reshuffles its lucky set on every round, so this is the draw timeline.
    node_rounds: BTreeMap<(u64, u64), Instant>,
    hold_start: Option<Instant>,
    /// Peers with grandpa open when the hold window ended.
    open_at_end: Option<usize>,
    anchor: WallAnchor,
    commits_csv: Option<fs::File>,
    samples_csv: Option<fs::File>,
    /// Wire size of every commit delivered.
    commit_bytes: Vec<f64>,
    /// Per-sample lag values over the hold window, pooled across peers.
    lag_s: Vec<f64>,
    lag_blocks: Vec<f64>,
    samples_over_cap: u64,
    samples: u64,
    live: watch::Sender<Live>,
}

impl Collector {
    fn node_finalized_at(&self, height: u32) -> Option<Instant> {
        self.node_finalized
            .range(..=height)
            .next_back()
            .map(|(_, at)| *at)
    }

    fn note_node_height(&mut self, height: u32, at: Instant) {
        self.node_finalized
            .entry(height)
            .and_modify(|first| {
                if at < *first {
                    *first = at;
                }
            })
            .or_insert(at);
        self.node_head = self.node_head.max(height);
    }

    fn on_event(&mut self, event: GossipEvent) {
        match event {
            GossipEvent::HoldStart { at } => self.hold_start = Some(at),
            GossipEvent::HoldEnd => {
                self.open_at_end = Some(self.peers.iter().filter(|p| p.grandpa_open).count())
            }
            GossipEvent::GrandpaOpen { peer } => self.peers[peer].grandpa_open = true,
            GossipEvent::GrandpaClosed { peer } => self.peers[peer].grandpa_open = false,
            GossipEvent::Neighbor { peer, at, packet } => {
                self.note_node_height(packet.commit_finalized_height, at);
                self.node_rounds
                    .entry((packet.set_id, packet.round))
                    .and_modify(|first| {
                        if at < *first {
                            *first = at;
                        }
                    })
                    .or_insert(at);
                // A holder starts out "synced" to the height it first sees, the
                // way smoldot is right after warp sync.
                let state = &mut self.peers[peer];
                if state.finalized.is_none() {
                    state.finalized = Some((packet.commit_finalized_height, at));
                }
            }
            GossipEvent::Commit {
                peer,
                at,
                round,
                set_id,
                target,
                bytes,
            } => {
                self.note_node_height(target, at);
                self.commit_bytes.push(bytes as f64);
                let node_at = self.node_finalized_at(target).unwrap_or(at);
                let in_hold = self.hold_start.is_some_and(|start| at >= start);
                let state = &mut self.peers[peer];
                state.commits += 1;
                if in_hold {
                    state.commits_in_hold += 1;
                    if state.last_round == Some((set_id, round)) {
                        state.duplicates += 1;
                    }
                }
                state.last_round = Some((set_id, round));
                let prev = state.finalized;
                if let Some((prev_height, prev_at)) = prev {
                    if in_hold && target > prev_height {
                        state.gaps.push(at.duration_since(prev_at));
                    }
                }
                if prev.is_none_or(|(h, _)| target > h) {
                    state.finalized = Some((target, at));
                }
                if let Some(file) = self.commits_csv.as_mut() {
                    let _ = writeln!(
                        file,
                        "{},{peer},{set_id},{round},{target},{},{:.0},{},{bytes}",
                        self.anchor.epoch_ms(at),
                        self.node_head,
                        at.duration_since(node_at).as_secs_f64() * 1000.0,
                        in_hold,
                    );
                }
            }
        }
    }

    /// Sample every open holder's lag against the node's timeline.
    fn sample(&mut self, now: Instant) {
        let in_hold = self.hold_start.is_some_and(|start| now >= start);
        let mut lag_s = Vec::new();
        let mut lag_blocks = Vec::new();
        for state in self.peers.iter().filter(|p| p.grandpa_open) {
            let Some((height, _)) = state.finalized else {
                continue;
            };
            let Some(node_at) = self.node_finalized_at(height) else {
                continue;
            };
            lag_s.push(now.saturating_duration_since(node_at).as_secs_f64());
            lag_blocks.push(self.node_head.saturating_sub(height) as f64);
        }
        let fresh = lag_blocks.iter().filter(|b| **b <= 2.0).count();
        let peers_open = lag_s.len();
        let s = percentiles(&mut lag_s);
        let b = percentiles(&mut lag_blocks);
        let live = Live {
            node_head: self.node_head,
            peers_open,
            lag_s_p50: s.p50,
            lag_s_p90: s.p90,
            lag_s_max: s.max,
            lag_blocks_p50: b.p50,
            lag_blocks_max: b.max,
            fresh_pct: if peers_open > 0 {
                fresh as f64 * 100.0 / peers_open as f64
            } else {
                0.0
            },
        };
        let _ = self.live.send(live.clone());

        if in_hold {
            self.samples += lag_s.len() as u64;
            self.samples_over_cap += lag_s
                .iter()
                .filter(|l| **l > REBROADCAST_AFTER.as_secs_f64() + 30.0)
                .count() as u64;
            self.lag_s.extend_from_slice(&lag_s);
            self.lag_blocks.extend_from_slice(&lag_blocks);
        }
        if let Some(file) = self.samples_csv.as_mut() {
            let elapsed = self
                .hold_start
                .map(|start| now.saturating_duration_since(start).as_secs_f64())
                .unwrap_or(0.0);
            let _ = writeln!(
                file,
                "{},{},{elapsed:.1},{},{peers_open},{:.1},{:.1},{:.1},{:.0},{:.0},{:.0},{:.1}",
                self.anchor.epoch_ms(now),
                if in_hold { "hold" } else { "connect" },
                self.node_head,
                s.p50,
                s.p90,
                s.max,
                b.p50,
                b.p90,
                b.max,
                live.fresh_pct,
            );
        }
    }

    fn finish(mut self) -> Summary {
        let hold_start = self.hold_start.unwrap_or_else(Instant::now);
        let mut heights: Vec<Instant> = self
            .node_finalized
            .values()
            .copied()
            .filter(|at| *at >= hold_start)
            .collect();
        heights.sort();
        let commit_interval_s = if heights.len() >= 2 {
            heights
                .windows(2)
                .map(|w| w[1].duration_since(w[0]).as_secs_f64())
                .sum::<f64>()
                / (heights.len() - 1) as f64
        } else {
            0.0
        };
        let rounds: Vec<Instant> = self
            .node_rounds
            .values()
            .copied()
            .filter(|at| *at >= hold_start)
            .collect();
        let round_interval_s = if rounds.len() >= 2 {
            let mut sorted = rounds.clone();
            sorted.sort();
            sorted
                .windows(2)
                .map(|w| w[1].duration_since(w[0]).as_secs_f64())
                .sum::<f64>()
                / (sorted.len() - 1) as f64
        } else {
            0.0
        };
        let mut gaps: Vec<f64> = self
            .peers
            .iter()
            .flat_map(|p| p.gaps.iter().map(|g| g.as_secs_f64()))
            .collect();
        Summary {
            node_commits: heights.len(),
            commit_interval_s,
            node_rounds: rounds.len(),
            round_interval_s,
            peers_open_at_end: self
                .open_at_end
                .unwrap_or_else(|| self.peers.iter().filter(|p| p.grandpa_open).count()),
            peers_with_commit: self.peers.iter().filter(|p| p.commits > 0).count(),
            commits_delivered: self.peers.iter().map(|p| p.commits_in_hold).sum(),
            duplicates: self.peers.iter().map(|p| p.duplicates).sum(),
            gap_s: percentiles(&mut gaps),
            lag_s: percentiles(&mut self.lag_s),
            lag_blocks: percentiles(&mut self.lag_blocks),
            samples_over_cap: self.samples_over_cap,
            samples: self.samples,
            commit_bytes: percentiles(&mut self.commit_bytes),
        }
    }
}

async fn collect(
    mut collector: Collector,
    mut events: mpsc::UnboundedReceiver<GossipEvent>,
) -> Summary {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => collector.on_event(event),
                // Every holder has dropped its sender: the run is over.
                None => break,
            },
            _ = tick.tick() => collector.sample(Instant::now()),
        }
    }
    collector.finish()
}

/// Run one holder: dial, hold block-announces and grandpa, mirror the node's
/// set in our neighbor packet, and advance our finalized height on commits.
#[allow(clippy::too_many_arguments)]
async fn run_peer(
    id: usize,
    addr: Multiaddr,
    data: ProtocolsData,
    idle_timeout: Duration,
    mut stop: watch::Receiver<bool>,
    events: mpsc::UnboundedSender<GossipEvent>,
    metrics: Arc<Metrics>,
) {
    let mut swarm = match build_swarm(data, idle_timeout).await {
        Ok(swarm) => swarm,
        Err(e) => {
            log::error!("peer {id}: build_swarm failed: {e}");
            metrics.dial_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    if let Err(e) = swarm.dial(addr) {
        log::error!("peer {id}: dial failed: {e}");
        metrics.dial_failed.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let mut holding = false;
    let mut grandpa = GrandpaFollower::default();

    loop {
        if *stop.borrow() {
            break;
        }
        futures::select! {
            event = swarm.select_next_some().fuse() => match event {
                SwarmEvent::ConnectionEstablished { .. } => {
                    metrics.connected.fetch_add(1, Ordering::Relaxed);
                }
                SwarmEvent::OutgoingConnectionError { error, .. } => {
                    log::warn!("peer {id}: connection error: {error}");
                    metrics.dial_failed.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                SwarmEvent::ConnectionClosed { cause, .. } => {
                    log::debug!("peer {id}: connection closed: {cause:?}");
                    break;
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::CustomProtocolOpen { index, sender, .. }) => {
                    if index == BLOCK_ANNOUNCES_INDEX && !holding {
                        holding = true;
                        metrics.held.fetch_add(1, Ordering::Relaxed);
                    } else if index == GRANDPA_INDEX && grandpa.on_open(sender) {
                        metrics.grandpa_open.fetch_add(1, Ordering::Relaxed);
                        let _ = events.send(GossipEvent::GrandpaOpen { peer: id });
                    }
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::CustomProtocolRefused { index, .. }) => {
                    if index == BLOCK_ANNOUNCES_INDEX {
                        metrics.refused.fetch_add(1, Ordering::Relaxed);
                    } else if index == GRANDPA_INDEX {
                        metrics.grandpa_refused.fetch_add(1, Ordering::Relaxed);
                    }
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::CustomProtocolClosed { index, .. }) => {
                    if index == BLOCK_ANNOUNCES_INDEX && holding {
                        holding = false;
                        metrics.held.fetch_sub(1, Ordering::Relaxed);
                    } else if index == GRANDPA_INDEX && grandpa.on_close() {
                        metrics.grandpa_open.fetch_sub(1, Ordering::Relaxed);
                        let _ = events.send(GossipEvent::GrandpaClosed { peer: id });
                    }
                }
                SwarmEvent::Behaviour(NotificationsToSwarm::Notification { index, message, .. })
                    if index == GRANDPA_INDEX =>
                {
                    // Stamp the time first, before any work on the message.
                    let at = Instant::now();
                    match grandpa.on_notification(&message) {
                        GrandpaMessage::Neighbor(packet) => {
                            metrics.neighbors.fetch_add(1, Ordering::Relaxed);
                            let _ = events.send(GossipEvent::Neighbor { peer: id, at, packet });
                        }
                        GrandpaMessage::Commit { round, set_id, target } => {
                            metrics.commits.fetch_add(1, Ordering::Relaxed);
                            let _ = events.send(GossipEvent::Commit {
                                peer: id, at, round, set_id, target, bytes: message.len(),
                            });
                        }
                        GrandpaMessage::Vote => {
                            metrics.votes.fetch_add(1, Ordering::Relaxed);
                        }
                        GrandpaMessage::Other => {
                            metrics.other.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ => {}
            },
            changed = stop.changed().fuse() => if changed.is_err() {
                break;
            },
        }
    }

    if holding {
        metrics.held.fetch_sub(1, Ordering::Relaxed);
    }
    if grandpa.is_open() {
        metrics.grandpa_open.fetch_sub(1, Ordering::Relaxed);
    }
}

fn print_progress(metrics: &Metrics, live: &Live, phase: &str, elapsed: f64, opened: usize) {
    print!(
        "\r  [{phase}] t={elapsed:.0}s offered={opened} held={} grandpa={} synced={} refused={}/{} node_head={} commits={} neighbors={} | lag s p50={:.0} p90={:.0} max={:.0} | blocks p50={:.0} max={:.0} | fresh={:.0}%   ",
        metrics.held.load(Ordering::Relaxed),
        metrics.grandpa_open.load(Ordering::Relaxed),
        live.peers_open,
        metrics.refused.load(Ordering::Relaxed),
        metrics.grandpa_refused.load(Ordering::Relaxed),
        live.node_head,
        metrics.commits.load(Ordering::Relaxed),
        metrics.neighbors.load(Ordering::Relaxed),
        live.lag_s_p50,
        live.lag_s_p90,
        live.lag_s_max,
        live.lag_blocks_p50,
        live.lag_blocks_max,
        live.fresh_pct,
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

fn write_summary(
    path: &Path,
    peers: usize,
    role: HoldRole,
    hold_secs: f64,
    s: &Summary,
) -> std::io::Result<()> {
    let mut file = fs::File::create(path)?;
    let predicted = s.peers_open_at_end as f64 / LUCKY_PEERS as f64 * s.round_interval_s;
    writeln!(file, "peers={peers}")?;
    writeln!(file, "role={role:?}")?;
    writeln!(file, "hold_s={hold_secs:.0}")?;
    writeln!(file, "peers_open={}", s.peers_open_at_end)?;
    writeln!(file, "peers_with_commit={}", s.peers_with_commit)?;
    writeln!(file, "node_commits={}", s.node_commits)?;
    writeln!(file, "commit_interval_s={:.2}", s.commit_interval_s)?;
    writeln!(file, "node_rounds={}", s.node_rounds)?;
    writeln!(file, "round_interval_s={:.2}", s.round_interval_s)?;
    writeln!(file, "commits_delivered={}", s.commits_delivered)?;
    writeln!(file, "duplicates={}", s.duplicates)?;
    writeln!(file, "predicted_gap_s={predicted:.1}")?;
    for (name, p) in [
        ("gap_s", s.gap_s),
        ("lag_s", s.lag_s),
        ("lag_blocks", s.lag_blocks),
    ] {
        writeln!(
            file,
            "{name}_p50={:.1}\n{name}_p90={:.1}\n{name}_p99={:.1}\n{name}_max={:.1}\n{name}_mean={:.1}",
            p.p50, p.p90, p.p99, p.max, p.mean
        )?;
    }
    writeln!(file, "samples={}", s.samples)?;
    writeln!(file, "commit_bytes_p50={:.0}", s.commit_bytes.p50)?;
    writeln!(file, "commit_bytes_max={:.0}", s.commit_bytes.max)?;
    writeln!(file, "samples_over_cap={}", s.samples_over_cap)?;
    Ok(())
}

fn print_report(
    peers: usize,
    role: HoldRole,
    hold_secs: f64,
    held_at_end: u64,
    metrics: &Metrics,
    s: &Summary,
) {
    let open = s.peers_open_at_end;
    let per_commit = if s.node_commits > 0 {
        s.commits_delivered as f64 / s.node_commits as f64
    } else {
        0.0
    };
    let per_round = if s.node_rounds > 0 {
        s.commits_delivered as f64 / s.node_rounds as f64
    } else {
        0.0
    };
    let dup_pct = if s.commits_delivered > 0 {
        100.0 * s.duplicates as f64 / s.commits_delivered as f64
    } else {
        0.0
    };
    let kb_per_s = if s.commit_interval_s > 0.0 {
        s.commit_bytes.mean / s.commit_interval_s / 1000.0
    } else {
        0.0
    };

    println!("\n=== finality-lag: {open} {role:?} peers, {hold_secs:.0} s ===");
    println!(
        "lag        p50 {:>4.0} s | p90 {:>4.0} s | max {:>4.0} s   ({:.0} / {:.0} / {:.0} blocks)   how far a peer's finalized head trails the node",
        s.lag_s.p50, s.lag_s.p90, s.lag_s.max, s.lag_blocks.p50, s.lag_blocks.p90, s.lag_blocks.max,
    );
    println!(
        "gap        p50 {:>4.0} s | p90 {:>4.0} s | max {:>4.0} s   between two commits reaching the same peer",
        s.gap_s.p50, s.gap_s.p90, s.gap_s.max,
    );
    println!(
        "node       round every {:.1} s | commit every {:.1} s | {} rounds, {} commits in the window",
        s.round_interval_s, s.commit_interval_s, s.node_rounds, s.node_commits,
    );
    println!(
        "delivered  each commit reached {per_commit:.1} of {open} peers | {per_round:.1} deliveries per round | {} duplicates ({dup_pct:.0}%)",
        s.duplicates,
    );
    println!(
        "commit     {:.0} kB on the wire ({:.0} min, {:.0} max) | {kb_per_s:.1} kB/s per light peer | {:.0} kB/s for these {open}",
        s.commit_bytes.p50 / 1000.0,
        s.commit_bytes.min / 1000.0,
        s.commit_bytes.max / 1000.0,
        kb_per_s * open as f64,
    );

    // Only speak up when something needs attention.
    let mut notes = Vec::new();
    if held_at_end < peers as u64 || open < peers {
        notes.push(format!(
            "{held_at_end}/{peers} held block-announces, {open}/{peers} grandpa at window end"
        ));
    }
    if metrics.refused.load(Ordering::Relaxed) > 0 {
        notes.push(format!(
            "{} block-announces refused (node at its light-peer limit?)",
            metrics.refused.load(Ordering::Relaxed)
        ));
    }
    if s.peers_with_commit < open {
        notes.push(format!(
            "{} of {open} peers never received a commit",
            open - s.peers_with_commit
        ));
    }
    if s.samples_over_cap > 0 {
        notes.push(format!(
            "{} of {} peer-samples more than {:.1} min behind",
            s.samples_over_cap,
            s.samples,
            REBROADCAST_AFTER.as_secs_f64() / 60.0 + 0.5
        ));
    }
    let votes = metrics.votes.load(Ordering::Relaxed);
    let other = metrics.other.load(Ordering::Relaxed);
    if votes > 0 || other > 0 {
        notes.push(format!(
            "{votes} votes and {other} undecodable messages on the grandpa substreams"
        ));
    }
    if notes.is_empty() {
        println!("health     ok: every peer held both substreams and received commits");
    } else {
        for note in notes {
            println!("health     {note}");
        }
    }
}

/// Entry point for the `finality-lag` command.
#[allow(clippy::too_many_arguments)]
pub async fn finality_lag(
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
    out_dir: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let peers = peers.max(1);

    if let Some(dir) = &out_dir {
        fs::create_dir_all(dir)?;
    }
    let anchor = WallAnchor::now();

    let address = address
        .or_else(|| chain.map(|c| c.address().to_string()))
        .ok_or("provide --address or --chain")?;
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
        grandpa: true,
    };

    println!("Address:    {address}");
    println!("Protocols:  /{genesis}/block-announces/1 + /{genesis}/grandpa/1");
    println!(
        "Role:       {:?} (handshake byte {})",
        role,
        role.protocol_role().encoded()
    );
    println!(
        "Run:        peers={peers} hold={:.0}s after all peers connect (one new peer every {ramp_ms} ms)",
        duration.as_secs_f64(),
    );

    let multiaddr: Multiaddr = address.parse()?;
    let metrics = Arc::new(Metrics::default());
    let (stop_tx, stop_rx) = watch::channel(false);
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (live_tx, live_rx) = watch::channel(Live::default());

    let open_csv = |name: &str, header: &str| -> std::io::Result<Option<fs::File>> {
        let Some(dir) = &out_dir else {
            return Ok(None);
        };
        let mut file = fs::File::create(dir.join(name))?;
        writeln!(file, "{header}")?;
        Ok(Some(file))
    };
    let collector = Collector {
        peers: (0..peers).map(|_| PeerState::default()).collect(),
        node_finalized: BTreeMap::new(),
        node_head: 0,
        node_rounds: BTreeMap::new(),
        hold_start: None,
        open_at_end: None,
        anchor,
        commits_csv: open_csv(
            "commits.csv",
            "epoch_ms,peer,set_id,round,target,node_head,delay_ms,in_hold_window,bytes",
        )?,
        samples_csv: open_csv(
            "lag-samples.csv",
            "epoch_ms,phase,elapsed_s,node_head,peers_open,lag_s_p50,lag_s_p90,lag_s_max,lag_blocks_p50,lag_blocks_p90,lag_blocks_max,fresh_pct",
        )?,
        commit_bytes: Vec::new(),
        lag_s: Vec::new(),
        lag_blocks: Vec::new(),
        samples_over_cap: 0,
        samples: 0,
        live: live_tx,
    };
    let collector = tokio::spawn(collect(collector, events_rx));

    // Phase 1: ramp the peers in, then let the dials settle (same shape as
    // hold-peers).
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
    let resolved =
        |m: &Metrics| m.connected.load(Ordering::Relaxed) + m.dial_failed.load(Ordering::Relaxed);
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
                    "\n  warning: only {}/{peers} dials resolved within {:.0}s; starting the hold window anyway",
                    resolved(&metrics),
                    connect_timeout.as_secs_f64(),
                );
                break;
            }
        }
        futures::select! {
            _ = spawn_tick.tick().fuse() => {
                let batch = if ramp_ms == 0 { peers - opened } else { 1 };
                for _ in 0..batch {
                    if opened >= peers {
                        break;
                    }
                    handles.push(tokio::spawn(run_peer(
                        opened,
                        multiaddr.clone(),
                        data.clone(),
                        idle_timeout,
                        stop_rx.clone(),
                        events_tx.clone(),
                        metrics.clone(),
                    )));
                    opened += 1;
                }
            }
            _ = settle_tick.tick().fuse() => {}
            _ = progress_tick.tick().fuse() => {
                let live = live_rx.borrow().clone();
                print_progress(&metrics, &live, "connect", connect_start.elapsed().as_secs_f64(), opened);
            }
        }
    }

    let connect_secs = connect_start.elapsed().as_secs_f64();
    println!(
        "\n\nAll {peers} peers dialed in {connect_secs:.1}s ({} connected, {} failed, {} held, {} grandpa). Holding for {:.0}s...\n",
        metrics.connected.load(Ordering::Relaxed),
        metrics.dial_failed.load(Ordering::Relaxed),
        metrics.held.load(Ordering::Relaxed),
        metrics.grandpa_open.load(Ordering::Relaxed),
        duration.as_secs_f64(),
    );

    // Phase 2: the hold window.
    let hold_start = Instant::now();
    let _ = events_tx.send(GossipEvent::HoldStart { at: hold_start });
    let hold_sleep = tokio::time::sleep_until(TokioInstant::now() + duration);
    tokio::pin!(hold_sleep);
    loop {
        futures::select! {
            _ = (&mut hold_sleep).fuse() => break,
            _ = progress_tick.tick().fuse() => {
                let live = live_rx.borrow().clone();
                print_progress(&metrics, &live, "hold", hold_start.elapsed().as_secs_f64(), opened);
            }
        }
    }
    let hold_secs = hold_start.elapsed().as_secs_f64();
    let held_at_end = metrics.held.load(Ordering::Relaxed);

    // Mark the end before the holders drop their substreams, so "open at the
    // end" describes the window and not the teardown.
    let _ = events_tx.send(GossipEvent::HoldEnd);
    let _ = stop_tx.send(true);
    println!("\nHold window over, draining {} peer(s)...", handles.len());
    join_all(handles).await;
    drop(events_tx);
    let summary = collector.await.unwrap_or_default();

    print_report(peers, role, hold_secs, held_at_end, &metrics, &summary);

    if let Some(dir) = &out_dir {
        write_summary(&dir.join("summary.txt"), peers, role, hold_secs, &summary)?;
        println!(
            "\nwrote {}, {} and {}",
            dir.join("commits.csv").display(),
            dir.join("lag-samples.csv").display(),
            dir.join("summary.txt").display(),
        );
    }
    Ok(())
}
