// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Impersonate warp-syncing clients against one full node, to load-test its
//! GRANDPA warp-proof server (`/<genesis>/sync/warp`).
//!
//! Why this is its own command rather than a `spam-light` method: warp sync is a
//! *sequence per client*, not a method mix. A real client sends one request at a
//! time and each response feeds the next, so load comes from the number of
//! clients syncing at once, not from pipeline depth. Each client here therefore
//! keeps exactly one request in flight — which is also what makes the node's
//! inbound warp queue the binding constraint.
//!
//! That queue is the thing worth measuring: `MAX_WARP_REQUEST_QUEUE` is a
//! hardcoded 20 (`sync/src/warp_request_handler.rs:38`) with no CLI flag, an
//! order of magnitude below `--light-client-request-queue-size`. Past it the
//! request is dropped, and client-side that is indistinguishable from a rejected
//! `begin`: both are a closed substream with no body.
//!
//! Tell them apart with `substrate_sub_libp2p_requests_in_failure_total`, whose
//! `reason` label differs per network backend:
//!
//! | cause | litep2p | libp2p |
//! |---|---|---|
//! | inbound queue full | `sending into a full channel` | `busy-omitted` |
//! | handler refused (bad `begin`) | `rejected` | (no response recorded) |
//!
//! litep2p passes the `try_send` error's `Display` straight through as the label
//! (`litep2p/shim/request_response/mod.rs:330`), which is where that long
//! `async_channel` string comes from; libp2p maps `InboundFailure::ResponseOmission`
//! to `busy-omitted` (`service.rs:1533`).
//!
//! Do **not** use `requests_in_success_total` as a serve-time measure on litep2p:
//! its `started` instant is captured *after* the response future resolves
//! (`mod.rs:318-320`), so it times only handing the response to the transport —
//! microseconds — and excludes proof generation entirely. The libp2p backend
//! stamps arrival time instead and does include it.
//!
//! No response decoding is needed. `begin` only has to be a finalized canonical
//! block hash (`grandpa/src/warp_proof.rs:95-115`), so we walk the chain with
//! hashes fetched from RPC up front, and read the fragment count and
//! `is_finished` flag straight off the response body. See
//! [`subp2p_explorer::warp`].

use crate::commands::authorities::{client, fetch_genesis_hash};
use crate::commands::light_common::{
    build_swarm, classify_failure, merge_error_map, percentile_ms, Chain, SpamBehaviour,
    SpamBehaviourEvent,
};
use futures::{future::join_all, FutureExt, StreamExt};
use jsonrpsee::{client_transport::ws::Url, core::client::ClientT, rpc_params};
use libp2p::{
    identify,
    request_response::{self, Message as RrMessage, OutboundFailure},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subp2p_explorer::warp;

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Lock-free counters shared by all clients, read by the orchestrator for the
/// aggregate progress line.
#[derive(Default)]
struct Metrics {
    issued: AtomicU64,
    ok: AtomicU64,
    err: AtomicU64,
    timeout: AtomicU64,
    bytes: AtomicU64,
    connected: AtomicU64,
}

/// Everything one client observed, merged across clients for the summary.
#[derive(Default)]
struct WarpStats {
    issued: u64,
    ok: u64,
    err: u64,
    timeout: u64,
    bytes_total: u64,
    bytes_max: u64,
    fragments_total: u64,
    /// Responses whose trailing flag said the proof was complete.
    finished_responses: u64,
    /// Responses that arrived but whose body we could not summarise.
    unparseable: u64,
    latencies_us: Vec<u64>,
    sample: Option<String>,
    last_err: Option<String>,
    errors: HashMap<String, u64>,
}

impl WarpStats {
    fn merge(&mut self, other: WarpStats) {
        self.issued += other.issued;
        self.ok += other.ok;
        self.err += other.err;
        self.timeout += other.timeout;
        self.bytes_total += other.bytes_total;
        self.bytes_max = self.bytes_max.max(other.bytes_max);
        self.fragments_total += other.fragments_total;
        self.finished_responses += other.finished_responses;
        self.unparseable += other.unparseable;
        self.latencies_us.extend(other.latencies_us);
        if self.sample.is_none() {
            self.sample = other.sample;
        }
        if self.last_err.is_none() {
            self.last_err = other.last_err;
        }
        merge_error_map(&mut self.errors, other.errors);
    }
}

// ---------------------------------------------------------------------------
// One warp-syncing client
// ---------------------------------------------------------------------------

/// One fake warp-syncing client: its own identity, one request in flight at a
/// time, walking `begins` until it has sent `requests` of them.
struct WarpClient {
    id: usize,
    swarm: Swarm<SpamBehaviour>,
    peer: Option<PeerId>,
    /// Pre-fetched `begin` hashes, cycled through in order.
    begins: Arc<Vec<[u8; 32]>>,
    stats: WarpStats,
    pending: Option<(usize, Instant)>,
    issued: usize,
    requests: usize,
    protocol: String,
    metrics: Arc<Metrics>,
}

impl WarpClient {
    fn done(&self) -> bool {
        self.issued >= self.requests && self.pending.is_none()
    }

    /// Send the next warp request, if the connection is up and there is budget
    /// left. Exactly one request is in flight at a time, mirroring a real client.
    fn send_next(&mut self) {
        let Some(peer) = self.peer else { return };
        if self.pending.is_some() || self.issued >= self.requests {
            return;
        }
        let idx = self.issued % self.begins.len();
        let payload = warp::encode_request(&self.begins[idx]);
        // The returned request id is not tracked: with one request in flight
        // there is nothing to correlate against.
        let _ = self
            .swarm
            .behaviour_mut()
            .light
            .send_request(&peer, payload);
        self.pending = Some((idx, Instant::now()));
        self.stats.issued += 1;
        self.issued += 1;
        self.metrics.issued.fetch_add(1, Ordering::Relaxed);
    }

    fn on_response(&mut self, response: Result<Vec<u8>, ()>) {
        let Some((idx, t0)) = self.pending.take() else {
            return;
        };
        match response {
            Ok(bytes) => {
                self.stats.ok += 1;
                self.metrics.ok.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .latencies_us
                    .push(t0.elapsed().as_micros() as u64);
                let len = bytes.len() as u64;
                self.stats.bytes_total += len;
                self.stats.bytes_max = self.stats.bytes_max.max(len);
                self.metrics.bytes.fetch_add(len, Ordering::Relaxed);

                match warp::summarize_response(&bytes) {
                    Some(s) => {
                        self.stats.fragments_total += s.fragments;
                        if s.is_finished {
                            self.stats.finished_responses += 1;
                        }
                        if self.stats.sample.is_none() {
                            self.stats.sample = Some(format!(
                                "{} fragments, is_finished={}, {} bytes (begin #{idx})",
                                s.fragments, s.is_finished, s.len
                            ));
                        }
                    }
                    None => self.stats.unparseable += 1,
                }
            }
            // The node closed the substream without a body. Two very different
            // causes look identical here: the inbound queue was full and the
            // request was silently dropped, or `begin` was rejected (not
            // finalized / not canonical / set-change history incomplete). The
            // node's `busy-omitted` counter separates them.
            Err(()) => {
                self.stats.err += 1;
                self.metrics.err.fetch_add(1, Ordering::Relaxed);
                let reason =
                    "no-response: substream closed with no proof (queue-full drop or bad begin)";
                *self.stats.errors.entry(reason.to_string()).or_insert(0) += 1;
                self.stats.last_err = Some(reason.to_string());
            }
        }
    }

    fn on_failure(&mut self, error: OutboundFailure) {
        if self.pending.take().is_none() {
            return;
        }
        if matches!(error, OutboundFailure::Timeout) {
            self.stats.timeout += 1;
            self.metrics.timeout.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.err += 1;
            self.metrics.err.fetch_add(1, Ordering::Relaxed);
        }
        let reason = classify_failure(&error);
        self.stats.last_err = Some(reason.clone());
        *self.stats.errors.entry(reason).or_insert(0) += 1;
    }

    fn handle_event(&mut self, event: SwarmEvent<SpamBehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                if self.peer.is_none() {
                    self.peer = Some(peer_id);
                    self.metrics.connected.fetch_add(1, Ordering::Relaxed);
                    log::info!("client {} connected to {peer_id}", self.id);
                    self.send_next();
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                log::info!(
                    "client {} identified {peer_id} agent={:?}",
                    self.id,
                    info.agent_version
                );
                if self.id == 0 && !info.protocols.iter().any(|p| p.as_ref() == self.protocol) {
                    println!(
                        "WARNING: peer does not advertise {} (fork_id? pass --protocol)",
                        self.protocol
                    );
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                request_response::Event::Message { message, .. },
            )) => {
                if let RrMessage::Response { response, .. } = message {
                    self.on_response(response);
                    self.send_next();
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                request_response::Event::OutboundFailure { error, .. },
            )) => {
                self.on_failure(error);
                self.send_next();
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                log::warn!(
                    "client {} connection error (peer={peer_id:?}): {error}",
                    self.id
                );
            }
            other => log::trace!("swarm event: {other:?}"),
        }
    }

    async fn run(&mut self, deadline: tokio::time::Instant) {
        self.send_next();
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        loop {
            futures::select! {
                event = self.swarm.select_next_some().fuse() => {
                    self.handle_event(event);
                    if self.done() {
                        break;
                    }
                }
                _ = (&mut sleep).fuse() => break,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// `begin` hashes
// ---------------------------------------------------------------------------

/// Pre-fetch the `begin` hashes the run will cycle through.
///
/// Fetched up front rather than between requests so no RPC round trip lands in
/// the middle of a latency measurement. `begin` need only be a finalized
/// canonical block hash, so plain block numbers are enough — no need to land on
/// authority-set-change blocks, and no response decoding.
///
/// Stops early (with a warning) at the first block the node cannot resolve,
/// which is how overshooting the finalized head shows up.
async fn fetch_begin_hashes(
    rpc_url: Url,
    start_block: u64,
    step: u64,
    count: usize,
) -> Result<Vec<[u8; 32]>, Box<dyn Error>> {
    let rpc = client(rpc_url).await?;
    let mut out = Vec::with_capacity(count);

    for i in 0..count {
        let number = start_block + step * i as u64;
        let hash: Option<String> = rpc
            .request("chain_getBlockHash", rpc_params![number])
            .await?;
        let Some(hash) = hash else {
            println!(
                "  note: block #{number} not available (past the finalized head?); \
                 cycling the {} hash(es) fetched so far",
                out.len()
            );
            break;
        };
        let raw = hex::decode(hash.trim_start_matches("0x"))?;
        let raw: [u8; 32] = raw
            .try_into()
            .map_err(|_| format!("block hash for #{number} is not 32 bytes"))?;
        out.push(raw);
    }

    if out.is_empty() {
        return Err("could not resolve any begin block hash".into());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn print_progress(m: &Metrics, total: usize, start: Instant) {
    let issued = m.issued.load(Ordering::Relaxed);
    let ok = m.ok.load(Ordering::Relaxed);
    let err = m.err.load(Ordering::Relaxed);
    let timeout = m.timeout.load(Ordering::Relaxed);
    let bytes = m.bytes.load(Ordering::Relaxed);
    let connected = m.connected.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    print!(
        "\r  issued={issued}/{total} clients={connected} ok={ok} err={err} timeout={timeout} | {:.1} MiB ({:.1} MiB/s) | {:.0}s   ",
        mib(bytes),
        mib(bytes) / elapsed,
        elapsed
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

#[allow(clippy::too_many_arguments)]
fn print_report(
    stats: &WarpStats,
    elapsed: f64,
    clients: usize,
    connected: u64,
    requests: usize,
    begins: usize,
    protocol: &str,
) {
    let mut lat = stats.latencies_us.clone();
    lat.sort_unstable();
    let avg_bytes = if stats.ok > 0 {
        stats.bytes_total / stats.ok
    } else {
        0
    };
    let avg_fragments = if stats.ok > 0 {
        stats.fragments_total as f64 / stats.ok as f64
    } else {
        0.0
    };

    println!("\n=== /sync/warp summary ===");
    println!("protocol:    {protocol}");
    println!(
        "clients:     {clients} (connected {connected}) | requests/client {requests} | begin hashes {begins} | total {}",
        clients.saturating_mul(requests)
    );
    println!(
        "issued={} ok={} err={} timeout={} in {elapsed:.1}s => {:.1} ok req/s",
        stats.issued,
        stats.ok,
        stats.err,
        stats.timeout,
        stats.ok as f64 / elapsed
    );
    println!(
        "latency ms:  p50={:.1} p90={:.1} p99={:.1} max={:.1}",
        percentile_ms(&lat, 50),
        percentile_ms(&lat, 90),
        percentile_ms(&lat, 99),
        percentile_ms(&lat, 100),
    );
    println!(
        "proof bytes: total={:.1} MiB | avg/response={avg_bytes} B ({:.2} MiB) | max={:.2} MiB | {:.2} MiB/s",
        mib(stats.bytes_total),
        mib(avg_bytes),
        mib(stats.bytes_max),
        mib(stats.bytes_total) / elapsed,
    );
    println!(
        "fragments:   total={} | avg/response={avg_fragments:.1}",
        stats.fragments_total
    );
    println!(
        "is_finished: {} of {} ok response(s) reported a complete proof{}",
        stats.finished_responses,
        stats.ok,
        if stats.unparseable > 0 {
            format!(" | {} unparseable body(ies)", stats.unparseable)
        } else {
            String::new()
        }
    );
    if let Some(sample) = &stats.sample {
        println!("sample:      {sample}");
    }
    if !stats.errors.is_empty() {
        let mut rows: Vec<(&String, &u64)> = stats.errors.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        println!("errors (reason -> count):");
        for (reason, count) in rows {
            println!("  {count:>6}  {reason}");
        }
        println!(
            "  note: a closed substream with no proof is either a queue-full drop or a\n        \
             rejected begin. Separate them on the node's Prometheus endpoint with\n        \
             substrate_sub_libp2p_requests_in_failure_total{{reason=...}}:\n          \
             litep2p: \"sending into a full channel\" = shed, \"rejected\" = bad begin\n          \
             libp2p:  \"busy-omitted\" = shed"
        );
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Entry point for the `warp-sync` command.
#[allow(clippy::too_many_arguments)]
pub async fn warp_sync(
    chain: Option<Chain>,
    url: Option<String>,
    address: Option<String>,
    genesis: Option<String>,
    protocol: Option<String>,
    begin: Option<String>,
    begin_block: u64,
    step: u64,
    clients: usize,
    requests: usize,
    stagger_ms: u64,
    request_timeout: Duration,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let clients = clients.max(1);
    let requests = requests.max(1);

    let url = url
        .or_else(|| chain.map(|c| c.rpc_url().to_string()))
        .ok_or("provide --url or --chain")?;
    let address = address
        .or_else(|| chain.map(|c| c.address().to_string()))
        .ok_or("provide --address or --chain")?;
    let rpc_url = Url::parse(&url)?;

    let genesis = match genesis {
        Some(g) => g.trim_start_matches("0x").to_string(),
        None => {
            println!("Fetching genesis from RPC...");
            fetch_genesis_hash(rpc_url.clone()).await?
        }
    };
    let protocol = protocol.unwrap_or_else(|| warp::protocol_name(&genesis));

    println!("URL:         {url}");
    println!("Address:     {address}");
    println!("Protocol:    {protocol}");

    // Resolve the begin hash(es). An explicit --begin pins one; otherwise walk
    // block numbers from --begin-block in --step increments.
    let begins: Vec<[u8; 32]> = match begin {
        Some(hash) => {
            let raw = hex::decode(hash.trim_start_matches("0x"))?;
            let raw: [u8; 32] = raw
                .try_into()
                .map_err(|_| "--begin is not a 32-byte block hash")?;
            println!("Begin:       0x{} (pinned)", hex::encode(raw));
            vec![raw]
        }
        None if step == 0 => {
            // Every request starts from the same block. With --begin-block 0 that
            // is genesis, which makes the node walk the whole authority-set
            // history each time: the most expensive request it can be asked for.
            let raw = hex::decode(&genesis)?;
            let raw: [u8; 32] = raw.try_into().map_err(|_| "genesis hash is not 32 bytes")?;
            if begin_block == 0 {
                println!("Begin:       genesis 0x{} (fixed — heaviest case)", genesis);
                vec![raw]
            } else {
                println!("Resolving begin block #{begin_block}...");
                fetch_begin_hashes(rpc_url.clone(), begin_block, 0, 1).await?
            }
        }
        None => {
            println!(
                "Resolving {requests} begin hashes from #{begin_block} step {step} via RPC..."
            );
            fetch_begin_hashes(rpc_url.clone(), begin_block, step, requests).await?
        }
    };

    println!(
        "Load:        clients={clients} requests/client={requests} (1 in flight each) => total {} req (req timeout {request_timeout:?})",
        clients.saturating_mul(requests)
    );
    if begins.len() == 1 {
        println!(
            "             every request re-sends the same begin, so the node rebuilds \
             the same proof each time"
        );
    }

    let multiaddr: Multiaddr = address.parse()?;
    let begins = Arc::new(begins);
    let metrics = Arc::new(Metrics::default());
    let deadline = tokio::time::Instant::now() + timeout;
    let start = Instant::now();

    println!("Spawning {clients} client(s)...\n");
    let mut handles = Vec::with_capacity(clients);
    for id in 0..clients {
        let proto = protocol.clone();
        let begins_c = begins.clone();
        let metrics_c = metrics.clone();
        let addr_c = multiaddr.clone();
        handles.push(tokio::spawn(async move {
            // One request in flight per client, so the per-connection stream cap
            // never binds; keep libp2p's default headroom.
            let mut swarm = match build_swarm(&proto, request_timeout, 16).await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("client {id}: build_swarm failed: {e}");
                    return WarpStats::default();
                }
            };
            if let Err(e) = swarm.dial(addr_c) {
                log::error!("client {id}: dial failed: {e}");
                return WarpStats::default();
            }
            let mut c = WarpClient {
                id,
                swarm,
                peer: None,
                begins: begins_c,
                stats: WarpStats::default(),
                pending: None,
                issued: 0,
                requests,
                protocol: proto,
                metrics: metrics_c,
            };
            c.run(deadline).await;
            c.stats
        }));
        if stagger_ms > 0 && id + 1 < clients {
            tokio::time::sleep(Duration::from_millis(stagger_ms)).await;
        }
    }

    let joined = join_all(handles);
    tokio::pin!(joined);
    let mut progress = tokio::time::interval(Duration::from_secs(1));
    progress.tick().await;
    let total = clients.saturating_mul(requests);
    let results = loop {
        futures::select! {
            r = (&mut joined).fuse() => break r,
            _ = progress.tick().fuse() => print_progress(&metrics, total, start),
        }
    };
    println!();

    let mut merged = WarpStats::default();
    for r in results {
        match r {
            Ok(stats) => merged.merge(stats),
            Err(e) => log::error!("client task join error: {e}"),
        }
    }

    print_report(
        &merged,
        start.elapsed().as_secs_f64().max(0.001),
        clients,
        metrics.connected.load(Ordering::Relaxed),
        requests,
        begins.len(),
        &protocol,
    );

    Ok(())
}
