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
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use subp2p_explorer::warp;
use tokio::time::Instant as TokioInstant;

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Lock-free counters shared by all clients, read by the orchestrator for the
/// aggregate progress line and the CSV samples.
#[derive(Default)]
struct Metrics {
    issued: AtomicU64,
    ok: AtomicU64,
    /// `Err(())` — the node closed the substream with no proof. Counted apart
    /// from `err` because this is the load-shedding signal, whereas `err` is
    /// transport trouble on our side.
    shed: AtomicU64,
    timeout: AtomicU64,
    err: AtomicU64,
    /// Issued but abandoned: connection closed or deadline hit while in flight.
    aborted: AtomicU64,
    bytes: AtomicU64,
    fragments: AtomicU64,
    connected: AtomicU64,
    /// Soak mode only: connections currently alive, and the high-water mark.
    concurrent: AtomicU64,
    peak_concurrent: AtomicU64,
    opened: AtomicU64,
}

impl Metrics {
    fn bump_concurrent(&self) {
        let c = self.concurrent.fetch_add(1, Ordering::Relaxed) + 1;
        let mut p = self.peak_concurrent.load(Ordering::Relaxed);
        while c > p {
            match self.peak_concurrent.compare_exchange_weak(
                p,
                c,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => p = x,
            }
        }
    }
}

/// Everything one client observed, merged across clients for the summary.
#[derive(Default)]
struct WarpStats {
    issued: u64,
    ok: u64,
    /// Node closed the substream with no proof: shed, or `begin` refused.
    shed: u64,
    err: u64,
    timeout: u64,
    /// Issued, then abandoned without an outcome. Tracked so that
    /// `ok + shed + err + timeout + aborted == issued` always holds.
    aborted: u64,
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
        self.shed += other.shed;
        self.err += other.err;
        self.timeout += other.timeout;
        self.aborted += other.aborted;
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
        record_response(&mut self.stats, &self.metrics, response, idx, t0);
    }

    fn on_failure(&mut self, error: OutboundFailure) {
        if self.pending.take().is_none() {
            return;
        }
        record_failure(&mut self.stats, &self.metrics, error);
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
        // Still in flight when the deadline fired: no outcome will arrive.
        if self.pending.take().is_some() {
            self.stats.aborted += 1;
            self.metrics.aborted.fetch_add(1, Ordering::Relaxed);
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
    let shed = m.shed.load(Ordering::Relaxed);
    let err = m.err.load(Ordering::Relaxed);
    let timeout = m.timeout.load(Ordering::Relaxed);
    let bytes = m.bytes.load(Ordering::Relaxed);
    let connected = m.connected.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    print!(
        "\r  issued={issued}/{total} clients={connected} ok={ok} shed={shed} timeout={timeout} err={err} | {:.1} MiB ({:.1} MiB/s) | {:.0}s   ",
        mib(bytes),
        mib(bytes) / elapsed,
        elapsed
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Soak progress: rolling rates matter more than totals over a long run.
fn print_soak_progress(m: &Metrics, start: Instant, lifetime: usize) {
    let issued = m.issued.load(Ordering::Relaxed);
    let ok = m.ok.load(Ordering::Relaxed);
    let shed = m.shed.load(Ordering::Relaxed);
    let timeout = m.timeout.load(Ordering::Relaxed);
    let bytes = m.bytes.load(Ordering::Relaxed);
    let concurrent = m.concurrent.load(Ordering::Relaxed);
    let peak = m.peak_concurrent.load(Ordering::Relaxed);
    let opened = m.opened.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    print!(
        "\r  t={:.0}s conns={concurrent}(peak {peak}, opened {opened}x{lifetime}) | offered {:.1}/s served {:.1}/s shed {:.1}/s | {:.1} MiB/s | ok={ok} shed={shed} timeout={timeout} aborted={}   ",
        elapsed,
        issued as f64 / elapsed,
        ok as f64 / elapsed,
        shed as f64 / elapsed,
        mib(bytes) / elapsed,
        m.aborted.load(Ordering::Relaxed),
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Rolling deltas since the previous snapshot, so drift over a long soak is
/// visible in the console as well as the CSV. `last` is `(time, ok, shed, bytes)`.
fn print_drift(m: &Metrics, last: &mut (Instant, u64, u64, u64)) {
    let now = Instant::now();
    let ok = m.ok.load(Ordering::Relaxed);
    let shed = m.shed.load(Ordering::Relaxed);
    let bytes = m.bytes.load(Ordering::Relaxed);
    let dt = now.duration_since(last.0).as_secs_f64().max(0.001);
    println!(
        "\n  [drift t+{:.0}s] served {:.1}/s shed {:.1}/s {:.1} MiB/s | concurrent {} | cum ok {ok} shed {shed}",
        dt,
        (ok - last.1) as f64 / dt,
        (shed - last.2) as f64 / dt,
        mib(bytes - last.3) / dt,
        m.concurrent.load(Ordering::Relaxed),
    );
    *last = (now, ok, shed, bytes);
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

/// One row per progress tick to `warp-samples.csv`. Stamped with the wall clock
/// so the series lines up with run-monitor-node's node.csv — which is the whole
/// point of a soak: RSS drift on one side against shed rate on the other.
struct CsvSampler {
    file: fs::File,
}

impl CsvSampler {
    fn create(path: &Path) -> std::io::Result<Self> {
        let mut file = fs::File::create(path)?;
        writeln!(
            file,
            "epoch_ms,elapsed_s,issued,ok,shed,timeout,err,bytes_total,fragments,aborted,concurrent,peak_concurrent,opened"
        )?;
        Ok(Self { file })
    }

    fn sample(&mut self, m: &Metrics, elapsed: f64) {
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let _ = writeln!(
            self.file,
            "{epoch_ms},{elapsed:.1},{},{},{},{},{},{},{},{},{},{},{}",
            m.issued.load(Ordering::Relaxed),
            m.ok.load(Ordering::Relaxed),
            m.shed.load(Ordering::Relaxed),
            m.timeout.load(Ordering::Relaxed),
            m.err.load(Ordering::Relaxed),
            m.bytes.load(Ordering::Relaxed),
            m.fragments.load(Ordering::Relaxed),
            m.aborted.load(Ordering::Relaxed),
            m.concurrent.load(Ordering::Relaxed),
            m.peak_concurrent.load(Ordering::Relaxed),
            m.opened.load(Ordering::Relaxed),
        );
        let _ = self.file.flush();
    }
}

/// `header` is the mode-specific block (already formatted) printed under the
/// protocol line, so burst and soak runs can describe themselves differently
/// without threading both sets of knobs through here.
fn print_report(stats: &WarpStats, elapsed: f64, header: &str, protocol: &str) {
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
    print!("{header}");
    println!(
        "issued={} ok={} shed={} timeout={} err={} aborted={} in {elapsed:.1}s => {:.1} ok req/s",
        stats.issued,
        stats.ok,
        stats.shed,
        stats.timeout,
        stats.err,
        stats.aborted,
        stats.ok as f64 / elapsed
    );
    let accounted = stats.ok + stats.shed + stats.timeout + stats.err + stats.aborted;
    if accounted != stats.issued {
        println!(
            "  WARNING: {accounted} outcomes for {} issued — accounting gap of {}",
            stats.issued,
            stats.issued.abs_diff(accounted)
        );
    }
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
// Shared recording
// ---------------------------------------------------------------------------

/// Fold one settled request into the stats. `idx` is the index of the `begin`
/// hash used, kept only to label the first sample.
fn record_response(
    stats: &mut WarpStats,
    metrics: &Metrics,
    response: Result<Vec<u8>, ()>,
    idx: usize,
    t0: Instant,
) {
    match response {
        Ok(bytes) => {
            stats.ok += 1;
            metrics.ok.fetch_add(1, Ordering::Relaxed);
            stats.latencies_us.push(t0.elapsed().as_micros() as u64);
            let len = bytes.len() as u64;
            stats.bytes_total += len;
            stats.bytes_max = stats.bytes_max.max(len);
            metrics.bytes.fetch_add(len, Ordering::Relaxed);

            match warp::summarize_response(&bytes) {
                Some(s) => {
                    stats.fragments_total += s.fragments;
                    metrics.fragments.fetch_add(s.fragments, Ordering::Relaxed);
                    if s.is_finished {
                        stats.finished_responses += 1;
                    }
                    if stats.sample.is_none() {
                        stats.sample = Some(format!(
                            "{} fragments, is_finished={}, {} bytes (begin #{idx})",
                            s.fragments, s.is_finished, s.len
                        ));
                    }
                }
                None => stats.unparseable += 1,
            }
        }
        // The node closed the substream without a body. Two very different causes
        // look identical here: the inbound queue was full and the request was
        // dropped, or `begin` was refused (not finalized / not canonical /
        // set-change history incomplete). Only the node's failure counter's
        // `reason` label separates them — see the module docs.
        Err(()) => {
            stats.shed += 1;
            metrics.shed.fetch_add(1, Ordering::Relaxed);
            let reason =
                "no-response: substream closed with no proof (queue-full drop or bad begin)";
            *stats.errors.entry(reason.to_string()).or_insert(0) += 1;
            stats.last_err = Some(reason.to_string());
        }
    }
}

fn record_failure(stats: &mut WarpStats, metrics: &Metrics, error: OutboundFailure) {
    if matches!(error, OutboundFailure::Timeout) {
        stats.timeout += 1;
        metrics.timeout.fetch_add(1, Ordering::Relaxed);
    } else {
        stats.err += 1;
        metrics.err.fetch_add(1, Ordering::Relaxed);
    }
    let reason = classify_failure(&error);
    stats.last_err = Some(reason.clone());
    *stats.errors.entry(reason).or_insert(0) += 1;
}

// ---------------------------------------------------------------------------
// Soak mode: open-loop, with connection churn
// ---------------------------------------------------------------------------

/// Outcome of awaiting the single in-flight request of a soak connection.
enum Outcome {
    Responded(Result<Vec<u8>, ()>),
    Failed(OutboundFailure),
    /// Deadline reached or the connection closed — retire this connection.
    Aborted,
}

/// Wait for this connection's one in-flight request to settle, or give up at the
/// run deadline / on connection close.
async fn await_request(swarm: &mut Swarm<SpamBehaviour>, deadline: TokioInstant) -> Outcome {
    let sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep);
    loop {
        futures::select! {
            ev = swarm.select_next_some().fuse() => match ev {
                SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                    request_response::Event::Message {
                        message: RrMessage::Response { response, .. },
                        ..
                    },
                )) => return Outcome::Responded(response),
                SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                    request_response::Event::OutboundFailure { error, .. },
                )) => return Outcome::Failed(error),
                SwarmEvent::ConnectionClosed { .. } => return Outcome::Aborted,
                _ => {}
            },
            _ = (&mut sleep).fuse() => return Outcome::Aborted,
        }
    }
}

/// One soak connection: fresh identity, dial, then `lifetime` warp requests
/// **back to back**, then disconnect.
///
/// It deliberately does not self-pace between requests. The rate is applied to
/// connection *arrivals* by the orchestrator, which is both how a real
/// warp-syncing client behaves — arrive, pull proofs as fast as the link allows,
/// leave — and the only model that works here: a connection that sits idle is
/// closed by the node after ~10s, since a request-response substream is not a
/// keep-alive protocol. Per-request pacing left connections idle for
/// `max_concurrent / rate` seconds between turns, so at low rates the node reaped
/// them before their next request and the work was simply lost.
#[allow(clippy::too_many_arguments)]
async fn run_soak_connection(
    id: usize,
    addr: Multiaddr,
    protocol: String,
    request_timeout: Duration,
    begins: Arc<Vec<[u8; 32]>>,
    lifetime: usize,
    deadline: TokioInstant,
    metrics: Arc<Metrics>,
) -> WarpStats {
    let mut stats = WarpStats::default();

    // The orchestrator counted this connection as concurrent at spawn time, so
    // every exit path has to give that back.
    let finish = |s: WarpStats, m: &Metrics| {
        m.concurrent.fetch_sub(1, Ordering::Relaxed);
        s
    };

    let mut swarm = match build_swarm(&protocol, request_timeout, 16).await {
        Ok(s) => s,
        Err(e) => {
            log::error!("conn {id}: build_swarm failed: {e}");
            return finish(stats, &metrics);
        }
    };
    if let Err(e) = swarm.dial(addr) {
        log::error!("conn {id}: dial failed: {e}");
        return finish(stats, &metrics);
    }

    let peer = {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        let mut found = None;
        loop {
            futures::select! {
                ev = swarm.select_next_some().fuse() => match ev {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        found = Some(peer_id);
                        break;
                    }
                    SwarmEvent::OutgoingConnectionError { error, .. } => {
                        log::warn!("conn {id}: connection error: {error}");
                        break;
                    }
                    _ => {}
                },
                _ = (&mut sleep).fuse() => break,
            }
        }
        match found {
            Some(p) => p,
            None => return finish(stats, &metrics),
        }
    };
    metrics.connected.fetch_add(1, Ordering::Relaxed);

    let mut issued = 0usize;
    while issued < lifetime && TokioInstant::now() < deadline {
        let idx = issued % begins.len();
        let payload = warp::encode_request(&begins[idx]);
        let _ = swarm.behaviour_mut().light.send_request(&peer, payload);
        issued += 1;
        stats.issued += 1;
        metrics.issued.fetch_add(1, Ordering::Relaxed);
        let t0 = Instant::now();

        match await_request(&mut swarm, deadline).await {
            Outcome::Responded(r) => record_response(&mut stats, &metrics, r, idx, t0),
            Outcome::Failed(f) => record_failure(&mut stats, &metrics, f),
            // Abandoned in flight — connection closed under us, or the run
            // ended. Recorded rather than dropped.
            Outcome::Aborted => {
                stats.aborted += 1;
                metrics.aborted.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    finish(stats, &metrics)
}

/// Drive the open-loop soak: keep a pool of at most `max_concurrent` connections
/// alive, each living for `lifetime` requests, until `duration` elapses.
#[allow(clippy::too_many_arguments)]
async fn run_soak(
    protocol: String,
    multiaddr: Multiaddr,
    begins: Arc<Vec<[u8; 32]>>,
    rate: u64,
    duration: Duration,
    lifetime: usize,
    max_concurrent: usize,
    request_timeout: Duration,
    mut sampler: Option<CsvSampler>,
) -> WarpStats {
    let metrics = Arc::new(Metrics::default());
    let start = Instant::now();
    let deadline = TokioInstant::now() + duration;

    // Arrivals carry the rate. Each connection performs `lifetime` requests, so
    // to offer `rate` requests/second we admit rate/lifetime connections per
    // second, accumulated fractionally on the 100ms manage tick so any rate is
    // exact. `max_concurrent` only caps the pool; it does not set the pace.
    let arrivals_per_tick = rate as f64 / lifetime as f64 * 0.1;
    // Seeded at 1 so the first connection is admitted on the first tick rather
    // than after a full arrival interval — otherwise a short run loses one whole
    // connection's worth of requests and reports under the target rate.
    let mut arrival_acc = 1f64;

    let mut tasks = tokio::task::JoinSet::new();
    let mut merged = WarpStats::default();
    let mut opened = 0usize;
    let mut last_drift = (Instant::now(), 0u64, 0u64, 0u64);

    let final_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(final_sleep);
    let mut manage = tokio::time::interval(Duration::from_millis(100));
    let mut progress = tokio::time::interval(Duration::from_secs(1));
    let mut drift = tokio::time::interval(Duration::from_secs(30));
    manage.tick().await;
    progress.tick().await;
    drift.tick().await;

    loop {
        tokio::select! {
            _ = &mut final_sleep => break,
            _ = manage.tick() => {
                // Admit however many arrivals have accrued, subject to the pool
                // cap. Each is a fresh identity — retiring connections are
                // replaced, never recycled, which is what gives the run churn.
                arrival_acc += arrivals_per_tick;
                while arrival_acc >= 1.0 {
                    if (metrics.concurrent.load(Ordering::Relaxed) as usize) >= max_concurrent {
                        // Pool is saturated: the node cannot keep up with the
                        // offered rate. Drop the backlog rather than banking it,
                        // so recovery cannot burst past the target.
                        arrival_acc = 0.0;
                        break;
                    }
                    arrival_acc -= 1.0;
                    metrics.bump_concurrent();
                    metrics.opened.fetch_add(1, Ordering::Relaxed);
                    tasks.spawn(run_soak_connection(
                        opened,
                        multiaddr.clone(),
                        protocol.clone(),
                        request_timeout,
                        begins.clone(),
                        lifetime,
                        deadline,
                        metrics.clone(),
                    ));
                    opened += 1;
                }
            }
            // One CSV row per second — the sampling must live here and not at
            // the bottom of the loop, which also spins on the 100ms manage tick
            // and on every connection retirement.
            _ = progress.tick() => {
                print_soak_progress(&metrics, start, lifetime);
                if let Some(s) = sampler.as_mut() {
                    s.sample(&metrics, start.elapsed().as_secs_f64());
                }
            }
            _ = drift.tick() => print_drift(&metrics, &mut last_drift),
            // Reap finished connections as they retire, so their stats are folded
            // in incrementally rather than all at the end.
            Some(joined) = tasks.join_next() => match joined {
                Ok(stats) => merged.merge(stats),
                Err(e) => log::error!("soak connection join error: {e}"),
            },
        }
    }

    // Drain whatever is still in flight at the deadline.
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(stats) => merged.merge(stats),
            Err(e) => log::error!("soak connection join error: {e}"),
        }
    }
    println!();

    if let Some(s) = sampler.as_mut() {
        s.sample(&metrics, start.elapsed().as_secs_f64());
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let header = format!(
        "mode:        soak (open loop) | offered rate {rate}/s for {:.0}s\n\
         conns:       {} opened, peak {} concurrent (cap {max_concurrent}) | {lifetime} req each\n\
         begins:      {} hash(es)\n",
        duration.as_secs_f64(),
        metrics.opened.load(Ordering::Relaxed),
        metrics.peak_concurrent.load(Ordering::Relaxed),
        begins.len(),
    );
    print_report(&merged, elapsed, &header, &protocol);
    println!(
        "offered:     {:.2}/s achieved vs {rate}/s target",
        merged.issued as f64 / elapsed
    );

    merged
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
    rate: Option<u64>,
    duration: Duration,
    max_concurrent: usize,
    out_dir: Option<PathBuf>,
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

    if begins.len() == 1 {
        println!(
            "             every request re-sends the same begin, so the node rebuilds \
             the same proof each time"
        );
    }

    let multiaddr: Multiaddr = address.parse()?;
    let begins = Arc::new(begins);

    let mut sampler = None;
    if let Some(dir) = &out_dir {
        fs::create_dir_all(dir)?;
        sampler = Some(CsvSampler::create(&dir.join("warp-samples.csv"))?);
    }

    // Soak mode: --rate switches from the closed-loop burst to an open-loop run
    // with connection churn, for questions a 7-second burst cannot answer —
    // memory retention, buffers left by clients that leave mid-transfer, and
    // whether sustained warp serving interferes with block import.
    if let Some(rate) = rate {
        let rate = rate.max(1);
        println!(
            "Soak:        rate={rate}/s duration={:.0}s | up to {max_concurrent} concurrent conns, \
             {requests} req each then reconnect (req timeout {request_timeout:?})",
            duration.as_secs_f64()
        );
        println!(
            "             ~{:.1} MiB total at 8 MiB/proof — check that before a long run",
            rate as f64 * duration.as_secs_f64() * 8.0
        );
        run_soak(
            protocol,
            multiaddr,
            begins,
            rate,
            duration,
            requests,
            max_concurrent.max(1),
            request_timeout,
            sampler,
        )
        .await;
        if let Some(dir) = &out_dir {
            println!("wrote:       {}", dir.join("warp-samples.csv").display());
        }
        return Ok(());
    }

    println!(
        "Load:        clients={clients} requests/client={requests} (1 in flight each) => total {} req (req timeout {request_timeout:?})",
        clients.saturating_mul(requests)
    );
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
            _ = progress.tick().fuse() => {
                print_progress(&metrics, total, start);
                if let Some(s) = sampler.as_mut() {
                    s.sample(&metrics, start.elapsed().as_secs_f64());
                }
            }
        }
    };
    println!();
    if let Some(s) = sampler.as_mut() {
        s.sample(&metrics, start.elapsed().as_secs_f64());
    }

    let mut merged = WarpStats::default();
    for r in results {
        match r {
            Ok(stats) => merged.merge(stats),
            Err(e) => log::error!("client task join error: {e}"),
        }
    }

    let header = format!(
        "mode:        burst (closed loop)\n\
         clients:     {clients} (connected {}) | requests/client {requests} | begin hashes {} | total {}\n",
        metrics.connected.load(Ordering::Relaxed),
        begins.len(),
        clients.saturating_mul(requests),
    );
    print_report(
        &merged,
        start.elapsed().as_secs_f64().max(0.001),
        &header,
        &protocol,
    );

    if let Some(dir) = &out_dir {
        println!("wrote:       {}", dir.join("warp-samples.csv").display());
    }

    Ok(())
}
