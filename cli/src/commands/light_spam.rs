// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Spam the substrate light-client request-response protocol
//! (`/<genesis>/light/2`) of an *appointed* full node, to load-test its
//! server-side light-request handler (see `smoldot-info.txt`: a global queue of
//! 20, single-threaded, no per-peer fairness).
//!
//! Unlike the smoldot-based spammer, this never syncs: it dials one host
//! directly, learns its peer id during the noise handshake, and issues
//! `RemoteCallRequest`s over `/light/2`.
//!
//! Scaling model (closed-loop saturation sweep): each connection is a worker
//! with a bounded in-flight window (`--concurrency`) refilled on completion up to
//! a total budget (`--count`); `--connections` independent clients (distinct
//! PeerIds) run in parallel and are aggregated. Pushing the window past the
//! server's ~20-deep queue reveals the latency knee + load-shedding. The shared,
//! command-agnostic building blocks live in [`crate::commands::light_common`].

use crate::commands::authorities::fetch_genesis_hash;
use crate::commands::light_common::{
    build_swarm, classify_failure, head_source, merge_error_map, parse_method_spec, percentile_ms,
    print_dotns_derivation, record_light_response, record_light_shed, Chain, Head, MethodKind,
    MethodStats, SpamBehaviour, SpamBehaviourEvent,
};
use futures::{future::join_all, FutureExt, StreamExt};
use jsonrpsee::client_transport::ws::Url;
use libp2p::{
    identify,
    request_response::{self, Message as RrMessage, OutboundFailure, OutboundRequestId},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use rand::RngCore;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subp2p_explorer::light;
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// The spammer
// ---------------------------------------------------------------------------

/// Lock-free counters shared by all connection workers, read by the orchestrator
/// for the single aggregate progress line.
#[derive(Default)]
struct Metrics {
    issued: AtomicU64,
    ok: AtomicU64,
    /// Response arrived with no proof — the node could not serve it.
    unserved: AtomicU64,
    /// Issued but abandoned: still in flight when the run deadline fired.
    aborted: AtomicU64,
    /// Node closed the substream with no proof — the load-shedding signal.
    shed: AtomicU64,
    err: AtomicU64,
    timeout: AtomicU64,
    in_flight: AtomicU64,
    connected: AtomicU64,
}

/// What a finished worker hands back for aggregation.
struct WorkerReport {
    stats: Vec<MethodStats>,
    errors: HashMap<String, u64>,
    issued: usize,
}

impl WorkerReport {
    fn empty(n_methods: usize) -> Self {
        Self {
            stats: (0..n_methods).map(|_| MethodStats::default()).collect(),
            errors: HashMap::new(),
            issued: 0,
        }
    }
}

/// One connection worker: a swarm with its own identity (=> distinct PeerId),
/// dialing the target host and issuing `/light/2` requests at `concurrency`
/// depth up to `count`.
struct LightSpammer {
    id: usize,
    swarm: Swarm<SpamBehaviour>,
    peer: Option<PeerId>,
    head: watch::Receiver<Head>,
    methods: Vec<(String, MethodKind)>,
    schedule: Vec<usize>,
    stats: Vec<MethodStats>,
    /// Aggregate failure breakdown: human-readable reason -> count.
    errors: HashMap<String, u64>,
    pending: HashMap<OutboundRequestId, (usize, Instant)>,
    issued: usize,
    /// Where this worker starts in the round-robin schedule. Random per worker:
    /// every worker starting at slot 0 means a method whose first slot lies past
    /// `count` is never issued, and it marches all workers through the schedule in
    /// lockstep. See the note in `soak_light::run_connection`.
    sched_offset: usize,
    in_flight: usize,
    count: usize,
    concurrency: usize,
    light_protocol: String,
    metrics: Arc<Metrics>,
}

impl LightSpammer {
    fn done(&self) -> bool {
        self.issued >= self.count && self.in_flight == 0
    }

    /// Top up the in-flight window up to `concurrency`, until the total budget
    /// `count` is reached. No-op until a peer connection exists.
    fn fill_window(&mut self) {
        let Some(peer) = self.peer else { return };
        while self.in_flight < self.concurrency && self.issued < self.count {
            let idx = self.schedule[(self.sched_offset + self.issued) % self.schedule.len()];
            let (hash, num) = self.head.borrow().clone();
            let payload = self.methods[idx].1.build(&hash, num);
            let id = self
                .swarm
                .behaviour_mut()
                .light
                .send_request(&peer, payload);
            self.pending.insert(id, (idx, Instant::now()));
            self.stats[idx].issued += 1;
            self.issued += 1;
            self.in_flight += 1;
            self.metrics.issued.fetch_add(1, Ordering::Relaxed);
            self.metrics.in_flight.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_response(&mut self, id: OutboundRequestId, response: Result<Vec<u8>, ()>) {
        let Some((idx, t0)) = self.pending.remove(&id) else {
            return;
        };
        self.in_flight -= 1;
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
        let st = &mut self.stats[idx];
        match response {
            Ok(bytes) => {
                let before = st.ok;
                if let Some(reason) =
                    record_light_response(st, &bytes, t0.elapsed().as_micros() as u64)
                {
                    st.last_err = Some(reason.clone());
                    *self.errors.entry(reason).or_insert(0) += 1;
                }
                if st.ok > before {
                    self.metrics.ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.metrics.unserved.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Substream closed without a byte: the node refused or dropped it.
            Err(()) => {
                record_light_shed(st, &mut self.errors);
                self.metrics.shed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn on_failure(&mut self, id: OutboundRequestId, error: OutboundFailure) {
        let Some((idx, _t0)) = self.pending.remove(&id) else {
            return;
        };
        self.in_flight -= 1;
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
        let st = &mut self.stats[idx];
        if matches!(error, OutboundFailure::Timeout) {
            st.timeout += 1;
            self.metrics.timeout.fetch_add(1, Ordering::Relaxed);
        } else {
            st.err += 1;
            self.metrics.err.fetch_add(1, Ordering::Relaxed);
        }
        let reason = classify_failure(&error);
        st.last_err = Some(reason.clone());
        *self.errors.entry(reason).or_insert(0) += 1;
    }

    fn handle_event(&mut self, event: SwarmEvent<SpamBehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                if self.peer.is_none() {
                    self.peer = Some(peer_id);
                    self.metrics.connected.fetch_add(1, Ordering::Relaxed);
                    log::info!("worker {} connected to {peer_id}", self.id);
                    self.fill_window();
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                let light_protos: Vec<&str> = info
                    .protocols
                    .iter()
                    .map(|p| p.as_ref())
                    .filter(|p| p.contains("/light/"))
                    .collect();
                log::info!(
                    "worker {} identified {peer_id} agent={:?} light_protocols={light_protos:?}",
                    self.id,
                    info.agent_version
                );
                if self.id == 0
                    && !info
                        .protocols
                        .iter()
                        .any(|p| p.as_ref() == self.light_protocol)
                {
                    println!(
                        "WARNING: peer does not advertise {} (fork_id? pass --protocol)",
                        self.light_protocol
                    );
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                request_response::Event::Message { message, .. },
            )) => {
                if let RrMessage::Response {
                    request_id,
                    response,
                } = message
                {
                    self.on_response(request_id, response);
                    self.fill_window();
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                self.on_failure(request_id, error);
                self.fill_window();
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                log::warn!(
                    "worker {} connection error (peer={peer_id:?}): {error}",
                    self.id
                );
            }
            other => log::trace!("swarm event: {other:?}"),
        }
    }

    /// Drive this worker until its `count` is reached or the shared `deadline`
    /// fires (so partial stats survive an overall timeout).
    async fn run(&mut self, deadline: tokio::time::Instant) {
        self.fill_window();
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
        self.abort_pending();
    }

    /// Anything still in flight when the deadline fires never gets an outcome.
    /// Book it as aborted so the buckets sum to `issued`.
    fn abort_pending(&mut self) {
        for (idx, _) in std::mem::take(&mut self.pending).into_values() {
            self.stats[idx].aborted += 1;
            self.metrics.aborted.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn into_report(self) -> WorkerReport {
        WorkerReport {
            stats: self.stats,
            errors: self.errors,
            issued: self.issued,
        }
    }
}

/// Merge per-connection worker reports into aggregate per-method stats, a global
/// error breakdown, and the total issued count. All workers share the method
/// spec, so `stats[i]` aligns by index.
fn merge_reports(
    reports: Vec<WorkerReport>,
    n_methods: usize,
) -> (Vec<MethodStats>, HashMap<String, u64>, usize) {
    let mut merged: Vec<MethodStats> = (0..n_methods).map(|_| MethodStats::default()).collect();
    let mut errors: HashMap<String, u64> = HashMap::new();
    let mut issued = 0usize;
    for r in reports {
        issued += r.issued;
        merge_error_map(&mut errors, r.errors);
        for (i, s) in r.stats.into_iter().enumerate() {
            let m = &mut merged[i];
            m.issued += s.issued;
            m.ok += s.ok;
            m.unserved += s.unserved;
            m.aborted += s.aborted;
            m.shed += s.shed;
            m.err += s.err;
            m.timeout += s.timeout;
            m.proof_bytes_total += s.proof_bytes_total;
            m.latencies_us.extend(s.latencies_us);
            m.unserved_us.extend(s.unserved_us);
            if m.sample.is_none() {
                m.sample = s.sample;
            }
            if m.last_err.is_none() {
                m.last_err = s.last_err;
            }
        }
    }
    (merged, errors, issued)
}

/// Print the single aggregate progress line from the shared metrics.
fn print_progress(m: &Metrics, total: usize, start: Instant) {
    let issued = m.issued.load(Ordering::Relaxed);
    let ok = m.ok.load(Ordering::Relaxed);
    let unserved = m.unserved.load(Ordering::Relaxed);
    let shed = m.shed.load(Ordering::Relaxed);
    let err = m.err.load(Ordering::Relaxed);
    let timeout = m.timeout.load(Ordering::Relaxed);
    let in_flight = m.in_flight.load(Ordering::Relaxed);
    let connected = m.connected.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    print!(
        "\r  issued={issued}/{total} conns={connected} in_flight={in_flight} ok={ok} unserved={unserved} shed={shed} timeout={timeout} err={err} | {:.0} req/s | {:.0}s   ",
        ok as f64 / elapsed,
        elapsed
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Print the final aggregate report.
#[allow(clippy::too_many_arguments)]
fn print_report(
    methods: &[(String, MethodKind)],
    stats: &[MethodStats],
    errors: &HashMap<String, u64>,
    issued: usize,
    elapsed: f64,
    connections: usize,
    count: usize,
    connected: u64,
    light_protocol: &str,
) {
    let mut all: Vec<u64> = Vec::new();
    let (mut ok, mut unserved, mut shed, mut aborted, mut err, mut timeout, mut proof) =
        (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    for s in stats {
        ok += s.ok;
        unserved += s.unserved;
        aborted += s.aborted;
        shed += s.shed;
        err += s.err;
        timeout += s.timeout;
        proof += s.proof_bytes_total;
        all.extend_from_slice(&s.latencies_us);
    }
    all.sort_unstable();

    println!("\n=== /light/2 spam summary ===");
    println!("protocol:    {light_protocol}");
    println!(
        "connections: {connections} (connected {connected}) | count/conn {count} | total {}",
        connections.saturating_mul(count)
    );
    println!(
        "issued={issued} ok={ok} unserved={unserved} shed={shed} timeout={timeout} err={err} aborted={aborted} in {elapsed:.1}s => {:.0} ok req/s",
        ok as f64 / elapsed
    );
    let accounted = ok + unserved + shed + timeout + err + aborted;
    if accounted != issued as u64 {
        println!(
            "  WARNING: {accounted} outcomes for {issued} issued — accounting gap of {}",
            (issued as u64).abs_diff(accounted)
        );
    }
    if unserved > 0 {
        println!("  note: {unserved} response(s) carried no proof: the node answered but could");
        println!("        not serve the request — runtime method absent on this chain, or a");
        println!("        pruned block. Not successes; they also skip execution, so their");
        println!("        latency is listed separately per method rather than mixed in.");
    }
    println!(
        "latency ms: p50={:.1} p90={:.1} p99={:.1} | proof bytes total={}",
        percentile_ms(&all, 50),
        percentile_ms(&all, 90),
        percentile_ms(&all, 99),
        proof
    );
    if !errors.is_empty() {
        let mut rows: Vec<(&String, &u64)> = errors.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        println!("errors (reason -> count):");
        for (reason, count) in rows {
            println!("  {count:>6}  {reason}");
        }
    }
    println!("per method:");
    for (i, (label, _)) in methods.iter().enumerate() {
        let s = &stats[i];
        let mut lat = s.latencies_us.clone();
        lat.sort_unstable();
        let avg_proof = if s.ok > 0 {
            s.proof_bytes_total / s.ok
        } else {
            0
        };
        let mut unserved_lat = s.unserved_us.clone();
        unserved_lat.sort_unstable();
        let unserved_note = if s.unserved > 0 {
            format!(
                " unserved={} (p50={:.1}ms)",
                s.unserved,
                percentile_ms(&unserved_lat, 50)
            )
        } else {
            String::new()
        };
        // With nothing served there is no served latency; "0.0ms" would read as
        // "instant" rather than "no data".
        let served_lat = if s.ok > 0 {
            format!(
                "p50={:.1}ms p99={:.1}ms",
                percentile_ms(&lat, 50),
                percentile_ms(&lat, 99)
            )
        } else {
            "p50=n/a p99=n/a".to_string()
        };
        println!(
            "  {label}: issued={} ok={}{unserved_note} shed={} timeout={} err={} | {served_lat} | avg proof={}B{}{}",
            s.issued,
            s.ok,
            s.shed,
            s.timeout,
            s.err,
            avg_proof,
            s.sample
                .as_ref()
                .map(|x| format!(" | sample={x}"))
                .unwrap_or_default(),
            s.last_err
                .as_ref()
                .map(|x| format!(" | last_err={x}"))
                .unwrap_or_default(),
        );
    }
}

/// Entry point for the `spam-light` command.
#[allow(clippy::too_many_arguments)]
pub async fn spam_light(
    chain: Option<Chain>,
    url: Option<String>,
    address: Option<String>,
    genesis: Option<String>,
    block: Option<String>,
    protocol: Option<String>,
    method: Option<String>,
    count: usize,
    concurrency: usize,
    connections: usize,
    stagger_ms: u64,
    request_timeout: Duration,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let connections = connections.max(1);

    // Resolve url / address / method from the preset when not given explicitly.
    let url = url
        .or_else(|| chain.map(|c| c.rpc_url().to_string()))
        .ok_or("provide --url or --chain")?;
    let address = address
        .or_else(|| chain.map(|c| c.address().to_string()))
        .ok_or("provide --address or --chain")?;
    let method_spec = method
        .or_else(|| chain.map(|c| c.default_methods().to_string()))
        .unwrap_or_else(|| "account_nonce".to_string());

    let rpc_url = Url::parse(&url)?;
    let (methods, schedule) = parse_method_spec(&method_spec)?;

    let genesis = match genesis {
        Some(g) => g.trim_start_matches("0x").to_string(),
        None => {
            println!("Fetching genesis from RPC...");
            fetch_genesis_hash(rpc_url.clone()).await?
        }
    };
    let light_protocol = protocol.unwrap_or_else(|| light::protocol_name(&genesis));

    println!("URL:         {url}");
    println!("Address:     {address}");
    println!("Protocol:    {light_protocol}");
    println!(
        "Methods:     {} (schedule len {})",
        methods
            .iter()
            .map(|(l, _)| l.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        schedule.len()
    );
    println!(
        "Load:        connections={connections} count/conn={count} concurrency/conn={concurrency} => total {} req, up to {} in flight (req timeout {request_timeout:?})",
        connections.saturating_mul(count),
        connections.saturating_mul(concurrency),
    );

    print_dotns_derivation(&methods);

    println!("Resolving execution block...");
    let head = head_source(rpc_url, block).await?;
    {
        let (hash, num) = head.borrow().clone();
        println!("Block:       #{num} 0x{}", hex::encode(&hash));
    }

    let multiaddr: Multiaddr = address.parse()?;
    // Raise the request-response per-connection stream cap above the spam
    // window so the client never self-throttles with "max sub-streams reached"
    // (libp2p's default is 100); the server stays the only limiter.
    let max_streams = concurrency.saturating_mul(2).max(256);

    let metrics = Arc::new(Metrics::default());
    let deadline = tokio::time::Instant::now() + timeout;
    let start = Instant::now();

    println!("Spawning {connections} connection(s)...\n");
    let mut handles = Vec::with_capacity(connections);
    for id in 0..connections {
        let proto = light_protocol.clone();
        let methods_c = methods.clone();
        let schedule_c = schedule.clone();
        let head_c = head.clone();
        let metrics_c = metrics.clone();
        let addr_c = multiaddr.clone();
        let n = methods_c.len();
        handles.push(tokio::spawn(async move {
            let mut swarm = match build_swarm(&proto, request_timeout, max_streams).await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("worker {id}: build_swarm failed: {e}");
                    return WorkerReport::empty(n);
                }
            };
            if let Err(e) = swarm.dial(addr_c) {
                log::error!("worker {id}: dial failed: {e}");
                return WorkerReport::empty(n);
            }
            let sched_offset = rand::thread_rng().next_u32() as usize % schedule_c.len();
            let mut worker = LightSpammer {
                id,
                swarm,
                peer: None,
                head: head_c,
                stats: (0..n).map(|_| MethodStats::default()).collect(),
                errors: HashMap::new(),
                methods: methods_c,
                schedule: schedule_c,
                pending: HashMap::new(),
                issued: 0,
                sched_offset,
                in_flight: 0,
                count,
                concurrency,
                light_protocol: proto,
                metrics: metrics_c,
            };
            worker.run(deadline).await;
            worker.into_report()
        }));
        if stagger_ms > 0 && id + 1 < connections {
            tokio::time::sleep(Duration::from_millis(stagger_ms)).await;
        }
    }

    // Drive the single aggregate progress line until every worker finishes.
    let joined = join_all(handles);
    tokio::pin!(joined);
    let mut progress = tokio::time::interval(Duration::from_secs(1));
    progress.tick().await;
    let total = connections.saturating_mul(count);
    let results = loop {
        futures::select! {
            r = (&mut joined).fuse() => break r,
            _ = progress.tick().fuse() => print_progress(&metrics, total, start),
        }
    };
    println!();

    let reports: Vec<WorkerReport> = results
        .into_iter()
        .filter_map(|r| match r {
            Ok(report) => Some(report),
            Err(e) => {
                log::error!("worker task join error: {e}");
                None
            }
        })
        .collect();

    let (merged, errors, issued) = merge_reports(reports, methods.len());
    let connected = metrics.connected.load(Ordering::Relaxed);
    print_report(
        &methods,
        &merged,
        &errors,
        issued,
        start.elapsed().as_secs_f64().max(0.001),
        connections,
        count,
        connected,
        &light_protocol,
    );

    Ok(())
}
