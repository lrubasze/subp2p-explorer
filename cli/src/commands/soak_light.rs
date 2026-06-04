// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Sustained-rate / long-duration soak test for the `/light/2` protocol.
//!
//! Where `spam-light` is a closed-loop saturation sweep, `soak-light` is
//! **open-loop**: it offers a target rate for a fixed duration with realistic
//! client churn, to check that a node stays healthy under sustained (possibly
//! over-capacity) load.
//!
//! Model: `--clients` is the *total* number of connections opened over the run
//! (cumulative, not concurrent). Each connection has a fresh libp2p identity,
//! issues `lifetime = round(rate × duration / clients)` requests **one in flight
//! at a time**, then closes (no recycling — a brand-new connection replaces it).
//! Connections open on a schedule (`clients` over `duration`), so the aggregate
//! offered rate ≈ `--rate` and concurrent connections emerge as ≈ rate × latency,
//! capped by `--max-concurrent`. Shared building blocks come from
//! [`crate::commands::light_common`].

use crate::commands::authorities::fetch_genesis_hash;
use crate::commands::light_common::{
    build_swarm, classify_failure, head_source, merge_error_map, parse_method_spec, percentile_ms,
    print_dotns_derivation, Chain, Head, MethodKind, MethodStats, SpamBehaviour, SpamBehaviourEvent,
};
use futures::{future::join_all, FutureExt, StreamExt};
use jsonrpsee::client_transport::ws::Url;
use libp2p::{
    request_response::{self, Message as RrMessage, OutboundFailure, OutboundRequestId},
    swarm::SwarmEvent,
    Multiaddr, Swarm,
};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subp2p_explorer::light;
use tokio::sync::watch;
use tokio::time::Instant as TokioInstant;

/// Lock-free counters shared by all connection workers, read by the orchestrator
/// for the live progress line and drift snapshots.
#[derive(Default)]
struct SoakMetrics {
    issued: AtomicU64,
    ok: AtomicU64,
    /// `Err(())` — node closed the substream with no response (load-shedding).
    shed: AtomicU64,
    timeout: AtomicU64,
    /// Other outbound failures (dial/io/connection-closed).
    err: AtomicU64,
    concurrent: AtomicU64,
    opened: AtomicU64,
    peak_concurrent: AtomicU64,
}

impl SoakMetrics {
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

/// What one connection hands back for aggregation.
struct SoakReport {
    stats: Vec<MethodStats>,
    errors: HashMap<String, u64>,
    issued: usize,
    /// dial → connection-established latency, if it connected.
    setup_us: Option<u64>,
}

impl SoakReport {
    fn empty(n: usize) -> Self {
        Self {
            stats: (0..n).map(|_| MethodStats::default()).collect(),
            errors: HashMap::new(),
            issued: 0,
            setup_us: None,
        }
    }
}

/// Outcome of awaiting a single in-flight request.
enum Outcome {
    Responded(Result<Vec<u8>, ()>),
    Failed(OutboundFailure),
    /// Deadline reached or connection closed — stop this connection.
    Aborted,
}

/// Wait for the response/failure of exactly `req_id` (1 in-flight at a time), or
/// abort at the run deadline / on connection close.
async fn await_request(
    swarm: &mut Swarm<SpamBehaviour>,
    req_id: OutboundRequestId,
    deadline: TokioInstant,
) -> Outcome {
    let sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep);
    loop {
        futures::select! {
            ev = swarm.select_next_some().fuse() => match ev {
                SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                    request_response::Event::Message {
                        message: RrMessage::Response { request_id, response },
                        ..
                    },
                )) if request_id == req_id => return Outcome::Responded(response),
                SwarmEvent::Behaviour(SpamBehaviourEvent::Light(
                    request_response::Event::OutboundFailure { request_id, error, .. },
                )) if request_id == req_id => return Outcome::Failed(error),
                SwarmEvent::ConnectionClosed { .. } => return Outcome::Aborted,
                _ => {}
            },
            _ = (&mut sleep).fuse() => return Outcome::Aborted,
        }
    }
}

/// Run one connection: fresh identity, dial, then issue up to `lifetime` requests
/// one-in-flight back-to-back until the run `deadline`. Decrements
/// `metrics.concurrent` on exit (the orchestrator incremented it at spawn).
#[allow(clippy::too_many_arguments)]
async fn run_connection(
    id: usize,
    addr: Multiaddr,
    light_protocol: String,
    request_timeout: Duration,
    max_streams: usize,
    methods: Vec<(String, MethodKind)>,
    schedule: Vec<usize>,
    head: watch::Receiver<Head>,
    lifetime: usize,
    deadline: TokioInstant,
    pacer: Arc<tokio::sync::Semaphore>,
    metrics: Arc<SoakMetrics>,
) -> SoakReport {
    let n = methods.len();
    let mut report = SoakReport::empty(n);

    let finish = |r: SoakReport, m: &SoakMetrics| {
        m.concurrent.fetch_sub(1, Ordering::Relaxed);
        r
    };

    let mut swarm = match build_swarm(&light_protocol, request_timeout, max_streams).await {
        Ok(s) => s,
        Err(e) => {
            log::error!("conn {id}: build_swarm failed: {e}");
            return finish(report, &metrics);
        }
    };
    let t_dial = Instant::now();
    if let Err(e) = swarm.dial(addr) {
        log::error!("conn {id}: dial failed: {e}");
        return finish(report, &metrics);
    }

    // Wait for the connection (or bail at the deadline / on error).
    let peer = {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        let mut peer = None;
        loop {
            futures::select! {
                ev = swarm.select_next_some().fuse() => match ev {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => { peer = Some(peer_id); break; }
                    SwarmEvent::OutgoingConnectionError { error, .. } => {
                        log::warn!("conn {id}: connection error: {error}");
                        break;
                    }
                    _ => {}
                },
                _ = (&mut sleep).fuse() => break,
            }
        }
        match peer {
            Some(p) => p,
            None => return finish(report, &metrics),
        }
    };
    report.setup_us = Some(t_dial.elapsed().as_micros() as u64);
    log::info!("conn {id} connected to {peer}");

    while report.issued < lifetime && TokioInstant::now() < deadline {
        // Pace against the global rate: wait for a permit (or stop at the deadline).
        tokio::select! {
            permit = pacer.acquire() => match permit {
                Ok(p) => p.forget(),
                Err(_) => break,
            },
            _ = tokio::time::sleep_until(deadline) => break,
        }
        if TokioInstant::now() >= deadline {
            break;
        }
        let idx = schedule[report.issued % schedule.len()];
        let (hash, num) = head.borrow().clone();
        let payload = methods[idx].1.build(&hash, num);
        let req_id = swarm.behaviour_mut().light.send_request(&peer, payload);
        report.issued += 1;
        report.stats[idx].issued += 1;
        metrics.issued.fetch_add(1, Ordering::Relaxed);
        let t0 = Instant::now();

        let st = &mut report.stats[idx];
        match await_request(&mut swarm, req_id, deadline).await {
            Outcome::Responded(Ok(bytes)) => {
                st.ok += 1;
                metrics.ok.fetch_add(1, Ordering::Relaxed);
                st.latencies_us.push(t0.elapsed().as_micros() as u64);
                if let Ok(decoded) = light::decode_response(&bytes) {
                    if let Some(p) = decoded.proof_len() {
                        st.proof_bytes_total += p as u64;
                    }
                    if st.sample.is_none() {
                        st.sample = Some(format!("{decoded:?} ({} wire bytes)", bytes.len()));
                    }
                }
            }
            Outcome::Responded(Err(())) => {
                st.err += 1;
                metrics.shed.fetch_add(1, Ordering::Relaxed);
                *report
                    .errors
                    .entry("no-response: peer closed substream (no proof)".to_string())
                    .or_insert(0) += 1;
            }
            Outcome::Failed(f) => {
                if matches!(f, OutboundFailure::Timeout) {
                    st.timeout += 1;
                    metrics.timeout.fetch_add(1, Ordering::Relaxed);
                } else {
                    st.err += 1;
                    metrics.err.fetch_add(1, Ordering::Relaxed);
                }
                let reason = classify_failure(&f);
                st.last_err = Some(reason.clone());
                *report.errors.entry(reason).or_insert(0) += 1;
            }
            Outcome::Aborted => break,
        }
    }

    finish(report, &metrics)
}

fn print_progress(m: &SoakMetrics, start: Instant, opened: usize, clients: usize) {
    let issued = m.issued.load(Ordering::Relaxed);
    let ok = m.ok.load(Ordering::Relaxed);
    let shed = m.shed.load(Ordering::Relaxed);
    let timeout = m.timeout.load(Ordering::Relaxed);
    let err = m.err.load(Ordering::Relaxed);
    let concurrent = m.concurrent.load(Ordering::Relaxed);
    let peak = m.peak_concurrent.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    print!(
        "\r  t={:.0}s opened={opened}/{clients} concurrent={concurrent}(peak {peak}) | offered {:.0}/s served {:.0}/s shed {:.0}/s | ok={ok} shed={shed} timeout={timeout} err={err}   ",
        elapsed,
        issued as f64 / elapsed,
        ok as f64 / elapsed,
        shed as f64 / elapsed,
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Emit a periodic drift snapshot (rolling rates since the previous snapshot) so
/// long-run trends are visible. `last` is `(time, cum_ok, cum_shed)`.
fn print_drift(m: &SoakMetrics, last: &mut (Instant, u64, u64)) {
    let now = Instant::now();
    let ok = m.ok.load(Ordering::Relaxed);
    let shed = m.shed.load(Ordering::Relaxed);
    let concurrent = m.concurrent.load(Ordering::Relaxed);
    let dt = now.duration_since(last.0).as_secs_f64().max(0.001);
    println!(
        "\n  [drift t+{:.0}s] served {:.0}/s shed {:.0}/s | concurrent {concurrent} | cum ok {ok} shed {shed}",
        now.duration_since(last.0).as_secs_f64(),
        (ok - last.1) as f64 / dt,
        (shed - last.2) as f64 / dt,
    );
    *last = (now, ok, shed);
}

#[allow(clippy::too_many_arguments)]
fn print_report(
    methods: &[(String, MethodKind)],
    stats: &[MethodStats],
    errors: &HashMap<String, u64>,
    setups: &mut [u64],
    issued: usize,
    elapsed: f64,
    rate: u64,
    clients: usize,
    lifetime: usize,
    t_open: f64,
    opened: u64,
    peak: u64,
    light_protocol: &str,
) {
    let mut all: Vec<u64> = Vec::new();
    let (mut ok, mut timeout, mut proof) = (0u64, 0u64, 0u64);
    for s in stats {
        ok += s.ok;
        timeout += s.timeout;
        proof += s.proof_bytes_total;
        all.extend_from_slice(&s.latencies_us);
    }
    // shed is the no-response bucket; other_err is the rest of `err`.
    let shed = errors
        .get("no-response: peer closed substream (no proof)")
        .copied()
        .unwrap_or(0);
    let err_total: u64 = stats.iter().map(|s| s.err).sum();
    let other_err = err_total.saturating_sub(shed);
    all.sort_unstable();
    setups.sort_unstable();

    println!("\n=== /light/2 soak summary ===");
    println!("protocol:   {light_protocol}");
    println!(
        "load:       rate={rate}/s clients={clients} (lifetime={lifetime} req/conn, new conn every {t_open:.2}s)"
    );
    println!(
        "duration:   {elapsed:.1}s | clients opened {opened} | peak concurrent {peak}"
    );
    println!(
        "offered:    {:.0} req/s (issued {issued}) | served {:.0} req/s | shed {:.0} req/s",
        issued as f64 / elapsed,
        ok as f64 / elapsed,
        shed as f64 / elapsed,
    );
    println!("requests:   ok={ok} shed={shed} timeout={timeout} other_err={other_err}");
    println!(
        "send->resp ms: p50={:.1} p90={:.1} p99={:.1} | proof bytes total={proof}",
        percentile_ms(&all, 50),
        percentile_ms(&all, 90),
        percentile_ms(&all, 99),
    );
    println!(
        "conn setup ms: p50={:.1} p90={:.1} p99={:.1} (over {} connects)",
        percentile_ms(setups, 50),
        percentile_ms(setups, 90),
        percentile_ms(setups, 99),
        setups.len(),
    );
    if !errors.is_empty() {
        let mut rows: Vec<(&String, &u64)> = errors.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1));
        println!("errors (reason -> count):");
        for (reason, count) in rows {
            println!("  {count:>8}  {reason}");
        }
    }
    println!("per method:");
    for (i, (label, _)) in methods.iter().enumerate() {
        let s = &stats[i];
        let mut lat = s.latencies_us.clone();
        lat.sort_unstable();
        let avg_proof = if s.ok > 0 { s.proof_bytes_total / s.ok } else { 0 };
        println!(
            "  {label}: issued={} ok={} err={} timeout={} | p50={:.1}ms p99={:.1}ms | avg proof={}B{}",
            s.issued,
            s.ok,
            s.err,
            s.timeout,
            percentile_ms(&lat, 50),
            percentile_ms(&lat, 99),
            avg_proof,
            s.sample
                .as_ref()
                .map(|x| format!(" | sample={x}"))
                .unwrap_or_default(),
        );
    }
}

/// Entry point for the `soak-light` command.
#[allow(clippy::too_many_arguments)]
pub async fn soak_light(
    chain: Option<Chain>,
    url: Option<String>,
    address: Option<String>,
    genesis: Option<String>,
    block: Option<String>,
    protocol: Option<String>,
    method: Option<String>,
    rate: u64,
    duration: Duration,
    clients: usize,
    max_concurrent: usize,
    request_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let clients = clients.max(1);
    let rate = rate.max(1);

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

    let duration_secs = duration.as_secs_f64();
    let lifetime = ((rate as f64 * duration_secs) / clients as f64).round().max(1.0) as usize;
    let t_open = duration_secs / clients as f64;

    println!("URL:         {url}");
    println!("Address:     {address}");
    println!("Protocol:    {light_protocol}");
    println!(
        "Methods:     {}",
        methods
            .iter()
            .map(|(l, _)| l.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "Soak:        rate={rate}/s duration={:.0}s clients={clients} => lifetime={lifetime} req/conn, new conn every {t_open:.2}s (max concurrent {max_concurrent})",
        duration_secs,
    );

    print_dotns_derivation(&methods);

    println!("Resolving execution block...");
    let head = head_source(rpc_url, block).await?;
    {
        let (hash, num) = head.borrow().clone();
        println!("Block:       #{num} 0x{}", hex::encode(&hash));
    }

    let multiaddr: Multiaddr = address.parse()?;
    // 1 in-flight per connection — a small per-connection stream cap is plenty.
    let max_streams = 8usize;

    let metrics = Arc::new(SoakMetrics::default());
    let start = Instant::now();
    let deadline = TokioInstant::now() + duration;

    // Token-bucket pacer: releases permits at `rate`/s into a bounded pool, so
    // the aggregate offered rate holds at `--rate` once the connection pool is
    // warm. The bucket is capped (~50 ms of burst) so a stalled pool can't
    // accumulate an unbounded backlog.
    let pacer = Arc::new(tokio::sync::Semaphore::new(0));
    // Permits accrue at exactly `rate`/s via a fractional accumulator (so any
    // rate is exact regardless of tick granularity); outstanding permits are
    // capped at ~50 ms of rate so a stalled pool can't build an unbounded
    // backlog (and can't catch-up-burst past the target afterwards).
    let cap = ((rate / 20) as usize).max(1);
    let pacer_task = {
        let pacer = pacer.clone();
        tokio::spawn(async move {
            let tick_secs = 0.005;
            let per_tick = rate as f64 * tick_secs;
            let mut acc = 0f64;
            let mut tick = tokio::time::interval(Duration::from_millis(5));
            loop {
                tick.tick().await;
                if TokioInstant::now() >= deadline {
                    break;
                }
                acc = (acc + per_tick).min(cap as f64);
                let room = cap.saturating_sub(pacer.available_permits());
                let add = (acc.floor() as usize).min(room);
                if add > 0 {
                    pacer.add_permits(add);
                    acc -= add as f64;
                }
            }
        })
    };

    println!("Starting soak...\n");
    let mut handles = Vec::new();
    let mut opened = 0usize;
    let mut last_drift = (Instant::now(), 0u64, 0u64);

    // Spawn at most this many connections per 100 ms manage tick, so the pool
    // ramps to the needed concurrency (~rate × latency) in ~1-2 s without
    // over-shooting much past it.
    const SPAWN_BATCH: usize = 32;

    let final_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(final_sleep);
    let mut manage_tick = tokio::time::interval(Duration::from_millis(100));
    let mut progress_tick = tokio::time::interval(Duration::from_secs(1));
    let mut drift_tick = tokio::time::interval(Duration::from_secs(30));
    manage_tick.tick().await;
    progress_tick.tick().await;
    drift_tick.tick().await;

    loop {
        futures::select! {
            _ = (&mut final_sleep).fuse() => break,
            _ = manage_tick.tick().fuse() => {
                // Grow the pool on demand: while the pacer has unconsumed permits
                // (the live workers can't keep up) and we're under both caps,
                // open a fresh connection. This handles initial ramp-up and
                // recycling (a retired connection frees demand) uniformly.
                let mut spawned = 0;
                while spawned < SPAWN_BATCH
                    && opened < clients
                    && metrics.concurrent.load(Ordering::Relaxed) < max_concurrent as u64
                    && pacer.available_permits() > 0
                {
                    metrics.bump_concurrent();
                    metrics.opened.fetch_add(1, Ordering::Relaxed);
                    opened += 1;
                    spawned += 1;
                    handles.push(tokio::spawn(run_connection(
                        opened,
                        multiaddr.clone(),
                        light_protocol.clone(),
                        request_timeout,
                        max_streams,
                        methods.clone(),
                        schedule.clone(),
                        head.clone(),
                        lifetime,
                        deadline,
                        pacer.clone(),
                        metrics.clone(),
                    )));
                }
            }
            _ = progress_tick.tick().fuse() => print_progress(&metrics, start, opened, clients),
            _ = drift_tick.tick().fuse() => print_drift(&metrics, &mut last_drift),
        }
    }
    println!("\nDuration reached, draining {} connection(s)...", handles.len());

    pacer_task.abort();
    let results = join_all(handles).await;

    // Aggregate.
    let n = methods.len();
    let mut merged: Vec<MethodStats> = (0..n).map(|_| MethodStats::default()).collect();
    let mut errors: HashMap<String, u64> = HashMap::new();
    let mut setups: Vec<u64> = Vec::new();
    let mut issued = 0usize;
    for r in results.into_iter().flatten() {
        issued += r.issued;
        merge_error_map(&mut errors, r.errors);
        if let Some(us) = r.setup_us {
            setups.push(us);
        }
        for (i, s) in r.stats.into_iter().enumerate() {
            let m = &mut merged[i];
            m.issued += s.issued;
            m.ok += s.ok;
            m.err += s.err;
            m.timeout += s.timeout;
            m.proof_bytes_total += s.proof_bytes_total;
            m.latencies_us.extend(s.latencies_us);
            if m.sample.is_none() {
                m.sample = s.sample;
            }
        }
    }

    print_report(
        &methods,
        &merged,
        &errors,
        &mut setups,
        issued,
        start.elapsed().as_secs_f64().max(0.001),
        rate,
        clients,
        lifetime,
        t_open,
        metrics.opened.load(Ordering::Relaxed),
        metrics.peak_concurrent.load(Ordering::Relaxed),
        &light_protocol,
    );

    Ok(())
}
