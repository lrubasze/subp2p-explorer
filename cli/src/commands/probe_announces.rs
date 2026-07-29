// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Measure block-announcement quality from a small, unloaded process.
//!
//! `hold-peers` reports spread and coverage too, but at high peer counts its own
//! scheduling delay contaminates the timings: thousands of holders share one
//! runtime, so a late arrival may be our fault rather than the node's. This
//! command is the fix. Run it *alongside* a `hold-peers` process — the big one
//! generates the load, this small one does the measuring, and because it lives in
//! its own process its timings are unaffected by that load.
//!
//! With only a handful of peers there is no useful fan-out spread to measure, so
//! the reference clock comes from elsewhere: the node's own RPC head stream. Both
//! timestamps are taken in this process, so no clock synchronisation is needed,
//! and the RPC path does not care how many peers the node has. If the gap between
//! "RPC announced block N" and "our peers were told about block N" grows as the
//! load process adds peers, that is announcement degradation attributable to the
//! node.
//!
//! A block the RPC reported but no probe peer was ever told about is a dropped
//! announcement. With a few peers this is a sensitive check, because the node
//! drops per peer and we only need one of ours to miss it.

use crate::commands::authorities::{client, fetch_genesis_hash};
use crate::commands::hold_peers::{run_peer, Arrival, HoldMetrics, HoldRole};
use crate::commands::light_common::{percentile_ms, Chain};
use jsonrpsee::{client_transport::ws::Url, core::client::SubscriptionClientT, rpc_params};
use libp2p::Multiaddr;
use primitive_types::H256;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use subp2p_explorer::notifications::behavior::ProtocolsData;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant as TokioInstant;

/// Ignore blocks seen in the last stretch of the window: an announcement still in
/// flight when we stop would otherwise look dropped.
const TAIL_GUARD: Duration = Duration::from_millis(500);

/// One block as the probe saw it, from both the RPC stream and the p2p peers.
#[derive(Default)]
struct BlockRow {
    /// When the node's RPC told us about this block.
    rpc_at: Option<Instant>,
    /// First and last probe peer to be told about it over p2p.
    first: Option<Instant>,
    last: Option<Instant>,
    /// Probe peers that received it.
    count: u64,
    /// Probe peers held when it first arrived.
    held: u64,
}

impl BlockRow {
    /// The earliest evidence this block exists, used to place it in the window.
    fn seen_at(&self) -> Option<Instant> {
        match (self.rpc_at, self.first) {
            (Some(rpc), Some(p2p)) => Some(rpc.min(p2p)),
            (Some(rpc), None) => Some(rpc),
            (None, p2p) => p2p,
        }
    }

    /// How far the p2p announcement trailed the RPC notification.
    fn lag(&self) -> Option<Duration> {
        let (rpc, first) = (self.rpc_at?, self.first?);
        first.checked_duration_since(rpc)
    }
}

/// Parse the `number` field of a JSON header (a hex string) into a `u32`.
fn header_number(header: &serde_json::Value) -> Option<u32> {
    let raw = header.get("number")?.as_str()?;
    u64::from_str_radix(raw.trim_start_matches("0x"), 16)
        .ok()
        .map(|number| number as u32)
}

/// Entry point for the `probe-announces` command.
#[allow(clippy::too_many_arguments)]
pub async fn probe_announces(
    chain: Option<Chain>,
    url: Option<String>,
    address: Option<String>,
    genesis: Option<String>,
    role: HoldRole,
    peers: usize,
    duration: Duration,
    idle_timeout: Duration,
    connect_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let peers = peers.max(1);

    let address = address
        .or_else(|| chain.map(|c| c.address().to_string()))
        .ok_or("provide --address or --chain")?;
    // The RPC stream is the reference clock, so unlike hold-peers this command
    // cannot run without one.
    let url = url
        .or_else(|| chain.map(|c| c.rpc_url().to_string()))
        .ok_or("provide --url or --chain: the RPC head stream is the reference clock")?;
    let rpc_url = Url::parse(&url)?;

    let genesis = match genesis {
        Some(genesis) => genesis.trim_start_matches("0x").to_string(),
        None => {
            println!("Fetching genesis from RPC...");
            fetch_genesis_hash(rpc_url.clone()).await?
        }
    };

    let data = ProtocolsData {
        genesis_hash: H256::from_slice(hex::decode(&genesis)?.as_slice()),
        node_role: role.protocol_role(),
    };

    println!("Address:    {address}");
    println!("URL:        {url}");
    println!("Protocol:   /{genesis}/block-announces/1");
    println!(
        "Role:       {:?} (handshake byte {})",
        role,
        role.protocol_role().encoded()
    );
    println!(
        "Probe:      {peers} peer(s) for {:.0}s",
        duration.as_secs_f64()
    );
    println!("Run this alongside a hold-peers process; this one only measures.\n");

    let multiaddr: Multiaddr = address.parse()?;
    let metrics = Arc::new(HoldMetrics::default());
    let (stop_tx, stop_rx) = watch::channel(false);
    let (arrivals_tx, mut arrivals_rx) = mpsc::unbounded_channel::<Arrival>();
    let (rpc_tx, mut rpc_rx) = mpsc::unbounded_channel::<(u32, Instant)>();

    // The reference clock: every new head the node reports over RPC, stamped on
    // arrival here.
    let rpc = client(rpc_url).await?;
    let rpc_task = tokio::spawn(async move {
        let mut sub = match rpc
            .subscribe::<serde_json::Value, _>(
                "chain_subscribeAllHeads",
                rpc_params![],
                "chain_unsubscribeAllHeads",
            )
            .await
        {
            Ok(sub) => sub,
            Err(e) => {
                log::error!("head subscription failed, no reference clock: {e}");
                return;
            }
        };
        while let Some(Ok(header)) = sub.next().await {
            let at = Instant::now();
            if let Some(number) = header_number(&header) {
                let _ = rpc_tx.send((number, at));
            }
        }
    });

    let mut handles = Vec::with_capacity(peers);
    for id in 1..=peers {
        metrics.dialed.fetch_add(1, Ordering::Relaxed);
        handles.push(tokio::spawn(run_peer(
            id,
            multiaddr.clone(),
            data.clone(),
            idle_timeout,
            stop_rx.clone(),
            arrivals_tx.clone(),
            metrics.clone(),
        )));
    }

    // Let the peers connect before the measurement window opens, so an early
    // block is not counted against peers that were not up yet.
    let resolve_deadline = TokioInstant::now() + connect_timeout;
    while metrics.connected.load(Ordering::Relaxed) + metrics.dial_failed.load(Ordering::Relaxed)
        < peers as u64
    {
        if TokioInstant::now() >= resolve_deadline {
            println!("  warning: not every probe peer connected within the grace period");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Drain anything that arrived while connecting, so it cannot land in the
    // window with a half-built peer set.
    while arrivals_rx.try_recv().is_ok() {}
    while rpc_rx.try_recv().is_ok() {}

    let held = metrics.held.load(Ordering::Relaxed);
    println!(
        "Probing with {held} held peer(s) for {:.0}s...\n",
        duration.as_secs_f64()
    );

    let start = Instant::now();
    let sleep = tokio::time::sleep_until(TokioInstant::now() + duration);
    tokio::pin!(sleep);
    let mut rows: BTreeMap<u32, BlockRow> = BTreeMap::new();

    loop {
        tokio::select! {
            _ = &mut sleep => break,
            Some(arrival) = arrivals_rx.recv() => {
                let Some(number) = arrival.number else { continue };
                let row = rows.entry(number).or_default();
                row.count += 1;
                if row.first.is_none_or(|first| arrival.at < first) {
                    row.first = Some(arrival.at);
                    row.held = arrival.held;
                }
                if row.last.is_none_or(|last| arrival.at > last) {
                    row.last = Some(arrival.at);
                }
            }
            Some((number, at)) = rpc_rx.recv() => {
                let row = rows.entry(number).or_default();
                if row.rpc_at.is_none_or(|seen| at < seen) {
                    row.rpc_at = Some(at);
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let _ = stop_tx.send(true);
    rpc_task.abort();
    for handle in handles {
        let _ = handle.await;
    }

    print_report(&rows, start, elapsed, held, &address);
    Ok(())
}

fn print_report(
    rows: &BTreeMap<u32, BlockRow>,
    start: Instant,
    elapsed: Duration,
    held: u64,
    address: &str,
) {
    let cutoff = start + elapsed.saturating_sub(TAIL_GUARD);
    let rows: Vec<(&u32, &BlockRow)> = rows
        .iter()
        .filter(|(_, row)| row.seen_at().is_some_and(|at| at <= cutoff))
        .collect();

    println!("\n=== probe-announces summary ===");
    println!("target:     {address}");
    println!(
        "probe:      {held} held peer(s) over {:.1}s",
        elapsed.as_secs_f64()
    );

    if rows.is_empty() {
        println!("blocks:     none observed");
        return;
    }

    let mut lags: Vec<u64> = rows
        .iter()
        .filter_map(|(_, row)| row.lag().map(|lag| lag.as_micros() as u64))
        .collect();
    lags.sort_unstable();

    let mut spreads: Vec<u64> = rows
        .iter()
        .filter_map(|(_, row)| {
            let (first, last) = (row.first?, row.last?);
            Some(last.duration_since(first).as_micros() as u64)
        })
        .collect();
    spreads.sort_unstable();

    // A block the RPC reported but no probe peer was told about.
    let missed: Vec<u32> = rows
        .iter()
        .filter(|(_, row)| row.rpc_at.is_some() && row.first.is_none())
        .map(|(number, _)| **number)
        .collect();
    // A block where some, but not all, probe peers were told.
    let partial = rows
        .iter()
        .filter(|(_, row)| row.count > 0 && row.held > 0 && row.count < row.held)
        .count();

    println!("blocks:     {} observed in the window", rows.len());
    if !lags.is_empty() {
        println!(
            "rpc->p2p ms: p50={:.1} p90={:.1} p99={:.1} max={:.1} (over {} block(s))",
            percentile_ms(&lags, 50),
            percentile_ms(&lags, 90),
            percentile_ms(&lags, 99),
            percentile_ms(&lags, 100),
            lags.len(),
        );
    }
    if !spreads.is_empty() {
        println!(
            "spread ms:  p50={:.1} max={:.1} (first to last probe peer)",
            percentile_ms(&spreads, 50),
            percentile_ms(&spreads, 100),
        );
    }
    println!(
        "dropped:    {} block(s) never announced to any probe peer{}",
        missed.len(),
        if missed.is_empty() {
            String::new()
        } else {
            format!(
                " ({})",
                missed
                    .iter()
                    .map(|number| format!("#{number}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        },
    );
    println!("partial:    {partial} block(s) reached some but not all probe peers");

    println!("per block:");
    for (number, row) in &rows {
        let lag = row
            .lag()
            .map(|lag| format!("{:.1}ms after RPC", lag.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| {
                if row.rpc_at.is_none() {
                    "not seen on RPC".to_string()
                } else {
                    "NEVER ANNOUNCED".to_string()
                }
            });
        println!("  #{number}: {}/{} peer(s) | {lag}", row.count, row.held);
    }
}
