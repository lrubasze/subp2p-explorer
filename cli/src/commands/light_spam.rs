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
//! Scaling model: one swarm / one event loop, a bounded in-flight window
//! (`--concurrency`) refilled on completion up to a total budget (`--count`).
//! Because the server executes light requests on a global queue of ~20, pushing
//! the window past that reveals the latency knee + timeouts. A background RPC
//! subscription to the finalized head keeps the execution block fresh so the
//! node keeps executing (a pruned block → cheap `None` rejection, no load).

use crate::commands::authorities::{client, fetch_genesis_hash};
use futures::{FutureExt, StreamExt};
use jsonrpsee::{
    client_transport::ws::Url,
    core::client::{ClientT, SubscriptionClientT},
    rpc_params,
};
use libp2p::{
    identify, identity,
    request_response::{self, Message as RrMessage, OutboundFailure, OutboundRequestId},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use rand::RngCore;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};
use subp2p_explorer::{
    light::{self, LightCodec},
    peer_behavior::AGENT,
};
use tokio::sync::watch;

/// Minimal swarm for the light spammer: identify (to learn the peer id and its
/// advertised protocols) plus the outbound `/light/2` request-response client.
#[derive(libp2p::swarm::NetworkBehaviour)]
struct SpamBehaviour {
    identify: identify::Behaviour,
    light: request_response::Behaviour<LightCodec>,
}

// ---------------------------------------------------------------------------
// Chain presets
// ---------------------------------------------------------------------------

/// Built-in Paseo Next chain presets (RPC url, default p2p host, default method
/// mix). Everything is overridable on the command line.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Chain {
    /// Paseo Next Asset Hub (revive / `ReviveApi_get_storage`).
    PaseoNextAssetHub,
    /// Paseo Next Bulletin (transaction storage).
    PaseoNextBulletin,
}

impl Chain {
    fn rpc_url(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => "wss://paseo-asset-hub-next-rpc.polkadot.io",
            Chain::PaseoNextBulletin => "wss://paseo-bulletin-next-rpc.polkadot.io",
        }
    }

    /// A default p2p host to dial (a guess for convenience; override with
    /// `--address`). wss on :443 behind the ingress.
    fn address(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => {
                "/dns4/paseo-asset-hub-next-collator-node-0.parity-testnet.parity.io/tcp/443/wss"
            }
            Chain::PaseoNextBulletin => {
                "/dns4/paseo-bulletin-next-rpc-node-0.polkadot.io/tcp/443/wss"
            }
        }
    }

    fn default_methods(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => "revive_get_storage,account_nonce",
            Chain::PaseoNextBulletin => {
                "account_nonce,can_store,account_authorization,indexed_transactions"
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Methods
// ---------------------------------------------------------------------------

/// A dotNS contract on Paseo Next Asset Hub (the `DOTNS_REGISTRY` from the revive
/// storage mental-model doc). Used as the default address for
/// `revive_get_storage`; the 32-byte key is randomised per request (cache-bust).
const REVIVE_DEFAULT_ADDRESS: &str = "a1b2b939E82b2ecE55Bd8a0E283818BfC1CA6CDc";

/// Ethereum Keccak-256 (NOT SHA3-256).
fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// EIP-137 namehash of a dotted domain, e.g. `playground.dot`.
///
/// `node("") = 0x00…00`; `node(label.parent) = keccak256(node(parent) ++ keccak256(label))`,
/// processing labels right-to-left (TLD first).
fn namehash(domain: &str) -> [u8; 32] {
    let mut node = [0u8; 32];
    let labels: Vec<&str> = domain.split('.').filter(|l| !l.is_empty()).collect();
    for label in labels.into_iter().rev() {
        let label_hash = keccak256(label.as_bytes());
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(&node);
        buf[32..].copy_from_slice(&label_hash);
        node = keccak256(&buf);
    }
    node
}

/// Solidity storage slot for `mapping(bytes32 => …)` at declaration `slot`:
/// `keccak256(key ++ uint256(slot))` (`abi.ts:60`).
fn mapping_slot_key(key: &[u8; 32], slot: u64) -> [u8; 32] {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(key);
    // uint256 big-endian: the slot number lives in the low 8 bytes.
    buf[56..64].copy_from_slice(&slot.to_be_bytes());
    keccak256(&buf)
}

/// A precomputed dotNS read: the full derivation `domain → namehash → slotKey`,
/// and the assembled `get_storage` argument (`address ++ slotKey`).
#[derive(Debug, Clone)]
struct DotnsEntry {
    domain: String,
    namehash: [u8; 32],
    slot: u64,
    slot_key: [u8; 32],
    address: [u8; 20],
    /// `address (20) ++ slot_key (32)` — the `ReviveApi_get_storage` data argument.
    data: Vec<u8>,
}

/// How a single request is built. Most are runtime-API calls (`RemoteCallRequest`);
/// `read` is a storage Merkle proof (`RemoteReadRequest`).
#[derive(Debug, Clone)]
enum MethodKind {
    /// `AccountNonceApi_account_nonce(AccountId32)` — random account.
    AccountNonce,
    /// `BulletinTransactionStorageApi_can_store(AccountId32, u32 len)` — Bulletin.
    CanStore,
    /// `BulletinTransactionStorageApi_account_authorization(AccountId32)` — Bulletin.
    AccountAuthorization,
    /// `TransactionStorageApi_indexed_transactions(u32 block)` — Bulletin. Uses
    /// the current finalized head number (usually empty → light proof).
    IndexedTransactions,
    /// `ReviveApi_get_storage(H160, [u8;32])` — Asset Hub. Fixed contract
    /// address, random 32-byte key.
    ReviveGetStorage { address: [u8; 20] },
    /// `ReviveApi_get_storage` against `DOTNS_REGISTRY` reading the real
    /// `REGISTRY_RECORDS` (owner) slot of one of the given dotNS domains.
    ReviveDotns { entries: Vec<DotnsEntry> },
    /// Arbitrary `RemoteCallRequest` with a fixed method + hex-encoded data.
    GenericCall { method: String, data: Vec<u8> },
    /// Arbitrary `RemoteReadRequest` for one or more hex-encoded storage keys.
    GenericRead { keys: Vec<Vec<u8>> },
}

fn random_account() -> Vec<u8> {
    let mut a = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut a);
    a.to_vec()
}

impl MethodKind {
    /// Build the prost-encoded `/light/2` request for this method, executing at
    /// block `head_hash` (raw 32 bytes), with `head_num` available as the
    /// `indexed_transactions` argument.
    fn build(&self, head_hash: &[u8], head_num: u32) -> Vec<u8> {
        let block = head_hash.to_vec();
        match self {
            MethodKind::AccountNonce => light::remote_call(
                block,
                "AccountNonceApi_account_nonce".to_string(),
                random_account(),
            ),
            MethodKind::CanStore => {
                let mut data = random_account();
                data.extend_from_slice(&(1024u32 * 1024).to_le_bytes());
                light::remote_call(
                    block,
                    "BulletinTransactionStorageApi_can_store".to_string(),
                    data,
                )
            }
            MethodKind::AccountAuthorization => light::remote_call(
                block,
                "BulletinTransactionStorageApi_account_authorization".to_string(),
                random_account(),
            ),
            MethodKind::IndexedTransactions => light::remote_call(
                block,
                "TransactionStorageApi_indexed_transactions".to_string(),
                head_num.to_le_bytes().to_vec(),
            ),
            MethodKind::ReviveGetStorage { address } => {
                let mut data = address.to_vec();
                let mut key = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut key);
                data.extend_from_slice(&key);
                light::remote_call(block, "ReviveApi_get_storage".to_string(), data)
            }
            MethodKind::ReviveDotns { entries } => {
                let i = rand::thread_rng().next_u32() as usize % entries.len();
                light::remote_call(
                    block,
                    "ReviveApi_get_storage".to_string(),
                    entries[i].data.clone(),
                )
            }
            MethodKind::GenericCall { method, data } => {
                light::remote_call(block, method.clone(), data.clone())
            }
            MethodKind::GenericRead { keys } => light::remote_read(block, keys.clone()),
        }
    }
}

/// Parse a single method token into a labelled [`MethodKind`].
fn parse_method(token: &str) -> Result<(String, MethodKind), Box<dyn Error>> {
    let kind = match token {
        "account_nonce" => MethodKind::AccountNonce,
        "can_store" => MethodKind::CanStore,
        "account_authorization" => MethodKind::AccountAuthorization,
        "indexed_transactions" => MethodKind::IndexedTransactions,
        "revive_get_storage" => {
            let address: [u8; 20] = hex::decode(REVIVE_DEFAULT_ADDRESS)?
                .try_into()
                .map_err(|_| "revive default address is not 20 bytes")?;
            MethodKind::ReviveGetStorage { address }
        }
        // revive_dotns:<domain>[,<domain>…] — read the real REGISTRY_RECORDS
        // (owner) slot for each domain off DOTNS_REGISTRY.
        _ if token.starts_with("revive_dotns:") => {
            let address: [u8; 20] = hex::decode(REVIVE_DEFAULT_ADDRESS)?
                .try_into()
                .map_err(|_| "revive default address is not 20 bytes")?;
            // Domains are separated by '+' (not ',', which is the method-mix
            // separator) so revive_dotns can be combined with other methods.
            let rest = &token["revive_dotns:".len()..];
            let mut entries = Vec::new();
            for domain in rest.split('+').map(str::trim).filter(|s| !s.is_empty()) {
                let namehash = namehash(domain);
                let slot = 0u64; // REGISTRY_RECORDS: mapping(bytes32 => address) at slot 0
                let slot_key = mapping_slot_key(&namehash, slot);
                let mut data = address.to_vec();
                data.extend_from_slice(&slot_key);
                entries.push(DotnsEntry {
                    domain: domain.to_string(),
                    namehash,
                    slot,
                    slot_key,
                    address,
                    data,
                });
            }
            if entries.is_empty() {
                return Err("revive_dotns needs at least one domain".into());
            }
            MethodKind::ReviveDotns { entries }
        }
        // Core_version etc: a no-arg runtime call, handy for connectivity checks.
        _ if !token.contains(':') => MethodKind::GenericCall {
            method: token.to_string(),
            data: Vec::new(),
        },
        _ if token.starts_with("call:") => {
            // call:<method>:<hexdata>
            let rest = &token["call:".len()..];
            let (method, data_hex) = rest.split_once(':').unwrap_or((rest, ""));
            MethodKind::GenericCall {
                method: method.to_string(),
                data: hex::decode(data_hex.trim_start_matches("0x"))?,
            }
        }
        _ if token.starts_with("read:") => {
            // read:<hexkey>[,<hexkey>...]
            let rest = &token["read:".len()..];
            let keys = rest
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|k| hex::decode(k.trim_start_matches("0x")))
                .collect::<Result<Vec<_>, _>>()?;
            MethodKind::GenericRead { keys }
        }
        other => return Err(format!("unknown method '{other}'").into()),
    };
    Ok((token.to_string(), kind))
}

/// Parse a comma-separated, optionally `:weight`-suffixed method spec into a set
/// of labelled methods plus a weight-expanded round-robin schedule (indices into
/// the methods vector). Port of `call-load.js::parseMethodSpec`.
fn parse_method_spec(spec: &str) -> Result<(Vec<(String, MethodKind)>, Vec<usize>), Box<dyn Error>> {
    let mut methods: Vec<(String, MethodKind)> = Vec::new();
    let mut schedule: Vec<usize> = Vec::new();

    for tok in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        // A weight suffix only applies to the named methods (no embedded ':').
        // Generic `call:`/`read:` tokens carry their own colons, so we only peel
        // a trailing ":<n>" when the whole token isn't a generic form.
        let (name, weight) = if tok.starts_with("call:")
            || tok.starts_with("read:")
            || tok.starts_with("revive_dotns:")
        {
            (tok, 1usize)
        } else if let Some((n, w)) = tok.rsplit_once(':') {
            match w.parse::<usize>() {
                Ok(parsed) => (n, parsed.max(1)),
                Err(_) => (tok, 1),
            }
        } else {
            (tok, 1)
        };

        let (label, kind) = parse_method(name)?;
        let idx = methods.len();
        methods.push((label, kind));
        for _ in 0..weight {
            schedule.push(idx);
        }
    }

    if methods.is_empty() {
        return Err("empty method spec".into());
    }
    Ok((methods, schedule))
}

// ---------------------------------------------------------------------------
// Head tracking (fresh, non-pruned execution block)
// ---------------------------------------------------------------------------

/// Parse the `number` field of a JSON header (hex string) into a `u32`.
fn header_number(header: &serde_json::Value) -> Option<u32> {
    let s = header.get("number")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16)
        .ok()
        .map(|n| n as u32)
}

/// A `(block_hash_bytes, block_number)` pair for the block we execute against.
type Head = (Vec<u8>, u32);

/// Build a head source. With `fixed_block`, returns a constant head (its number
/// is resolved via `chain_getHeader`). Otherwise seeds with the current
/// finalized head and spawns a `chain_subscribeFinalizedHeads` task that keeps
/// the watch channel updated so the execution block never goes stale.
async fn head_source(
    rpc_url: Url,
    fixed_block: Option<String>,
) -> Result<watch::Receiver<Head>, Box<dyn Error>> {
    let rpc = client(rpc_url).await?;

    if let Some(block) = fixed_block {
        let block = format!("0x{}", block.trim_start_matches("0x"));
        let header: serde_json::Value = rpc
            .request("chain_getHeader", rpc_params![block.clone()])
            .await?;
        let num = header_number(&header).unwrap_or(0);
        let hash = hex::decode(block.trim_start_matches("0x"))?;
        let (_tx, rx) = watch::channel((hash, num));
        return Ok(rx);
    }

    // Seed with the current finalized head.
    let head_hash: String = rpc.request("chain_getFinalizedHead", rpc_params![]).await?;
    let header: serde_json::Value = rpc
        .request("chain_getHeader", rpc_params![head_hash.clone()])
        .await?;
    let num = header_number(&header).unwrap_or(0);
    let hash = hex::decode(head_hash.trim_start_matches("0x"))?;
    let (tx, rx) = watch::channel((hash, num));

    // Keep the head fresh in the background.
    tokio::spawn(async move {
        let mut sub = match rpc
            .subscribe::<serde_json::Value, _>(
                "chain_subscribeFinalizedHeads",
                rpc_params![],
                "chain_unsubscribeFinalizedHeads",
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                log::warn!("head subscription failed, using seed block only: {e}");
                return;
            }
        };
        while let Some(Ok(header)) = sub.next().await {
            let Some(num) = header_number(&header) else { continue };
            if let Ok(hash_hex) = rpc
                .request::<String, _>("chain_getBlockHash", rpc_params![num])
                .await
            {
                if let Ok(bytes) = hex::decode(hash_hex.trim_start_matches("0x")) {
                    let _ = tx.send((bytes, num));
                }
            }
        }
    });

    Ok(rx)
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MethodStats {
    issued: u64,
    ok: u64,
    err: u64,
    timeout: u64,
    proof_bytes_total: u64,
    latencies_us: Vec<u64>,
    sample: Option<String>,
    last_err: Option<String>,
}

/// Classify an [`OutboundFailure`] into a stable, human-readable bucket key.
///
/// The `Io` case keeps the inner message (e.g. "max sub-streams reached", which
/// is a *client-side* request-response cap, not a server signal) so the error
/// summary distinguishes tool artifacts from real node behaviour.
fn classify_failure(error: &OutboundFailure) -> String {
    match error {
        OutboundFailure::DialFailure => "dial-failure".to_string(),
        OutboundFailure::Timeout => "timeout (no response within --request-timeout)".to_string(),
        OutboundFailure::ConnectionClosed => "connection-closed".to_string(),
        OutboundFailure::UnsupportedProtocols => "unsupported-protocols".to_string(),
        OutboundFailure::Io(err) => format!("io: {err}"),
    }
}

/// `p`-th percentile (0..=100) of the latencies, in milliseconds.
fn percentile_ms(sorted_us: &[u64], p: usize) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let rank = (p * (sorted_us.len() - 1)) / 100;
    sorted_us[rank] as f64 / 1000.0
}

// ---------------------------------------------------------------------------
// The spammer
// ---------------------------------------------------------------------------

struct LightSpammer {
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
    in_flight: usize,
    count: usize,
    concurrency: usize,
    light_protocol: String,
    start: Instant,
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
            let idx = self.schedule[self.issued % self.schedule.len()];
            let (hash, num) = self.head.borrow().clone();
            let payload = self.methods[idx].1.build(&hash, num);
            let id = self.swarm.behaviour_mut().light.send_request(&peer, payload);
            self.pending.insert(id, (idx, Instant::now()));
            self.stats[idx].issued += 1;
            self.issued += 1;
            self.in_flight += 1;
        }
    }

    fn on_response(&mut self, id: OutboundRequestId, response: Result<Vec<u8>, ()>) {
        let Some((idx, t0)) = self.pending.remove(&id) else { return };
        self.in_flight -= 1;
        let st = &mut self.stats[idx];
        match response {
            Ok(bytes) => {
                st.ok += 1;
                st.latencies_us.push(t0.elapsed().as_micros() as u64);
                if let Ok(decoded) = light::decode_response(&bytes) {
                    if let Some(p) = decoded.proof_len() {
                        st.proof_bytes_total += p as u64;
                    }
                    if st.sample.is_none() {
                        st.sample = Some(format!(
                            "{decoded:?} ({} wire bytes)",
                            bytes.len()
                        ));
                    }
                }
            }
            // Substream closed without a response (node couldn't/declined).
            Err(()) => {
                st.err += 1;
                *self
                    .errors
                    .entry("no-response: peer closed substream (no proof)".to_string())
                    .or_insert(0) += 1;
            }
        }
    }

    fn on_failure(&mut self, id: OutboundRequestId, error: OutboundFailure) {
        let Some((idx, _t0)) = self.pending.remove(&id) else { return };
        self.in_flight -= 1;
        let st = &mut self.stats[idx];
        if matches!(error, OutboundFailure::Timeout) {
            st.timeout += 1;
        } else {
            st.err += 1;
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
                    log::info!("connected to {peer_id}, starting spam");
                    self.start = Instant::now();
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
                    "identified {peer_id} agent={:?} light_protocols={light_protos:?}",
                    info.agent_version
                );
                if !info.protocols.iter().any(|p| p.as_ref() == self.light_protocol) {
                    println!(
                        "WARNING: peer does not advertise {} (fork_id? pass --protocol)",
                        self.light_protocol
                    );
                }
            }
            SwarmEvent::Behaviour(SpamBehaviourEvent::Light(request_response::Event::Message {
                message,
                ..
            })) => {
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
                println!("Connection error (peer={peer_id:?}): {error}");
            }
            other => log::trace!("swarm event: {other:?}"),
        }
    }

    fn print_progress(&self) {
        let (ok, err, timeout): (u64, u64, u64) = self.stats.iter().fold((0, 0, 0), |a, s| {
            (a.0 + s.ok, a.1 + s.err, a.2 + s.timeout)
        });
        let elapsed = self.start.elapsed().as_secs_f64().max(0.001);
        let rps = ok as f64 / elapsed;
        print!(
            "\r  issued={}/{} in_flight={} ok={} err={} timeout={} | {:.0} req/s | {:.0}s   ",
            self.issued, self.count, self.in_flight, ok, err, timeout, rps, elapsed
        );
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }

    async fn run(&mut self, overall_timeout: Duration) {
        self.fill_window();
        let mut progress = tokio::time::interval(Duration::from_secs(1));
        progress.tick().await;
        let deadline = tokio::time::sleep(overall_timeout);
        tokio::pin!(deadline);

        loop {
            futures::select! {
                event = self.swarm.select_next_some().fuse() => {
                    self.handle_event(event);
                    if self.done() {
                        break;
                    }
                }
                _ = progress.tick().fuse() => self.print_progress(),
                _ = (&mut deadline).fuse() => {
                    println!("\n  overall timeout ({}s) reached", overall_timeout.as_secs());
                    break;
                }
            }
        }
        println!();
    }

    fn report(&self) {
        let elapsed = self.start.elapsed().as_secs_f64().max(0.001);
        let mut all: Vec<u64> = Vec::new();
        let (mut ok, mut err, mut timeout, mut proof) = (0u64, 0u64, 0u64, 0u64);
        for s in &self.stats {
            ok += s.ok;
            err += s.err;
            timeout += s.timeout;
            proof += s.proof_bytes_total;
            all.extend_from_slice(&s.latencies_us);
        }
        all.sort_unstable();

        println!("\n=== /light/2 spam summary ===");
        println!("protocol: {}", self.light_protocol);
        println!(
            "issued={} ok={} err={} timeout={} in {:.1}s => {:.0} ok req/s",
            self.issued,
            ok,
            err,
            timeout,
            elapsed,
            ok as f64 / elapsed
        );
        println!(
            "latency ms: p50={:.1} p90={:.1} p99={:.1} | proof bytes total={}",
            percentile_ms(&all, 50),
            percentile_ms(&all, 90),
            percentile_ms(&all, 99),
            proof
        );
        if !self.errors.is_empty() {
            let mut rows: Vec<(&String, &u64)> = self.errors.iter().collect();
            rows.sort_by(|a, b| b.1.cmp(a.1));
            println!("errors (reason -> count):");
            for (reason, count) in rows {
                println!("  {count:>6}  {reason}");
            }
        }
        println!("per method:");
        for (i, (label, _)) in self.methods.iter().enumerate() {
            let s = &self.stats[i];
            let mut lat = s.latencies_us.clone();
            lat.sort_unstable();
            let avg_proof = if s.ok > 0 { s.proof_bytes_total / s.ok } else { 0 };
            println!(
                "  {label}: issued={} ok={} err={} timeout={} | p50={:.1}ms p99={:.1}ms | avg proof={}B{}{}",
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
                s.last_err
                    .as_ref()
                    .map(|x| format!(" | last_err={x}"))
                    .unwrap_or_default(),
            );
        }
    }
}

/// Build the spammer swarm. Mirrors the transport stack of
/// `crate::utils::build_swarm` (TCP + DNS + WebSocket + noise + yamux); the wss
/// upgrade is required to reach `*.polkadot.io` hosts on :443.
async fn build_swarm(
    light_protocol: &str,
    request_timeout: Duration,
    max_concurrent_streams: usize,
) -> Result<Swarm<SpamBehaviour>, Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();
    let light = light::behaviour(light_protocol, request_timeout, max_concurrent_streams)?;

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
        .with_behaviour(|key| {
            let identify = identify::Behaviour::new(
                identify::Config::new("/substrate/1.0".to_string(), key.public())
                    .with_agent_version(AGENT.to_string())
                    .with_cache_size(0),
            );
            SpamBehaviour { identify, light }
        })
        .expect("Can construct behaviour; qed")
        .build();

    Ok(swarm)
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
    request_timeout: Duration,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
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
    println!("Count:       {count} (concurrency {concurrency}, req timeout {request_timeout:?})");

    // Show how each revive_dotns argument is assembled from the domain.
    for (label, kind) in &methods {
        if let MethodKind::ReviveDotns { entries } = kind {
            println!("{label} — ReviveApi_get_storage argument derivation:");
            for e in entries {
                println!("  domain   = {}", e.domain);
                println!("    namehash = 0x{}  (EIP-137)", hex::encode(e.namehash));
                println!(
                    "    slot     = {}  (REGISTRY_RECORDS: mapping(bytes32 => address))",
                    e.slot
                );
                println!(
                    "    slotKey  = keccak256(namehash ++ uint256(slot)) = 0x{}",
                    hex::encode(e.slot_key)
                );
                println!(
                    "    address  = 0x{}  (DOTNS_REGISTRY)",
                    hex::encode(e.address)
                );
                println!(
                    "    data     = address ++ slotKey = 0x{}  ({} bytes)",
                    hex::encode(&e.data),
                    e.data.len()
                );
            }
        }
    }

    println!("Resolving execution block...");
    let head = head_source(rpc_url, block).await?;
    {
        let (hash, num) = head.borrow().clone();
        println!("Block:       #{num} 0x{}", hex::encode(&hash));
    }

    // Raise the request-response per-connection stream cap above the spam
    // window so the client never self-throttles with "max sub-streams reached"
    // (libp2p's default is 100); the server stays the only limiter.
    let max_streams = concurrency.saturating_mul(2).max(256);
    let swarm = build_swarm(&light_protocol, request_timeout, max_streams).await?;
    let mut spammer = LightSpammer {
        swarm,
        peer: None,
        head,
        stats: (0..methods.len()).map(|_| MethodStats::default()).collect(),
        errors: HashMap::new(),
        methods,
        schedule,
        pending: HashMap::new(),
        issued: 0,
        in_flight: 0,
        count,
        concurrency,
        light_protocol,
        start: Instant::now(),
    };

    let multiaddr: Multiaddr = address.parse()?;
    spammer.swarm.dial(multiaddr.clone())?;
    println!("Dialing {multiaddr}...\n");

    spammer.run(timeout).await;
    spammer.report();

    Ok(())
}
