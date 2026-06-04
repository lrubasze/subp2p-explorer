// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! Shared building blocks for the `/light/2` load-test commands (`spam-light`,
//! `soak-light`): chain presets, the method registry + encoders, the
//! finalized-head tracker, the minimal swarm, and per-method stats helpers.

use crate::commands::authorities::client;
use jsonrpsee::{
    client_transport::ws::Url,
    core::client::{ClientT, SubscriptionClientT},
    rpc_params,
};
use libp2p::{
    identify, identity,
    request_response::{self, OutboundFailure},
    Swarm,
};
use rand::RngCore;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use subp2p_explorer::{
    light::{self, LightCodec},
    peer_behavior::AGENT,
};
use tokio::sync::watch;

/// Minimal swarm for the light load tests: identify (to learn the peer id and its
/// advertised protocols) plus the outbound `/light/2` request-response client.
#[derive(libp2p::swarm::NetworkBehaviour)]
pub(crate) struct SpamBehaviour {
    pub identify: identify::Behaviour,
    pub light: request_response::Behaviour<LightCodec>,
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
    pub(crate) fn rpc_url(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => "wss://paseo-asset-hub-next-rpc.polkadot.io",
            Chain::PaseoNextBulletin => "wss://paseo-bulletin-next-rpc.polkadot.io",
        }
    }

    /// A default p2p host to dial (a guess for convenience; override with
    /// `--address`). wss on :443 behind the ingress.
    pub(crate) fn address(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => {
                "/dns4/paseo-asset-hub-next-collator-node-0.parity-testnet.parity.io/tcp/443/wss"
            }
            Chain::PaseoNextBulletin => {
                "/dns4/paseo-bulletin-next-rpc-node-0.polkadot.io/tcp/443/wss"
            }
        }
    }

    pub(crate) fn default_methods(&self) -> &'static str {
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
pub(crate) const REVIVE_DEFAULT_ADDRESS: &str = "a1b2b939E82b2ecE55Bd8a0E283818BfC1CA6CDc";

/// Ethereum Keccak-256 (NOT SHA3-256).
pub(crate) fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// EIP-137 namehash of a dotted domain, e.g. `playground.dot`.
///
/// `node("") = 0x00…00`; `node(label.parent) = keccak256(node(parent) ++ keccak256(label))`,
/// processing labels right-to-left (TLD first).
pub(crate) fn namehash(domain: &str) -> [u8; 32] {
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
pub(crate) fn mapping_slot_key(key: &[u8; 32], slot: u64) -> [u8; 32] {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(key);
    // uint256 big-endian: the slot number lives in the low 8 bytes.
    buf[56..64].copy_from_slice(&slot.to_be_bytes());
    keccak256(&buf)
}

/// A precomputed dotNS read: the full derivation `domain → namehash → slotKey`,
/// and the assembled `get_storage` argument (`address ++ slotKey`).
#[derive(Debug, Clone)]
pub(crate) struct DotnsEntry {
    pub domain: String,
    pub namehash: [u8; 32],
    pub slot: u64,
    pub slot_key: [u8; 32],
    pub address: [u8; 20],
    /// `address (20) ++ slot_key (32)` — the `ReviveApi_get_storage` data argument.
    pub data: Vec<u8>,
}

/// How a single request is built. Most are runtime-API calls (`RemoteCallRequest`);
/// `read` is a storage Merkle proof (`RemoteReadRequest`).
#[derive(Debug, Clone)]
pub(crate) enum MethodKind {
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

pub(crate) fn random_account() -> Vec<u8> {
    let mut a = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut a);
    a.to_vec()
}

impl MethodKind {
    /// Build the prost-encoded `/light/2` request for this method, executing at
    /// block `head_hash` (raw 32 bytes), with `head_num` available as the
    /// `indexed_transactions` argument.
    pub(crate) fn build(&self, head_hash: &[u8], head_num: u32) -> Vec<u8> {
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
pub(crate) fn parse_method(token: &str) -> Result<(String, MethodKind), Box<dyn Error>> {
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
        // revive_dotns:<domain>[+<domain>…] — read the real REGISTRY_RECORDS
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
pub(crate) fn parse_method_spec(
    spec: &str,
) -> Result<(Vec<(String, MethodKind)>, Vec<usize>), Box<dyn Error>> {
    let mut methods: Vec<(String, MethodKind)> = Vec::new();
    let mut schedule: Vec<usize> = Vec::new();

    for tok in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        // A weight suffix only applies to the named methods (no embedded ':').
        // Generic `call:`/`read:`/`revive_dotns:` tokens carry their own colons,
        // so we only peel a trailing ":<n>" when the whole token isn't a generic form.
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

/// Print how each `revive_dotns` argument is assembled from its domain. Called
/// once at startup (not per connection).
pub(crate) fn print_dotns_derivation(methods: &[(String, MethodKind)]) {
    for (label, kind) in methods {
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
                println!("    address  = 0x{}  (DOTNS_REGISTRY)", hex::encode(e.address));
                println!(
                    "    data     = address ++ slotKey = 0x{}  ({} bytes)",
                    hex::encode(&e.data),
                    e.data.len()
                );
            }
        }
    }
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
pub(crate) type Head = (Vec<u8>, u32);

/// Build a head source. With `fixed_block`, returns a constant head (its number
/// is resolved via `chain_getHeader`). Otherwise seeds with the current
/// finalized head and spawns a `chain_subscribeFinalizedHeads` task that keeps
/// the watch channel updated so the execution block never goes stale.
pub(crate) async fn head_source(
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
// Stats helpers
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct MethodStats {
    pub issued: u64,
    pub ok: u64,
    pub err: u64,
    pub timeout: u64,
    pub proof_bytes_total: u64,
    pub latencies_us: Vec<u64>,
    pub sample: Option<String>,
    pub last_err: Option<String>,
}

/// Classify an [`OutboundFailure`] into a stable, human-readable bucket key.
///
/// The `Io` case keeps the inner message (e.g. "max sub-streams reached", which
/// is a *client-side* request-response cap, not a server signal) so the error
/// summary distinguishes tool artifacts from real node behaviour.
pub(crate) fn classify_failure(error: &OutboundFailure) -> String {
    match error {
        OutboundFailure::DialFailure => "dial-failure".to_string(),
        OutboundFailure::Timeout => "timeout (no response within --request-timeout)".to_string(),
        OutboundFailure::ConnectionClosed => "connection-closed".to_string(),
        OutboundFailure::UnsupportedProtocols => "unsupported-protocols".to_string(),
        OutboundFailure::Io(err) => format!("io: {err}"),
    }
}

/// `p`-th percentile (0..=100) of the latencies, in milliseconds.
pub(crate) fn percentile_ms(sorted_us: &[u64], p: usize) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let rank = (p * (sorted_us.len() - 1)) / 100;
    sorted_us[rank] as f64 / 1000.0
}

/// Merge a `reason -> count` error map into an aggregate.
pub(crate) fn merge_error_map(into: &mut HashMap<String, u64>, from: HashMap<String, u64>) {
    for (reason, c) in from {
        *into.entry(reason).or_insert(0) += c;
    }
}

// ---------------------------------------------------------------------------
// Swarm
// ---------------------------------------------------------------------------

/// Build a load-test swarm with a fresh random identity (=> distinct PeerId).
/// Mirrors the transport stack of `crate::utils::build_swarm` (TCP + DNS +
/// WebSocket + noise + yamux); the wss upgrade is required to reach
/// `*.polkadot.io` hosts on :443.
pub(crate) async fn build_swarm(
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
