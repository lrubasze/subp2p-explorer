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

/// Built-in chain presets (RPC url, default p2p collator host, default method
/// mix). Everything is overridable on the command line. Genesis is fetched from
/// the preset RPC, so no relay-chain knowledge is needed — we dial the parachain
/// node directly.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Chain {
    /// Paseo Next Asset Hub (revive / `ReviveApi_get_storage`).
    PaseoNextAssetHub,
    /// Paseo Next Bulletin (transaction storage).
    PaseoNextBulletin,
    /// Web3 Summit Network Asset Hub (revive / `ReviveApi_get_storage`).
    SummitAssetHub,
    /// Web3 Summit Network Bulletin (transaction storage).
    SummitBulletin,
    /// Web3 Summit Network People (identity / individuality runtime).
    SummitPeople,
}

impl Chain {
    pub(crate) fn rpc_url(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => "wss://paseo-asset-hub-next-rpc.polkadot.io",
            Chain::PaseoNextBulletin => "wss://paseo-bulletin-next-rpc.polkadot.io",
            Chain::SummitAssetHub => "wss://summit-asset-hub-rpc.polkadot.io",
            Chain::SummitBulletin => "wss://summit-bulletin-rpc.polkadot.io",
            Chain::SummitPeople => "wss://summit-people-rpc.polkadot.io",
        }
    }

    /// A default p2p collator host to dial (a guess for convenience; override
    /// with `--address`). wss on :443 behind the ingress.
    pub(crate) fn address(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub => {
                "/dns4/paseo-asset-hub-next-collator-node-0.parity-testnet.parity.io/tcp/443/wss"
            }
            Chain::PaseoNextBulletin => {
                "/dns4/paseo-bulletin-next-rpc-node-0.polkadot.io/tcp/443/wss"
            }
            Chain::SummitAssetHub => {
                "/dns4/summit-asset-hub-collator-node-0.parity-chains.parity.io/tcp/443/wss"
            }
            Chain::SummitBulletin => {
                "/dns4/summit-bulletin-collator-node-0.parity-chains.parity.io/tcp/443/wss"
            }
            Chain::SummitPeople => {
                "/dns4/summit-people-collator-node-0.parity-chains.parity.io/tcp/443/wss"
            }
        }
    }

    pub(crate) fn default_methods(&self) -> &'static str {
        match self {
            Chain::PaseoNextAssetHub | Chain::SummitAssetHub => "revive_get_storage,account_nonce",
            Chain::PaseoNextBulletin | Chain::SummitBulletin => {
                "account_nonce,can_store,account_authorization,indexed_transactions"
            }
            Chain::SummitPeople => "account_nonce",
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

/// One key in a read request: a fixed prefix, optionally followed by
/// `random_bytes` fresh random bytes materialised per request.
///
/// The randomised form exists to defeat cache warming. A fixed key pulls its
/// trie path into the node's node cache on the first request, so every request
/// after it is a cache hit walking the same path — for a small read, where the
/// walk *is* the cost, that measures re-answering one cached lookup rather than
/// serving reads. Real clients scatter their keys across the trie. This is the
/// same reasoning behind the random key in [`MethodKind::ReviveGetStorage`].
///
/// Note that a random key almost certainly does not exist, so the response is an
/// absence proof: a real trie walk with real cache misses, but fewer bytes than a
/// hit. For honest response sizes use fixed keys that resolve (e.g.
/// `revive_dotns`) and report the two separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadKey {
    /// Literal leading bytes, e.g. a `twox128(pallet) ++ twox128(item)` prefix.
    pub prefix: Vec<u8>,
    /// Fresh random bytes appended per request. 0 = a fully fixed key.
    pub random_bytes: usize,
}

impl ReadKey {
    /// A key with no randomised tail.
    pub(crate) fn fixed(prefix: Vec<u8>) -> Self {
        Self {
            prefix,
            random_bytes: 0,
        }
    }

    /// Build the concrete key for one request.
    pub(crate) fn materialize(&self) -> Vec<u8> {
        if self.random_bytes == 0 {
            return self.prefix.clone();
        }
        let mut key = Vec::with_capacity(self.prefix.len() + self.random_bytes);
        key.extend_from_slice(&self.prefix);
        let mut tail = vec![0u8; self.random_bytes];
        rand::thread_rng().fill_bytes(&mut tail);
        key.extend_from_slice(&tail);
        key
    }
}

/// Materialize a whole key list for one request.
fn materialize_keys(keys: &[ReadKey]) -> Vec<Vec<u8>> {
    keys.iter().map(ReadKey::materialize).collect()
}

/// How a single request is built. Most are runtime-API calls (`RemoteCallRequest`);
/// `read` is a main-trie storage Merkle proof (`RemoteReadRequest`) and
/// `read_child` the child-trie equivalent (`RemoteReadChildRequest`).
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
    /// Arbitrary `RemoteReadRequest` for one or more storage keys.
    GenericRead { keys: Vec<ReadKey> },
    /// Arbitrary `RemoteReadChildRequest`: the same keys read from the child trie
    /// named `child_trie` (bare name — the `:child_storage:default:` prefix is
    /// added by the encoder).
    ///
    /// This is the shape a real product sends. A dotli dotNS lookup reads a
    /// revive contract's slots, and revive keeps every contract's storage in its
    /// own child trie (`frame/revive/src/storage.rs`, `ChildInfo::new_default`),
    /// so the client does a main-trie read for the contract's `trie_id` and then
    /// a child read per 32-byte slot.
    GenericReadChild {
        child_trie: Vec<u8>,
        keys: Vec<ReadKey>,
    },
}

pub(crate) fn random_account() -> Vec<u8> {
    let mut a = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut a);
    a.to_vec()
}

/// A `RemoteCallRequest` for a runtime API that takes no arguments. Every call
/// in smoldot's warp-sync set is of this shape (`RuntimeCall::parameter_vectored`
/// returns an empty iterator in `smoldot/lib/src/chain/chain_information/build.rs`).
fn no_arg_call(method: &str) -> MethodKind {
    MethodKind::GenericCall {
        method: method.to_string(),
        data: Vec::new(),
    }
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
            MethodKind::GenericRead { keys } => light::remote_read(block, materialize_keys(keys)),
            MethodKind::GenericReadChild { child_trie, keys } => {
                light::remote_read_child(block, child_trie, materialize_keys(keys))
            }
        }
    }
}

/// Parse a `+`-separated key list into [`ReadKey`]s.
///
/// Each key is `<hex>` for a fixed key, or `<hex>:rand[N]` for that hex prefix
/// followed by `N` fresh random bytes per request (`N` defaults to 32, the width
/// of both an `AccountId32` and an EVM storage slot). `<hex>` may be empty in the
/// randomised form, which gives a wholly random key.
///
/// Keys are separated with `+`, not `,`: `parse_method_spec` splits the spec on
/// `,` before this ever runs, so a comma would detach every key after the first.
fn parse_read_keys(spec: &str, what: &str) -> Result<Vec<ReadKey>, Box<dyn Error>> {
    let mut keys = Vec::new();
    for token in spec.split('+').map(str::trim).filter(|s| !s.is_empty()) {
        let (hex_part, random_bytes) = match token.split_once(':') {
            Some((hex_part, tail)) => {
                let tail = tail.trim();
                let digits = tail
                    .strip_prefix("rand")
                    .ok_or_else(|| format!("unknown key suffix ':{tail}' — expected ':rand[N]'"))?;
                let n = if digits.is_empty() {
                    32
                } else {
                    digits.parse::<usize>().map_err(|_| {
                        format!("':rand{digits}' needs a byte count, e.g. ':rand32'")
                    })?
                };
                if n == 0 {
                    return Err("':rand0' adds no bytes — drop the suffix for a fixed key".into());
                }
                (hex_part.trim(), n)
            }
            None => (token, 0),
        };
        let prefix = hex::decode(hex_part.trim_start_matches("0x"))?;
        if prefix.is_empty() && random_bytes == 0 {
            return Err(format!("{what}: empty storage key").into());
        }
        keys.push(ReadKey {
            prefix,
            random_bytes,
        });
    }
    if keys.is_empty() {
        return Err(format!("{what} needs at least one hex storage key").into());
    }
    Ok(keys)
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

        // --- smoldot warp-sync tail ---------------------------------------
        // Steps 3 and 4 of smoldot's warp sync are the *only* `/light/2` traffic
        // a warp-syncing light client generates; there is no `/state/2` and no
        // body download (`light-base` sets `download_bodies: false`). See
        // `smoldot/lib/src/sync/warp_sync.rs` (module docs, and
        // `runtime_calls_default_value` for the per-consensus call set).
        //
        // These are named presets purely for discoverability and honest labels
        // in the per-method stats — each one is reachable through the generic
        // `read:` / bare-runtime-API forms too.

        // Step 3, the runtime download, and by far the heaviest single request
        // in a warp sync: the response carries the entire runtime blob. Measured
        // on Kusama, `:code` is 1,724,612 bytes and its read proof is 1,725,384
        // — i.e. 772 bytes of surrounding trie nodes, the rest is the Wasm.
        // Both keys go in one request, as smoldot sends them (`warp_sync.rs`,
        // `keys: vec![code_key_to_request, b":heappages"]`).
        "warp_code" => MethodKind::GenericRead {
            keys: vec![
                ReadKey::fixed(b":code".to_vec()),
                ReadKey::fixed(b":heappages".to_vec()),
            ],
        },

        // Step 4, the consensus parameters. Small on the wire — the proof holds
        // only the storage items the call touched, *not* `:code`, which the node
        // resolves off the unwrapped trie backend before the recorder is built
        // (`polkadot-sdk/substrate/client/service/src/client/call_executor.rs`,
        // `prove_execution`). The cost is CPU: each one is a real Wasm
        // instantiation + call in the node's proving backend.
        "babe_configuration" => no_arg_call("BabeApi_configuration"),
        "babe_current_epoch" => no_arg_call("BabeApi_current_epoch"),
        "babe_next_epoch" => no_arg_call("BabeApi_next_epoch"),
        // Aura chains (most parachains, incl. the Asset Hub / Bulletin / People
        // presets above) take this pair instead of the three Babe calls.
        "aura_slot_duration" => no_arg_call("AuraApi_slot_duration"),
        "aura_authorities" => no_arg_call("AuraApi_authorities"),

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
        // A long even-length run of hex digits is a storage key someone separated
        // with `,` instead of `+`, not a runtime API name (those always carry an
        // `Api_method` shape). Catching it here keeps the old failure mode —
        // a stray key quietly becoming a runtime call — from coming back.
        _ if !token.contains(':')
            && token.len() >= 16
            && token.len() % 2 == 0
            && token.chars().all(|c| c.is_ascii_hexdigit()) =>
        {
            return Err(format!(
                "'{token}' looks like a hex storage key, not a runtime API name. \
                 Multiple read keys are separated with '+': read:<key1>+<key2>"
            )
            .into())
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
        // read:<hexkey>[+<hexkey>...] — all keys go in one `RemoteReadRequest`.
        //
        // Keys are separated with `+`, not `,`, for the same reason
        // `revive_dotns` uses `+`: `parse_method_spec` splits the whole spec on
        // `,` before this function sees a token, so a comma here would detach
        // every key after the first. Those stray keys used to fall through to
        // the bare-runtime-API arm and silently become runtime calls named after
        // the hex string.
        _ if token.starts_with("read:") => {
            let keys = parse_read_keys(&token["read:".len()..], "read:")?;
            MethodKind::GenericRead { keys }
        }
        // read_child:<hex trie_id>:<hexkey>[+<hexkey>…] — the same keys read from
        // one contract's child trie, in a single `RemoteReadChildRequest`.
        //
        // `trie_id` is the bare child trie name; the encoder adds the
        // `:child_storage:default:` prefix. Getting it is the client's first hop:
        // read `Revive::AccountInfoOf[address]` off the main trie and decode
        // `ContractInfo.trie_id` (the SCALE `Vec<u8>` right after the 0x00
        // Contract enum tag), e.g.
        //
        //   KEY=0x$(printf %s "$(twox128 Revive)$(twox128 AccountInfoOf)")<address>
        //   curl … -d '{"method":"state_getStorage","params":["'$KEY'"]}'
        //
        // A `trie_id` that does not exist on the target chain is the same trap as
        // a wrong `revive_get_storage` address: the node still answers, just off a
        // much cheaper path (nothing to walk), so the run looks healthy while
        // measuring the wrong thing. Sanity-check the response sizes against a
        // known-good slot before trusting a number.
        _ if token.starts_with("read_child:") => {
            let rest = &token["read_child:".len()..];
            let (trie_id_hex, keys_spec) = rest.split_once(':').ok_or(
                "read_child needs a child trie and keys: \
                 read_child:<hex trie_id>:<hexkey>[+<hexkey>…]",
            )?;
            let child_trie = hex::decode(trie_id_hex.trim().trim_start_matches("0x"))?;
            if child_trie.is_empty() {
                return Err("read_child: empty child trie id".into());
            }
            let keys = parse_read_keys(keys_spec, "read_child:")?;
            MethodKind::GenericReadChild { child_trie, keys }
        }
        other => return Err(format!("unknown method '{other}'").into()),
    };
    Ok((token.to_string(), kind))
}

/// Parse a comma-separated, weighted method spec into a set of labelled methods
/// plus a weight-expanded round-robin schedule (indices into the methods vector).
///
/// Two weight syntaxes:
///
/// - `@<n>` works on **every** method form, including the generic `read:` /
///   `call:` / `read_child:` ones. `read:<key>:rand48@400`.
/// - `:<n>` works only on the plain named presets (`warp_code:3`), because the
///   generic forms carry their own colons and peeling a trailing `:<n>` off those
///   would hand `parse_method` a truncated token — a `:rand48` read would lose its
///   suffix, and a key ending in digits would lose its tail.
///
/// `@` is the one separator that cannot collide: it appears in no hex key, no
/// runtime API name, and no `:rand[N]` suffix. Quote the spec in the shell, since
/// an unquoted `@` is fine but a `*` would have globbed.
pub(crate) fn parse_method_spec(
    spec: &str,
) -> Result<(Vec<(String, MethodKind)>, Vec<usize>), Box<dyn Error>> {
    let mut methods: Vec<(String, MethodKind)> = Vec::new();
    let mut schedule: Vec<usize> = Vec::new();
    let mut weights: Vec<(usize, usize)> = Vec::new();

    for tok in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        // An explicit `@<n>` wins, and is the only form that can weight a
        // generic method. A trailing `@` with a non-numeric tail is an error
        // rather than a silently unweighted method: it is always a typo.
        let at_weight = match tok.rsplit_once('@') {
            Some((head, w)) => {
                let parsed: usize = w.trim().parse().map_err(|_| {
                    format!("'{tok}': '@{w}' is not a weight — expected '@<n>', e.g. '@400'")
                })?;
                if parsed == 0 {
                    return Err(format!("'{tok}': weight 0 would never be issued").into());
                }
                Some((head.trim(), parsed))
            }
            None => None,
        };

        let (name, weight) = if let Some((head, w)) = at_weight {
            (head, w)
        } else if tok.starts_with("call:")
            || tok.starts_with("read:")
            || tok.starts_with("read_child:")
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
        weights.push((idx, weight));
    }

    if methods.is_empty() {
        return Err("empty method spec".into());
    }

    // Interleave the schedule instead of emitting each method's slots as one
    // consecutive block.
    //
    // A blocky schedule is not a mix, it is a sequence of monocultures: weights
    // 800/100/80 would send 800 small reads, then 100 nonces, then 80 mid reads.
    // Worse, a connection walks the schedule from 0 and only issues
    // `rate*duration/clients` requests before it retires, so with a short
    // per-connection budget every connection dies inside the first block and the
    // other methods are *never issued at all*. MEASURED: a 90 s run of a
    // seven-method mix issued 598,016 small reads and zero of everything else.
    //
    // Placing each method's k-th slot at (k + 0.5)/w along the unit interval and
    // sorting gives an even spread: for every method and every prefix of the
    // schedule, the count is within 1 of the ideal share. So any window of the
    // schedule — including a short-lived connection's — approximates the weights.
    let total: usize = weights.iter().map(|(_, w)| w).sum();
    let mut slots: Vec<(f64, usize)> = Vec::with_capacity(total);
    for &(idx, w) in &weights {
        for k in 0..w {
            slots.push(((k as f64 + 0.5) / w as f64, idx));
        }
    }
    // Ties (two methods of equal weight) resolve by method order, which keeps the
    // schedule deterministic across runs.
    slots.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
    schedule.extend(slots.into_iter().map(|(_, idx)| idx));

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
    let rpc = client(rpc_url.clone()).await?;

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

    // Keep the head fresh in the background, reconnecting for as long as the run
    // lasts.
    //
    // This used to be a single subscription whose task exited on the first stream
    // error, which is a silent failure of the worst kind: the head freezes, the
    // run keeps completing requests at full rate, and once the frozen block falls
    // out of the node's ~256-block state window every response comes back with no
    // proof — off a path that does no trie work at all. A 12-hour soak would
    // report healthy throughput for the error path and nobody would see it in the
    // rate. So: retry forever, and shout when the head goes stale.
    tokio::spawn(async move {
        // Kusama finalises roughly every 6 s; ~256 blocks of state (~25 min) is
        // the window before a frozen head starts returning proofless responses.
        // Warn long before that, at 10 missed blocks.
        const STALE_AFTER: Duration = Duration::from_secs(60);
        let mut backoff = Duration::from_secs(1);
        let mut rpc = Some(rpc);
        let mut last_ok = std::time::Instant::now();

        loop {
            // Rebuild the client if the previous connection died: a jsonrpsee
            // client does not recover from a dropped transport.
            let conn = match rpc.take() {
                Some(c) => c,
                // `client`'s error is a plain `Box<dyn Error>`, which is not
                // `Send`, so it must not be held across the sleep below — flatten
                // it to a String before any await.
                None => match client(rpc_url.clone()).await.map_err(|e| e.to_string()) {
                    Ok(c) => {
                        log::info!("head tracker: reconnected to the RPC");
                        backoff = Duration::from_secs(1);
                        c
                    }
                    Err(e) => {
                        log::warn!("head tracker: RPC reconnect failed: {e}");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                },
            };

            match conn
                .subscribe::<serde_json::Value, _>(
                    "chain_subscribeFinalizedHeads",
                    rpc_params![],
                    "chain_unsubscribeFinalizedHeads",
                )
                .await
            {
                Ok(mut sub) => {
                    backoff = Duration::from_secs(1);
                    loop {
                        match sub.next().await {
                            Some(Ok(header)) => {
                                let Some(num) = header_number(&header) else {
                                    continue;
                                };
                                match conn
                                    .request::<String, _>("chain_getBlockHash", rpc_params![num])
                                    .await
                                {
                                    Ok(hash_hex) => {
                                        if let Ok(bytes) =
                                            hex::decode(hash_hex.trim_start_matches("0x"))
                                        {
                                            let _ = tx.send((bytes, num));
                                            last_ok = std::time::Instant::now();
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("head tracker: block hash lookup failed: {e}");
                                        break;
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::warn!("head tracker: subscription error: {e}");
                                break;
                            }
                            // Stream ended: the server closed the subscription.
                            None => {
                                log::warn!("head tracker: subscription closed by the server");
                                break;
                            }
                        }
                    }
                }
                Err(e) => log::warn!("head tracker: subscribe failed: {e}"),
            }

            // Dropped: the client is suspect, so the next iteration rebuilds it.
            let stale = last_ok.elapsed();
            if stale > STALE_AFTER {
                log::error!(
                    "head tracker: execution block is {}s stale — responses may carry no proof \
                     once it leaves the node's state window; treat this run's numbers as suspect",
                    stale.as_secs()
                );
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
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
    /// A response that actually carried a proof.
    pub ok: u64,
    /// The node closed the substream without writing a single byte. Distinct from
    /// `unserved` (a valid, complete response that simply carries no proof) and
    /// from `err` (which is reserved for transport trouble): this is the node
    /// actively refusing or dropping the request, i.e. the load-shedding signal.
    pub shed: u64,
    /// A response arrived, but with no proof: the node could not serve the
    /// request. `RemoteCallResponse { proof: None }` is what substrate replies
    /// with when the runtime call fails — an unknown runtime method, a pruned
    /// block — and it logs that at trace only
    /// (`light_client_requests/handler.rs:194-206`). Counted apart from `ok`
    /// because it is closer to a failure, and apart from `err` because the node
    /// did answer. Its latency is tracked separately too: an unserved call skips
    /// execution, so folding it into `latencies_us` would flatter the numbers.
    pub unserved: u64,
    /// Transport failure only: dial failure, connection closed, io error. A
    /// refusal by the node is `shed`, not this.
    pub err: u64,
    pub timeout: u64,
    pub proof_bytes_total: u64,
    pub latencies_us: Vec<u64>,
    /// Latencies of the `unserved` responses — a useful control, since it is the
    /// round-trip cost with the proving work removed.
    pub unserved_us: Vec<u64>,
    /// Issued, then abandoned without an outcome: the connection closed or the
    /// run deadline fired while the request was in flight. Tracked so that
    /// `ok + unserved + err + timeout + aborted == issued` always holds — without
    /// it these vanish silently and a broken run looks merely quiet.
    pub aborted: u64,
    pub sample: Option<String>,
    pub last_err: Option<String>,
}

/// Fold a decoded `/light/2` response into per-method stats, splitting a served
/// proof from a proof-less reply.
///
/// Returns the human-readable reason if the body could not be decoded at all, so
/// the caller can add it to its error map.
pub(crate) fn record_light_response(
    st: &mut MethodStats,
    bytes: &[u8],
    latency_us: u64,
) -> Option<String> {
    match light::decode_response(bytes) {
        Ok(decoded) => {
            if st.sample.is_none() {
                st.sample = Some(format!("{decoded:?} ({} wire bytes)", bytes.len()));
            }
            match decoded.proof_len() {
                Some(p) => {
                    st.ok += 1;
                    st.proof_bytes_total += p as u64;
                    st.latencies_us.push(latency_us);
                }
                None => {
                    st.unserved += 1;
                    st.unserved_us.push(latency_us);
                }
            }
            None
        }
        Err(e) => {
            // Well-framed but not valid protobuf. Not a served proof either.
            st.unserved += 1;
            st.unserved_us.push(latency_us);
            Some(format!("undecodable response body: {e}"))
        }
    }
}

/// The reason key used for a shed request, shared so the two light commands
/// bucket it identically.
pub(crate) const SHED_REASON: &str = "shed: node closed the substream with no proof";

/// Record a shed request: the node closed the substream without writing anything
/// (`Err(())` from [`light::LightCodec`], mirroring substrate's `GenericCodec`).
pub(crate) fn record_light_shed(st: &mut MethodStats, errors: &mut HashMap<String, u64>) {
    st.shed += 1;
    st.last_err = Some(SHED_REASON.to_string());
    *errors.entry(SHED_REASON.to_string()).or_insert(0) += 1;
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
/// How long we keep a connection with no open substream. Must comfortably exceed
/// the longest gap between a connection's requests: in an open-loop soak that is
/// the wait for a rate-limiter permit, which at a low `--rate` and a full
/// connection pool can be tens of seconds.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(300);

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
        // libp2p's default idle_connection_timeout is 0: a connection with no
        // open substream is closed at once. The closed-loop commands never
        // noticed, because they always have a request in flight — but an
        // open-loop soak connection sits idle between requests while it waits
        // for its rate-limiter permit, and was being reaped by our own swarm
        // mid-run. `hold_peers` sets this explicitly for the same reason.
        .with_swarm_config(|c| c.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
        .build();

    Ok(swarm)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The runtime-API names and storage keys are copied from smoldot; a typo
    /// here would surface only as an opaque node-side error at run time, so pin
    /// them. Sources: `smoldot/lib/src/chain/chain_information/build.rs`
    /// (`RuntimeCall::function_name`) and `smoldot/lib/src/sync/warp_sync.rs`.
    #[test]
    fn warp_sync_presets_have_the_right_shape() {
        let (_, kind) = parse_method("warp_code").expect("warp_code parses");
        match kind {
            MethodKind::GenericRead { keys } => {
                // Both keys in one request, in smoldot's order, neither randomised.
                assert_eq!(
                    keys,
                    vec![
                        ReadKey::fixed(b":code".to_vec()),
                        ReadKey::fixed(b":heappages".to_vec()),
                    ]
                );
            }
            other => panic!("warp_code should be a storage read, got {other:?}"),
        }

        for (token, expected) in [
            ("babe_configuration", "BabeApi_configuration"),
            ("babe_current_epoch", "BabeApi_current_epoch"),
            ("babe_next_epoch", "BabeApi_next_epoch"),
            ("aura_slot_duration", "AuraApi_slot_duration"),
            ("aura_authorities", "AuraApi_authorities"),
        ] {
            let (label, kind) = parse_method(token).expect("preset parses");
            assert_eq!(label, token, "the stats label is the token as typed");
            match kind {
                MethodKind::GenericCall { method, data } => {
                    assert_eq!(method, expected);
                    assert!(data.is_empty(), "{token} takes no arguments");
                }
                other => panic!("{token} should be a runtime call, got {other:?}"),
            }
        }
    }

    /// `read:` keys are separated with `+`. A `,` would be eaten by
    /// `parse_method_spec`'s method splitter before `parse_method` ever saw the
    /// token, detaching every key after the first.
    #[test]
    fn multi_key_reads_use_plus_and_stay_one_request() {
        let (methods, schedule) =
            parse_method_spec("read:aabb+0xccdd").expect("a two-key read parses");

        assert_eq!(methods.len(), 1, "both keys belong to one request");
        assert_eq!(schedule.len(), 1);
        match &methods[0].1 {
            // `0x` prefixes are optional and stripped per key.
            MethodKind::GenericRead { keys } => {
                assert_eq!(
                    keys,
                    &vec![
                        ReadKey::fixed(vec![0xaa, 0xbb]),
                        ReadKey::fixed(vec![0xcc, 0xdd])
                    ]
                );
            }
            other => panic!("expected a storage read, got {other:?}"),
        }
    }

    /// `:rand[N]` is what keeps a small read from measuring the node's node cache.
    /// The prefix is fixed, the tail is fresh per request, and the width defaults
    /// to 32 bytes (an `AccountId32` / an EVM slot).
    #[test]
    fn random_key_suffix_widens_the_prefix_and_varies_per_request() {
        let account_prefix = "26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9";
        let (_, kind) =
            parse_method(&format!("read:{account_prefix}:rand")).expect("a randomised read parses");

        let keys = match &kind {
            MethodKind::GenericRead { keys } => keys,
            other => panic!("expected a storage read, got {other:?}"),
        };
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].prefix, hex::decode(account_prefix).unwrap());
        assert_eq!(keys[0].random_bytes, 32, ":rand defaults to 32 bytes");

        // The fixed prefix is preserved and the tail actually moves.
        let first = keys[0].materialize();
        let second = keys[0].materialize();
        assert_eq!(first.len(), 32 + 32);
        assert_eq!(first[..32], second[..32], "the prefix is fixed");
        assert_ne!(first, second, "the tail is fresh per request");

        // An explicit width, and a fully random key (empty prefix).
        let (_, kind) = parse_method("read:aabb:rand8").expect("an explicit width parses");
        match kind {
            MethodKind::GenericRead { keys } => {
                assert_eq!(keys[0].random_bytes, 8);
                assert_eq!(keys[0].materialize().len(), 2 + 8);
            }
            other => panic!("expected a storage read, got {other:?}"),
        }
        let (_, kind) = parse_method("read::rand16").expect("an empty prefix parses");
        match kind {
            MethodKind::GenericRead { keys } => {
                assert!(keys[0].prefix.is_empty());
                assert_eq!(keys[0].materialize().len(), 16);
            }
            other => panic!("expected a storage read, got {other:?}"),
        }

        // A fixed key stays byte-for-byte stable, and a typo'd suffix is an error
        // rather than a silently fixed key.
        let (_, kind) = parse_method("read:aabb").expect("a fixed key parses");
        match kind {
            MethodKind::GenericRead { keys } => {
                assert_eq!(keys[0].random_bytes, 0);
                assert_eq!(keys[0].materialize(), keys[0].materialize());
            }
            other => panic!("expected a storage read, got {other:?}"),
        }
        assert!(parse_method("read:aabb:random32").is_err());
        assert!(parse_method("read:aabb:rand0").is_err());
    }

    /// `read_child` must produce a `RemoteReadChildRequest`, and the encoder must
    /// prefix the bare trie id — the node runs `ChildType::from_prefixed_key` on
    /// the field and rejects a bare name with `InvalidChildStorageKey`.
    #[test]
    fn child_reads_carry_the_trie_id_and_encode_as_a_child_request() {
        let trie_id = "0102030405060708";
        let slot = "deadbeef";
        let (label, kind) = parse_method(&format!("read_child:{trie_id}:{slot}+aabb:rand32"))
            .expect("a child read parses");
        assert_eq!(label, format!("read_child:{trie_id}:{slot}+aabb:rand32"));

        match &kind {
            MethodKind::GenericReadChild { child_trie, keys } => {
                assert_eq!(child_trie, &hex::decode(trie_id).unwrap());
                assert_eq!(keys.len(), 2, "both keys ride in one request");
                assert_eq!(keys[0], ReadKey::fixed(hex::decode(slot).unwrap()));
                assert_eq!(keys[1].random_bytes, 32);
            }
            other => panic!("expected a child storage read, got {other:?}"),
        }

        // The two variants must not encode to the same bytes — that they are the
        // right protobuf messages is asserted in `subp2p_explorer::light`.
        let head = [7u8; 32];
        let (_, main) = parse_method(&format!("read:{slot}")).expect("a main read parses");
        assert_ne!(
            kind.build(&head, 0),
            main.build(&head, 0),
            "a child read must not encode as a main-trie read"
        );
    }

    /// The generic forms carry their own colons, so the `:weight` peeler must
    /// leave them alone. An all-digit key is the case that bites: `rsplit_once`
    /// would read it as a weight and hand `parse_method` a truncated token.
    #[test]
    fn generic_forms_keep_their_colons_out_of_the_weight_suffix() {
        let (methods, schedule) =
            parse_method_spec("read_child:aabb:1234").expect("an all-digit child key parses");
        assert_eq!(schedule.len(), 1, "no weight was peeled");
        match &methods[0].1 {
            MethodKind::GenericReadChild { child_trie, keys } => {
                assert_eq!(child_trie, &vec![0xaa, 0xbb]);
                assert_eq!(keys, &vec![ReadKey::fixed(vec![0x12, 0x34])]);
            }
            other => panic!("expected a child storage read, got {other:?}"),
        }

        // The same hazard on a main-trie read.
        let (methods, schedule) = parse_method_spec("read:1234").expect("an all-digit key parses");
        assert_eq!(schedule.len(), 1);
        assert_eq!(methods[0].0, "read:1234");

        // Named methods still take a weight.
        let (methods, schedule) =
            parse_method_spec("warp_code:3").expect("a weighted preset parses");
        assert_eq!(methods.len(), 1);
        assert_eq!(schedule.len(), 3);
    }

    /// `@<n>` is the only way to weight a generic `read:`/`call:` form, which is
    /// what a realistic mixed soak needs: the reads carry most of the traffic, and
    /// `:weight` cannot reach them.
    #[test]
    fn at_weights_apply_to_every_method_form() {
        let account = "26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9";
        let spec = format!("read:{account}:rand48@400,account_nonce@200,Metadata_metadata@1");
        let (methods, schedule) = parse_method_spec(&spec).expect("an @-weighted mix parses");

        assert_eq!(
            methods.len(),
            3,
            "one entry per method, not per schedule slot"
        );
        assert_eq!(schedule.len(), 601, "400 + 200 + 1");
        assert_eq!(schedule.iter().filter(|&&i| i == 0).count(), 400);
        assert_eq!(schedule.iter().filter(|&&i| i == 1).count(), 200);
        assert_eq!(schedule.iter().filter(|&&i| i == 2).count(), 1);

        // The weight is stripped before `parse_method`, so the read keeps both its
        // key and its `:rand48` suffix — the whole point of not reusing `:`.
        assert_eq!(methods[0].0, format!("read:{account}:rand48"));
        match &methods[0].1 {
            MethodKind::GenericRead { keys } => {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].random_bytes, 48, ":rand48 survived the weight peel");
                assert_eq!(keys[0].prefix, hex::decode(account).unwrap());
            }
            other => panic!("expected a read, got {other:?}"),
        }

        // A weight of 1 is the default, and `@` composes with the older `:weight`.
        let (_, schedule) = parse_method_spec("warp_code:3,Core_version@2").unwrap();
        assert_eq!(schedule.len(), 5);
    }

    /// The schedule must be interleaved, not blocky. This is the bug that made a
    /// 90-second seven-method soak issue 598,016 small reads and zero of anything
    /// else: each connection walks the schedule from 0 and retires after
    /// `rate*duration/clients` requests, so with a blocky schedule every
    /// connection died inside the first method's block.
    ///
    /// The property that prevents it: for every method and every prefix of the
    /// schedule, the running count is within 1 of that prefix's ideal share.
    #[test]
    fn the_schedule_spreads_each_method_across_the_whole_cycle() {
        let account = "26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9";
        let spec = format!(
            "read:{account}:rand48@800,account_nonce@100,read:cec5070d@80,\
             babe_configuration@6,Core_version@4,warp_code@4,Metadata_metadata@2"
        );
        let (methods, schedule) = parse_method_spec(&spec).expect("the mix parses");
        let weights = [800usize, 100, 80, 6, 4, 4, 2];
        let total: usize = weights.iter().sum();
        assert_eq!(methods.len(), 7);
        assert_eq!(schedule.len(), total, "996 slots");

        // Totals still honour the weights.
        for (i, &w) in weights.iter().enumerate() {
            assert_eq!(
                schedule.iter().filter(|&&s| s == i).count(),
                w,
                "method {i} appears {w} times"
            );
        }

        // Every prefix stays close to the ideal share — the spread property. The
        // bound is a small constant rather than 1: each method's own slots are
        // evenly spaced, but interleaving seven such sequences lets the dominant
        // one drift by up to about half a slot per competing method. MEASURED max
        // deviation for this mix is 1.55, so 2 is tight enough to still catch a
        // blocky schedule, which deviates by hundreds.
        let mut seen = [0usize; 7];
        for (n, &idx) in schedule.iter().enumerate() {
            seen[idx] += 1;
            let len = n + 1;
            for (i, &w) in weights.iter().enumerate() {
                let ideal = len as f64 * w as f64 / total as f64;
                assert!(
                    (seen[i] as f64 - ideal).abs() <= 2.0,
                    "prefix {len}: method {i} seen {} vs ideal {ideal:.2}",
                    seen[i]
                );
            }
        }

        // Concretely: the case that failed. A connection with a 146-request budget
        // must issue a representative mix, not 146 small reads.
        let window = &schedule[..146];
        let small_reads = window.iter().filter(|&&s| s == 0).count();
        assert!(
            (110..=125).contains(&small_reads),
            "expected ~117 small reads in 146 slots, got {small_reads}"
        );
        assert!(
            window.contains(&1),
            "account_nonce must appear within the first 146 slots"
        );
        assert!(
            window.contains(&2),
            "the mid read must appear within the first 146 slots"
        );
    }

    /// A malformed weight is a typo every time. Silently treating it as weight 1
    /// would quietly change the shape of a 12-hour run.
    #[test]
    fn malformed_at_weights_are_rejected() {
        assert!(parse_method_spec("account_nonce@").is_err(), "no digits");
        assert!(
            parse_method_spec("account_nonce@abc").is_err(),
            "not a number"
        );
        assert!(
            parse_method_spec("account_nonce@0").is_err(),
            "never issued"
        );
        assert!(parse_method_spec("account_nonce@-1").is_err(), "negative");
    }

    /// The missing-argument shapes must fail loudly, not fall through to the
    /// bare-runtime-API arm and become a call named after the hex.
    #[test]
    fn malformed_child_reads_are_rejected() {
        // No keys at all.
        assert!(parse_method("read_child:0102").is_err());
        // Keys but no trie id.
        assert!(parse_method("read_child:aabb").is_err());
        // Empty trie id.
        assert!(parse_method("read_child::aabb").is_err());
        // Odd-length hex.
        assert!(parse_method("read_child:010:aabb").is_err());
    }

    /// The old silent failure: a key separated with `,` got detached and became a
    /// runtime call named after the hex. It must now be a hard error.
    #[test]
    fn a_stray_hex_key_is_rejected_not_called_as_a_runtime_api() {
        let key = "26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9";
        let err = parse_method_spec(&format!("read:aabb,{key}"))
            .expect_err("a comma-separated key must not parse");
        assert!(
            err.to_string().contains('+'),
            "the error should point at the '+' separator, got: {err}"
        );

        // Real runtime API names are unaffected.
        assert!(parse_method_spec("Core_version").is_ok());
        assert!(parse_method_spec("Metadata_metadata").is_ok());
    }

    /// The presets are plain names, so they must still take a `:weight` suffix
    /// and compose with the rest of a mix.
    #[test]
    fn warp_sync_presets_compose_into_a_weighted_mix() {
        let (methods, schedule) =
            parse_method_spec("warp_code,babe_configuration:3,babe_current_epoch,babe_next_epoch")
                .expect("the Babe warp-sync tail parses as a mix");

        assert_eq!(methods.len(), 4);
        // 1 + 3 + 1 + 1 — the weight expands in place.
        assert_eq!(schedule.len(), 6);
        assert_eq!(methods[0].0, "warp_code");
        assert_eq!(methods[1].0, "babe_configuration");
    }
}
