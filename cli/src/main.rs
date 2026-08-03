// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

mod commands;
mod utils;

use clap::Parser as ClapParser;
use commands::{
    authorities::{discover_authorities, resolve_bootnodes},
    authority_check::check_authorities,
    bootnodes::verify_bootnodes,
    dial_peer::dial_peer,
    discover_peer::discover_peer,
    discovery::discover_network,
    extrinsics::submit_extrinsics,
    hold_peers::{hold_peers, HoldRole},
    light_common::Chain,
    light_spam::spam_light,
    probe_announces::probe_announces,
    soak_light::soak_light,
};
use jsonrpsee::client_transport::ws::Url;
use std::{error::Error, io::Read, path::PathBuf};

/// Command for interacting with the CLI.
#[derive(Debug, ClapParser)]
enum Command {
    Authorities(Authorities),
    AuthorityCheck(AuthorityCheckOpts),
    DialPeer(DialPeerOpts),
    SendExtrinisic(SendExtrinisicOpts),
    DiscoverNetwork(DiscoverNetworkOpts),
    DiscoverPeer(DiscoverPeerOpts),
    VerifyBootnodes(BootnodesOpts),
    SpamLight(SpamLightOpts),
    SoakLight(SoakLightOpts),
    HoldPeers(HoldPeersOpts),
    ProbeAnnounces(ProbeAnnouncesOpts),
}

/// Measure block-announcement quality from a small, unloaded process.
///
/// Run this alongside a `hold-peers` process: that one generates the peer load,
/// this one measures. Because it lives in its own process with only a handful of
/// peers, its timings are not distorted by the load.
///
/// The node's RPC head stream is the reference clock. Both timestamps are taken
/// here, so no clock synchronisation is needed, and the RPC path does not care how
/// many peers the node has. A growing gap between "RPC reported block N" and "our
/// peers were told about block N" is node-side degradation. A block the RPC
/// reported but no probe peer was told about is a dropped announcement.
#[derive(Debug, ClapParser)]
pub struct ProbeAnnouncesOpts {
    /// Chain preset (supplies a default p2p host and RPC url).
    #[clap(long, short, value_enum)]
    chain: Option<Chain>,
    /// RPC endpoint. Required: it is the reference clock, not just a genesis
    /// lookup.
    #[clap(long, short)]
    url: Option<String>,
    /// Multiaddress of the full node to dial. Required unless --chain.
    #[clap(long, short)]
    address: Option<String>,
    /// Hex-encoded genesis hash. Fetched from the RPC if omitted.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Role the probe peers advertise.
    #[clap(long, short, value_enum, default_value = "light")]
    role: HoldRole,
    /// Number of probe peers. Keep this small — the point is to stay unloaded.
    #[clap(long, short, default_value = "4")]
    peers: usize,
    /// How long to measure, in seconds.
    #[clap(long, short, value_parser = parse_duration)]
    duration: std::time::Duration,
    /// How long we keep a connection with no open substream, in seconds.
    #[clap(long, default_value = "300", value_parser = parse_duration)]
    idle_timeout: std::time::Duration,
    /// Grace period, in seconds, for the probe peers to connect before measuring.
    #[clap(long, default_value = "30", value_parser = parse_duration)]
    connect_timeout: std::time::Duration,
}

/// Open many cheap fake peers against one full node and hold them, to measure how
/// many concurrent peers the node accepts.
///
/// The node gates inbound light peers with `--in-peers-light` and inbound full
/// peers with `--in-peers`, both checked when the peer opens the block-announces
/// substream. A holder opens that substream with the requested role and then does
/// nothing, which is all the node's limit actually counts — so thousands of
/// holders fit in one process, where real smoldot clients would not.
///
/// Needs no live chain state: pass `--genesis` and it runs without any RPC.
#[derive(Debug, ClapParser)]
pub struct HoldPeersOpts {
    /// Chain preset (supplies a default p2p host and RPC url).
    #[clap(long, short, value_enum)]
    chain: Option<Chain>,
    /// RPC endpoint, used only to fetch the genesis hash. Not needed if
    /// --genesis is given.
    #[clap(long, short)]
    url: Option<String>,
    /// Multiaddress of the full node to dial. Required unless --chain.
    #[clap(long, short)]
    address: Option<String>,
    /// Hex-encoded genesis hash. Fetched from the RPC if omitted.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Role each holder advertises. This decides which of the node's limits
    /// applies: light hits --in-peers-light, full and authority hit --in-peers.
    #[clap(long, short, value_enum, default_value = "light")]
    role: HoldRole,
    /// Number of concurrent peers to open and hold.
    #[clap(long, short, default_value = "100")]
    peers: usize,
    /// Gap between opening peers, in milliseconds. 0 opens them all at once.
    #[clap(long, default_value = "10")]
    ramp_ms: u64,
    /// How long to hold, in seconds. Timed from the moment every peer has
    /// connected, not from process start, so a long ramp does not eat into it.
    #[clap(long, short, value_parser = parse_duration)]
    duration: std::time::Duration,
    /// Grace period, in seconds, for dials to settle after the last peer was
    /// opened. Once it expires the hold window starts regardless, so a dial that
    /// never connects or fails cannot stall the run.
    #[clap(long, default_value = "30", value_parser = parse_duration)]
    connect_timeout: std::time::Duration,
    /// How long we keep a connection with no open substream, in seconds. This
    /// governs our side only: the node reaps a refused peer's connection after
    /// its own idle timeout (10s by default), whatever this is set to.
    #[clap(long, default_value = "300", value_parser = parse_duration)]
    idle_timeout: std::time::Duration,
    /// Directory to also write machine-readable results into, for plotting:
    /// hold-samples.csv (one row per second) and hold-blocks.csv (one row per
    /// announcement). Created if missing.
    #[clap(long)]
    out_dir: Option<std::path::PathBuf>,
}

/// Spam the `/<genesis>/light/2` request-response protocol of an appointed full
/// node to load-test its light-client request handler.
///
/// Dials the host directly (no discovery, no smoldot sync), learns its peer id
/// during the handshake, and issues `RemoteCallRequest`s with a bounded
/// in-flight window. The execution block is kept fresh via an RPC subscription
/// to the finalized head.
#[derive(Debug, ClapParser)]
pub struct SpamLightOpts {
    /// Chain preset: fills in the RPC url, a default p2p host, and a default
    /// method mix. Any of those can still be overridden by the flags below.
    #[clap(long, short, value_enum)]
    chain: Option<Chain>,
    /// The URL of the chain RPC endpoint (fetches genesis + tracks the head).
    /// Required unless --chain is given.
    #[clap(long, short)]
    url: Option<String>,
    /// Multiaddress of the full node to dial. The peer id is optional and is
    /// learned during the noise handshake. Required unless --chain is given.
    ///
    /// For example, "/dns4/paseo-bulletin-next-rpc-node-0.polkadot.io/tcp/443/wss".
    #[clap(long, short)]
    address: Option<String>,
    /// Hex-encoded genesis hash of the chain. Fetched from the RPC if omitted.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Pin a hex-encoded block hash to execute against (disables the head
    /// subscription). Defaults to tracking the finalized head.
    #[clap(long, short)]
    block: Option<String>,
    /// Override the full light protocol name (e.g. for chains with a fork id:
    /// "/<genesis>/<fork_id>/light/2"). Defaults to "/<genesis>/light/2".
    #[clap(long, short)]
    protocol: Option<String>,
    /// Method mix to spam, comma-separated with optional ":weight". Names:
    /// account_nonce, can_store, account_authorization, indexed_transactions,
    /// revive_get_storage; generic "call:<method>:<hexdata>" /
    /// "read:<hexkey>[+<hexkey>…]" (keys joined with '+', since ',' separates
    /// methods);
    /// or a bare runtime-API name (no args, e.g. Core_version). Defaults to the
    /// chain preset, else account_nonce.
    ///
    /// The `/light/2` half of a smoldot warp sync is also available as presets:
    /// warp_code (the runtime download — one request, ~1.65 MiB of response on
    /// Kusama), plus the consensus calls babe_configuration, babe_current_epoch,
    /// babe_next_epoch (Babe chains) or aura_slot_duration, aura_authorities
    /// (Aura chains, i.e. most parachains). A whole warp-sync tail for a Babe
    /// chain is
    /// "warp_code,babe_configuration,babe_current_epoch,babe_next_epoch".
    #[clap(long, short)]
    method: Option<String>,
    /// Total number of requests to issue.
    #[clap(long, default_value = "100")]
    count: usize,
    /// Maximum in-flight requests per connection (the spam window).
    #[clap(long, default_value = "8")]
    concurrency: usize,
    /// Number of independent connections, each with its own PeerId. Each runs the
    /// full --count at --concurrency, so total load = connections × count.
    #[clap(long, default_value = "1")]
    connections: usize,
    /// Delay in milliseconds between opening successive connections (smooths the
    /// dial/TLS burst for large --connections). 0 = all at once.
    #[clap(long, default_value = "0")]
    stagger_ms: u64,
    /// Per-request timeout in seconds (counted separately as the saturation signal).
    #[clap(long, default_value = "10", value_parser = parse_duration)]
    request_timeout: std::time::Duration,
    /// Overall wall-clock timeout in seconds for the whole run.
    #[clap(long, short, default_value = "120", value_parser = parse_duration)]
    timeout: std::time::Duration,
}

/// Sustained-rate / long-duration soak test for `/<genesis>/light/2`.
///
/// Open-loop: offers `--rate` req/s for `--duration`, opening `--clients` total
/// connections over the run (each a fresh identity, 1 request in flight, closed
/// after its derived share of the load). Verifies a node stays healthy under
/// sustained (possibly over-capacity) load with realistic client churn.
#[derive(Debug, ClapParser)]
pub struct SoakLightOpts {
    /// Chain preset (RPC url, default p2p host, default method mix).
    #[clap(long, short, value_enum)]
    chain: Option<Chain>,
    /// RPC endpoint (fetches genesis + tracks the head). Required unless --chain.
    #[clap(long, short)]
    url: Option<String>,
    /// Multiaddress of the full node to dial. Required unless --chain.
    #[clap(long, short)]
    address: Option<String>,
    /// Hex-encoded genesis hash. Fetched from the RPC if omitted.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Pin a hex-encoded execution block hash (disables head subscription).
    #[clap(long, short)]
    block: Option<String>,
    /// Override the full light protocol name (e.g. fork-id chains).
    #[clap(long, short)]
    protocol: Option<String>,
    /// Method mix (see `spam-light --help` for the syntax). Defaults to the chain
    /// preset, else account_nonce.
    #[clap(long, short)]
    method: Option<String>,
    /// Offered request rate (requests/second).
    #[clap(long, short)]
    rate: u64,
    /// How long to run, in seconds.
    #[clap(long, short, value_parser = parse_duration)]
    duration: std::time::Duration,
    /// Total number of connections opened over the run (cumulative, not
    /// concurrent). Per-connection request budget = rate*duration/clients.
    #[clap(long)]
    clients: usize,
    /// Safety cap on simultaneous connections (backstop only).
    #[clap(long, default_value = "8192")]
    max_concurrent: usize,
    /// Per-request timeout in seconds.
    #[clap(long, default_value = "10", value_parser = parse_duration)]
    request_timeout: std::time::Duration,
}

/// Discover the authorities of the p2p network.
#[derive(Debug, ClapParser)]
pub struct Authorities {
    /// The URL of the chain RPC endpoint.
    #[clap(long, short)]
    url: String,
    /// Hex-encoded genesis hash of the chain.
    ///
    /// For example, "781e4046b4e8b5e83d33dde04b32e7cb5d43344b1f19b574f6d31cbbd99fe738"
    #[clap(long, short)]
    genesis: String,
    /// Bootnodes of the chain, must contain a multiaddress together with the peer ID.
    /// For example, "/ip4/127.0.0.1/tcp/30333/ws/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".
    #[clap(long, use_value_delimiter = true, value_parser)]
    bootnodes: Vec<String>,
    /// The number of seconds the authority discovery process should run for.
    #[clap(long, short, value_parser = parse_duration)]
    timeout: std::time::Duration,
    /// The address format name of the chain.
    /// Used to display the SS58 address of the authorities.
    ///
    /// For example:
    /// - "polkadot" for Polkadot
    /// - "substrate" for Substrate
    /// - "kusama" for Kusama
    #[clap(long, short)]
    address_format: String,
    /// Print the raw identity list of discovered peers.
    #[clap(long, short)]
    raw_output: bool,
    /// The number of seconds for each individual Kademlia DHT query before it is
    /// considered failed. Lower values free up query slots faster when records
    /// do not exist in the DHT.
    #[clap(long, default_value = "15", value_parser = parse_duration)]
    query_timeout: std::time::Duration,
}

/// Check authority health: discover DHT records, test connectivity per address,
/// and report per-authority and global statistics.
#[derive(Debug, ClapParser)]
pub struct AuthorityCheckOpts {
    /// The URL of the chain RPC endpoint.
    #[clap(long, short)]
    url: String,
    /// Hex-encoded genesis hash of the chain.
    ///
    /// If not provided, the genesis hash is fetched from the RPC endpoint.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Bootnodes of the chain, must contain a multiaddress together with the peer ID.
    ///
    /// If not provided, bootnodes are fetched from the chain spec via the RPC endpoint.
    #[clap(long, use_value_delimiter = true, value_parser)]
    bootnodes: Vec<String>,
    /// The number of seconds for DHT discovery.
    #[clap(long, short, value_parser = parse_duration)]
    timeout: std::time::Duration,
    /// The number of seconds to wait for each individual TCP connection check.
    #[clap(long, short = 'd', default_value = "10", value_parser = parse_duration)]
    dial_timeout: std::time::Duration,
    /// The address format name of the chain (e.g., "polkadot", "kusama").
    ///
    /// If not provided, the SS58 prefix is fetched from the RPC endpoint.
    #[clap(long, short)]
    address_format: Option<String>,
    /// The number of seconds for each individual Kademlia DHT query before it is
    /// considered failed. Lower values free up query slots faster when records
    /// do not exist in the DHT.
    #[clap(long, default_value = "15", value_parser = parse_duration)]
    query_timeout: std::time::Duration,
    /// The RPC endpoint of the chain that hosts on-chain identities
    /// (e.g., the People parachain `wss://polkadot-people-rpc.polkadot.io`).
    ///
    /// When provided, authority display names are resolved from the Identity
    /// pallet on that chain. If omitted, identities are looked up on the
    /// relay chain itself.
    #[clap(long)]
    identity_rpc: Option<String>,
    /// Show only authorities that have failures (no DHT record, unreachable
    /// public addresses, or no public addresses at all).
    #[clap(long)]
    show_failing_only: bool,
    /// Write the full results as a JSON report to the given file path.
    #[clap(long)]
    json: Option<PathBuf>,
}

/// Dial one or more multiaddresses and fetch the identify message from each peer.
#[derive(Debug, ClapParser)]
pub struct DialPeerOpts {
    /// Multiaddresses to dial.
    ///
    /// For example, "/ip4/35.75.15.11/tcp/30333" or
    /// "/dns/example.com/tcp/30333/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".
    #[clap(long, use_value_delimiter = true, value_parser)]
    address: Vec<String>,
    /// The number of seconds to wait for responses before giving up.
    #[clap(long, short, default_value = "30", value_parser = parse_duration)]
    timeout: std::time::Duration,
}

/// Send extrinsic on the p2p network.
#[derive(Debug, ClapParser)]
pub struct SendExtrinisicOpts {
    /// Hex-encoded genesis hash of the chain.
    ///
    /// For example, "781e4046b4e8b5e83d33dde04b32e7cb5d43344b1f19b574f6d31cbbd99fe738"
    #[clap(long, short)]
    genesis: String,
    /// Bootnodes of the chain, must contain a multiaddress together with the peer ID.
    /// For example, "/ip4/127.0.0.1/tcp/30333/ws/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".
    #[clap(long, use_value_delimiter = true, value_parser)]
    bootnodes: Vec<String>,
    /// Hex-encoded scale-encoded vector of extrinsics to submit to peers.
    #[clap(long, short)]
    extrinsics: String,
}

/// Discover the p2p network.
#[derive(Debug, ClapParser)]
pub struct DiscoverNetworkOpts {
    /// The URL of the chain RPC endpoint.
    #[clap(long, short)]
    url: String,
    /// Hex-encoded genesis hash of the chain.
    ///
    /// If not provided, the genesis hash is fetched from the RPC endpoint.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Bootnodes of the chain, must contain a multiaddress together with the peer ID.
    ///
    /// If not provided, bootnodes are fetched from the chain spec via the RPC endpoint.
    #[clap(long, use_value_delimiter = true, value_parser)]
    bootnodes: Vec<String>,
    /// The number of cities to print in decreasing order by the number of peers.
    ///
    /// Defaults to 10.
    #[clap(long, short)]
    cities: Option<usize>,
    /// Print the raw list of peers with geolocation.
    #[clap(long, short)]
    raw_geolocation: bool,
    /// Show only authorities.
    #[clap(long, short)]
    only_authorities: bool,
    /// Print every peer that responded to the identify protocol, along with
    /// its agent version and announced role (if any).
    #[clap(long)]
    identified: bool,
    /// The number of seconds the discovery process should run for.
    #[clap(long, short, value_parser = parse_duration)]
    timeout: std::time::Duration,
    /// The number of seconds for each individual Kademlia DHT query before it is
    /// considered failed. Lower values free up query slots faster when records
    /// do not exist in the DHT.
    #[clap(long, default_value = "15", value_parser = parse_duration)]
    query_timeout: std::time::Duration,
}

/// Discover a single peer on the p2p network.
///
/// Performs aggressive Kademlia `get-closest-peers` queries keyed exclusively
/// on the target peer ID, force-dialing every peer surfaced along the way
/// until the target is identified or the timeout fires.
#[derive(Debug, ClapParser)]
pub struct DiscoverPeerOpts {
    /// The URL of the chain RPC endpoint.
    #[clap(long, short)]
    url: String,
    /// The target peer ID to hunt (e.g. "12D3KooW...").
    #[clap(long)]
    peer: String,
    /// Hex-encoded genesis hash of the chain.
    ///
    /// If not provided, the genesis hash is fetched from the RPC endpoint.
    #[clap(long, short)]
    genesis: Option<String>,
    /// Bootnodes of the chain, must contain a multiaddress together with the peer ID.
    ///
    /// If not provided, bootnodes are fetched from the chain spec via the RPC endpoint.
    #[clap(long, use_value_delimiter = true, value_parser)]
    bootnodes: Vec<String>,
    /// Print every peer that responded to the identify protocol, along with
    /// its agent version and announced role (if any).
    #[clap(long)]
    identified: bool,
    /// The number of seconds the discovery process should run for.
    #[clap(long, short, value_parser = parse_duration)]
    timeout: std::time::Duration,
    /// The number of seconds for each individual Kademlia DHT query before it is
    /// considered failed. Lower values free up query slots faster when records
    /// do not exist in the DHT.
    #[clap(long, default_value = "15", value_parser = parse_duration)]
    query_timeout: std::time::Duration,
}

fn parse_duration(arg: &str) -> Result<std::time::Duration, std::num::ParseIntError> {
    let seconds = arg.parse()?;
    Ok(std::time::Duration::from_secs(seconds))
}

/// Verify bootnodes are reachable on the p2p network.
///
/// This will attempt to connect ot each provided bootnode and
#[derive(Debug, ClapParser)]
pub struct BootnodesOpts {
    /// Bootnodes of the chain, must contain a multiaddress together with the peer ID.
    ///
    /// For example, "/ip4/127.0.0.1/tcp/30333/ws/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".
    #[clap(long, use_value_delimiter = true, value_parser)]
    bootnodes: Vec<String>,
    /// Hex-encoded genesis hash of the chain.
    ///
    /// When this is provided, the supported p2p protocols of the bootnodes will be
    /// verified against the provided genesis hash.
    ///
    /// For example, "781e4046b4e8b5e83d33dde04b32e7cb5d43344b1f19b574f6d31cbbd99fe738"
    #[clap(long, short)]
    genesis: Option<String>,

    /// Verify the bootnodes using the provided chain spec.
    ///
    /// This is incompatible with `--bootnodes`.
    #[clap(long, value_parser)]
    chain_spec: Option<PathBuf>,
}

impl BootnodesOpts {
    /// Verify the bootnodes.
    pub async fn verify_bootnodes(&self) -> Result<(), Box<dyn Error>> {
        match (&self.bootnodes, &self.genesis, &self.chain_spec) {
            (bootnodes, _, Some(_)) if !bootnodes.is_empty() => {
                Err("`--bootnodes` is incompatible with `--chain-spec`".into())
            }
            (bootnodes, _, None) => verify_bootnodes(bootnodes.clone(), self.genesis.clone()).await,
            (_, genesis, Some(spec)) => {
                let mut file = std::fs::File::open(spec)?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;

                let spec = serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map_err(|e| format!("Invalid chain spec: {}", e))?;

                let bootnodes = spec
                    .get("bootNodes")
                    .ok_or("Missing `bootNodes`")?
                    .as_array()
                    .ok_or("Invalid `bootNodes` format, expected array")?
                    .iter()
                    .map(|node| {
                        node.as_str()
                            .map(|s| s.to_string())
                            .ok_or("Invalid `bootNodes` format, expected string")
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                verify_bootnodes(bootnodes, genesis.clone()).await
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Command::parse();
    match args {
        Command::SendExtrinisic(opts) => {
            submit_extrinsics(opts.genesis, opts.bootnodes, opts.extrinsics).await
        }
        Command::DiscoverNetwork(opts) => {
            discover_network(
                opts.url,
                opts.genesis,
                opts.bootnodes,
                opts.cities,
                opts.raw_geolocation,
                opts.only_authorities,
                opts.identified,
                opts.timeout,
                opts.query_timeout,
            )
            .await
        }
        Command::DiscoverPeer(opts) => {
            discover_peer(
                opts.url,
                opts.peer,
                opts.genesis,
                opts.bootnodes,
                opts.identified,
                opts.timeout,
                opts.query_timeout,
            )
            .await
        }
        Command::DialPeer(opts) => dial_peer(opts.address, opts.timeout).await,
        Command::SpamLight(opts) => {
            spam_light(
                opts.chain,
                opts.url,
                opts.address,
                opts.genesis,
                opts.block,
                opts.protocol,
                opts.method,
                opts.count,
                opts.concurrency,
                opts.connections,
                opts.stagger_ms,
                opts.request_timeout,
                opts.timeout,
            )
            .await
        }
        Command::SoakLight(opts) => {
            soak_light(
                opts.chain,
                opts.url,
                opts.address,
                opts.genesis,
                opts.block,
                opts.protocol,
                opts.method,
                opts.rate,
                opts.duration,
                opts.clients,
                opts.max_concurrent,
                opts.request_timeout,
            )
            .await
        }
        Command::HoldPeers(opts) => {
            hold_peers(
                opts.chain,
                opts.url,
                opts.address,
                opts.genesis,
                opts.role,
                opts.peers,
                opts.ramp_ms,
                opts.duration,
                opts.idle_timeout,
                opts.connect_timeout,
                opts.out_dir,
            )
            .await
        }
        Command::ProbeAnnounces(opts) => {
            probe_announces(
                opts.chain,
                opts.url,
                opts.address,
                opts.genesis,
                opts.role,
                opts.peers,
                opts.duration,
                opts.idle_timeout,
                opts.connect_timeout,
            )
            .await
        }
        Command::VerifyBootnodes(opts) => opts.verify_bootnodes().await,
        Command::Authorities(opts) => {
            let rpc_url = Url::parse(&opts.url)?;
            let bootnodes =
                resolve_bootnodes(&rpc_url, opts.bootnodes, &mut std::io::stdout()).await?;
            discover_authorities(
                opts.url,
                opts.genesis,
                bootnodes,
                opts.timeout,
                opts.address_format,
                opts.raw_output,
                opts.query_timeout,
            )
            .await
            .map(|_| ())
        }
        Command::AuthorityCheck(opts) => {
            check_authorities(
                opts.url,
                opts.genesis,
                opts.bootnodes,
                opts.timeout,
                opts.dial_timeout,
                opts.address_format,
                opts.query_timeout,
                opts.identity_rpc,
                opts.show_failing_only,
                opts.json,
            )
            .await
        }
    }
}
