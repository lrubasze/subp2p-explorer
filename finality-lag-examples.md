# `finality-lag`

Measure how long a light peer waits to learn that a block is final, and how that
wait grows with the number of light peers the node has. Reproduces
[paritytech/smoldot#3375](https://github.com/paritytech/smoldot/issues/3375).

```bash
cargo build --release -p subp2p-explorer-cli
```

## The mechanism under test

A light client learns finality only from GRANDPA *commit* messages on
`/<genesis>/grandpa/1`. The node's gossip validator
(`sc-consensus-grandpa/src/communication/gossip.rs`) does not send commits to
every light peer:

- `global_message_allowed` lets a commit go to a light peer only while the peer is
  in `lucky_light_peers`, a set of `LUCKY_PEERS = 4` light peers drawn at random.
- The set is re-drawn by `Peers::reshuffle` on every `note_round`. A full node
  runs the GRANDPA voter as a non-voter (observer mode is compiled out), so it
  notes a round each time the validators complete one: ~4.5 s on Kusama.
- `sc-network-gossip` keeps only the best commit alive and re-runs `propagate`
  for it every 750 ms. A peer that does not know the message yet is treated as a
  fresh broadcast, gated by the lucky set *as it stands now*. Net effect: every
  round, four more randomly chosen light peers get the current best commit.
- Nothing rescues a peer that was not picked. The "send to everyone" stage needs
  the round to be older than 15 s, which a healthy chain never reaches. The
  5-minute `REBROADCAST_AFTER` pass looks like a fallback but is not: `propagate`
  only keeps the periodic intent for peers that *already know* the message and
  downgrades it to a normal broadcast for the rest, where the lucky gate applies
  again (MEASURED: peers 10 min behind at 128 light peers, 16 min at 256).

So a light peer is picked with probability `4 / N` per round, and the gap between
commits reaching it — its finality lag — is geometric with mean

```
N / 4 × T_round        no cap; the p99 is 4-5x the mean
```

With Kusama's ~4.1 s rounds: 120 light peers → ~2 min mean, worst cases ~10 min.

A second detail matters for load testing: a peer only receives commits if its
neighbor-packet view is in the node's current set (`View::consider_global`).
A holder that refuses the substream, or never sends a neighbor packet, is
invisible to this mechanism and does not load it.

## Quick start

```bash
PEERS=8 DURATION=600 ./run-finality-lag                       # baseline: 8 light peers
LOAD=120 DURATION=1200 OUT_DIR=results/flag-128 ./run-finality-lag   # + 120 emulated smoldot peers
./sweep-finality-lag                                          # 8, 32, 64, 128, 256 light peers
./finality-lag-table results/flag-*
```

Direct invocation:

```bash
./target/release/subp2p-explorer-cli finality-lag \
  --url wss://kusama-rpc.polkadot.io --address /ip4/195.154.218.123/tcp/30333 \
  --peers 8 --duration 600 --out-dir results/flag-8
```

The RPC is used only for the genesis hash; `--genesis` skips it. The reference
clock for "when did the node finalize block H" is the node's own neighbor
packets, which it multicasts to every peer on each commit it imports.

## Composing with the other tools

`finality-lag` is the **probe**: a handful of peers in its own process, so its
timings are not distorted by load. The **load** comes from the existing tools,
which emulate smoldot clients:

| tool | what it emulates | touches the gossip path? |
|---|---|---|
| `hold-peers --grandpa true` (default) | N smoldot peers holding slots and following `/grandpa/1` | yes — they compete for the 4 lucky slots |
| `hold-peers --grandpa false` | pre-22-Sep-2026 holders: slot only | no |
| `soak-light`, `spam-light`, `warp-sync`, `mix-rate` | smoldot request traffic on `/light/2`, `/sync/warp` | no |

`run-finality-lag` starts the `hold-peers` load itself when `LOAD` is set and
waits for it to finish connecting before the probe's window starts. Serving load
runs in another shell, e.g. `./run-soak-light` or `./mix-rate`, exactly as in
`SCALING.md`'s mixed runs. The node's *real* light peers are on top of whatever
we add and unknown; a small-N baseline run reveals them (with N = 8 and no
outsiders, deliveries per round ≈ 4).

## Reading the output

```
=== finality-lag summary ===
peers:      8 offered as Light; 8 held block-announces and 8 grandpa at the end; refused 0 / 8 (...)
node:       41 rounds (one lucky draw each) and 24 commits in the window: a round every 4.4s, a commit every 7.3s
delivered:  118 commits reached our peers in the window = 2.88 per round (at most 4 ...); 8 of 8 peers ever got one
gap s:      between commits reaching the same peer: p50=9 p90=24 p99=29 max=37 mean=12 | predicted N/4*T_round = 9 for our 8 peers alone
lag s:      how far behind the node a peer's finalized head is, sampled every second: p50=8 p90=20 p99=32 max=45 mean=10
lag blocks: p50=0 p90=2 p99=4 max=5 mean=0.8
cap:        0 of 180 peer-samples were more than 5.5 min behind
```

- **gap s** is the headline: time between two commits reaching the same peer.
  Compare with `predicted`; with load, scale the prediction by
  `(probe + load) / probe`.
- **lag s** is what a smoldot user experiences: at any second, how stale each
  peer's finalized head is relative to the node's. Its mean ≈ gap for
  random-draw gaps.
- **starved** counts peer-samples beyond 5.5 min behind. It is not zero, which
  is the measurement that the 5-min periodic rebroadcast does not act as a cap.
- **grandpa refused** is our own early open attempt, before the node had granted
  the block-announces slot; the node then opens the substream to us.

Files with `--out-dir`: `commits.csv` (every commit delivered: peer, round,
target, delay after the node finalized it), `lag-samples.csv` (per-second
percentiles), `summary.txt` (the numbers above as key=value, for
`finality-lag-table`).

## Results (22 Sep 2026)

MEASURED against the dev-machine2 Kusama node (`polkadot 1.24.0`, ~40 full
peers, `--in-peers-light 5000`), 8 probe peers plus `hold-peers` load, hold
windows 600-1200 s. GRANDPA rounds came every 4.1 s throughout. Raw data in
`results/flag-<light peers>/`; `./finality-lag-table results/flag-*` regenerates
the table.

| light peers | predicted mean gap N/4·T | mean gap | gap p50 / p90 / p99 | finality lag p50 / p90 / max | lag in blocks p50 / max | peer-samples >5.5 min behind |
|---|---|---|---|---|---|---|
| 8   | 8 s   | 10 s  | 8 / 20 / 30 s    | 7 / 17 / 61 s    | 0 / 8  | 0% |
| 32  | 33 s  | 28 s  | 20 / 57 / 100 s  | 21 / 70 / 181 s  | 3 / 28 | 0% |
| 64  | 66 s  | 59 s  | 38 / 144 / 209 s | 48 / 152 / 344 s | 7 / 49 | 0.3% |
| 128 | 131 s | 133 s | 86 / 292 / 512 s | 98 / 319 / 603 s | 14 / 94 | 9% |
| 256 | 263 s | 250 s | 226 / 358 / 527 s | 244 / 635 / 981 s | 24 / 99 | 39% |

The mean gap tracks N/4 × T_round within 10% at every point, and the tail keeps
growing past 5 min, so the periodic rebroadcast is not a cap. This reproduces
the issue's "about 3 minutes at 120 light clients": 120/4 × 4.1 s ≈ 2 min mean,
p90 near 5 min.

## Caveats

- The measured lag is the node → light-client delivery delay only. A real
  smoldot adds verification time and, in recent versions, may warp-sync ahead
  when a neighbor packet shows a large finalized gap
  (`sync_service/substrate_compat.rs`, `neighbor_packet_outcome`), which
  shortens *large* lags but not the ordinary ones.
- Real smoldot clients connect to several nodes; the issue's setup, and this
  one, force a single node, which is what exposes the per-node lucky set.
- Holders claim genesis as their best block and never request blocks; nothing in
  the gossip path depends on that.
