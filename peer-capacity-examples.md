# `hold-peers` / `probe-announces`

Measure how many concurrent peers a full node accepts, and how its block
announcements degrade as that count grows. Both speak `/<genesis>/block-announces/1`
directly — no smoldot, no sync, no runtime execution.

`hold-peers` generates the load; `probe-announces` measures from a second, idle
process. They are meant to run as a pair.

A holder works because the node checks `--in-peers-light` / `--in-peers` when the
peer opens the block-announces substream. Opening it with the right role byte and
then doing nothing is exactly what the limit counts, so thousands of holders fit in
one process where real smoldot clients would not.

```bash
cargo build --release -p subp2p-explorer-cli
```

## The node under test

Raise the limit you are measuring first, or the run stops at the default (100
light, 32 full):

```bash
polkadot --dev --in-peers-light 2000 --port 30333 --rpc-port 9944 \
  --no-telemetry --no-prometheus --no-hardware-benchmarks -lsync=debug
```

`-lsync=debug` logs `Too many light nodes, rejecting` — the node-side cross-check
on the numbers this tool reports. `--in-peers-light` also raises the peerset's
inbound cap (the sum of both limits), so it affects full-peer runs too.

### Get the genesis hash right

**The `--dev` genesis is not fixed across builds.** `polkadot 1.22.0` runs a
`rococo_dev` chain with genesis `6178df72995de3ce5ae8b8a0215e957f6ed381a4e7c303c7cac2633454185bf9`;
older builds differ. Don't copy a hash between builds — either omit `--genesis` and
pass `--url`, or read it from the node's startup line
(`Initializing Genesis block/state (… header-hash: …)`).

> A wrong genesis looks exactly like a node at its limit: every peer connects, is
> refused, and the summary reports a ceiling of 0. The tell is node-side — a real
> limit logs one `Too many light nodes` line per refusal, a mismatch logs none.

## Quick start

```bash
PEERS=2000 DURATION=120 ./run-hold-peers      # load
DURATION=60 ./run-probe-announces             # measurement
```

Both scripts default to Kusama; uncomment the `127.0.0.1` lines for a local run.
Equivalent direct calls:

```bash
./target/release/subp2p-explorer-cli hold-peers \
  --genesis 6178df72995de3ce5ae8b8a0215e957f6ed381a4e7c303c7cac2633454185bf9 \
  --address /ip4/127.0.0.1/tcp/30333 \
  --role light --peers 2000 --ramp-ms 10 --duration 120

./target/release/subp2p-explorer-cli probe-announces \
  --url ws://127.0.0.1:9944 --address /ip4/127.0.0.1/tcp/30333 \
  --role light --peers 4 --duration 60
```

## Flags

Shared: `--chain` `-c` preset, `--address` `-a` multiaddr to dial, `--genesis` `-g`
hex hash, `--role` `-r` (`light`, `full` or `authority`; default `light`),
`--peers` `-p`, `--duration` `-d` seconds (required), `--connect-timeout` (30),
`--idle-timeout` (300).

| | `hold-peers` | `probe-announces` |
|---|---|---|
| `--url` | only to fetch genesis; skippable with `--genesis` | **required** — it is the reference clock |
| `--peers` | default `100`; the load | default `4`; keep small to stay unloaded |
| `--ramp-ms` | default `10`, gap between opening peers | — |
| `--duration` | hold window, timed from the **end of the ramp** | measurement window |

`--role` decides which limit applies: `light` hits `--in-peers-light`, `full` and
`authority` hit `--in-peers`. `--idle-timeout` governs our side only.

## Why two processes

At high peer counts thousands of holders share one runtime, so a late arrival in
`hold-peers` may be our own scheduling rather than the node's — its timings are an
upper bound. `probe-announces` holds a handful of peers and stays responsive.

With so few peers there is no useful fan-out to measure, so it uses the node's RPC
head stream as the clock. Both timestamps are taken in the same process (nothing to
synchronise) and the RPC path doesn't care how many peers the node has, so a growing
gap between "RPC reported block N" and "our peers were told about N" is node-side.
A block the RPC reported that no probe peer heard about is a dropped announcement.

## Reading the output

A live progress line runs during both phases:

```
  [hold] t=12s offered=20/20 connected=20 held=5(peak 5) refused=15 evicted=0 announces=15
```

Then the summary. This is a real run — 20 light peers against `--in-peers-light 5`:

```
=== hold-peers summary ===
target:     /ip4/127.0.0.1/tcp/30333
protocol:   /6178df72995de3ce5ae8b8a0215e957f6ed381a4e7c303c7cac2633454185bf9/block-announces/1
role:       Light (handshake byte 2)
offered:    20 peers (one every 10 ms)
connect:    0.2s to dial every peer and let the dials settle
hold:       30.0s window, timed from the end of connect
held:       peak 5 | 5 at window start | 5 at window end
outcome:    connected=20 accepted=5 refused=15 evicted=0 dial_failed=0
dial->held ms: p50=3.4 p90=3.4 p99=3.4 (over 5 accepted peers)

The node refused peers, so it is at its limit for this role. Its ceiling is 5.
announce:   40 received over 8 announcement(s) in the hold window (0 earlier one(s) skipped)
spread ms:  p50=0.1 p90=0.2 p99=0.2 max=0.2 (first to last holder, same block)
coverage:   mean 100.0% | worst 100.0% (5 of 5 holders on #17) | 0 of 8 announcement(s) incomplete
blocks:     #13..#20 | 8 seen | 0 number(s) never announced to us
```

- **`connected` ≠ `held`.** Refusal rejects the substream but leaves the connection
  up, so counting connections reports peers you don't have. Held is counted on the
  block-announces substream alone.
- **`refused`** means the node is at its limit for this role — cross-check the count
  against the node log.
- **`held: peak | start | end`** agree when you use a ramp; see the burst caveat.
- **`spread`** is fan-out for one block, first holder to last. The node loops over
  peers one at a time, so it grows with peer count. Only blocks first seen inside
  the hold window count, since the node re-announces with identical bytes.
- **`coverage`** below 100% means dropped announcements — the send is
  fire-and-forget, so a peer with a full buffer is skipped silently.

`probe-announces` against an idle node, the baseline for comparison:

```
=== probe-announces summary ===
target:     /ip4/127.0.0.1/tcp/30333
probe:      4 held peer(s) over 25.0s
blocks:     7 observed in the window
rpc->p2p ms: p50=0.1 p90=0.2 p99=0.2 max=0.4 (over 4 block(s))
spread ms:  p50=0.1 max=0.2 (first to last probe peer)
dropped:    0 block(s) never announced to any probe peer
partial:    0 block(s) reached some but not all probe peers
per block:
  #20: 4/4 peer(s) | not seen on RPC
  #21: 4/4 peer(s) | not seen on RPC
  #22: 4/4 peer(s) | not seen on RPC
  #23: 4/4 peer(s) | 0.1ms after RPC
  #24: 4/4 peer(s) | 0.2ms after RPC
  #25: 4/4 peer(s) | 0.1ms after RPC
  #26: 4/4 peer(s) | 0.4ms after RPC
```

- **`rpc->p2p`** is the headline: how far the p2p announcement trailed the RPC
  notification. Compare idle against loaded.
- **`spread`** across even 4 peers samples how long the node's announce loop takes,
  since those peers sit at scattered positions in it.
- The first few blocks may print `not seen on RPC` (3 of 7 above) — the pre-window
  drain can discard an RPC notification whose announcement lands just after the
  window opens. They're excluded from the lag stats, hence `7 observed` but
  `over 4 block(s)`. Use a longer `--duration` so they're a small fraction.

## Limits and caveats

- **10,000 incoming connections is a hard ceiling** —
  `MAX_CONNECTIONS_ESTABLISHED_INCOMING`, a `const` in
  `substrate/client/network/src/lib.rs` with no CLI flag. Past it the dialer sees
  `Handshake failed: unexpected end of file`. Going further means patching and
  rebuilding the node.
- **Keep a ramp.** With `--ramp-ms 0` the node admits the whole burst before its
  slot accounting catches up — 50 full peers were all admitted against
  `--in-peers 32`, then converged to 32. For a burst run read the steady value, not
  the peak. That the overshoot depends on the *light* setting even for full peers
  looks wrong and is an open question against polkadot-sdk.
- **`--idle-timeout` can't keep a refused peer connected.** The node reaps a
  connection with no keep-alive substream after its own ~10 s timeout regardless.
- **Incomplete coverage at high peer counts is not reproducible yet.** Identical
  settings have produced both complete and incomplete coverage, so treat any single
  run as provisional.

## Related

`examples.md` and `light-request-capacity-test-plan.md` cover the request-serving
half (`spam-light` / `soak-light`), which loads the `/light/2` handler. Those two
never become peers at all — no block-announces substream, so the node doesn't count
them and they get no announcements. The two halves measure independent limits.
