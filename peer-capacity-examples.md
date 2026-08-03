# `hold-peers` / `probe-announces` — examples

Measure how many concurrent peers a full node accepts, and how the quality of its
block announcements degrades as that count grows. Both commands speak the raw
`/<genesis>/block-announces/1` notification protocol directly — **no smoldot, no
sync, no runtime execution**.

`hold-peers` generates the load. `probe-announces` measures it from a second,
deliberately idle process. They are designed to run as a pair.

> Why a fake peer is enough: the node gates inbound light peers with
> `--in-peers-light` and inbound full peers with `--in-peers`, and **both limits
> are checked when the peer opens the block-announces substream**. A peer that
> opens that substream with the right role byte and then does nothing is exactly
> what the limit counts. That makes a holder far cheaper than a real smoldot
> client, which needs warp sync and runtime execution before it becomes a peer at
> all — a prior smoldot run configured for 1000 clients only ever held ~525 at
> once, so true concurrency was never demonstrated.

Each holder is an independent swarm with its own libp2p identity running the
`Notifications` behaviour alone — no discovery, no ping, no identify. An open
notification substream is enough to keep the connection alive, so a holder only
has to stay polled and drop whatever the node announces to it.

Build once:

```bash
cargo build --release -p subp2p-explorer-cli
# binary: ./target/release/subp2p-explorer-cli
```

---

## The node under test

**Raise the limit you are measuring first**, or the run just stops at the default
(100 light, 32 full):

```bash
polkadot --dev --in-peers-light 2000 --port 30333 --rpc-port 9944 \
  --no-telemetry --no-prometheus --no-hardware-benchmarks -lsync=debug
```

`-lsync=debug` is what logs `Too many light nodes, rejecting`, the node-side
cross-check on the numbers this tool reports. Note that `--in-peers-light` raises
the peerset's inbound cap too, which is the sum of the two limits — so it affects
full-peer runs as well.

### Get the genesis hash right

`hold-peers` needs no live chain state, so `--genesis` lets a run happen with no
RPC at all. But **the `--dev` genesis is not a fixed constant across builds** — it
depends on the chain spec that binary's `--dev` resolves to, which has changed.
`polkadot 1.22.0` runs a `rococo_dev` chain with genesis
`6178df72995de3ce5ae8b8a0215e957f6ed381a4e7c303c7cac2633454185bf9`, while an older
build documented `4969df70…`. Don't copy a hash between builds.

Either let the tool fetch it — omit `--genesis` and pass `--url ws://127.0.0.1:9944`
instead — or read it from the node once:

```bash
curl -s -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"chain_getBlockHash","params":[0]}' \
  http://127.0.0.1:9944
```

The node also prints it at startup: `Initializing Genesis block/state (… header-hash: 0x6178…5bf9)`.

> **A wrong genesis looks exactly like a node at its limit.** Every peer connects
> and is then refused, so you get `held=0 refused=<all>` and the summary confidently
> reports a ceiling of 0. The tell is the node side: a genuine limit logs one
> `Too many light nodes, rejecting` per refusal, and a genesis mismatch logs **none**
> (it is rejected before the light gate, and unlike an ordinary refusal it does
> carry a reputation penalty). Always cross-check the counts against that log line.

---

## Quick start

Via the wrapper scripts (every knob is an env var):

```bash
PEERS=2000 DURATION=120 ./run-hold-peers      # generates the load
DURATION=60 ./run-probe-announces             # measures, in its own process
```

**Both scripts default to Kusama.** For a local run, uncomment the `127.0.0.1`
lines at the top and comment out the Kusama pair.

Equivalent direct calls:

```bash
# 2000 light peers against a local dev node, no RPC needed (genesis is this
# binary's --dev hash; see "Get the genesis hash right" below)
./target/release/subp2p-explorer-cli hold-peers \
  --genesis 6178df72995de3ce5ae8b8a0215e957f6ed381a4e7c303c7cac2633454185bf9 \
  --address /ip4/127.0.0.1/tcp/30333 \
  --role light --peers 2000 --ramp-ms 10 --duration 120

# 4 probe peers measuring the same node while the above runs
./target/release/subp2p-explorer-cli probe-announces \
  --url ws://127.0.0.1:9944 \
  --address /ip4/127.0.0.1/tcp/30333 \
  --role light --peers 4 --duration 60
```

---

## Flags: `hold-peers`

| flag | short | default | meaning |
|---|---|---|---|
| `--chain` | `-c` | — | preset supplying a default p2p host and RPC url |
| `--url` | `-u` | preset | RPC endpoint, **only** to fetch the genesis hash; unused if `--genesis` is given |
| `--address` | `-a` | preset | multiaddr of the node to dial; required unless `--chain` |
| `--genesis` | `-g` | from RPC | hex genesis hash; pass it and the command needs no RPC at all |
| `--role` | `-r` | `light` | `light` \| `full` \| `authority`. **Decides which limit applies:** `light` hits `--in-peers-light`, the other two hit `--in-peers` |
| `--peers` | `-p` | `100` | concurrent peers to open and hold |
| `--ramp-ms` | | `10` | gap between opening peers; `0` opens them all at once |
| `--duration` | `-d` | *required* | hold window in seconds, timed from the **end of the ramp** so a long ramp does not eat into it |
| `--connect-timeout` | | `30` | grace period for dials to settle after the last peer was opened; once it expires the hold window starts regardless, so a dial that never resolves cannot stall the run |
| `--idle-timeout` | | `300` | how long *we* keep a connection with no open substream. Our side only — see the caveat below |

## Flags: `probe-announces`

| flag | short | default | meaning |
|---|---|---|---|
| `--chain` | `-c` | — | preset supplying a default p2p host and RPC url |
| `--url` | `-u` | preset | RPC endpoint. **Required** — it is the reference clock, not just a genesis lookup |
| `--address` | `-a` | preset | multiaddr of the node to dial; required unless `--chain` |
| `--genesis` | `-g` | from RPC | hex genesis hash; skips the genesis fetch only, the RPC is still used |
| `--role` | `-r` | `light` | role the probe peers advertise |
| `--peers` | `-p` | `4` | probe peers. **Keep this small** — the point is to stay unloaded |
| `--duration` | `-d` | *required* | measurement window in seconds |
| `--connect-timeout` | | `30` | grace period for the probe peers to connect before measuring starts |
| `--idle-timeout` | | `300` | how long we keep a connection with no open substream |

---

## Why the second process exists

`hold-peers` reports spread and coverage itself, but at high peer counts thousands
of holders share one tokio runtime, so a late arrival may be **our own scheduling
rather than the node's**. Those timings are an upper bound, not a measurement.

`probe-announces` holds only a handful of peers, so it stays responsive however
loaded the other process is. With so few peers there is no useful fan-out spread
to measure, so the reference clock comes from elsewhere: **the node's own RPC head
stream**. Both timestamps are taken in the same process, so nothing needs
synchronising, and the RPC path is indifferent to how many peers the node has. A
growing gap between "RPC reported block N" and "our peers were told about N" is
therefore node-side degradation.

A block the RPC reported that no probe peer was ever told about is a **dropped
announcement**. Few peers is the sensitive configuration here, because the node
drops per peer, so only one of yours has to miss it.

---

## Reading the output — `hold-peers`

A live progress line during both phases:

```
  [hold] t=42s offered=2000/2000 connected=2000 held=2000(peak 2000) refused=0 evicted=0 announces=13847
```

Then the summary. This is a real run — 20 light peers against
`--in-peers-light 5`, which is also the tool's own correctness check:

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

- **`connected` vs `accepted`/`held`** — these are genuinely different states. The
  node refuses the *substream* but **leaves the connection up**, so counting
  connections would report peers you do not actually have. Held is counted on the
  block-announces substream alone.
- **`refused`** — the node was at its limit for this role. Cross-check against
  `Too many light nodes, rejecting` in the node log; the counts should match
  exactly. There is **no reputation penalty** for refusal.
- **`evicted`** — a peer that was held and then lost. Note the node reaps a
  *refused* peer's connection after its own idle timeout regardless of
  `--idle-timeout`.
- **`held: peak | start | end`** — with a ramp these agree. With `--ramp-ms 0`
  they do not; see the burst caveat below.
- **`dial->held`** — how long from dialling to holding the substream, over
  accepted peers only.
- **`announce` / `spread`** — fan-out timing for the same block, first holder to
  last. The node announces by looping over peers one at a time, so spread grows
  with peer count. Only blocks **first seen inside the hold window** are counted:
  grouping is by message bytes and the node re-announces with identical bytes, so
  a block first seen during the ramp would merge two rounds seconds apart.
- **`coverage`** — holders that got the block over holders held when it arrived.
  Below 100% means dropped announcements: the send is fire-and-forget, so a peer
  whose buffer is full is skipped silently.
- **The closing sentence** interprets the run — either the node hit its ceiling
  (and names it) or every offered peer was held (so the limit is higher than you
  offered).

## Reading the output — `probe-announces`

A real run against an idle dev node — this is the baseline you compare a loaded
run against:

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

- **`rpc->p2p`** — the headline. How far the p2p announcement trailed the RPC
  notification for the same block. Compare an idle run against a loaded one; the
  growth is the node's announcement delay.
- **`spread`** — informative even across only 4 peers, because those peers sit at
  scattered positions in the node's per-peer announce loop, so the first-to-last
  gap samples how long that loop takes.
- **`dropped`** — the RPC reported it, no probe peer heard about it.
- **`partial`** — reached some but not all probe peers.
- Blocks in the last 500 ms of the window are excluded, so an announcement still
  in flight when measuring stops is not miscounted as dropped.
- **Known cosmetic artifact:** the first few blocks of a window can print
  `not seen on RPC` — 3 of 7 in the run above — because the pre-window drain can
  discard an RPC notification whose p2p announcement lands just after the window
  opens. Those blocks are excluded from the lag statistics, which is why
  `blocks: 7 observed` but the lag line says `over 4 block(s)`. Use a longer
  `--duration` so they are a small fraction of the sample.

---

## What has been measured

Against polkadot-sdk @ `513c7971` with the litep2p backend, on a local `--dev`
node unless stated otherwise. The role and announcement-quality tables were later
spot-checked against `polkadot 1.22.0` and held; the announcement-quality figures
below have not been re-run in full, so treat them as version-specific.

**The role byte lands correctly.** The decisive test is differential — same tool,
same node, only the role changed:

| setup | result |
|---|---|
| `--in-peers-light 5`, 20 light peers | **5 held, 15 refused** — matching exactly 15 `Too many light nodes, rejecting` lines in the node log. Re-verified against `polkadot 1.22.0`, same result |
| `--role full` against `--in-peers 4` | **4 held** |
| `--in-peers-light 200`, 100 peers | 100 held; node reported `Idle (100 peers)` |

**Announcement quality degrades with peer count.** The `hold-peers` view (an upper
bound, since it includes our own scheduling):

| peers | spread p50 | spread max | coverage |
|---|---|---|---|
| 50 | 0.9 ms | 1.0 ms | 100% |
| 500 | 5.3 ms | 6.4 ms | 100% |
| 2000 | 19.3 ms | 27.1 ms | mean 92.2%, worst 29.6% (one run) |

`probe-announces` confirms this is the node and not our own scheduling:

| load | rpc→p2p lag p50 | probe spread p50 |
|---|---|---|
| idle | 0.2 ms | 0.1 ms |
| 1500 holders | 1.4 ms | 9.2 ms |
| 2000 holders | 6.8 ms | 11.2 ms |

Against a real Kusama node, the 10,000-connection cap was reached at ~9,945 peers.

---

## Limits and caveats

- **Hard ceiling of 10,000 incoming connections.** It is a `const`
  (`MAX_CONNECTIONS_ESTABLISHED_INCOMING`, `substrate/client/network/src/lib.rs`),
  with no CLI flag. Past it, litep2p accepts the TCP socket then rejects the
  pending connection, which surfaces here as
  `Handshake failed: unexpected end of file`. Going further requires patching the
  const and rebuilding the node.
- **Keep a ramp.** With `--ramp-ms 0` the node admits the whole burst before its
  slot accounting catches up. Measured against `--in-peers 32`: a burst of 50 full
  peers was *all* admitted — bounded only by `in_peers + in_peers_light` — then
  converged to exactly 32. **For a burst run, read the steady value and ignore the
  peak.** That the overshoot depends on the *light* setting even for full peers
  looks wrong and is an open question against polkadot-sdk.
- **`--idle-timeout` cannot keep a refused peer connected.** The node reaps a
  connection with no keep-alive substream after its own timeout — measured at
  10.003 s — whatever you set here.
- **No per-IP connection limit** exists in the node, so thousands of peers from one
  host is fine.
- **Ping and identify are not required.** litep2p marks notification protocols
  keep-alive and ping/identify not, and its ping cannot disconnect anyone
  (`max_failures` is stored but never read).
- **Only `genesis_hash` is validated** in the handshake, so `best_number: 0` with a
  real genesis `best_hash` is accepted penalty-free for both roles. No head or RPC
  plumbing is needed to become a peer.
- **The coverage drop at 2000 peers is intermittent** — one run showed mean 92.2%
  with a block reaching 593 of 2000 holders, a later run at identical settings
  showed 100%. It needs several repeats before any claim is made.

---

## Related

- `examples.md` — the request-serving half (`spam-light` / `soak-light`), which
  loads the `/light/2` handler instead of the peer slots.
- `light-request-capacity-test-plan.md` — methodology and measured results for that
  half.

> `spam-light` / `soak-light` **never become peers at all.** Their behaviour is
> identify plus `/light/2` only, with no block-announces substream, so the node
> never counts them: they do not appear in `Idle (N peers)` and receive no
> announcements. Request serving is gated only on ban status, which is why they
> work anyway. The two halves measure genuinely independent limits.
