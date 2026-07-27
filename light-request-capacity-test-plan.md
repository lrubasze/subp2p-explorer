# Single-node light-request capacity test

How to measure how many `/<genesis>/light/2` requests a single full node can serve,
and the data we have so far.

> **Tooling location.** The load-test CLI used here lives in the
> `subp2p-explorer` repo, branch **`lrubasze/node-load-test`**:
> <https://github.com/michalkucharczyk/subp2p-explorer/tree/lrubasze/node-load-test>
> (commands `spam-light` / `soak-light`, crate `subp2p-explorer-cli`). The
> `run-spam-light` / `run-soak-light` scripts referenced below are thin wrappers
> around those commands.

## Motivation

Few data exists on how many light-client requests one node can serve. The
server-side handler processes requests on a **single serial worker** behind a
**global bounded queue** (`MAX_LIGHT_REQUEST_QUEUE = 20`,
`substrate/client/network/light/src/light_client_requests/handler.rs`). The
comment on that constant states the value was copied from the block-request
limit "due to lack of data on light client request handling in production
systems." This test produces that missing data.

Key facts about the server path (polkadot-sdk):

- One shared queue of **20** pending requests for the whole protocol, across all
  connections and peers (not per-peer).
- A **single** task drains it, running `client.execution_proof(...)` one request
  at a time (`handler.rs` run loop).
- When the queue is full the incoming request is **silently dropped**
  (`try_send` fails → `InboundFailure::Omission`); no back-pressure, the remote
  just times out.

So node capacity is essentially `1 / service_time` of that single worker, and
`service_time` depends heavily on the runtime method being called.

## What we already measured

Two `run-spam-light` runs, identical load — 1 connection, `--concurrency 10`,
1000 requests, the same 7-method mix — RPC via `wss://kusama-rpc.polkadot.io`,
executing against a recent finalized Kusama block:

| | Kusama node under test (v1.24.0) | Parity `kusama-bootnode-0` (v1.19.0) |
|---|---|---|
| Throughput | **686 ok req/s** | 144 ok req/s |
| Latency p50 / p99 | 11.8 / 31.4 ms | 61.7 / 95.9 ms |
| err / timeout | 0 / 0 | 0 / 0 |
| Total proof bytes | 18,135,419 | 18,127,045 |

Conclusions (with the supporting arithmetic):

1. **Neither run saturated the node.** Offered in-flight was 10, below the
   20-slot queue, and `err`/`timeout` stayed 0. **686 req/s is a lower bound,
   not the ceiling.**
2. **The two nodes differ by network distance, not capacity.** Proof work was
   identical (~18.13 MB both). The latency gap (~50 ms) is constant across all 7
   methods regardless of proof size. Little's law confirms both runs were
   *window-bound*: `10 / 14.6 ms ≈ 686`, `10 / 69 ms ≈ 144`. The bootnode's
   144 req/s is a round-trip-time artifact and must not be read as its capacity.
3. **Derived service-time bound.** A serial worker sustaining 686 completions/s
   spends **≤ 1.46 ms per request** (1/686) on this mix, so the true ceiling is
   `1/service_time ≥ 686 req/s` — and probably well above it, since the worker
   was never the bottleneck.

### Proof size per method (network-independent — the solid finding)

| Method | Avg proof size |
|---|---|
| `ParachainHost_candidate_events` | ~40 KB (heaviest) |
| `BabeApi_current_epoch` | ~29 KB |
| `GrandpaApi_grandpa_authorities` | ~29 KB |
| `ParachainHost_validators` | ~23 KB |
| `account_nonce` | ~2.4 KB |
| `ParachainHost_disputes` | ~1.5 KB |
| `Metadata_metadata` | ~0.9 KB (lightest) |

Note `Metadata_metadata` is tiny in *proof* terms despite the large metadata
blob, because metadata is built from the runtime, not read from state.

## Getting the missing data (the ceiling)

We never overflowed the queue, so the ceiling is still unknown. Procedure:

1. **Run the client close to the node.** Same DC, or `--url ws://127.0.0.1:9944`
   on the node host. Otherwise results are RTT-bound and you re-measure the
   network (see the bootnode run above).

2. **Overflow the 20-slot queue.** Two options:
   - `run-spam-light` (closed-loop): push the window past 20 —
     `--concurrency 32 --connections 4` (128 in-flight), `--count 20000`.
   - `run-soak-light` (open-loop, cleaner): sweep `--rate` **above the 686
     floor** — 1000 → 2000 → 3000 → 5000 req/s. Service time ≤ 1.46 ms puts the
     ceiling in the low thousands.

3. **Find the knee.** The offered rate where **`err`/`timeout` climb off zero**
   (queue overflow → `InboundFailure::Omission`) and **completed req/s
   plateaus** is the ceiling ≈ `1/service_time`.

4. **Measure cheap vs heavy separately.** Run `account_nonce` alone (best case)
   and `ParachainHost_candidate_events` alone (~40 KB proof — expect a *lower*
   ceiling, the pessimistic number under a heavy-method attack). Report a number
   per method, not one blended figure.

5. **Capture node-side CPU** during the sweep. Proof generation is
   single-core-bound; the node pinning one core at the plateau confirms you
   found the real bottleneck.

## Deliverable

Per method: a curve of **offered rate → completed rate + drop rate**, the
plateau (max sustained req/s), and CPU at saturation. This turns "few data
exists" into concrete figures such as "node X serves ~N `account_nonce`/s and
~M `candidate_events`/s before dropping," and informs whether the 20-slot
queue and single-worker design should be revisited (e.g. a bounded proof-worker
pool, and/or per-peer fairness so one client cannot monopolise all 20 slots).

## Reference commands

Baseline (per-method latency + proof size, low load):

```bash
cargo run -p subp2p-explorer-cli --release -- spam-light \
  --url ws://127.0.0.1:9944 \
  --address /ip4/<NODE_IP>/tcp/30333 \
  --method account_nonce --count 200 --concurrency 1 --connections 1
```

Saturation sweep (open-loop, step `--rate` until drops appear):

```bash
cargo run -p subp2p-explorer-cli --release -- soak-light \
  --url ws://127.0.0.1:9944 \
  --address /ip4/<NODE_IP>/tcp/30333 \
  --method ParachainHost_candidate_events \
  --rate 1000 --duration 60 --clients 200
```

Notes:
- `--url` needs a reachable RPC of the **same chain** (only genesis + finalized
  head are read from it); the actual load goes to `--address`.
- The peer id in `--address` (`/p2p/12D3…`) is optional — it is learned during
  the noise handshake, and if present is verified.
- Method tokens are chain-specific. The ones above are for the **Kusama relay**;
  see `spam-light --help` for the full syntax (`call:`/`read:`/weights).
