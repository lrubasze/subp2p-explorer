# Single-node light-request capacity

How many `/<genesis>/light/2` requests can one full node serve?

**The numbers live in [`SCALING.md`](SCALING.md).** This file is the method and the
upstream context. It was originally a plan; the campaign has since run, and the
sections below are corrected against what it measured — see *What this document used
to claim* at the end, because the original model was wrong in a way worth recording.

> **Tooling.** Branch `lrubasze/node-load-test` of `subp2p-explorer` (commands
> `spam-light` / `soak-light`, crate `subp2p-explorer-cli`). The harness is
> `run-light-sweep` (ceilings), `run-two-proc` (is a ceiling ours or the node's?),
> `mix-rate` (derive a mixed-soak rate), `run-light-soak` (long runs),
> `run-monitor-health` + `run-monitor-node` (node health).

## Motivation

Few data existed on how many light-client requests one node can serve. The comment
on `MAX_LIGHT_REQUEST_QUEUE` in
`substrate/client/network/light/src/light_client_requests/handler.rs` says the value
was copied from the block-request limit "due to lack of data on light client request
handling in production systems." This campaign produced that data.

## The headline

**"Light requests per second" is not a number.** Across the agreed request set,
response sizes span 1 B to 1.73 MB and ceilings span 128/s to 25,254/s — 197×. Any
answer has to name the request. Two terms set the cost. **Wasm**: a read and a call
over the same trie path, with proofs of 2,424 vs 2,425 B, differ by **4.4×**, which is
instantiation and execution alone. **Bytes**: negligible low down, but dominant by
1.73 MB, where `:code` — a plain read with no Wasm — manages 926/s, 27× slower than
the small read. Which term dominates depends on the response size.

Above that sits a second result: for everything byte-heavy, a spec node's
**500 Mbit/s uplink runs out before the node does**, by up to 26×.

## Server-side facts (polkadot-sdk @ `0e1812505`, litep2p)

- One shared queue of **20** pending requests for the whole protocol, across all
  connections and peers — not per-peer. Settable via
  `--light-client-request-queue-size`; 20 is the upstream default.
- It is a **burst** limit, not a capacity limit. Arrivals far above 20 in flight
  shed heavily at *unchanged* mean load — MEASURED: the same offered rate shed 20.3%
  at 128 concurrent and 0% at 16.
- When the queue is full the request is dropped, and on **litep2p** that is visible
  from both ends: the client sees the substream closed with no proof, and the node
  counts `requests_in_failure_total{reason="sending into a full channel"}`.
  Backend-specific — libp2p says `busy-omitted` and does not record handler refusals.
- `RemoteReadChildRequest` is the third message type (oneof field 4, alongside
  call=1 and read=2) and is a **read**: answered from `read_child_proof` with no
  Wasm. `storage_key` must carry the `:child_storage:default:` prefix.
- **Capacity is `1 / service_time` of a single serial worker** — confirmed in the
  source; see below for what that service time actually is per request type.

## Method

Four things, in order. Each exists because a shortcut past it produced a wrong
number at some point in this campaign.

**1. Sweep concurrency, not rate.** Set `--rate` far above anything achievable and
`soak-light` becomes closed-loop at concurrency = `--clients`. Sweep from 1 upward
and take the **peak** of the curve. Both directions are traps: too few connections
measures your own client (achievable rate is `clients ÷ latency`, so `shed=0` below
target means you measured yourself), and too many produces congestion collapse
(`Core_version`: 7,699/s at 16 clients, 6,476/s at 32). The knee ranges from 2 to 48
across the set, so no client count transfers between operations.

**2. Read serve time at one client only.** Past the knee `send->resp` is queueing,
not service: `Metadata_metadata` reads 15.6 ms at 2 clients and 92.8 ms at 12 for
the *same* 128/s, each extra client adding exactly `1/128 s`.

**3. Confirm saturation by shedding, never by extrapolation.** A peak sitting on the
last rung of the ladder with no shedding anywhere is an unfinished ladder, not a
ceiling. That mistake is what made `:code` reads look like ~690/s when they are
~930/s.

**4. Cross-check every client figure against the node's own counter.**
`requests_in_success_total_count` matched the client to within `clients` requests on
every point of every sweep — the difference being the in-flight requests aborted at
the deadline. Note the trap: the sibling **`_sum` is not serve time on litep2p**,
which stamps it after the response future resolves and so under-reports by ~8100×.

For a mixed run, add: derive the offered rate from the measured ceilings
(`mix-rate`) so operations share the node by cost rather than each running at its own
solo fraction, and check the per-method `issued` counts against the weights before
trusting the run.

## Deliverable

Per operation: proof size, serve time, knee, ceiling, effective parallelism, byte
rate, and the share of a 500 Mbit/s uplink it consumes. Plus a long mixed run for
sustained behaviour. All in `SCALING.md`.

## The serial-worker model: confirmed, with one addition

The original version of this document stated that the handler "processes requests on
a **single serial worker**" and that "node capacity is essentially
`1 / service_time` of that single worker". **That is correct**, VERIFIED in
polkadot-sdk @ `0e1812505`:

- `substrate/client/service/src/builder.rs` spawns exactly one task,
  `spawn_handle.spawn("light-client-request-handler", ..., handler.run())`.
- `run()` is `while let Some(request) = self.request_receiver.next().await { match
  self.handle_request(..) }`, and `handle_request` is a **synchronous** `fn` taking
  `&mut self` that calls `client.execution_proof(..)` / `read_proof(..)` inline.
  No spawn, no await, so one proof at a time.

So `ceiling = 1 / handler service time`, and `1/ceiling` recovers that budget: 40 µs
for a small read, 176 µs for `account_nonce`, 7.8 ms for `Metadata_metadata` — a
195× span in front of a single shared 20-slot queue.

**The addition, which the original did not anticipate: the handler is not where most
of the CPU goes for cheap requests.** A serial worker can use at most one core, but
serving 25,254 small reads/s costs the process **5.4 cores** net of a 0.52-core idle
baseline — so ~4.4 cores are the network path (per-request substream setup, response
writes), not proving. `Metadata_metadata` is the reverse: 1.09 cores total, ~92% of it
inside the handler.

That also means these measurements cannot say *which* of the two saturates for the
cheap request types, because the handler's own service time is not instrumented —
and `requests_in_success_total_sum`, the metric that looks like it would tell you,
under-reports by ~8100× on litep2p.

**A correction to a previous revision of this file:** it claimed the measurements
refuted the serial-worker model, arguing from whole-process core counts and from
`ceiling × round-trip` being 2.2-7.6. Both were misread. Whole-process CPU includes
the network path, and `ceiling × round-trip` is **pipeline depth** — requests sitting
in the client, the TCP path and the queue — not concurrent proving. Little's law
gives requests *in the system*, which legitimately exceeds 1 for a serial server fed
by a queue.

The original per-method proof sizes and the "686 req/s" figure are still superseded.
That run offered 10 in flight against a 20-slot queue with `err`/`timeout` at zero,
which by trap 1 above means it measured the client: `10 / 14.6 ms ≈ 686`. It was
correctly labelled a lower bound at the time; the actual figure for that mix is
several thousand per second.
