# Serving ceilings measured against the dev-machine2 Kusama node

> **These three numbers are the network path, not the node.** They are recorded
> here because the runs are reproducible and the commands are useful, but do not
> quote them as node capacity. See "What actually bound them" below.

## What was run

| load | observed | response size | ⇒ byte rate |
|---|---|---|---|
| warp calls (`babe_*` / `aura_*`) | 3800 req/s | ~29 kB | **110 MB/s** |
| `warp_code` reads | 70 req/s | 1,725,384 B | **121 MB/s** |
| `/sync/warp` proofs | 14 req/s | 8,373,366 B | **117 MB/s** |
| light connections | 4000 (max) | — | n/a |

```sh
# calls — babe_configuration,babe_current_epoch,babe_next_epoch
#         (aura_slot_duration,aura_authorities are absent on Kusama: 2-byte replies)
METHOD="babe_configuration,babe_current_epoch,babe_next_epoch" \
  RATE=3800 DURATION=60 CLIENTS=10 ./run-soak-light

# reads — warp_code, 1.7 MB per response
METHOD="warp_code" RATE=70 DURATION=60 CLIENTS=10 ./run-soak-light

# warp proofs
OUT_DIR=results/warp-soak-smoke MAX_CONCURRENT=20 RATE=20 DURATION=60 ./run-warp-sync
```

## What actually bound them

All three land within 10% of the same byte rate despite response sizes spanning
three orders of magnitude (29 kB → 8.4 MB). That is one shared resource, and it is
the routed path between the load generator and the node.

**MEASURED** — plain `ssh dd`, no polkadot process involved:

```sh
ssh -c aes128-gcm@openssh.com dev-machine2 'dd if=/dev/zero bs=1M count=1000' | dd of=/dev/null bs=1M
# 1048576000 bytes (1.0 GB) copied, 8.97956 s, 117 MB/s
```

- single stream: **117 MB/s** (≈936 Mbps)
- 3 parallel streams, aggregate: **109 MB/s** — so not per-stream, not ssh crypto
- prior warp runs, 25 concurrent clients: 116–121 MB/s aggregate

**MEASURED** — the hosts are not on a common segment. Both have 10 GbE NICs up,
but they sit in different /24s and traffic is routed:

```
client  195.154.212.240/24
node    195.154.218.123/24   via 195.154.212.1 dev enp65s0f0
```

Corroborating, from the `STAGGER_MS` sweep: warp throughput was flat at
14.0–14.5 req/s across all five stagger settings. Flat-regardless-of-concurrency
is the signature of a fixed pipe, not of a node resource.

## Consequences

- **The three ceilings are one ceiling.** "% of each leg's ceiling" and "% of the
  path" are the same axis. A mix at 50% of each leg is 150% of the path.
- **Proof-generation cost is not established.** The ~0.2–0.4 ms attributed to
  proving came from babe calls (29 kB proof) minus absent `aura_*` calls (2-byte
  reply). **CALCULATED:** 29,000 B ÷ 117 MB/s = **248 µs** of pure transmission —
  the whole differential. Generation may be ~0.
- **Warp p50 figures (1091 ms → 536 ms across the stagger sweep) are queueing on a
  saturated path**, not node latency.
- **The 20-deep warp queue mechanism still holds** — shed counts matched the node's
  own counters exactly — but the threshold (25 simultaneous arrivals shed, the same
  25 spread over 1.2 s shed none) is a function of drain rate, so it does not port
  to a faster path.
- **`4000` light connections is not a byte-rate result** and is untouched by the
  above, but it is also unattributed: the node's compiled-in cap is 10,000
  (`MAX_CONNECTIONS_ESTABLISHED_INCOMING`), so what bound it at 4000 is unknown.
  Note `soak-light --clients` acts as a concurrency cap, which starved at least one
  run.
- **Phase 1–2 is unaffected.** Block announcements at 5000 held peers are
  ~250 kB/s, 0.2% of the path. The 3500–4250 knee, the `--in-peers-light 2000`
  recommendation and the `libp2p-node` busy fractions all stand.

## Getting real numbers

1. **Loopback** — run the load generator on dev-machine2 against
   `127.0.0.1:30333`. No network at all, so proof generation and the event loop are
   finally exposed. Caveat: the generator then contends for the node's 16 cores, so
   this localises the bottleneck rather than giving clean absolutes.
2. **A faster path** — `enp65s0f1` is DOWN on both hosts. If those can be brought
   up against each other that is a 10 GbE direct link, and every figure above needs
   redoing.
