# Serving ceilings — dev-machine2 Kusama node

Two environments; always say which one a number came from.

- **loopback** — generator on dev-machine2, `/ip4/127.0.0.1/tcp/30333`. This is
  node capacity. Caveat: the generator shares the node's 16 cores.
- **routed path** — generator on another host. **MEASURED 117 MB/s** (≈936 Mbps)
  despite 10 GbE NICs on both, because the two sit in different /24s.

## Node capacity (loopback)

| load | ceiling | byte rate | per-unit cost |
|---|---|---|---|
| `/sync/warp` proofs | **26.6/s** | 213 MiB/s | 79 ms/proof, ~2-way parallel |
| `warp_code` reads (1.7 MB) | **~690/s** | ~1.09 GiB/s | — |
| babe calls | **~4.1–4.5k/s** | ~121 MB/s | ~0.2 ms proving |

All three confirmed by shedding, not inferred — e.g. calls: 19938 offered → 4117
served / 15820 shed.

**Warp**, from two runs: 79 ms at concurrency 1 (11.1/s), 739 ms at concurrency 20
(26.6/s). CALCULATED: 26.6 × 0.0791 = **~2 parallel workers**, and Little's Law
predicts 752 ms at concurrency 20 against 739 measured. Heaviest case only — every
request replayed from the genesis `begin`, so no cache benefit.

**Proving cost**, from the aura control (absent methods reply in 2 bytes with no
execution), run on loopback where a 29 kB proof costs ~27 µs to transmit: babe
**1.6–1.7 ms** vs aura **1.4–1.5 ms** ⇒ **~0.2 ms of proving**. An earlier revision
of this file claimed ~0, attributing the whole differential to transmission; the
loopback control refutes that.

## What a 1 Gbps link can reach

117 MB/s ÷ response size, against the ceilings above:

| leg | 1 Gbps allows | node does | reachable |
|---|---|---|---|
| babe calls | ~3980/s | ~4300/s | **93%** |
| warp proofs | ~14/s | 26.6/s | **53%** |
| `warp_code` reads | ~68/s | ~690/s | **10%** |

**On a 1 Gbps node only calls can meaningfully stress the serving path.**

The path limit is node-independent: `ssh dd` moves 117 MB/s single-stream, 109 MB/s
over 3 streams, with no polkadot process involved. Every ceiling recorded here
before the loopback runs — warp 14/s, reads 70/s, calls 3800/s — was that path. All
three worked out to 110–121 MB/s despite response sizes spanning 29 kB to 8.4 MB.

## Commands

Loopback runs use `ADDRESS=/ip4/127.0.0.1/tcp/30333`, binary on dev-machine2.

```sh
# warp: single-proof cost, then saturated
OUT_DIR=results/warp-c1  MAX_CONCURRENT=1  RATE=50 DURATION=60 ./run-warp-sync
OUT_DIR=results/warp-c20 MAX_CONCURRENT=20 RATE=50 DURATION=60 ./run-warp-sync

# calls
METHOD="babe_configuration,babe_current_epoch,babe_next_epoch" \
  RATE=20000 DURATION=60 CLIENTS=200 ./run-soak-light

# reads
METHOD="warp_code" RATE=1000 DURATION=60 CLIENTS=200 ./run-soak-light

# proving-cost control: 3 real babe calls + 2 absent aura calls
METHOD="babe_configuration,babe_current_epoch,babe_next_epoch,aura_slot_duration,aura_authorities" \
  RATE=20000 DURATION=60 CLIENTS=10 ./run-soak-light
```

## Traps

- **`soak-light --clients` is a concurrency cap**, not a total. One request in
  flight per connection, so the achievable rate is `CLIENTS ÷ latency` whatever
  `RATE` says. At `CLIENTS=10` that capped calls at 4762/s and reads at 820/s —
  both runs measured the client. Set it far above what the rate needs, and treat
  `shed=0` at a rate below target as a sign you measured yourself.
- **`requests_in_success_total{_sum}` is not serve time on litep2p** — 9.7 µs
  recorded against 79 ms of real work. `_count` is exact and matched the client
  (+666 vs `ok=666`).

## Open

- Reads (~1.09 GiB/s of near-pure copy — 772 B of trie nodes around a 1.7 MB blob)
  may be our client/loopback limit rather than the node's.
- Why warp parallelism is ~2.
- **Mixed load is unmeasured.** The ceilings were measured in isolation but share
  the litep2p event loop and the trie/DB. 50% of each = 770 MB/s ≈ 6.2 Gbps, so
  loopback or 10 GbE only. Warp is the leg to watch: 79 ms per request at ~2-way
  parallelism is the most likely to interfere with block import.
- `enp65s0f1` is DOWN on both hosts — a 10 GbE direct link if brought up, which
  would also stop the generator competing for the node's cores.

Phase 1–2 peer results are unaffected: announcements at 5000 held peers are
~250 kB/s, 0.2% of even the routed path.
