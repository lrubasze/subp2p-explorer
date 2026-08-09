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

The mixed run below needs the holders remote and the serving legs on loopback, so it
is two hosts at once. Holders own the monitor and `node.csv`; the legs write nothing
(their summaries go to the logs), which sidesteps the fact that both `run-hold-peers`
and `run-warp-sync` would otherwise each start their own `run-monitor-node`.

```sh
# on the client box
OUT_DIR=results/mix-1 PEERS=2000 DURATION=300 ./run-hold-peers &
sleep 30

# on dev-machine2, all three concurrently, 270s
B="--url ws://127.0.0.1:9944 --address /ip4/127.0.0.1/tcp/30333"
subp2p-explorer-cli soak-light $B --method "babe_configuration,babe_current_epoch,babe_next_epoch" \
  --rate 2000 --clients 200 --duration 270 &
subp2p-explorer-cli soak-light $B --method warp_code --rate 345 --clients 200 --duration 270 &
subp2p-explorer-cli warp-sync $B --requests 1 --begin-block 0 --step 0 \
  --request-timeout 20 --rate 13 --duration 270 --max-concurrent 20 &
wait
```

Then `./chain-drift results/mix-1` for the verdict.

## Traps

- **`soak-light --clients` is a concurrency cap**, not a total. One request in
  flight per connection, so the achievable rate is `CLIENTS ÷ latency` whatever
  `RATE` says. At `CLIENTS=10` that capped calls at 4762/s and reads at 820/s —
  both runs measured the client. Set it far above what the rate needs, and treat
  `shed=0` at a rate below target as a sign you measured yourself.
- **`requests_in_success_total{_sum}` is not serve time on litep2p** — 9.7 µs
  recorded against 79 ms of real work. `_count` is exact and matched the client
  (+666 vs `ok=666`).

## Mixed load

**50% of each ceiling at once does not degrade a node holding 2000 light peers.**
Six loaded runs against six baselines, 300 s each, alternating:

| | node CPU | chain drift (see `chain-drift`) |
|---|---|---|
| 2000 peers only | 85% | +0.0, −0.1, +0.1, +5.9, +0.1, +6.0 |
| 2000 peers + mix | **418%** | +5.7, −0.4, −0.3, −0.3, +5.7, −6.3 |

The mix mean (+0.7 s) is *lower* than baseline (+2.0 s). Load was real — 5× the
CPU, and every leg held target (calls ~1975/s, reads ~337/s, warp 13.0/s).

The mix ran calls 2000/s + reads 345/s + warp 13/s = 755 MB/s ≈ 6.0 Gbps, so the
serving legs were on loopback while the 2000 holders stayed remote (~250 kB/s). The
loopback generator cost 3.25 of 16 cores against the node's 3.7 — real
contamination, but there was headroom and the result is negative anyway.

**Both light legs shed slightly under the mix** — calls ~1.1%, reads ~2.7% — at
half their solo ceilings. Suggestive of contention between legs, but the solo runs
used different `--clients`, so it is not yet a clean comparison.

**Watch the resolution.** The drift is quantised to one block (~6 s): `wall_ms`
is a fixed ~312 s window and `chain_ms` is always a multiple of 6000. So a 300 s
run cannot resolve better than ±6 s, which is what produces the bimodal ~0/~±6
pattern above in *both* conditions. Longer windows, not more repeats, are what buy
precision.

## Sustained load — 6 h and 12 h

Same mix, run long: 2000 held peers (remote) plus all three legs on loopback, then a
1 h peers-only recovery tail.

| | 6 h run | 12 h run |
|---|---|---|
| load / tail | 5 h + 1 h | 11 h + 1 h |
| **chain drift** | **+1.5 s** (0.007%) | **−0.6 s** (−0.001%) |
| peers held | 2000/2000 | 2000/2000 |
| node CPU under load | 475% | 471–480% |
| calls served | 34,890,505 (3.1% shed) | 76,431,693 (3.5% shed) |
| reads served | 6,011,711 (3.2% shed) | 13,411,291 (1.8% shed) |
| **warp proofs** | **233,999 — 0 failures** | **514,799 — 0 failures** |
| bytes served | ~7 TB | **~29.7 TB** |

**The node keeps pace indefinitely at this load.** Over 12 h it tracked the chain to
within 0.6 s across 6974 blocks — a tenth of one block, an order of magnitude below
the quantisation floor.

**Warp serving is flawless under sustained load.** 748,798 proofs across both runs
with zero shed, zero timeout, zero error, zero aborted, holding 103.81 MiB/s
throughout. The node's own counter agrees with the client to within 1–2 requests
(+234,001 vs 233,999; +514,800 vs 514,799).

**Light-leg latency roughly doubles under the mix** but stays stable: calls p50
5.9 ms (2.1 ms solo), reads p50 11.7 ms (12.2 ms solo). Shed sits at 2–4% and does
not climb over 11 h.

### RSS does not plateau

| | h0 | h3 | h7 | h10 | tail |
|---|---|---|---|---|---|
| 6 h run | 4361 | 5495 | — | — | 5118 MB |
| 12 h run | 5358 | 6127 | 7143 | **7213** | 6859–6917 MB |

The 6 h run looked like a cache filling toward an asymptote: growth decelerated from
+218 to +80 MB per 20 min, there was a −148 MB release mid-run, and the tail gave
back **−670 MB** and went flat within 15 minutes.

**Twelve hours does not support that reading.** Growth continued at ~150 MB/h and
was still rising at h10, and the tail returned only ~350 MB. Across both runs — ~18 h
of near-continuous load — RSS went 4175 → ~6900 MB. At that rate it would be ~25 GB
in a week, which matters on a 32 GB reference machine.

Two things to hold in mind before treating it as a leak: this is load no real node
sees (13.4 M reads of a 1.7 MB blob, 515 K full warp proofs), and **peers-only RSS
was ~1806 MB**, so essentially none of this is peer-related. It is a serving
observation. Unresolved.

## Open

- Reads (~1.09 GiB/s of near-pure copy — 772 B of trie nodes around a 1.7 MB blob)
  may be our client/loopback limit rather than the node's.
- Why warp parallelism is ~2. The aggregate is node-bound (two independent
  generator processes at concurrency 10 each gave 25.5/s, the same as one process
  at 20), and node CPU accounting supports the per-proof cost: (234−82)% ÷ 25.9/s =
  **59 ms**, and (183−82)% ÷ 13/s = **78 ms** of node CPU per proof. What is *not*
  separable is worker count from per-worker service time.
- Whether the per-leg shedding above is real contention (2-4% under sustained load,
  stable over 11 h).
- **Why RSS does not plateau under sustained serving load** (~150 MB/h at h10 of the
  12 h run, only ~350 MB returned when load stopped). Not peer-related; needs a
  heap profile rather than more soak time.
- `enp65s0f1` is DOWN on both hosts — a 10 GbE direct link if brought up, which
  would let the mix run without the generator competing for the node's cores.

Phase 1–2 peer results are unaffected: announcements at 5000 held peers are
~250 kB/s, 0.2% of even the routed path.
