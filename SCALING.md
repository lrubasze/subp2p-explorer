# Serving ceilings — dev-machine2 Kusama node

Two environments; always say which one a number came from.

- **loopback** — generator on dev-machine2, `/ip4/127.0.0.1/tcp/30333`. This is
  node capacity. Caveat: the generator shares the node's 16 cores.
- **routed path** — generator on another host. **MEASURED 117 MB/s** (≈936 Mbps)
  despite 10 GbE NICs on both, because the two sit in different /24s.

## Node capacity (loopback) — per operation

**"Light requests per second" is not a number.** Response sizes across the agreed
set span 1 B to 1.73 MB, and ceilings span 128/s to 25,254/s — a 197× spread. Any
answer has to name the request.

MEASURED 1 Sep 2026, `run-light-sweep`, 30 s per point, one operation at a time.
Ceilings are the **peak of a concurrency curve**, and every one is bracketed by
shedding or by a measured collapse on the far side — none is extrapolated.

| op | request | proof | serve time | knee | **ceiling** | par | MB/s |
|---|---|---|---|---|---|---|---|
| `sread` | read `System::Account[rand48]` | 2,424 B | 0.3 ms | 48 | **25,254/s** | 7.6 | 58 |
| `mread` | read `Session::Validators` | 23,399 B | 0.3 ms | 48 | **21,839/s** | 6.6 | 487 |
| `lread` | read `:code` + `:heappages` | 1,725,396 B | 5.6 ms | 32 | **~930/s** | 5.3 | 1,557 |
| `qcall` | call `Core_version` | 1 B | 0.5 ms | 16 | **7,699/s** | 3.8 | 0 |
| `mcall` | call `account_nonce` | 2,425 B | 0.6 ms | 8 | **5,683/s** | 3.4 | 13 |
| `bcall` | call `babe_configuration` | 29,374 B | 0.7 ms | 6 | **4,008/s** | 2.8 | 112 |
| `hcall` | call `Metadata_metadata` | 887 B | 16.8 ms | 2 | **128/s** | 2.2 | 0.1 |
| — | `/sync/warp` proof | 8,373,366 B | 79 ms | 20 † | **26.6/s** | 2.1 | 213 |

† The warp row is carried over from the earlier campaign, not re-measured here; 20
is the concurrency at which it was saturated, not a swept knee.

`par = ceiling × serve time` — requests in flight at saturation. It is **not
constant** (2.2 → 7.6), so there is no fixed worker pool: cheap requests interleave
better. Serve time is read at **one** client; past the knee `send->resp` is
queueing, not service.

Three findings the table encodes:

**Call vs read is the axis that matters, not size.** `mcall` and `sread` touch the
same trie path and return proofs of 2,425 vs 2,424 B — bytes and path held constant,
the only difference is Wasm. The read serves **4.4× more** (25,254 vs 5,683/s), and
Wasm costs ~0.3 ms of serve time *and* halves effective parallelism (3.4 vs 7.6).
Never rank calls by response size: for a runtime call the response is the execution
proof, so a call can burn milliseconds and return 887 bytes.

**Bytes are nearly free below ~1 MB.** `sread` → `mread` is **10× the bytes for
13.5% of the throughput** (25,254 → 21,839/s). The bottleneck is the trie walk and
per-request overhead. Bytes only take over much higher: `lread` at 1.73 MB tops out
at ~930/s, so the crossover sits between 23 kB and 1.73 MB and is still unmeasured.

**The heavy call is the outlier by two orders of magnitude.** `Metadata_metadata` is
128/s against 4,008/s for `babe_configuration` — same shape of work, 31× apart —
because the runtime rebuilds and SCALE-encodes all 447,555 B of metadata per call
and the node then ships only an 887 B proof.

**Not measurable on this chain: `read_child`.** The third `/light/2` message type
needs a child trie, and MEASURED on Kusama at the finalized head there are **zero**
`:child_storage:` keys and no Contracts/Revive pallet in the metadata. It needs a
chain with contracts (Asset Hub + revive) *and* a contract address that actually
exists — the CLI's `REVIVE_DEFAULT_ADDRESS` does not exist on Paseo Next Asset Hub,
so every `revive_*` number ever taken against that chain measured the
`DoesntExist` short-circuit, not a child read. Since a child read is answered from
`read_child_proof` with no Wasm, expect it to sit near the `sread` figure; that is a
prediction, not a measurement.

### `:code` reads: ~930/s, and node-bound

This supersedes the **~690/s** this file used to record, which was a floor taken
from an unfinished ladder. Extending it found the peak at 32–48 clients:

| clients | 16 | 24 | 32 | 48 | 64 |
|---|---|---|---|---|---|
| served/s | 790 | 851 | **926** | 946 | 672 |
| shed | 0 | 0 | 3,311 | 85,696 | 21,279 |

Quote **~930/s** (32 clients, 0.4% shed), not the 946 at 48 where three quarters of
offered requests are refused. Beyond 48 it collapses to 672/s.

**It is the node's limit, not ours.** Two generator processes at 24 clients each
gave 464 + 465 = **929/s** against **922/s** for one process at 48 — ratio 1.01×.
A client-side limit would have roughly doubled. Caveat: both generators share the
box with the node, so this rules out a single-process bottleneck, not total box CPU,
and at 1.56 GB/s the generator is a substantial co-tenant. So ~930/s remains a
lower bound on what a node with the box to itself would serve.

## What the uplink allows — the number that actually matters

Node ceilings are mostly unreachable in the field. Against the
[reference spec](https://docs.polkadot.com/node-infrastructure/run-a-validator/requirements/)
uplink of **500 Mbit/s** (62.5 MB/s):

| op | node ceiling | 500 Mbit/s allows | reachable |
|---|---|---|---|
| `qcall` / `hcall` | 7,699/s / 128/s | not byte-limited | **100%** |
| `mcall` | 5,683/s | 25,773/s | **100%** |
| `sread` | 25,254/s | 25,783/s | **98%** |
| `bcall` | 4,008/s | 2,128/s | **53%** |
| `mread` | 21,839/s | 2,671/s | **12%** |
| `/sync/warp` | 26.6/s | 7.5/s | **28%** |
| `lread` (`:code`) | ~930/s | 36/s | **4%** |

CALCULATED: allowed = 62.5 MB/s ÷ proof size.

**For everything byte-heavy the link runs out first, by up to 26×.** Two
consequences: precision on the `lread` ceiling is operationally irrelevant, and
**~7 concurrent warp-syncing clients saturate a 500 Mbit/s uplink** (7.5/s ÷ 1.06
proofs/s per client). That is the useful operational number, and it is a property of
the link, not the node.

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

The per-operation campaign, end to end:

```sh
# 1. ceilings: sweep concurrency per operation, 30 s a point
./run-light-sweep                                   # all 7 ops -> sweep.csv
OPS=lread CLIENTS_LIST="24 32 48 64 96" ./run-light-sweep   # extend one ladder
./sweep-table results/serving-2026-09-01/sweep/sweep.csv

# 2. is a ceiling ours or the node's?
OP_METHOD=warp_code CLIENTS=48 ./run-two-proc

# 3. derive the mixed-soak rate from those ceilings (aggregate utilisation)
WEIGHTS="sread:800 mcall:100 mread:80 bcall:6 qcall:4 lread:4 hcall:2" \
  TARGET_UTIL=0.5 ./mix-rate results/serving-2026-09-01/sweep/sweep.csv

# 4. the long run (prints the RATE/METHOD that step 3 emits)
RATE=6657 CLIENTS=1024 MAX_CONCURRENT=16 DURATION=43200 \
  METHOD='...' OUT_DIR=results/serving-2026-09-01/soak-12h ./run-light-soak

# node health at any point, or sampled continuously
./run-monitor-health --once
OUT_DIR=results/x ./run-monitor-health
```

Loopback runs use `ADDRESS=/ip4/127.0.0.1/tcp/30333`, binary on dev-machine2.
The remote non-interactive shell is zsh **without `~/bin` on `PATH`**, so scripts
call the binary by absolute path.

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

- **Concurrency has to be swept from below, and both directions are traps.** One
  request in flight per connection, so too few connections measures the *client*:
  achievable rate is `CLIENTS ÷ latency` whatever `RATE` says, and at `CLIENTS=10`
  that capped calls at 4762/s and reads at 820/s. `shed=0` at a rate below target
  means you measured yourself.
  **But "set it far above what the rate needs" — this file's previous advice — is
  the opposite error and just as wrong.** MEASURED on `Core_version`: 7,699/s at 16
  clients, 7,172/s at 24, **6,476/s at 32** with 219,400 shed. On `lread`: 946/s at
  48, **672/s at 64**. The curve has a peak; past it you measure self-inflicted
  congestion collapse. Sweep up and stop where throughput stops rising.
- **`--max-concurrent` is not a formality, and leaving it at the 8192 default can
  cost you 20% of your throughput.** `soak-light` grows the pool whenever the pacer
  has unconsumed permits, which feeds back on itself: more connections → the
  20-slot light queue overflows → latency rises → permits go unconsumed → more
  connections. MEASURED, identical load (the 7-op mix at 6,657/s offered, 60 s):

  | `--clients` / `--max-concurrent` | peak concurrent | served | shed |
  |---|---|---|---|
  | 4096 / default | 128 | 5,294/s | **20.3%** |
  | 64 / **16** | 16 | **6,651/s** | **0%** |

  Same node, same offered rate — the difference was entirely the harness. Size the
  cap from Little's law (`concurrency = rate × mean serve time`; 2.6 for that mix)
  and keep it below the node's 20-slot queue. A cap far above the queue depth
  measures queue admission, not capacity.
- **A weighted mix needs `@N`, and needs the schedule interleaved.** `:weight` is
  peeled only for the plain named presets — `read:`/`call:`/`read_child:` tokens are
  exempt, so before `@N` existed a mix containing reads could not be weighted at
  all. And the schedule used to be emitted as consecutive blocks, which is not a mix
  but a sequence of monocultures: a connection walks it from slot 0 and retires
  after `rate*duration/clients` requests, so short-lived connections never reach the
  later methods. MEASURED before the fix: a 90 s seven-method run issued 598,016
  small reads and **zero** of the other six. Fixed by spreading each method's slots
  evenly *and* starting every connection at a random offset. Verify a mix by
  checking the per-method `issued` counts against the weights before trusting a run.
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

## Sustained load per operation, ~25 min each, alone on the node (2 Sep 2026)

Each operation at 50% of **its own** solo ceiling — alone it has the whole node, so
half its own ceiling is half the node. (In a mix the shares must be summed by cost
instead; see `mix-rate`.) Concurrency sized per operation from Little's law, every
cap below the node's 20-slot queue.

| op | rate | served | p50 | ok | shed | RSS under load |
|---|---|---|---|---|---|---|
| `hcall` | 64/s | 64/s | **17.0 ms** | 97,148 | 0 | −509 MB |
| `lread` | 473/s | 471/s | 9.2 ms | 722,800 | 0 | +22 MB |
| `mcall` | 2,841/s | 2,841/s | 1.7 ms | 4,377,743 | 0 | +97 MB |
| `sread` | 12,627/s | 12,625/s | 0.8 ms | 19,608,473 | 0 | +99 MB |
| `bcall` | 2,004/s | 2,004/s | 2.1 ms | 3,154,231 | 0 | +144 MB |
| `mread` | 10,919/s | 10,918/s | 0.8 ms | 17,633,631 | 0 | +54 MB |
| `qcall` | 3,849/s | 3,849/s | 1.4 ms | 6,458,441 | 0 | −2 MB |

**Every operation holds half its ceiling indefinitely with nothing refused.** So the
ceilings are not fragile peaks — half of each is comfortably sustainable.

**The heavy call is faster in company than alone.** `Metadata_metadata` reads
17.0 ms here and 16.8 ms in the 30 s sweep, but **10.5 ms in the 12 h mix at the
same 64/s**. Two independent solo runs agree, so it is not a sweep artefact. No
explanation yet; a plausible line to pull is CPU frequency behaviour (solo it uses
~1.2 cores and the box is otherwise near-idle, in the mix ~2.9).

**RSS moves with no reference to the load.** −509 MB on the lightest operation,
+144 MB on a mid one, ±100 MB elsewhere, with releases in both directions. Combined
with the 12 h result, memory here tracks something other than serving volume.

## Sustained mixed load, 12 h, no synthetic peers (1–2 Sep 2026)

One process offering all seven operations at once, weighted as a plausible client
population, at an offered rate set so **aggregate** utilisation is ~50% of the
measured ceilings (`mix-rate`). No held peers — the node's own 30–42 real ones only.

| | |
|---|---|
| duration / rate | 43,201 s @ **6,656 req/s** offered, sustained flat |
| **served** | **287,567,634** |
| **shed / unserved / timeout / err / aborted** | **0 / 0 / 0 / 0 / 0** |
| node's own counter | 287,567,850 — **+216, or 0.0001%** |
| node queue overflows / handler refusals | **0 / 0** |
| bytes | 3.21 TB @ 74.4 MB/s |
| latency | p50 **1.2 ms**, p90 2.3, p99 11.9 |
| **chain drift** | **+0.7 s over 7,049 blocks** (0.0016%) |
| node CPU | 2.89 cores average |
| trie node-cache hit rate | 99.83% |
| connections | 1,024 cycled, 16 concurrent, setup p50 1.8 ms |

**A node serving 6,656 light requests/s for twelve hours refused nothing at all.**
Not "shed a stable 2–4%" as the earlier peer-holding mix did — literally zero, on
both the client's count and the node's own failure counters. And it stayed within
0.7 s of the chain across 7,049 blocks, a tenth of one block.

| method | issued | share | p50 | p99 | proof |
|---|---|---|---|---|---|
| `sread` | 230,978,013 | 80.32% | 1.2 ms | 11.6 | 2,425 B |
| `mcall` | 28,872,249 | 10.04% | 1.5 ms | 11.9 | 2,425 B |
| `mread` | 23,097,798 | 8.03% | 1.3 ms | 11.7 | 23,399 B |
| `bcall` | 1,732,333 | 0.60% | 1.7 ms | 12.1 | 29,374 B |
| `qcall` | 1,154,897 | 0.40% | 1.3 ms | 11.1 | 1 B |
| `lread` | 1,154,896 | 0.40% | 8.2 ms | 19.0 | 1,725,396 B |
| `hcall` | 577,448 | 0.20% | 10.5 ms | 22.4 | 887 B |

Every method 100% served. Shares match the requested weights to within 0.01
percentage points, which is the check that the mix was actually a mix.

**Open observation:** `hcall` p50 is **10.5 ms here against a 16.8 ms solo serve
time** — faster inside the mix than alone, which is backwards and not yet explained.
The per-operation sustained runs offer the controlled comparison (same 64/s rate).

### RSS: 12 h of serving, and it went *down*

| | before | after 12 h load | after 120 s idle |
|---|---|---|---|
| RSS | **8.08 GB** | **7.99 GB** | **7.96 GB** |

**−90 MB across twelve hours** of continuous serving, oscillating ±400 MB with
repeated releases (linear fit over 2,169 samples: +15.8 MB/h, inside the noise).

This **refutes "sustained serving load grows RSS"** as a general claim — which is
what this file previously concluded from runs that grew ~150–200 MB/h. But it does
**not** identify the cause, and there are two live explanations that this run cannot
separate:

1. **Peers drive it.** Every earlier long run held 2000 synthetic peers; this one
   held none. That inverts the earlier note here that "essentially none of this is
   peer-related."
2. **The node was already at its plateau.** It had been up 49 days and started at
   8.08 GB, where the earlier runs started at 4.2–5.4 GB and were plausibly still
   filling caches toward exactly this level.

Explanation 2 is the more mundane and, on this evidence, at least as likely. The
test that separates them is a **restarted node** (RSS back at ~2 GB) serving with no
peers: if RSS climbs to ~8 GB and stops, it was cache fill and there is nothing to
fix. That is one 6 h run and it should be the next thing anyone does here.

## Sustained load — 6 h and 12 h (earlier campaign, *with* 2000 held peers)

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

- ~~Reads may be our client/loopback limit~~ — **settled**: node-bound at ~930/s
  (two-process test, 1.01×). Residual: both generators share the box, so total box
  CPU is not excluded.
- ~~Whether the per-leg shedding is real contention~~ — **largely settled**: the
  12 h mixed run at ~50% aggregate utilisation shed **nothing at all**, on both the
  client and the node's counters. The earlier 2–4% was measured with an uncapped
  connection pool, which is now known to shed 20% on a load that sheds 0% when
  capped. Re-read those numbers as harness artefacts unless reproduced with a cap.
- **Whether RSS growth is peers or cache fill** — see above. One 6 h run on a freshly
  restarted node with no peers settles it.
- **Why `Metadata_metadata` is faster in a mix (10.5 ms) than solo (16.8 ms).**
- Why warp parallelism is ~2. The aggregate is node-bound (two independent
  generator processes at concurrency 10 each gave 25.5/s, the same as one process
  at 20), and node CPU accounting supports the per-proof cost: (234−82)% ÷ 25.9/s =
  **59 ms**, and (183−82)% ÷ 13/s = **78 ms** of node CPU per proof. What is *not*
  separable is worker count from per-worker service time.
- Whether the per-leg shedding above is real contention (2-4% under sustained load,
  stable over 11 h).
- ~~Why RSS does not plateau under sustained serving load~~ — **the premise is
  wrong.** Those runs all held 2000 peers; a 12 h serving-only run went *down* by
  90 MB. The claim "not peer-related" was made against a peers-only baseline
  (~1806 MB) taken on a node in a different state, and does not survive. The live
  question is now peers-vs-cache-fill, above.
- `enp65s0f1` is DOWN on both hosts — a 10 GbE direct link if brought up, which
  would let the mix run without the generator competing for the node's cores.

Phase 1–2 peer results are unaffected: announcements at 5000 held peers are
~250 kB/s, 0.2% of even the routed path.
