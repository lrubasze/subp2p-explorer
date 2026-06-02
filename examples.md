# `spam-light` — examples

Load-test a full node's light-client request handler by speaking the raw
`/<genesis>/light/2` request-response protocol directly — **no smoldot, no sync**.
The tool dials an appointed host, learns its peer id during the noise handshake,
keeps a fresh finalized block to execute against (via an RPC subscription), and
fires `RemoteCallRequest`s with a bounded in-flight window.

> Why this stresses the node: per `smoldot-info.txt`, the server-side `/light/2`
> handler has a **global queue of ~20, single-threaded, no per-peer fairness**.
> Every `RemoteCallRequest` forces the node to *execute* a runtime method in its
> proving backend and return a Merkle/execution proof — real CPU + a real proof.

Build once:

```bash
cargo build -p subp2p-explorer-cli
# binary: ./target/debug/subp2p-explorer-cli
```

All examples below use `cargo run`; the equivalent binary call is
`./target/debug/subp2p-explorer-cli spam-light …`.

---

## Quick start (chain presets)

A `--chain` preset fills in the RPC url, a default p2p host, and a default method
mix — so the shortest possible runs are:

```bash
# Bulletin Next: account_nonce + can_store + account_authorization + indexed_transactions
cargo run -p subp2p-explorer-cli -- spam-light --chain paseo-next-bulletin

# Asset Hub Next: revive_get_storage + account_nonce
cargo run -p subp2p-explorer-cli -- spam-light --chain paseo-next-asset-hub
```

Each of those issues `--count 100` requests at `--concurrency 8`. Everything the
preset sets is overridable with the flags below.

---

## Flags

| flag | short | default | meaning |
|---|---|---|---|
| `--chain` | `-c` | — | `paseo-next-asset-hub` \| `paseo-next-bulletin` (preset url/host/methods) |
| `--url` | `-u` | preset | RPC endpoint (fetches genesis, tracks the finalized head) |
| `--address` | `-a` | preset | multiaddr to dial; `/p2p/<id>` optional (learned in handshake) |
| `--genesis` | `-g` | from RPC | hex genesis hash (builds the `/<genesis>/light/2` name) |
| `--block` | `-b` | finalized head | pin a hex block hash to execute against (disables head subscription) |
| `--protocol` | `-p` | `/<genesis>/light/2` | override the full protocol name (e.g. fork-id chains) |
| `--method` | `-m` | preset / `account_nonce` | comma-separated method mix with optional `:weight` |
| `--count` | | `100` | total requests to issue |
| `--concurrency` | | `8` | max in-flight requests (the spam window) |
| `--request-timeout` | | `10` | per-request timeout, seconds (counted separately) |
| `--timeout` | `-t` | `120` | overall wall-clock timeout, seconds |

---

## Methods

`data` is the concatenated plain-SCALE encoding of the runtime-API arguments.
Args are cache-busted per request (random account / random key) so smoldot-style
caches don't absorb the load.

| name | chain | runtime API | args |
|---|---|---|---|
| `account_nonce` | both | `AccountNonceApi_account_nonce` | random `AccountId32` (32 B) |
| `can_store` | Bulletin | `BulletinTransactionStorageApi_can_store` | `AccountId32` ++ `u32` len (36 B) |
| `account_authorization` | Bulletin | `BulletinTransactionStorageApi_account_authorization` | `AccountId32` (32 B) |
| `indexed_transactions` | Bulletin | `TransactionStorageApi_indexed_transactions` | `u32` head number (4 B) |
| `revive_get_storage` | Asset Hub | `ReviveApi_get_storage` | `H160` (20 B) ++ random key (32 B) = 52 B |
| `call:<method>:<hexdata>` | any | `<method>` | raw hex `data` |
| `read:<hexkey>[,<hexkey>…]` | any | (`RemoteReadRequest`) | storage Merkle proof for the keys |
| `<BareName>` | any | `<BareName>` | no args (e.g. `Core_version`, `Metadata_metadata`) |

> `revive` lives on **Asset Hub**, not Bulletin. `revive_get_storage` defaults to a
> dotNS contract address and a random 32-byte slot key (a proof of absence in the
> contract's child trie is still real execution).

---

## Bulletin Next examples

```bash
# Single method
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin --method account_nonce --count 500 --concurrency 16

# Even mix of all four Bulletin methods
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin \
  --method "account_nonce,can_store,account_authorization,indexed_transactions" \
  --count 1000 --concurrency 32

# Weighted mix — hammer can_store 3x as often as the others (3:1:1)
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin \
  --method "can_store:3,account_nonce,indexed_transactions" \
  --count 1200 --concurrency 24
```

---

## Asset Hub Next examples

```bash
# Default preset: revive_get_storage + account_nonce
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-asset-hub --count 500 --concurrency 16

# Revive storage reads only (the dot.li get_storage path)
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-asset-hub --method revive_get_storage \
  --count 1000 --concurrency 32

# Revive against a specific contract via the generic escape hatch
# data = H160 (20 B) ++ slotKey (32 B), hex-concatenated:
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-asset-hub \
  --method "call:ReviveApi_get_storage:a1b2b939e82b2ece55bd8a0e283818bfc1ca6cdc46ac7f91e4a3efd0d43518a33a18c9095a670570fe1c157617e2733d52cb0980"
```

---

## Generic escape hatch (any chain / any method)

```bash
# A no-arg runtime call — connectivity smoke test (Core_version is cached → ~1 B proof)
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin --method Core_version --count 5 --concurrency 1

# A no-arg call that actually executes + reads :code: (large proof)
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin --method Metadata_metadata --count 20 --concurrency 4

# Arbitrary call with explicit SCALE-hex args:  call:<method>:<hexdata>
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin \
  --method "call:AccountNonceApi_account_nonce:d43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d"

# Storage read proof (RemoteReadRequest) for one or more raw storage keys
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin \
  --method "read:26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9"
```

---

## Targeting a specific host (no preset)

Everything works without `--chain` if you supply `--url` (for genesis + head) and
`--address` (the node to hammer). The peer id in the multiaddr is optional.

```bash
cargo run -p subp2p-explorer-cli -- spam-light \
  --url wss://paseo-bulletin-next-rpc.polkadot.io \
  --address /dns4/paseo-bulletin-next-rpc-node-1.polkadot.io/tcp/443/wss \
  --method account_nonce --count 300 --concurrency 16

# Pin the genesis + execution block, and override the protocol name (fork-id chains)
cargo run -p subp2p-explorer-cli -- spam-light \
  --url wss://paseo-bulletin-next-rpc.polkadot.io \
  --address /ip4/127.0.0.1/tcp/30333/ws \
  --genesis 8cfe6717dc4becfda2e13c488a1e2061ff2dfee96e7d031157f72d36716c0a22 \
  --block 60e4719088c9090974582503f7e33b78dfbab0a81101571716e7e7997037241c \
  --protocol "/8cfe6717dc4becfda2e13c488a1e2061ff2dfee96e7d031157f72d36716c0a22/light/2"
```

Default preset hosts:

- `paseo-next-bulletin` → `/dns4/paseo-bulletin-next-rpc-node-0.polkadot.io/tcp/443/wss`
- `paseo-next-asset-hub` → `/dns4/paseo-asset-hub-next-collator-node-0.parity-testnet.parity.io/tcp/443/wss`

---

## Targeting collators

To hit a **collator** instead of an RPC node, override `--address` with the
collator's DNS endpoint (from the SRE doc). Collators serve `/light/2` just like
RPC nodes — verified reachable on `:443/wss` — and self-identify in their agent
string (e.g. `… (paseo-bulletin-next-collator-node-0) (litep2p)`). The preset
still provides the RPC url + genesis + method mix; only the address changes.

Collator hosts: `paseo-{bulletin,asset-hub,people-next-system}-next-collator-node-{0,1}.parity-testnet.parity.io`
(People Next uses `paseo-people-next-system-collator-node-{0,1}`).

```bash
# Bulletin Next — collator node 0 (the bulletin preset otherwise defaults to the rpc-node)
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin \
  --address /dns4/paseo-bulletin-next-collator-node-0.parity-testnet.parity.io/tcp/443/wss \
  --count 1000 --concurrency 32

# Bulletin Next — collator node 1
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-bulletin \
  --address /dns4/paseo-bulletin-next-collator-node-1.parity-testnet.parity.io/tcp/443/wss \
  --count 1000 --concurrency 32

# Asset Hub Next — collator node 1 (node 0 is already the preset default)
cargo run -p subp2p-explorer-cli -- spam-light \
  --chain paseo-next-asset-hub \
  --address /dns4/paseo-asset-hub-next-collator-node-1.parity-testnet.parity.io/tcp/443/wss \
  --count 1000 --concurrency 32
```

> Collators are usually **not** discoverable via the DHT (they don't advertise
> public addresses; RPC/bootnodes do), so target them by hostname rather than via
> `discover-network`.

---

## Finding the saturation knee (scaling)

Push `--concurrency` past the server's queue and watch throughput plateau while
the error rate climbs — the node starts **resetting substreams** (load-shedding)
instead of serving. Sweep it:

```bash
for C in 8 32 96; do
  echo "## concurrency=$C"
  cargo run -p subp2p-explorer-cli -- spam-light \
    --chain paseo-next-bulletin --method account_nonce \
    --count 800 --concurrency $C 2>/dev/null | grep -E "ok req/s|latency ms"
done
```

Observed against `paseo-bulletin-next-rpc-node-0` (illustrative):

| concurrency | ok | err (node dropped) | throughput | p99 |
|---|---|---|---|---|
| 8 | 800/800 | 0 | 54 req/s | 608 ms |
| 32 | 773 | 27 (3.4%) | 186 req/s | 613 ms |
| 96 | 571 | 229 (29%) | 484 req/s | 232 ms |

Toward 10k: keep one connection, set `--count 10000 --concurrency 64`, raise
`--timeout` to fit the run. (Breadth — many connections / peer ids to emulate many
light clients — is a planned next knob; the server queue is global, so breadth
tests starvation rather than bypassing the queue.)

---

## Reading the output

```
=== /light/2 spam summary ===
protocol: /8cfe…0a22/light/2
issued=800 ok=800 err=0 timeout=0 in 14.8s => 54 ok req/s
latency ms: p50=115.6 p90=183.4 p99=607.8 | proof bytes total=1974202
per method:
  account_nonce: issued=15 ok=15 err=0 timeout=0 | p50=101.2ms p99=138.0ms | avg proof=2464B | sample=Call { proof_len: Some(2496) } …
```

- **ok** — got a response (a `RemoteCallResponse`; `proof_len` is the execution proof size).
- **err** — substream closed without a response: the node *declined or dropped* the
  request (a non-empty proof needs real work; under load this is the load-shedding signal).
- **timeout** — no response within `--request-timeout` (the other saturation signal).
- **proof bytes** — total / per-method-average execution-proof size; bigger ⇒ more
  storage touched (e.g. `revive_get_storage` ≈ 5 KB vs `account_nonce` ≈ 2 KB).
- **p50/p90/p99** — response latency percentiles (ms).

> A `Core_version` run shows `proof_len: Some(1)` — it's answered from the cached
> `RuntimeVersion` without executing. Use it only as a pipe-is-working check; use the
> real methods above to generate actual load.

---

## Tip: pin to specific cores

For heavy local runs, pin the process so it doesn't starve the rest of the machine:

```bash
taskset -c 0-8 cargo run -p subp2p-explorer-cli -- spam-light --chain paseo-next-bulletin --count 10000 --concurrency 64
```
