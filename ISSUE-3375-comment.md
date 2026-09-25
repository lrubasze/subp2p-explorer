Measured the current behaviour and option 2 (rotating groups of 100) side by side on one Polkadot full node, to check the estimates above.

**Setup.** One full node (16 cores, ~15 real peers, `--in-peers-light 5000`), master `1.24.0-7c899fb56f1` vs a build with rotating groups (`e43799e8dd5`), run back to back. Synthetic light peers that behave like smoldot on `/grandpa/1`: they open the substream with the light role, answer the node's neighbor packet, and move their finalized head only when a commit reaches them. 8 of them record every commit they receive; the rest just hold slots and follow. The node's own neighbor packets (multicast on every commit it imports) give the timeline of its finalized height, so lag = how far a peer's last commit trails the node's finalized head, sampled every second. GRANDPA rounds were every 4.1 s and commits every 7–10 s throughout. Tool: `finality-lag` in [subp2p-explorer](https://github.com/lrubasze/subp2p-explorer/blob/lrubasze/node-load-test/finality-lag-examples.md).

**Finality lag** (p50 / p90 / max over all peer-seconds; blocks p50 / max):

| light peers | master | master, blocks | rotating groups of 100 | blocks |
|---:|---|---|---|---|
| 10 | 9 / 22 / 53 s | 0 / 8 | 4 / 10 / 16 s | 0 / 0 |
| 100 | 56 / 171 / 294 s | 9 / 49 | 4 / 10 / 16 s | 0 / 0 |
| 200 | 129 / 353 / 565 s | 21 / 94 | 6 / 14 / 25 s | 0 / 4 |
| 500 | 219 / 638 / 910 s | 36 / 152 | 13 / 23 / 34 s | 1 / 5 |

**Node cost**, `/grandpa/1` egress above each binary's own baseline with no light peers (463 and 440 kB/s), and CPU:

| light peers | master egress | groups egress | master CPU | groups CPU |
|---:|---|---|---|---|
| 10 | +12 kB/s | +115 kB/s | 42% | 47% |
| 100 | +78 kB/s | +870 kB/s | 52% | 55% |
| 200 | +79 kB/s | +1460 kB/s | 61% | 66% |
| 500 | +40 kB/s | +1458 kB/s | 69% | 73% |

RSS and block import (chain drift) were unaffected in both cases.

**Against the estimates:**

- *Today, 500 peers:* estimated 12.5 min. Measured median 3.7 min, mean gap between commits 5.3 min, p90 10.6 min, worst 15 min (152 blocks). The mean is about half the estimate because rounds are 4.1 s rather than 6 s and a commit stays the node's best for ~2 rounds, so it gets two draws. The estimate is closer to the tail than the typical case; the shape (linear in N, uncapped) is as described. Bandwidth 0.28 Mbit/s estimated, 0.3–0.6 measured, so flat in N.
- *Groups of 100, 500 peers:* estimated 7 Mbit/s and 15 s. Measured 11.5 Mbit/s and p50 13 s (p90 23 s, max 34 s). The bandwidth is higher for the same 4.1 s-round reason (100 × 53 kB / 4.1 s = 10.3 Mbit/s) plus duplicate commit copies (below). Egress did not grow between 200 and 500 peers, which is the point of the cap. Lag steps with `ceil(N/100)`: 0 blocks through 100 peers, 1 block median at 500.
- *Commits to all:* not run at 500, but from 72 peers receiving every commit the per-peer cost is 5.3–6.5 kB/s, i.e. 21–26 Mbit/s at 500 with a commit every 8–10 s, 35 only if finality ran one commit per block.
- *Commit size:* 52,989 B on the wire = 401 precommits × 132 B + 57 B header. With 600 validators that is exactly the ⌊2/3·600⌋+1 quorum, so "fewer votes per commit" is indeed not on the table.

**One thing independent of the fix:** 2–22% of the commits a node forwards are second copies of a commit it already forwarded, same round and target, a few ms apart. They are different validators' commits for the same round arriving inside the node's import window: `validate_commit_message` compares against the node's best commit, but that view only moves in `note_commit_finalized` after import, so both copies pass, both get imported, both get gossiped. With groups of 100 each duplicate costs another 100 × 53 kB. Dropping a commit whose `(set, round, target)` was already gossiped would fix it for every peer type.

Net: option 2 does what the estimate says, at ~1.5× the estimated bandwidth, and the current behaviour is somewhat better than 12.5 min at the median but as bad as estimated in the tail. Group size is the knob; 200 would halve the lag for ~2.6 MB/s on Polkadot, and Kusama commits are 2.5× larger.
