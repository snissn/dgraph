# Issue #17 decision evidence

This directory is the normalized, committed copy of the durability-matched live A/B evidence
produced by Dgraph commit `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`.

- [`report.md`](report.md) is the decision report and links to the 12 raw live results under
  `live/`.
- [`context.txt`](context.txt) records the benchmark CPU, RAM, storage, filesystem, environment,
  dependency, commands, and workload shape.
- [`microbench/raw.txt`](microbench/raw.txt) is the five-sample adapter microbenchmark output.
- `profiles/` contains the separate five-second TreeDB CPU profiles, their top summaries, and the
  profile-run result metadata.

Every timed read was checked against its expected value and one-hop cycle edge. The post-run and
restart checksum canonically hashes `source value -> target value` plus edge-free unique writes, so
it is independent of leased UIDs. All 12 cells share one checksum and node count.

Profile-run throughput is diagnostic only and is not included in the A/B decision. The relaxed
profile is syscall/allocation-heavy, but cannot split gomap substrate cost from Dgraph
adapter/runtime overhead without comparative profiles. The durable profile collected only 880 ms of
CPU over five wall-clock seconds, which is consistent with I/O wait but is not causal proof.

The original run lived under `/mnt/fast4tb`; absolute scratch paths and exact commands remain in the
raw JSON for auditability. Reproduction uses a new immutable artifact directory:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off \
  worker/treedb/run_durability_ab.sh --artifact-dir NEW_DIR
```
