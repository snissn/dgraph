# Issue #17 decision evidence

This directory is the normalized, committed copy of the durability-matched live A/B evidence
produced by Dgraph commit `0d7d559c9ec4cae14c16b9990c41f206e2602862`.

- [`report.md`](report.md) is the decision report and links to the 12 raw live results under
  `live/`.
- [`context.txt`](context.txt) records the benchmark host, dependency, command, and workload shape.
- [`microbench/raw.txt`](microbench/raw.txt) is the five-sample adapter microbenchmark output.
- `profiles/` contains the separate five-second TreeDB CPU profiles, their top summaries, and the
  profile-run result metadata.

The profile runs use larger operation counts to keep the timed phase open for five seconds. Their
throughput is diagnostic only and is not included in the A/B decision. The durable profile is mostly
blocked on I/O and therefore collected only 850 ms of CPU samples over five wall-clock seconds; the
low sample count is itself consistent with an I/O-wait-dominated path, but is not a standalone
causal proof.

The original run lived under `/mnt/fast4tb`; absolute scratch paths and exact commands remain in the
raw JSON for auditability. Reproduction uses a new immutable artifact directory:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off \
  worker/treedb/run_durability_ab.sh --artifact-dir NEW_DIR
```
