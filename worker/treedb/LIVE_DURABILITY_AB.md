# Live Badger vs TreeDB durability A/B

This harness is the runtime complement to `BenchmarkDgraphTreeDBMatrix`. The older matrix remains a
primitive capability matrix; it is not relabeled as a live Dgraph comparison.

Run from a clean Dgraph checkout with heavy scratch space outside the repository:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off \
  worker/treedb/run_durability_ab.sh \
  --artifact-dir /mnt/fast4tb/dgraph-treedb-ab/$(date -u +%Y%m%dT%H%M%SZ)
```

Use `--smoke` only to validate construction. A decision run uses three repeats per cell by default.
The script requires an artifact directory outside the repository, refuses to reuse it, records the
exact Git/module/host context, runs the existing posting-store adapter microbenchmarks, and then
runs four live cells:

| Durability class | Badger                                                   | TreeDB                                                 |
| ---------------- | -------------------------------------------------------- | ------------------------------------------------------ |
| relaxed          | `--badger syncwrites=false`, production tier, events off | relaxed command WAL, benchmark-minimal tier, events on |
| durable          | `--badger syncwrites=true`, production tier, events off  | durable command WAL, benchmark-minimal tier, events on |

Each cell uses one Zero and one Alpha, the same deterministic schema, leased UID dataset, 60% point
reads, 20% one-hop reads, 20% unique writes, fixed operation count, concurrency, seed, and excluded
warmup. Every timed read is checked against the expected value and cycle edge. Post-run and restart
checksums include canonical `source value -> target value` topology, so they are UID-independent;
unique write nodes are expected to have no edge. Raw JSON includes throughput, p50/p95/p99 latency,
Alpha CPU and RSS/HWM, logical and allocated posting bytes, available write/GC/flush/checkpoint
counters, recovery time, runtime-observed backend/durability, schema status, posting checksum/count,
restart parity, unsupported-feature status, SHAs, dirty state, command,
CPU/RAM/storage/filesystem/environment, and contamination.

Aggregation fails on an incomplete matrix, wrong backend or durability, workload mismatch, missing
metric contract, setup/timed overlap, dirty or excluded run, cross-run
revision/host/storage/environment mismatch, duplicate run ID, missing or duplicate per-cell repeat
ordinal, posting mismatch, schema failure, restart failure, or unsupported-feature status failure.
Start and final samples finding `construction_audit.py`, or host load above 75% of logical CPUs,
mark a run excluded. Unavailable counters are retained with a source and reason; they are never
manufactured. Relaxed and durable results have separate report headings and separate decisions.
Badger remains the production default. Badger vlog-write counters are not presented as semantic
flush counts.

The issue #17 decision evidence produced from Dgraph commit
`6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5` is committed under
[`artifacts/issue-17`](artifacts/issue-17/README.md). The committed report uses paths relative to
that directory; each raw result retains the original absolute command and scratch path for audit.

Non-smoke decision runs automatically capture separate relaxed and durable TreeDB CPU profiles after
the matrix. Profile-run throughput is never treated as benchmark evidence, and the report links only
artifacts verified to exist. To reproduce one profile manually, reuse the committed runner binary
produced in the artifact root and raise `--timed-ops` enough to cover `--profile-seconds`:

```sh
ARTIFACT=/mnt/fast4tb/dgraph-treedb-ab/FINAL_RUN
"$ARTIFACT/bin/livebench" \
  --dgraph-bin "$ARTIFACT/bin/dgraph" \
  --artifact-dir "$ARTIFACT/profiles/treedb-durable" \
  --backend treedb --durability durable --timed-ops 10000 \
  --cpu-profile "$ARTIFACT/profiles/treedb-durable.pprof" \
  --profile-seconds 5
go tool pprof -top "$ARTIFACT/bin/dgraph" \
  "$ARTIFACT/profiles/treedb-durable.pprof"
```
