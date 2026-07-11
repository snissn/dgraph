# Dgraph Badger vs TreeDB live durability A/B

- Repeats per cell: 3
- Workload fingerprint: `39f52e70e6d43be8c315bddc06cd77b9d654de4b04d1646ace3d03c73df11c45`
- Logical parity and restart gate: **PASS**
- Comparisons never mix durability classes.

## Relaxed durability

| Backend | Throughput median (ops/s) |         min-max |    CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) |
| ------- | ------------------------: | --------------: | ----: | --------------: | --------------: | --------------: | -----------------: |
| badger  |                   6635.11 | 6299.97-6638.52 | 2.43% |           0.529 |           1.000 |           1.137 |              1.506 |
| treedb  |                   6250.78 | 6173.11-6308.50 | 0.89% |           0.561 |           1.057 |           1.170 |              1.505 |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp         | GC cycles | flushes      | checkpoints       |
| ------- | -------------------: | -------------------: | ------------------------: | --------------------------: | -------------------------: | ----------------- | --------- | ------------ | ----------------- |
| badger  |            0.9 (3/3) |          919.0 (3/3) |              2177.0 (3/3) |                   0.4 (3/3) |                142.9 (3/3) | unavailable (0/3) | 1.0 (3/3) | 0.0 (3/3)    | unavailable (0/3) |
| treedb  |            1.0 (3/3) |         1111.5 (3/3) |                 5.7 (3/3) |                   5.8 (3/3) |                155.5 (3/3) | unavailable (0/3) | 0.0 (3/3) | 1034.0 (3/3) | 0.0 (3/3)         |

TreeDB throughput delta versus durability-matched Badger: **-5.79%** (gate: no worse than -3%).

## Durable durability

| Backend | Throughput median (ops/s) |       min-max |     CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) |
| ------- | ------------------------: | ------------: | -----: | --------------: | --------------: | --------------: | -----------------: |
| badger  |                    640.34 | 624.52-788.97 | 10.82% |           4.106 |          18.388 |          28.826 |              1.506 |
| treedb  |                    337.61 | 303.36-341.95 |  5.27% |           7.136 |          33.570 |          45.898 |              1.506 |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp         | GC cycles | flushes   | checkpoints       |
| ------- | -------------------: | -------------------: | ------------------------: | --------------------------: | -------------------------: | ----------------- | --------- | --------- | ----------------- |
| badger  |            1.0 (3/3) |          918.6 (3/3) |              2177.0 (3/3) |                   0.4 (3/3) |                138.5 (3/3) | unavailable (0/3) | 1.0 (3/3) | 0.0 (3/3) | unavailable (0/3) |
| treedb  |            1.2 (3/3) |         1071.3 (3/3) |                 5.2 (3/3) |                   5.2 (3/3) |                148.6 (3/3) | unavailable (0/3) | 0.0 (3/3) | 0.0 (3/3) | 0.0 (3/3)         |

TreeDB throughput delta versus durability-matched Badger: **-47.28%** (gate: no worse than -3%).

## Decision

**DO NOT advance the TreeDB backend on current performance.** Logical parity, schema, posting,
unsupported-feature, and recovery gates passed. The performance decision applies only to this
benchmark-minimal topology and workload; Badger remains the production default.

## Raw artifacts and reproduction

Reproduce from the recorded Dgraph SHA with
`TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir NEW_DIR`.
Paths below are relative to the artifact root; each JSON retains its exact original absolute command
and raw path.

- `badger-relaxed-r1`: `live/badger-relaxed-r1/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-relaxed-r2`: `live/badger-relaxed-r2/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-relaxed-r3`: `live/badger-relaxed-r3/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-relaxed-r1`: `live/treedb-relaxed-r1/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-relaxed-r2`: `live/treedb-relaxed-r2/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-relaxed-r3`: `live/treedb-relaxed-r3/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-durable-r1`: `live/badger-durable-r1/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-durable-r2`: `live/badger-durable-r2/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-durable-r3`: `live/badger-durable-r3/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-durable-r1`: `live/treedb-durable-r1/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-durable-r2`: `live/treedb-durable-r2/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-durable-r3`: `live/treedb-durable-r3/result.json`; Dgraph
  `0d7d559c9ec4cae14c16b9990c41f206e2602862`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`

Excluded runs are rejected by aggregation. CPU and RSS are Alpha-process measurements. Disk metrics
cover the postings directory; logical and allocated bytes are reported separately. TreeDB logical
write bytes use its public-batch counter, but write amplification remains unavailable because an
equivalent physical-byte counter is not exposed. Badger checkpoint count is reported unavailable,
not synthesized.
