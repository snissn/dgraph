# Dgraph Badger vs TreeDB live durability A/B

- Repeats per cell: 3
- Workload fingerprint: `39f52e70e6d43be8c315bddc06cd77b9d654de4b04d1646ace3d03c73df11c45`
- Logical parity and restart gate: **PASS**
- Comparisons never mix durability classes.

- Host: `mikers-B560-DS3H-AC-Y1`; CPU: `11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz`; RAM:
  `31.2 GiB`
- Artifact/posting storage: `/dev/nvme1n1p1` (`Samsung SSD 990 PRO 4TB`, 4000.8 GB), `ext4` mounted
  at `/mnt/fast4tb`
- Environment: `GOWORK=off TMPDIR=/mnt/fast4tb/tmp GOMAXPROCS= GOFLAGS=`

## Relaxed durability

| Backend | Throughput median (ops/s) |         min-max |    CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) |
| ------- | ------------------------: | --------------: | ----: | --------------: | --------------: | --------------: | -----------------: |
| badger  |                   6422.73 | 6090.17-6544.08 | 3.02% |           0.547 |           1.039 |           1.196 |              1.505 |
| treedb  |                   6070.35 | 5381.32-6126.63 | 5.78% |           0.580 |           1.099 |           1.269 |              1.506 |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp         | GC cycles | flushes           | checkpoints       |
| ------- | -------------------: | -------------------: | ------------------------: | --------------------------: | -------------------------: | ----------------- | --------- | ----------------- | ----------------- |
| badger  |            0.9 (3/3) |          919.3 (3/3) |              2177.0 (3/3) |                   0.4 (3/3) |                144.5 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb  |            1.0 (3/3) |         1107.0 (3/3) |                 5.6 (3/3) |                   5.7 (3/3) |                150.3 (3/3) | unavailable (0/3) | 0.0 (3/3) | 994.0 (3/3)       | 0.0 (3/3)         |

TreeDB throughput delta versus durability-matched Badger: **-5.49%** (gate: no worse than -3%).

## Durable durability

| Backend | Throughput median (ops/s) |        min-max |     CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) |
| ------- | ------------------------: | -------------: | -----: | --------------: | --------------: | --------------: | -----------------: |
| badger  |                    658.44 | 642.84-1008.14 | 21.91% |           4.033 |          17.647 |          23.338 |              1.505 |
| treedb  |                    365.61 |  311.59-390.95 |  9.30% |           7.539 |          30.496 |          39.938 |              1.506 |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp         | GC cycles | flushes           | checkpoints       |
| ------- | -------------------: | -------------------: | ------------------------: | --------------------------: | -------------------------: | ----------------- | --------- | ----------------- | ----------------- |
| badger  |            1.0 (3/3) |          918.8 (3/3) |              2177.0 (3/3) |                   0.4 (3/3) |                137.8 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb  |            1.2 (3/3) |         1082.8 (3/3) |                 5.2 (3/3) |                   5.2 (3/3) |                150.8 (3/3) | unavailable (0/3) | 0.0 (3/3) | 0.0 (3/3)         | 0.0 (3/3)         |

TreeDB throughput delta versus durability-matched Badger: **-44.47%** (gate: no worse than -3%).

## Decision

**STOP advancement/integration at this phase; keep Badger as the production default.** Logical
parity, schema, posting, unsupported-feature, and recovery gates passed. The performance decision
applies only to this benchmark-minimal topology and workload.

## Profile attribution

- Relaxed TreeDB: [`profiles/treedb-relaxed.pprof`](profiles/treedb-relaxed.pprof) and
  [`profiles/treedb-relaxed-top.txt`](profiles/treedb-relaxed-top.txt). The profile is
  syscall/allocation-heavy, but without matched substrate-only and adapter/runtime profiles it
  cannot attribute the split between gomap itself and Dgraph integration overhead.
- Durable TreeDB: [`profiles/treedb-durable.pprof`](profiles/treedb-durable.pprof) and
  [`profiles/treedb-durable-top.txt`](profiles/treedb-durable-top.txt). The five-second wall-clock
  profile collected little CPU, which is consistent with I/O wait; it is not causal proof of the
  durable throughput gap.

## Raw artifacts and reproduction

Reproduce from the recorded Dgraph SHA with
`TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir NEW_DIR`.
Paths below are relative to the artifact root; each JSON retains its exact original absolute command
and raw path.

- `badger-relaxed-r1`: `live/badger-relaxed-r1/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-relaxed-r2`: `live/badger-relaxed-r2/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-relaxed-r3`: `live/badger-relaxed-r3/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-relaxed-r1`: `live/treedb-relaxed-r1/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-relaxed-r2`: `live/treedb-relaxed-r2/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-relaxed-r3`: `live/treedb-relaxed-r3/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-durable-r1`: `live/badger-durable-r1/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-durable-r2`: `live/badger-durable-r2/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `badger-durable-r3`: `live/badger-durable-r3/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-durable-r1`: `live/treedb-durable-r1/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-durable-r2`: `live/treedb-durable-r2/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`
- `treedb-durable-r3`: `live/treedb-durable-r3/result.json`; Dgraph
  `6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5`; gomap `v0.6.2-0.20260711114710-3a3e3c72a1a8`; dirty
  `false`

Excluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha
`VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large
logical size with small allocated size comes from sparse preallocated files, so logical and
allocated bytes must be read together. TreeDB logical write bytes use its public-batch counter, but
write amplification remains unavailable because an equivalent physical-byte counter is not exposed.
Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are
exposed; vlog writes are not relabeled as flushes.
