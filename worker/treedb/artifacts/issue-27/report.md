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
| badger  |                   6112.00 | 6077.86-6130.20 | 0.36% |           0.567 |           1.093 |           1.240 |              1.506 |
| treedb  |                   4339.61 | 3971.39-4534.41 | 5.45% |           0.806 |           1.538 |           2.268 |              1.506 |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp         | GC cycles | flushes           | checkpoints       |
| ------- | -------------------: | -------------------: | ------------------------: | --------------------------: | -------------------------: | ----------------- | --------- | ----------------- | ----------------- |
| badger  |            0.9 (3/3) |          919.2 (3/3) |              2177.0 (3/3) |                   0.4 (3/3) |                136.0 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb  |            1.5 (3/3) |         1438.5 (3/3) |                33.8 (3/3) |                  33.9 (3/3) |                153.2 (3/3) | unavailable (0/3) | 0.0 (3/3) | 1013.0 (3/3)      | 0.0 (3/3)         |

TreeDB durability diagnostics (timed-phase deltas unless marked high-water):

| ordinary writes | durable writes | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |
| --------------: | -------------: | ---------------------: | -----------: | ----------: | --------------------------: | ---------------------: | ----------------------: | -------------------: |
|      1013 (3/3) |        0 (3/3) |      0 (3/3) / 0 (3/3) |      0 (3/3) |     0 (3/3) |                     0 (3/3) |                0 (3/3) |                 0 (3/3) |              0 (3/3) |

| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |
| --------------------: | ------------: | -----------: | -----------------------: | --------------------------: | -------------------------: |
|            2400 (3/3) |   62525 (3/3) | 26.052 (3/3) |                 92 (3/3) |                     0 (3/3) |                    0 (3/3) |

TreeDB throughput delta versus durability-matched Badger: **-29.00%** (gate: no worse than -3%).

## Durable durability

| Backend | Throughput median (ops/s) |        min-max |     CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) |
| ------- | ------------------------: | -------------: | -----: | --------------: | --------------: | --------------: | -----------------: |
| badger  |                   1024.64 | 601.79-1101.36 | 24.16% |           3.063 |          10.317 |          20.248 |              1.506 |
| treedb  |                    389.53 |  379.19-406.96 |  2.92% |           6.172 |          30.173 |          49.930 |              1.506 |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp         | GC cycles | flushes           | checkpoints       |
| ------- | -------------------: | -------------------: | ------------------------: | --------------------------: | -------------------------: | ----------------- | --------- | ----------------- | ----------------- |
| badger  |            1.1 (3/3) |          919.0 (3/3) |              2177.0 (3/3) |                   0.4 (3/3) |                147.0 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb  |            1.5 (3/3) |         1316.5 (3/3) |                29.5 (3/3) |                  29.6 (3/3) |                139.6 (3/3) | unavailable (0/3) | 0.0 (3/3) | 0.0 (3/3)         | 0.0 (3/3)         |

TreeDB durability diagnostics (timed-phase deltas unless marked high-water):

| ordinary writes | durable writes | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |
| --------------: | -------------: | ---------------------: | -----------: | ----------: | --------------------------: | ---------------------: | ----------------------: | -------------------: |
|         0 (3/3) |      862 (3/3) |      0 (3/3) / 0 (3/3) |      0 (3/3) |     0 (3/3) |                     0 (3/3) |              862 (3/3) |                 0 (3/3) |              0 (3/3) |

| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |
| --------------------: | ------------: | -----------: | -----------------------: | --------------------------: | -------------------------: |
|            2400 (3/3) |   45041 (3/3) | 18.767 (3/3) |                 54 (3/3) |                     0 (3/3) |                    0 (3/3) |

TreeDB throughput delta versus durability-matched Badger: **-61.98%** (gate: no worse than -3%).

## Decision

**STOP advancement/integration at this phase; keep Badger as the production default.** Logical
parity, schema, posting, unsupported-feature, and recovery gates passed. The performance decision
applies only to this benchmark-minimal topology and workload.

## Profile artifacts

Separate TreeDB profile runs were collected after the decision matrix; their throughput is
diagnostic and is not part of the A/B decision.

- Relaxed TreeDB: [`profiles/treedb-relaxed.pprof`](profiles/treedb-relaxed.pprof) and
  [`profiles/treedb-relaxed-top.txt`](profiles/treedb-relaxed-top.txt).
- Durable TreeDB: [`profiles/treedb-durable.pprof`](profiles/treedb-durable.pprof) and
  [`profiles/treedb-durable-top.txt`](profiles/treedb-durable-top.txt).

These artifacts do not by themselves attribute cost between gomap and Dgraph integration, establish
I/O wait, or prove a causal explanation for either throughput delta.

## Raw artifacts and reproduction

Reproduce from the recorded Dgraph SHA with
`TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir /absolute/path/outside/repository/NEW_DIR`.
Paths below are relative to the artifact root; each JSON retains its exact original absolute command
and raw path.

- `badger-relaxed-r1`: `live/badger-relaxed-r1/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `badger-relaxed-r2`: `live/badger-relaxed-r2/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `badger-relaxed-r3`: `live/badger-relaxed-r3/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `treedb-relaxed-r1`: `live/treedb-relaxed-r1/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `treedb-relaxed-r2`: `live/treedb-relaxed-r2/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `treedb-relaxed-r3`: `live/treedb-relaxed-r3/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `badger-durable-r1`: `live/badger-durable-r1/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `badger-durable-r2`: `live/badger-durable-r2/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `badger-durable-r3`: `live/badger-durable-r3/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `treedb-durable-r1`: `live/treedb-durable-r1/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `treedb-durable-r2`: `live/treedb-durable-r2/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`
- `treedb-durable-r3`: `live/treedb-durable-r3/result.json`; Dgraph
  `6bfe9e7b6d3e60c3147581af12db4443bd810809`; gomap `v0.6.2-0.20260721010427-121462d80f1f`; dirty
  `false`

Excluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha
`VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large
logical size with small allocated size comes from sparse preallocated files, so logical and
allocated bytes must be read together. TreeDB logical write bytes use its public-batch counter, but
write amplification remains unavailable because an equivalent physical-byte counter is not exposed.
Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are
exposed; vlog writes are not relabeled as flushes.
