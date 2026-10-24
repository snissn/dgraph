# Dgraph Badger vs TreeDB live durability A/B

Resolved Zero/Alpha survival profile: **filesystem**. Startup and Alpha-restart configuration and posting-route witnesses are retained with each row. The recovery timer measures a clean Alpha restart while Zero stays running; this is not abrupt-crash or power-loss certification.

- Repeats per cell: 3
- Workload fingerprint: `4db80a5a242e4b41cd51ad0710d1ac912d366a8beb4bc7ca1458942d645ea178`
- Logical parity and restart gate: **PASS**
- Comparisons never mix durability classes.

The restart column retains `recovery_seconds`, the legacy HTTP-health boundary, which is not equivalent across backends. The additive read-ready and read/write-ready columns report `recovery_read_seconds` and `recovery_read_write_seconds`: elapsed time from the same Alpha exec boundary through full posting validation, then a new CommitNow ACK and verified fresh read. These are clean Alpha restart measurements with Zero alive, not abrupt-crash or power-loss certification.

- Host: `mikers-MS-7E71`; CPU: `AMD Ryzen 7 9700X 8-Core Processor`; RAM: `29.9 GiB`
- Artifact/posting storage: `/dev/nvme1n1p2` (`Samsung SSD 990 EVO 2TB`, 2000.4 GB), `ext4` mounted at `/`
- Environment: `GOWORK=off TMPDIR=/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/tmp GOMAXPROCS=2 GOFLAGS=-p=2 -mod=readonly -modfile=/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/baseline-build-inputs/build.mod`

## Relaxed durability

| Backend | Throughput median (ops/s) | min-max | CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) | read-ready median (s) | read/write-ready median (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| badger | 270.26 | 224.05-346.57 | 18.02% | 14.040 | 26.156 | 30.653 | 1.507 | 3.314 (3/3) | 3.340 (3/3) |
| treedb | 475.78 | 336.74-476.16 | 15.28% | 8.412 | 14.987 | 18.284 | 1.507 | 3.382 (3/3) | 3.398 (3/3) |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp | GC cycles | flushes | checkpoints |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| badger | 0.7 (3/3) | 932.2 (3/3) | 2177.0 (3/3) | 0.3 (3/3) | 146.3 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb | 0.7 (3/3) | 1019.5 (3/3) | 4.6 (3/3) | 4.6 (3/3) | 152.2 (3/3) | unavailable (0/3) | 0.0 (3/3) | 1004.0 (3/3) | 0.0 (3/3) |

TreeDB durability diagnostics (timed-phase deltas unless marked high-water):

Each value below is an independent per-metric median across the 3 accepted repeats. The row is not one observed repeat.

| public batch writes | public batch durable writes | point appends | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1004 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) / 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) |

| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 (3/3) | 0 (3/3) | unavailable (0/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) |

TreeDB throughput delta versus durability-matched Badger: **+76.04%** (gate: no worse than -3%).

## Durable durability

| Backend | Throughput median (ops/s) | min-max | CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) | read-ready median (s) | read/write-ready median (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| badger | 267.05 | 189.21-267.79 | 15.28% | 14.046 | 26.106 | 30.396 | 1.507 | 3.323 (3/3) | 3.358 (3/3) |
| treedb | 195.57 | 194.90-197.77 | 0.62% | 17.678 | 35.007 | 40.605 | 1.507 | 3.365 (3/3) | 3.404 (3/3) |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp | GC cycles | flushes | checkpoints |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| badger | 0.7 (3/3) | 932.9 (3/3) | 2177.0 (3/3) | 0.3 (3/3) | 149.5 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb | 0.8 (3/3) | 1017.1 (3/3) | 4.5 (3/3) | 4.6 (3/3) | 159.6 (3/3) | unavailable (0/3) | 0.0 (3/3) | 0.0 (3/3) | 0.0 (3/3) |

TreeDB durability diagnostics (timed-phase deltas unless marked high-water):

Each value below is an independent per-metric median across the 3 accepted repeats. The row is not one observed repeat.

| public batch writes | public batch durable writes | point appends | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 (3/3) | 998 (3/3) | 0 (3/3) | 0 (3/3) / 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 998 (3/3) | 0 (3/3) | 0 (3/3) |

| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 (3/3) | 0 (3/3) | unavailable (0/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) |

TreeDB throughput delta versus durability-matched Badger: **-26.77%** (gate: no worse than -3%).

## Decision

**STOP advancement/integration at this phase; keep Badger as the production default.** Logical parity, schema, posting, unsupported-feature, and recovery gates passed. The performance decision applies only to this benchmark-minimal topology and workload.

## Profile artifacts

Separate TreeDB profile runs were collected after the decision matrix; their throughput is diagnostic and is not part of the A/B decision.

- Relaxed TreeDB: [`profiles/treedb-relaxed.pprof`](profiles/treedb-relaxed.pprof) and [`profiles/treedb-relaxed-top.txt`](profiles/treedb-relaxed-top.txt).
- Durable TreeDB: [`profiles/treedb-durable.pprof`](profiles/treedb-durable.pprof) and [`profiles/treedb-durable-top.txt`](profiles/treedb-durable-top.txt).

These artifacts do not by themselves attribute cost between gomap and Dgraph integration, establish I/O wait, or prove a causal explanation for either throughput delta.

## Raw artifacts and reproduction

Reproduce from the recorded Dgraph SHA with `TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir /absolute/path/outside/repository/NEW_DIR`. Paths below are relative to the artifact root; each JSON retains its exact original absolute command and raw path.

- `badger-relaxed-r1`: `live/badger-relaxed-r1/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `badger-relaxed-r2`: `live/badger-relaxed-r2/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `badger-relaxed-r3`: `live/badger-relaxed-r3/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `treedb-relaxed-r1`: `live/treedb-relaxed-r1/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `treedb-relaxed-r2`: `live/treedb-relaxed-r2/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `treedb-relaxed-r3`: `live/treedb-relaxed-r3/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `badger-durable-r1`: `live/badger-durable-r1/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `badger-durable-r2`: `live/badger-durable-r2/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `badger-durable-r3`: `live/badger-durable-r3/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `treedb-durable-r1`: `live/treedb-durable-r1/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `treedb-durable-r2`: `live/treedb-durable-r2/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`
- `treedb-durable-r3`: `live/treedb-durable-r3/result.json`; Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`; gomap `v0.6.2-0.20260721120929-84e3f6652651 => /home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap@(devel)`; dirty `false`

Excluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha `VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large logical size with small allocated size comes from sparse preallocated files, so logical and allocated bytes must be read together. TreeDB logical write bytes use its public-batch counter only when the point-append counter proves no direct-point writes occurred; otherwise they fail closed as unavailable. Write amplification remains unavailable because an equivalent physical-byte counter is not exposed. Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are exposed; vlog writes are not relabeled as flushes.
