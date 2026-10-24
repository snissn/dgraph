# Dgraph Badger vs TreeDB live durability A/B

Resolved Zero/Alpha survival profile: **process**. Startup and Alpha-restart configuration and posting-route witnesses are retained with each row. The recovery timer measures a clean Alpha restart while Zero stays running; this is not abrupt-crash or power-loss certification.

- Repeats per cell: 3
- Workload fingerprint: `d0772456e60ba13fd017bb5db10bdf397fdf87f87fab743555a5d3835fc70ad7`
- Logical parity and restart gate: **PASS**
- Comparisons never mix durability classes.

The restart column retains `recovery_seconds`, the legacy HTTP-health boundary, which is not equivalent across backends. The additive read-ready and read/write-ready columns report `recovery_read_seconds` and `recovery_read_write_seconds`: elapsed time from the same Alpha exec boundary through full posting validation, then a new CommitNow ACK and verified fresh read. These are clean Alpha restart measurements with Zero alive, not abrupt-crash or power-loss certification.

- Host: `mikers-MS-7E71`; CPU: `AMD Ryzen 7 9700X 8-Core Processor`; RAM: `29.9 GiB`
- Artifact/posting storage: `/dev/nvme1n1p2` (`Samsung SSD 990 EVO 2TB`, 2000.4 GB), `ext4` mounted at `/`
- Environment: `GOWORK=off TMPDIR=/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/tmp GOMAXPROCS=2 GOFLAGS=-p=2`

## Relaxed durability

| Backend | Throughput median (ops/s) | min-max | CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) | read-ready median (s) | read/write-ready median (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| badger | 9938.15 | 9911.11-10238.24 | 1.48% | 0.354 | 0.697 | 0.908 | 1.507 | 3.275 (3/3) | 3.276 (3/3) |
| treedb | 9564.26 | 9512.16-9590.19 | 0.34% | 0.375 | 0.717 | 0.851 | 1.507 | 3.329 (3/3) | 3.331 (3/3) |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp | GC cycles | flushes | checkpoints |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| badger | 0.4 (3/3) | 933.1 (3/3) | 2177.0 (3/3) | 0.4 (3/3) | 149.4 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb | 0.4 (3/3) | 1026.7 (3/3) | 4.6 (3/3) | 4.6 (3/3) | 149.1 (3/3) | unavailable (0/3) | 0.0 (3/3) | 959.0 (3/3) | 0.0 (3/3) |

TreeDB durability diagnostics (timed-phase deltas unless marked high-water):

Each value below is an independent per-metric median across the 3 accepted repeats. The row is not one observed repeat.

| public batch writes | public batch durable writes | point appends | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 959 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) / 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) |

| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 (3/3) | 0 (3/3) | unavailable (0/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) |

TreeDB throughput delta versus durability-matched Badger: **-3.76%** (gate: no worse than -3%).

## Durable durability

| Backend | Throughput median (ops/s) | min-max | CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) | read-ready median (s) | read/write-ready median (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| badger | 837.21 | 812.96-892.65 | 3.93% | 3.321 | 12.461 | 15.915 | 1.508 | 3.312 (3/3) | 3.423 (3/3) |
| treedb | 337.29 | 331.58-437.57 | 13.20% | 11.373 | 29.276 | 35.994 | 1.508 | 4.344 (3/3) | 4.422 (3/3) |

| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp | GC cycles | flushes | checkpoints |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| badger | 0.5 (3/3) | 932.8 (3/3) | 2177.0 (3/3) | 0.4 (3/3) | 157.0 (3/3) | unavailable (0/3) | 1.0 (3/3) | unavailable (0/3) | unavailable (0/3) |
| treedb | 0.6 (3/3) | 1026.7 (3/3) | 4.6 (3/3) | 4.6 (3/3) | 161.6 (3/3) | unavailable (0/3) | 0.0 (3/3) | 46.0 (3/3) | 0.0 (3/3) |

TreeDB durability diagnostics (timed-phase deltas unless marked high-water):

Each value below is an independent per-metric median across the 3 accepted repeats. The row is not one observed repeat.

| public batch writes | public batch durable writes | point appends | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 (3/3) | 1101 (3/3) | 0 (3/3) | 46 (3/3) / 8 (3/3) | 46 (3/3) | 7 (3/3) | 8 (3/3) | 1063 (3/3) | 0 (3/3) | 0 (3/3) |

| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 (3/3) | 0 (3/3) | unavailable (0/3) | 0 (3/3) | 0 (3/3) | 0 (3/3) |

TreeDB throughput delta versus durability-matched Badger: **-59.71%** (gate: no worse than -3%).

## Decision

**STOP advancement/integration at this phase; keep Badger as the production default.** Logical parity, schema, posting, unsupported-feature, and recovery gates passed. The performance decision applies only to this benchmark-minimal topology and workload.

## Profile artifacts

Separate TreeDB profile runs were collected after the decision matrix; their throughput is diagnostic and is not part of the A/B decision.

- Relaxed TreeDB: [`profiles/treedb-relaxed.pprof`](profiles/treedb-relaxed.pprof) and [`profiles/treedb-relaxed-top.txt`](profiles/treedb-relaxed-top.txt).
- Durable TreeDB: [`profiles/treedb-durable.pprof`](profiles/treedb-durable.pprof) and [`profiles/treedb-durable-top.txt`](profiles/treedb-durable-top.txt).

These artifacts do not by themselves attribute cost between gomap and Dgraph integration, establish I/O wait, or prove a causal explanation for either throughput delta.

## Raw artifacts and reproduction

Reproduce from the recorded Dgraph SHA with `TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir /absolute/path/outside/repository/NEW_DIR`. Paths below are relative to the artifact root; each JSON retains its exact original absolute command and raw path.

- `badger-relaxed-r1`: `live/badger-relaxed-r1/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `badger-relaxed-r2`: `live/badger-relaxed-r2/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `badger-relaxed-r3`: `live/badger-relaxed-r3/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `treedb-relaxed-r1`: `live/treedb-relaxed-r1/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `treedb-relaxed-r2`: `live/treedb-relaxed-r2/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `treedb-relaxed-r3`: `live/treedb-relaxed-r3/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `badger-durable-r1`: `live/badger-durable-r1/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `badger-durable-r2`: `live/badger-durable-r2/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `badger-durable-r3`: `live/badger-durable-r3/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `treedb-durable-r1`: `live/treedb-durable-r1/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `treedb-durable-r2`: `live/treedb-durable-r2/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`
- `treedb-durable-r3`: `live/treedb-durable-r3/result.json`; Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`; gomap `v0.6.2-0.20261023204715-29a2fdf153ce`; dirty `false`

Excluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha `VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large logical size with small allocated size comes from sparse preallocated files, so logical and allocated bytes must be read together. TreeDB logical write bytes use its public-batch counter only when the point-append counter proves no direct-point writes occurred; otherwise they fail closed as unavailable. Write amplification remains unavailable because an equivalent physical-byte counter is not exposed. Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are exposed; vlog writes are not relabeled as flushes.
