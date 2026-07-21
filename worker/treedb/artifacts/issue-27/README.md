# Issue #27 final decision evidence

This directory is the normalized, committed copy of the durability-matched live A/B produced by
Dgraph commit `6bfe9e7b6d3e60c3147581af12db4443bd810809` with the exact certified Gomap candidate
`121462d80f1f11190776b4d54cab7ed34e413963`.

The Gomap candidate is bound to release
[`treedb-power-loss-cert-121462d80`](https://github.com/snissn/gomap/releases/tag/treedb-power-loss-cert-121462d80).
Its certificate seal SHA256 is `a244c19b2251e33ba32082b9946679666c8c5753084d0e5cd0f588aecfb50c30`;
the retained archive SHA256 is `6a0a59446acdea76f966f90cd593977e51a23ff701044e5920435a827d34004d`.

- [`report.md`](report.md) is the final decision report and links to the 12 raw live results under
  `live/`.
- [`context.txt`](context.txt) records the benchmark CPU, RAM, storage, filesystem, environment,
  dependency, commands, workload shape, and contamination gates.
- [`microbench/raw.txt`](microbench/raw.txt) is the five-sample adapter microbenchmark output.
- `profiles/` contains separate five-second TreeDB CPU profiles, top summaries, and profile-run
  metadata. Profile-run throughput is diagnostic and is not part of the A/B decision.

All logical parity, schema, posting, fail-closed unsupported-feature, and restart/reopen checks
passed. Both performance gates failed, so the decision is **STOP** and Badger remains the production
default:

| Durability | Badger median | TreeDB median | TreeDB delta |              Gate |
| ---------- | ------------: | ------------: | -----------: | ----------------: |
| relaxed    | 6112.00 ops/s | 4339.61 ops/s |      -29.00% | no worse than -3% |
| durable    | 1024.64 ops/s |  389.53 ops/s |      -61.98% | no worse than -3% |

The durable Badger row has high repeat variability (24.16% CV), so the exact percentage is not a
stable estimate of the long-run gap. TreeDB nevertheless misses the gate in every repeat and has
materially worse median p95/p99 latency. An earlier clean exact-candidate matrix is retained only in
scratch storage: it passed parity but predated schema-v3 diagnostic retention, so it was
deliberately not frozen as decision evidence.

## Measured bottlenecks

The TreeDB durable rows record a median 862 public durable writes and 862 command-WAL file syncs,
with zero value-log logical/file syncs and zero group commits, groups, participants, group syncs, or
maximum group size. This rules out the value-log double-barrier hypothesis for this workload.
Dgraph's TreeDB adapter processes callback-mode commits through one FIFO worker and invokes
`mvcc.CommitAt` serially. Gomap's dependency-closed group commit therefore has no overlapping
callers from which to form a group. This is a downstream integration bottleneck; it does not
invalidate Gomap's local group-commit evidence.

The relaxed rows have no foreground file syncs and no iterator-snapshot or leaf-log-segment
rotations, but each of 2400 point-successor calls inspects a median 62,525 sources: 26.052 sources
per call, with a process-lifetime maximum of 92. The separate relaxed CPU profile attributes 13.23%
of total sampled CPU cumulatively to background canonical flush work, with root publication,
allocation, and point-read/seek paths also visible. This evidence establishes a live-workload
source-fan-in and flush/publication residual; it does not yet prove that one leaf function alone
owns the full 29% gap.

The adapter microbenchmarks show near-neutral generic interface overhead for random seeks and
batches after state buildup. That narrows both residuals to concrete storage-path and
commit-scheduling behavior rather than the existence of the backend-neutral interface itself.

## Reproduction

The original run lived under `/mnt/fast4tb`; absolute scratch paths and exact commands remain in the
raw JSON for auditability. Reproduce from the recorded Dgraph SHA in a new immutable artifact
directory:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off \
  worker/treedb/run_durability_ab.sh \
  --artifact-dir /absolute/path/outside/repository/NEW_DIR
```

`NEW_DIR` is a placeholder for a new absolute directory outside the repository. Relative,
in-repository, dirty, contaminated, incomplete, or mixed-contract runs are rejected by the harness.
