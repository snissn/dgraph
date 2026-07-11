<!--
SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
SPDX-License-Identifier: Apache-2.0
-->

# TreeDBStore conformance and performance evidence

This artifact covers Dgraph issue #21 at gomap `3a3e3c72a1a8f7cb208d2770bbcf4bcb7d0332be`
(`v0.6.2-0.20260711114710-3a3e3c72a1a8`). TreeDB remains explicit and experimental; Badger remains
the runtime default.

## Correctness and recovery

`treedb_store_test.go` runs golden and randomized managed-timestamp histories against both
BadgerStore and TreeDBStore, including metadata, discard markers, tombstones, binary keys/values,
point reads, forward/reverse/prefix/exact-key iteration, callback errors, atomic batches, pruning,
concurrent readers, and sticky iterator errors. `TestTreeDBStoreDurableCrashReopen` commits a
versioned envelope in a subprocess using the durable command-WAL profile, exits without `Close`, and
verifies its timestamp, metadata, discard marker, payload, and digest after reopen.

Lower-level gomap recovery remains authoritative for storage faults. The pinned module includes
`mvcc.TestCommitAtDurableProcessCrashRecovery` (including a truncated WAL tail) and
`mvcc.TestPruneDurableProcessCrashAfterDeleteBatch`.

Exact validation commands:

```sh
GOWORK=off go test ./posting ./worker/treedb -count=1
GOWORK=off go test -race ./posting ./worker/treedb -count=1
```

The race run passed (`posting` 226.092s, `worker/treedb` 1.417s).

## Adapter microbenchmarks

Command:

```sh
GOWORK=off go test ./posting -run '^$' \
  -bench '^BenchmarkTreeDBStoreAdapterOverhead$' \
  -benchtime=300ms -count=5 -benchmem
```

Both direct and adapter rows use the same gomap MVCC owner with `ProfileCommandWALRelaxed` and
`CommitRelaxed`; setup is outside the timer. This isolates Dgraph envelope/seam cost without hiding
it behind fsync. Durable acknowledgement is covered by the subprocess recovery test and will be
compared end-to-end in issue #17. Values are consumed through `Item.Value`, matching the zero-copy
posting-list decode path. Results are five-sample medians:

| Operation                                            |      Direct MVCC |      TreeDBStore |  Delta | Direct allocs | Adapter allocs |
| ---------------------------------------------------- | ---------------: | ---------------: | -----: | ------------: | -------------: |
| Point get                                            |      4,011 ns/op |      4,085 ns/op |  +1.8% |            56 |             58 |
| All-version scan (256 items)                         |     99,683 ns/op |    107,192 ns/op |  +7.5% |           583 |            586 |
| Grouped random seek                                  |      2,068 ns/op |      2,110 ns/op |  +2.0% |             4 |              4 |
| Single write                                         |      4,569 ns/op |      5,212 ns/op | +14.1% |            14 |             21 |
| 16-key atomic batch                                  |     18,695 ns/op |     22,371 ns/op | +19.7% |            64 |            121 |
| Exact-key eight-version scan with 32 prefix siblings |      3,822 ns/op |      4,302 ns/op | +12.6% |            30 |             33 |
| Close/reopen                                         | 25,559,172 ns/op | 24,687,304 ns/op |  -3.4% |         2,401 |          2,399 |

The worst median adapter delta is +19.7%, within the 20% gate. A separate five-sample write run
reports post-close file-size deltas with fixture setup excluded: single-write medians are 12.68
direct versus 12.66 adapter disk bytes/item; 16-key batch medians are 10.25 direct versus 10.15
adapter disk bytes/item. These are logical file-size deltas, not physical-device write
amplification.

A resource-envelope smoke command used `/usr/bin/time -v` around the benchmark process
(`-benchtime=100ms -count=1`). It reported 4.49s user CPU, 1.18s system CPU, 109% CPU, 5.17s wall
time, and 584,960 KiB maximum RSS. This process-level RSS includes the Go test binary and all
sequential fixtures; per-operation heap cost is the `B/op` metric above.

Local raw evidence and SHA-256 digests:

| Artifact                                             | SHA-256                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------ |
| `/tmp/dgraph-21-bench/adapter-full-final.txt`        | `6efe3f034bd901ea8ce1fa92d3c1b2310ba3d79d8817748447c990f52e991ba4` |
| `/tmp/dgraph-21-bench/adapter-write-disk-final.txt`  | `e6ce404f4a71af156f03a315f0be418aef86482134fbc2a66e32bcaaa8014811` |
| `/tmp/dgraph-21-bench/adapter-resource-smoke.txt`    | `931138356ccdaeb687dd4e47881d9db49a157ece4def7b9eff2bddeabcc788cc` |
| `/tmp/dgraph-21-bench/adapter-resource-envelope.txt` | `f0600d0b6d7cccd5206293014fea076565f8af61fca43daa21f51c00a6567a6f` |

## Shared Badger seam regression guard

The same five-sample, 500ms benchmark was run at base `bf3f16b3e` and this branch. Median changes
were managed write -1.94% (4.753us to 4.661us), point read -0.51% (527.0ns to 524.3ns), and
all-version scan -1.40% (406.8us to 401.1us), with a -1.29% geomean and unchanged
`B/op`/allocations. Therefore the shared seam changes introduce no greater-than-3% Badger
regression.

Raw artifacts:

- `/tmp/dgraph-21-bench/badger-seam/base.txt`:
  `d0815293e66d12773f3be16b69db34b2a6d971a5d5b7073a899747aec978cb48`
- `/tmp/dgraph-21-bench/badger-seam/branch.txt`:
  `4d4943af7fc07d1a11d0988ffa4cd4e76ef0c23cf8da38d83e09ca1aa598c329`

## Boundary and unsupported behavior

Ordinary iterator seeks retain one physical gomap snapshot; `Rewind` opens a fresh snapshot at the
same read timestamp. This is the supported Dgraph history: the posting watermark prevents a commit
below an executing read timestamp from publishing late. Point reads and new iterators at that
timestamp do observe newly published state, matching managed Badger. Reopening on every seek was
rejected after a directional run measured roughly 4.5x direct seek latency.

Nonzero TTL is rejected before any staged transaction mutation can publish. Stream import/export,
subscriptions, Badger protobuf operational workflows, cache controls, encryption/key registry, and
in-memory mode remain outside the benchmark-minimal adapter. `TreeDBStore` owns close, status,
stats, value-log GC, full storage compaction, discard-floor advancement, and pruning; issue #19 must
wire that owner surface into Alpha before the benchmark-minimal runtime gate can open.
