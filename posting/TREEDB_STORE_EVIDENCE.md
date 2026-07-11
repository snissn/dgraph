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
point reads, forward/reverse/prefix/exact-key iteration (including a codec-maximum logical key),
callback errors, true callback-path commit pipelining, close-versus-in-flight-commit ordering,
atomic batches, pruning, concurrent readers, and sticky iterator errors.
`TestTreeDBStoreDurableCrashReopen` commits a versioned envelope in a subprocess using the durable
command-WAL profile, exits without `Close`, and verifies its timestamp, metadata, discard marker,
payload, and digest after reopen.

Lower-level gomap recovery remains authoritative for storage faults. The pinned module includes
`mvcc.TestCommitAtDurableProcessCrashRecovery` (including a truncated WAL tail) and
`mvcc.TestPruneDurableProcessCrashAfterDeleteBatch`.

Exact validation commands:

```sh
GOWORK=off go test ./posting ./worker/treedb -count=1
GOWORK=off go test -race ./posting ./worker/treedb -count=1
```

The exact-head race run passed (`posting` 230.428s, `worker/treedb` 1.423s).

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

| Operation                                            | Direct ns/op | Adapter ns/op | Latency delta | Direct B/op | Adapter B/op | B/op delta | Direct allocs | Adapter allocs |
| ---------------------------------------------------- | -----------: | ------------: | ------------: | ----------: | -----------: | ---------: | ------------: | -------------: |
| Point get                                            |        4,016 |         4,030 |         +0.3% |       5,462 |        5,566 |      +1.9% |            56 |             58 |
| All-version scan (256 items)                         |       99,625 |       106,653 |         +7.1% |      51,599 |       51,839 |      +0.5% |           583 |            586 |
| Grouped random seek                                  |        2,084 |         2,132 |         +2.3% |         383 |          383 |       0.0% |             4 |              4 |
| Single write                                         |        4,689 |         4,913 |         +4.8% |       4,568 |        4,752 |      +4.0% |            14 |             16 |
| 16-key atomic batch                                  |       18,685 |        20,503 |         +9.7% |      17,847 |       20,480 |     +14.8% |            64 |             81 |
| Exact-key eight-version scan with 32 prefix siblings |        3,802 |         4,415 |        +16.1% |       1,448 |        1,688 |     +16.6% |            30 |             33 |
| Close/reopen                                         |   21,356,208 |    25,054,475 |        +17.3% |  42,915,626 |   42,858,942 |      -0.1% |         2,389 |          2,395 |

The worst median latency delta is +17.3% and the worst median byte-allocation delta is +16.6%, both
within the 20% gate. Profiling the prior write staging identified avoidable map/string,
envelope-copy, and mutation-slice costs. The adapter now combines each owned key and envelope in one
allocation and reuses scrubbed mutation buffers. Allocation _counts_ remain +2 for a single write
and +17 for a 16-key batch because the adapter contract must deep-own caller key/value bytes; the
generic MVCC fixture supplies already-owned mutations. Those counts are reported rather than treated
as zero-cost translation.

Post-close file-size medians with fixture setup excluded are 12.69 direct versus 12.69 adapter disk
bytes/item for single writes and 10.25 versus 10.20 for 16-key batches. These are logical file-size
deltas, not physical-device write amplification.

The callback path has its own five-sample depth-16 benchmark:

```sh
GOWORK=off go test ./posting -run '^$' \
  -bench '^BenchmarkTreeDBStoreCallbackPipeline$' \
  -benchtime=300ms -count=5 -benchmem
```

The synchronous one-at-a-time median was 74.4 us per 16 commits; `TxnWriter` callback pipelining was
88.6 us (+19.1%), with 71.08 versus 74.20 KiB/op and 288 versus 323 allocs/op. The callback samples
ranged from 87.4 to 99.1 us. This benchmark reports the bounded scheduling cost; the correctness
claim does not depend on timing. A deterministic test holds an admitted storage commit, proves
`TxnWriter.SetAt` has already returned, and proves `Close` does not return until the storage commit
finishes.

A resource-envelope smoke command used `/usr/bin/time -v` around the benchmark process
(`-benchtime=100ms -count=1`). It reported 5.20s user CPU, 1.36s system CPU, 95% CPU, 6.85s wall
time, and 570,488 KiB maximum RSS. This process-level RSS includes the Go test binary and all
sequential fixtures; per-operation heap cost is the `B/op` metric above.

Local raw evidence and SHA-256 digests:

| Artifact                                                 | SHA-256                                                            |
| -------------------------------------------------------- | ------------------------------------------------------------------ |
| `/tmp/dgraph-21-bench-fix/adapter-full-final.txt`        | `30c2339e1446d7e1ceca459bf23af0c2fbaaae3d0c3dd0c4a4f8f10ef8e5dd64` |
| `/tmp/dgraph-21-bench-fix/callback-pipeline-final.txt`   | `4b5fa4c6dcf6c6cbd025f853d157a0a81e4ee7a340b2c3f0b2760ce2d9d5aad0` |
| `/tmp/dgraph-21-bench-fix/adapter-resource-smoke.txt`    | `2f81e009ef383696562a53d606137121d66eaf0d3a9481ecd32c1d90f7c0979f` |
| `/tmp/dgraph-21-bench-fix/adapter-resource-envelope.txt` | `d1eed10d446cd3a63c535282512e604aae57e08cceb2c4f8ae8a9dcbaa092ce0` |

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
