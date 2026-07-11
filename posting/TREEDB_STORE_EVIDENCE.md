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

The exact-head race run passed (`posting` 238.597s, `worker/treedb` 1.650s).

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
| Point get                                            |        3,953 |         4,083 |         +3.3% |       5,462 |        5,566 |      +1.9% |            56 |             58 |
| All-version scan (256 items)                         |       98,842 |       106,588 |         +7.8% |      51,599 |       51,839 |      +0.5% |           583 |            586 |
| Grouped random seek                                  |        2,082 |         2,134 |         +2.5% |         383 |          383 |       0.0% |             4 |              4 |
| Single write                                         |        4,560 |         4,887 |         +7.2% |       4,567 |        4,605 |      +0.8% |            14 |             15 |
| 16-key atomic batch                                  |       18,822 |        19,813 |         +5.3% |      18,320 |       17,758 |      -3.1% |            64 |             65 |
| Exact-key eight-version scan with 32 prefix siblings |        3,820 |         4,353 |        +14.0% |       1,448 |        1,688 |     +16.6% |            30 |             33 |
| Close/reopen                                         |   19,619,457 |    21,430,323 |         +9.2% |  42,914,100 |   42,951,609 |      +0.1% |         2,388 |          2,398 |

The worst median latency delta is +14.0%, the worst byte-allocation delta is +16.6%, and the worst
allocation-count delta is +10.0%; all three are within the 20% gate. Profiling the prior write
staging identified avoidable map/string, envelope-copy, and mutation-slice costs. The adapter now
deep-owns caller key/value bytes in a pooled, scrubbed byte arena and reuses mutation buffers. This
reduces a 16-key batch from the prior 81 allocations to 65, versus 64 for direct MVCC, without
weakening the caller-copy boundary.

Post-close file-size medians with fixture setup excluded are 12.70 direct versus 12.70 adapter disk
bytes/item for single writes and 10.24 versus 10.20 for 16-key batches. These are logical file-size
deltas, not physical-device write amplification.

The callback path has its own five-sample depth-16 and sustained depth-128 benchmark:

```sh
GOWORK=off go test ./posting -run '^$' \
  -bench '^BenchmarkTreeDBStoreCallbackPipeline$' \
  -benchtime=300ms -count=5 -benchmem
```

At depth 16, the synchronous median was 74.7 us per 16 commits; `TxnWriter` measured 84.9 us
(+13.6%), with 72,272 versus 73,037 B/op (+1.1%) and 272 versus 307 allocs/op (+12.9%). At depth
128, crossing the 64-entry queue capacity, synchronous measured 566.2 us and `TxnWriter` measured
644.4 us (+13.8%), with 576,661 versus 584,272 B/op (+1.3%) and 2,180 versus 2,439 allocs/op
(+11.9%). This benchmark reports bounded FIFO scheduling cost; the correctness claim does not depend
on timing. Deterministic tests hold the worker, prove the 65th queued commit backpressures, verify
duplicate-key/same-timestamp admission order, prove `TxnWriter.SetAt` returns while an admitted
commit is in flight, and prove `Close` waits for storage completion.

A resource-envelope smoke command used `/usr/bin/time -v` around the benchmark process
(`-benchtime=100ms -count=1`). It reported 4.58s user CPU, 1.05s system CPU, 112% CPU, 5.01s wall
time, and 585,856 KiB maximum RSS. This process-level RSS includes the Go test binary and all
sequential fixtures; per-operation heap cost is the `B/op` metric above.

Local raw evidence and SHA-256 digests:

| Artifact                                          | SHA-256                                                            |
| ------------------------------------------------- | ------------------------------------------------------------------ |
| `/tmp/dgraph-21-bench-fifo/adapter-final.txt`     | `6986169b5448524ec680a007739670617a9d1ed3ee681f345dfecc167be9c114` |
| `/tmp/dgraph-21-bench-fifo/callback-final.txt`    | `93a6da46356d79de6d9e580739476aa6c6f72fa7eacb8fd03284c45498c0d2e0` |
| `/tmp/dgraph-21-bench-fifo/resource-envelope.txt` | `4a7caf738b6bb22e0b2b43a90bc422ce4b9c2d0eaf00748ae79421c61c7c989c` |

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
