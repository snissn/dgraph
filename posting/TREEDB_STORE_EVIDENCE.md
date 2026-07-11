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
callback errors, bounded FIFO callback-path commit pipelining and backpressure,
close-versus-in-flight-commit ordering, pooled-arena data scrubbing, atomic batches, pruning,
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

The exact-head race run passed (`posting` 240.325s, `worker/treedb` 1.347s).

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
| Point get                                            |        4,028 |         4,039 |         +0.3% |       5,462 |        5,566 |      +1.9% |            56 |             58 |
| All-version scan (256 items)                         |       98,651 |       107,818 |         +9.3% |      51,600 |       51,841 |      +0.5% |           583 |            586 |
| Grouped random seek                                  |        2,105 |         2,160 |         +2.6% |         383 |          383 |       0.0% |             4 |              4 |
| Single write                                         |        4,702 |         5,043 |         +7.3% |       4,568 |        4,591 |      +0.5% |            14 |             15 |
| 16-key atomic batch                                  |       19,095 |        20,201 |         +5.8% |      17,309 |       17,632 |      +1.9% |            64 |             65 |
| Exact-key eight-version scan with 32 prefix siblings |        3,795 |         4,334 |        +14.2% |       1,448 |        1,688 |     +16.6% |            30 |             33 |
| Close/reopen                                         |   24,379,237 |    25,711,095 |         +5.5% |  42,945,776 |   42,871,654 |      -0.2% |         2,387 |          2,397 |

The worst median latency delta is +14.2%, the worst byte-allocation delta is +16.6%, and the worst
allocation-count delta is +10.0%; all three are within the 20% gate. Profiling the prior write
staging identified avoidable map/string, envelope-copy, and mutation-slice costs. The adapter now
deep-owns caller key/value bytes in a pooled, scrubbed byte arena and reuses mutation buffers. This
reduces a 16-key batch from the prior 81 allocations to 65, versus 64 for direct MVCC, without
weakening the caller-copy boundary.

Reopen latency uses the dedicated paired subbenchmark below, which alternates direct and adapter
order within every sample to avoid filesystem phase bias. Its median per-sample overhead metric was
+4.8%; the table conservatively computes +5.5% from the separate direct and adapter medians. Reopen
`B/op` and allocation counts come from the ordinary separately-accounted rows in the matrix.

```sh
GOWORK=off go test ./posting -run '^$' \
  -bench '^BenchmarkTreeDBStoreAdapterOverhead/Reopen/PairedLatency$' \
  -benchtime=500ms -count=5
```

Post-close file-size medians with fixture setup excluded are 12.70 direct versus 12.69 adapter disk
bytes/item for single writes and 10.26 versus 10.20 for 16-key batches. These are logical file-size
deltas, not physical-device write amplification.

The callback path has its own five-sample depth-16 and sustained depth-128 benchmark:

```sh
GOWORK=off go test ./posting -run '^$' \
  -bench '^BenchmarkTreeDBStoreCallbackPipeline$' \
  -benchtime=300ms -count=5 -benchmem
```

At depth 16, the synchronous median was 74.0 us per 16 commits; `TxnWriter` measured 83.9 us
(+13.4%), with 72,281 versus 72,926 B/op (+0.9%) and 272 versus 307 allocs/op (+12.9%). At depth
128, crossing the 64-entry queue capacity, synchronous measured 573.4 us and `TxnWriter` measured
630.3 us (+9.9%), with 576,650 versus 584,109 B/op (+1.3%) and 2,180 versus 2,439 allocs/op
(+11.9%). This benchmark reports bounded FIFO scheduling cost; the correctness claim does not depend
on timing. Deterministic tests hold the worker, prove the 65th queued commit backpressures, verify
duplicate-key/same-timestamp admission order, prove `TxnWriter.SetAt` returns while an admitted
commit is in flight, and prove `Close` waits for storage completion.

A resource-envelope smoke command used `/usr/bin/time -v` around the benchmark process
(`-benchtime=100ms -count=1`). It reported 4.57s user CPU, 1.16s system CPU, 108% CPU, 5.27s wall
time, and 583,424 KiB maximum RSS. This process-level RSS includes the Go test binary and all
sequential fixtures; per-operation heap cost is the `B/op` metric above.

Local raw evidence and SHA-256 digests:

| Artifact                                                | SHA-256                                                            |
| ------------------------------------------------------- | ------------------------------------------------------------------ |
| `/tmp/dgraph-21-bench-fifo/adapter-scrub-final.txt`     | `8836380452603c6093872b938e41d61be445d436fdc69360c24c79a735a54843` |
| `/tmp/dgraph-21-bench-fifo/callback-scrub-final.txt`    | `a76ca1f06e86a9824957051d943625cf730c1e55958a23eff6e1bdaf76449107` |
| `/tmp/dgraph-21-bench-fifo/reopen-paired-final.txt`     | `dcf91428f3fbf72b1acd1b24ede3b05c0fa1bcc48f57b6b3e51a369c51d165ba` |
| `/tmp/dgraph-21-bench-fifo/resource-scrub-smoke.txt`    | `451844504760b0a1f49bcaf88816867cc894c2a1a974c8de4c38bf6c3f7b1827` |
| `/tmp/dgraph-21-bench-fifo/resource-scrub-envelope.txt` | `849530b3ff16db53fd0dad646e1ac634ce5d0d317cdef95cf920543994026dc4` |

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
