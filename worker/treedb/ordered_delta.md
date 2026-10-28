# Bounded ordered Delta admission

This source candidate keeps `DGRAPH_ORDERED_DELTA_PROVISIONAL=1` as an explicit
qualification switch. It remains disabled by default. Selection requires the
ordinary TreeDB durable `wal_on_sync` route, HardSync disabled, no Raft-checkpoint
prototype, and an open store. Other routes retain synchronous serial application.
The native completion progress repair tracked by GoMap #5301 must land and the
public dependency must be updated before enabling this path for production.

Zero's checksum sequence counts Raft proposals, rather than checksum-map changes.
Adjacent independent commit-bearing Deltas can prepare together when their full
maps compare equal and their sequences and frontiers do not regress. Each original
retains its proposal key, index, transaction list, source sequence, full checksum
map and frontier. No aggregate is split and no original frontier is copied to
another. A changed map (including another group's checksum), regressing sequence,
unknown field, abort/missing transaction, repeated identity, shared posting key,
watermark-only entry or nonconsecutive index remains a barrier.

The stream owner retains at most three original proposal operations and 64 KiB
of encoded input. Successor admission follows a real CommittedEntries observation,
not a Raft.Propose return. Retries retain the same key, immutable bytes,
registration and operation context; an already-started retry can still append a
duplicate. The single apply owner deduplicates original keys and prepares at most
three original Deltas across actual Ready/apply-channel arrivals. Snapshot custody
is bounded to 1 MiB payload/version-key bytes, 256 map entries and 64 keys per
transaction. Admission either retains a whole aggregate or changes no custody.

Each original's accepted callbacks drain before its cache invalidation, Oracle
update, proposal result and Applied watermark. Callback waiters cannot publish.
Any accepted materialization failure drains all reachable accepted work and
fail-stops before failed/successor publication; committed Raft originals remain
replay authority. Pre-commit transport or leadership changes retain retry
ownership. Stream replacement drains group-owned handles. Group shutdown may
cancel proposal waits, which is not a durable acknowledgement.

Tests cover strict-rule RED versus advancing-sequence grouping, full-map and
sequence boundaries (including zero/maximum/wraparound), immutable snapshots,
whole-window budgets, shared keys/duplicate IDs/abort/unknown barriers,
callback/error/panic/close/backpressure, real gRPC/Ready mechanism activation,
process-exit/replay/reopen and follower-to-leader completion. The three-Raft fixture
runs one live posting replica and later replays all persisted replica logs; it is
not a three-live-Alpha network test. The process-exit fixture explicitly syncs its
Raft log and does not certify production power-loss recovery.

The separate opt-in `clusterqualification` fixture runs a real Zero and three
concurrently live Alpha posting replicas through public transactions. Its test
executable can hold the actual native pre-sync boundary, preserve observed
watermark barriers, kill the leader at that cut, and verify survivor progress,
same-directory replay/catchup, conflicting decisions and all-Alpha process
recovery. It does not inject a Raft Sync or change process survival. See that
fixture's README for exact provisional dependency, source/binary binding,
conditional activation and remaining network/snapshot/crash-cut limits.

No experimental native tracing, analyzer or watermark bridge is included. The
already-merged Delta observer remains unchanged. Natural grouping diagnostics do
not establish throughput or sync savings. Production/default qualification still
requires a merged dependency pin, independent exact-head review, uninstrumented
performance, full recovery and multiple-live-replica gates, and current-head CI.
