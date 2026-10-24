# Durable posting commit cohorts

The explicit durable TreeDB command-WAL COW backend uses one ordered submitter, two reusable
caller-owned aggregate records and one completion owner. The submitter copies ready operands into an
unassigned native batch while the preceding aggregate waits for its physical barrier. Root
preparation runs once at seal; ordered append and installation transfer the original batch to the
completion owner, which drives the existing contiguous durable-prefix certificate before ACK. Every
transaction retains its original timestamp and callback. Storage submission uses no per-request
worker or future; existing asynchronous callback dispatch is unchanged. Unsupported profiles retain
the synchronous FIFO `mvcc.CommitGroupAt` owner; relaxed and externally recoverable stores retain
their existing paths and recovery authority.

An idle aggregate drains ready requests and uses the existing known-size ordered submission, without
opening an incremental builder or waiting to collect more input. While a predecessor is incomplete,
the successor stages arriving operands into one open native batch. It keeps collecting until
predecessor completion, a request/byte/physical-work/shared-staging bound, or shutdown. Temporary
queue emptiness, elapsed time and producer Flush/Wait do not seal that successor. Producer fences
only wake progress. Pressure can therefore prepare, append and install a full successor while the
preceding physical sync runs; a sparse successor can remain unassigned until it completes.

A sealed native aggregate carries scheduling metadata so the engine does not add a collection wait.
This metadata creates no force ticket, log position, durability certificate or new recovery owner.
Ordinary generic engine callers retain their existing collection policy; the adapter adds no timer.

This replaces the eight-worker conflict scheduler. Distinct versions of the same logical key can
share a durable publication instead of waiting for separate disk confirmations. MVCC validation and
the storage engine still own atomic publication, dependency durability and the acknowledgement
boundary.

## Bounds and lifecycle

Admission owns at most 65 requests, including the active cohort, the channel and one carried
request. Waiting for admission capacity does not hold the lifecycle mutex. Under that mutex the
closed/root-epoch checks, wait-group admission and queue handoff remain atomic with Close and
external root installation.

The two records share at most 4 MiB of staged key/envelope bytes. A transaction larger than that
budget remains one atomic transaction and runs alone. A request that would exceed the remaining
group budget is carried to the next group, retaining its admission slot and buffers. Queued input
still shares the 65-request ownership bound; the group budget is not a maximum transaction size or a
bound on arbitrary input.

Both submission paths retain the original floor, lifecycle and completion owners. Mutation buffers
are scrubbed and returned to the existing pool only after original durable completion. Validation
vectors are reused, borrowed operands are cleared after native staging, and duplicate-version
identities own their keys until seal or Abort. Invalid appends roll back only their own new
identities. Oversized scratch is dropped at the cut boundary. Completion delivery remains FIFO.
Callback functions run asynchronously and may reenter Close; callback execution order is not
guaranteed. Close drains every admitted request before closing the owned database. Requests still
waiting for capacity recheck closed state before admission.

## Validation and ambiguous failures

`CommitGroupAt` validates timestamps, logical keys, duplicate physical versions and the discard
floor before storage admission. These explicit validation errors permit recursive FIFO subdivision,
isolating invalid requests from valid neighbours. Repeated writes to the same key at the same
timestamp retain the existing last-write behavior by publishing the conflicting requests separately.
Distinct timestamps retain their history in one group. Validation subdivision can create several
storage calls within one selected cohort; those still share its total request and byte ownership.

An error containing `mvcc.ErrStorage` is never retried, even if joined with a validation sentinel.
The storage outcome may be ambiguous. Every affected request receives its error exactly once; a
failed sync attempt is not a durable acknowledgement.

## Runtime evidence

`/debug/store` exposes adapter scheduling separately from engine WAL grouping:

| Stat                                              | Meaning                                                                                               |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `dgraph.treedb.commit_pipeline`                   | `pressure_v4` for native staging, `fifo` for unsupported profiles                                     |
| `dgraph.treedb.durable_staged_input_bytes`        | Current shared staged-input custody; not peak heap                                                    |
| `dgraph.treedb.durable_overlap_submissions_total` | Submit finished before predecessor completion; physical overlap requires the separate sync-gate proof |
| `dgraph.treedb.durable_requests_total`            | Requests selected into scheduler cohorts                                                              |
| `dgraph.treedb.durable_cohorts_total`             | Scheduler cohorts attempted, including singleton cohorts                                              |
| `dgraph.treedb.durable_cohort_size_max`           | Lifetime largest selected cohort                                                                      |

These are scheduling counters, not successful commits or physical syncs. Validation subdivision can
issue more than one storage operation for a cohort. Compare their timed deltas with TreeDB's public
write and actual file-sync diagnostics. A multi-request adapter cohort can require one MVCC write
even when the engine's concurrent group-participant counter remains zero.

Tests exercise mixed synchronous/callback callers, history and duplicate timestamps, validation
isolation, ambiguous failure without retry, carried and oversized requests, Close, bounded
admission, and abrupt exit after grouped acknowledgements with verified persistent value pointers.
Performance acceptance requires the real Dgraph comparison; a held-request test establishes
reachable grouping but does not establish natural workload throughput.
