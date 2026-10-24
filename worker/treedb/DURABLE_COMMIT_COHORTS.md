# Durable posting commit cohorts

The explicit TreeDB backend routes both synchronous and callback durable `CommitAt` requests through
one bounded FIFO owner. It drains requests that are already ready into `mvcc.CommitGroupAt`,
retaining each transaction's timestamp, mutations and completion. The group is one atomic storage
publication and waits for its durable result before releasing requests. Work arriving during that
disk wait accumulates for the next group. There is no adapter batching timer or goroutine per
storage request. Relaxed and externally recoverable stores retain their existing paths and recovery
authority.

This replaces the eight-worker conflict scheduler. Distinct versions of the same logical key can
share a durable publication instead of waiting for separate disk confirmations. MVCC validation and
the storage engine still own atomic publication, dependency durability and the acknowledgement
boundary.

## Bounds and lifecycle

Admission owns at most 65 requests, including the active cohort, the channel and one carried
request. Waiting for admission capacity does not hold the lifecycle mutex. Under that mutex the
closed/root-epoch checks, wait-group admission and queue handoff remain atomic with Close and
external root installation.

Each selected group owns at most 4 MiB of staged key/envelope bytes. A transaction larger than that
budget remains one atomic transaction and runs alone. A request that would exceed the remaining
group budget is carried to the next group, retaining its admission slot and buffers. Queued input
still shares the 65-request ownership bound; the group budget is not a maximum transaction size or a
bound on arbitrary input.

Mutation buffers are scrubbed and returned to the existing pool only after the storage call
completes. Completion delivery remains FIFO. Callback functions run asynchronously and may reenter
Close; callback execution order is not guaranteed. Close drains every admitted request before
closing the owned database. Requests still waiting for capacity recheck closed state before
admission.

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

| Stat                                    | Meaning                                                  |
| --------------------------------------- | -------------------------------------------------------- |
| `dgraph.treedb.durable_requests_total`  | Requests selected into scheduler cohorts                 |
| `dgraph.treedb.durable_cohorts_total`   | Scheduler cohorts attempted, including singleton cohorts |
| `dgraph.treedb.durable_cohort_size_max` | Lifetime largest selected cohort                         |

These are scheduling counters, not successful commits or physical syncs. Validation subdivision can
issue more than one storage operation for a cohort. Compare their timed deltas with TreeDB's public
write and actual file-sync diagnostics. A multi-request adapter cohort can require one MVCC write
even when the engine's concurrent group-participant counter remains zero.

Tests exercise mixed synchronous/callback callers, history and duplicate timestamps, validation
isolation, ambiguous failure without retry, carried and oversized requests, Close, bounded
admission, and abrupt exit after grouped acknowledgements with verified persistent value pointers.
Performance acceptance requires the real Dgraph comparison; a held-request test establishes
reachable grouping but does not establish natural workload throughput.
