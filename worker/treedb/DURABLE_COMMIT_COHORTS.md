# Durable posting commit cohorts

The explicit durable TreeDB command-WAL COW backend uses one ordered submitter, two reusable
caller-owned aggregate records and one completion owner. The submitter copies ready operands into an
unassigned native batch while the preceding aggregate waits for its physical barrier. Root
preparation runs once at seal; ordered append and installation transfer the original batch to the
completion owner, which drives the existing contiguous durable-prefix certificate before ACK. Every
transaction retains its original timestamp and callback. No goroutine or future is allocated per
request. Unsupported profiles retain the synchronous FIFO `mvcc.CommitGroupAt` owner; relaxed and
externally recoverable stores retain their existing paths and recovery authority.

Begin establishes one monotonic deadline using the engine's existing 200us collection budget. An
idle aggregate submits immediately. A successor drains ready work and may wait for more until that
deadline, predecessor completion, a resource bound or shutdown. Processing already-ready work is
bounded by request/byte/physical-work limits. Producer Flush/Wait fences wake progress and preserve
completion ordering; they do not cut a small successor at temporary queue emptiness. A sealed native
aggregate carries scheduling metadata so the engine never adds a second collection window. This
metadata creates no force ticket, log position, durability certificate or new recovery owner.
Ordinary generic engine callers age the same budget from their first pending registration rather
than restarting it after the preceding physical I/O.

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

Mutation buffers are scrubbed and returned to the existing pool only after original durable
completion. Validation vectors are reused, borrowed operands are cleared after native staging, and
duplicate-version identities own their keys until seal or Abort. Invalid appends roll back only
their own new identities. Oversized scratch is dropped at the cut boundary. Completion delivery
remains FIFO. Callback functions run asynchronously and may reenter Close; callback execution order
is not guaranteed. Close drains every admitted request before closing the owned database. Requests
still waiting for capacity recheck closed state before admission.

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
| `dgraph.treedb.commit_pipeline`                   | `one_window_v3` for native staging, `fifo` for unsupported profiles                                   |
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
