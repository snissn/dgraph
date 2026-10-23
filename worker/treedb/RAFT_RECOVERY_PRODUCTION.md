# Raft recovery consumer integration (Dgraph #34 / Gomap #5207)

This branch extends the diagnostic checkpoint prototype toward production. Passing a focused
consumer test does not complete the production tier. The `benchmark_minimal` capability guard
remains until the obligations below have public caller proofs. No guard is removed merely because
TreeDB can execute the underlying write.

## Ordered rejection protocol

Normal concurrent transaction conflicts are decided by Zero. Alpha applies the retained ordered
Oracle Delta containing `CommitTs=0` for an abort. `posting.Txn.ShouldAbort` currently has no
writers; its existence does not show that an ordinary competing write can return an apply-time
conflict. Admission usually rejects malformed values before Raft. One reachable exception is a fresh
predicate with UID and scalar edges in the same proposal: both admission checks see no schema, while
serial application derives a schema from the first edge.

External recovery now validates all normal edges against existing or inferred schema before any
schema write, transaction registration, or posting mutation. Only an error from this pure pass is a
terminal semantic rejection. The caller receives the original validation error, and the entry
advances the applied frontier as a no-op. Earlier proposals in an existing transaction retain their
prior semantics; rejection does not manufacture a Zero abort or commit. Normal commit-now RPC error
handling still asks Zero to abort. Runtime `ErrConflict`, stale posting-list errors, schema/admin
failures, storage failures, and callback failures are not classified by message text or swallowed.
Parallel edge application joins all errors so a conflict cannot hide an I/O failure. An uncertain
apply failure stops progress before certification.

Before a certificate, rejection must be reproduced from the retained proposal and the schema state
reconstructed by preceding ordered input. A cached error alone is insufficient authority. After a
certificate, contract `dgraph-normal-delta-v3` binds successful and rejected proposal key/size
signatures, rejection text, and last-seen times into the application metadata. A retry returns the
same rejection. Existing clock expiry remains a pending-free, synchronously certified transition
before further apply; replay cannot independently expire outcomes. Metadata exceeding 1 MiB retains
the old certificate and history. No new queue, publisher, pool, or storage layer is introduced.

Password hashing consumes randomness. Normal admission converts plaintext before persistence; late
plaintext-password conversion during replay is refused rather than called a pure validation failure.
Extending this path requires a persisted deterministic result.

`TestRaftSemanticRejectionAndOrderedAbortReplay` exercises real normal mutation and Delta apply: a
mixed UID/scalar proposal on a fresh predicate, no schema or pending side effect on rejection,
competing writes resolved by ordered abort/commit, repeated rejected proposal, reopen from an older
certificate and recomputation without a restored error, and retry after certification and reopen. It
also verifies that runtime conflicts and joined uncertain errors do not qualify. This is a semantic
replay proof; it does not by itself prove RPC-to-immediate-crash acknowledgement.

The separate `verify_recovery.py` actual-server diagnostic then reproduces that committed rejection,
crashes both servers immediately after its response, and repeats CommitNow plus immediate crashes
three times. Fresh exact-index queries and UID reads verify all acknowledged values after each
restart. No network/read/checkpoint drain occurs between ACK and the adjacent SIGKILL syscalls. The
initial-root diagnostic replays from certificate index zero; it must not be described as a
nonzero-certificate suffix test or a power-loss test. Failed HTTP-envelope/parser attempts are
retained alongside the successful runs.

The semantic test also fills the actual retained-entry budget while one transaction stays pending.
The production proposal caller refuses another mutation, but a Delta decision passes debt admission.
After applying the ordered abort, a new certificate restores mutation capacity. This proves the
admission/decision/certificate mechanism; a live unavailable-Zero/old-transaction timeout campaign
is still required for operational liveness.

## Remaining consumer completion sequence

| Obligation                    | Existing path and required proof                                                                                                                                              | Status                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Immediate acknowledged suffix | CommitNow response, abrupt Alpha and Zero termination before read/drain, restart and verify all acknowledged transaction decisions                                            | Initial-root server proof green; nonzero cut proof open   |
| Pending-free progress         | Retained debt admission pauses mutations; ordered commit/abort bypasses debt; old transaction abort restores a cut and credits; unavailable Zero remains visible backpressure | Controlled real apply proof green; live Zero timeout open |
| Fresh schema/index/type       | Existing serial schema writer and normal indexed posting path; certify, recover old cut, replay, query indexes and duplicates                                                 | Fresh basic schema supported; index replay proof open     |
| Existing schema rebuild       | Existing asynchronous reindex/temporary Badger path must become a serial certified lifecycle with deterministic failure disposition                                           | Guarded; protocol open                                    |
| Drop/namespace/restore/import | Each operation needs ordered input closure, pending disposition, schema/cache invalidation and a root cut; external files require digest-bound retained authority             | Guarded; protocol open                                    |
| Peer snapshot/rejoin          | Export a certified root and dependencies at a Raft snapshot; validate transfer; atomically install with receiver identity and frontiers before retention/read health          | Adapter/WAL/startup crash proofs green; live RPC guarded  |
| Maintenance                   | Rollup, MVCC pruning, vlog GC/rewrite and compaction must respect current/previous certified roots and active readers through every crash cut                                 | Guarded; root-pinning API integration open                |
| Operational routing           | Backup/export/admin and shutdown must use capability-aware store paths; no `nil` Badger dereference or bypass of owner lifecycle                                              | Existing fail-closed guards; full production routes open  |
| Retry identity/bounds         | Proposal key/size compatibility remains inherited; stronger content binding and proven retry expiry behavior at long-load limits need qualification                           | Open                                                      |
| Allocation and runtime        | Existing bounded mutation arena reused; capture public admission/apply/allocation profile only after correctness and root runtime grant                                       | Open                                                      |

Peer installation uses the external owner's explicit generation/digest-bound export and authorized
install APIs. The source certificate has a different Raft ID and WAL-directory owner identity.
Periodic local certification retains pinned roots; network transfer exports a full physical image.
The receiver preserves source semantic metadata and rewrites only its local Raft ID. A trusted
authorization callback must accept a durable WAL-owned install intent before owner CURRENT changes.

The selected peer-install state machine preserves both the posting and Raft authorities:

1. Receive the complete checkpoint archive into a private WAL-directory staging file. Sync its
   contents and directory; record bounded size and digest. Old CURRENT and replay history remain.
2. After owner archive validation, the mandatory authorization callback checks source group,
   application version, snapshot index/term/captured ConfState and frontiers. Persist and sync an
   accepted install intent containing that exact snapshot, receiver identity and rewritten metadata,
   current term/vote, and the stable archive reference/digest. No CURRENT switch precedes this
   intent.
3. The owner publishes the receiver-identity certificate and installed root atomically. Reader
   admission/drain, accepted storage drain and stale-write epoch rejection protect the DB pointer
   replacement. The semantic caller must finish `TxnWriter.Flush` callbacks first; arbitrary adapter
   callbacks may call Close and are not waited on while the lifecycle gate is held. Schema and
   posting caches are reloaded before read health.
4. Complete the certificate-authorized local Raft snapshot install and sync its dependencies. Commit
   initially reaches only the snapshot index; later Ready suffix entries must be durable before a
   higher HardState commit can be published. Covered old history can now be removed.
5. Restore semantic/dedup/Oracle frontiers from the receiver certificate, then remove the intent and
   archive durably. Every startup with an intent completes steps 3–5 idempotently before normal
   certificate validation and health. It recognizes an already installed matching root/metadata.

An outgoing snapshot must retain its captured certificate ConfState; replacing it with live
membership breaks that binding. A stale requested generation/digest must fail export explicitly.
`TestRaftTreeDBTransferDrainsReadersAndRejectsStaleWriters` holds a real read transaction and
iterator and an admitted asynchronous storage commit while install waits, then checks root
replacement, receiver identity and stale-write refusal. `TestAcceptedSnapshotInstallCrashCuts`
abruptly exits at accepted-intent, Raft metadata sync and divergent-suffix reset; two startup
completions preserve term/vote and snapshot-only commit.
`TestRaftSnapshotStartupReconcilesCertifiedOwnerBeforeReplay` uses real mutation/Delta data and the
real owner authorization callback. It abruptly exits before owner Open, after CURRENT installation
and after WAL completion, repeats each startup, and checks the same installed generation,
value/schema, dedup, Oracle, captured ConfState and election state. Missing/corrupt archives,
corrupt intent and wrong WAL identity refuse before physical owner Open; an owner-namespace canary
and marker remain untouched. The archive lives outside owner namespaces until semantic and WAL
reconciliation completes. Owner internal publication crash cuts are covered by Gomap's transfer
suite. Live Ready/RPC snapshot acceptance and membership scheduling remain guarded until their own
proofs pass.

The checksummed version-1 accepted intent is limited to 4 MiB, receiver application metadata to 1
MiB and source certificate to 2 MiB. A single archive is capped at 8 GiB. Acceptance syncs dirty WAL
files, directory and metadata, then archive and directory, then atomic intent and directory; the
owner cannot publish CURRENT earlier. Completion syncs exact snapshot/HardState before WAL reset,
then syncs reset before deleting the intent. Intent deletion is synced before archive deletion,
leaving at worst an unreferenced archive after a cleanup crash. Orphan cleanup and total transfer
staging limits remain operational obligations before peer support is enabled.

The sibling Gomap implementation now uses pinned retained roots on an independent API-only branch
based on ordinary main, without the separate captured-WAL engine. The prototype document's
image-copy byte accounting and maintenance limits describe its original evidence only. Consumer
results must identify the actual dependency commit and root mechanism; a bounded certificate file
size is not the size of the retained database or recovery working set.
