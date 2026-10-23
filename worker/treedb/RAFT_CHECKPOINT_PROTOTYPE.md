# Raft-owned TreeDB materialization prototype

This is an opt-in, single-Alpha diagnostic prototype. It is not production replay closure, snapshot
transfer, performance qualification, or authorization to merge. The mutation RPC contract is
unchanged: normal transaction success still depends on the existing Alpha/Zero consensus workflow.
Posting callbacks mean complete installation backed by retained Alpha Raft input under an explicit
external recovery owner; they do not claim a local TreeDB command-WAL barrier. Standalone TreeDB
durable commits and the existing durable selector retain their own durability contracts.

## Selection and supported surface

Start both Zero and Alpha with `--survive=filesystem`. Alpha requires:

```
--posting-store='backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true;'
```

Alpha rejects non-filesystem survival, multiple Alphas/learners, ACL, and CDC. Zero's survival
configuration is an operator/test requirement; Alpha cannot infer Zero's local configuration from
its RPC protocol. The owner binds the absolute Alpha WAL directory and a versioned application
identity. A fresh posting directory or an existing external-owner directory is required. Switching
an existing normal TreeDB posting directory to this mode is rejected.

The supported semantic path is normal scalar/UID posting mutations and Alpha Oracle Delta
commit/abort decisions, plus fresh/identical scalar/UID schemas and types. Built-in initial
schema/types are deterministic application-versioned bootstrap state, initialized before replay.
Schema changes over existing schema, count/reverse/vector/delete schema, drop/predicate deletion,
namespace deletion, restore/import/external streaming/KV workflows, peer snapshot install, and
membership changes are rejected. Background rollup is disabled; external-mode GC, physical
compaction, discard-floor advancement and MVCC pruning are rejected. Existing backend-specific
operational routes remain fail closed.

Both normal TreeDB selectors and this prototype explicitly select `cow_ordered`.
`PostingStoreRuntimeStatus` identifies the prototype as `raft_checkpoint`, with
`durable_commits=false`: a TreeDB command-WAL fsync is not its acknowledgement boundary. The
external recovery owner, rather than the ordinary relaxed-store constructor, is required to open
this path.

## Checkpoint and restart protocol

The existing serial semantic apply loop runs mutations and Delta application. Delta application
drains the existing posting writer and then advances Oracle. The prototype reuses the existing
posting adapter and MVCC publication path; it adds no batching queue or second publisher.

A pending-free application cut binds:

- immutable physical TreeDB root and every captured file/dependency digest;
- external owner identity and application/schema contract version;
- Alpha Raft/group identity, term/index and configuration state;
- Oracle `MaxAssigned` visibility and group checksum;
- zero pending transactions and replay starting at the next Raft index.

Pending prewrites live in Oracle rather than the posting database. They are never excluded from
required history: a nonempty pending set prevents a new certificate, and all Raft history after the
previous certified cut remains. A scalar applied hint cannot authorize recovery or deletion.

Ordering is
`durable exact image + CURRENT certificate -> authorize retention -> durable local Raft snapshot metadata -> delete covered old log files`.
`CreateSnapshot` cannot exceed the externally certified index, and received snapshots cannot bypass
the missing install protocol. Payload truncation is disabled for the guarded WAL. External Ready
synchronization tracks dirty files at append/rotation, flushes those files and changed directory
names before Raft metadata, and includes committed applications even when `Ready.MustSync` is false.

On startup the owner validates digests and reconstructs working storage from exactly the immutable
certified image. Working files, including an incidentally flushed uncertified suffix, are discarded.
Dgraph validates identity/version, term, commit bound and contiguous suffix availability, restores
Oracle and applied frontiers from the certificate, and replays committed entries before health
admission. An independently saved `CheckpointIndex` ahead of the certificate is ignored. Missing
history or mismatched identity fails startup.

## Bounds and limits

Checkpoint triggers are 1,024 applied entries or 16 MiB retained input at a pending-free cut;
orderly shutdown also attempts a pending-free checkpoint. New mutation admission reserves credits
under a mutex and stops at 4,096 retained entries or 64 MiB of accepted/persisted recovery debt.
Commit/abort decisions retain progress capacity. At the limit the Zero stream continues consuming,
skips coalescible watermark-only Deltas so a blocked transaction cannot grow idle watermark history
forever. All transaction decisions remain, including aborts that race prewrite application; the
leader continues aborting old transactions. This can delay read visibility under pressure. Decision
records and existing bounded in-flight Ready/RPC work may exceed the mutation-admission thresholds;
these thresholds are not an asserted total RSS cap or recovery-time SLA.

This prototype obtains exact-root recovery by copying the whole physical image at each checkpoint.
Each image is limited to 8 GiB / 8,192 files. The Gomap owner retains current and previous images.
This O(database image) copying and its checkpoint latency are explicit scalability limits. It is not
the final pinned root implementation. The prototype disables reclamation/rollup; sustained
maintenance equilibrium, snapshot transfer, complete admin lifecycle, tail and memory budgets remain
unqualified.

## Focused validation

Run with `GOWORK=off` and task-specific `TMPDIR` on `/mnt/fast4tb`:

```
go test ./worker ./posting ./raftwal \
  -run 'TestRaftCheckpoint|TestCheckpointCertificate|TestServerStateTreeDB|TestTreeDBStore' \
  -count=1
```

The normal-mutation/Delta test certifies an old scalar value, applies a newer committed suffix plus
pending prewrite, flushes/closes working storage, reopens the exact old certified value, replays to
the newer value and reconstructed pending transaction, aborts it and certifies the next complete
cut. It exercises both a crash after retention publication and certificate publication before
retention, with an intentionally bogus independently saved applied hint. Validation tests reject
identity/term/pending/history gaps and prove reservation backpressure. WAL tests refuse uncertified
pruning/install, preserve history across authorization-only restart, and track dirty dependencies
across rotation without syncing all retained files per Ready.

These focused tests are correctness diagnostics. The separate actual-server validation driver
records filesystem-survival settings, acknowledged RPC data, SIGKILL/restart verification, source
identity and diagnostic comparison results. Neither orderly-close tests nor one actual-server run
establishes power-loss qualification or production replay closure.
