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
certificate, contract `dgraph-normal-delta-v6` binds successful and rejected proposal identity,
StartTs, canonical content SHA256, rejection disposition, and observation time into the application
metadata. A reused retry key with different content is a deterministic rejection. The canonical hash
excludes the Raft index, so a real retry at a different log position retains its identity. Local
wall time never retires external retry outcomes. Metadata exceeding 1 MiB retains the old
certificate and history. No new queue, publisher, pool, or storage layer is introduced.

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
A certificate now includes its canonical pending state and restores mutation capacity before the
ordered abort arrives. Reopen restores that pending transaction without its pruned prewrite log; a
later decision remains admissible. This proves the admission/decision/certificate mechanism;
continuous live traffic and unavailable-Zero behavior remain separate operational gates.

## Remaining consumer completion sequence

| Obligation                    | Existing path and required proof                                                                                                                                     | Status                                                                                                                 |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Immediate acknowledged suffix | CommitNow response, abrupt Alpha and Zero termination before read/drain, restart and verify all acknowledged transaction decisions                                   | Initial-root and nonzero native-peer cut process proofs green; power-loss qualification separate                       |
| Pending-state progress        | Canonical overlay certifies unresolved state; retained debt admission pauses mutations until a cut; ordered decisions bypass both pending/debt limits                | Canonical pending cut/reopen/debt-release proof green; continuous live load open                                       |
| Fresh schema/index/type       | Existing serial schema writer and normal indexed posting path; certify, recover old cut, replay, query indexes and duplicates                                        | Fresh basic schema and indexed pending commit/abort replay proof green                                                 |
| Existing schema rebuild       | Scalar tokenizer rebuild uses synchronous existing aggregation with logical tombstones; captured schema/root and old-cut replay agree                                | Scalar tokenizers/count/reverse/type/list/upsert/unique/lang/no-conflict proofs green; vectors guarded                 |
| Drop/namespace/restore/import | Each operation needs ordered input closure, pending disposition, schema/cache invalidation and a root cut; external files require digest-bound retained authority    | Ordered standard drop/namespace proof green; restore/import remain guarded                                             |
| Peer snapshot/rejoin          | Export a certified root and dependencies at a Raft snapshot; validate transfer; atomically install with receiver identity and frontiers before retention/read health | Native RPC, startup crash cuts, actual three-voter join/rejoin and immediate-ACK process crash green; learners guarded |
| Maintenance                   | Rollup, MVCC pruning, vlog GC/rewrite and compaction must respect current/previous certified roots and active readers through every crash cut                        | Physical GC/compaction and ordered S/local P baseline+prune green; authenticated explicit S publisher active           |
| Operational routing           | Backup/export/admin and shutdown must use capability-aware store paths; no `nil` Badger dereference or bypass of owner lifecycle                                     | Ordered admin and cancellable close implemented; inherited external-file routes guarded                                |
| Retry identity/bounds         | Canonical StartTs/content binding; ordered semantic expiry and bounded metadata admission; qualify long-load limits                                                  | Focused replay and rejection proof green                                                                               |
| Allocation and runtime        | Existing bounded mutation arena reused; capture public admission/apply/allocation profile only after correctness and root runtime grant                              | Open                                                                                                                   |

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
suite. Live Ready/RPC and voter membership scheduling are exercised by the native peer proof below.

The checksummed version-1 accepted intent is limited to 4 MiB, receiver application metadata to 1
MiB and source certificate to 2 MiB. A single archive is capped at 8 GiB. Acceptance syncs dirty WAL
files, directory and metadata, then archive and directory, then atomic intent and directory; the
owner cannot publish CURRENT earlier. Completion syncs exact snapshot/HardState before WAL reset,
then syncs reset before deleting the intent. Intent deletion is synced before archive deletion,
leaving at worst an unreferenced archive after a cleanup crash. Startup validates accepted authority
before reclaiming only owned unaccepted `snapshot-install-*.tar` files and syncing the directory.
Accepted archives survive cleanup; corrupt/missing accepted input fails before any cleanup.
Unexpected nonregular staging objects refuse startup. One serialized Ready receiver stages at most
one bounded archive per process, and restart reclaims interrupted, unaccepted downloads.

The Gomap dependency pinned in the repository's [go.mod](../../go.mod) uses retained exact roots
without the separate captured-WAL engine. Consumer delivery uses the published module without a
local replacement; development diagnostics may retain explicitly witnessed replacement builds. The
prototype document's image-copy byte accounting and maintenance limits describe its original
evidence only. Consumer results must identify the actual dependency commit and root mechanism; a
bounded certificate file size is not the size of the retained database or recovery working set.

## Native peer transport proof (production integration in progress)

Snapshots explicitly opt in with additive `Snapshot.transfer_format` equal to
`dgraph-treedb-checkpoint-v1`. Legacy Badger requests keep their existing field meanings; `SinceTs`
is never a format discriminator. Native chunks are bounded to 1 MiB, total input is capped at 8 GiB,
and receiver requests have a five-minute transport deadline. Missing, mixed-mode and malformed
formats fail before authorization. Native export binds the exact CURRENT generation and digest.
Incoming source metadata cannot regress the receiver's already drained semantic or Oracle frontier.

ConfChange and empty entries share the existing ordered semantic apply channel. A membership change
forces certification including canonical pending state; its captured ConfState is retained in
outgoing snapshot metadata. Forwarded snapshots update transport source context without substituting
live membership. Ordinary voter membership is enabled. Learner startup remains guarded pending its
own lifecycle qualification. Every external-recovery startup proposes an ordered empty Delta and
waits for its applied prefix before publishing health. A fresh peer's local Commit=0 is insufficient
evidence of catch-up. Snapshot completion cannot publish initial health before this startup barrier.

A stale source certificate is a preparation failure, not permission to Advance an unapplied Ready.
The receiver first persists only current term/vote while retaining its old durable
commit/snapshot/suffix authority. After draining earlier ordered entries it stops the old Raft
instance, verifies semantic progress equals the old durable commit, and recreates Raft with that
semantic ConfState and the same WAL. The application state, admitted queries, Oracle and retry state
are preserved. Only InitialState's ConfState is overridden in the runtime storage wrapper; all
log/snapshot operations delegate to the durable store. Crashes reconstruct from the ordinary
certificate plus retained WAL. Any accepted install intent or uncertain publication forbids this
retry and requires process reopen.

The public native test uses the actual registered gRPC handler, rejects a stale source cut, restarts
an actual etcd Raft instance, receives a fresh MsgSnap, installs the new root and advances Ready
only after matching WAL persistence. It also serializes an actual queued ConfChange and abruptly
terminates a subprocess immediately after election-promise sync. Malformed header/chunk/terminal
controls leave no staging file or accepted intent. These are focused cross-store and transport
proofs, not a complete production feature or power-loss gate.

## Replicated expiry and local history reclamation

`PostingFloorRequest{version:1,floor:S}` is an explicit ordered transition through the existing
proposal and acknowledgment path. S must precede both the **preceding ordered** assigned frontier
and every pending transaction start. Its result is independent of local readers, maintenance and
wall clocks. A normal mutation at or below S is rejected before retry lookup, including a retry
whose previous outcome was retired. Only nonzero StartTs outcomes covered by S are retired. The
certificate binds S and its transition index separately from physical floor P. Malformed Oracle
commit decisions with a nonzero CommitTs at or below StartTs are pure rejections before effects. A
dedicated ordered assigned frontier reconstructs from certificate+Delta, avoiding the process
Oracle's intentionally monotone high-water mark during in-process reopen/retry.

Ordinary task admission checks S before Oracle/cache access, waits without retaining posting pins,
and then rechecks and acquires a task-lifetime pin before any cache/storage access. The maintenance
capability excludes new request pins, drains existing requests, then drains raw transaction/iterator
handles under the root gate. Its private Store avoids recursive reader-gate acquisition and cannot
escape the capability lifetime. Old unadmitted write handles are epoch fenced. The same
whole-request drain protects native root replacement.

A complete baseline pass reconstructs every posting main/split anchor at B=P+1, drains every
callback, clears global and pending transaction cached Lists while preserving pending deltas and
conflict versions, then advances/prunes physical history. This preserves generic MVCC's strict
read/commit admission above P, including split anchors. P never exceeds S and can lag because of
readers or newer pending starts. Partial baseline, prune or ambiguous certification errors seal
admission until exact-root reopen; they never resume readers through a partially rewritten list.

The production runner forces a certificate, including canonical newer pending state, before Finish.
An explicit FinishWorking capability also remains available with the identical previous certificate
and validated, synced retained replay history. It changes no certified frontier or retention; a
crash then reconstructs S, pending state and application suffix from the previous exact cut plus
WAL. Current/previous physical roots remain pinned through physical GC/compaction.

Focused tests cover two successive split baselines against an unpruned control, B/B+1 views,
callback/cancellation failures, stale capability handles, task admission before cache/Oracle,
old-root reopen after uncertified mutation, ordered-floor replay, content collisions, stale retries
after signature retirement, and normal commit/reopen above a certified physical floor. The
continuous-pending operational and full lifecycle gates remain separate.

## Canonical pending checkpoints

Version 5 binds a fixed-size overlay format/count/logical-charge reference to the same certified
TreeDB root as posting MVCC, schema, S/P, Oracle, retry state and captured membership. Every
accepted normal mutation executes the existing edge/index pipeline once against an isolated
canonical Txn. Fresh inferred schema is a private write-context override until admission succeeds.
Before any schema/raw/Oracle publication, structurally valid canonical output must fit 4,096 pending
records and 64 MiB total logical charge. Refusal is a pure ordered no-op and preserves earlier
proposals in the same transaction. Commit/abort decisions bypass these limits. Unknown
read/write/callback failures retain their causes and stop certification; they are never capacity
refusals.

Each accepted proposal atomically replaces one private raw TreeDB record plus its incremental
manifest under the existing external owner. The record contains StartTs, sealed posting/index
deltas, exact conflict versions and conflict keys, and shouldAbort. It excludes disposable cached
Lists and local observation clocks. Fixed-width timestamp/version slots and omission of query-only
cache keys keep logical charge independent of local P and physical layout. Retained-state limits are
not a total RSS bound: staging and index/delete fanout retain the ordinary execution cost.

Checkpointing reads only fixed-size accounting; it neither serializes all pending transactions nor
builds a second aggregate hash protocol. The exact DATA root is the whole-overlay authority and each
canonical record has its own checksum. Startup/install scans the bounded record set once, verifies
keys/checksums/count/charge, and atomically restores Oracle pending state before suffix apply or
health. Drained commit/abort writers remove their record before the semantic frontier can certify.
Canonical state is restored directly, never by re-executing raw edges under a later schema. Schema
transitions that affect pending predicates are deterministic pre-effect refusals; exact no-ops avoid
legacy asynchronous Zero abort requests. Ordered schema timestamps use the preceding ordered Delta
frontier, not a potentially later process-local Oracle hint.

Focused coverage includes indexed pending commit and abort after checkpoint/reopen with original
prewrite WAL pruned, real full-count capacity/refusal and decision release, corruption/accounting
refusal, pending-aware physical baselines, and pending state through native RPC/install crash cuts.
See [RAFT_PENDING_STATE.md](RAFT_PENDING_STATE.md) for canonical format and charge details. These
focused proofs do not replace continuous-load or full schema/admin qualification. The authenticated
publisher below selects S explicitly; no automatic local clock/frontier expiry has been introduced.

## Operational history retirement

The authenticated GraphQL `/admin` mutation is the maintained publisher of replicated S:

```graphql
mutation {
  retirePostingHistory(floor: 123) {
    floor
    response {
      code
      message
    }
  }
}
```

It applies to the addressed Alpha's Raft group through the existing ordered proposal/response path.
It requires a configured `--security 'token=...'` and the matching `X-Dgraph-AuthToken`, in addition
to the existing IP whitelist, Guardian-of-Galaxy middleware and admin audit logging. There is no
unconfigured-OSS authentication bypass for this operation. Multi-group operators must explicitly
request each group's floor and handle independent eligibility errors.

This request authorizes expiry of reads and mutation retries at or below the selected floor. It is
an operator decision; no local clock, read pin, physical floor, or inferred latest timestamp selects
it. It must not regress S and must strictly precede the preceding ordered assigned frontier and all
unresolved starts. CommitNow's Zero response may precede Alpha's ordered Delta: an ineligible
request returns an error without effects and can be retried with the same explicitly chosen floor.
An exact current floor is an idempotent success. A successful transition forces certification before
its response, while physical P can still lag active readers. Existing admitted readers drain before
physical reclamation; future old requests fail before cache or storage access.

`raftproduction/verify_history_floor.py` exercises the actual authenticated public API,
missing/wrong credentials, too-high and regressing floors, delayed eligibility, a successful floor
response followed by adjacent Alpha+Zero SIGKILL, nonzero certificate recovery, stale read refusal
and an admissible fresh exact-index query. Its failed attempts remain in diagnostic artifacts. This
is process-crash correctness evidence, not a power-loss or performance qualification.

## Actual voter lifecycle diagnostic

`raftproduction/verify_native_peers.py` launches a filesystem-durable Zero with replication factor
three and three filesystem-durable Alphas. It creates an indexed predicate and unresolved normal
transaction, forces an explicit S certificate, then joins voters two and three. Each join must log a
native checkpoint containing one pending transaction before its startup replay/health barrier. The
client commits the imported transaction through voter three and verifies indexed reads at all peers.
It kills voter two, advances committed data and S on the quorum, restarts voter two, and requires a
new native install before verifying the newer value. Finally it sends SIGKILL to all Alphas and Zero
immediately after a new CommitNow ACK, restarts all four, and verifies every indexed value at every
peer. HTTP calls between ACK and the adjacent kill syscalls are excluded.

The first development run, `artifacts/dgraph/native-peers-server-01`, passed: receiver two installed
cut 16/pending 1, receiver three cut 20/pending 1, and receiver two rejoined at cut 34/pending 0.
The imported StartTs 5 committed at 13; the immediately killed ACK used StartTs 22/CommitTs 23. Its
binary SHA256 is `2e5bbec221a78b24e627acfbfc1a69b9c88299fde90d2c463b803b36ddb5df0e`. That binary
includes the concurrently developing schema lane and is a source-working-tree diagnostic, not final
exact-head qualification. Each attempt retains complete commands, result and server logs. SIGKILL
preserves page cache; this proof makes no power-loss, timing, learner, or full production-tier
claim.

Ordered standard drop, namespace-ban and unavailable-Zero close semantics are documented in
[RAFT_ADMIN_LIFECYCLE.md](RAFT_ADMIN_LIFECYCLE.md). Sustained lifetime bounds and production-tier
qualification remain open even after focused semantic and actual-server controls pass.

The v6 consumer binds fixed pending/terminal accounting and sequenced Zero checksum authority to the
exact root. Intrinsic State/Delta/native Snapshot/floor controls do not consume persistent generic
retry signatures. This closes arbitrary timestamp-less control-key retention, not S=0 history
growth; see [RAFT_TERMINAL_STATE.md](RAFT_TERMINAL_STATE.md) for the soft admission limit and
retirement contract.

## Resource and physical-maintenance diagnostics

`raftproduction/verify_retention_resources.py` uses normal CommitNow, authenticated explicit S, the
existing maintenance scheduler, pending transactions and orderly Close/reopen. It keeps sampled
combined Alpha/Zero RSS, allocated posting/Raft/private-temporary file bytes, exact source and
runtime witnesses, certificates and all failures. `--maintenance-wait-seconds=310` with
`--require-physical-floor-progress` requires certified P to advance during that observation window.
This is logical history retirement, not evidence that physical GC ran.

The [offline observer](raftproduction/resourcewitness/README.md) accepts both original version-1
local-replacement manifests and version-2 published-module manifests. The published path requires
the binary's module version/checksum, the direct Dgraph source pin and `go.sum`, and the pristine
Gomap checkout's official module-file checksum to agree. Nested evidence modules are excluded by
maintained Go module rules, while the complete clean Git tree identity is recorded separately.
Ignored build outputs can still invalidate the module checksum: use a source-only checkout and keep
offline source hashing outside workload timing. Neither manifest mode substitutes for a reproducible
build or changes the resource diagnostic's qualification limits.

The original `repeat` payload remains the default. `--payload-mode=shake256 --value-bytes=16384`
uses distinct, deterministic high-entropy ASCII for each overwrite so compression cannot reduce the
entire fixture to a repeated byte. Per-phase `*-storage-files.json` packets retain posting file
names, device/inode identities, logical/allocated bytes and their packet hashes. Together with
retained `/debug/store` snapshots these can distinguish observed file replacement/deletion and space
reuse across certificate rotations. The walks are non-atomic; they do not authorize deletion, prove
engine reachability, identify a GC cause, or establish a steady-state bound by themselves. No extra
GC loop, administrative compact endpoint, background-cadence change or automatic S advance is
introduced. The driver continues to report `equilibrium_qualified=false`.

### Bounded preparation of decision materialization

The external posting adapter uses the same immutable, engine-derived producer-work estimator as
ordinary TreeDB construction. `SetEntry`/`Delete` reject work before copying the rejected operand: a
nonempty fitting prefix reports the existing `ErrTxnTooBig` precommit cursor boundary; an entry that
cannot fit an empty transaction reports terminal `ErrTreeDBEntryTooLarge`. The estimator shapes
input; canonical COW admission remains authoritative. The finite queue bounds request count, not a
claim that all queued adapter arenas are charged to the engine's in-flight budget. Engine limits,
checkpoint cadence and the serial external `Apply` route are unchanged. The sizing constants contain
no live engine pointer and remain valid across checkpoint installation, which reopens with the
original options and a new directory.

One Dgraph transaction may materialize through several independently atomic native storage calls at
its original commit timestamp. This is not raw multi-chunk atomicity: a read bypassing the Oracle
may observe a physical prefix. The original OracleDelta success boundary still requires **all**
posting callbacks to drain before pending/terminal resolution, cache publication, Oracle/checksum
advancement, semantic progress and subsequent certification/retirement. A terminal oversized later
entry exits producer retry without resubmitting accepted prefixes and joins all already-owned
callbacks before propagating its original error. Unrelated Badger retry behavior is preserved. A
callback/storage error is never a successful visibility or checkpoint boundary.

On failed decision materialization the old certificate and retained original Raft records remain
recovery authority. A controlled reopen discards uncertified working changes and replays those
records at their original timestamps; no intermediate prefix is certified. Local controlled reopen
tests are not claims of process-crash coverage. A single raw oversized native request still refuses
under the unchanged budgets, and external Checkpoint must never be called recursively from an
`Apply` callback to make an oversized request appear to fit.

### Ready durable cuts

A fresh, explicitly selected TreeDB `raft_checkpoint` Alpha uses the Ready-cut
Raft storage format. Backend and durability selectors are normalized and validated
before either storage authority is opened. Fresh Badger and Zero directories keep
the legacy format. Existing directories are opened by their actual format: an
existing legacy directory is not converted by selecting `raft_checkpoint`, and an
existing Ready directory is not reinterpreted as legacy. Competing format roots or
orphan Ready fragments without an authoritative root fail closed. Startup logs the
actual format; this cold receipt is separate from posting-store sync counters.

The Ready journal owns one selected append file and a checksummed
`raft-ready.current` root. A complete cut contains full HardState, identity and
checkpoint hint, optional snapshot metadata/data, and its contiguous entry suffix.
Header, body and terminal witness checksums bound the complete frame. Save streams
caller bytes into the file before exposing its derived index; it does not retain
caller payloads or allocate a second complete Ready image. The selected physical format is version2: a fully zero-initialized fixed-capacity
file with a distinct logical end. Two generation-bound checksummed prefix slots
carry only sequence/end/integrity evidence for those same Ready frames. A dirty
required Sync writes one slot and performs one data-persistence barrier (Linux
fdatasync, full file sync elsewhere). Initial allocation/size and every new base
are fully synced before root publication. Published files are not resized or
truncated to logical end on Close. Existing experimental version1 Ready roots
are explicitly refused; legacy wal.meta directories remain supported. An ordinary required
Sync flushes that initialized file once. Creating a file, rotating an encryption
key or replacing a generation also requires its own namespace/key publication
fences; these are not claimed to be one-barrier ordinary Ready operations.

Alpha and Zero keep their synchronous Ready/Save/required-Sync/apply/Advance
ordering. The shared required-Sync helper returns a terminal new-format error
before apply or Advance. Invalid pre-write input is terminal without poisoning;
uncertain append, sync, close, generation or namespace failures poison the owner
and require reopen. A later successful same-descriptor sync is not a recovery
proof after writeback EIO. Legacy transient-error retry behavior is unchanged.

Recovery validates each complete physical envelope before length-driven metadata
allocation and semantic replay. It rejects committed-prefix, election, commit,
snapshot and contiguous-log regressions. Only a positively incomplete, unwitnessed final append is discarded; complete
corrupt frames, malformed prefix witnesses, unexplained nonzero reserve or a
valid witness beyond the verified prefix are errors. Cold accepted-install
identity and normal replay share this physical interpretation. A newer valid
witness covering damaged bytes never falls back to an older slot: reordered
unacknowledged persistence and later corruption of acknowledged data cannot be
distinguished from that image. Conservative refusal is intentional, just as the
former stream refused a damaged complete frame within physical EOF. No claim
of detection of arbitrary coordinated whole-file rollback/erasure is made. Accepted snapshot-install intent,
archive and receiver identity are validated before tail repair or cleanup.
Encrypted frames use independent random per-record CTR nonces, including when a
reopen reuses the sequence and byte offset of a discarded tail; encryption keys
continue to use the maintained key registry.

Only the posting owner's durable certificate authorizes retention. Cold generation
replacement streams the certified snapshot and **all** newer saved entries and
HardState into one complete base, syncs it, publishes and syncs the root, then
retires the predecessor and syncs the directory. It never dual-writes `wal.meta`.
Unselected generation files and owned unpublished-root temporary files are reclaimed
on reopen only after accepted-install and selected-generation validation succeed.
An unfinished accepted install retains its original archive/intent until exact
receiver reconciliation and durable completion. Legacy install fault stages remain
covered separately from new cut/root publication boundaries. Ordinary append stays within initialized space. Capacity pressure uses the same
private base replacement. Pressure reserves checked geometric headroom for every
retained prefix, including prefixes above16MiB; a single larger atomic input stays
legal. Cold certificate replacement separately bounds normal retained reserve to
64KiB–16MiB, without discarding live bytes. These are physical sizing policies,
not legal Ready-size limits or a change to certificate cadence. An oversized
generation grants no permanent oversized normal reserve after certification. Synchronous replacement borrows only the quiescent journal-owned I/O workspace
under the original storage lock. Source reads keep the exact predecessor open;
full in-place encrypted chunks are consumed before reuse. Success rebinds output
to the installed generation, while failure discards private buffered bytes and
retains fail-stop poisoning. There is no pooled or recycled entry/index ownership.
Initialization and root/retirement fences remain whole-lifecycle work. The active journal can grow through these
replacements while certification is blocked; this is not a new background checkpoint,
retention cadence, or claim of a bound independent of the existing certificate.
