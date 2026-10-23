# Ordered posting administration and close

`raft_checkpoint` applies standard DropData, DropAll, namespace-scoped drop, predicate deletion and
non-root namespace banning through retained ordered proposals. It does not call physical
`DropPrefix`, clear retry identity, reset the Oracle or automatically retire semantic history.
Existing operator authentication and drop restrictions remain in the public callers.

Before effects, affected **canonical nonempty pending deltas** cause a typed semantic rejection.
Local query-cache keys cannot affect the decision. Unrelated pending transactions remain intact. The
operator must resolve affected transactions and submit a fresh request; retrying a previously
rejected proposal identity returns its original rejection. A duplicate successful pre-drop mutation
also retains its disposition and cannot resurrect dropped data. DropAll affects every pending
predicate; DropData preserves schema/type definitions. Predicate and namespace operations preserve
other namespaces. Namespace deletion's public caller records an explicit assigned StartTs in the
retained proposal, rather than deriving it later from a process-local Oracle.

A schema-transition capability blocks new request pins, drains admitted tasks and raw readers, and
fences stale write handles. The complete scan writes timestamped empty posting anchors, including
index, reverse, count and split keys. Schema/type records use MVCC tombstones. All final callbacks
drain before in-memory schema publication and cache invalidation. Initial reserved schema and types
are recreated at the same ordered timestamp for applicable drops. Old admissible timestamp views
retain their data and indexes. Partial write, callback or publication failures seal admission until
certified-root reopen; they cannot become a terminal no-op or resume mixed schema/data reads.

Non-root namespace banning additionally writes a versioned private marker into the same external
owner root. The root namespace cannot be banned. This marker survives DropAll, physical maintenance,
certification, reopen and native peer transfer. Uncertified markers are discarded with the rest of
the uncertified suffix. Startup validates the marker namespace before pending restoration or health.
Normal mutation/schema/type checks reject banned namespaces before effects. Task admission checks
before Oracle/cache access and again after acquiring the request pin. Root-namespace admission has
an allocation-free fast path. Internal historical readers are not public namespace admission.

## Orderly close and unavailable Zero

The existing node stop drains ordered application and forces a final certificate, including
canonical pending state, before closing the posting owner. GraphQL initialization is registered with
the admin closer and its schema query inherits cancellation. Its timestamp request uses buffered
delivery, so cancellation while Zero is unavailable cannot block subsequent dispatcher delivery.
This closes a real deadlock: initialization held the admin schema lock while waiting for Zero; the
subscription callback needed that lock before the admin closer could finish.

A Zero outage does not confer new commit authority. Already available fixed-timestamp queries can
continue; requests needing Zero's timestamp/tablet/decision service may fail or wait under their
existing contract. A failed additional mutation can request abort of its existing transaction, so
such a request is not a valid pending-survival control. The orderly-close control preserves an
untouched pending transaction, restarts Zero and Alpha, then resolves its restored conflict keys.
Zero intentionally aborts pre-restart transaction timestamps; the control verifies that ordered
abort and a fresh successful transaction. It does not claim an unresolved transaction can commit
across a Zero restart.

## Evidence and limits

`TestRaftLogicalDropCertifiedReplay` covers each operation, affected-pending pure refusal,
unrelated-pending preservation, repeated rejected and pre-drop identities, old-cut replay,
certification/reopen and historical indexed reads. Ban controls include root refusal, pre-cache task
rejection, schema/mutation refusal and marker corruption. The native transfer test carries the raw
ban marker into the receiver. `TestTimestampContextCancellationDoesNotBlockDispatcher` checks late
Zero delivery after caller cancellation and shutdown cancellation.

`raftproduction/verify_admin_lifecycle.py` is an actual-server process diagnostic with explicit
filesystem survival on both servers. It tests pending DropData refusal and fresh retry after abort,
DropData/schema retention, predicate deletion, DropAll followed immediately by adjacent Alpha/Zero
SIGKILL, restart, an available timestamp read during Zero outage, orderly Alpha close with canonical
pending state, Zero-authorized abort after reopen and a fresh committed mutation. Failed driver and
lifecycle attempts must remain in the artifact packet. This is not a power-loss, performance or
sustained-resource acceptance test.

Backup/restore/import, vector indexing, ACL and CDC remain separately guarded inherited
capabilities; this packet does not establish their replay closure. Learner membership is also
guarded. The existing `benchmark_minimal` tier label remains an explicit integration limitation.

## Retention and steady operation

The complete contract is in [RAFT_PENDING_STATE.md](RAFT_PENDING_STATE.md). Pending canonical state
is limited to 4096 rows and 64 MiB logical charge. Existing transaction growth may proceed at the
row count limit if total charge still fits. Commit/abort decisions can release capacity. Temporary
mutation staging, index fanout and individual posting-list memory are not bounded by that retained
state charge.

With default semantic floor S=0, successful and rejected retry identities are retained indefinitely.
Checkpoint metadata above 1 MiB cannot be certified: an ordinary checkpoint retains the previous
cut, and a forced checkpoint returns an error. Mutation proposal admission refuses at 4096
uncertified entries or 64 MiB retained recovery input, including reservations; transaction decisions
bypass this limit, and watermark-only Deltas are suppressed at the limit. These thresholds do not
bound all control traffic, log bytes, heap or disk. Timestamp-less control signatures are not
retired by S.

Continued semantic history retirement requires the explicitly authenticated operator request
`retirePostingHistory(floor: UInt64!)`. It is never chosen from a local clock, local physical floor
or latest unsequenced MaxAssigned. Its ordered floor must be monotone and strictly below the
preceding assigned frontier and every pending start. Physical P may lag S while readers drain.
Threshold controls and successful checkpoints are not proof of sustained equilibrium: retained
log/history, heap, controls and long-running admission require their own source-bound campaign.
Whole-lifetime control-signature bounds remain an open operational gate.

The developmental `admin-lifecycle-server-06` packet passed with binary SHA256
`86a62508ce5ad5059ec4caf3ac2de5c3fe3d855fff4093755d6862092245eb8d`. Its final outage close certified
index51/pending1, returned exit0, and reopened certificate51 before health. The subsequent Zero
abort and fresh mutation completed; final checkpoint58 had pending0. The binary includes concurrent
schema source and predates the final namespace timestamp-cancellation cleanup, so this is
working-tree process evidence, not exact-final-head performance acceptance. Attempts01–05 are
retained: parser failure, explicit unavailable Zero operation, captured shutdown deadlock and
incorrectly expected commit across Zero restart. Focused final race09 covers logical drops/bans and
timestamp cancellation; transfer-race10 verifies the namespace marker on native install.
