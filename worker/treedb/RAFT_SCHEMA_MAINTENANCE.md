# Ordered schema and physical maintenance

The `raft_checkpoint` policy performs scalar tokenizer, count/reverse and type/list changes through
the existing ordered apply path. This is a focused production integration packet, not acceptance of
the whole TreeDB operational or production capability tier.

## Ordered schema transition

Proposal admission and committed application both call `checkRaftSupportedProposal`. Existing
predicates may change tokenizer membership, count/reverse, scalar type and scalar/list
representation. Before effects, a complete logical scan at the preceding ordered Oracle frontier
checks scalar conversion and refuses populated list-to-scalar or UID/scalar changes. Empty logical
data may change shape even when replicas retain different physical deltas or baselines. Read,
decoding and I/O errors cannot become terminal semantic rejections. Language, upsert, uniqueness and
other schema metadata transitions retain their separate guards. Fresh basic predicates and types
keep their existing semantic writer. Isolated timestamped type deletion uses the existing
schema-store tombstone path; type updates publish their in-memory definition after their schema
write succeeds. Vector and external-file lifecycle remain separate capabilities. Posting-data and
namespace deletion are documented by their separate ordered admin integration.

Committed input is already routed; application does not consult Zero's later tablet assignment. The
same rule is used by ordinary `runMutation`. Schema validation precedes materialization. The query
schema omits unavailable or removed tokenizers while the mutation schema supplies the intended
tokenizers to the existing index builders. A schema capability reuses the maintenance request and
root drain, epoch fencing, private snapshot reads and callback tracking; no global Store swap
occurs. It changes neither the physical floor nor the certificate/retention frontier. Old index keys
become timestamped empty lists, including split keys, rather than being physically removed by
`DropPrefix`.

The maintained `rebuilder` callback, posting-list decoder, index mutation code and posting-list
rollup produce index contents. A store-neutral snapshot scan feeds those algorithms. Chunks of 256
source lists are aggregated in temporary Badger at unique scratch timestamps, then merged into
complete posting lists at the schema transition timestamp. Reverse edges and both forward/reverse
count indexes reuse their maintained builders. Scalar-to-list conversion first copies complete
source main/split contents into scratch, preserving language-tagged and untouched values while its
conversion delta changes the scalar fingerprint. Scratch storage has no recovery authority: no
certificate references it, it is never imported on restart, and replay recomputes it from the
selected certified root plus retained ordered proposal. Scratch memory, disk and I/O scale with the
rebuilt predicate; the 256-list chunk is not a constant bound on an individual list or token's size.

Every final write callback drains before the builder returns. Only after that and the final schema
write succeeds is the complete query schema published. There is no background builder, new Raft
queue or independent root publisher. The semantic apply goroutine cannot certify a partial schema
operation. An I/O, callback or setup error is an uncertain apply failure; the current certificate
and required history stay selectable, and recovery reruns the operation. It is not converted to a
normal mutation's terminal semantic rejection. A started schema capability which fails to complete
seals request admission until certified reopen. Successful logical schema application can resume
without an immediate certificate because the previous exact root and retained ordered schema
proposal remain its recovery authority.

## Physical maintenance

`TreeDBStore.ValueLogGC` and `CompactStorage` use the existing adapter lifecycle mutex and
`ExternalRecovery.Apply`. The owner and DB pointer stay live for the whole operation. Maintained
Gomap recoverable-root collection includes both externally retained certificate roots, working roots
and readers. Its GC and rewrite code revalidates resource closure before reclamation. The
specialized unsafe fenced-reclaim compaction option remains refused.

These operations preserve logical versions and do not advance the MVCC discard floor, authorize Raft
retention, expire retry outcomes or publish a semantic checkpoint. Physical metadata or a newer
working seal cannot supersede the selected certificate. Maintenance failure propagates to the
caller. In particular, success is not permission to prune older posting deltas.

## Focused evidence

`posting.TestStoreScalarIndexRebuildAggregatesChunksAndPreservesHistory` covers 600 UIDs sharing one
token across chunk boundaries, timestamped removal, retained older reads and same-timestamp rebuild
retries. `posting.TestStoreIndexRebuildPropagatesCallbackFailure` checks asynchronous failure and
cancellation propagation.

`worker.TestRaftScalarSchemaRebuildCertifiedReplay` uses normal mutation, Delta, schema and
certificate callers. It blocks an actual index callback and checks that apply has not returned and
query schema remains unindexed. It restores an older pre-index certificate after a newer working
rebuild, replays the schema, certifies and reopens it, retries the represented proposal and applies
a later indexed mutation. GC and full physical compaction run with two retained cuts and a large
old/new value. Reopen restores the certified value and index, then suffix replay recreates exact
membership. The test checks that physical compaction leaves the logical floor at zero and refuses
unsafe reclaim.

`posting.TestStoreCountReverseRebuildAggregatesAndPreservesHistory` aggregates 600 source UIDs
across chunk boundaries, including a cold split source, shared reverse targets, forward/reverse
counts, removal and historical reads.
`TestStoreScalarListConversionPreservesTaggedValuesAndOldReads` checks conversion and same-timestamp
retry without losing tagged values. Pure data-validation tests compare logical eligibility with
different physical histories and preserve unknown error classes. Iteration callback errors and
empty-list cache anchor timestamps have dedicated regressions.

`worker.TestRaftSchemaVariantsCertifiedPendingAndRejection` applies mixed count/reverse, scalar
type/list and token changes through the ordinary tracked proposal path. It certifies and reopens
built indexes, checkpoints pending indexed/list mutations, reopens and commits their canonical
deltas, rejects an incompatible later predicate before removing an earlier predicate's indexes, and
certifies/reopens index removal while retaining older reads. Schema capability tests check
whole-request drain, private reader invalidation, stale write fencing and sealing on failure.

These are focused correctness/race controls. They do not establish real-server schema ACK-to-SIGKILL
behavior, power-loss certification, rebuild throughput, large-predicate resource budgets or full
operational capability acceptance.

## Logical maintenance integration

Generic MVCC pruning retains one anchor per key. A posting delta can require many older deltas and
an older complete list, so pruning follows a complete posting-aware baseline pass. The versioned
ordered floor proposal advances replicated semantic expiration `S` from the preceding ordered Oracle
frontier and pending starts. Only `S` authorizes stale-mutation rejection before retry lookup; local
physical floor `P <= S` never affects replicated outcomes. No wall clock selects `S`.

Task entry checks expiration before Oracle waits or cache access. A second check acquires a lifetime
pin after Oracle/checksum waits and before any cache/storage use. An Oracle waiter holds no posting
dependency and cannot prevent the Delta which wakes it from applying. The inner task retains its pin
until it actually exits, even if `ServeTask` has already returned on client cancellation. Requests
already using data can delay local maintenance while `S` advances independently.

The local runner takes the semantic apply/install barrier, checks health and completed recovery, and
verifies the old exact certificate plus the complete retained, synced WAL suffix. Its private
epoch-bound adapter capability pauses new request admissions and drains all existing requests, raw
readers, writers and callbacks. Draining all readers matters even above `P`: cached lists may retain
old split anchors, and independently written main/split records must not be observed midway through
the baseline pass.

Every main list is reconstructed at `B=P+1` through maintained posting decoding and rollup. Every
complete/empty main and split result is explicitly written at exactly `B`, including cold lists.
This boundary is required because Gomap rejects reads and writes at or below `P`; a split read uses
its main list's baseline timestamp. Newer original deltas remain unchanged. Private list reads
retain their supplied Store for split dependencies and do not enqueue asynchronous rollups. All
callbacks drain before the pass completes. Pending transaction list caches are converted through
`Txn.Update`, preserving serialized deltas and conflict versions; their materialized lists and the
global memory cache are then discarded. Only after the complete pass succeeds can the persisted
floor advance and generic pruning run.

The runner forces an exact certificate binding `S`, `P`, root and canonical pending namespace before
admission resumes, including when newer transactions remain pending. It chooses `B` strictly below
every pending StartTs. A later valid Delta has CommitTs strictly above StartTs, so it cannot
overwrite the baseline. Crash/reopen restores certified `S/P` and pending deltas/conflict versions
before retained suffix input. The explicit working-only capability remains tested: it advances
neither the certified frontier nor retention and restores the old `S/P` before replay. A
partial/canceled/ambiguous started pass seals admission until reopen; it cannot silently resume on a
mixed main/split working state. Private read handles are invalidated and stale write epochs fenced
before the capability releases.

Focused controls cover two stores with different local pruning schedules, exact `B`/`B+1` reads,
long delta chains, cold/empty/split lists, preserved newer delta bytes, callback
failure/cancellation, request draining, stale private handles, the future-Oracle waiter deadlock,
old-certificate restore, replayed pending deltas/conflict keys and later commit/abort decisions.
They are in `posting/raft_posting_*_test.go` and
`worker/raft_{posting_maintenance,task_admission}_test.go`. These are in-process correctness/race
controls, not power-loss or actual-server lifetime acceptance.

The existing apply housekeeping tick invokes the runner outside the semantic mutex; it never selects
a semantic floor from the clock or adds another Raft queue. Resource-budget and actual-server
qualification remain separate gates. The incremental canonical pending overlay allows certificates
with pending transactions without embedding all pending payload in JSON; its format, bounded
admission and restore contract are documented in [RAFT_PENDING_STATE.md](RAFT_PENDING_STATE.md).
Continuous-workload resource/debt qualification and full bounded-lifetime acceptance remain open.

## Other operations and inherited boundaries

| Flow                              | Required closure / current disposition                                                                                                                                                   |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Scalar tokenizer changes          | Implemented serial transition above; actual-server crash qualification remains                                                                                                           |
| Type definition/deletion          | Existing timestamped schema writer/tombstone; older-cut replay and reopen control implemented; no posting-data or namespace prefix deletion                                              |
| Count/reverse                     | Maintained builders integrated with explicit private Store, complete split sources, chunk aggregation, pending mutation and certificate/reopen proofs; inherited query limits remain     |
| Type/list conversion              | Maintained scalar-to-list conversion integrated with complete source copying; pure conversion/data-shape validation precedes effects; populated list-to-scalar/UID-scalar changes reject |
| Upsert/unique changes             | Requires existing-data uniqueness validation plus Zero conflict-key and retry semantics before publication; currently guarded standard flow                                              |
| Vector                            | Factory/cache and graph side effects need exact root closure and deterministic rebuild authority; already outside ordinary TreeDB adapter capabilities                                   |
| Drop/predicate/namespace deletion | Ordered pending disposition, versioned tombstones, schema/cache reset and namespace admission epoch must complete before a cut; physical Badger prefix deletion is not a replacement     |
| Restore/import                    | External files must be immutable, digest-bound retained input; whole-root transfer must use authorized install, while selective graft needs an ordered namespace transition              |
| Rollup/prune                      | Guarded posting-complete baseline/admission runner on existing housekeeping tick; bounded pending checkpoint/debt and actual-server qualification remain open                            |
| Backup/export                     | Existing paths require Badger streams/format; export can use a pinned readTs generic scan, backup needs a matching restore format and retained incremental history contract              |
| CDC/ACL                           | Existing configuration refusal remains; subscriptions, retained event frontier and ACL cache reconstruction need store-neutral ownership                                                 |
| Move/split                        | Source/receiver ordered authority, root/dependency transfer, pending decisions and membership ownership must be bound together; no silent KV import                                      |

Sort/count/inequality, Badger subscription formats, backup/restore formats and vector operational
support are inherited adapter limitations; this packet does not create them or remove their
capability gates. Standard Dgraph schema, namespace and maintenance flows still need the named
integrations before a production-tier claim.
