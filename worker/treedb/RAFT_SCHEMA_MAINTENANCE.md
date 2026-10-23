# Ordered schema and physical maintenance

The `raft_checkpoint` policy now performs scalar tokenizer changes through the existing ordered
apply path. This is a focused production integration packet, not acceptance of the whole TreeDB
operational or production capability tier.

## Scalar tokenizer transition

Proposal admission and committed application both call `checkRaftSupportedProposal`. Existing
predicates may change tokenizer membership while retaining their type, list, language, upsert,
uniqueness and other schema properties. Fresh basic predicates and types keep their existing
semantic writer. Vector, count, reverse, type/list conversion, destructive and external-file
operations remain refused by this route.

Committed input is already routed; application does not consult Zero's later tablet assignment. The
same rule is used by ordinary `runMutation`. Schema validation precedes materialization. The query
schema omits unavailable or removed tokenizers while the mutation schema supplies the intended
tokenizers to the existing index builders. Old index keys become timestamped empty lists, including
split keys, rather than being physically removed by `DropPrefix`.

The maintained `rebuilder` callback, posting-list decoder, index mutation code and posting-list
rollup produce index contents. A store-neutral snapshot scan feeds those algorithms. Chunks of 256
source lists are aggregated in temporary Badger at unique scratch timestamps, then merged into
complete posting lists at the schema transition timestamp. Scratch storage has no recovery
authority: no certificate references it, it is never imported on restart, and replay recomputes it
from the selected certified root plus retained ordered proposal. Scratch memory, disk and I/O scale
with the rebuilt predicate; the 256-list chunk is not a constant bound on an individual list or
token's size.

Every final write callback drains before the builder returns. Only after that and the final schema
write succeeds is the complete query schema published. There is no background builder, new Raft
queue or independent root publisher. The semantic apply goroutine cannot certify a partial schema
operation. An I/O, callback or setup error is an uncertain apply failure; the current certificate
and required history stay selectable, and recovery reruns the operation. It is not converted to a
normal mutation's terminal semantic rejection.

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

These are focused correctness/race controls. They do not establish real-server schema ACK-to-SIGKILL
behavior, power-loss certification, rebuild throughput, large-predicate resource budgets or full
operational capability acceptance.

## Required logical maintenance protocol

Generic MVCC pruning retains one anchor per key. A posting delta is not a full anchor: it can
require many older deltas and a complete baseline. Pruning must therefore follow a posting-aware
baseline pass, not just a Raft snapshot hint. The selected algorithm is:

1. Serialize maintenance with ordered apply, snapshot installation and all admitted callbacks. Fence
   background cold rollups and writers. Take an active read-timestamp admission cut, including
   transactions that can issue future reads, rather than counting only currently open iterators.
2. Select a monotonically increasing floor strictly below protected read and pending transaction
   timestamps, and below the Oracle assigned frontier. Conservatively cap the first implementation
   by both selectable certificate Oracle frontiers. New admissions at or below the proposed floor
   must be excluded under the same admission lock; an elapsed timeout is not a pin.
3. Materialize complete or empty posting baselines at the chosen timestamp for every main list and
   its split dependencies. Preserve all newer deltas and versions. Rebase cold-list rollup output
   explicitly: its ordinary `maxCommitTs+1` can be below a previously persisted floor. Drain writes
   and invalidate affected caches before declaring the baseline pass complete.
4. Persist the floor only after the entire baseline pass succeeds. Then invoke maintained MVCC
   pruning, retaining complete anchors and all newer versions. Current/previous physical certificate
   roots remain pinned throughout.
5. Publish a certificate binding the maintenance floor/completion and the exact resulting root
   before releasing admission. An ambiguous publication keeps admission closed until owner
   Close/Open determines the selected certificate. A failed prepublication pass leaves the prior
   certificate/history intact; startup discards unlogged working maintenance and replays retained
   input.

The adapter currently holds root lifetime pins, but its timestamp-only read transaction can issue
later point reads against a newer MVCC cut. It has no active readTs registry, so rootGate alone does
not prove this logical protocol. This protocol is design work pending implementation and proof. It
must not be confused with the implemented physical maintenance paths above.

## Other operations and inherited boundaries

| Flow                              | Required closure / current disposition                                                                                                                                               |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Scalar tokenizer changes          | Implemented serial transition above; actual-server crash qualification remains                                                                                                       |
| Count/reverse                     | Maintained builders exist, but count baseline/split dependencies and query capability need adapter proofs; newly guarded standard Dgraph flows                                       |
| Type/list conversion              | Reuse maintained `BuildData` conversion and generic aggregation only after conversion/retry/read-cut tests; currently guarded standard flow                                          |
| Upsert/unique changes             | Requires existing-data uniqueness validation plus Zero conflict-key and retry semantics before publication; currently guarded standard flow                                          |
| Vector                            | Factory/cache and graph side effects need exact root closure and deterministic rebuild authority; already outside ordinary TreeDB adapter capabilities                               |
| Drop/predicate/namespace deletion | Ordered pending disposition, versioned tombstones, schema/cache reset and namespace admission epoch must complete before a cut; physical Badger prefix deletion is not a replacement |
| Restore/import                    | External files must be immutable, digest-bound retained input; whole-root transfer must use authorized install, while selective graft needs an ordered namespace transition          |
| Rollup/prune                      | Posting-complete baseline and admission protocol above; currently disabled for this policy                                                                                           |
| Backup/export                     | Existing paths require Badger streams/format; export can use a pinned readTs generic scan, backup needs a matching restore format and retained incremental history contract          |
| CDC/ACL                           | Existing configuration refusal remains; subscriptions, retained event frontier and ACL cache reconstruction need store-neutral ownership                                             |
| Move/split                        | Source/receiver ordered authority, root/dependency transfer, pending decisions and membership ownership must be bound together; no silent KV import                                  |

Sort/count/inequality, Badger subscription formats, backup/restore formats and vector operational
support are inherited adapter limitations; this packet does not create them or remove their
capability gates. Standard Dgraph schema, namespace and maintenance flows still need the named
integrations before a production-tier claim.
