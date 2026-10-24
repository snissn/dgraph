# Canonical pending state in an exact DATA root

The `raft_checkpoint` owner stores each pending transaction in a private raw TreeDB namespace,
inside the same `ExternalRecovery.Apply` authority as posting data. An exact certificate binds that
namespace through its retained DATA root. A fixed-size manifest records format, transaction count
and logical charge; checkpointing does not serialize or rehash all pending payloads into JSON. This
is not an independent publisher or an aggregate digest protocol.

## State and staging

The recovery state consists of `Txn.StartTs`, nonempty serialized `cache.deltas`, the exact
`cache.maxVersions` for those delta keys, conflict keys and `shouldAbort`. The cache's `commitTs` is
also preserved. `Txn.Update` uses the maintained cache transition to seal deltas and discard
materialized Lists. Global cache contents, materialized Lists and query-only empty delta/version
entries are disposable. Local `lastUpdate` is reconstructed on restore: it asks Zero for decisions
and does not authorize semantic expiration.

Ordered mutation processing clones this state without registering a new Oracle transaction. It runs
the existing mutation/index pipeline against the clone and context-scoped inferred schema. Only
after canonical encoding and final total count/charge admission may it publish inferred schema and
replace the pending record plus manifest, then install the staged Oracle transaction. The codec's
`ErrRaftPendingStateTooLarge` and the adapter's `ErrRaftPendingCapacity` identify only
prepublication capacity refusal. Malformed state, failed storage and uncertain publication remain
apply failures; they cannot be converted to successful terminal no-ops.

A Delta validates the whole decision batch, drains normal posting commit callbacks, then replaces
pending records with exact-root terminal dispositions in bounded owner batches. Commit/abort
decisions bypass pending admission limits. Recovery validates the complete bounded namespace and its
manifest before atomically installing Oracle pending state, suffix replay or read health. A missing,
duplicate, malformed or mismatched record fails closed. Raw input edges are never reinterpreted
under a later schema to recreate a certified pending transaction.

## Record format `dgraph-pending-v1`

All integers are unsigned, big endian. Sections use lexicographic byte-key order or ascending
conflict-key order, with no duplicates. Length prefixes and section counts are fixed-width u32;
timestamps and conflict keys are fixed-width u64.

| Field                           | Encoding                                                               |
| ------------------------------- | ---------------------------------------------------------------------- |
| Magic                           | Eight bytes `DGRPEND1`                                                 |
| StartTs, cache commitTs         | Two u64 values; StartTs must be nonzero                                |
| shouldAbort                     | One byte, exactly zero or one                                          |
| Delta, version, conflict counts | Three u32 values; delta and version counts must match                  |
| Delta section                   | Repeated length-prefixed key and length-prefixed serialized delta      |
| Version section                 | Same exact keyset, each length-prefixed key followed by u64 maxVersion |
| Conflict section                | Strictly increasing nonzero u64 keys                                   |
| Integrity                       | SHA-256 of all preceding bytes, exactly 32 bytes                       |

Posting keys must be canonical main data/index/reverse/count keys in the ordinary namespace.
Split/schema/type keys and aliases are rejected. Delta payloads contain only canonical repeated
`PostingList.postings` messages, with no complete pack, split descriptor or committed list
timestamp. A posting's StartTs is zero or equals the transaction's StartTs; its CommitTs is zero.
Validation processes one Posting at a time, checks canonical protobuf encoding and rejects unknown
fields, trailing record bytes, oversized records and malformed counts/lengths before allocating
count-sized maps. Restored maps and byte slices own their contents.

The decoder safety ceiling is 64 MiB per record. The current adapter separately admits at most 4096
transactions and 64 MiB total logical charge. Charge is canonical record length plus a fixed 64-byte
row charge, not a heap or disk usage estimate. Fixed-width timestamps preserve exact per-key
conflict versions without allowing different local physical floors or varint widths to change
capacity decisions. Query-only cache keys are excluded. Temporary mutation execution, index fanout
and decoding allocations remain proportional to the bounded input/state; this contract does not
assert constant transient memory.

## Retry lifetime and operational backpressure

The pending namespace limit is separate from retry history and recovery debt. With semantic floor
`S=0`, successful and terminally rejected proposal signatures stay in `appliedProposals`; local
time, checkpoint success and physical GC do not retire them. Certificate JSON includes those
signatures and has a 1 MiB ceiling. Exceeding it skips an ordinary checkpoint or fails a forced one
while preserving the previous exact certificate and required WAL. New mutation proposal admission
then pauses at 4096 uncertified entries or 64 MiB recovery input (including in-flight reservations).
These are backpressure thresholds, not an equilibrium bound on all log, disk or heap use.

Transaction decisions bypass mutation debt so a full budget cannot prevent resolution; advisory
watermark-only Deltas are suppressed at the debt limit. State, Delta, native Snapshot and
posting-floor controls now use intrinsic ordered disposition instead of generic persistent key
signatures. Terminal StartTs dispositions remain in raw DATA until explicit S retirement. New
prewrites pause when pending plus terminal count reaches 65536, reserving disposition space for
existing pending transactions; decisions, including previously unseen global decisions, remain
admissible beyond that soft limit. This is deliberately not a hard bound on all semantic history.
Mutation/admin identities and terminal rows at S=0 still require explicit operator retirement for
sustained progress. See [RAFT_TERMINAL_STATE.md](RAFT_TERMINAL_STATE.md) for exact retry, checksum
and failure contracts.

## Schema and local maintenance

The pure ordered schema check rejects an actual schema change for a predicate with canonical pending
deltas. Type-definition transitions and existing type deletion are refused while any transaction is
pending. Exact schema/type definitions and absent type deletion are no-ops; unrelated predicate
schema changes remain eligible. Query-only cache keys cannot affect this decision. The check never
launches Zero abort requests. Schema timestamps come from the preceding ordered Oracle frontier, not
a later local Zero notification.

Physical posting maintenance chooses baseline `B=P+1` strictly below every pending StartTs. It
preserves pending deltas, conflict keys and conflict versions while discarding disposable Lists.
After complete main/split baselines and pruning, its production runner forces an exact certificate
containing the new P and pending namespace before reopening admission. An interrupted or ambiguous
started pass seals admission until certified-root reopen. The explicit working-only capability
changes neither certification nor retention and is not the runner's pending-transaction liveness
path.

## Focused validation and limits

`posting/raft_pending_state_test.go` checks canonical round trips, isolated staging, owned bytes,
exact conflict versions, charge invariance near a capacity boundary, query-only cache exclusion,
strict malformed/truncated/duplicate refusal and atomic Oracle replacement.
`worker/raft_schema_test.go` checks pure pending-aware schema decisions and idempotent no-ops.
`worker/raft_posting_maintenance_test.go` certifies and reopens a pruned root with a newer pending
transaction, compares restored conflict keys, and applies later commit and abort decisions. It also
restores an older certificate after working-only maintenance and replays retained input.

These are in-process correctness and race controls. They do not establish actual-server
ACK-to-SIGKILL behavior for pending/schema transitions, power-loss certification, workload
throughput, a bound on total posting data, or full operational capability acceptance. The production
integration packet's broader crash, peer transfer and resource gates remain separate.
