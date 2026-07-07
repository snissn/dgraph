# TreeDB Integration Scaffold

This package is the Dgraph-side TreeDB entry point for the posting-store replacement work. It pins
and compile-tests current TreeDB APIs without changing the runtime Alpha posting store, which still
opens Badger in `worker.ServerState.InitStorage`.

Current TreeDB head used by this branch:

- module: `github.com/snissn/gomap`
- version: `v0.6.2-0.20260706235004-1d9e97618e4e`
- commit: `1d9e97618e4ed3801fc92bb358b190930261fc7b`

What is wired here:

- durable TreeDB command-WAL profile selection
- Dgraph-shaped durable command-WAL profile selection that rejects benchmark-only no-WAL profiles
- TreeDB generation retention left at the profile default unless a caller explicitly overrides it
- compile assertions for TreeDB point reads/writes, versioned values, native conditional transaction
  APIs, snapshots, forward/reverse iteration, batches, value-log GC, full storage compaction, stats,
  and close
- an open/read/write/snapshot/iterator smoke test, plus a check that native conditional transactions
  are currently rejected under the durable command-WAL profile
- fail-closed checks for encryption and in-memory mode

Feature readiness is tracked through `FeatureRegistry()` and enforced with
`CheckRequiredFeatures(...)`. `UnsupportedFeatures()` remains as a compatibility string view over
registry entries whose status is not `supported`. Status meanings:

- `supported`: implemented with tests and safe to rely on for the current scaffold scope.
- `disabled_want`: desirable, but intentionally off until evidence or runtime wiring exists.
- `disabled_need_blocker`: required for a real Dgraph TreeDB backend and blocking until implemented
  or redesigned.
- `unsupported`: intentionally not supported for this integration lane.

Current non-supported TreeDB readiness entries. These are Badger-compatibility gaps in Dgraph's
current call sites, not proof that TreeDB lacks every lower-level primitive in the area:

- `badger_managed_transactions` (`disabled_need_blocker`): `OpenManaged`, `NewTransactionAt`,
  `CommitAt`, `NewManagedWriteBatch`, `SetEntryAt`.
- `command_wal_conditional_transactions` (`disabled_need_blocker`): native conditional transactions
  currently fail closed under the durable command-WAL profile.
- `badger_entry_metadata_ttl` (`disabled_need_blocker`): `Entry.UserMeta`, `Item.UserMeta`,
  `Entry.ExpiresAt`.
- `badger_all_version_iterators` (`disabled_need_blocker`): `NewKeyIterator`,
  `IteratorOptions.AllVersions`, `Prefix`, and prefetch settings.
- `badger_stream_import_export` (`disabled_need_blocker`): `NewStreamAt`, `Stream.Orchestrate`,
  `NewStreamWriter`.
- `badger_subscriptions` (`disabled_need_blocker`): `worker.SubscribeForUpdates`.
- `encryption_key_registry` (`unsupported`): encryption/key-registry requests fail closed until
  TreeDB exposes compatible semantics.
- `badger_protobuf_compatibility` (`disabled_need_blocker`): Badger `pb.KV`, `pb.KVList`, and
  `pb.Match` shapes.
- `metrics_cache_apis` (`disabled_want`): Badger cache sizing and metrics used by monitoring are not
  wired for TreeDB.
- `in_memory_posting_store` (`unsupported`): Dgraph's posting store is persistent; in-memory TreeDB
  mode fails closed.

Focused validation:

```sh
GOWORK=off go test ./worker/treedb -count=1
```
