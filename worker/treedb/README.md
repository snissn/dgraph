# TreeDB Integration Scaffold

This package is the Dgraph-side TreeDB entry point for the posting-store replacement work. It pins and compile-tests current TreeDB APIs without changing the runtime Alpha posting store, which still opens Badger in `worker.ServerState.InitStorage`.

Current TreeDB head used by this branch:

- module: `github.com/snissn/gomap`
- version: `v0.6.2-0.20260706235004-1d9e97618e4e`
- commit: `1d9e97618e4ed3801fc92bb358b190930261fc7b`

What is wired here:

- durable TreeDB command-WAL profile selection
- Dgraph-shaped version retention default matching the current Badger `WithNumVersionsToKeep(math.MaxInt32)` posting-store setting
- compile assertions for TreeDB point reads/writes, versioned values, snapshots, forward/reverse iteration, batches, value-log GC, full storage compaction, stats, and close
- an open/read/write/snapshot/iterator smoke test
- fail-closed checks for encryption and in-memory mode

What still blocks replacing Badger in the Alpha posting store:

- Badger managed transaction API: `OpenManaged`, `NewTransactionAt`, `CommitAt`, `NewManagedWriteBatch`, `SetEntryAt`
- Badger entry metadata and TTL: `Entry.UserMeta`, `Item.UserMeta`, `Entry.ExpiresAt`
- Badger all-version/key iterators: `NewKeyIterator` plus `IteratorOptions.AllVersions`, `Prefix`, and prefetch settings
- Badger stream import/export API: `NewStreamAt`, `Stream.Orchestrate`, `NewStreamWriter`
- Badger subscription API used by `worker.SubscribeForUpdates`
- Badger encryption and key-registry APIs used by posting stores, backups, debug, and `raftwal`
- Badger protobuf compatibility: `github.com/dgraph-io/badger/v4/pb` `KV`, `KVList`, and `Match`
- Badger cache metrics and cache sizing APIs used by Dgraph monitoring

Focused validation:

```sh
GOWORK=off go test ./worker/treedb -count=1
```
