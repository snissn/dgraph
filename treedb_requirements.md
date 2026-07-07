# TreeDB Requirements for Badger Replacement in Dgraph

This document captures the Badger API surface Dgraph currently exposes through
its posting-store code. TreeDB is now pinned in this branch through
`github.com/snissn/gomap v0.6.2-0.20260706235004-1d9e97618e4e` and compile-smoked
under `worker/treedb`, but the Alpha runtime still uses Badger.

## 1. Managed Mode (Timestamps)
Dgraph manages its own timestamps (MVCC) and uses Badger's "Managed Mode".
- `badger.OpenManaged(opts)`: Opening the DB in managed mode.
- `badger.NewTransactionAt(ts, update)`: Creating a transaction at a specific read timestamp.
- `txn.CommitAt(ts, callback)`: Committing a transaction at a specific commit timestamp.
- **Requirement**: TreeDB must support external timestamp management and allow reading/writing at specific versions.
- **Current status**: Latest TreeDB exposes native entry revisions and `GetVersioned`, but Dgraph still needs a Badger-compatible transaction boundary for `NewTransactionAt`, `CommitAt`, and managed write batches before the posting store can switch.

## 2. Core KV API
- `txn.Get(key)`: Retrieve an item by key.
- `txn.SetEntry(entry)`: Write an entry with metadata and TTL.
- `badger.Entry`: Support for `Key`, `Value`, `UserMeta`, and `ExpiresAt`.
- `item.Value(func(val []byte) error)`: Efficient value retrieval.
- `item.UserMeta()`: Dgraph uses a single byte of metadata to tag keys (e.g., `BitEmptyPosting`, `BitDeltaPosting`).
- `item.Version()`: Retrieving the timestamp at which a key was written.

## 3. Iteration Features
Dgraph uses complex iteration patterns for range queries and rollups.
- `badger.Iterator`: Support for `Seek`, `Valid`, `Next`, `Rewind`.
- `badger.IteratorOptions`:
    - `PrefetchValues`: Whether to prefetch values during iteration.
    - `PrefetchSize`: Number of items to prefetch.
    - `Reverse`: Support for reverse iteration.
    - `AllVersions`: Support for seeing all versions of a key during iteration.
    - `Prefix`: Scoping iteration to a specific prefix.
- `txn.NewIterator(opts)`: Creating an iterator.
- `txn.NewKeyIterator(key, opts)`: Iterating over all versions of a *single* key (used in rollups).

## 4. Streaming and Bulk Loading
- `badger.Stream`: Efficiently streaming data out of the DB (used for backups and exports).
    - `NewStreamAt(ts)`: Creating a stream at a timestamp.
    - `stream.Orchestrate(ctx)`: Executing the stream.
    - Callbacks: `ChooseKey`, `KeyToList`, `Send`.
- `badger.StreamWriter`: Fast bulk loading of data into the DB.
    - `NewStreamWriter()`: Creating a stream writer.
    - `writer.Write(buf)`: Writing batches of data.
    - `writer.Flush()`: Ensuring all data is written.

## 5. Subscription (CDC)
- `badger.Subscribe(ctx, callback, matches)`: Subscribing to key-value changes.
- `badgerpb.Match`: Support for prefix-based filtering and ignoring specific bytes.
- **Requirement**: As noted, TreeDB currently lacks this and it is a critical feature for Dgraph's internal communication and potentially CDC.

## 6. Administrative and Lifecycle
- `badger.DB.Close()`: Graceful shutdown.
- `badger.DB.Sync()`: Manual flush to disk.
- `badger.DB.RunValueLogGC(discardRatio)`: Value log garbage collection.
- `badger.DB.Size()`: Getting the size of the DB (LSM and Value Log).
- `badger.DB.Flatten()`: Compacting the LSM tree levels.

## 7. Encryption and Security
- `badger.Options.WithEncryptionKey(key)`: Support for transparent data encryption at rest.
- `badger.KeyRegistry`: Management of data keys for encryption.
- **Requirement**: Dgraph's `raftwal` uses `badger.OpenKeyRegistry` to manage encryption for the Write-Ahead Log.

## 8. Performance and Operational Options
- `badger.Options`:
    - `WithDir`, `WithValueDir`: Separate directories for LSM and value log.
    - `WithValueThreshold`: Threshold for storing values in the value log.
    - `WithNumVersionsToKeep`: Maximum versions to keep for each key.
    - `WithNamespaceOffset`: Offsetting keys by a namespace (used for multi-tenancy).
    - `WithCompression`: Support for Snappy or Zstandard compression.
    - `WithSyncWrites`: Whether to sync writes immediately (Dgraph typically sets this to false and syncs manually).

## 9. Internal Types (Protobuf)
Dgraph uses Badger's protobuf types in its internal communication.
- `badgerpb.KV`, `badgerpb.KVList`: Data structures for batches of KVs.
- `badgerpb.Match`: Structure for subscription filters.

## 10. Memory and Cache
- Badger's internal block and index caches (often using Ristretto).
- Dgraph monitors Badger's cache health and metrics.

## Current PR Boundary

This branch intentionally stops at a compile-tested TreeDB open/read/write
scaffold. A direct runtime toggle would require changing Dgraph package
contracts that currently accept `*badger.DB`, `*badger.Txn`, Badger iterators,
Badger stream types, and Badger protobuf messages.
