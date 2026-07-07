# TreeDB vs Badger Comparison (for Dgraph Replacement)

This document categorizes Badger features used by Dgraph into those TreeDB already supports and those it does not, along with implementation notes for missing features.

TreeDB has been synced to `snissn/gomap` commit
`1d9e97618e4ed3801fc92bb358b190930261fc7b` through Go pseudo-version
`v0.6.2-0.20260706235004-1d9e97618e4e`.

## 1. Features TreeDB Already Has

### Core KV API
- **CRUD Operations**: `Get`, `Set`, `Delete`, `Has`, `GetMany`.
- **Atomic Batches**: `NewBatch`, `Batch.Set`, `Batch.Delete`, `Batch.Write`.
- **Point-in-Time Reads**: `AcquireSnapshot` providing a consistent view of the database.
- **Tombstones**: Support for deleting keys while preserving historical consistency.
- **Versioned Values**: `GetVersioned` and revision-aware internal batch paths exist in latest TreeDB.

### Iteration
- **Basic Iteration**: `Iterator(start, end)` and `ReverseIterator(start, end)` with `Valid`, `Next`, `Key`, and `Value`.
- **Copy Control**: `KeyCopy` and `ValueCopy` provide caller-owned bytes when Dgraph cannot keep view lifetimes local.
- **Gap**: TreeDB does not expose Badger's transaction-scoped `Seek`, `Rewind`, `Prefix`, or `AllVersions` iterator option surface yet.

### Persistence and Lifecycle
- **Durable Writes**: `SetSync`, `DeleteSync`, and `Batch.WriteSync` for fsync-backed writes.
- **Graceful Shutdown**: `Close()` method to drain snapshots and flush state.
- **Garbage Collection**: `ValueLogGC` to reclaim space from unreferenced values.
- **Full Compaction**: `CompactStorage` as a high-level API to run rewrite/GC/vacuum phases.

### Operational Options
- **Separate Directories**: Options for separate storage domains (index, vlog).
- **Compression**: Support for Zstandard and other block-level compression.

---

## 2. Features TreeDB Does Not Yet Have

### External Timestamp Management (Managed Mode)
- **Badger Feature**: `OpenManaged`, `NewTransactionAt(ts)`, `CommitAt(ts)`.
- **Dgraph Usage**: Dgraph manages its own MVCC timestamps.
- **TreeDB Gap**: TreeDB exposes native entry revisions, but not the Badger-compatible managed transaction API Dgraph currently calls.
- **Implementation Steps**:
    1.  Add `CommitAt(ts)` to the Batch API to override internal sequence generation.
    2.  Update `AcquireSnapshotAt(ts)` to allow reading from a specific historical version.
    3.  Ensure `ValueLogGC` respects external timestamps when determining reachability.

### Subscription (CDC)
- **Badger Feature**: `Subscribe(ctx, callback, matches)`.
- **Dgraph Usage**: Watching for prefix-based changes.
- **TreeDB Gap**: No built-in pub/sub or event bus for KV changes.
- **Implementation Steps**:
    1.  Implement a `SubscriptionManager` that tracks active subscribers and their filters.
    2.  Hook into the `Commit` path to identify modified keys and notify matching subscribers.
    3.  Define a stable Protobuf-compatible message format for change events.

### Entry Metadata and TTL
- **Badger Feature**: `Entry.UserMeta`, `Entry.ExpiresAt`.
- **Dgraph Usage**: Uses `UserMeta` for tagging postings list types.
- **TreeDB Gap**: TreeDB nodes and value log frames do not currently store a dedicated `UserMeta` byte or TTL timestamp per entry.
- **Implementation Steps**:
    1.  Update the `LeafEntry` struct and on-disk leaf node layout to include an optional metadata byte.
    2.  Add a `TTL` field to the value log frame header.
    3.  Implement background expiration logic in the `ValueLogGC` to drop expired entries.

### Streaming API
- **Badger Feature**: `Stream` and `StreamWriter`.
- **Dgraph Usage**: Fast bulk exports/imports and backups.
- **TreeDB Gap**: No dedicated high-throughput streaming interface.
- **Implementation Steps**:
    1.  Implement a `Stream` type that uses multiple goroutines to scan the B-tree and stream values.
    2.  Create a `StreamWriter` that bypasses the standard write path for optimized bulk loading (similar to `bulk.Build` but for live DBs).

### Encryption at Rest
- **Badger Feature**: `WithEncryptionKey`, `KeyRegistry`.
- **Dgraph Usage**: Encrypting both the main DB and the WAL.
- **TreeDB Gap**: Architecture supports encryption post-processors, but the `KeyRegistry` and management logic are missing.
- **Implementation Steps**:
    1.  Implement a `KeyRegistry` to manage data encryption keys (DEKs).
    2.  Integrate the existing `AES_GCM_SIV` codec into the value log and pager write paths.
    3.  Provide an option to encrypt the Command WAL segments.

### Administrative Stats
- **Badger Feature**: `Flatten`, `DB.Size`.
- **Dgraph Usage**: Monitoring DB size and manually triggering level flattening.
- **TreeDB Gap**: `Flatten` (LSM concept) isn't directly applicable to TreeDB's B-tree, but a similar "full vacuum/rebuild" exists. `Size` needs a consolidated return.
- **Implementation Steps**:
    1.  Consolidate storage usage into a simple `Size()` method.
    2.  Expose `VacuumIndexOnline` as a replacement for `Flatten` for space reclamation and page locality.

---

## 3. Current Dgraph PR Shape

The initial PR does not change the live posting store. It adds:

1. `github.com/snissn/gomap` pinned to latest TreeDB head.
2. `worker/treedb`, a Dgraph-shaped TreeDB open/read/write scaffold using the durable command-WAL profile.
3. Compile assertions for the TreeDB APIs Dgraph can already build on.
4. Tests proving the scaffold opens TreeDB, writes, reads, snapshots, batches, and iterates.

The runtime backend switch should come after the missing Badger-compatible
contracts above are implemented or Dgraph's posting-store call sites are
abstracted away from Badger types.
