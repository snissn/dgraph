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

## Posting-store adapter contract

Issue #5 adds the first Dgraph-side adapter seam in `posting.Store`. The contract is deliberately
narrow and maps to concrete posting-store call sites instead of a generic key/value wishlist:

- managed read transactions at a caller-provided read timestamp;
- managed write transactions committed at a caller-provided commit timestamp;
- posting entry `UserMeta`, `ExpiresAt`, and discard-earlier-version markers;
- prefix iteration with forward/reverse, value prefetch, and `AllVersions` controls; and
- item views exposing keys, values, versions, metadata, expiry, deletion/expiry state, and value
  size.

`posting.NewBadgerStore` is the only production implementation, and `posting.NewTxnWriter` still
selects Badger by default. `posting.NewTxnWriterForStore` exists only as an explicit experiment
seam. TreeDB runtime selection remains blocked until the non-supported feature rows above are
resolved or fail closed with tests; the adapter must not silently drop Badger metadata, timestamps,
versions, stream/import/export semantics, subscriptions, or encryption guarantees.

## Compatibility blocker matrix

Issue #6 records final Dgraph-side decisions for the Badger feature families that currently block a
TreeDB posting-store backend in `PostingCompatibilityMatrix()`. Runtime selector code should call
`CheckPostingBackendReady()` and must refuse TreeDB while required rows remain non-supported.

Current selector-blocking rows:

- managed timestamp transactions: `disabled_need_blocker`
- command-WAL-compatible conditional transactions: `disabled_need_blocker`
- entry metadata, TTL, and discard markers: `disabled_need_blocker`
- all-version/key iteration: `disabled_need_blocker`
- stream backup/export/import: `disabled_need_blocker`
- subscriptions: `disabled_need_blocker`
- Badger protobuf compatibility: `disabled_need_blocker`
- encryption/key registry: `unsupported`

Metrics/cache APIs are classified as `disabled_want`: they must be surfaced as unavailable in
operator output, but they are not sufficient by themselves to enable or block an experimental
selector. Unsupported or blocker rows must fail closed with the operator-facing messages from the
matrix rather than falling back silently or returning partial TreeDB behavior.

## Experimental Alpha selector

Issue #7 adds the explicit Alpha selector flag:

```sh
dgraph alpha --posting-store backend=badger   # default
dgraph alpha --posting-store backend=treedb   # experimental, currently fail-closed
```

The selector is parsed into `worker.Config.PostingStoreBackend`. Badger remains the default and
continues to open through the existing managed Badger path. TreeDB startup calls
`CheckPostingStoreBackendReady()` and refuses to open while required compatibility rows remain
`disabled_need_blocker` or `unsupported`; there is no silent fallback from a requested TreeDB
backend to Badger.

## Operator gates and default decision

Issue #8 finalizes the current operator-facing gate report in `OperatorGateReport()`:

| Gate                        | Current state   | Operator decision                                                                                                           |
| --------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Badger default              | `pass`          | Badger remains the default Alpha posting-store backend.                                                                     |
| TreeDB primitive durability | `evidence_only` | TreeDB can open, write, close, and reopen in the scaffold, but this is not a Dgraph posting-store backend.                  |
| TreeDB selector             | `fail_closed`   | An explicit TreeDB selector is accepted but startup refuses to open while blockers remain.                                  |
| Posting/schema workflows    | `fail_closed`   | Dgraph posting and schema workflows remain Badger-only until metadata and all-version semantics are implemented and tested. |
| Backup/restore/export       | `fail_closed`   | Stream backup/export/import and snapshot workflows remain Badger-only.                                                      |
| Subscriptions               | `fail_closed`   | `worker.SubscribeForUpdates` remains Badger-only.                                                                           |
| Encryption/key registry     | `unsupported`   | TreeDB encryption/key-registry integration is unsupported in this integration lane.                                         |
| Benchmark matrix            | `pass`          | The benchmark matrix is available for current evidence and future before/after runs.                                        |
| Default decision            | `pass`          | `keep_badger_default`; TreeDB stays explicit, experimental, and fail-closed.                                                |

Final decision for this tracker: keep Badger as the default. TreeDB is not production-ready and is
not a drop-in posting-store backend until the fail-closed rows above move to supported with tests
and benchmark evidence.

Focused validation:

```sh
GOWORK=off go test ./worker/treedb -count=1
```

## Benchmark matrix

This package also owns the Dgraph Badger-vs-TreeDB evidence matrix for the TreeDB posting-store
work. The matrix is intentionally limited to benchmark evidence and does not add a runtime backend
selector or adapter implementation.

Smoke run, suitable for PR validation:

```sh
worker/treedb/run_benchmark_matrix.sh --smoke
```

Fuller local run, suitable for before/after evidence on future hot-path PRs:

```sh
BENCHTIME=1s COUNT=5 worker/treedb/run_benchmark_matrix.sh \
  --artifact-dir /tmp/dgraph-treedb-bench/full-$(git rev-parse --short HEAD)
```

The script runs:

```sh
GOWORK=off go test ./worker/treedb -run '^$' \
  -bench '^BenchmarkDgraphTreeDBMatrix$' -benchtime "$BENCHTIME" \
  -count "$COUNT" -timeout "${TIMEOUT:-10m}" -benchmem -v
```

Artifacts are written under `/tmp/dgraph-treedb-bench/<UTC timestamp>` by default, or to
`ARTIFACT_DIR` / `--artifact-dir` when provided:

- `context.txt`: repository, baseline/candidate refs, `GOWORK=off`, Go/CPU/kernel, cache/TMPDIR,
  exact command, and fixture shape.
- `raw.txt`: complete `go test` benchmark output, including explicit skipped blocker rows.
- `summary.md`: parsed benchmark table plus the Dgraph-required TreeDB blocker/skip table.

Timed rows include Badger managed write/read/all-version baselines and TreeDB `Set`, `Get`,
`GetVersioned`, batch write/sync write, snapshot read/iterate, iterator, reverse iterator, and
`Stats` primitives. Skipped rows document Dgraph-required Badger contracts TreeDB cannot yet run:
managed timestamp transactions, entry metadata/TTL, all-version key iterators, stream export/import,
subscriptions, and encryption/key registry support.
