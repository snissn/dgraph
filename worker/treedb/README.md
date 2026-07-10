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

`RequiredFeaturesForTier(...)`, `CapabilityTierBlockers(...)`, and `CheckCapabilityTier(...)`
mechanically separate three cumulative gates:

| Tier                | Purpose                                                   | Required capability families                                                                                                                                               | Current decision                                                                                                              |
| ------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `benchmark_minimal` | Restricted live Alpha A/B benchmark                       | durable open/lifecycle, point and snapshot primitives, a TreeDBStore implementation, externally assigned timestamps, `UserMeta`/discard markers, and all-version iteration | fail closed until the adapter, managed timestamps, metadata/discard markers, all-version iteration, and lifecycle wiring pass |
| `operational`       | Operational parity after a successful Alpha decision gate | everything above plus nonzero TTL, backup/export/import, subscriptions, Badger protobuf translation, and monitoring/cache status                                           | not ticketed by the Alpha graph; unsupported invocations fail explicitly                                                      |
| `production`        | A future production-readiness decision                    | everything above plus encryption/key-registry integration                                                                                                                  | unsupported; TreeDB is not a production backend                                                                               |

TreeDB-native conditional transactions and in-memory posting storage are excluded from all three
tiers. Dgraph owns posting conflict detection, and this integration lane is persistent. Their
absence therefore cannot silently become a benchmark blocker or a supported capability.

Current non-supported TreeDB readiness entries. These are Badger-compatibility gaps in Dgraph's
current call sites, not proof that TreeDB lacks every lower-level primitive in the area:

- `badger_managed_transactions` (`disabled_need_blocker`, benchmark-minimal): `OpenManaged`,
  `NewTransactionAt`, `CommitAt`, `NewManagedWriteBatch`, `SetEntryAt`.
- `treedb_store_implementation` (`disabled_need_blocker`, benchmark-minimal): the backend-neutral
  seam exists, but issue #21 must provide the TreeDB implementation.
- `command_wal_conditional_transactions` (`unsupported`, no tier): native conditional transactions
  fail closed; the Dgraph adapter must use Dgraph-owned conflict detection.
- `badger_entry_metadata` (`disabled_need_blocker`, benchmark-minimal): `Entry.UserMeta`,
  `Item.UserMeta`, and discard-earlier-version markers.
- `badger_entry_ttl` (`unsupported`, operational): nonzero `Entry.ExpiresAt` values must be rejected
  until an expiry contract exists. TTL does not block the benchmark-minimal tier.
- `badger_all_version_iterators` (`disabled_need_blocker`, benchmark-minimal): `NewKeyIterator`,
  `IteratorOptions.AllVersions`, `Prefix`, and prefetch settings.
- `lifecycle_gc_stats` (`disabled_need_blocker`, benchmark-minimal): TreeDB close, GC, compaction,
  and stats primitives exist, but Alpha lifecycle wiring belongs to the restricted runtime work.
- `badger_stream_import_export` (`disabled_need_blocker`, operational): `NewStreamAt`,
  `Stream.Orchestrate`, `NewStreamWriter`.
- `badger_subscriptions` (`disabled_need_blocker`, operational): `worker.SubscribeForUpdates`.
- `encryption_key_registry` (`unsupported`, production): encryption/key-registry requests fail
  closed until TreeDB exposes compatible semantics.
- `badger_protobuf_compatibility` (`disabled_need_blocker`, operational): Badger `pb.KV`,
  `pb.KVList`, and `pb.Match` shapes.
- `metrics_cache_apis` (`disabled_want`, operational): Badger cache sizing and metrics used by
  monitoring are not wired for TreeDB.
- `in_memory_posting_store` (`unsupported`, no tier): Dgraph's posting store is persistent;
  in-memory TreeDB mode fails closed.

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
seam. TreeDB runtime selection remains blocked until the benchmark-minimal feature rows above are
resolved. Later-tier calls must use `CheckFeatureAvailable(...)` and fail at invocation rather than
silently dropping TTL, stream/import/export, subscription, protobuf, monitoring, or encryption
semantics.

## Compatibility blocker matrix

Issue #6 recorded the original Dgraph-side decisions for Badger feature families. Issue #18 assigns
those records to cumulative tiers in `PostingCompatibilityMatrix()`. Runtime selector code calls
`CheckPostingBackendReady()`, which checks only `benchmark_minimal`; later-tier entry points must
gate their own capability with `CheckFeatureAvailable(...)`.

Current benchmark-minimal selector-blocking rows:

- TreeDBStore implementation: `disabled_need_blocker`
- managed timestamp transactions: `disabled_need_blocker`
- entry metadata and discard markers: `disabled_need_blocker`
- all-version/key iteration: `disabled_need_blocker`
- Alpha lifecycle/GC/stats wiring: `disabled_need_blocker`

TTL, stream backup/export/import, subscriptions, Badger protobuf compatibility, and metrics/cache
APIs are operational requirements. Encryption/key registry is a production requirement. Conditional
transactions and in-memory mode are excluded from the integration tiers. Unsupported or blocker rows
fail with the stable feature ID/status from `FeatureReadinessError`; they never fall back to Badger
or return partial TreeDB behavior.

## Experimental Alpha selector

Issue #7 adds the explicit Alpha selector flag:

```sh
dgraph alpha --posting-store backend=badger   # default
dgraph alpha --posting-store backend=treedb   # experimental, currently fail-closed
```

The selector is parsed into `worker.Config.PostingStoreBackend`. Badger remains the default and
continues to open through the existing managed Badger path. TreeDB startup calls
`CheckPostingStoreBackendReady()` and refuses to open while the cumulative `benchmark_minimal`
requirements remain blocked; there is no silent fallback from a requested TreeDB backend to Badger.
Startup requirements outside that tier are checked separately: an encrypted TreeDB selection is
rejected by `CheckPostingStoreBackendReadyForConfig()` before the experimental opener is reached.
Passing this classification gate does not open TreeDB yet: restricted runtime enablement belongs to
issue #19.

## Performance boundary

Capability classification runs only during backend selection or at an optional feature's entry
point. It is not called from per-key, per-posting, iterator-step, or commit loops. This issue
changes control-plane registry lookups and documentation only, so Badger data-path benchmark
comparison is not applicable.

## Operator gates and default decision

Issue #8 finalizes the current operator-facing gate report in `OperatorGateReport()`:

| Gate                        | Current state   | Operator decision                                                                                                           |
| --------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Badger default              | `pass`          | Badger remains the default Alpha posting-store backend.                                                                     |
| Benchmark-minimal tier      | `fail_closed`   | Managed timestamps, metadata/discard markers, and all-version iteration block the restricted Alpha benchmark.               |
| Operational tier            | `fail_closed`   | Operational parity is later work and does not block benchmark-minimal startup.                                              |
| Production tier             | `unsupported`   | Production readiness is a future decision; encryption/key-registry support is not implemented.                              |
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
