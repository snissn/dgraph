# TreeDB Integration Scaffold

This package is the Dgraph-side TreeDB entry point for the posting-store replacement work. It pins
and compile-tests current TreeDB APIs and supports an explicit restricted benchmark-minimal Alpha
runtime. Badger remains the default and production backend.

Current TreeDB head used by this branch:

- module: `github.com/snissn/gomap`
- version: `v0.6.2-0.20260711114710-3a3e3c72a1a8`
- commit: `3a3e3c72a1a8f7cb208d2770bbcf4bcb7d0332be`

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
- one-owner Alpha lifecycle wiring, TreeDB GC/stats/status, basic posting/schema persistence, and a
  required ordered successful-commit event bus for internal ACL and GraphQL schema watchers

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

| Tier                | Purpose                                                   | Required capability families                                                                                                                                               | Current decision                                                         |
| ------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `benchmark_minimal` | Restricted live Alpha A/B benchmark                       | durable open/lifecycle, point and snapshot primitives, a TreeDBStore implementation, externally assigned timestamps, `UserMeta`/discard markers, and all-version iteration | supported only through the explicit restricted TreeDB Alpha selector     |
| `operational`       | Operational parity after a successful Alpha decision gate | everything above plus nonzero TTL, backup/export/import, subscriptions, Badger protobuf translation, and monitoring/cache status                                           | not ticketed by the Alpha graph; unsupported invocations fail explicitly |
| `production`        | A future production-readiness decision                    | everything above plus encryption/key-registry integration                                                                                                                  | unsupported; TreeDB is not a production backend                          |

TreeDB-native conditional transactions and in-memory posting storage are excluded from all three
tiers. Dgraph owns posting conflict detection, and this integration lane is persistent. Their
absence therefore cannot silently become a benchmark blocker or a supported capability.

Current non-supported TreeDB readiness entries. These are Badger-compatibility gaps in Dgraph's
current call sites, not proof that TreeDB lacks every lower-level primitive in the area:

- `command_wal_conditional_transactions` (`unsupported`, no tier): native conditional transactions
  fail closed; the Dgraph adapter must use Dgraph-owned conflict detection.
- `badger_entry_ttl` (`unsupported`, operational): nonzero `Entry.ExpiresAt` values must be rejected
  until an expiry contract exists. TTL does not block the benchmark-minimal tier.
- `badger_stream_import_export` (`disabled_need_blocker`, operational): `NewStreamAt`,
  `Stream.Orchestrate`, `NewStreamWriter`.
- `badger_subscriptions` (`disabled_need_blocker`, operational): the restricted runtime bridges
  future successful commits to Dgraph's internal `worker.SubscribeForUpdates` watchers; TreeDB Alpha
  startup requires events enabled, but does not claim the full Badger `DB.Subscribe` contract.
- `encryption_key_registry` (`unsupported`, production): encryption/key-registry requests fail
  closed until TreeDB exposes compatible semantics.
- `badger_protobuf_compatibility` (`disabled_need_blocker`, operational): Badger `pb.KV`,
  `pb.KVList`, and `pb.Match` shapes.
- `metrics_cache_apis` (`disabled_want`, operational): Badger cache sizing and metrics used by
  monitoring are not wired for TreeDB.
- `in_memory_posting_store` (`unsupported`, no tier): Dgraph's posting store is persistent;
  in-memory TreeDB mode fails closed.

## Posting-store adapter contract

Issue #20 adds the Dgraph-side adapter seam in `posting.Store`. The contract is deliberately narrow
and maps to concrete posting-store call sites instead of a generic key/value wishlist:

- managed read transactions at a caller-provided read timestamp;
- managed write transactions committed at a caller-provided commit timestamp;
- posting entry `UserMeta`, `ExpiresAt`, and discard-earlier-version markers;
- prefix iteration with forward/reverse, value prefetch, and `AllVersions` controls; and
- item views exposing keys, values, versions, metadata, expiry, deletion/expiry state, and value
  size.

`posting.NewBadgerStore` remains the production implementation, and `posting.NewTxnWriter` still
selects Badger by default. `posting.TreeDBStore` implements the benchmark-minimal seam over gomap
external MVCC; `posting.NewTxnWriterForStore` selects it only through an explicit experiment path.
The restricted Alpha runtime uses that seam for basic posting and schema paths. Later-tier calls
must use `CheckFeatureAvailable(...)` and fail at invocation rather than silently dropping TTL,
stream/import/export, subscription, protobuf, monitoring, or encryption semantics.

## Compatibility blocker matrix

Issue #6 recorded the original Dgraph-side decisions for Badger feature families. Issue #18 assigns
those records to cumulative tiers in `PostingCompatibilityMatrix()`. Runtime selector code calls
`CheckPostingBackendReady()`, which checks only `benchmark_minimal`; later-tier entry points must
gate their own capability with `CheckFeatureAvailable(...)`.

The adapter-backed benchmark-minimal compatibility rows now pass: lifecycle/GC/stats, managed
timestamps, entry metadata/discard markers, and all-version/key iteration are `supported`.

TTL, stream backup/export/import, full Badger subscription/protobuf compatibility, and metrics/cache
APIs are operational requirements. The restricted event bridge is narrower: it emits future ordered
successful commits only for Dgraph's internal watchers. Encryption/key registry is a production
requirement. Conditional transactions and in-memory mode are excluded from the integration tiers.
Unsupported or blocker rows fail with the stable feature ID/status from `FeatureReadinessError`;
they never fall back to Badger or return partial TreeDB behavior.

The remaining direct `worker.pstore` uses are intentionally bounded: snapshot send/receive, external
snapshot import, online restore, predicate move, indexed sort, count-range iteration, inequality
token iteration, Badger subscriptions, and cache resizing are protected by an exported entry-point
or immediate operation guard; offline restore always opens its own real Badger handle. Draft
sync/discard/table sizing uses explicit `pstore != nil` Badger branches. Guard-before-state tests
cover subscription, indexed sort, snapshot send/import, backup, export, restore, and cache resizing;
the same `requireBadgerPostingStore` guard protects the remaining listed entry points.

## Experimental Alpha selector

The explicit Alpha selector accepts a restricted TreeDB runtime:

```sh
dgraph alpha --posting-store 'backend=badger; tier=production; durability=durable; events=false;'
dgraph alpha --posting-store 'backend=treedb'
dgraph alpha --posting-store 'backend=treedb; tier=benchmark_minimal; durability=durable; events=true;'
```

Badger remains the default and continues to open through the existing managed Badger path. TreeDB
startup accepts exactly `benchmark_minimal` with `durable` or `relaxed` durability and opens one
TreeDB-backed posting-store owner; there is no silent fallback to Badger. Encryption, in-memory,
nonzero TTL, backup/export/import/restore, snapshot transfer, and Badger-compatible subscriptions
fail explicitly at their entry points. TreeDB defaults to `events=true` and rejects an explicit
`events=false`, because ACL and GraphQL schema caches depend on matching future successful commits
through the restricted Dgraph-owned event bridge. The low-level disabled-event path exists only for
overhead measurement and defensive shutdown; it is not a valid TreeDB Alpha runtime selection.

## Performance boundary

Capability classification runs only during backend selection or at an optional feature's entry
point. It is not called from per-key, per-posting, iterator-step, or commit loops. The low-level
commit-event decorator can be omitted in the disabled-path microbenchmark and avoids event copies
when enabled without subscribers. TreeDB Alpha startup always enables it. The event subscription
boundary is write-transaction creation: a transaction created before the first subscriber registers
is intentionally not instrumented, even if it commits while that subscriber is active.

## Operator gates and default decision

Issue #8 finalizes the current operator-facing gate report in `OperatorGateReport()`:

| Gate                        | Current state | Operator decision                                                                                |
| --------------------------- | ------------- | ------------------------------------------------------------------------------------------------ |
| Badger default              | `pass`        | Badger remains the default Alpha posting-store backend.                                          |
| Benchmark-minimal tier      | `pass`        | The explicit restricted selector may run the live Alpha benchmark.                               |
| Operational tier            | `fail_closed` | Operational parity is later work and does not block benchmark-minimal startup.                   |
| Production tier             | `unsupported` | Production readiness is a future decision; encryption/key-registry support is not implemented.   |
| TreeDB primitive durability | `pass`        | TreeDB can open, write, close, reopen, run GC, and report stats through the Alpha owner.         |
| TreeDB selector             | `pass`        | An explicit TreeDB selector opens exactly one restricted benchmark-minimal backend.              |
| Posting/schema workflows    | `pass`        | Basic point posting mutation/read and schema persistence pass across restart.                    |
| Backup/restore/export       | `fail_closed` | Stream backup/export/import and snapshot workflows remain Badger-only.                           |
| Subscriptions               | `fail_closed` | Restricted internal delivery is supported with events enabled; full Badger compatibility is not. |
| Encryption/key registry     | `unsupported` | TreeDB encryption/key-registry integration is unsupported in this integration lane.              |
| Benchmark matrix            | `pass`        | The benchmark matrix is available for current evidence and future before/after runs.             |
| Default decision            | `pass`        | `keep_badger_default`; TreeDB stays explicit, experimental, and restricted.                      |

Final decision for this tracker: keep Badger as the default. TreeDB is not production-ready or a
drop-in posting-store backend; only the benchmark-minimal tier is enabled, and later-tier workflows
remain unavailable until their fail-closed rows move to supported with tests and evidence.

Focused validation:

```sh
GOWORK=off go test ./worker/treedb -count=1
```

## Benchmark matrix

This package also owns the Dgraph Badger-vs-TreeDB evidence matrix for the TreeDB posting-store
work. The matrix is intentionally limited to primitive benchmark evidence; the live durability-
matched runtime A/B belongs to the dependent benchmark issue.

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
`Stats` primitives. The posting package separately benchmarks direct gomap MVCC against
`posting.TreeDBStore`. Skipped rows document later-tier contracts TreeDB cannot yet run: TTL, stream
export/import, subscriptions, and encryption/key registry support.
