# TreeDB Integration Scaffold

This package is the Dgraph-side TreeDB entry point for the posting-store replacement work. It pins
and compile-tests current TreeDB APIs and supports an explicit restricted benchmark-minimal Alpha
runtime. Badger remains the default and production backend.

Durable posting writes use the bounded timestamp-preserving scheduler described in
[Durable posting commit cohorts](DURABLE_COMMIT_COHORTS.md).

The exact `github.com/snissn/gomap` dependency is pinned in the repository's [go.mod](../../go.mod).
Resolve it with `GOWORK=off go list -m -json github.com/snissn/gomap`; qualification must record
that version, checksum and any local replacement rather than reuse a historical source label here.
The public pin includes the ordinary command-WAL append-capture owner and its fail-stop error
handling; the Dgraph `pressure_v4` adapter remains unchanged. The CRC assembly dependency is also
pinned to a published revision, so this configuration needs no local Gomap or CRC replacement.
Dependency resolution and owning-package checks alone do not qualify throughput, memory or recovery.

What is wired here:

- durable TreeDB command-WAL profile selection
- Dgraph-shaped durable command-WAL profile selection that rejects benchmark-only no-WAL profiles
- explicit `raft_checkpoint` selection with Raft-owned recovery and an externally owned no-WAL DATA
  root, independent of the standalone command-WAL engine
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

The ordinary durable command-WAL COW path uses one ordered submitter, two reusable aggregate records
and one completion owner. Idle groups drain ready requests and use known-size ordered submission
without opening an incremental builder or delaying first work. While a disk barrier runs, the
successor stages native operands without preparing a root or assigning a WAL command. It seals on an
actual physical-work/byte/request/shared-staging bound, predecessor completion or shutdown. There is
no adapter collection timer. Sealed submission never starts an engine collection wait. Temporary
queue emptiness and producer completion fences do not split an open successor. `TxnWriter.Wait`
forwards a progress wake through the commit-event decorator before waiting. Both records share a 4
MiB staged-input allowance; oversized atomic inputs stage alone. The original 65 request credits and
TreeDB's own resource admission remain in force. Append and COW installation stay ordered, and
callbacks/events wait for the original captured durable prefix. Unsupported profiles retain the
synchronous FIFO implementation.

TreeDB estimates conservative physical work before copying each posting entry. A nonempty write
transaction reports `badger.ErrTxnTooBig` before changing its accepted prefix, so Dgraph's existing
`CommitToDisk` loop commits bounded chunks at the original timestamp. An oversized first entry is a
terminal error. The adapter also bounds combined cohorts so it cannot recombine those chunks into an
oversized command. Each assigned TreeDB command remains atomic; Dgraph advances Oracle visibility
and success-only cache publication only after all chunk callbacks succeed. If a later chunk fails,
the pending transaction keeps its original Raft mutation and decision available for replay even when
the attempted-apply marker has advanced. Storage errors never authorize adapter subdivision or retry
of an assigned command.

### Snapshot publication and discard floors

Applying a Raft snapshot preserves Badger's `SetDiscardTs` hint. It does not advance TreeDB's
persisted discard floor: snapshot `ReadTs` is the maximum published commit timestamp observed in the
scanned Raft logs, not the minimum timestamp still needed by readers or posting dependencies. TreeDB
rejects fresh reads and commits at or below its floor even before pruning any versions. Posting
lists can lazily read split parts at an older root `minTs`, and a cold rollup can publish an older
represented commit timestamp despite unrelated newer commits. These paths must remain admitted after
snapshot application.

For ordinary `durable`/`relaxed` TreeDB, automatic history pruning remains disabled; the explicit
`TreeDBStore.AdvanceDiscardFloor` API requires a caller to prove the reader, dependency and
late-publication boundary. The `raft_checkpoint` route instead uses an explicit replicated semantic
floor S and private maintenance admission for local physical floor P. Its complete main/split
baselines, read drain and exact checkpoint sequence are described in
[RAFT_RECOVERY_PRODUCTION.md](RAFT_RECOVERY_PRODUCTION.md). Snapshot `ReadTs` alone authorizes
neither route's physical floor. Value-log segment deletion remains reachability-based. Stopping
automatic advancement cannot repair an already raised monotonic floor; TreeDB is pre-alpha, so
affected experiments should rebuild their DB directories rather than attempt to lower persisted
floors.

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

The remaining direct `worker.pstore` uses are intentionally bounded: legacy snapshot send/receive,
external snapshot import, online restore, predicate move, indexed sort, count-range iteration,
inequality token iteration, Badger subscriptions, and cache resizing are protected by an exported
entry-point or immediate operation guard; offline restore always opens its own real Badger handle.
Draft sync/discard/table sizing uses explicit `pstore != nil` Badger branches. Guard-before-state
tests cover subscription, indexed sort, legacy snapshot send/import, backup, export, restore, and
cache resizing; the same `requireBadgerPostingStore` guard protects the remaining listed entry
points.

## Experimental Alpha selector

The explicit Alpha selector accepts a restricted TreeDB runtime:

```sh
dgraph alpha --posting-store 'backend=badger; tier=production; durability=durable; events=false;'
dgraph alpha --posting-store 'backend=treedb'
dgraph alpha --posting-store 'backend=treedb; tier=benchmark_minimal; durability=durable; events=true;'
dgraph alpha --posting-store 'backend=treedb; tier=benchmark_minimal; durability=raft_checkpoint; events=true;'
```

Badger remains the default and continues to open through the existing managed Badger path. TreeDB
startup accepts exactly `benchmark_minimal` with `durable`, `relaxed` or `raft_checkpoint`
durability and opens one TreeDB-backed posting-store owner; there is no silent fallback to Badger.
`raft_checkpoint` uses the ordered Raft WAL as recovery authority and retained exact DATA roots. Its
explicit native Raft checkpoint transport supports voter join/rejoin; learners remain guarded. It
does not enable legacy Badger snapshot/import formats or operational backup/export/restore.
Encryption, in-memory, nonzero TTL, those external-file routes and Badger-compatible subscriptions
fail explicitly at their entry points. TreeDB defaults to `events=true` and rejects an explicit
`events=false`, because ACL and GraphQL schema caches depend on matching future successful commits
through the restricted Dgraph-owned event bridge. The low-level disabled-event path exists only for
overhead measurement and defensive shutdown; it is not a valid TreeDB Alpha runtime selection.

See [RAFT_RECOVERY_PRODUCTION.md](RAFT_RECOVERY_PRODUCTION.md) for the route's ordered schema/admin,
retry, pending-state, maintenance and native snapshot contracts and remaining qualification gates.
The selector does not promote TreeDB to the operational or production capability tier.

## Performance boundary

Capability classification runs only during backend selection or at an optional feature's entry
point. It is not called from per-key, per-posting, iterator-step, or commit loops. The low-level
commit-event decorator can be omitted in the disabled-path microbenchmark and avoids event copies
when enabled without subscribers. TreeDB Alpha startup always enables it. The event subscription
boundary is write-transaction creation: a transaction created before the first subscriber registers
is intentionally not instrumented, even if it commits while that subscriber is active.

## Operator gates and default decision

Issue #8 finalizes the current operator-facing gate report in `OperatorGateReport()`:

| Gate                        | Current state | Operator decision                                                                                                  |
| --------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------ |
| Badger default              | `pass`        | Badger remains the default Alpha posting-store backend.                                                            |
| Benchmark-minimal tier      | `pass`        | The explicit restricted selector may run the live Alpha benchmark.                                                 |
| Operational tier            | `fail_closed` | Operational parity is later work and does not block benchmark-minimal startup.                                     |
| Production tier             | `unsupported` | Production readiness is a future decision; encryption/key-registry support is not implemented.                     |
| TreeDB primitive durability | `pass`        | TreeDB can open, write, close, reopen, run GC, and report stats through the Alpha owner.                           |
| TreeDB selector             | `pass`        | An explicit TreeDB selector opens exactly one restricted benchmark-minimal backend.                                |
| Posting/schema workflows    | `pass`        | Basic point posting mutation/read and schema persistence pass across restart.                                      |
| Backup/restore/export       | `fail_closed` | Stream backup/export/import and legacy snapshots remain Badger-only; native Raft checkpoint transport is separate. |
| Subscriptions               | `fail_closed` | Restricted internal delivery is supported with events enabled; full Badger compatibility is not.                   |
| Encryption/key registry     | `unsupported` | TreeDB encryption/key-registry integration is unsupported in this integration lane.                                |
| Benchmark matrix            | `pass`        | The benchmark matrix is available for current evidence and future before/after runs.                               |
| Default decision            | `pass`        | `keep_badger_default`; TreeDB stays explicit, experimental, and restricted.                                        |

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

### Explicit durability benchmark identity

The canonical `run_durability_ab.sh` still runs Badger/TreeDB × relaxed/durable × three repeats with
the same gRPC workload and 3% durable throughput gate. New schema-v5 rows explicitly pass
`--survive` to both Zero and Alpha and retain each process's resolved redacted `HardSync`
configuration. Alpha posting backend, profile and memtable are checked before setup/timing and after
restart. The measured executable SHA256 and Go build information are recorded and the binary is
checked again before result publication. Raw configuration/store witnesses are immutable
`runtime-<phase>-<process>.*` snapshots; their hashes are in the row. Loading and reporting
schema-v5 rows reopens every snapshot relative to the result file, verifies its hash and checks that
the resolved configuration and posting route support the recorded claims. Missing, modified or
contradictory witnesses are refused, including changes after loading.

`SURVIVE=process` preserves the historical default. Use `SURVIVE=filesystem` for a separate matched
filesystem-survival matrix. Survival participates in the workload fingerprint, so the report refuses
pooling the two profiles. Existing schema-v3/v4 rows are readable unchanged: their `durable` labels
describe posting store sync and the script omitted `--survive`, using process survival. They do not
establish filesystem or complete RPC power-loss durability. Report rendering adds this erratum
without editing the old artifacts.

`REQUIRE_TREEDB_MEMTABLE=cow_ordered` refuses a TreeDB arm unless its actual startup and restart
stats confirm COW. This checks the server binary's selection; it does not silently change that
binary's memtable configuration. For the Raft-owned durable candidate, additionally set
`TREEDB_DURABLE_POLICY=raft_checkpoint` and `SURVIVE=filesystem`. Only the TreeDB durable arm
changes recovery owner; its relaxed arm keeps the normal relaxed posting WAL and Badger is
unchanged. An older server lacking the candidate selector fails before measurement. Each repeat of a
cell must use the same policy and requested memtable. Defaults do not enable the candidate.

The Raft-owned profile has no posting-store command-WAL group collector. Its five group metrics are
explicitly unavailable with that reason; they are not reported as zero physical syncs. Persistent
value-log counters remain required. Logical write bytes are unavailable if their existing
command-WAL counter is absent; client payload bytes are not substituted for storage bytes.

A schema-v5 row is a source/runtime-bound benchmark result, not a durability certificate. The
existing restart timer ends at Alpha health after a **clean** Alpha restart; Zero remains running.
Runtime observers execute outside that timer and the workload timer. Immediate-ACK SIGKILL,
accepted-install recovery, real peer changes and filesystem power-loss qualification remain separate
required proofs. CPU/endpoint/operation/storage diagnostics remain excluded from accepted timing
rows. Microbenchmark stdout/stderr are retained separately; `raw.txt` remains a compatibility
concatenation.

The executable must contain Go `vcs.revision` metadata matching the checkout that launches the
harness. If the selected toolchain omits that metadata for linked Git worktrees, build from a normal
clone of the exact commit instead. Missing revision metadata is an error before server startup;
passing a source path or naming a binary after a commit does not substitute for that binding.
