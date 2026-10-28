# Live Badger vs TreeDB durability A/B

This harness is the runtime complement to `BenchmarkDgraphTreeDBMatrix`. The older matrix remains a
primitive capability matrix; it is not relabeled as a live Dgraph comparison.

Run from a clean Dgraph checkout with heavy scratch space outside the repository:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off \
  worker/treedb/run_durability_ab.sh \
  --artifact-dir /mnt/fast4tb/dgraph-treedb-ab/$(date -u +%Y%m%dT%H%M%SZ)
```

Use `--smoke` only to validate construction. A decision run uses three repeats per cell by default.
The script requires an artifact directory outside the repository, refuses to reuse it, records the
exact Git/module/host context, runs the existing posting-store adapter microbenchmarks, and then
runs four live cells:

| Durability class | Badger                                                   | TreeDB                                                 |
| ---------------- | -------------------------------------------------------- | ------------------------------------------------------ |
| relaxed          | `--badger syncwrites=false`, production tier, events off | relaxed command WAL, benchmark-minimal tier, events on |
| durable          | `--badger syncwrites=true`, production tier, events off  | durable command WAL, benchmark-minimal tier, events on |

Each cell uses one Zero and one Alpha, the same deterministic schema, leased UID dataset, 60% point
reads, 20% one-hop reads, 20% unique writes, fixed operation count, concurrency, seed, and excluded
warmup. The livebench command also accepts `--workload-mix=write-heavy` for 10% point reads, 10%
one-hop reads and 80% unique writes. Its recorded query mix changes the workload fingerprint; it
cannot be pooled with the default mixed workload. Both excluded warmup and timed operations use the
selected mix. Every timed read checks the expected value; one-hop reads also check the cycle edge.
Post-run and restart checksums include canonical `source value -> target value` topology, so they
are UID-independent; unique write nodes are expected to have no edge. Validation enumerates every
node carrying `bench.value` and rejects unexpected, duplicate, or omitted UIDs before hashing. Raw
JSON includes throughput, p50/p95/p99 latency, Alpha CPU and RSS/HWM, logical and allocated posting
bytes, available write/GC/flush/checkpoint counters, recovery time, runtime-observed
backend/durability, schema status, posting checksum/count, restart parity, unsupported-feature
status, SHAs, dirty state, command, CPU/RAM/storage/filesystem/environment, and contamination.

New runs also record ten equal wall-time windows of successful RPC completions, throughput and p99
latency within the workload. Completion times reuse the existing RPC measurements; the sample
collector adds no server requests or locks. Empty windows retain zero completions and unavailable
latency. Optional maintenance metrics use the existing status requests outside the timed phase:
backend tree-apply calls and operations, plus before/after mutable bytes, queue backlog, queue
length and dependency debt. Backend apply calls are distinct from command-WAL flushes; absent
counters remain unavailable. These boundary observations alone do not establish steady-state
maintenance.

`recovery_seconds` retains its original restart-to-HTTP-health boundary. That boundary differs by
backend: Raft-owned TreeDB waits for election and ordered replay, while legacy Badger can report
health before election. New runs also record `recovery_read_seconds` through runtime identity checks
and a fresh full posting validation, then `recovery_read_write_seconds` through a new-node CommitNow
ACK and a fresh verified read at or after its commit timestamp. Both start at the same Alpha exec
boundary. These include the declared validation work, rather than estimating an internal open-only
latency. The new probe runs after all timed metrics and restart checksums; its node does not enter
them. The `recovery_probe=read-commit-read-v1` configuration is fingerprinted so these results
cannot silently pool with older health-only runs. This is a clean Alpha restart with Zero alive, not
a crash or power-loss recovery measurement.

Reports retain the original `restart median (s)` column for `recovery_seconds`. For a matrix with
the aligned probe, they add `read-ready median (s)` and `read/write-ready median (s)` columns for
the two readiness metrics, including available/total repeat counts. Compare these additive columns
for the declared equivalent end-to-end boundaries; the legacy HTTP-health column is not a matched
readiness comparison. Health-only matrices retain their original table and do not manufacture
aligned times or zeros. Mixed probe fingerprints, missing/invalid aligned metrics and excluded
diagnostic rows are rejected before reporting. Each column is an independent per-metric median, not
a sequence of measurements from one representative repeat.

Aggregation fails on an incomplete matrix, wrong backend or durability, workload mismatch, missing
metric contract, setup/timed overlap, dirty or excluded run, cross-run
revision/host/storage/environment mismatch, duplicate run ID, missing or duplicate per-cell repeat
ordinal, posting mismatch, schema failure, restart failure, or unsupported-feature status failure.
Start and final samples finding `construction_audit.py`, or host load above 75% of logical CPUs,
mark a run excluded. Unavailable counters are retained with a source and reason; they are never
manufactured. Relaxed and durable results have separate report headings and separate decisions.
Badger remains the production default. Badger vlog-write counters are not presented as semantic
flush counts.

The issue #17 decision evidence produced from Dgraph commit
`6ae8d25b27ca6ebf20d8bec4c5de6151aba341a5` is committed under
[`artifacts/issue-17`](artifacts/issue-17/README.md). The committed report uses paths relative to
that directory; each raw result retains the original absolute command and scratch path for audit.

Issue #27 repeated the unchanged decision matrix after pinning the exact power-loss-certified Gomap
candidate `121462d80f1f11190776b4d54cab7ed34e413963`. Its schema-v3 raw results retain command-WAL,
value-log, group-commit, point-successor, and rotation diagnostics. The final report, profiles,
microbenchmarks, exact provenance, STOP decision, and measured residuals are committed under
[`artifacts/issue-27`](artifacts/issue-27/README.md). Badger remains the production default.

Non-smoke decision runs automatically capture separate relaxed and durable TreeDB CPU profiles after
the matrix. Profile-run throughput is never treated as benchmark evidence, and the report links only
artifacts verified to exist. To reproduce one profile manually, reuse the committed runner binary
produced in the artifact root and raise `--timed-ops` enough to cover `--profile-seconds`:

```sh
ARTIFACT=/mnt/fast4tb/dgraph-treedb-ab/FINAL_RUN
"$ARTIFACT/bin/livebench" \
  --dgraph-bin "$ARTIFACT/bin/dgraph" \
  --artifact-dir "$ARTIFACT/profiles/treedb-durable" \
  --backend treedb --durability durable --timed-ops 10000 \
  --cpu-profile "$ARTIFACT/profiles/treedb-durable.pprof" \
  --profile-seconds 5
go tool pprof -top "$ARTIFACT/bin/dgraph" \
  "$ARTIFACT/profiles/treedb-durable.pprof"
```

For a separate TreeDB CPU-profile diagnostic, add
`--endpoint-diagnostic "$ARTIFACT/profiles/treedb-durable.endpoint.json"` to the manual command
above. The sidecar path must be new, outside the posting directory, and separate from the native
result, logs, and CPU profile. The flag requires `--backend treedb` and either `--cpu-profile` or
the explicitly excluded `--operation-diagnostic` run described below. Without the operation flag,
the original CPU-profile requirement is unchanged. Every endpoint-diagnostic run is explicitly
excluded from performance acceptance, including CPU-only endpoint runs. CPU profiling without an
endpoint or other diagnostic flag retains its existing behavior.

The immutable schema-v1 sidecar records posting-directory relative filenames, logical sizes, stat
blocks and allocated bytes, modification times, and existing `/debug/store` status and numeric
counters. Its two observations target workload completion before profile wait, and exactly
`TimedFinished + 5s` before validation or restart. No foreground operations or checkpoint run
between them. Each observation retains start/end timestamps and delay from its target, with separate
file-finish and status-start timestamps. Background publication can continue; these sequential
metadata/status reads are not atomic snapshots. Observation overhead and the passive wait belong
only to the diagnostic run and must not replace acceptance measurements or revise their original
storage endpoints.

For operation-kind and server-phase diagnosis on either backend, use a separate run with
`--operation-diagnostic "$ARTIFACT/operations.json"`. Its new immutable schema-v1 sidecar retains
every deterministic operation index and worker, kind (`point_read`, `one_hop_read`, `write`),
start/RPC-return/end timestamps, monotonic RPC/full-wall nanoseconds, outcome, and copied optional
`api.Latency` scalars. Full wall includes client response validation; RPC duration includes the
existing client wrapper and transport. Server phases are not independent additive timings. No keys,
responses or payloads are retained. Rows are preallocated only when opted in and written by their
owning workers, outside the native sample channel.

The sidecar also retains the raw bodies and request timestamps of the existing before-workload and
native-after-metrics `/debug/store` and labeled Prometheus fetches. No per-operation polling or
extra requests are added. The after boundary can follow profile completion or the endpoint passive
interval; it is not called an immediate workload-end cut. A failed workload cancels promptly, drains
its workers, and retains the operation rows with `workload_succeeded=false`; its after boundary
remains absent (empty timestamps/body). Row outcomes describe RPC/read validation, while aggregate
write-UID validation can also fail the workload.

Serialization follows `TimedFinished`. Operation instrumentation, observer overhead, and any passive
wait remain diagnostic only: native result schema and the existing microsecond-rounded latency
calculation are unchanged, but these runs explicitly set `Context.Excluded` and an overhead reason.
Native acceptance aggregation rejects them. The flag is optional and does not enable runtime
tracing.

## Paired Alpha memory attribution (separate diagnostic only)

Use `--memory-diagnostic /absolute/private-artifacts/memory` as a new filename prefix in a
separately admitted run. Its parent directory must exist (the new run artifact directory itself is
also supported). Both Badger and TreeDB use identical hooks: after warmup before timed baseline
counters, and after the native post-workload CPU/HWM/disk/store/Prometheus metrics have been
captured, before schema/posting validation or restart. It refuses combination with CPU, endpoint,
operation or storage diagnostics so other waits cannot move these boundaries.

Each boundary writes seven immutable raw artifacts: Alpha `/proc/PID/status`,
`/proc/PID/smaps_rollup` and `/debug/vars`, followed by `/debug/pprof/heap?gc=1`, then those same
three raw-state sources again. The GC-assisted heap profile gives a current retained Go heap sample;
the manifest records whether that request was attempted. Pre-GC raw state is preserved before the
request, and post-GC raw state also includes serving/serializing the profile. Each schema-v1
manifest binds the actual Alpha PID, run/backend/durability, server hash, named boundary, capture
envelope, completion/error, and each raw artifact's source, timestamps, path, byte count and
SHA-256. A failed capture preserves the completed raw artifacts and an incomplete/error manifest and
refuses a successful native result. Existing destinations, cluster-state paths and resolved aliases
into cluster state are refused using the maintained sidecar path guards. Heap artifact paths are
forwarded in `Context.Profiles` for ordinary `go tool pprof` inspection.

For a within-run retained-heap delta, using that exact run's prefix and binary:

```sh
go tool pprof -top -sample_index=inuse_space \
  -base /absolute/private-artifacts/memory.after_warmup_before_counters.heap.gc1.pprof \
  /absolute/private-artifacts/memory.after_native_metrics_before_validation.heap.gc1.pprof
```

This subtraction compares two GC-assisted live-heap samples; it is not allocation bytes/op or a
direct decomposition of the kernel RSS/HWM difference between backends.

The first observation is outside the timed CPU baseline but deliberately changes the process's
GC/heap state before workload execution. It can itself raise the later kernel `VmHWM`; the second
GC/profile request runs only after native metrics were captured. Every such run is explicitly
excluded from performance acceptance. Do not substitute its HWM, throughput or GC count for the
uninstrumented A/B rows. `/debug/vars` and heap profile serialization allocate and perturb state;
these sequential observations are not atomic snapshots. Forced GC does not promise immediate return
of idle memory to the OS or clearing kernel HWM. Go heap retention is distinct from cumulative
allocation churn, off-heap allocator memory, file mappings and RSS. Compare raw runtime state with
`smaps_rollup` before attributing the original RSS difference. This diagnostic neither establishes
sustained memory equilibrium nor qualifies resource/performance acceptance.

## Native-boundary storage attribution (separate diagnostic only)

Use `--storage-diagnostic /new/path/storage.json` on a separately admitted run to attribute the
native logical and allocated posting totals to files. The new flag works with either backend, alone
or with `--operation-diagnostic`; it rejects `--cpu-profile` and `--endpoint-diagnostic`, whose
waits can delay this boundary. In particular, the older endpoint diagnostic waits until
`TimedFinished + 5s` before the native disk walk. Its native totals and per-file cuts cannot be
substituted for the original unprofiled storage boundary.

The immutable schema-v2 storage sidecar records the run/backend/durability, posting directory,
`TimedFinished`, `native_posting_disk_usage_prevalidation` boundary, walk start/finish and
serialization start, completion/error, observed regular-file count, native logical/allocated totals,
and relative per-file names, sizes, stat blocks, allocated bytes and modification times. Rows and
native totals use the same `FileInfo` from the SAME existing walk, after the existing CPU/HWM reads
and before post-metric requests, schema/posting validation or restart. There is no second walk,
extra stat, status request, passive wait, checkpoint or file deletion. Only regular files
contribute, as in the original native measurement.

Schema v2 also binds the full native config and reproduction context (source, measured binary
hash/build info, Go version, environment and command). It records the separate harness executable
SHA256/build info, collected before native processes start. Freeze the harness tree/diff,
dependencies, build flags and original workload manifest before any separately authorized
collection; executable provenance does not by itself authenticate historical binary equivalence.

Each same-walk row adds device/inode as decimal strings, link count, file mode, identity
availability and `observed_at`. The callback processes an already obtained FileInfo: this timestamp
follows the underlying stat and is **not** the exact stat time. Missing Stat_t is retained as an
identity-unavailable partial row and rejects completed attribution. Repeated device/inode identities
are listed as aliases; their sizes still count once per path, preserving the native metric.
Directory enumeration omits open-unlinked files. Blocks*512 measures filesystem allocation, not
extent ownership or live payload. Sparse, compressed, shared/reflink and block-rounded files limit
interpretation; allocation greater than apparent size does not establish preallocation.

The sidecar reuses the existing later Prometheus and `/debug/store` requests, preserving their raw
bodies (including available non-200 or truncated response bytes), HTTP status, start/end and request errors. `walk_to_status_gap_ns` spans the walk end to store-request
start, including the intervening Prometheus request; it must not be labelled status-request cost.
Raw stats remain strings, including uint64 root identities beyond 2^53 and boolean values. Resolved
profile, events policy, and the `treedb.write_path.mode` command-WAL route are typed optional fields;
missing fields and Badger-specific unavailable TreeDB gauges carry reasons, not invented zeroes.
Request failures and engine `stats_error` remain distinct errors. Status follows the file walk and
its component stats have different locks/times: it is not an atomic root/page/leaf census. Current
page high-water, cumulative allocator counters, durable-root fields and leaf retention gauges must
not be subtracted or promoted to physical reachability/live-dead byte accounting.


This is a sequential metadata walk while Alpha/background publication may continue. The envelope
brackets the walk; it does not timestamp each filesystem change or establish an atomic/quiescent
snapshot. Files created after a directory was enumerated may be absent, and mutation between
observations remains possible. Modification times are metadata, not observation timestamps. These
limits apply to both the native sums and their same-observation file attribution. The later request envelope is validated separately from file
inventory; missing optional engine fields do not invalidate a complete inventory.

`validateStorageDiagnostic` compares the sidecar against the paired native result: identity, posting
root, named boundary, ordered envelope, diagnostic exclusion, complete/error-free nonempty rows
matching the walk count, unique confined relative paths, nonnegative sizes/blocks,
allocated=blocks\*512, checked integer multiplication/summation, aliases and exact file-sum/native totals. The diagnostic requires byte totals at most
2^53 to preserve exact equality with the existing float64 native metrics. It does not infer
allocated<=logical: sparse and block-rounded files can differ in either direction. Missing, partial,
sum-inconsistent or boundary-inconsistent observations refuse validation. Failed/partial raw
observations remain immutable sidecars with request/walk/validation errors or an invalid/incomplete contract, and cannot
produce a successful native result. Successful sidecar validation is a storage-observer check;
native checksum, schema, restart and provenance validation are separately required. Internal
consistency does not authenticate a coordinated rewrite of an entire packet.

The path must be new, outside the entire run cluster (postings and Alpha/Zero WAL directories), and
separate from native results, logs and other sidecars. A destination cannot be the cluster directory
or any ancestor that the run must create. Ordinary sidecars beside the cluster within the artifact
directory remain valid. Storage, operation and endpoint sidecar guards resolve existing ancestor
symlinks for both the posting root and destination, including future nonexistent suffixes. Aliases
into postings or reserved outputs refuse before artifact creation/process launch. Dangling/cyclic
aliases and inaccessible ancestors also refuse. Each observer rechecks at its write boundary and
writes to the resolved destination; immutable creation still uses O_EXCL. This prevents silently
following a changed caller alias, but does not claim immunity to adversarial concurrent replacement
of resolved parent directories. Keep runner paths under exclusive ownership through the run. The
opt-in observer explicitly excludes the native result from performance acceptance. Default-off
behavior, native result schema, durability, WAL and persistent value-log semantics remain unchanged.
Historical endpoint and operation sidecars keep their existing schemas and measurement identities.
This flag grants no collection campaign and clears none of M3's four original HOLDs; future
collection needs exact source/tool/binary bindings, independent review and root runner admission
under the unchanged policy.
