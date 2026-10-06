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
warmup. Every timed read is checked against the expected value and cycle edge. Post-run and restart
checksums include canonical `source value -> target value` topology, so they are UID-independent;
unique write nodes are expected to have no edge. Validation enumerates every node carrying
`bench.value` and rejects unexpected, duplicate, or omitted UIDs before hashing. Raw JSON includes
throughput, p50/p95/p99 latency, Alpha CPU and RSS/HWM, logical and allocated posting bytes,
available write/GC/flush/checkpoint counters, recovery time, runtime-observed backend/durability,
schema status, posting checksum/count, restart parity, unsupported-feature status, SHAs, dirty
state, command, CPU/RAM/storage/filesystem/environment, and contamination.

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
the original CPU-profile requirement is unchanged.

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

## Native-boundary storage attribution (separate diagnostic only)

Use `--storage-diagnostic /new/path/storage.json` on a separately admitted run to attribute the
native logical and allocated posting totals to files. The new flag works with either backend, alone
or with `--operation-diagnostic`; it rejects `--cpu-profile` and `--endpoint-diagnostic`, whose
waits can delay this boundary. In particular, the older endpoint diagnostic waits until
`TimedFinished + 5s` before the native disk walk. Its native totals and per-file cuts cannot be
substituted for the original unprofiled storage boundary.

The immutable schema-v1 storage sidecar records the run/backend/durability, posting directory,
`TimedFinished`, `native_posting_disk_usage_prevalidation` boundary, walk start/finish and
serialization start, completion/error, observed regular-file count, native logical/allocated totals,
and relative per-file names, sizes, stat blocks, allocated bytes and modification times. Rows and
native totals use the same `FileInfo` from the SAME existing walk, after the existing CPU/HWM reads
and before post-metric requests, schema/posting validation or restart. There is no second walk,
extra stat, status request, passive wait, checkpoint or file deletion. Only regular files
contribute, as in the original native measurement.

This is a sequential metadata walk while Alpha/background publication may continue. The envelope
brackets the walk; it does not timestamp each filesystem change or establish an atomic/quiescent
snapshot. Files created after a directory was enumerated may be absent, and mutation between
observations remains possible. Modification times are metadata, not observation timestamps. These
limits apply to both the native sums and their same-observation file attribution.

`validateStorageDiagnostic` compares the sidecar against the paired native result: identity, posting
root, named boundary, ordered envelope, diagnostic exclusion, complete/error-free nonempty rows
matching the walk count, unique confined relative paths, nonnegative sizes/blocks,
allocated=blocks\*512, and exact file-sum/native totals. The diagnostic requires byte totals at most
2^53-1 to preserve exact equality with the existing float64 native metrics. It does not infer
allocated<=logical: sparse and block-rounded files can differ in either direction. Missing, partial,
sum-inconsistent or boundary-inconsistent observations refuse validation. Failed/partial raw
observations remain immutable sidecars with an error or an invalid/incomplete contract, and cannot
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
