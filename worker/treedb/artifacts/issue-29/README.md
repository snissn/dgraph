# Issue #29 terminal decision evidence

This directory is the normalized, committed copy of the final durability-matched live A/B produced
by clean Dgraph commit `7722f69b06ba85d13b6fb28a7b57a8e6d711677b` with exact Gomap candidate
`84e3f6652651e17401ea2006da99ef90d3533a6f`.

The Gomap candidate is bound to release
[`treedb-power-loss-cert-84e3f665`](https://github.com/snissn/gomap/releases/tag/treedb-power-loss-cert-84e3f665).
Its certificate bundle seal SHA256 is
`9ab873c48169929ee0918284baac5ab487f815a8b9faa13a87a10f3e133c747c`; the retained archive SHA256 is
`d698298df6b41408a95af43e88167e92757ad6d73ebb74fcddae52099c595b6a`.

- [`report.md`](report.md) is the final decision report and links to the 12 raw live results under
  `live/`.
- [`context.txt`](context.txt) records the benchmark CPU, RAM, storage, filesystem, environment,
  dependency, commands, workload shape, and initial host load.
- [`microbench/raw.txt`](microbench/raw.txt) is the five-sample adapter microbenchmark output.
- `profiles/` contains separate TreeDB CPU profiles, top summaries, and profile-run metadata.
  Profile-run throughput is diagnostic and is not part of the A/B decision.

All logical parity, schema, posting, fail-closed unsupported-feature, and restart/reopen checks
passed. Both performance gates failed, so the decision is **STOP** and Badger remains the production
default:

| Durability | Badger median | TreeDB median | TreeDB delta |              Gate |
| ---------- | ------------: | ------------: | -----------: | ----------------: |
| relaxed    | 6130.60 ops/s | 4989.56 ops/s |      -18.61% | no worse than -3% |
| durable    |  991.80 ops/s |  519.95 ops/s |      -47.57% | no worse than -3% |

The durable Badger row is variable (11.82% CV), but TreeDB remains below every accepted Badger
repeat. The relaxed rows are stable (Badger 0.77% CV and TreeDB 0.19% CV), so that miss is not a
single-repeat outlier. The workload, fingerprint, topology, seed, measurement boundary, and three
accepted repeats per cell are unchanged from issue #27.

## Measured residual

Independent per-metric medians across the durable TreeDB rows record 811 public durable store writes
for 400 timed application writes, 327 group participants, 174 groups, and 635 command-WAL file
syncs. These values summarize separate metric columns; they are not a tuple from one observed
repeat. This improves on issue #27's complete serialization, but it is insufficient: the median
durable throughput remains 47.57% behind Badger and its p95/p99 latencies are materially higher.
Value-log logical and file syncs are both zero, so the remaining barrier is not value-log
synchronization.

The relaxed rows have no foreground command-WAL or value-log syncs and record a median 1041 engine
flushes. Their public-batch call counters are valid measured zeros. Separately, a post-run code-path
audit found that the direct-point route bypasses the public-batch logical-byte counter used by the
resource table, but the frozen JSON lacks the point-append coverage counter needed to prove whether
that route was active in these rows. Normalizing that raw logical-byte zero as unavailable is
therefore a conservative later erratum/inference about counter coverage, not an artifact-backed
measurement. The flush count is not interpreted as publications per application write. The 2400
point-successor calls inspect a median 6648 sources, or 2.770 sources per call, with no iterator-
snapshot or leaf-log-segment rotations. Issue #3941 substantially reduced the earlier source-fan-in
residual, but that reduction did not close the unchanged terminal workload's performance gate.

The next bounded target is therefore the measured durable residual: classify why durable Dgraph
store calls fail to join a group, expose group eligibility and rejection reasons, and overlap or
coalesce only calls proven to have independent or identical ordering and acknowledgement semantics.
This work is tracked by [#34](https://github.com/snissn/dgraph/issues/34). The current evidence does
not support a relaxed publication-fan-out claim or justify speculative value-log, checkpoint,
iterator-rotation, or durability-weakening work.

The profiles show syscall, allocation, and runtime costs but are not sufficient alone to assign the
entire gap to one leaf function or to distinguish CPU execution from I/O wait. The decision rests on
the live matched matrix and retained counters, not profile-run throughput.

## Reproduction

The immutable matrix JSON uses schema v3, which predates the point-append coverage diagnostic. The
current report loader accepts that one missing legacy metric and fails the logical-byte value closed
as unavailable during rendering. Newly generated schema-v4 results require the diagnostic. The
compatibility path does not synthesize a point-append value or relax any other required metric.

Regenerate the committed report byte for byte into a new file with:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off go run ./worker/treedb/livebench/reportcmd \
  --repeats 3 --profile-dir worker/treedb/artifacts/issue-29/profiles \
  --out /absolute/path/to/NEW-report.md \
  worker/treedb/artifacts/issue-29/live/*/result.json
cmp /absolute/path/to/NEW-report.md worker/treedb/artifacts/issue-29/report.md
```

The original immutable run is retained outside the repository at
`/mnt/fast4tb/dgraph-29-84e3f665-artifacts`. Reproduce from the recorded Dgraph SHA in a new
absolute artifact directory:

```sh
TMPDIR=/mnt/fast4tb/tmp GOWORK=off \
  worker/treedb/run_durability_ab.sh \
  --artifact-dir /absolute/path/outside/repository/NEW_DIR
```

`NEW_DIR` is a placeholder for a new absolute directory outside the repository. Relative,
in-repository, dirty, incomplete, or mixed-contract runs are rejected by the harness.
