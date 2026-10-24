# Raft-owned recovery prototype: implementation and diagnostic evidence

Status: **supported-path prototype validated; production integration and historical promotion gates
remain open**. No main merge. Both topic branches are preserved for review.

## Architecture and source binding

The selected architecture is durable Raft input -> serial mutation/Delta apply -> complete COW
materialization -> exact certified checkpoint -> authorized Raft retirement. Recovery restores
exactly the certificate, checks identity and contiguous retained history, then replays the committed
suffix before health. This removes the independent posting-store command-WAL barrier from the Dgraph
path. It preserves the existing HTTP transaction ACK boundary and standalone TreeDB WriteSync.

The complete architecture, protocol invariants, resource design and production completion plan are
in
[Gomap's design](https://github.com/snissn/gomap/blob/2cb705090aa77da14bb0cf4afc8e9b185115e8f8/docs/TREEDB_EXTERNAL_RECOVERY_PROTOTYPE.md).
Dgraph integration details and explicit unsupported operations are in
[RAFT_CHECKPOINT_PROTOTYPE.md](../RAFT_CHECKPOINT_PROTOTYPE.md).

- Gomap product: `2cb705090aa77da14bb0cf4afc8e9b185115e8f8`, branch
  `codex/5204-raft-checkpoint-prototype`.
- Dgraph binary product: `c3b057cd7d26da79c36007504734c45ac7ff690e`, branch
  `codex/34-raft-checkpoint-prototype`.
- Diagnostic driver: `9036af092b60548b048a95ba51373bcce2b2a936`; its only change from the binary
  source is Python port preflight reuse.
- Exact executable/driver SHA-256, clean source status, resolved sibling module and environment are
  retained in `PROTOTYPE_RESULTS.json` and the raw manifest.
- Go 1.26.9, GOWORK=off, CGO enabled, GOMAXPROCS=2 for each server; Intel i5-11400F (6 cores/12
  threads), `/mnt/fast4tb` ext4 on `/dev/nvme0n1p1`.
- Remote Go-module revision lookup timed out; build uses the published exact Gomap commit through
  the documented sibling replacement. This is not an independently verified remote module fetch.

## What was built and checked

TreeDB has an explicit exclusive external-recovery owner, an immutable physical-image certificate
binding root/application metadata/file inventory/hashes, ordered file+directory publication, exact
restore and current/previous generation retention. A newer flushed working suffix is discarded on
recovery. Persistent value-log dependencies are included. Ordinary Open refuses an external owner.

Dgraph persists required Raft data and commit frontier before apply, serializes supported mutations
and Oracle Delta decisions, certifies only pending-free cuts, restores Oracle and
successful-proposal dedup state, and publishes dedup expiration before it can affect later replay.
Snapshot/retirement cannot exceed the certified index. Dirty-file tracking avoids rescanning all
retained Raft files per Ready. Mutation admission reserves bounded recovery-debt credits while
decisions retain progress.

Standalone TreeDB also now uses one captured-prefix path for singleton/group durability. Successors
can append/install while predecessor WAL fsync is blocked; exact-prefix ACK, pins, rotation, Close
and failure poisoning were checked. V2 external dependency sync ordering remains serialized and
needs a format change for full dependency-I/O overlap.

Focused public-path, crash-cut, persistent-pointer, corruption/identity, retention, dedup/expiry,
normal mutation/Delta replay and failure tests passed. Final focused race runs passed for TreeDB,
worker, posting and raftwal. Independent read-only review found no remaining material blocker for
the documented supported path. This is not full repository CI or production durability
certification.

## Full matched diagnostic

All six cells used the same final executable and fixed workload: 500 fixture nodes with scalar
values and circular UID edges, 100 sequential warmups, then 5,000 operations at concurrency 4: 60%
point reads, 20% one-hop reads, 20% unique-node inserts. Seed 20260711. Each arm used a new
directory, explicit filesystem survival for both Zero and Alpha, then SIGKILL of both servers and
verification after restart. Both TreeDB arms proved `cow_ordered` activation in runtime status.
Order was durable COW, candidate, Badger, Badger, candidate, durable COW. There was no selective
sample retry. Host load/memory were sampled once per second; no other task-owned heavy work ran.

| Attempt                       | Mode            | Ops/s | Write p50 / p95 / p99 ms | Recovery s | Interval allocated bytes/op |
| ----------------------------- | --------------- | ----: | -----------------------: | ---------: | --------------------------: |
| matched-v2-01-durable         | durable         | 230.2 |    26.00 / 40.05 / 48.15 |      10.21 |                      130856 |
| matched-v2-02-raft_checkpoint | raft_checkpoint | 420.8 |    15.61 / 20.57 / 24.52 |       5.84 |                      116852 |
| matched-v2-03-badger          | badger          | 315.8 |    18.81 / 27.17 / 32.07 |       5.74 |                       87776 |
| matched-v2-04-badger          | badger          | 315.8 |    18.80 / 26.64 / 31.83 |       5.74 |                       88013 |
| matched-v2-05-raft_checkpoint | raft_checkpoint | 421.9 |    15.62 / 20.59 / 51.93 |       5.84 |                      116953 |
| matched-v2-06-durable         | durable         | 238.0 |    25.57 / 37.84 / 43.55 |       9.69 |                      125818 |

Descriptive means: ordinary COW **234.1**, candidate **421.3**, Badger **315.8 ops/s**.
Candidate/ordinary COW is **1.80x**; candidate/Badger is **1.33x**. Two short samples per arm
provide viability evidence, not statistical qualification or a universal performance claim.

The write tail is not uniformly better: candidate p99 was **24.52 ms and 51.93 ms**, versus **32.07
ms and 31.83 ms** for Badger. The second candidate sample has a material tail regression. Its cause
was not isolated by this short capture; checkpoint-pause and other tail costs remain qualification
work even though aggregate throughput and p95 improved.

All six runs verified acknowledged values and fixture edges after SIGKILL. Candidate restart cuts:

| Attempt                       | Certified/recovered index | Replayed committed frontier before health | Image bytes | Verified nodes |
| ----------------------------- | ------------------------: | ----------------------------------------: | ----------: | -------------: |
| matched-v2-02-raft_checkpoint |                      3082 |                                      3873 |      274970 |           1521 |
| matched-v2-05-raft_checkpoint |                      3078 |                                      3881 |      273356 |           1521 |

The pre-kill certificate index/Oracle and actual restart certificate were cross-checked against
logs, with no later pre-kill certificate and replay before health. This closes the driver's
check-to-kill observation gap for these retained samples. An extra acknowledged and read-visible
insert was outside that unchanged image. Kill followed correctness validation: this does not test a
crash immediately at client ACK while its Delta is still pending.

The candidate recorded zero TreeDB command-WAL sync and zero public batch WriteSync calls. Raft and
checkpoint synchronization still occur; zero posting-WAL sync is not zero durability I/O. Full raw
counters, per-operation-type latency and Alpha MemStats intervals are in the JSON.

## Interpretation and remaining gaps

This is an architecture-policy comparison at the existing HTTP boundary. The candidate disables
rollup/vacuum/reclaim; ordinary COW keeps its normal maintenance. Badger uses syncwrites=true and
the existing filesystem-survival sync path. Thus the measured ratio cannot isolate WAL removal, the
COW index or equivalent steady-state maintenance cost. The client ACK is existing Alpha prewrite
plus Zero commit decision, without a new synchronous wait for Alpha Delta materialization. Per-write
latency is not local posting-store fsync latency. Reads exercise existing visibility waits.

Interval allocated bytes/op is `(Alpha MemStats.TotalAlloc_after - before) / 5000`; it includes
background/observer allocations and is not Go benchmark B/op. HeapAlloc after is live runtime
accounting, not peak RSS. Allocations remain higher than Badger in this fixture. No CPU attribution
or sustained memory/space equilibrium claim follows from these samples.

Mean interval allocation was 116,902 bytes/op for the candidate, 128,337 for ordinary COW and 87,895
for Badger: about 9% less than ordinary COW, but **33% more than Badger**. Ordinary COW recorded
2,418--2,428 posting-WAL file sync calls in the measured counter intervals, with 14.50--14.88
seconds of cumulative sync-call time; candidate recorded zero. Candidate public Checkpoint counted
three calls and 96.5--104.7 ms cumulative time. Those Checkpoint counters exclude parts of external
image copying, hashing and certificate publication and are not complete checkpoint pause timing.

Recovery seconds includes sequential Zero+Alpha startup/election/health and excludes the final
verification queries. OS cache is warm. SIGKILL is process-crash testing, not physical power loss.
The small fixture does not establish large-database checkpoint or recovery scaling.

The full production plan retains this authority model and resolves the remaining structural work:

1. Replace full-image copying/hashing with persistently retained exact COW roots and incremental
   resource-closure export/retirement, safe against GC, rewrite, reopen and snapshot transfer.
2. Complete pending-transaction/Oracle closure or prove pending-free admission and abort liveness,
   including bounded queues, in-flight decisions, metadata, retained history and recovery time.
3. Extend versioned semantic replay across schema/index/admin/restore/CDC and maintenance; then
   enable peer snapshot install and membership under the same certificate+suffix authority. Classify
   deterministic mutation rejection/conflict into an ordered replayable abort/no-op with certified
   retry disposition. The prototype currently terminates on every apply error, so a persisted
   rejection may recur during recovery; general mutation error availability is unclosed. Keep
   ambiguous partial storage failure fatal, and test abort/conflict/duplicate replay cuts.
4. Validate all file/directory/rename/certificate/Raft-retirement power-loss cuts and ambiguous
   completion, along with group/leader failure, conflicts, aborts and visibility behavior.
5. Add shared fresh-directory initialization ownership across ordinary/external open policies.
6. Establish sustained checkpoint/log/root/image retirement equilibrium and large-database latency,
   CPU, allocation, peak RSS, disk and recovery gates against comparable baselines.
7. Pass full exact-head CI/review and existing promotion criteria before integration. No issue is
   closed merely because the supported prototype works.

The current checkpoint copies the entire image while semantic apply pauses, then re-copies on
recovery. Defaults cap each image at 8 GiB / 8192 files; opaque metadata is capped at 1 MiB.
Mutation debt admission thresholds (4096 entries / 64 MiB) are not a total RSS cap or recovery SLA.
Disabled maintenance and unsupported operations are explicit prototype boundaries, not completed
production features. The design document gives the coherent final algorithm for these obligations.

## All attempts and reproducibility

All attempts below remain recorded, including setup, harness and product startup failures. Old
builds and the incomplete first matched campaign are not pooled into final matched statistics.

| Attempt                       | Success | Ops/s (if timing reached) | Error                                                                                                                                                  |
| ----------------------------- | ------- | ------------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| matched-01-durable            | True    |                     238.8 |                                                                                                                                                        |
| matched-02-raft_checkpoint    | False   |                      null | OSError(98, 'Address already in use')                                                                                                                  |
| matched-03-badger             | False   |                      null | OSError(98, 'Address already in use')                                                                                                                  |
| matched-04-badger             | False   |                      null | OSError(98, 'Address already in use')                                                                                                                  |
| matched-05-raft_checkpoint    | False   |                      null | OSError(98, 'Address already in use')                                                                                                                  |
| matched-06-durable            | False   |                      null | OSError(98, 'Address already in use')                                                                                                                  |
| matched-v2-01-durable         | True    |                     230.2 |                                                                                                                                                        |
| matched-v2-02-raft_checkpoint | True    |                     420.8 |                                                                                                                                                        |
| matched-v2-03-badger          | True    |                     315.8 |                                                                                                                                                        |
| matched-v2-04-badger          | True    |                     315.8 |                                                                                                                                                        |
| matched-v2-05-raft_checkpoint | True    |                     421.9 |                                                                                                                                                        |
| matched-v2-06-durable         | True    |                     238.0 |                                                                                                                                                        |
| server-control-badger-01      | True    |                     311.5 |                                                                                                                                                        |
| server-control-cow-01         | False   |                      null | RuntimeError("/mutate?commitNow=true: [{'message': 'Uid: [65536] cannot be greater than lease: [0]', 'extensions': {'code': 'ErrorInvalidRequest'}}]") |
| server-control-cow-02         | False   |                     234.5 | RuntimeError("one-hop posting mismatch: {'uid': '0x1', 'bench_value': 'base-60', 'bench_next': {'uid': '0x41', 'bench_value': 'base-61'}}")            |
| server-control-cow-03         | True    |                     235.0 |                                                                                                                                                        |
| server-smoke-01               | False   |                      null | No result.json; bootstrap/setup failed. See attempt logs and report.                                                                                   |
| server-smoke-02               | False   |                      null | RuntimeError('server exited 1; inspect server logs')                                                                                                   |
| server-smoke-03               | True    |                     418.0 |                                                                                                                                                        |

Early failures: Python 3.10 hashing compatibility; default-disabled CDC incorrectly rejected during
Alpha startup (fixed and tested); fixture UID lease initialization; scalar UID-edge decoding. The
first matched campaign completed its first durable cell but then had five port-preflight failures
after server shutdown, consistent with TIME_WAIT reuse. No listener remained. SO_REUSEADDR plus
fresh per-cell ports fixed the harness; the entire six-cell matrix was rerun as a new campaign.

Raw artifacts are retained locally at
`/mnt/fast4tb/gomap-raft-checkpoint-prototype-20261023/artifacts`, including source/build manifest,
commands, executable hashes, every result, raw runtime status, server logs, checksums, test logs,
host monitoring and execution ledgers. They are not claimed to be publicly uploaded. The normalized
results and complete attempt inventory are committed alongside this report.

Build and driver commands are in [README.md](README.md). To reproduce the final matrix, run modes
`durable raft_checkpoint badger badger raft_checkpoint durable` serially with new output
directories, the pinned sibling Gomap, GOMAXPROCS=2, and driver flags
`--ops 5000 --dataset 500 --concurrency 4`. Use fresh Zero offsets `20000+100*i` and Alpha offsets
`21000+100*i`, i=1..6. Preserve every result. This diagnostic does not replace the existing gRPC
livebench promotion workload.
