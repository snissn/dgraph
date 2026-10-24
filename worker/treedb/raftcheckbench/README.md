# Real-server Raft/checkpoint prototype diagnostic

`verify_recovery.py` is a separate correctness driver for the production integration. It submits a
mixed UID/scalar mutation on a fresh predicate and then valid CommitNow writes through a fresh exact
index. After each response it sends SIGKILL to Alpha and Zero before any read, drain, checkpoint
polling, or artifact write. Every restart checks acknowledged values, exact-index queries, and the
recorded pre-health replay frontier. It reports response-to-signal time but does not collect
throughput or power-loss evidence. Its small initial-root run has certificate index zero; the
semantic tests separately exercise a nonzero exact certified cut.

```sh
python3 worker/treedb/raftcheckbench/verify_recovery.py \
  --dgraph-bin /mnt/fast4tb/raft-production/bin/dgraph \
  --out /mnt/fast4tb/raft-production/immediate-ack-01 --ack-cuts 3
```

`run.py` launches one Zero and one Alpha, both with explicit filesystem survival, on new isolated
data directories. It exercises HTTP CommitNow, UID point reads and one-hop reads in a fixed 20/60/20
mix. It validates every acknowledged value and fixture edge, kills **both** servers with SIGKILL,
restarts from their files, and validates them again. The candidate additionally proves a newly
acknowledged suffix lies outside its unchanged checkpoint certificate before the kill.

The prototype `go.mod` pins Gomap `v0.6.2-0.20261023065356-2cb705090aa7` and uses
`replace ... => ../gomap`. Place a checkout of Gomap commit
`2cb705090aa77da14bb0cf4afc8e9b185115e8f8` in that sibling directory before building. This avoids a
task-specific absolute path; the exact commit is published on
`codex/5204-raft-checkpoint-prototype`. The validation manifest records the resolved replacement
path and actual SHA.

Example, from this Dgraph checkout with that sibling prototype dependency:

```sh
go build -o /mnt/fast4tb/raft-prototype/bin/dgraph ./dgraph
python3 worker/treedb/raftcheckbench/run.py \
  --dgraph-bin /mnt/fast4tb/raft-prototype/bin/dgraph \
  --out /mnt/fast4tb/raft-prototype/candidate-01 \
  --mode raft_checkpoint --ops 5000 --dataset 500 --concurrency 4
```

Use `--mode durable` for the ordinary COW TreeDB selector and `--mode badger` for Badger's durable
posting store. The current prototype server explicitly selects `cow_ordered` for both TreeDB arms;
the driver refuses a missing activation marker in `/debug/store`. All arms preserve the current
client ACK boundary. The candidate has a different recovery owner: durable Raft plus certified
images, rather than an independently durable posting WAL.

The output directory must be new. `result.json`, workload choices, expected data, raw `/debug/store`
and `/debug/vars` before/after/restart, commands and all server logs are retained. Every attempt,
including failure, belongs in the report. Host load is observed at the start/end; this is not a full
interference monitor or an accepted-repeat selector. Set GOMAXPROCS and source/toolchain identity
explicitly in the enclosing run manifest; the result records the exact executable hash.

This is a separate diagnostic schema. Its HTTP fixture, short timing window and SIGKILL recovery
proof do not replace the existing gRPC livebench promotion matrix, physical power-loss tests,
complete semantic replay, sustained resource equilibrium, or statistical qualification. Do not pool
its numbers with older process-survival or differently shaped workloads. A failure remains a failure
even when a later attempt passes.

The committed [prototype report](PROTOTYPE_REPORT.md) and
[normalized results](PROTOTYPE_RESULTS.json) record the complete comparison and failed attempts. The
measured driver was `9036af092`; its retained samples close the certificate check-to-kill
observation gap by independently matching the pre-kill certificate with actual recovery logs. The
current driver additionally reads CURRENT after both processes have stopped and rejects a changed
pointer. This assertion is after the timing window; the final report does not relabel the measured
driver source.
