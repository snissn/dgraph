# Serial Delta opportunity inquiry

Set `DGRAPH_DELTA_INQUIRY=1` in **Alpha's** environment before startup. The default is disabled.
Retain the complete `DELTA_INQUIRY` glog records from one process, including startup and graceful
apply-channel close. Do not combine restarts or nodes. The records contain proposal identifiers and
transaction timestamps, but no posting keys or values. Logging is synchronous, may produce large
records for large Deltas, and changes scheduling: these runs are diagnostic and excluded from
performance acceptance. No benchmark host or new campaign is authorized by this tool.

Bind each capture to the clean Dgraph SHA, resolved GoMap module, Go toolchain, build command,
executable hash, unchanged workload/argv/environment, filesystem and backend/durability startup
status. Preserve every attempt, raw log and `/debug/store` observation already collected by the
harness. Do not add status requests inside the measured interval for this inquiry. The records do
not replace source/binary binding or native physical-sync counters.

`version=1` JSON records follow the `DELTA_INQUIRY ` marker. A per-node mutex assigns contiguous
sequence numbers, wall timestamps and monotonic `elapsed_ns` at logging; sequence is authoritative
for observed ordering. The enabled record retains available embedded build/toolchain/module settings;
external clean source and executable hashes remain required when VCS metadata is absent.
`index` and `key` identify the Raft proposal; `request` is local to that Delta's writer.
`bytes` is encoded Raft entry size; `payload_bytes` counts successful SetEntry key/value or Delete
key lengths, excluding metadata/envelopes. Counts are adapter input, not retained heap or physical
work. `start_ts`, `commit_ts` and raw Delta decisions retain their original timestamps.

The event seams are the existing production owners:

- `enqueue_begin/end`: offer before the channel send and return after it. Begin alone proves no
  queue admission. Apply may begin before enqueue_end; that ambiguity is retained.
- `apply_begin`, `delta_begin`: actual serial application. The profile label is a candidate only
  for configured ordinary durable TreeDB with actual `wal_on_sync`/durable status; other modes
  remain instrumented but excluded. Proposal duplicate skips produce no delta_begin.
- `materialize_begin/end`: original txn.Update and CommitToDisk, including legacy retry behavior.
- `submit_begin/return`, `callback`: actual CommitAt request, immediate return and callback entry.
  Callback can occur before return. No new callback, retry or storage request is introduced.
- `flush_begin/end`, `window_seal`: original TxnWriter Flush and forwarded scheduling fence;
  sealing is no durability certificate. A callback record precedes forwarding the original callback.
- optional `hard_sync_begin/end`, cache invalidation, `oracle_begin/end`, `proposal_done_begin`,
  `applied_done`: existing publication/completion boundaries. Applied is an attempted-apply marker;
  ordinary existing error paths may advance it without proving durability. Oracle end records
  actual MaxAssigned after ProcessDelta. No error semantics are changed.
- `apply_closed`: normal channel-close endpoint. Abrupt or partial logs remain evidence but are
  refused for complete-frequency inference.

Run offline:

```sh
python3 worker/treedb/delta_inquiry/analyze.py alpha.log > inquiry.json
python3 -m unittest discover -s worker/treedb/delta_inquiry -v
```

The analyzer reports adjacent pairs in offered Raft order. It excludes interposed non-Deltas,
Raft index gaps (including controls filtered before applyCh), unsupported profiles, duplicate/invalid
StartTs, aborts, watermark-only/no-storage work, errors,
unbalanced request/return/callback records, incomplete application and send-overlap uncertainty. A pair requires successor enqueue_end before
predecessor flush_end and actual storage-bearing submissions in both Deltas. It is an **upper bound
on candidates**, not proof that preparing the successor is safe: cache ownership, retries, checksum,
transaction independence, barriers, retained input and recovery must be proved separately. Pairs may
overlap; do not add their bytes as independent cohorts. Missing/mixed sequences are rejected.

No lookahead is consumed, no txn.Update moved, no future timestamp published early, and no new
queue, timer, goroutine, wait, scan, checkpoint or native sync is added. This packet does not map
adapter requests to individual native cohort/command/barrier identities. Existing adapter/engine
counters remain separate, non-atomic attribution. A subsequent implementation requires a reviewed
ownership proof and unchanged correctness/recovery/performance gates under Dgraph #34. Badger
remains the default; historical STOP evidence and broader obligations remain open.
