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

`version=1` JSON records follow the `DELTA_INQUIRY` marker and following space. Every new record has
a random per-observer process `session` identity. A per-node mutex assigns contiguous sequence
numbers, wall timestamps and monotonic `elapsed_ns` at logging; sequence is authoritative for
serialized emission order. Upstream capture order can differ, as described below. The enabled record
retains available embedded build/toolchain/module settings; external clean source and executable
hashes remain required when VCS metadata is absent. `index` and `key` identify the Raft proposal;
`request` is local to that Delta's writer. `bytes` is encoded Raft entry size; `payload_bytes`
counts successful SetEntry key/value or Delete key lengths, excluding metadata/envelopes. Counts are
adapter input, not retained heap or physical work. `start_ts`, `commit_ts` and raw Delta decisions
retain their original timestamps.

The event seams are the existing production owners:

- `enqueue_begin/end`: offer before the channel send and return after it. Begin alone proves no
  queue admission. Apply may begin before enqueue_end; that ambiguity is retained.
- `apply_begin`, `delta_begin`: actual serial application. The profile label is a candidate only for
  configured ordinary durable TreeDB with actual `wal_on_sync`/durable status; other modes remain
  instrumented but excluded. Proposal duplicate skips produce no delta_begin.
- `materialize_begin/end`: original txn.Update and CommitToDisk, including legacy retry behavior.
- `submit_begin/return`, `callback`: actual CommitAt request, immediate return and callback entry.
  Callback can occur before return. No new callback, retry or storage request is introduced.
- `flush_begin/end`, `window_seal`: original TxnWriter Flush and forwarded scheduling fence; sealing
  is no durability certificate. A callback record precedes forwarding the original callback.
- optional `hard_sync_begin/end`, cache invalidation, `oracle_begin/end`, `proposal_done_begin`,
  `applied_done`: existing publication/completion boundaries. Applied is an attempted-apply marker;
  ordinary existing error paths may advance it without proving durability. Oracle end records actual
  MaxAssigned after ProcessDelta. No error semantics are changed.
- `apply_closed`: normal channel-close endpoint. Abrupt or partial logs remain evidence but are
  refused for complete-frequency inference.

Run offline:

```sh
python3 worker/treedb/delta_inquiry/analyze.py alpha.log > inquiry.json
python3 -m unittest discover -s worker/treedb/delta_inquiry -v
```

The analyzer reports adjacent pairs in offered Raft order. It excludes interposed non-Deltas, Raft
index gaps (including controls filtered before applyCh), unsupported profiles, duplicate/invalid
StartTs, aborts, watermark-only/no-storage work, errors, unbalanced request/return/callback records,
incomplete application and send-overlap uncertainty. A pair requires successor enqueue_end before
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

## Zero receive and original aggregate observations

The same default-off flag also observes the existing Zero receiver. It retains one channel with
capacity 100, the same select/SLURP/merge/sort, and the same serial `proposeAndWait`. A tiny channel
envelope carries message IDs alongside the original pointer; it does not change Delta ownership or
introduce another queue, timer, receiver, scheduling owner or storage submission.

Each connected stream has a process-local `stream` epoch and each successful `Recv` has a
stream-local monotonic `message` ID. `oracle_received` captures `time.Now()` immediately after
`Recv` returns, before descriptor construction or the shared logging mutex. While the receiver still
exclusively owns the Delta, it synchronously serializes original decisions, encoded OracleDelta
bytes, MaxAssigned, ChecksumSequence, a deterministic checksum-map digest and whether unknown
protobuf fields exist. No posting keys/values or checksum map contents are recorded. After sending,
the receiver logs IDs only, because the consumer may already be merging/sorting the transferred
Delta.

`oracle_send_begin/end/canceled` distinguish an offer from a successful return from the original
channel send. Send return can occur after consumption; it is an observation upper bound on
successful handoff, not exact queue occupancy. `oracle_consumed` maps each consumed message to the
original aggregate. The aggregate records its descriptor after the existing sort/MaxAssigned update,
or records its existing watermark-retention skip or merge failure. `oracle_propose_begin/end`
bracket each existing outer `proposeAndWait` attempt; `oracle_proposal_encoded` binds it to its
actual encoded proposal key and bytes (including the 8-byte key). Existing internal retries retain
that key and encoded bytes. Failures before encoding (including an already canceled context) retain
an explicit `encoded=false` report outcome without inventing a proposal key. Return errors retain
their original outcome; return alone does not prove commitment, apply or durability. No
committed-entry signal is invented.

`sequence` and monotonic `elapsed_ns` describe serialized emission order. `captured_ns` describes
the observation before logging, relative to the same process monotonic origin. Capture order can
differ from emission order across goroutines. Use IDs and capture times for upstream intervals,
never emission sequence alone; neither timestamp establishes when Zero sent the decision.
Synchronous logging can block the receiver and radically change batching and backpressure. Receipt,
send-return and consumption are separate observations.

Decision lists larger than 32 use a header with `txn_count`, followed by bounded `decision_part`
records carrying `parent_sequence`, zero-based `ordinal`, original StartTs/CommitTs, and
nil/unknown-field flags. This also bounds the existing Raft entry decision descriptors.
Reconstruction requires every ordinal exactly once; truncated glog records, absent children, mixed
processes/restarts or missing endpoints fail closed. The split bounds individual decision records,
not total logging overhead or process memory. Error strings and unrelated raw glog lines still
require complete retained capture and normal loss checks.

```sh
python3 worker/treedb/delta_inquiry/upstream.py alpha.log > upstream.json
```

The upstream analyzer requires at least one connected stream, startup, contiguous emission sequence,
normal apply close, Oracle owner close and both receiver/consumer close for every stream. Those
independent owners may close in either order. Cancellation can leave received/admitted messages
unconsumed or a partial aggregate discarded under the original behavior; these are reported
explicitly, never replaced by zero arrivals. An abrupt process death or lost record remains retained
evidence without complete frequency inference. Preserve raw logs from one process including all
files and stderr duplicates; new captures validate session identity as well as sequence;
mixed/duplicated records are rejected, not deduplicated. Legacy #57 traces without session identity
retain the external single-process prerequisite and are unsupported by the upstream analyzer.

For each original proposal wait, the report separately lists prior received and confirmed-admitted
backlog still unconsumed at wait begin, and messages observed received and messages whose send
returned during that interval, separately, with conservative decision classifications. It reports
original constituent messages, proposal identity, skipped/failed/unfinished aggregates and
unconsumed messages. These counts describe this perturbed run only. They cannot determine how B/C
would have been split into distinct original aggregates under a different three-inflight admission
policy, whether corresponding prewrites are available/independent, whether native cohorts overlap,
how many physical syncs occur, or any performance benefit. No received decision is promoted to
semantic eligibility. Existing #34 ownership, replay, native group and uninstrumented qualification
gates remain.

The offline upstream analyzer uses capture-time indexes for arrival intervals and an endpoint sweep
for backlog at each original wait start. This avoids scanning all messages for every proposal.
Results preserve original emission/insertion order even when captures precede earlier emitted
records. Equal interval boundaries remain excluded from "during" arrivals; backlog at begin includes
receipt/handoff at begin and excludes consumption at begin. The sweep supports repeated starts and
resets for synthetic reversed starts. The whole trace and result remain in memory; this is an
offline diagnostic, not a streaming parser or a bounded-memory server path. Analyzer-only changes
can be source-bound separately while retaining the frozen observer executable.

Upstream phase classification is explicit. The pre-existing downstream `oracle_begin`/`oracle_end`
publication records have no stream identity and remain covered by complete-trace sequence/session
checks, but do not belong to the Zero receiver. Unknown `oracle_*` phases and genuine upstream
records with missing or unknown stream epochs still fail closed. A preserved first analysis of a
real mixed trace exposed the earlier prefix-classification bug; repairing the offline parser does
not alter that raw capture or the frozen executable.

If no Oracle stream ever connects, owner shutdown alone is not zero-arrival evidence; the upstream
analyzer rejects the trace without frequency inference. Preserve the connection failures and raw
logs as unavailable-observation evidence.
