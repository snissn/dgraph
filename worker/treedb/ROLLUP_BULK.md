# Bounded incremental-rollup writes

Incremental rollup keeps its original received key batches, priority selection,
hash throttle, per-key non-readonly timestamps, Oracle readiness waits and
`List.Rollup` versions. Only completed rollup output uses the optional native
multi-version write route. Foreground `TxnWriter` behavior is unchanged.

One producer holds at most 64 KV references in its output chunk. The 4 MiB bound
reserves space for the maximum 64 TreeDB group descriptors; payload charges key,
value and posting-envelope bytes. A single oversized entry follows the original
backend atomic-entry admission/error rule. A large split list is consumed in
order through successive chunks, retaining its original output ownership and
partial-write semantics. Completion is drained before another chunk is produced,
before a real Oracle readiness wait, at the received batch boundary, and at
producer shutdown. No list/root/lifecycle lock is held during the drain.

TreeDB admits each native part as one original FIFO request containing explicit
per-version groups. It uses the existing mutation pool, copied arena, root epoch,
request slots, two aggregate records, shared staging allowance and completion
owner. Cohort collection caps both requests and native groups at 65, in addition
to existing byte/work limits. Descriptor backing capacity is charged to the byte
allowance, cleared on release and retained only within the existing pool's
64-entry limit. Physical-key duplicates split before admission; distinct versions
of the same logical key survive. Work-limit shaping splits only an unsubmitted
suffix. Buffered entries retain the root epoch captured at logical creation and
are refused after a root install.

Badger uses its managed write batch, `SetEntryAt`, the existing metadata/discard
flags, one pending native transaction and `Flush`. `Cancel` drains unfinished
error paths. A native batch can already have committed a prefix when it fails;
neither backend promises cross-backend all-or-nothing behavior. Only the existing
TreeDB pre-storage validation subdivision can retry smaller groups. Storage or
ambiguous errors never cause scalar fallback or replay.

The commit-event decorator captures subscription state for each logical entry.
Uninstrumented entries can share native work. Before an instrumented entry it
drains native work and commits through the original decorated scalar transaction,
then waits for completion before another event admission. A one-slot event bus
therefore works without reserving slots that depend on an unsubmitted batch.
Joining after an entry's creation does not retroactively instrument that entry.
Unsupported stores likewise use scalar transactions, selected before submission.

## Activation and limits

Both backends expose process-cumulative `dgraph.rollup.*_total` diagnostics through
the existing `/debug/store` statistics map:

- `produced_kvs`, `produced_bytes`: output from successful `List.Rollup` preparation.
- `submitted_kvs`, `submitted_bytes`: operands passed to a storage attempt.
- `completed_kvs`, `completed_bytes`: operands whose original completion returned
  success; native errors leave the whole attempted part unconfirmed even if a
  prefix was stored.
- `unconfirmed_kvs`, `unconfirmed_bytes`: attempted operands returning an error.
- `native_chunks`, `native_completed_chunks`: attempted/completed native backend
  parts, not physical syncs or engine cohorts. Work shaping can split one producer
  chunk into multiple native parts.

Byte counters charge key + value + the five-byte posting envelope; they are not
heap capacities or physical WAL bytes. Counters are individually atomic, not a
coherent transaction or final-drain barrier. Existing TreeDB cohort/sync counters
retain their own broader scopes. The producer logs `Rollup accepted-work drain`
after its original shutdown Flush, including its error and counters; there is no
extra endpoint wait or flush. Unconsumed channel/pool keys remain unmeasured and
the counters cannot prove equal total maintenance merely from zero pending work.
No public latency, throughput, tail or capacity improvement is established by
these counters or by focused correctness tests.

`posting/rollup_bulk_test.go` covers the real producer/backend route, explicit
versions and duplicate cuts, split outputs, durable reopen, completion and byte
backpressure, errors without retry, event creation/failure/depth one, cancellation,
work shaping, FIFO foreground neighbours and Close. The existing real Raft
checkpoint-install test also rejects rollup output buffered before root replacement.
