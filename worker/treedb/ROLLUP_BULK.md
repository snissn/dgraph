# Bounded incremental-rollup writes

Only TreeDB's native incremental-rollup route changes batching. Badger and unsupported stores retain
the original asynchronous `TxnWriter`: per-key readiness, per-KV callbacks, and the original
process-end completion drain. Selection occurs once before constructing the native writer. The
scalar producer and per-key body retain their original concrete `TxnWriter` calls, callback
ownership and pool-return path; they do not run native readiness or produced-work accounting.
Foreground `TxnWriter` behavior is unchanged.

TreeDB uses the original received key batch, priorities, hash throttle, distinct per-key
non-readonly timestamps and Oracle waits. It first prepares at most 16 borrowed key/timestamp
descriptors, in order, without receiving ahead. No List or root lock, materialized output, event
reservation or storage request spans this readiness phase. It then materializes one key at a time
using the original `GetNoStore` and `List.Rollup(readTs)` included-version-plus-one rules. Root
epoch is captured before reading, not after output was computed. Pending root/part writes drain
before a read in that same physical-key family. Unrelated keys do not force this dependency cut. All
admitted output drains and its event outcomes are handed to the bus before the next readiness phase;
subscriber publication need not finish and the bus need not become empty.

One producer holds at most 64 KV references and their completion/event metadata. The 4-MiB chunk
allowance reserves maximum group, completion and event descriptor headroom; operands charge
key/value/envelope bytes plus independent event-owned key/value and mutation metadata where
instrumented. This is a bounded logical payload allowance, not an exact Go heap-capacity or
process-RSS claim. One oversized entry follows the existing atomic-entry admission/error rule. A
large single-key split output is consumed in order through successive bounded chunks; this does not
materialize all 16 keys at once. During submission the original Rollup KV payload remains live while
the native mutation arena copies its key/value data; instrumented entries also retain an independent
event copy. If A denotes original charged operand bytes and E denotes charged event bytes, the
producer cutoff bounds A+E plus reserved descriptors to 4 MiB, while submission can temporarily
retain 2A+E: below 8 MiB of these logical payload bytes for ordinary bounded chunks. This does not
bound allocation capacity, engine copies or total heap. Arena growth can retain unused capacity; the
existing pool retains only its established small-capacity backings. The current single-key
List.Rollup output can exceed one chunk and remains live while its successive chunks drain, and the
existing oversized atomic-entry exception remains. Completed event payloads can outlive the producer
chunk under the original independently bounded event-slot budget. No new pool or lifetime owner
combines these separate bounds.

Count, byte, duplicate key/version, read-dependency, root transition, event-pressure and
received-window boundaries can cut native work. No lock spans storage completion.

TreeDB admits each native part as one original FIFO request with explicit per-version groups,
reusing its mutation pool/arena, root epoch, slots, two aggregate records and original completion
owner. Shared staging counts group backing and the bounded completion-vector bytes. Cohorts still
cap both native groups and requests at65 and retain byte/work limits. Only an unsubmitted suffix can
split to meet work limits. Only wholly pre-storage validation refusals allow the existing subgroup
validation retry; storage/ambiguous failures never retry. A bounded vector preserves each
independently confirmed outcome in durable, FIFO, relaxed and external recovery modes. The producer
sees the first error; successful neighboring groups still complete successfully. Root installation
cannot bless old materialized data with the replacement root's epoch.

## Events and backpressure

The original global subscriber count is sampled at each logical entry's creation, before a drain can
allow a subscriber to join. Instrumented entries own independent mutation copies. A raw or matching
subscriber joining before publication still receives such an entry even if only a nonmatching
subscriber existed at creation. Entries created before the first subscriber remain uninstrumented.

The original bus owns one admission slot and sequence per event. A producer tries the next slot; on
pressure it submits/completes its existing native prefix before blocking for another. Depth1 can
legitimately produce native singletons; depth256 admits larger prefixes when available. No
aggregate-slot multiplier, new event queue, sequence owner or per-entry goroutine exists. Every
reserved event is completed exactly once with its own storage result, including refused and
unsubmitted error holes. Ambiguous storage outcomes publish no guessed success. Bus ordering and
slot release remain governed by its original run loop.

`SubscribeMatching` optionally stores an immutable, nonblocking key predicate on the existing
subscriber and filters only at publication. Raw `Subscribe` remains match-all. The Worker schema
subscription uses this capability but retains its own authoritative per-mutation KV conversion.
Registration acknowledgement, late joins, cancellation and store-before-bus shutdown stay intact.
Storage completion does not wait for subscriber delivery. Creation-time union-negative filtering is
deliberately absent because it could miss a later subscriber.

## Activation and limits

The existing `/debug/store` map exposes process-cumulative `dgraph.rollup.*_total` counters. Reads
are individually atomic, not a coherent conservation cut. The producer counters are TreeDB-specific;
Badger's preserved scalar route does not gain hot-path accounting or completion callback closures. A
Badger stats snapshot can still expose the detached map; its zero values are not measurements of
scalar maintenance work.

- `produced_kvs/bytes`: output of successful native per-key materialization; the preserved scalar
  producer is not instrumented.
- `submitted_kvs/bytes`, `completed_kvs/bytes`, `unconfirmed_kvs/bytes`: native producer operands
  and their per-logical outcomes; direct unsupported-writer fallback completion is not instrumented
  by these counters.
- `native_chunks`, `native_kvs`: accepted original native FIFO parts and their operand count. Work
  shaping can turn one producer chunk into multiple parts.
- `native_completed_chunks`: native parts whose entire original completion succeeded.
  `native_completed_kvs` counts independently successful operands, including successes from a
  partially failed validation subdivision. These counts are not physical syncs, engine cohorts or
  total rollup completion.
- `readiness_waits`: actual per-key Oracle waits on the native producer. `event_pressure_cuts` and
  `read_dependency_cuts`: attempts to cut pending native work at these producer boundaries; a
  pressure observation with an empty prefix does not imply a submitted chunk.

Byte counters charge key/value/envelope operands, not physical WAL or heap bytes. The native
producer shutdown log follows its original final Flush and retains its error. Native accepted work
is drained; unconsumed pool/channel keys and prepared-but-unmaterialized work after cancellation
remain outside that receipt. Counters cannot establish equivalent maintenance work or public
performance.

Owning controls separately cover the actual Worker subscription and a deterministically received
16-key batch with real delayed per-key readiness and multi-entry native activation, raw/filtered
late joins, depth-one saturation/cancellation, exact validation outcomes in all completion modes,
storage ambiguity without retry, physical sync before event publication, reopen, root replacement
after reading, discard floors, and split/repeated-key parity against the maintained asynchronous
writer. Public adoption still requires the unchanged workload/durability/latency and recovery gates.
