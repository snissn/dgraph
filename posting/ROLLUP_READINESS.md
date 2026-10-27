# TreeDB rollup readiness and scalar submission

Ordinary durable TreeDB rollups prepare the existing received window before producing writes. Each
selected key keeps its original distinct non-readonly timestamp and complete Oracle readiness wait.
The window contains at most 16 key/timestamp descriptors. During those waits it holds no
current-window posting List, payload, transaction, event reservation, storage admission or root
lock. It does not receive a second window ahead.

After readiness, keys materialize in their original order through `GetNoStore`,
`List.Rollup(readTs)`, `RemoveCacheFor` and the original asynchronous `TxnWriter.Write`. Each output
retains the maximum included version plus one. Each KV still creates its own ordinary transaction
and callback. There is no per-key completion drain, native multiversion batch, shared timestamp,
event filter or new queue. The existing TreeDB cohort owner decides grouping, pressure cuts, durable
completion and backpressure. The producer keeps its original final `Flush`.

Selection occurs once at producer startup. Only a concrete durable, staged TreeDB store, including
its existing commit-event decorators, uses readiness preparation. Badger, unknown decorators,
relaxed TreeDB, unstaged TreeDB and explicit external-checkpoint stores keep the original concrete
producer and per-key body. Subscriber presence does not select a path. The event bus retains
transaction creation eligibility, per-entry capacity and sequence admission, ordered completion and
publication to subscribers present at publication.

This changes maintenance scheduling: later keys obtain their read timestamps before earlier keys
materialize. The hash throttle still chooses keys in the original order using the received window's
single time observation. Repeated keys remain throttled. Materialization remains ordered and uses
fresh original read/transaction paths after readiness; it does not carry a root epoch from before a
root swap or bless an old List with a new epoch. Existing store floor, epoch, namespace, Close and
admission checks remain authoritative. External-checkpoint/root-install routes are excluded.

Cancellation abandons the current unsubmitted descriptors, returns the received batch to its
existing pool and drains previously accepted writes through the original final cleanup. It does not
materialize a ready prefix after cancellation. Ordinary per-key materialization/write errors retain
the original log-and-continue behavior and callback errors retain the original writer drain
behavior. Ambiguous writes are never retried. This is lossy background maintenance; acknowledged
foreground data is not abandoned.

The additional fixed descriptor array contains 16 slice headers and timestamps (512 bytes on a
64-bit platform); it borrows the received batch's existing key bytes. No posting or event payload is
copied in the readiness phase. The existing throttle map, received batch pool, per-key rollup
payloads, scalar mutation/event copies and commit queue retain their existing ownership and bounds.
The 16-descriptor bound is not a total heap or posting-size bound. The existing per-key rollup can
still produce multiple KVs, and existing oversized-entry rejection/backpressure applies. No new
payload pool or lifetime owner is introduced.

`rollup_readiness_test.go` holds distinct Oracle waits and a real physical WAL sync to prove zero
current-window admission during readiness followed by original scalar requests in the real cohort
owner. Other controls cover actual fallback producers, cancellation, saturated depth-one events,
late joins, split/repeated/deleted lists, retained history, floor refusal and Close while
descriptors are held. Existing event, root-transfer, ownership, storage-error and admission tests
cover the unchanged owners. The deterministic received-window fixture does not assume the public
lossy `sync.Pool` assembler retains 16 keys under the race detector.

These controls establish construction and activation, not a public latency improvement. Public
throughput, latency, CPU, recovery, space and useful-maintenance-progress comparisons remain
required; a larger possible ready reservoir alone does not establish a speedup.
