# Oracle connector wake after Alpha election

Dgraph issue #53 removes the avoidable election-to-retry-tick delay when an Alpha
becomes leader. The existing `node.Run` consumer of Raft `Ready.SoftState` sends a
nonblocking, coalesced hint to the existing Oracle connector. The channel is
initialized before the Raft node starts, so a wake preceding connector startup is
retained. Membership notifications keep their separate channel and consumer.

A hint grants no leadership or visibility: the connector rechecks `AmLeader`, and
Oracle deltas still pass through the existing proposal and replicated application
path. Election configuration, health/readiness, timestamps and watermarks are
unchanged. There is still only one synchronous connector. An active stream keeps
its existing loss/proposal guards; the wake does not add a second stream or abort
the active one. No channel is closed or replaced after startup.

The existing one-second timer remains responsible for retry and Zero-leader
checks. Explicit connection-error sleeps remain unchanged. A pending hint can be
consumed after a stream returns, but cannot accumulate more than one immediate
recheck; it is discarded if the Alpha is then a follower. Leadership regained
while connected can retain that hint for reconnect. Shutdown is checked again
before returning from the connection gate, including when a wake or timer is
also ready.

`worker/oracle_wakeup_test.go` holds the retry clock to distinguish event activation
from polling, and covers a wake before consumer startup, stale follower hints,
loss/regain, duplicate/full coalescing, concurrent signals, periodic retry and
shutdown. A real local gRPC fixture covers stream error, retry without another
wake, Zero-leader reconnect, duplicate signals without parallel streams and
cancellation. It sends no Oracle delta. Existing future-timestamp and sequenced
Oracle controls remain separate visibility checks.

These local controls establish scheduling activation and safety. Public restart,
foreground latency and throughput acceptance require a new exact-source matched
comparison. They do not revise the retained R12 failures or establish a p99 fix.
