# Independent history-floor artifact audit

**ACCEPT for the bounded explicit semantic-floor ACK/crash/recovery diagnostic.** No concrete failure found. This is not physical reclamation, performance, multi-replica, or power-loss acceptance. `result.json` truthfully records `diagnostic_only=true` and `success=true`.

## Checks against retained evidence

| Obligation | Actual evidence | Verdict |
|---|---|---|
| Authentication | Both absent-token and wrong-token responses are `ErrorUnauthorized` / `Invalid X-Dgraph-AuthToken`. | PASS |
| Explicit, eligible ordered floor | Committed mutation StartTs5/CommitTs6. UINT64_MAX request rejected with `must precede assigned frontier and every pending start`. Same operator-selected S5 was refused five times before its ordered eligibility, then accepted; the driver never substitutes a later clock/frontier-derived floor. | PASS |
| Certificate before successful floor ACK | `alpha-initial.log:107`: index20/oracle6/pending0/semantic_floor5/physical_floor0 at17:13:15.228047. Successful response tracing ends17:13:15.228089. This is the final pre-kill certificate; earlier certificate was index1/S0. | PASS |
| Immediate ACK-boundary crash | Driver lines115–126 receives the successful floor response, then immediately calls Alpha SIGKILL followed by Zero SIGKILL, with no intervening filesystem/network operation, and waits both. Initial logs end without graceful shutdown; both restarted logs are retained. This is sequential process termination, not simultaneous power loss. | PASS at driver/retained-log scope |
| Exact certified recovery | `alpha-restart.log:45` selects certified20/oracle6, matching the pre-kill S5 certificate, not a later certificate. Line71 records committed replay through22 before health. | PASS |
| Stale request semantics after restart | Read at timestamp5 returns `ErrorInvalidRequest` with `read timestamp 5 is at or below replicated posting floor 5`; requested floor4 is rejected as regression (ordered proposal index25). | PASS |
| Idempotent and admissible behavior | Same floor5 succeeds after restart. The driver then asserts the fresh indexed query returns exactly UID0x1 / `floor_value=preserved` before setting success. This final query response is not separately serialized; its assertion is source-bound, not an independently retained response transcript. | PASS with stated transcript limit |

The five S5 eligibility refusals are retained functional polling while the earlier CommitNow decision reaches the ordered Alpha frontier, not discarded failed performance rows. They do not demonstrate pending-transaction boundary exclusion by themselves: the successful certificate has pending0. The over-high refusal demonstrates the eligibility guard; exact pending-boundary behavior remains separate source/test evidence.

## Executed source and resolved runtime

- All four logs' startup banners identify Dgraph SHA256 `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175`, exactly the result binary hash and accepted merged-consumer smoke manifest. Go1.26.9/GOMAXPROCS2 is logged. Linker Commit SHA fields are blank, so they are not used as source authority.
- The separately retained `smoke-evidence/executed-source-manifest.json` binds this same binary to merged Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`, official Gomap `v0.6.2-0.20261023204715-29a2fdf153ce`, no replacement. The floor directory alone is not a fresh module-authentication receipt.
- Both Zero and Alpha initial/restart resolved `x.WorkerConfig` show **HardSync:true** (Zero line22; Alpha line22). Alpha initial/restart line23 resolves backend`treedb`, tier`benchmark_minimal`, durability`raft_checkpoint`, events`true`; startup and recovery logs independently activate that route. Thus survival/backend are observed, not inferred solely from argv.
- No floor-specific `/debug/store` response or resolved `memtable` sidecar is retained. `cow_ordered` is source-supported by the exact route construction (`worker/server_state.go:197`) and same-binary smoke witnesses; it is **not an independently sampled floor-process memtable witness**. Do not claim otherwise.

## Cleanup and log classification

The restarted Alpha receives SIGINT at17:13:25.798360, stops HTTP/gRPC/Raft, forces closing certificate index27/oracle10004/S5/P0, disposes state, and logs `Server shutdown. Bye!` at17:13:25.880625. Restarted Zero receives interrupt and logs `Raft WAL closed with err: <nil>` at17:13:25.914803. This closing index27 certificate occurs **after** all recovery assertions and cannot explain the index20 restart selection.

Driver `finally` stops Alpha then Zero, waits30s each with bounded kill+wait fallback, closes logs, and writes the result. No fatal/panic is present. Startup group-discovery/subscription and shutdown canceled-listener messages are contextual expected transitions, not silently ignored test failures. The archive receipt records61440bytes/SHA256 `2fd548fd59919af4fa4c1da8e204071a6c2a53e3ec7e5c87698084b6a61d69bf` (local archive bytes independently match the receipt; no remote process inspection performed here). Local logs do not independently prove all remote processes/ports absent; that requires the root launcher/process receipt.

## Limits

S5 survives the immediate both-server process crash and protects request admission; P remains0 at both certification points. This does **not** prove baseline materialization/pruning, terminal/dedup row deletion volume, retained-generation reclamation, long-pending liveness, continuous equilibrium, arbitrary S selection, all auth/namespace roles, multi-peer consensus, disk-loss behavior, or the full Dgraph power-loss authority chain. Data correctness is a single indexed string with post-restart stale and fresh queries. The existing exact-root/maintenance and modeled stable-cut controls remain separate.

## Immutable inputs

- `result.json`: `645eeeff8d287e120b9d47eaf5ce384991f7724ce27a6914b1fd4ffb44de3e6a`.
- `alpha-initial.log`: `ab77aa2d234e08f7c15cb3bfce3bc12ebb04657bc3faa49bc164d86143ba2f65`.
- `alpha-restart.log`: `a174904add2d27accf783590a3c3e1aa5ad89dd592d26a100f7cdfa796bce6b7`.
- `zero-initial.log`: `115291dc67091a75d4fe9425485df418cb1b1cfbb0d9f536fd0051ce0a2d3488`.
- `zero-restart.log`: `d7fcb1fcd00f4a95d275bef11c593ff050664e25de99e9c24dbb9788bd006f05`.
- Driver `worker/treedb/raftproduction/verify_history_floor.py`: `f82f33e656c53d4c4d6edbdb6181ebc2354559bd2b9f849275509896ebabd8f3` (inspected lines69–156; not executed by reviewer).

Only this audit artifact was written. No source/state change, SSH, runtime, test, or GitHub operation was performed.
