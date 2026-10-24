# Final merged runtime: semantic acceptance aide

Source-only aide, no execution or new controls. Expected merged Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`, merged Gomap `f906b8a2aa1279ed6b6627028cba71d491db04c3`. The source reader here still has local Dgraph2d6bee; merged bad4 object is not present locally. Thus root must verify the four hashes below against the ACTUAL merged111 source before using this checklist. Root's authentic build/module receipts, not this note, bind final binaries. Local scripts were read only; no network fetch was performed.

## Required before advancing between steps

Each command needs its OWN new output directory (drivers refuse existing dirs), process exit0, `result.json.success == true`, `diagnostic_only == true`, and no `error`. Retain stdout/stderr, complete logs, command list, original result and any failure. Check each result's server binary SHA256 equals the SAME final merged build. Schema also binds companion SHA256 and source/buildinfo. These are process correctness diagnostics, not throughput, physical-power-loss or full product qualification. Do not interpret their `success` as parent34/5207 completion. Join/check all child PIDs and release all listening ports before the next driver: `stop()` can escalate to SIGKILL and most drivers do not independently certify graceful cleanup.

Order and existing command interfaces (use root-frozen absolute `$SOURCE/$BIN/$OUT/$GOMAP/$GO`; no offset/workload edits):

```sh
python3 "$SOURCE/worker/treedb/raftproduction/verify_schema.py" --dgraph-bin "$BIN/dgraph" --proof-bin "$BIN/schema-proof" --policy raft_checkpoint --source-dir "$SOURCE" --gomap-source-dir "$GOMAP" --go-bin "$GO" --out "$OUT/schema"
python3 "$SOURCE/worker/treedb/raftproduction/verify_history_floor.py" --dgraph-bin "$BIN/dgraph" --out "$OUT/floor"
python3 "$SOURCE/worker/treedb/raftproduction/verify_admin_lifecycle.py" --dgraph-bin "$BIN/dgraph" --out "$OUT/admin"
python3 "$SOURCE/worker/treedb/raftproduction/verify_native_peers.py" --dgraph-bin "$BIN/dgraph" --out "$OUT/peers"
```

## 1. Schema / public ACK / pending / suffix

Result: `schema/result.json`. Require `policy:raft_checkpoint`, equal `sources` and `sources_at_finish`, actual merged Dgraph source head/clean status and root-selected pristine/authenticated Gomap source identity. Retain `dgraph.buildinfo.txt`, `proof.buildinfo.txt`, `state.json`, every phase `.jsonl` and `.stderr`, `alpha-N.log`, `zero-N.log`, and `runtime-alpha-N.store.txt`. Source paths are NOT inferred from binary location; pass BOTH explicitly (default Gomap sibling path does not fit every native clone layout).

Exact ordered phases: seed, count-cut, count-check, conversion-cut, conversion-check, conversion-final, unique-prepare, unique-decide, final (9entries). Require each JSONL first event `success:true` and expected phase/event. Four ACK cuts: count-cut, conversion-cut, conversion-check, unique-decide. Retain four `cuts` with nonnegative `response_to_kill_ns`; these are measured intervals, no new numerical acceptance threshold. Cut-event and transcript are separate lines; preserve BOTH, do not read only the last line. unique-prepare must produce `alpha_restart`; require `pending_restart.alpha_exit==0` and its close log `certified: ... pending=2`. Zero stays alive across this Alpha-only restart; restarting Zero there would abort the old pending txns and invalidate the intended arbitration proof.

Every `runtime` entry carries exactly one resolved `x.WorkerConfig`/`HardSync:true` per current Zero/Alpha and retained raw log hash at health. Each raw store sidecar must show backend=treedb/profile=raft_checkpoint and `treedb.cache.memtable_mode:cow_ordered`; a zero legacy point_successor counter is valid. Every restarted Alpha recovery entry must satisfy `certified_index <= pre_startup_durable_commit <= replayed_through`, with one unambiguous pre-startup Found hardstate, and final `uncertified_suffix_replayed:true` requires AT LEAST one durableCommit>certified cut. Do not substitute a post-election commit target.

The companion asserts actual count/reverse toggles and recovery; retained UID history at SeedTs; conversion failure before unrelated schema/index effects; populated list conversion/re-key/collision/count/delete semantics; scalar→list and scalar lazy presentation; existing duplicate Unique-enable refusal, targeted duplicate refusal, actual shared Zero conflict keys, first pending commit/second abort after pending reopen, and final ACKed values after all-process SIGKILL. Retain records in transcripts, not just phase names. Root namespace/smallfixture only; delayed frontend admission race, cold splits and scratch/resource bounds remain focused-proof/other-gate claims. Do NOT run this full driver as a Badger parity oracle: its atomic invalid-conversion refusal is stronger than ordinary Badger's historical path.

## 2. Authenticated replicated history floor

Result: `floor/result.json`; server hash `binary_sha256`. Require nonzero `floor` and `recovered_index`, successful `floor_response`, and retained auth refusal responses `unauthenticated`/`wrong_token` containing AuthToken. `too_high` must contain preceding-assigned refusal; `stale_read` must contain replicated posting floor; `stale_floor` regression refusal; `idempotent` must succeed. The final indexed preserved-value query is asserted internally before success. Keep initial/restart Zero/Alpha logs and complete mutation/floor responses. The public floor ACK is followed by adjacent both-server SIGKILL before status polls/artifact writes.

Only `floor_retries` of the SAME explicitly chosen floor are allowed here, bounded30s while preceding ordered Delta becomes eligible; this is existing protocol waiting, not a capture retry or choosing a favorable new floor. This script proves S admission/recovery, NOT physical P advancement or sustained reclamation. Runtime resolved options/COW mode are not independently captured into result here: compare its binary/commands with the schema/runtime build witness and inspect resolved HardSync/config logs; do not invent a `/debug/store` sidecar that this driver never emits.

## 3. Admin / drops / Zero outage / close

Result: `admin/result.json`; server hash `binary_sha256`. Require `pending_drop_refusal` containing pending-transactions refusal, successful `drop_data`, `drop_predicate`, `drop_all_ack`, `zero_outage_read`, `orderly_close_exit_code:0`, and `restored_pending_decision` containing abort. Internal assertions cover unchanged reads after refused drop, removed data/schema, immediate DropAll ACK→all-process SIGKILL, preserved fixed-timestamp read with Zero absent, old pending abort authorized by restarted Zero, fresh writes/read after reopen.

Retain `alpha-after-drop.log`, `alpha-after-close.log`, all other role logs and actual responses. The driver COMMENT expects a canonical pending checkpoint during close but it does NOT parse the pending count: independently inspect `alpha-after-drop.log` successful close `certified: ... pending=1` before treating this as pending-certificate evidence (the earlier successful server campaign had this witness). Do not claim old pending COMMIT survives Zero restart; this proof intentionally expects abort. The fixed-ts outage read avoids leasing timestamps from unavailable Zero.

## 4. Native peer transfer / rejoin / all-process ACK crash

Result: `peers/result.json`; server hash `binary_sha256`. Require `pending_transaction`, `seed_floor`, `retained_decision`, `pending_commit`, `offline_floor`, `immediate_ack`; keys `native_2_join`, `native_3_join`, `native_2_rejoin`. Each native tuple is `[index,source,receiver,pending,checksum_sequence,terminal]` (strings). Join2/3 must include pending==1 AND checksum_sequence>0 AND terminal>0 in the SAME tuple. Check receivers2/3 and positive index; preserve source identity. Driver checks textual native-installed < committed-replay-barrier < Server-ready on each join/rejoin. Root's accepted final plan additionally requires newer rejoin: compare installed index in native_2_rejoin with native_2_join; script does NOT explicitly assert the numeric increase. Keep raw log order and actual tuple, not only presence of result keys.

Internal final reads verify all5values seed/resolved/pending/after-offline/immediate-ack on ALL3replicas after Zero+3Alpha kill/restart. Keep `alpha2-join.log`, `alpha3-join.log`, `alpha2-rejoin.log`, each restart log and complete responses. Peer Alpha index starts1..3; offsets are A+100,A+200,A+300, not A,A+100,A+200. Shutdown sends SIGINT concurrently before waiting; verify no orphan peers after success/failure. This is a genuine transfer/replay proof but not all native negative/error branches or power-loss certification.

## Endpoint / budget pitfalls retained

All drivers use same security token in their commands and X-Dgraph-AuthToken. `/admin` is authenticated GraphQL, `/alter` schema/admin, `/query` DQL, `/mutate` and `/commit` actual Zero arbitration. `healthy()` allows90s and checks process liveness; urllib request30s; schema first-event phase wait180s then transcript20s, Alpha pending close60s; most stop waits30s+10s kill; admin explicit close15s then SIGQUIT+15s diagnostic failure; native floor waiting45s. There is no common CLI whole-run budget. Use the root's predeclared outer controller deadline, allowing aggregated legitimate starts/phases/cleanup; do not impose a single RPC30s budget on an entire driver or relax existing limits after failure. All outputs fail closed; preserve failure and stop advancement, no selective retry.

Sequential port defaults and all92 prior checked derived ports are in `artifacts/final-native111-runtime-prereqs-01/PORTS.json`; availability was temporal, not reservation. Recheck at launch. Schema/resource defaults collide, and semantic drivers share some ports. No Docker/third-party Python packages are needed. Original process/physical/noise/throughput/retention/capability rules remain separate; the explicit M0 stability override is not a general numerical or correctness waiver.

## Source bindings read for this aide

- `verify_schema.py`: `d537f39d40c1b6752ce2d4e5d7c4da08af5d4cf899a1c38a0022d9853bf55b0b`
- `verify_history_floor.py`: `f82f33e656c53d4c4d6edbdb6181ebc2354559bd2b9f849275509896ebabd8f3`
- `verify_admin_lifecycle.py`: `74b6978a0160b83ac0de67aac193d35811d1676cb43c68acb1147b372ec09b51`
- `verify_native_peers.py`: `d819e56e3c77f7bcef05e8cb767b85bd4b11612cd23efcd29506866b58a33fa9`
