# Pipeline interpretation — draft, baseline pending

**Status: source-supported architectural interpretation only. Baseline filesystem arm and final filesystem report/bindings are pending. Whole-campaign STOP remains LOCKED by the original process arm; none of the observations below unlocks qualification.** Final numerical statements must be rebound to the original complete arm artifacts, not this coordinator-supplied preliminary summary.

## Observed distinction

Original process/store_wal arm has 12 complete retained rows and correctness parity PASS, but unchanged throughput rule FAIL: relaxed median TreeDB 9564.256400 versus Badger 9938.154220 (−3.762246%); durable 337.291768 versus 837.205864 (−59.712207%). All Zero/Alpha startup and restart witnesses resolve process survival, HardSync:false; TreeDB resolves actual cow_ordered.

Root reports newly completed filesystem decision rows, before the separate excluded profiles/final report: relaxed TreeDB median **334.916** versus Badger **273.270** (approximately +22.559%); durable TreeDB **429.084** versus Badger **270.190** (approximately +58.808%). These rounded preliminary within-arm medians would satisfy the >=0.97 ratio-of-medians rule. I have not independently audited the completed filesystem row packet or final maintained report yet; this is not its verdict.

The filesystem relaxed TreeDB cell remains **store_wal**, not raft_checkpoint. Only its durable TreeDB cell selects the new recovery owner. Both Zero and Alpha select filesystem survival in that arm; actual runtime sidecars remain the authority for resolved settings.

## What the implementation actually changes

1. **Legacy durable posting-store path:** `worker/server_state.go:175–187` selects CommandWALDurable and TreeDBCommitDurable; `posting/treedb_store.go:145–153,774–795` maps that to MVCC CommitDurable. Durable posting materialization therefore enters TreeDB's own durable batch/command-WAL path in addition to Dgraph's Raft persistence. Adapter grouping can combine concurrent materializations, but does not remove that separate posting-WAL authority. Process survival leaves Zero/Alpha HardSync false; it does not turn the durable posting-store policy into an unsynced policy.

2. **Raft-owned materialization path:** `worker/server_state.go:189–207` requires filesystem survival and opens ProfileNoWALFast/cow_ordered through the explicit external-recovery owner. `posting/treedb_external_recovery.go:20–28` uses MVCC CommitRelaxed for installing working materialization; `posting/treedb_store.go:784–788` performs the commit under owner.Apply. Gomap `TreeDB/external_recovery.go:293–306` gates lifecycle/poison state but does not turn every Apply into a durable checkpoint. Thus a transaction's posting mutations need not each append/sync TreeDB's independent redo WAL.

3. **Authority is retained upstream, not removed:** `worker/draft.go:1557–1579` persists Ready state/entries, then under HardSync synchronizes Raft storage before scheduling committed work; the checkpoint route additionally requires that barrier for committed entries. Zero's own Ready loop synchronizes when HardSync and MustSync (`dgraph/cmd/zero/raft.go:1057–1069`). Raft-owned mode requires the upstream durable input and preserves it until a certified cut authorizes retention advancement. Zero's transaction decisions and Alpha's ordered application/ACK path still incur work; there is no claim of zero fsync or one fsync per RPC.

4. **Durable cuts amortize materialization rather than requiring one cut per write:** `checkpointRaftCut` executes under the serial semantic apply barrier after posting writes drain/Oracle progress. Default triggers include 1024 applied entries, 16MiB replay debt, and explicit forced/control/floor transitions (`worker/raft_checkpoint.go:32–33,429–438`). Certificate metadata binds the exact index/term, ordered checksum/frontier, pending overlay, retry identities, S/P floor and ConfState. Gomap Checkpoint first checkpoints working data, captures an exact physical cut, then publishes its certificate/CURRENT (`TreeDB/external_recovery.go:324–351`). This still performs real physical/namespace durability work. It uses retained-root metadata rather than copying the entire database for each local certificate.

5. **Retention follows the durable certificate:** `worker/raft_checkpoint.go:486–513` completes TreeDB CheckpointRaft before AuthorizeCheckpoint, Raft snapshot/applied hints and certified-index/debt advancement. Uncertain apply/checkpoint errors fail the authority path rather than publishing a success frontier (`worker/draft.go:928–936`). Normal ACK need not wait for a certificate at that same index: durable retained Raft input covers the replay suffix. Periodic certificate lag and replay debt remain real, not lost work. The process comparison cannot validate this policy because it intentionally uses the older store_wal route.

## Counter evidence, without synthetic medians

The original process durable rows jointly recorded:

| Repeat | public WriteSync | command-WAL file syncs | grouped commits/participants | groups | group syncs | flushes |
|---|---:|---:|---:|---:|---:|---:|
| r1 | 1118 | 1085 | 38/38 | 5 | 4 | 38 |
| r2 | 1101 | 1063 | 46/46 | 8 | 7 | 46 |
| r3 | 997 | 910 | 101/101 | 14 | 15 | 101 |

All three process relaxed rows have WriteSync=0 and command-WAL file syncs=0, with public Write calls 959/926/1055. Full six-row counters and original hashes are in `process-evidence/PROCESS_ARM_AUDIT.md`. Point-append and value-log sync counters are zero in these six rows; that does not negate the witnessed cow_ordered route.

Root reports all three new Raft-owned durable TreeDB rows have command-WAL sync=0 and public durable-batch calls=0. This is consistent with the selected no-independent-posting-WAL path, not proof that Raft/Zero/checkpoint barriers vanished. Group metrics are **UNAVAILABLE**, with maintained reason:

> raft_checkpoint disables the posting-store command WAL; durability authority is Raft

Source: `livebench/result.go:27–29`, `livebench/cmd/main.go:1113–1128`. Unavailable grouping metrics must not be rendered as zero-group durability or compared numerically against legacy group counts.

These are native counter samples around the timed workload, not an ACK accounting ledger. Before samples precede exercise; after samples follow CPU/HWM/posting-disk collection (`livebench/cmd/main.go:192–278`). Group completion can cross that sample interval; r1/r3 group and group-sync counts already demonstrate why a conservation identity must not be inferred. They count posting operations, not client transactions. No per-row Zero+Alpha physical fsync trace or full RPC phase timestamps are provided.

## Permitted interpretation and limits

The source and path counters support the intended structural result: **filesystem/raft_checkpoint avoids per-materialization independent TreeDB command-WAL durability while retaining Raft replay authority and periodic exact certified data cuts.** This is an ownership/pipeline change, not a generic weakening of durable ACK into volatile success.

The rounded filesystem numbers do not isolate its causal speedup. Comparing process/store_wal with filesystem/raft_checkpoint changes both survival barriers and recovery owner; there is no original process baseline. Even the filesystem relaxed cells change survival barriers relative to the process arm. The lower filesystem Badger throughput is compatible with additional Raft synchronization, but is not proof that sync latency exclusively explains it.

The accepted original-source comparison is candidate filesystem versus baseline filesystem, same backend/class/repeat, retaining all three ratios and their median. That baseline is still pending. It must retain Badger control drift rather than divide it out. The selected end-to-end comparison changes many source paths along with recovery owner; it cannot isolate one subsystem without further separately authorized causal evidence.

Separate CPU profiles are excluded diagnostics. CPU sampling describes on-CPU execution; it does not measure elapsed fsync wait or prove exclusive wall-time attribution. Kernel/blocking work, storage latency, scheduling, batching, Oracle decisions, cache/maintenance and Raft/Zero work remain possible contributors. No profile result changes a retained numeric failure.

All original ancillary >5% review gates and unavailable-metric rules remain. These short rows do not establish default physical equilibrium, bounded whole-system RSS, whole Dgraph power-loss durability or full guarded-parent capability completion. Physical-host quiet waiver is not a numeric waiver. The original process STOP is retained even if later filesystem arms satisfy their local parity rules.

## Source/evidence bindings used for this draft

Local Dgraph source checkout 2d6bee is the reviewed PR43 head included in executed merge bad4; Gomap checkout 7fd includes executed published W8. Relevant bytes were read, not rebuilt:

| File | SHA256 |
|---|---|
| Dgraph worker/server_state.go | af8842564541c13082c26005d833f2649507e674fe9ff652b0b261a39d79a339 |
| Dgraph worker/draft.go | d3f59105daf6daab5319316807cd82298bd20836123b87c59766acfe78f2a9f8 |
| Dgraph worker/raft_checkpoint.go | fe6299f7e58c654617173bfbb3ca6f64bfa11668668a5b83f14ad7e2d36f378b |
| Dgraph posting/treedb_external_recovery.go | 64fca16aee4b6316b1b23259076130af899e9f801900eeffdd27bbd94076cf56 |
| Dgraph posting/treedb_store.go | e4baa1db3cebed0d35a7f72ec836a8c0a9ee41fb7228a2c27b289a0fbc991d56 |
| Dgraph livebench/cmd/main.go | 1ab78fba06505a6d984c108bab2d808386895472e0e05c204a9b5e4f084d0459 |
| Gomap TreeDB/external_recovery.go | 23b7f71c55c8c779fa9ccbec0f8242e7f476375a326e2fc26b4bc51b4fd536e7 |

Original process audit SHA256 `d4a9d0dd5e8db024e99e8d04dae206cfe27195b2ab0414e38c1d5c45e82d16da`; frozen comparison mapping `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`. Preliminary filesystem medians/counter summary are coordinator-reported and await exact final artifact binding. No source/state changes, analysis invocation, workload, SSH, GitHub mutation or additional heavy job was performed.

