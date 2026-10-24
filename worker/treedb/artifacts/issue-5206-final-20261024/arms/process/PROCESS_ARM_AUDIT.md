# Independent process-arm audit

Verdict: **ACCEPT the retained arm evidence and original STOP classification; performance qualification remains NULL and the whole campaign remains STOP LOCKED.** This is an offline audit, not a new run or a qualification waiver.

## Evidence integrity and identity

Independently hashed all **161 selected files / 8,619,430 bytes** against `process-12-SELECTED_FILES.json`: zero missing or mismatched files. Its SHA256 is `21b56b42aed8693ffbc703dd5eb50a34f621b01e89f990bbf98c4d6d6b0cf35b`; invocation SHA256 `c0b1263174ba8dfdb80f42d3eb19aeb114b74aa05c4853b86e271cb86da26fe9`. Invocation-bound manifest/report hashes match the retained bytes:

- Manifest v2: `008eda694c391c88f23af0cad4b06ac145668a38eec7affc27378ea0813c518c`.
- Original report: `1d2899e0ddd88343570130a4ccb49062807366035a90f9bdeb752558e69295ba`.
- All 12 rows bind server `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175`, clean merged Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`, Go 1.26.9 and authentic published Gomap `v0.6.2-0.20261023204715-29a2fdf153ce`, sum `h1:Y85xjKKdrqj5UXLCzYCuVzYXbk0oDGf8zVBwqowuxrs=`, with no replacement.
- Retained resource-witness verification output is structurally identical to the v2 manifest. It records Dgraph tree `6260025d2c1f789529dfa9f9ba6b0aa533d2e245` and Gomap tree `a9e0f4b3fdabb9dd343e1f6757a704c61f751c3d`; all row build-info strings match that manifest, `vcs.modified=false` and row `dirty=false`.
- The copied packet contains the verification receipt, not the executable itself. I verified the consistent recorded hash/manifest/build-info chain; I did not independently rehash the remote server or reread remote source.

## Completeness, settings and correctness

Exactly 12 unique decision rows exist: three repeats 1–3 for each backend × durability class, none excluded or marked contaminated. The original script orders relaxed Badger, relaxed TreeDB, durable Badger, durable TreeDB, each r1–r3; no randomization or pairing redesign was applied.

Every row uses 500 nodes, 100 warmup operations, 2,000 timed operations, concurrency 4, seed 20260711, single Zero/single Alpha, 60% point reads / 20% one-hop reads / 20% writes. Recomputed the maintained config fingerprint independently for all rows: `d0772456e60ba13fd017bb5db10bdf397fdf87f87fab743555a5d3835fc70ad7`. Route-specific policy fields are deliberately omitted by that maintained fingerprint, so they were checked separately.

All 48 runtime witnesses (Zero/Alpha at startup and after Alpha restart) and their hashed sidecars agree on **process survival / HardSync:false**. Both TreeDB phases in all six TreeDB rows resolve actual `cow_ordered`; durable resolves `wal_on_sync`, relaxed `wal_on_relaxed_sync`, and the requested policy is `store_wal`. Badger resolves syncwrites=true for durable and false for relaxed. This is the legacy store-WAL control, not the filesystem/raft-checkpoint candidate path.

All 12 rows report schema/restart/unsupported checks true, 920 final nodes and identical posting checksum `2e08caad769d3f0f221ddeaaa36e528ae1b4764fefdc27df3913a788474074d8`. Restart is a clean Alpha restart with Zero alive; it does not establish abrupt-crash, ACK-crash suffix or power-loss recovery.

Two separate profile runs exist (relaxed 100,000 operations; durable 10,000), after the decision rows, with their pprof/top artifacts. They are not among the 12 report inputs and are not pooled into medians.

## Original throughput decision, independently recomputed

All raw ops/s, without discarded rows:

| Class/backend | r1 | r2 | r3 | Median |
|---|---:|---:|---:|---:|
| relaxed Badger | 9911.113222 | 9938.154220 | 10238.237228 | 9938.154220 |
| relaxed TreeDB | 9564.256400 | 9512.164370 | 9590.185757 | 9564.256400 |
| durable Badger | 812.960894 | 892.647003 | 837.205864 | 837.205864 |
| durable TreeDB | 331.575582 | 337.291768 | 437.567545 | 337.291768 |

Median TreeDB/Badger minus one is **−3.762246110% relaxed** and **−59.712206730% durable**. Both fail the unchanged >=97% parity rule. Original `report.md` STOP is correct even though the shell/report invocation exits 0. No process-arm original-baseline anchor exists; these are within-arm backend comparisons only.

The host preflight acquired an original 5-second window at the unchanged 2% external-process threshold after 10.056893 seconds (60-second acquisition cap). It is not continuous whole-arm quiet evidence. Invocation explicitly retains performance qualification NULL; the user physical-host waiver does not change numeric failures or certify quiet.

## Joint raw TreeDB counters

These are individual observed rows, not a synthetic combination of independent medians. Each counter is available in its row.

| Row | batch Write | batch WriteSync | grouped commits | groups | participants | group syncs | max group* | command-WAL file syncs | flushes | checkpoints | write bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| durable r1 | 0 | 1118 | 38 | 5 | 38 | 4 | 8 | 1085 | 38 | 0 | 167187 |
| durable r2 | 0 | 1101 | 46 | 8 | 46 | 7 | 8 | 1063 | 46 | 0 | 165455 |
| durable r3 | 0 | 997 | 101 | 14 | 101 | 15 | 8 | 910 | 101 | 0 | 155573 |
| relaxed r1 | 959 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 959 | 0 | 152684 |
| relaxed r2 | 926 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 926 | 0 | 150723 |
| relaxed r3 | 1055 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1055 | 0 | 163059 |

Command-WAL point-append calls, value-log sync calls and value-log file-sync calls are zero in all six TreeDB rows. *Max group is a process-lifetime high-water gauge, not a timed delta. The other diagnostic columns are native before/after counter deltas.

The durable data show many WriteSync and command-WAL file-sync calls alongside a smaller grouped subset; they do not show one common group covering all durable writes. Group-sync totals may differ from group totals within the sampled interval (r1 4 vs 5; r3 15 vs 14), so neither their ratio nor independently reported medians is a conservation equation. Source: `livebench/cmd/main.go:192–278,1089–1130`: before samples precede timed exercise; after samples follow CPU/HWM/posting-disk collection. These counters can straddle asynchronous/background completion boundaries and do not provide per-RPC attribution.

Zero Raft WAL syncs, complete Alpha Raft WAL syncs, physical syscall timing and full ACK phase accounting are not supplied by these counters. Zero legacy point-successor/point-append counters do not negate the independently witnessed cow_ordered route. Zero timed checkpoint delta is not a claim that setup, close or other lifecycle work never checkpointed. No causal percentage of the throughput gap can be assigned solely from this table.

## Disposition and limits

The evidence supports correctness parity for this small workload and a real recorded process/store-WAL performance STOP. It does not establish physical equilibrium, whole-system CPU/RSS/disk, full durability equivalence under filesystem loss, or full production-parent completion. Remaining filesystem arms may proceed only under the already recorded prospective diagnostic-continuation amendment; they cannot erase this arm or unlock the whole campaign.

Audit actions were bounded local JSON/source/log reading, hash checks and independent Python arithmetic only. No source/state changes, runtime, SSH, benchmark, GitHub mutation, retry or row replacement was performed.

