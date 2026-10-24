# Baseline arm independent artifact audit

**ACCEPT the retained evidence and its identity bindings. No performance acceptance or product promotion.** The original baseline report is STOP (child exit0, outer wrapper exit1); whole-campaign STOP remains locked.

## Archive and exact source

- Archive `baseline-filesystem-12-EVIDENCE.tar`: 8,089,600 bytes, SHA256 `8c84c738db0119e90dacef38663222144e80edcf549b81e8049b79dc7da993e9`.
- Independently verified all **161** unique relative regular-file members, rejecting absolute/parent paths and nonregular members; every archived byte equals its safely extracted counterpart. All **160** selected-file SHA256 entries match.
- V1 source manifest SHA256 `7dcaa8e87a9b1a772eb7094d675f85dc4211c1aa0e66f67ab502f5538a4ff445` binds adapted baseline Dgraph `7f2720bf0d7ed90e82b04c52911bff92e9f22726`, source tree `0ba5d97bc1d9c71f37b04c8e4b067ed6aee540e2`, clean source, Go1.26.9, and executed server SHA256 `58075e248543eb56ec618fe4099c04fd7fc741817558765e83e98af6432960a8`.
- Exact accepted replacement is `/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/source/baseline-gomap`, bound to Gomap `fc3c3db97ec2954d47a1adf731d501e7b73f181f`, tree `c202dbc8cbcfc85b929845433c322cc012168d39`. The nominal July module version is not the executed source identity.
- All 14 row build-info strings equal the manifest build-info, including the exact nominal Gomap dep plus adjacent local `(devel)` replacement; displayed context includes that same replacement. V2 candidate no-replacement rules were not incorrectly applied to v1.
- Original report SHA256 `c01ccc04b4d766041676817517db9410c2566a642744c58f2ac878398acf7242`; invocation SHA256 `6651a82356c352a171a239ae2c9ea1eed55a0eeebb4c168f56e8db1a86934873`. Both match receipt bindings. Normal GC/memory-limit defaults remain unset, GOMAXPROCS2 and exact accepted external readonly modfile are recorded.

## All rows and actual activation

All 12 original rows are present, exactly repeats1–3 of Badger/TreeDB × relaxed/durable. Every row is clean, unexcluded, without classified contaminants; source/binary/toolchain match. Each uses dataset500/warmup100/timed2000/concurrency4/seed20260711, 60/20/20 mix, single Zero/Alpha, filesystem survival, aligned read-commit-read-v1 recovery. Every schema/restart/unsupported validation is true.

Independently checked **48** original runtime configuration sidecars and **24** original Alpha store sidecars against their recorded hashes and raw role-log prefixes. Every Zero/Alpha startup and after-restart config reports HardSync:true. All TreeDB sidecars show actual `treedb.cache.memtable_mode:cow_ordered` with fixed configuration; ordinary store_wal relaxed reports wal_on_relaxed_sync and durable reports wal_on_sync. Badger actual profiles are syncwrites=false/true by class. A CLI label alone was not used as activation evidence.

| Original row | Throughput ops/s | Result SHA256 |
|---|---:|---|
| badger-durable-r1 | 267.786103 | `5c5167ce1476af2c8659686294bf368b1e50dd2d5c53706779718d0eae9d7016` |
| badger-durable-r2 | 189.206835 | `bf9763f06dbf00d808cd79f17cdf2eb3a13ba90de13c1caa7e4ee74700de68be` |
| badger-durable-r3 | 267.048139 | `61f9ca70ea2897909a5816abd7764da9ab7dd69380eb1544207a0941cda51671` |
| badger-relaxed-r1 | 224.053744 | `6090ae53712d78ba712c4ee50b519f015fe56b448c417ef557e48efc8db9e533` |
| badger-relaxed-r2 | 270.263791 | `cbc6dfb2c4830eef568d1a681af8ff6a256b28d4b89d0aa9854bf679232dea6c` |
| badger-relaxed-r3 | 346.565181 | `4c1ec0b62575b09d4bd8d704de78ff0b97afe35d76abfb552c9ff1aa13696d68` |
| treedb-durable-r1 | 197.765755 | `b21f5f79dcf34f13c6251b80e1362b4d142f1c0552119721f967c0dbd69ab8ce` |
| treedb-durable-r2 | 195.568790 | `d7187661b10281ecd1c0762091cb3518d76f7350eb10f222afda18f71a6877fe` |
| treedb-durable-r3 | 194.897117 | `6c824f1304b51066b4c7d11b37c364fb6c608ee94ce8f06b0589c4a229c62071` |
| treedb-relaxed-r1 | 476.160888 | `d2c33eed8d9ea2d91ff6a9027b524f98bbad55f0c9ce23b61ea47251c53cd76b` |
| treedb-relaxed-r2 | 475.779360 | `34ce809b4f5296dd90669a454d784e0f9f6443be7783de3baa8e0d2e0d4c0647` |
| treedb-relaxed-r3 | 336.738403 | `f94038b3a4e26e7baf5ce32a7c9614efd0cd4410019edc0b066a883b84e4df23` |

Across **all 36 original rows** (process, filesystem candidate, filesystem baseline), posting checksum is exactly `2e08caad769d3f0f221ddeaaa36e528ae1b4764fefdc27df3913a788474074d8` and node count exactly **920**. No cross-arm logical mismatch was found. This validates the observed benchmark state/restart checks, not a complete crash/power-loss chain.

## Original baseline decision and diagnostic profiles

The baseline ratio of throughput medians is **1.7604258350** relaxed (+76.04258350%, PASS) and **0.7323353418** durable (−26.76646582%, FAIL). The original report STOP is numerically correct and is retained; child exit0 indicates successful collection, not performance acceptance.

Both separate TreeDB profiles are present and excluded: relaxed100,000 operations, durable10,000 operations. Their source/build/runtime bindings also match, with eight additional config and four store sidecars verified. Profile data/top-text hashes were checked as archived selected inputs. Both CPU profiles cover 5 s; sampled CPU is 600 ms relaxed and 390 ms durable. They cannot quantify fsync wall time or establish system I/O attribution, and were not pooled with the 12 decision rows.

The final archived micro raw exactly equals the previously supplied preview: SHA256 `aa6be31e723d072e86ec7d5cf746c8ef6571862fda8c302667f1e13d7fd113ca`. This closes the micro preview's archive-byte binding only; the micro's ordinary adaptive mode remains distinct from the live server's proven COW mode.

The original physical-host quiet observation/waiver and all adverse numeric outcomes remain retained. Evidence integrity acceptance does not establish resource equilibrium, whole-system CPU, per-Dgraph-operation allocations, full storage footprint, guarded capability completion, or product qualification.

## Independent offline checks

`audit_baseline_artifacts.py` and `BASELINE_AUDIT_DATA.json` preserve the audit inputs/checks and independent arithmetic. A preliminary ad-hoc validator expected `memtable_mode_config:cow_ordered`; actual maintained semantics are `memtable_mode:cow_ordered` and `memtable_mode_config:fixed`. That offline assertion was corrected by reading the actual source/witness field, without changing evidence or running workloads. The final independent offline validation completed normally. No SSH, Go, runtime, workload, GitHub, canonical source or state operations occurred.
