# Independent filesystem arm artifact audit

ACCEPT the retained filesystem arm as complete diagnostic evidence. Its original within-arm report is ADVANCE. Whole-campaign qualification remains `STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE`; this arm does not waive the process arm or establish original-source regression acceptance.

## Completeness and identity

All 161 selected-file SHA256 bindings verified against the local extracted bytes (8,574,677 bytes). The selected manifest SHA256 is `838c9cb607989979b73f639018a7928fd282bfbbd967ba6c853101792a306fde`.

The invocation is ADVANCE with child exit0, collection role DIAGNOSTIC_CONTINUATION_ONLY, qualification null, exact frozen comparison mapping `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`, and amendment `33a7fbd763a7ab4326381a758561522163b46f629552dfd9f5e68584424a83ac`. Its manifest/report hashes match the retained files. Physical quiet is recorded only as ACQUIRED_ORIGINAL_WINDOW_NOT_CONTINUOUS, not a continuous-host qualification.

All twelve original rows have clean Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`, Go1.26.9, official Gomap `v0.6.2-0.20261023204715-29a2fdf153ce`, and actual server SHA256 `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175`. The version2 source manifest records no replacement and module sum `h1:Y85xjKKdrqj5UXLCzYCuVzYXbk0oDGf8zVBwqowuxrs=`. Manifest/verify stdout are retained with empty stderr. This audit verifies those archived bindings; it does not re-run the remote binary/module verifier.

There are exactly repeats1,2,3 for each Badger/TreeDB × relaxed/durable cell, no excluded/profile/contaminated row in the decision matrix, and no dropped repeat. Every row retains filesystem survival, dataset500, warmup100, timed2000, concurrency4, seed20260711, 60% point/20% one-hop/20% writes, and aligned read-commit-read-v1 recovery. Ordinary GC settings remain in the unchanged invocation (GOGC/GOMEMLIMIT unset).

## Runtime and correctness witnesses

All48 runtime config sidecar hashes match the JSON witnesses, contain their exact logged config witness, and match prefixes of the retained role logs. This includes Zero and Alpha at startup and after Alpha restart; every witness and command confirms filesystem survival and HardSync:true. All24 Alpha store sidecar hashes match their JSON bindings.

All twelve TreeDB Alpha store observations (six rows × startup/restart) contain actual `treedb.cache.memtable_mode:cow_ordered`. Durable TreeDB is `raft_checkpoint`; relaxed TreeDB remains `wal_on_relaxed_sync` (raw status durability is relaxed). The relaxed cell is not silently converted to the Raft-owned posting route. Legacy point-successor counter zero is valid and is not used as an activation failure or basis-slot proof.

Every original row has schema_ok, restart_ok, and unsupported_ok true. All twelve share node_count920 and posting checksum `2e08caad769d3f0f221ddeaaa36e528ae1b4764fefdc27df3913a788474074d8`. The original report records logical parity/restart PASS. The rows prove the maintained clean Alpha restart checks; they do not independently add abrupt crash or power-loss certification.

## Original numerical report

Recomputed directly from the twelve retained throughput observations, using the original ratio of independent throughput medians:

| Class | Badger r1/r2/r3 ops/s | TreeDB r1/r2/r3 ops/s | Badger median | TreeDB median | TreeDB/Badger | Delta | Original >=0.97 rule |
|---|---|---|---:|---:|---:|---:|---|
| relaxed | 273.675686 / 273.270140 / 273.084295 | 335.941270 / 334.916415 / 329.684742 | 273.270140 | 334.916415 | 1.225587 | +22.558731% | PASS |
| durable | 280.051421 / 269.183224 / 270.190498 | 398.002482 / 436.642597 / 429.084234 | 270.190498 | 429.084234 | 1.588080 | +58.808040% | PASS |

These match the original report's +22.56%/+58.81% and ADVANCE classification. They are within-arm comparisons only. Filesystem candidate versus original-source baseline still requires the frozen same-backend/class/repeat comparison; neither result offsets the process arm STOP.

All three durable Raft-owned rows record available measured posting-store command-WAL file syncs=0. All five grouping metrics (commits, groups, participants, syncs, max group size) are explicitly unavailable in all three rows, with the reason that raft_checkpoint disables the posting-store command WAL and Raft is durability authority. They must remain UNAVAILABLE, not be promoted to measured zero. Posting-store zero syncs do not establish zero Raft fsyncs or zero durability work. Durable logical write-byte coverage and equivalent write amplification are unavailable as the original report states.

## Separate profiles and limits

Both TreeDB profile result rows are separate under profiles/*-run, bind the same clean source/binary, and retain actual pprof paths. They use 100,000 relaxed / 10,000 durable operations and are excluded from the twelve-row decision, never substituted for a repeat. Pprof SHA256: relaxed `6063ade4558625668e98ece245a7449f2795adf809eb0823ccfbba25fa0557a8`; durable `c77285fc630fc8e27ac8acf129b185557404f9244371b9642dcdbc8c75d9e375`.

The CPU tops retain5s windows with630ms/790ms total sampled CPU. CPU profiles cannot quantify fsync wall-time, I/O wait, or a causal share of the throughput gap. No CPU-only conclusion explains the observed parity improvement. RSS is Alpha VmHWM including setup, disk covers postings only, aligned recovery is clean Alpha restart with Zero alive, and legacy health recovery remains descriptive. No physical equilibrium, whole-system allocation/CPU, full Raft debt bound, whole Dgraph power-loss chain, or production/default promotion follows.

## Exact principal bindings

- Invocation: `1d885200020de6967fc43cdc425afdf4ba2e3ca8328daa491c0b37a901e286cf`
- Source manifest: `008eda694c391c88f23af0cad4b06ac145668a38eec7affc27378ea0813c518c`
- Original report: `b72931b85dc2011fadb956ada0370944bb03e46d6b610bb5e6d01828f82b32a6`

Artifact-only audit and this file write. No runtime, SSH, tests, analyzer execution, source/state mutation, GitHub action, or new workload was performed.
