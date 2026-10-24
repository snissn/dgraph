# #5206 final Raft-checkpoint evidence — accepted STOP report

**Coordinator-accepted completed STOP evidence deliverable.** This packet contains all **36 original live decision JSON, six excluded profile-result JSON and six CPU profiles**, the original three arm reports, runtime sidecars and source/archive receipts, and the final normalized paired analysis. Independent baseline and final-analysis audits accept evidence identity/arithmetic. [Coordinator acceptance](ROOT_REPORT_ACCEPTANCE.json) completes the report deliverable without qualifying the product. No runtime row was rerun to obtain these results.

The full campaign is **`STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE`**. The original process/store-WAL report failed its unchanged throughput gates; its failure is retained. The subsequent filesystem and baseline arms are the prospectively authorized diagnostic continuation, not a replacement campaign. A filesystem arm's original **ADVANCE** report does not turn this campaign into PASS or promote TreeDB. **Badger remains the production default.**

## Original measured sources and scope

| Role | Exact source / binding |
| --- | --- |
| Candidate merged Dgraph | `bad4ced2c88e15076f73e3647a72e965f63be2b9`, clean tree `6260025d2c1f789529dfa9f9ba6b0aa533d2e245` |
| Merged Gomap integration | `f906b8a2aa1279ed6b6627028cba71d491db04c3` |
| Actual candidate published module | `v0.6.2-0.20261023204715-29a2fdf153ce`, commit `29a2fdf153ce6e214abf9502d7970de727a38967`; v2 manifest / no replacement |
| Executed candidate server | SHA256 `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175` |
| Executed baseline server | SHA256 `58075e248543eb56ec618fe4099c04fd7fc741817558765e83e98af6432960a8` |
| Original baseline Dgraph | `b87e967bc3c18d4c609e652d79c7ccb8e94ace1b` |
| Accepted local harness adaptation | `7f2720bf0d7ed90e82b04c52911bff92e9f22726`, tree `0ba5d97bc1d9c71f37b04c8e4b067ed6aee540e2` |
| Actual baseline Gomap | `fc3c3db97ec2954d47a1adf731d501e7b73f181f`, ordinary COW; v1 manifest explicitly records the accepted local replacement |
| Toolchain / environment | Go 1.26.9, linux/amd64, CGO enabled; `GOMAXPROCS=2`, `GOFLAGS=-p=2`, ordinary GC (`GOGC` / `GOMEMLIMIT` unset), per-arm invocation/context records |

These are the measured identities. This evidence/documentation descendant does not become a newly measured runtime. The baseline adaptation is **not claimed to be a publicly fetchable commit**: [accepted patch and module inputs](baseline-reproduction/accepted-preparation/), the [local Git bundle](baseline-reproduction/baseline-local.bundle) requiring public parent `b87e967...`, and the [build-only receipts](baseline-reproduction/local-build-only/) preserve its reconstruction. Their historical/local binary hashes are not substituted for the actually executed remote baseline server; the baseline arm's own v1 manifest supplies that identity.

Each arm uses the unchanged maintained livebench shape: Badger/TreeDB × relaxed/durable × three repeats; 500 seeded nodes, 100 warmup operations, 2,000 timed operations, concurrency four, seed `20260711`. The original two CPU-profile runs per arm use their original separate 100,000 relaxed / 10,000 durable operation settings and are excluded from the 36 decision rows. No samples are dropped or reclassified as profile runs.

| Arm | Survival / posting ownership | Current archive status |
| --- | --- | --- |
| Candidate process | Zero and Alpha process survival; TreeDB relaxed/durable both `store_wal` | 12 decision + 2 excluded profile results; original report **STOP** |
| Candidate filesystem | Zero and Alpha filesystem survival; durable TreeDB `raft_checkpoint`; relaxed TreeDB remains `store_wal` | 12 decision + 2 excluded profile results; original report **ADVANCE**, diagnostic continuation only |
| Baseline filesystem | Zero and Alpha filesystem survival; original TreeDB `store_wal` | 12 decision + 2 excluded profile results; original report **STOP** (durable throughput failure) |

The live-row TreeDB `cow_ordered` startup/restart evidence is independent of a configuration label. Badger rows are Badger, not COW. In particular, the separate adapter microbenchmarks are **ordinary adaptive mutable-memtable benchmarks, not COW**; see the supplementary review below.

## Gate status — original failures remain failures

The frozen comparison mapping is [preserved here](protocol/MATRIX_COMPARISON_MAPPING.md). The original within-arm throughput gate compares durability-matched TreeDB and Badger **ratios of medians**, requiring TreeDB ≥0.97 × Badger. It does not compare individual ordinal samples or pool survival modes.

| Original arm / class | Badger median ops/s | TreeDB median ops/s | TreeDB change | Original throughput result |
| --- | ---: | ---: | ---: | --- |
| Process / relaxed | 9,938.1542 | 9,564.2564 | −3.7622% | **FAIL** |
| Process / durable | 837.2059 | 337.2918 | −59.7122% | **FAIL** |
| Filesystem / relaxed | 273.2701 | 334.9164 | +22.5587% | Numeric gate passes in this diagnostic arm |
| Filesystem / durable | 270.1905 | 429.0842 | +58.8080% | Numeric gate passes in this diagnostic arm |
| Baseline filesystem / relaxed | 270.2638 | 475.7794 | +76.0426% | Numeric gate passes in this diagnostic arm |
| Baseline filesystem / durable | 267.0481 | 195.5688 | −26.7665% | **FAIL** |

The unmodified arm reports are [process](arms/process/report.md), [filesystem](arms/filesystem/report.md), and [baseline](arms/baseline/report.md). Exact original JSON, raw samples and counter tuples are retained; these rounded summary values do not replace them. Logical parity and clean-restart checks pass in all three arms. Shell exit zero alone is not an ADVANCE verdict.

The [final analysis](analysis/final-matrix-analysis-01.md) and [complete JSON](analysis/final-matrix-analysis-01.json) preserve all three raw candidate/baseline ratios and their median for each filesystem backend, durability class and repeat `r1..r3`. Process has no original process-baseline anchor. The source-paired TreeDB findings are:

| Metric: candidate filesystem versus original baseline filesystem | Relaxed paired median change | Durable paired median change | Disposition |
| --- | ---: | ---: | --- |
| Throughput | −29.4479% | +120.1594% | Relaxed decline requires review; durable improvement is not overall acceptance |
| Aggregate p95 latency | +45.0524% | −29.3899% | Relaxed **FAIL/review required** |
| Aggregate p99 latency | +38.2301% | −24.8130% | Relaxed **FAIL/review required** |
| Timed Alpha CPU | +2.7027% | −17.8571% | Within-bound results remain review-only |
| Read-ready recovery | −0.1945% | +20.4990% | Durable **FAIL/review required** |
| Read/write-ready recovery | −0.1156% | +27.8502% | Durable **FAIL/review required** |

These are medians of the three matched ratios, **not ratios of independent medians**. The latter is used only for the original within-arm 0.97 throughput gate. In particular, a ratio-of-medians increase must not be substituted for the relaxed recovery paired medians above. All three Badger control ratios, including their variation, remain in the analysis; none are normalized away. A >5% paired-median increase in available lower-is-better ancillary metrics requires FAIL/review disposition; ≤5% is not automatic acceptance. No new throughput tolerance, numeric waiver or favorable-row substitution is authorized. Zero/missing/unavailable denominators do not become invented ratios or improvements.

The filesystem result is consistent with changing durable posting ownership from an additional TreeDB command WAL to the Raft-owned checkpoint route: all three candidate durable TreeDB rows record posting command-WAL sync and public durable-batch counters at zero, while group metrics explicitly report unavailable under Raft authority. It does **not** prove zero physical sync work, exclusive causal attribution, or a same-survival process/filesystem A/B. Zero/Alpha Raft persistence and exact-root checkpoint publication remain required. The relaxed route still uses `store_wal`; its source-paired regression and the durable recovery regressions prevent a no-regression/promotion claim. Excluded CPU profiles measure sampled on-CPU work, not fsync waiting time.

The [diagnostic continuation amendment](protocol/DIAGNOSTIC_CONTINUATION_AMENDMENT.json) was accepted prospectively after process STOP. It permits the remaining 24 original rows once, preserving every original threshold and verdict; it does not unlock whole-campaign promotion. [The original orchestration STOP review](protocol/PROCESS_STOP_PROTOCOL_REVIEW.md) preserves that boundary.

## Supplementary allocation/lifecycle microbenchmarks

The [full micro allocation review](micro/MICRO_ALLOCATION_REVIEW.md), [all-cell comparison table](micro/final-micro-comparison-01.md), [all 280 raw samples and metrics](micro/final-micro-comparison-01.json), and [source/input bindings](micro/MICRO_ALLOCATION_REVIEW_BINDINGS.json) are included unchanged. **The preserved preview is now verified byte-for-byte against the completed baseline archive** (`aa6be31e723d072e86ec7d5cf746c8ef6571862fda8c302667f1e13d7fd113ca`). The review preserves its historical pending wording and appends the final archive-closure audit; the input mapping also records this verified custody binding. No historical result was substituted. Original input-path to archival-path mapping is in [INPUT_PATH_MAPPING.json](micro/INPUT_PATH_MAPPING.json).

The unchanged adapter constructors use `ProfileCommandWALRelaxed` and leave `MemtableMode` empty. Public Open selects `adaptive`, initially `append_only`; the livebench CLI's `REQUIRE_TREEDB_MEMTABLE=cow_ordered` does not alter this separate `go test` path. There is no micro runtime witness of which later ordinary adaptive mode was selected. Do not label any of these three arm microcaptures COW or Raft-owned.

Descriptive candidate-filesystem versus baseline findings include WriteBatch16/TreeDBStore **18,597→19,597 B/op (+5.377%)** with 62 allocations unchanged, while its median time improves 3.559%. Individual direct/adapter reopen time grows **12.867% / 13.752%**, with allocations +74/+76; paired-lifecycle total time is nearly unchanged (−0.0149%) with allocations +140. All observations, including unfavorable ones, remain visible. Adaptive `b.N`, final DB/history shape and concurrent background allocation differ; count-five samples are not paired AB timing. These are not server operation allocations, causal attribution, physical write amplification or an added micro acceptance gate.

## Source, semantic and raw evidence map

- `arms/process/`, `arms/filesystem/`, and `arms/baseline/`: original JSON, report, context, micro stdout, runtime configuration/store sidecars and separate CPU profiles/tops. Original invocation/source/selected-file/quiet receipts are in each `receipts/` directory. Actual sidecar counts are inventoried; no fixed 144-total assumption omits profile or store witnesses.
- `semantics/{schema,floor,admin,native}/result.json`: the four original driver results. Root [semantic acceptance](protocol/SEMANTIC_ROOT_ACCEPTANCE.json), [schema audit](protocol/SCHEMA_AUDIT.md), [floor audit](semantics/floor/FLOOR_AUDIT.md), and [admin/native audit](protocol/ADMIN_NATIVE_AUDIT.md) bound the process-recovery/operational evidence. These are bounded diagnostics, not full power-loss or equilibrium certification.
- `source-and-build/`: original Go environment, compile/source-selection receipts and baseline-v1/candidate-v2/executed manifests. Each per-arm manifest/invocation binds its own executed server and clean source. Prospective/build-only receipts are labeled as such.
- `raw-archives/`: deterministic `gzip -n -6 -c` copies of all currently available original selected-file tar archives, original unmodified receipts, original-tar/gzip hashes and sizes, member inventories and [reproduction instructions](raw-archives/REPRODUCTION.md). Decompression is verified to reproduce exact original tar bytes. All three arm archives and the four semantic archives are included. The seven gzip streams total 3,442,812 bytes and reproduce 28,385,280 original tar bytes. Runtime databases and executable binaries are not duplicated here; they remain under the coordinator's remote retention custody.
- [FILE_INVENTORY.json](FILE_INVENTORY.json) records original-to-archive paths, sizes and SHA256. [PACKAGE_SHA256SUMS](PACKAGE_SHA256SUMS) binds all current archival files except itself. Neither replaces the actual source manifests or original archive receipts. Paths inside unmodified historical reports/audits retain their original task layout; this README and input mapping give the normalized locations.

## Overrides and remaining limits

The [user physical-host quiet override](authorizations/USER_AUTHORIZATION.json) permits this host's admission/finalization despite observed external physical-host noise. Original quiet outcomes remain retained; an acquired five-second preflight is not a continuous whole-run quiet certificate. It does **not** waive numeric performance, correctness, missing observables, or workload/threshold rules.

Separately, the [integration M0 stability override](authorizations/M0_STABILITY_OVERRIDE.json) records the **original FAIL**: max-writer-pause CV **12.54285559% > unchanged 10%**, although all 20 semantic runs passed. The explicit user waiver made that stability task nonblocking for this integration/normal merge. It did not change the metric, fabricate check success, authorize an unchanged rerun, or turn the Dgraph process gates into PASS. Original failed/NULL component evidence and its separate cleanup/disposition remain retained by their owning receipts.

Alpha CPU is an observed timed-phase delta; RSS is Alpha HWM including setup; disk columns cover the postings directory. Available aggregate latency histograms are not per-operation p95/p99. Legacy HTTP-health recovery is not aligned across backends; read-ready and read/write-ready provide additional explicit boundaries. Clean Alpha restart with Zero alive and semantic-driver SIGKILL are not modeled power loss. CPU profiles measure sampled on-CPU work, not fsync wait or exclusive causal attribution.

Full per-operation tails, full RPC/Raft/apply/store ACK conservation and latency attribution, whole-cluster CPU/RSS/disk, equivalent physical write amplification, server-operation allocations, sustained default maintenance/retirement equilibrium and any unproved full RPC power-loss chain remain **UNQUALIFIED**. Retained engine component stable-image tests may be cited only at their exact source/scope. Raft-owned posting command-WAL sync zero is an ownership-specific counter result, not zero physical durability work: durable Raft input and exact-root checkpoint/namespace publication still perform their required persistence work.

## Owned next inquiry

The coordinator retains Gomap #5207 and Dgraph #34 for three source-bound inquiries. First, attribute the source-matched relaxed `store_wal` regression: the posting ownership policy is unchanged there, and the retained paired rows/profiles, including control variation, cannot causally blame COW. Second, discriminate durable recovery phases—restored cut, Raft suffix replay, posting validation, and fresh ACK/read readiness. The observed +20.4990% read-ready and +27.8502% read/write-ready paired medians already require FAIL/review; the relaxed recovery paired medians do not show a median regression. Third, the original process/store-WAL parity gaps remain outside the new filesystem policy. Targeted eligible-arrival/ACK-boundary attribution must precede changing the grouping algorithm.

This report selects or executes no new workload, introduces no arbitrary timer change, and authorizes no product promotion. Full capability, maintenance equilibrium and power-loss qualification remain with the open parents.

Gomap [#5207](https://github.com/snissn/gomap/issues/5207), Dgraph [#34](https://github.com/snissn/dgraph/issues/34) and M0 [#4519](https://github.com/snissn/gomap/issues/4519) remain **OPEN**. Guarded staged integration and a completed STOP evidence report do not establish the full parent product outcome, support guarded-off capabilities, change defaults, or waive unresolved acceptance axes. The coordinator accepts this as the completed #5206 STOP evidence deliverable; the open parents retain every unmet product obligation.

## Offline normalized-report reproduction

From the evidence branch checkout root, replay the retained inputs into a fresh fast-disk directory. These commands extract existing evidence and run only the stdlib artifact reader; they do **not** build a server or run a benchmark. The reader refuses an existing output prefix. Verify the package checksums and decompressed archive receipts before extraction as described in `raw-archives/REPRODUCTION.md`.

```bash
PACKET="$(pwd -P)/worker/treedb/artifacts/issue-5206-final-20261024"
findmnt --mountpoint /mnt/fast4tb
REPLAY="$(mktemp -d /mnt/fast4tb/5206-report-replay.XXXXXX)"
mkdir "$REPLAY/process-evidence" "$REPLAY/filesystem-evidence" "$REPLAY/baseline-evidence"
tar -xzf "$PACKET/raw-archives/process-12-EVIDENCE.tar.gz" -C "$REPLAY/process-evidence"
tar -xzf "$PACKET/raw-archives/filesystem-12-EVIDENCE.tar.gz" -C "$REPLAY/filesystem-evidence"
tar -xzf "$PACKET/raw-archives/baseline-filesystem-12-EVIDENCE.tar.gz" -C "$REPLAY/baseline-evidence"
cp "$PACKET/protocol/MATRIX_COMPARISON_MAPPING.md" "$REPLAY/"
cp "$PACKET/protocol/DIAGNOSTIC_CONTINUATION_AMENDMENT.json" "$REPLAY/"
python3 "$PACKET/analysis/analyze_final_matrix.py" --root "$REPLAY" --output-prefix "$REPLAY/final-matrix-analysis"
printf '%s  %s\n' f5b40f19fd2f77fabeaa631850532f362964b088ad6df040f198879567e6d740 "$REPLAY/final-matrix-analysis.json" | sha256sum -c -
printf '%s  %s\n' 0035a214aa2ad2b728baa1463e3ee0246bd669bd47840c60d40fe0cf3612b971 "$REPLAY/final-matrix-analysis.md" | sha256sum -c -
```

The original expanded runtime command arrays, environment and source authentication are in each arm's `receipts/*-invocation.json`, source manifest and `context.txt`; the exact driver orchestration is retained in `protocol/launch_{process,filesystem,baseline}.sh` and `protocol/run_matrix*.py`. These historical invocation receipts preserve the original remote layout. They are not instructions to execute a new runtime campaign, and no new execution is selected by this report.

## Analyzer correction and final acceptance

The first offline final-reader execution rejected the accepted baseline-v1 module display (`nominal => replacement@(devel)`). [Its original source and failure](analysis/analyzer-attempt-01/) are preserved. The [narrow correction receipt](analysis/ANALYZER_FINAL_SCHEMA_CORRECTION.json) binds the corrected reader, accepted replacement display/build-info checks and resulting outputs. Candidate no-replacement validation was retained. This was an artifact-parser correction, not a workload rerun or modification of any raw row. The earlier prepared reader and review remain historical custody rather than acceptance of later bytes.

The independent [baseline audit](arms/baseline/BASELINE_ARM_AUDIT.md) and [final arithmetic review](analysis/FINAL_ANALYSIS_REVIEW.md) accept identity and faithful reporting, not product performance. The latter independently checks all 108 individual source-paired ratios (32 lower-is-better cells and four throughput cells) and the six original within-arm gates. The [coordinator acceptance](ROOT_REPORT_ACCEPTANCE.json) records completion of this report deliverable. [Offline reconstruction](analysis/OFFLINE_REPRODUCTION_ACCEPTANCE.json) reproduced both normalized outputs byte-for-byte from archived raw inputs. The containing evidence-only commit supplies immutable public custody when pushed and linked from #5206; it is not a newly measured runtime or product promotion. **The campaign remains STOP regardless of publication acceptance.**
