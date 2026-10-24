# Final 36-row analysis independent arithmetic review

**ACCEPT as a faithful supplementary artifact analysis; whole campaign remains STOP. No overall PASS or promotion.**

Exact reviewed outputs:

- Reader SHA256 `d2d103675e1525942d7ffa95454b684c43ff9f8eb0649fac81703217f51a2442`.
- JSON SHA256 `f5b40f19fd2f77fabeaa631850532f362964b088ad6df040f198879567e6d740`.
- Markdown SHA256 `0035a214aa2ad2b728baa1463e3ee0246bd669bd47840c60d40fe0cf3612b971`.
- Frozen mapping SHA256 `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`.

The first reader's baseline-module refusal and original0375 source remain archived. The correction is a baseline-v1-only identity parser fix: require the exact accepted replacement display and adjacent dep/replacement build-info lines. Candidate v2 checks remain unchanged. No raw evidence, workload, metric, numerical rule or gate was changed.

Independently read the original results rather than reusing the analyzer's arithmetic: all 36 rows and their SHA256-bound JSON objects match the analysis; all cross-arm checksum/node pairs match. Recomputed every ratio-of-throughput-medians and checked each original report's two-decimal delta, The exact source-comparison inventory is **32 lower-is-better cells plus four throughput cells** (each three repeats, 108 ratios), all independently checked. Six profiles remain excluded, never substituted for original repeats. No sample was selected or dropped; no Badger normalization occurred.

## Six original within-arm gates

| Arm | Class | Badger median ops/s | TreeDB median ops/s | TreeDB/Badger | Delta % | Original gate |
|---|---|---:|---:|---:|---:|---|
| process | relaxed | 9938.154220 | 9564.256400 | 0.9623775389 | -3.762246 | FAIL |
| process | durable | 837.205864 | 337.291768 | 0.4028779327 | -59.712207 | FAIL |
| filesystem | relaxed | 273.270140 | 334.916415 | 1.2255873065 | +22.558731 | PASS |
| filesystem | durable | 270.190498 | 429.084234 | 1.5880803982 | +58.808040 | PASS |
| baseline | relaxed | 270.263791 | 475.779360 | 1.7604258350 | +76.042584 | PASS |
| baseline | durable | 267.048139 | 195.568790 | 0.7323353418 | -26.766466 | FAIL |

Process relaxed and durable both fail, and baseline durable fails. Candidate filesystem ADVANCE is a local original-report outcome; it cannot waive the prior process STOP or source-regression obligations.

## Every matched filesystem comparison

All percentages are `100*(candidate[r]/baseline[r]−1)` at the same backend/class/repeat. The median is computed from all three ratios, not a ratio of independent medians. These are sequential source-arm comparisons, not contemporaneous interleaved AB measurements. They do not isolate the recovery-owner cost from all other source changes.

| Backend | Class | Metric | r1 % | r2 % | r3 % | Paired median % | Original mapped review status |
|---|---|---|---:|---:|---:|---:|---|
| badger | relaxed | p95 | -14.4918 | -0.8143 | +35.7050 | -0.8143 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | p99 | -23.3813 | +1.8334 | +35.5475 | +1.8334 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | cpu_seconds | -8.4507 | +0.0000 | +6.2500 | +0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | rss_peak_bytes | +0.0712 | +0.1320 | +0.0977 | +0.0977 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | disk_logical_bytes | +0.0000 | +0.0000 | +0.0000 | +0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | disk_allocated_bytes | +0.0000 | +0.0000 | +0.0000 | +0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | recovery_read_seconds | -23.1679 | +0.0607 | +0.1024 | +0.0607 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | recovery_read_write_seconds | -23.0009 | +0.0600 | +0.4074 | +0.0600 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | throughput | +22.1473 | +1.1124 | -21.2026 | +1.1124 | DECLINE_REVIEW_REQUIRED |
| badger | durable | p95 | +0.9500 | -47.4535 | +1.8606 | +0.9500 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | p99 | +8.0011 | -63.7112 | +1.9309 | +1.9309 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | cpu_seconds | +0.0000 | -8.8608 | -2.8169 | -2.8169 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | rss_peak_bytes | -0.0473 | +0.0372 | +0.3195 | +0.0372 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | disk_logical_bytes | +0.0000 | +0.0000 | +0.0000 | +0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | disk_allocated_bytes | +0.0000 | +0.0000 | +0.0000 | +0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | recovery_read_seconds | +0.1934 | -23.1326 | -29.9747 | -23.1326 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | recovery_read_write_seconds | +0.0978 | -23.0521 | -29.9237 | -23.0521 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | throughput | +4.5803 | +42.2693 | +1.1767 | +4.5803 | NO_OBSERVED_DECLINE_NOT_ACCEPTANCE |
| treedb | relaxed | p95 | +45.7026 | +45.0524 | +0.0320 | +45.0524 | FAIL_REVIEW_REQUIRED |
| treedb | relaxed | p99 | +38.2301 | +43.8688 | +0.2100 | +38.2301 | FAIL_REVIEW_REQUIRED |
| treedb | relaxed | cpu_seconds | +2.7027 | +4.1096 | +0.0000 | +2.7027 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | rss_peak_bytes | +0.0008 | -0.0644 | -0.4730 | -0.0644 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | disk_logical_bytes | +0.0225 | -0.1495 | -0.3181 | -0.1495 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | disk_allocated_bytes | +0.0853 | -0.0851 | -0.2551 | -0.0851 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | recovery_read_seconds | -0.5592 | -0.1945 | +29.6006 | -0.1945 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | recovery_read_write_seconds | -0.2786 | -0.1156 | +29.3125 | -0.1156 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | throughput | -29.4479 | -29.6068 | -2.0947 | -29.4479 | DECLINE_REVIEW_REQUIRED |
| treedb | durable | p95 | -33.7642 | -23.0782 | -29.3899 | -29.3899 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | p99 | -24.8130 | -21.2831 | -29.3151 | -24.8130 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | cpu_seconds | -13.2530 | -22.3529 | -17.8571 | -17.8571 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | rss_peak_bytes | +1.9610 | +1.0303 | +1.8852 | +1.8852 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | disk_logical_bytes | -7.3690 | -7.0735 | -7.0860 | -7.0860 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | disk_allocated_bytes | -7.0145 | -6.8376 | -6.6781 | -6.8376 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | recovery_read_seconds | -18.5988 | +27.9597 | +20.4990 | +20.4990 | FAIL_REVIEW_REQUIRED |
| treedb | durable | recovery_read_write_seconds | -1.3130 | +27.8502 | +27.9478 | +27.8502 | FAIL_REVIEW_REQUIRED |
| treedb | durable | throughput | +101.2494 | +123.2680 | +120.1594 | +120.1594 | NO_OBSERVED_DECLINE_NOT_ACCEPTANCE |

## Material outcomes and boundaries

- **TreeDB relaxed source throughput falls −29.4479% paired median**, with all three repeat changes negative (−29.4479%, −29.6068%, −2.0947%). p95 grows **+45.0524%**, p99 **+38.2301%**, both FAIL/review under the selected >5% rule. Better same-arm parity against Badger does not excuse this original-source regression.
- **TreeDB durable source throughput improves +120.1594% paired median**, with all three positive. p95/p99 and Alpha CPU improve. These gains coexist with **durable aligned recovery-read +20.4990% and read/write +27.8502%**, both FAIL/review. The recovery pairs are mixed (read −18.5988/+27.9597/+20.4990%; read/write −1.3130/+27.8502/+27.9478%); none was dropped.
- Relaxed recovery paired medians are **−0.1945% read and −0.1156% read/write**, not regressions under the chosen statistic, even though r3 grows +29.6006/+29.3125%. Do not substitute the ratio of independent recovery medians for the preselected paired statistic, or hide that adverse repeat.
- Badger relaxed source throughput pairs are **+22.1473/+1.1124/−21.2026%**, while durable are **+4.5803/+42.2693/+1.1767%**. Substantial control drift is observable, but no source/physical-noise causal attribution is proved and no normalization or waiver is applied.
- No comparison operand in the selected metric table is missing/nonpositive here; all three observations remain available in each cell. Future/missing observations would be UNQUALIFIED, never zero-imputed or a selected-repeat median. Counts at zero remain valid absolute operational counters, not valid ratio denominators.

Alpha CPU is timed process CPU, RSS is setup-inclusive VmHWM, disk covers postings only and must show both logical and allocated measures. Recovery is a clean Alpha restart with Zero alive through validated read and new CommitNow/read, not an abrupt-crash/power-loss latency claim. Legacy HTTP-health recovery remains descriptive only. Sampled profiles cannot quantify fsync wall-time shares. Per-operation Dgraph allocations, full physical I/O/write amplification, complete end-to-end storage/heap equilibrium and whole-chain power-loss certification remain unavailable.

The independent source/row/byte checks support the report's integrity, **not** accepted performance or parent completion. `overall_pass` and `performance_qualification` remain null; `STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE` remains unchanged. The micro review separately surfaces its allocation/lifecycle differences and is not substituted for server metrics.

Independent data/check source: `baseline-evidence/BASELINE_AUDIT_DATA.json` and `baseline-evidence/audit_baseline_artifacts.py`. No runtime, SSH, Go, benchmark, canonical source, state or GitHub mutation occurred.
