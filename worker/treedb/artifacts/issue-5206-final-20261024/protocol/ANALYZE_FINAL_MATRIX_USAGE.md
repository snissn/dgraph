# Final matrix artifact reader

Prepared only; not executed against matrix artifacts. Run after all three evidence directories have arrived, with all twelve original rows, both separate profile result rows, invocation, source manifest, and original report per arm:

```sh
python3 /mnt/fast4tb/gomap-raft-production-20261023T0744Z/artifacts/final-native111-runtime-01/analyze_final_matrix.py \
  --root /mnt/fast4tb/gomap-raft-production-20261023T0744Z/artifacts/final-native111-runtime-01 \
  --output-prefix /mnt/fast4tb/gomap-raft-production-20261023T0744Z/artifacts/final-native111-runtime-01/final-matrix-analysis-01
```

Expected input arms are `process-evidence/process-12`, `filesystem-evidence/filesystem-12`, and `baseline-evidence/baseline-filesystem-12`. Companion invocation and source-manifest JSON files sit one directory above each arm. Missing/incomplete inputs or failed source/completeness bindings exit 2 as UNQUALIFIED without producing a partial comparison. Output `.json` and `.md` paths must both be new. No network, subprocesses, workload execution, or source modification is performed.

The JSON retains all 36 full original result objects and all six separate profile result objects with input hashes. Profiles are excluded by their diagnostic role/directory, not by fabricating `context.excluded=true`. It computes the original TreeDB/Badger ratio of throughput medians separately for each arm/class, plus matched filesystem candidate/baseline repeat changes for both backends/classes. All three ancillary repeat changes and their median remain visible; missing, unavailable, nonfinite or zero operands make that metric UNQUALIFIED. >5% paired-median growth is FAIL/review-required. Smaller growth is review-only, never automatic acceptance. Throughput source declines are flagged separately without introducing a 5% allowance. Legacy health recovery and operational counters remain absolute/descriptive in the retained JSON. Operational zero counters remain valid observations.

Source/manifest/receipt hashes, exact source revisions, module identity, binary hash, toolchain, fixed workload, stored runtime witness fields, original report correctness, and repeat conservation are checked. This supplements the original maintained report and already-executed resource-witness/runtime-sidecar verification; it does not independently re-run those verifiers or authenticate remote binaries. SHA256 is an integrity binding, not a new authenticity claim.

Whole-campaign STOP remains locked by the original process arm, regardless of later local ADVANCE reports. No overall PASS, qualification, promotion, retry, pooling across arms, process/filesystem source-regression comparison, or performance waiver is produced. The explicit physical-host quiet waiver and historical NULL observations remain in the unchanged arm receipts; this reader makes no new quiet claim.

Preparation validation: Python AST syntax parse only. No reader invocation, unit tests, benchmarks, SSH, runtime jobs, or source edits were performed.

Parser correction after source review: `required_treedb_memtable=cow_ordered` is required only for TreeDB rows. Actual v5 Badger configs omit this optional TreeDB-only field. Prepared-v1 script and usage are preserved under `analyzer-prepared-v1/`; this is a parser eligibility correction, not a workload, protocol, benchmark, or gate change. Analyzer remains unexecuted.
