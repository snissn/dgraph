# Process-arm STOP: protocol review

Source-only review, 2026-10-24. No execution, source/protocol amendment, or runtime artifact mutation performed.

## Decision

**The present frozen launcher must halt; it cannot silently continue.** Its prior-ADVANCE assertions were explicit and independently accepted. However, they are a coordinator-selected orchestration restriction, not a maintained reporter requirement that a failing legacy control prevents measurement of the separately selected filesystem candidate and original baseline.

**Recommend a recorded prospective root authorization to finish the remaining24 distinct original rows as diagnostic evidence, while locking this campaign's overall qualification to STOP.** No process rerun, failed-row replacement, threshold waiver or product promotion follows. Root has acceptance authority for the already-selected36-row scope; I found no separate user-issued global stop that requires new user permission. This recommendation alone does not authorize launching through the current halt.

## What is binding now

- `run_matrix_arm.py:18–19` requires process ADVANCE before candidate filesystem and filesystem ADVANCE before baseline. Lines59–66 preserve the actual reporter verdict and return nonzero for STOP.
- `MATRIX_RUNNER_REVIEW.md:9,12` expressly accepted those sequencing assertions and STOP propagation. This was not an unobserved implementation detail.
- `state/final-qualification-next-proof-v2.md:86` says a stopped script leaves remaining planned rows NULL with reason. Thus the currently unexecuted arms remain NULL/stopped until root prospectively records a different collection disposition.
- `EXECUTION_PLAN.json` independently selects process12 → candidate filesystem12 → original baseline filesystem12. Its acceptance text requires the actual report verdict, all failed/null evidence and no retry-until-pass; it does not itself state every earlier performance ADVANCE is a prerequisite to collecting a later arm.

## Why distinct-arm diagnostic continuation is coherent

- `state/final-qualification-plan-v1.json:13–32` defines process/store_wal as the unchanged ordinary fallback/no-regression control, explicitly **not** raft_checkpoint filesystem qualification. Lines33–50 separately select the candidate filesystem/raft_checkpoint versus original ordinary-COW/store_wal comparison.
- `state/final-qualification-next-proof-v2.md:64–76` selects the separate12-row profiles and matched original baseline; original report ADVANCE is not the complete parent verdict.
- `MATRIX_COMPARISON_MAPPING.md`, “Exact comparisons”1–5, requires each arm's original parity report, plus filesystem candidate/original matched-source comparisons. It explicitly says no original process anchor exists. A process result cannot substitute for the missing filesystem candidate or baseline.
- Maintained `worker/treedb/livebench/report.go:153–224` evaluates relaxed and durable TreeDB/Badger medians within the supplied result set. Delta below−3% produces STOP. It contains no cross-arm prerequisite.
- A baseline's own within-arm parity may also fail; excluding that baseline because a candidate failed would prevent the selected regression comparison and bias the surviving evidence. Retain each original verdict, including baseline STOP.
- Legacy store_wal and selected raft_checkpoint are deliberately different recovery policies. Their route labels establish the comparison roles, **not** a causal explanation of the performance gap.

## Current result and truthful limits

Coordinator reports first process/store_wal arm completed all12 decision rows plus2 excluded profiles, logical/restart correctness PASS, legacy COW route, and original report STOP: relaxed TreeDB9564 versus Badger9938ops/s (about−3.76%); durable337 versus837 (about−59.71%). These reported results are not independently recomputed here; root is retaining the full arm packet. Neither profiles nor later filesystem success can replace or erase this failure.

At present: **overall campaign STOP; no accepted final qualification; remaining24 rows NULL/unexecuted.** Even if later diagnostic filesystem rows pass their own gates, they do not repair the process failure or complete full-parent production obligations.

## Minimal prospective continuation receipt

Before collecting another row, root should record:

1. First arm and its exact source/report/manifest/output hashes remain immutable; original STOP and all rows/profiles retained. No first-arm retry.
2. Collection of candidate filesystem12 and baseline filesystem12 is authorized **only to complete the already-selected diagnostic comparison**. Overall campaign STOP is irrevocable for this packet; later results cannot grant overall ADVANCE.
3. Supersede only the launcher's cross-arm **performance-ADVANCE** collection prerequisites for these two distinct arms. Keep source/authenticity, completed semantic controls, quiet-policy truth, ordinary-GC environment, workload/order, timeouts, load/construction, cleanup and per-arm report gates unchanged.
4. A complete candidate filesystem numeric STOP still permits collecting the original baseline under this recorded diagnostic authorization; it is not a retry. Infrastructure, identity, correctness or incomplete-set failure requires a fresh root disposition rather than automatic bypass.
5. Each invocation retains its own ADVANCE/STOP/FAILED and nonzero status. Explicit diagnostic collection authorization must be a separate receipt, never a forged prior ADVANCE. Do not edit original reporter output or original process invocation.
6. Finish all original paired comparisons and missing-metric labels honestly; no promotion, no gate weakening, no pooling of profiles, no unchanged retries.

## Source bindings

- `run_matrix_arm.py`: SHA256 `3c9f08d3a65ef5a02481978f113b249d099957281befe289930256f04e01a761`.
- `EXECUTION_PLAN.json`: `7851c3e454b6aea893b87702c21cf83027627a81aa58af90b0b5e8270eadabfd`.
- `MATRIX_COMPARISON_MAPPING.md`: `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`.
- Maintained `livebench/report.go`: `1019cb7118add7fed3d610d7c8c174a570912b6192d738800a30c2f3e32f7a13`.
- `state/final-qualification-next-proof-v2.md`: `3206d10bb49b19316624be664b22f70cf6981b8cc535fda0f55154705ce9baf5`.

Root retains acceptance and execution authority. This is an artifact-only recommendation, not an implemented protocol change.
