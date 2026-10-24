# Matrix launcher independent source review

**ACCEPT for execution under the already accepted explicit launch environment**, binding `run_matrix_arm.py` SHA256 `3c9f08d3a65ef5a02481978f113b249d099957281befe289930256f04e01a761`. Source-only review; launcher was not executed. No actual row/performance acceptance follows from this review.

## Reviewed invariants

- Exact candidate clean Dgraph HEAD bad4ced2c88e15076f73e3647a72e965f63be2b9 and accepted baseline adaptation 7f2720bf0d7ed90e82b04c52911bff92e9f22726 are checked. Existing output/invocation paths prevent hidden same-path replay. All four operational diagnostic stages must have success=true/diagnostic_only=true; native result must bind the accepted binary46763a6b... . Their stronger individual acceptance audits remain prerequisites, not replaced by these summary checks.
- Maintained `worker/treedb/run_durability_ab.sh` executes unchanged, without `--smoke`, source patching, alternate harness, or retry loop. Twelve rows retain relaxed Badger r1–3, relaxed TreeDB r1–3, durable Badger r1–3, durable TreeDB r1–3. Explicit shape is repeats3/dataset500/warmup100/timed2000/concurrency4; original seed/topology/mix stay in the unchanged harness. COUNT/BENCHTIME/profile overrides are removed so maintained defaults remain5/1s and excluded100000/10000ops/5s. No timed instrumentation is added.
- Process uses store_wal; candidate filesystem uses raft_checkpoint; baseline filesystem uses store_wal; all require cow_ordered. The maintained selector still keeps relaxed TreeDB on store_wal. Process→candidate filesystem→baseline filesystem ordering is gated by prior invocation ADVANCE. A failed/STOP arm does not silently proceed or fill remaining arms with replacement rows.
- Original quiet helper is imported from the exact final Gomap source. Only RuntimeError with exact text `host quiet preflight acquisition exhausted; no timed samples started` **and** retained acquisition status=exhausted invokes the explicit physical-host waiver. Missing/malformed evidence, another RuntimeError, or any other exception fails closed. Original acquisition output is preserved. This is offline preconstruction admission, not a claim of continuous quiet. `performance_qualification` stays null.
- Original shell exit nonzero fails. On exit0, both manifest and verify modes of the already reviewed maintained resource witness must succeed against the newly built/executed server. Candidate requires v2 (published exact module/content binding, no replacement); accepted baseline requires v1 (explicit local replacement bound to its clean actual Gomap source). Dgraph source revision is checked again. Different manifest versions are intentional, not an authenticity waiver.
- Report status is parsed independently of shell success: exactly one original ADVANCE or STOP marker must exist. STOP is recorded and exits1; original row correctness/runtime/complete-set checks run in the unchanged reporter. The wrapper does not itself compute paired filesystem ancillary ratios; MATRIX_COMPARISON_MAPPING.md remains the explicit separate acceptance obligation. ADVANCE alone cannot establish the full matrix or parent verdict.
- One process group runs per arm; timeout10800s sends group SIGINT then waits45s, escalating group SIGKILL if needed. Original stdout/stderr, invocation environment, source/manifest/report/comparison hashes and terminal state are retained. This wrapper does not independently inventory every surviving descendant after exit; root's process/cleanup receipts remain necessary after a timeout or unexpected termination.

## Required launch boundary, not a discovered source defect

The wrapper deliberately inherits most compiler/runtime environment; it does **not** itself unset GOGC/GOMEMLIMIT or assert Go version/GOMAXPROCS. Therefore use the already accepted native setup environment before launch: official Go1.26.9, GOMAXPROCS2, GOWORKoff, CGO_ENABLED1, ordinaryGC with GOGC and GOMEMLIMIT unset, authenticated module settings and task-local TMP/cache paths. Candidate build flags must remain readonly; baseline overrides GOFLAGS only with the previously accepted readonly external build.mod helper. The invocation records GOGC/GOMEMLIMIT/GOMAXPROCS/GOFLAGS/GOWORK/GOTOOLCHAIN: inspect those before accepting any arm. No hidden GC override is present in this source; inheritance is not proof that the launching shell is correct.

The quiet waiver does not relax load/construction checks, original0.97 matched-Badger gate, paired ancillary review rules, source authenticity, correctness, or the original observer's truth. No unchanged retry, alternative workload, or automatic performance acceptance is authorized.

## Bindings inspected

- `EXECUTION_PLAN.json`: 7851c3e454b6aea893b87702c21cf83027627a81aa58af90b0b5e8270eadabfd.
- `MATRIX_COMPARISON_MAPPING.md`: 4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2.
- Maintained `run_durability_ab.sh`: e1faba06e36b9916bb71343fbc858c783c9cd5d3dab7a9d338b3485bf183e95b.
- Maintained `livebench/report.go`: 1019cb7118add7fed3d610d7c8c174a570912b6192d738800a30c2f3e32f7a13.

Python syntax parsed without running the launcher. Only this review artifact was written; no source/state edits, SSH, tests, builds, runtime, or GitHub changes.
