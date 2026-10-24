# Independent final matrix analyzer source review

**ACCEPT corrected prepared source for supplementary offline analysis after all three complete arms arrive.** No analyzer invocation or matrix execution was performed. This is source review, not certification of inputs not yet collected.

Reviewed analyzer SHA256: `0375c0eb19e9f8b3eac258b134e376bfa19dde8f1e2335b247d2e10bd875d892`.
Frozen comparison mapping SHA256: `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`.
Diagnostic continuation amendment SHA256: `33a7fbd763a7ab4326381a758561522163b46f629552dfd9f5e68584424a83ac`.
Execution plan SHA256: `7851c3e454b6aea893b87702c21cf83027627a81aa58af90b0b5e8270eadabfd`.

## Concrete prepared-v1 bug and corrected scope

Original analyzer `c730650db22f2b6f522ee172f933fd0346dbf3ae84cfab2f34cbe9074f04c3c2` incorrectly put `required_treedb_memtable=cow_ordered` in common WORKLOAD. Actual retained schema-v5 process Badger rows omit that optional field; maintained `livebench/cmd/runtime.go:44–55` sets both TreeDB selectors only for TreeDB. Thus the original parser would deterministically reject a valid first Badger row before analysis.

The owner preserved prepared-v1 and moved the requirement into `backend == 'treedb'`, alongside the existing policy check, and corrected the inaccurate comment. I reread the corrected bytes. Actual TreeDB configs and actual startup/restart cow_ordered witnesses remain mandatory. No workload, gate, fixture or numeric bound changed. No other concrete valid-input rejection was found using retained process results and smoke manifests as the available format evidence.

## Frozen rules preserved

- Each arm requires exactly 12 unique, correctly named backend/class/repeat rows with repeats 1–3. Exactly two distinct profile paths per arm are required and retained separately. All 36 result objects and all six profile result objects are kept in JSON; profiles never enter decision medians. Normal rows with attached profiles are refused.
- Each within-arm class uses **median(TreeDB throughput)/median(Badger throughput) >= 0.97**, not median cross-backend repeat ratios. Each arm's original correctness report, report SHA and STOP/ADVANCE receipt are preserved. Both durability classes remain separate.
- Source comparisons pair candidate filesystem and original-baseline filesystem for the same backend, class and repeat. Both TreeDB and Badger controls are retained. No process original-source baseline is fabricated and no process/filesystem comparison is promoted to source qualification.
- For every matched lower-is-better metric, JSON retains all three raw candidate/baseline observations and each derived percentage change; all-three median only. Missing, unavailable, nonfinite or nonpositive ratio operands or unit disagreement make that pair/median UNQUALIFIED. Zero denominators are not imputed or silently dropped.
- Ancillary median growth **>5%** is FAIL/review-required. <=5% is explicitly review-only, not acceptance. Source-paired throughput is separate: any observed repeat decline triggers review, with no invented 5% allowance.
- Absolute operational counters retain availability/value/unit/source/reason and all-three medians only. A real zero counter remains a valid absolute observation; this differs correctly from an unusable zero ratio operand. These medians do not claim row-wise conservation or causal ACK accounting.
- Candidate identity is exact clean merged Dgraph bad4, authentic W8 published module with official sum/no replacement, fixed server hash and Go1.26.9. Accepted baseline uses manifest **v1**, its harness-only adaptation 7f2720b and explicit fc3c local replacement; its nominal July module version is not substituted for the executed Gomap source identity. Retained smoke manifests have the expected v1/v2 shape.
- Profiles intentionally have different timed operation counts and attached profile paths; the parser does not apply the 2,000-operation decision-row requirement to them. Their supplied objects remain descriptive. It does not fabricate `context.excluded=true`.
- The amendment must carry the exact STOP lock; process receipt must remain original STOP. Summary has `overall_pass:null`, `performance_qualification:null` and whole-campaign STOP regardless of later arm outcomes. No runtime/quiet waiver or historical NULL is relabeled PASS.

## Usage and limits

The documented command points to the exact three expected evidence directory layouts and uses a fresh output prefix. Both output files must be absent, and no outputs are opened until every arm has passed the source/completeness checks. Missing arms must remain unexecuted preparation; do not run this script early to obtain a partial decision.

This reader is expressly supplementary: it checks recorded identity/witness fields and hashes, but does not rehash runtime sidecars, re-execute resource-witness source authentication, inspect remote executables or authenticate builders. Existing maintained verifier receipts/original reports and independent semantic audits remain prerequisites. It verifies logical parity **within each arm**; it does not additionally assert one identical checksum/count across all three arms. Review final cross-source parity from retained row objects rather than claim that absent assertion exists.

Known observability limits stay explicit: mixed-operation tails, Alpha-only CPU/HWM and postings-only disk are not whole-system/steady-state measurements; aligned recovery is clean Alpha restart with Zero alive. Missing ACK-phase conservation, physical I/O, equivalent write amplification, allocation and live-debt metrics remain unavailable. No full parent completion follows.

Output creation is exclusive, not a two-file atomic transaction: an I/O/serialization/render failure could leave a new partial output file. Such output is not a successful analysis and should be preserved as failed evidence with its error; do not reuse the occupied prefix or treat a JSON file's mere existence as completion. This does not affect valid known result formats or authorize rerunning measurement.

Only source/artifact reads and this review artifact write were performed. No analysis execution, tests, subprocess workload, source/state mutation, SSH, GitHub mutation or new gate was introduced.

