# Pre-row matrix comparison mapping

Status: proposed explicit application of the existing accepted rules, prepared before matrix collection. This artifact changes no workload, threshold, source, or observer. Root retains acceptance authority.

## Frozen arms and source identity

| Arm | Rows | Zero/Alpha survival | Durable TreeDB owner | Role |
|---|---:|---|---|---|
| C-P | 12 | process | store_wal | merged candidate, process profile |
| C-F | 12 | filesystem | raft_checkpoint | merged candidate, filesystem profile |
| B-F | 12 | filesystem | store_wal | accepted original ordinary-COW baseline |

Each arm retains the maintained order: relaxed Badger r1–r3, relaxed TreeDB r1–r3, durable Badger r1–r3, durable TreeDB r1–r3. Dataset500/warmup100/timed2000/concurrency4/seed20260711 and all other accepted settings remain unchanged. Profiles after these rows are excluded diagnostics.

The policy label describes the **durable** TreeDB cell. `runtimeSelectors` and `runtimeConfig` keep relaxed TreeDB on store_wal even in C-F. Both processes must independently prove the arm's resolved HardSync survival setting; TreeDB must prove cow_ordered and the actual posting route. A CLI label alone is insufficient.

Candidate manifest: Dgraph bad4ced2c88e15076f73e3647a72e965f63be2b9, official W8 Gomap v0.6.2-0.20261023204715-29a2fdf153ce, no replacement, manifest v2. Executed server SHA256 46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175. Merged Gomap f906b8a2aa1279ed6b6627028cba71d491db04c3 is ancestry/runtime-equality evidence, not the binary's claimed module version.

Baseline manifest v1: accepted harness-only Dgraph adaptation 7f2720bf0d7ed90e82b04c52911bff92e9f22726 (base b87e967bc3c18d4c609e652d79c7ccb8e94ace1b, tree 0ba5d97bc1d9c71f37b04c8e4b067ed6aee540e2), explicit local Gomap fc3c3db97ec2954d47a1adf731d501e7b73f181f replacement. Its go.mod nominal July version is **not** the executed Gomap identity. Server SHA256 58075e248543eb56ec618fe4099c04fd7fc741817558765e83e98af6432960a8. Do not apply v2's no-replacement rule retroactively to this accepted v1 baseline.

## Exact comparisons

1. **Within each arm, unchanged report gate:** separately for relaxed and durable, median(TreeDB throughput r1–r3) / median(Badger throughput r1–r3) >= 0.97. Do not pair individual cross-backend throughput ratios and substitute their median for this existing ratio of medians. Preserve each original report verdict, including B-F's own verdict; a shell exit0 does not establish ADVANCE. Badger controls must match the same survival and durability class. Never compare C-P TreeDB against filesystem Badger.
2. **Original-source regression comparison:** C-F TreeDB(class,r) pairs with B-F TreeDB(same class,r), r=1,2,3. Run the same comparison for C-F Badger versus B-F Badger as the retained control. This compares the selected end-to-end filesystem implementations; durable TreeDB deliberately changes recovery owner. It cannot isolate the cost of that owner from every other source change. Badger drift cannot be divided out to excuse a TreeDB regression.
3. **Ancillary statistic for each matched filesystem cell:** for each available positive lower-is-better metric, retain all three `100*(C-F[r]/B-F[r]-1)` values and their median. Apply the already selected >5% increase => FAIL/review-needed/block promotion rule to p95/p99, Alpha CPU, Alpha RSS/HWM, logical and allocated postings disk, and each aligned recovery phase. This is a review trigger, **not** permission to accept smaller systematic regressions. Zero/missing denominators or unavailable observations remain absolute-valued UNQUALIFIED/review-needed; do not discard one repeat or impute zero. Report summaries' independent medians are not this paired statistic.
4. **No original process anchor exists.** C-P has within-arm parity/correctness/runtime gates but no original-source paired regression qualification. C-P vs C-F is descriptive only: survival and recovery policy both differ. Do not apply the filesystem >5% rule to a process/filesystem contrast or label it a controlled policy-only effect. No new process baseline is authorized by this mapping.
5. Preserve before/after throughput and all controls under the original no-accepted-material-regression condition. There is no newly authorized 5% throughput-decline allowance. The 0.97 gate is cross-backend parity, not an exemption from source-regression review. Retain all36 rows/nulls/failures; no replacement or unchanged retry.

## Observable boundaries and unavailable claims

- Tails are aggregate mixed-operation p95/p99, not per-operation tails. CPU is Alpha's timed `/proc` delta; RSS is Alpha VmHWM including setup, not timed live RSS, total Zero+Alpha memory, or steady-state slope. Disk is postings directory logical/allocated bytes, not complete Raft/Zero/temp storage. Read both disk measures because sparse preallocation differs.
- Compare `recovery_read_seconds` and `recovery_read_write_seconds`, from the same Alpha exec through validated full read, then new CommitNow ACK plus fresh read. Legacy HTTP-health `recovery_seconds` is retained descriptively and must not substitute for aligned phases. These are clean Alpha restarts with Zero alive, not crash or power-loss latency.
- Whole-system CPU, per-operation allocation B/op, physical I/O wait/bytes for TreeDB, equivalent cross-backend write amplification, live Raft debt, and complete ACK-phase/conservation timing are unavailable. Conditional logical-byte counters, semantic flush/checkpoint counters, and Raft-owned command-WAL grouping metrics must retain their actual availability/reason. Missing is not zero. GC counts have differing backend definitions and cannot establish equal maintenance work or equilibrium.
- No full physical-equilibrium, sustained RSS bound, whole Dgraph power-loss chain, or guarded parent capability conclusion follows from36 short rows. The guarded benchmark_minimal stage and open parent obligations remain distinct.

## Observer and disposition

The execution plan's explicit user physical-host quiet waiver supersedes the older v2 plan's admission-stop rule only at that scope. Preserve the original quiet acquisition, observed noise/NULL truth, load/construction checks, and numeric/correctness gates. The separate M0 CV waiver does not waive any matrix numeric rule. No pooling across arms/classes/backends, selecting repeats, profile substitution, or automatic performance waiver is authorized.

## Source bindings

- EXECUTION_PLAN.json SHA256 7851c3e454b6aea893b87702c21cf83027627a81aa58af90b0b5e8270eadabfd.
- state/final-qualification-next-proof-v2.md SHA256 3206d10bb49b19316624be664b22f70cf6981b8cc535fda0f55154705ce9baf5 (row pairing and numerical rules).
- worker/treedb/livebench/report.go SHA256 1019cb7118add7fed3d610d7c8c174a570912b6192d738800a30c2f3e32f7a13, lines116–145,153–218,237–239.
- worker/treedb/livebench/cmd/runtime.go SHA256 a5aefe631c40c12daf6baf6e2974bf5edcbeafb6da4326eb55fbfd0b4e6f9765, runtimeSelectors/runtimeConfig and runtime witness validation.
- worker/treedb/livebench/cmd/main.go SHA256 1ab78fba06505a6d984c108bab2d808386895472e0e05c204a9b5e4f084d0459, metrics/recovery boundaries.
- worker/treedb/run_durability_ab.sh SHA256 e1faba06e36b9916bb71343fbc858c783c9cd5d3dab7a9d338b3485bf183e95b.
- smoke-evidence/{candidate-source-manifest.json,baseline-source-manifest.json,executed-source-manifest.json} supply exact executed source/module/binary identities above.

Read-only source inspection plus this artifact write only; no matrix, runtime, SSH, GitHub mutation, source edit, or state-graph change performed.
