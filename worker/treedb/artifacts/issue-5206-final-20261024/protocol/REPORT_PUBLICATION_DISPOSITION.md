# #5206 evidence publication and terminal disposition

Status: source/policy review only; no publication or issue-state change authorized by this receipt. The 36 decision rows and six excluded profiles are not yet all available to this reviewer. This is a publication recommendation, not an acceptance of the unfinished final packet.

## Decision

The smallest sufficient publication is one reviewed, committed, durably pushed **evidence-only issue branch**, linked by full commit SHA from #5206. No inspected repository instruction requires manufacturing a docs-only PR merely to archive evidence. A PR is required if the coordinator chooses to integrate the report into main under the usual contribution/review gates, or if an actual harness/report-code repair is needed. Such a repair must ship its relevant tests/docs in the same PR; an archival branch must not smuggle a code repair past those gates.

Recommended location: Dgraph `worker/treedb/artifacts/issue-5206/`, on a fresh issue branch from the measured merged consumer `bad4ced2c88e15076f73e3647a72e965f63be2b9`. This follows the existing issue-29 normalized terminal-report layout and keeps the reproduction command next to its maintained Dgraph reporter. Link this exact commit from Gomap #5206. A Gomap evidence directory is also policy-compatible, but duplicating the complete archive across both repositories adds no authority. The report commit is a **documentation descendant**, never a newly measured runtime. Keep the measured Dgraph/Gomap identities, module/binary identities and baseline adaptation unchanged in every row and report.

**#5206 can eventually close as a completed STOP evidence deliverable**, with coordinator acceptance, despite explicit UNQUALIFIED axes. Its own failure-action clause expressly permits this. That does not close the parent performance objective, establish successful qualification, make unchecked acceptance items pass, enable guarded features, or change the Badger default. It cannot close merely because implementation PRs merged or an incomplete report exists.

## Contract basis and policy inspected

The preserved #5206 body distinguishes its deliverable from promotion: “one authoritative normalized evidence report and promotion/STOP decision, committed with any necessary harness/report fixes under repo PR policy.” Its failure-action section says: “A gate failure records a measured STOP, exact failed rows, current owner, active causal layer and smallest next inquiry. It can complete this evidence deliverable while leaving the parent performance objective open.” It also requires durable repo-approved links, complete samples, commands/environment, definitions, nulls and remaining gaps; applicable PR/review/CI, not an unconditional evidence-only PR requirement.

Policy checked at local Dgraph `2d6bee5d616327a827924fd52a035101f0784cb8` and Gomap `7fd5943a9b84119f3cf26e433e53bd85205911f1`:

- Dgraph `CONTRIBUTING.md:183-189` describes PRs against main and requirements for code/features; `:200-206` requires formatting for contributions. It does not require every durable issue artifact to merge to main. There is no tracked Dgraph root or `worker/treedb` nested `AGENTS.md`.
- Gomap `CONTRIBUTING.md:43-50` requires reviewed inventory refresh when CI/source footprints change; `:54-62` requires reviewable changes and semantic contract/tests. Neither mandates a PR just for an immutable evidence branch.
- Gomap root `AGENTS.md` and nested `TreeDB/AGENTS.md` govern storage/durability changes and profiling contracts. Pure archival evidence changes none of those runtime contracts. `HashDB/AGENTS.md` is outside the proposed path.
- Existing main-tracked Dgraph `worker/treedb/artifacts/issue-29/README.md:3-19` contains a normalized committed report, raw live JSON, context and separately excluded profile metadata; `:21-28` states STOP/default unchanged. `:74-95` preserves report reproduction and distinguishes external immutable originals. This is a layout/clarity precedent, not proof that every future evidence branch must merge.

No effective GitHub branch policy is asserted from these local files. Any later PR/main publication still needs the actual current-head review/check policy and coordinator approval. This source-only task performed no GitHub mutation or protection refresh.

## Minimal complete archive after all arms finish

1. `README.md` with terminal **STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE**, explicit coordinator report disposition, every unmet axis, linked existing owner/next inquiry, and unchanged default. Preserve the prospective diagnostic-continuation amendment: the final 24 decision rows were additional diagnostics after original process STOP, not a restarted successful campaign.
2. The 36 original decision JSON files in separate `process/`, `filesystem/`, `baseline-filesystem/` arm directories; all six profile-result JSON in separately named excluded `profiles/` directories. Preserve bytes and names/relative-path mapping with SHA256 and byte length. Retain all original failure/null/preflight receipts; no selective omission or rewritten result fields.
3. Original three maintained per-arm reports, supplemental normalized cross-source analysis, sample table and gate table. Archive the exact analyzer source/version/hash and its parser-correction receipt if used. Its cross-source report supplements, rather than replaces, the maintained report. Include all three raw paired observations and median; keep cross-Badger ratio-of-medians >=0.97 independent of source-pair ancillary >5% review rules. No pooling the six profiles into 36 rows.
4. Exact expanded commands, environment/toolchain/storage/runtime settings, fingerprints, arm order, original frozen comparison mapping, prospective amendment and applicable explicit user overrides with their narrow scope. No inference that host-quiet waiver waived numeric performance, missing metrics, M0 versus Dgraph criteria, or full production gates.
5. Per-arm source manifests/build metadata, executable hashes, module authentication, log/sidecar identity receipts and selected-file inventories. Preserve candidate v2 published-module/noReplace authority versus baseline v1 explicit accepted replacement. Bind actual measured Dgraph `bad4ced2c88e15076f73e3647a72e965f63be2b9`, merged Gomap `f906b8a2aa1279ed6b6627028cba71d491db04c3`, executed W8 module commit `29a2fdf153ce6e214abf9502d7970de727a38967`, and baseline adaptation `7f2720bf0d7ed90e82b04c52911bff92e9f22726` / Gomap `fc3c3db97ec2954d47a1adf731d501e7b73f181f`. A report descendant does not alter these measured identities.
6. Full raw logs, observers and binary profiles can remain in a durable public repository release/archive if too large for Git, with immutable URL, checksum, size, inventory and custody/retention receipt in the branch. Local absolute paths alone are not public durable evidence. Never publish credentials/private runner material. The normalized JSON and source-hash receipts should themselves be committed as requested, not only listed in an ephemeral CI artifact.

Review the completed archive for exact 36+6 counts, hashes and reproducibility before coordinator acceptance. Do not rerun workloads merely to produce a cleaner report. A report-only commit does not invalidate its fixed measured-source evidence; it also cannot inherit performance claims for changed runtime bytes.

## Completion boundary

The existing process arm already establishes a real original-gate STOP (durable TreeDB 337.2918 vs Badger 837.2059 ops/s; relaxed 9564.2564 vs 9938.1542). Remaining arms may identify narrower capability/architecture gains, but cannot overwrite this campaign disposition. Final numbers must come from complete accepted archives, not this interim summary.

Unavailable per-operation tails, full ACK/store-boundary attribution, CPU/allocation or physical-write accounting, complete Raft footprint, sustained default maintenance equilibrium and any unproved exact-candidate RPC/power-loss chain remain **UNQUALIFIED** where unavailable. Existing component stable-image/crash tests may be cited at their exact scope; process kill/restart, survival labels, short filesystem rows and throughput do not substitute for those missing claims. A truthful STOP report may state these gaps without first implementing every missing fixture, because #5206 expressly allows terminal failure. It must not mark the corresponding success checklist items completed or claim no regressions in unmeasured quantities.

Closure is appropriate only after all requested 36+6 artifacts and required semantic/report audits are archived, the coordinator accepts this as the terminal evidence deliverable, and existing parent owners explicitly retain the uncompleted product/qualification work. Keep Gomap #5207 and Dgraph #34 open; leave defaults and guards unchanged. If the coordinator instead elects to keep #5206 as the active owner for future successful qualification, retain it open with the same STOP record. That is a tracking choice, not a contract forcing the already terminal evidence deliverable to remain unfinishable.

## Source bindings

SHA256 of inspected policy/snapshot files:

```text
320c5ed8a179e15886cd58832f76d7906f5d76c189500e756f0adb957d6ecf99  gomap/AGENTS.md
1cb6426fe7a764a79f8e97f7e164669232c350fb8ce2e7850558e0b7f42bb854  gomap/TreeDB/AGENTS.md
7842035e4e975b6010532c52ebf55a562f92a5d379f14fa0f2a0de91c5744b9b  gomap/CONTRIBUTING.md
f7bd19971c8910ce18cc8792bcd74879978126afc5392d9a5af37d15c6da00c9  dgraph/CONTRIBUTING.md
fa37a12164646269bee9421352a6cc2283200b98bfd578015f3347e44a8d09c3  artifacts/approved-merge-01/graph-disposition-snapshots/issues.json
```

The local issue snapshot reports #5206 open, updated 2026-10-23T17:16:37Z. This report analyzes that preserved accepted contract; it is not a fresh live issue-state claim. No source, state graph, runtime, SSH or GitHub change was made.
