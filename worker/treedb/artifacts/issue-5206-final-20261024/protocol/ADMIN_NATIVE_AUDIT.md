# Independent admin and native peer evidence audit

**ACCEPT for both bounded merged-runtime correctness diagnostics.** No material contradiction found in the retained results, actual server logs, native install ordering, or binary identity. Artifact-only audit: no SSH, tests, runtime jobs, source/state/GitHub edits, or new measurements. This is not full product/performance/resource/power-loss qualification.

## Identity and evidence integrity

Both result.json files have success=true, diagnostic_only=true and no error. Each equals the JSON retained in its corresponding ADMIN.stdout/NATIVE.stdout; both stderr files are empty. Root reported exit0. All launched commands use the same actual smoke-rebuilt Dgraph path and --survive=filesystem, with TreeDB benchmark_minimal/raft_checkpoint/events=true on Alphas. Native explicitly keeps Badger syncwrites=false, which is not the Raft-WAL HardSync policy.

Both recorded binary hashes are `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175`, matching the independently verified executed-source-manifest v2, smoke acceptance and schema audit. That manifest binds clean merged Dgraph **bad4ced2c88e15076f73e3647a72e965f63be2b9**, Go1.26.9, authentic published Gomap **29a2fdf153ce6e214abf9502d7970de727a38967** / v0.6.2-0.20261023204715-29a2fdf153ce / h1:Y85xjKKdrqj5UXLCzYCuVzYXbk0oDGf8zVBwqowuxrs=, and no replacement. The dependency witness is the published W8 checkout, not a claim that the binary imports merged Gomap f906 itself.

All six admin role logs and nine native role logs contain exactly one resolved x.WorkerConfig/HardSync:true each. No panic/fatal/QUIT diagnostic was found. Unlike the schema driver, these two results do not emit their own before/after source snapshots or /debug/store COW sidecars: binary identity links them to the prior exact-server COW witness; independent per-launch raw COW activation is not invented here.

Admin archive is 71680 bytes, SHA256 `d0f806c19fa02283564a01df072281f979668cbe17bba4728a822d0728b5463a`; all7 regular files match their extracted copies. Native archive is 133120 bytes, SHA256 `ec35dcb546c4f5e9ea2a43969136be7355146d6b5f815578f3fcd7c2badeac91`; all10 regular files match extraction. Archive sizes/hashes equal the original receipts.

## Admin/drop/outage/close

The retained responses show a pure pending-transactions drop refusal, successful DropData/predicate-drop/DropAll ACKs, and a fixed-timestamp10005 read of admin_value=after-drop while Zero is absent. The existing driver's exact sequence sends SIGKILL to Alpha and Zero directly after the DropAll ACK before result bookkeeping, reopens, verifies removed schema, recreates it and writes new data.

The important log witness omitted by the driver's automatic result validation is present: **alpha-after-drop.log:105 certifies index51/oracle10006/pending1**, followed by successful Server shutdown. Bye! at line115; result orderly_close_exit_code=0. This happens after the driver kills and waits for Zero, and before it restarts Zero and Alpha. Thus this is genuine pending-state close with Zero offline, rather than a graceful close after resolving all pending work.

On reopening, alpha-after-close.log recovers certificate51, has pre-startup durable commit51 and replays through53 before ready. The retained restored_pending_decision explicitly aborts the old pending transaction after Zero restart. Do not claim old pending COMMIT survived a Zero restart. The existing success path also verifies outage-first absent, after-drop retained, and a fresh after-close mutation readable. Final Alpha closes at certificate58/pending0; final Zero closes its Raft WAL with err:<nil> and says Goodbye.

Admin historical suffix after DropAll is real: alpha-after-drop.log records certificate1 <= durableCommit42 <= replay44 before ready. The pending close/reopen itself correctly has no preexisting suffix above certificate51.

## Native transfer authority and ordering

Tuples below are **index / source / receiver / pending / checksum_sequence / terminal**. All tuples match both result.json and the raw Native checkpoint installed log exactly. Both initial joins carry pending1 together with positive sequenced checksum authority and retained terminal1 in the same transferred cut.

| Log | Actual tuple | Install < replay barrier < ready line numbers |
| --- | --- | --- |
| alpha2-join.log | 19 / 1 / 2 / 1 / 17 / 1 | 69 < 70 < 71 |
| alpha3-join.log | 24 / 1 / 3 / 1 / 18 / 1 | 70 < 71 < 72 |
| alpha2-rejoin.log | 43 / 1 / 2 / 0 / 22 / 0 | 68 < 69 < 70 |

Replica2 rejoin **43 > 19** is independently confirmed; the script itself does not enforce this strict increase. Replica3 initially installs24. Rejoin legitimately has pending0/terminal0 after resolution and S advancement to22; do not require it to retain the initial pending/terminal counts. It does preserve checksum_sequence22. All three transfer paths publish install completion before committed replay barrier and only then health.

The native proof starts pending txn5, advances replicated floor3, creates resolved transaction8/commit9 above that floor, transfers both pending and terminal state to peers2/3, then commits txn5 through replica3 at commit18. The driver verifies the pending value on all three replicas. With replica2 offline, the leader writes after-offline and advances floor22; replica2's newer install43 catches it up.

The actual immediate-ack response is CommitNow start27/commit28, UID0x5. The existing source sends adjacent SIGKILL calls to all three Alphas then Zero, with no intervening network/filesystem operation, before recording the response. It then restarts all four processes and checks **seed, resolved, pending, after-offline, immediate-ack on each of the three replicas (15 asserted reads)** before setting success=true.

The driver does not retain those15 query bodies/responses as separate transcripts; this claim is supported by its exact success path plus retained result/logs, not by a fabricated standalone response archive. Reviewed script hashes equal the already accepted SEMANTIC_ACCEPTANCE bindings: admin `74b6978a0160b83ac0de67aac193d35811d1676cb43c68acb1147b372ec09b51`, native `d819e56e3c77f7bcef05e8cb767b85bd4b11612cd23efcd29506866b58a33fa9`. No new remote source acquisition was performed by this audit.

All restarted replicas select certificate43 and possess a pre-startup durable suffix through49, covering the acknowledged mutation; the raw logs prove:

| Replica | Certified | Pre-startup durable commit | Replayed before health |
| ---: | ---: | ---: | ---: |
| 1 | 43 | 49 | 52 |
| 2 | 43 | 49 | 53 |
| 3 | 43 | 49 | 53 |

This distinguishes durable pre-crash Raft state from newer election/startup progress. Each final Alpha closes at certificate68/pending0/semantic_floor22; all three logs end Server shutdown. Bye! Final Zero ends clean Raft-WAL close/Goodbye.

## Boundaries retained

These are small actual-process correctness proofs, not throughput or equilibrium evidence. SIGKILL is not physical power loss. Native tuples do not certify all transfer rejection/interruption branches. Admin drop scope is the exercised root-namespace lifecycle; no broader namespace claim is inferred. No standalone final /proc/port-absence receipt is included in these two archives, and success is not a substitute for root's sequential cleanup checks. Physical-host quiet, full36row performance gates and parent operational/resource obligations remain separate.

## Exact input hashes

- `SEMANTIC_ACCEPTANCE.md`: `f73971c1eedeb11169456abd14b58458c5e874f6f09972ee4cb9bd1aa1d6c519`.
- `SCHEMA_AUDIT.md`: `e539503adcfb5a3dbe5f619b4cdc89642d49349abd61f7972c9bc3a3fde84e19`.
- `smoke-evidence/executed-source-manifest.json`: `008eda694c391c88f23af0cad4b06ac145668a38eec7affc27378ea0813c518c`.
- `ADMIN_ARCHIVE_RECEIPT.json`: `b4514ca6d458178295ffd9eb1294b3528a7dfbfefe61c51120281564433f5b91`.
- `NATIVE_ARCHIVE_RECEIPT.json`: `53a9d5c1354fae736cae5451eec08d512cb4eaf69eb17721251ab3ca54e16584`.
- `ADMIN_EVIDENCE.tar`: `d0f806c19fa02283564a01df072281f979668cbe17bba4728a822d0728b5463a`.
- `NATIVE_EVIDENCE.tar`: `ec35dcb546c4f5e9ea2a43969136be7355146d6b5f815578f3fcd7c2badeac91`.
- `ADMIN.stdout`: `db0c9623b454cf2614d47c233159d94cab19fc9aa1b870e4c1fe70b044954396`.
- `ADMIN.stderr`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- `NATIVE.stdout`: `f9eefa916f9646664cf6d87b88bbba8f4f868afb3b8036aaa6fc125ad9706acc`.
- `NATIVE.stderr`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- `launch_admin.sh`: `f884b8d7852a5f3d61827604ab34a97afec74e96e7c5c50bdcf9a5cbe27522c1`.
- `launch_native.sh`: `d2487c8d7d876a018636cf761726147628b1d9f9fedcc20391d62159071457b9`.

Complete retained per-packet file hashes:

```text
c2b9df0094547bd36aaf1924b2c181dede1c0b5c1b7aa3ab76e0694a739ebc6f  admin-evidence/admin-close/alpha-after-close.log
a818022578907c6ec8c94259495670e639176c772a2ab69caf7b96730890f226  admin-evidence/admin-close/alpha-after-drop.log
fb25c1c41e9b0a642b92a6cd7969062db5673c671ae431504c4778516f11d7a8  admin-evidence/admin-close/alpha-initial.log
db0c9623b454cf2614d47c233159d94cab19fc9aa1b870e4c1fe70b044954396  admin-evidence/admin-close/result.json
e610cf1b87e0685273f487942241629f1dd13c5315c137b18c945e2e716fed5b  admin-evidence/admin-close/zero-after-drop.log
e0a36a742d4da70eda7e09f601df8182ae4364ad34d7dc1799228a4be6c51046  admin-evidence/admin-close/zero-after-outage.log
5e7f5353299a6ef62bcbe33115ad96a0c35eb9efdf51a3a58cd55413dcb4564f  admin-evidence/admin-close/zero-initial.log
8a75e3f77093758d2bc012faf610c564e7554bbfba46ad7015ab982f2e65c07c  native-evidence/native-peers/alpha1-initial.log
336936e733d5dd217b42f7baef7fdac4d87edcf72fb631a6c79e78aec3b82e83  native-evidence/native-peers/alpha1-restart.log
31456fca0573a4c0de35529f0ec043f79e87a8b7f8392cfe94a431bad03d6016  native-evidence/native-peers/alpha2-join.log
179adc8ee1146a835df9c958305085aa9a8ea6706414580faeea340841c1b0fc  native-evidence/native-peers/alpha2-rejoin.log
97c19ad0864318e0cb6adc199fa3d41f2ab58ef6c5693c3f2f426b3357225b7d  native-evidence/native-peers/alpha2-restart.log
84a5151437af845791d6e6995a8080ef1ae452461b34e91b010ba5f26dcdec8f  native-evidence/native-peers/alpha3-join.log
b95a5890cf937c1dd6ba7d8ebd2179d2c3ad4c2c655b6639d93be9f9c1c75c4f  native-evidence/native-peers/alpha3-restart.log
f9eefa916f9646664cf6d87b88bbba8f4f868afb3b8036aaa6fc125ad9706acc  native-evidence/native-peers/result.json
8cd883f4bf6648f0d0b4dc9436c264eb8039d7295fb7701eaa54198af17626e6  native-evidence/native-peers/zero-initial.log
2b58f62323318add464451ad64c295217301b9a87628362d3c03b07f79b3951c  native-evidence/native-peers/zero-restart.log
```
