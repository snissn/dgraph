# Independent merged schema evidence audit

**ACCEPT for the bounded schema/public-ACK/pending-recovery diagnostic.** No material contradiction or missing required semantic witness found. This is artifact-only review: no runtime, SSH, tests, GitHub mutation or source edits. It does not certify performance, physical power-loss safety, resource equilibrium or completion of the parent product gates.

## Exact source and binary binding

`result.json` reports success=true, diagnostic_only=true, policy=raft_checkpoint, and no error. The JSON portion of retained SCHEMA.stdout equals result.json; SCHEMA.stderr and all nine companion stderr files are empty. Root reported command exit0; this audit does not independently reconstruct the remote shell exit status from an absent process receipt.

Before/after source snapshots are identical and clean: Dgraph **bad4ced2c88e15076f73e3647a72e965f63be2b9** (merged), Gomap **29a2fdf153ce6e214abf9502d7970de727a38967** (the actual published dependency checkout). Do not describe the latter witness as merged Gomap f906b8a2: the executable correctly pins published W8, not a local replace. Both buildinfos say Go1.26.9, Dgraph vcs.revision=bad4ced2..., vcs.modified=false. Server buildinfo pins Gomap v0.6.2-0.20261023204715-29a2fdf153ce with h1:Y85xjKKdrqj5UXLCzYCuVzYXbk0oDGf8zVBwqowuxrs= and no replacement. The executed v2 source manifest independently agrees on server hash, source revision and module identity.

- Actual smoke-rebuilt server SHA256: `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175`.
- Schema companion SHA256: `687d5445ade522289cd1aef2941013d3bd95a97e1ba47b07f861c99f8ab141e8`.
- Server buildinfo SHA256: `97df60a70c282696b4a66bad34cdba46676540e9084412773883aa8b31ea1ed0`.
- Companion buildinfo SHA256: `573cb597c22cf78fb54f35185e4cf1d228d929ee9553cbcc02a84f6d1786a0b1`.

The two retained buildinfo files hash exactly to result.json. The server hash also equals SMOKE_ROOT_ACCEPTANCE and the executed/candidate v2 manifests. The companion binary itself is remote, so its content hash is the driver's retained receipt, corroborated here by its buildinfo; no new binary transfer or execution was performed.

## Nine phases and four actual ACK cuts

All first events have the exact expected phase/event and success=true. Four ACK phases retain the small response event AND a separate complete transcript, not only a summary. There are 84 recorded RPCs in total, including the expected negative cases.

| Phase | First event | Recorded RPCs |
| --- | --- | ---: |
| seed | phase_done | 3 |
| count-cut | ack_cut | 10 |
| count-check | phase_done | 19 |
| conversion-cut | ack_cut | 7 |
| conversion-check | ack_cut | 10 |
| conversion-final | phase_done | 7 |
| unique-prepare | alpha_restart | 12 |
| unique-decide | ack_cut | 4 |
| final | phase_done | 12 |

Four response-to-kill intervals, in phase order: count-cut 235722ns; conversion-cut 298273ns; conversion-check 180031ns; unique-decide 342434ns. All are nonnegative. These are controller observation intervals around the existing immediate Alpha+Zero SIGKILL path, not a newly accepted latency threshold or a syscall-level simultaneous-kill guarantee.

Decoded RPC responses support the asserted semantics: forward/reverse count disable/re-enable; UID posting history at seed timestamp4 (both original edges versus a later one-edge read); invalid multi-schema conversion refusal with original count/reverse and proof_bad:string intact; populated [string]→[int] collision/dedup/count/index and targeted delete/reinsert; string→list conversion and scalar integer presentation; duplicate-data Unique-enable refusal, then successful enable after correction, same-subject update and targeted duplicate refusal. Expected RPC errors are explicit negative controls, not hidden phase failures.

Unique pending starts30016/30017 retain a shared actual Zero conflict key `2h91fr2gaxjh8`. Alpha4 closes successfully with `certified: index=78 oracle=30017 pending=2`; Alpha5 reuses the same Zero PID3260054/generation4. After reopening, CommitOrAbort commits30016 at30019 and aborts30017; the query shows only pending UID0x2711 for gamma. The final stable-ack CommitNow has start30021/commit30022 and UID0x2713, and its value is present after the final all-process crash/reopen. Final reads also retain counts, converted values and alpha/beta/gamma ownership.

## Runtime and pre-startup recovery witnesses

All six raw /debug/store sidecars hash exactly to their result.json store_sha256 and show `treedb.cache.memtable_mode:cow_ordered`, backend=treedb/profile=raft_checkpoint/tier=benchmark_minimal/post_commit_events=true. A legacy point_successor counter is not an activation requirement. durable_commits=false is the intended posting-store option for Raft-owned durability, not evidence of missing HardSync.

Each of the six Alpha and five Zero logs has exactly one resolved x.WorkerConfig line with HardSync:true and the recorded PID. All twelve per-health log hashes (including the reused Zero at two health moments) exactly match a retained byte prefix of the corresponding growing final log. Thus later appends do not falsely appear as identity mismatches.

Each restart has exactly one pre-startup Found hardstate; raw line order is certificate recovery, Found hardstate, committed replay before health. The independently parsed integers equal result.json:

| Alpha generation | Certified | Pre-startup durable commit | Replayed before health | Durable suffix beyond certificate |
| ---: | ---: | ---: | ---: | --- |
| 2 | 1 | 27 | 29 | yes |
| 3 | 1 | 47 | 49 | yes |
| 4 | 1 | 55 | 57 | yes |
| 5 | 78 | 78 | 80 | no |
| 6 | 78 | 83 | 85 | yes |

Every row satisfies certified<=preCommit<=replayed. Generations2,3,4,6 prove actual preexisting durable suffix beyond the selected certificate; generation5 correctly reports no preexisting suffix (78==78), despite later startup entries advancing replay to80. In particular the final ACKed suffix survives selection of certificate78 and replay through85. No post-election commit target was substituted for the recorded pre-startup durable commit.

## Finish and limits

Alpha4 and final Alpha6 have explicit Server shutdown. Bye! lines; final Alpha6 certifies index86/pending0. Final Zero5 records Raft WAL closed with err:<nil> and All done. Goodbye! The controller result and complete transcripts show bounded successful finish; the packet does not include an independent final /proc/port-absence snapshot, so this note does not claim that additional process-inventory proof. Root should retain its sequential-launch cleanup checks separately.

Preserved limits: process SIGKILL is not power loss; no deterministic public delayed frontend/schema-admission race; small root-namespace fixtures do not force cold splits or bound scratch/resource cost; historical raw API evidence covers UID posting data, not historical schema definitions. The guarded benchmark_minimal capability matrix remains operative. No timing or full qualification claim follows from this success.

## Input integrity

SCHEMA_EVIDENCE.tar has 39 regular files, 2211840 bytes, SHA256 `eebd11e2f510e4928f67a337a309f6ba2fd2d82907f45305054f527859e43040`; every extracted file is byte-identical to its archived member. Selected exact input hashes:

- `SEMANTIC_ACCEPTANCE.md`: `f73971c1eedeb11169456abd14b58458c5e874f6f09972ee4cb9bd1aa1d6c519`.
- `SCHEMA_ARCHIVE_RECEIPT.json`: `ec2bbf58e41a5e76b04ce262f98cd5261cf949f3e9fc41fe7d438f235fca5e8e`.
- `SCHEMA_EVIDENCE.tar`: `eebd11e2f510e4928f67a337a309f6ba2fd2d82907f45305054f527859e43040`.
- `SCHEMA.stdout`: `bbc312cad084c844b126a36d1c683a1dfe1f46010300113f7704082c9bb66b64`.
- `SCHEMA.stderr`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- `smoke-evidence/executed-source-manifest.json`: `008eda694c391c88f23af0cad4b06ac145668a38eec7affc27378ea0813c518c`.
- `SMOKE_ROOT_ACCEPTANCE.json`: `f374b0330d9539054bd3022ecba373a214563c8b73f9f25ea9f9d0c137607d23`.
- `schema-evidence/schema-ack-suffix/result.json`: `e01aaa936a6b2b93b9bd9c7e52e3661214689eab992fc497a4083dc624ae1955`.
- `schema-evidence/schema-ack-suffix/state.json`: `64af87286bd75d25da4a9bc6c8457fb823cddd32564d1e2e94d7573302c60b20`.

Complete schema-file bindings (relative to schema-evidence/schema-ack-suffix):

```text
dc17a274c5381e12bdd160ad64cadf1b4c849f75f47674a716cf6210f450430c  alpha-1.log
8c26e303bba5877ed9f07585d2e62084d4d78d19acbe25244c12cc2756420afb  alpha-2.log
e822e606ee5215a699bbd9e20ae0ec2eee0521d3474d99d017c6b81cf8f43661  alpha-3.log
567ec8386946f784f2c4ef67c41550fa19b7026b15eb8a4d9f4f4d9ce7a57ca9  alpha-4.log
a27457f96436a081de4fc3b5fa974aa034b6fff7170d910e1b29fd7ce0df025b  alpha-5.log
4f953bc34528dab83e3d0f4b3f5153e46cbbbca387227bcb0f89ae132d7f9e40  alpha-6.log
b4f7611c0bf0408687dd4b1f2a1c85f29b6514cbf0bcf774e61b8b1904eefcbe  conversion-check.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  conversion-check.stderr
e64ace4565eaaa30e0daf9971559e099481c93f9c4fe1cd0632b61dccc075a7a  conversion-cut.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  conversion-cut.stderr
8a0d7c3b00c5589685f4b55d55c1a8b533f2dbbb2fba7be2cb2500c059e0a174  conversion-final.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  conversion-final.stderr
48f616b8f14eb12bf09909b548f09a651fc2bbebd2ea6ba326002f7e41424547  count-check.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  count-check.stderr
9df8d2818259bd04ce71d355e5fe7b992d1d160670b5664b31211617aee8c8e8  count-cut.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  count-cut.stderr
97df60a70c282696b4a66bad34cdba46676540e9084412773883aa8b31ea1ed0  dgraph.buildinfo.txt
dc1ecc4e5aed9e3d1585bc81480d144192faa6273b171f14ede4a0ee1d8c7c2b  final.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  final.stderr
573cb597c22cf78fb54f35185e4cf1d228d929ee9553cbcc02a84f6d1786a0b1  proof.buildinfo.txt
e01aaa936a6b2b93b9bd9c7e52e3661214689eab992fc497a4083dc624ae1955  result.json
2dc007f29dc2cf7687cbcb9f95997dbe799c3becfcb2ba0ada54d61344d7ef9f  runtime-alpha-1.store.txt
185d28be60b6d3e459ace541a026e4c22422c28c4c3e8bd7591b50b605a0f1f6  runtime-alpha-2.store.txt
ff6ec2b02461ce304abda243f207bff2692c611f7f2be26feef806f7ae93ca08  runtime-alpha-3.store.txt
5e453f481c79ca7e39f5cc079f7582fd23d727f397b9eee6588878e4a5763c07  runtime-alpha-4.store.txt
1ba27adcaaca25792f1a7c5b60422720abd56af2e28d8f75bafa3772bc9f3a8c  runtime-alpha-5.store.txt
cee3b6934e6558847cec05d100392e1f66e841b5d290beb30912ef7bc7768299  runtime-alpha-6.store.txt
71abcf447139b7e4ca583b2eaba42fff6c385d55a8346ce6d653ff2cb2a9a996  seed.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  seed.stderr
64af87286bd75d25da4a9bc6c8457fb823cddd32564d1e2e94d7573302c60b20  state.json
8d19c2e07281803f7593b644e27a2989182cc9fd38cc79f391a1b963833418c3  unique-decide.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  unique-decide.stderr
379ffa641c84ca2b409944a64ff4cc8ef127e6b4ff14f9395ab2acd947c8f423  unique-prepare.jsonl
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  unique-prepare.stderr
1f83b4ca9e47bd5063e86ab2039b5a9677b65348b3dfcdb06898942945688414  zero-1.log
b0406e44f152055deafbbda3eb7a93eec757e0462236ad0542eec86987822d06  zero-2.log
6da843fc1890e22c782a5697a92aafe6bf881bca6ffef59c07274a6c3e4f1a83  zero-3.log
d62c27162109d0fd8e1cc5a62aacb00dacb8b06be7b4b723a759dd51d89fb84b  zero-4.log
60fd89689002db193e4dd39a4dbef04e258d431b39f6718e9c75200bc8f50545  zero-5.log
```
