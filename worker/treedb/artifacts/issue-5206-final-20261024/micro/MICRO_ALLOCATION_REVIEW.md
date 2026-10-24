# Supplementary microbenchmark allocation review — actual baseline preview, archive binding pending

The actual completed baseline micro stdout supplied by the coordinator has now been compared with the retained filesystem candidate micro stdout. Its full-arm archive/source receipt remains pending; the coordinator must confirm byte identity with the eventual archived raw file. No historical timing was substituted. Whole-campaign `STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE` remains authoritative; this note adds no gate or acceptance verdict and does not substitute microbenchmarks for unavailable Dgraph operation allocations.

## Exact input and source equivalence

Candidate input: `filesystem-evidence/filesystem-12/microbench/raw.stdout` (identical `raw.txt`), SHA256 `0667f0c5ed6a9284550d675e017adc5c20ca404b27c65de9943216c69508c61a`. It contains 140 samples: **28 distinct cells, five samples each**, CPU suffix `-2`, Linux/amd64 Ryzen 7 9700X. Package PASS, 271.779 s. Context records the original command:

```sh
GOWORK=off go test ./posting -run '^$' -bench 'Benchmark(BadgerStoreSeam|TreeDBStoreAdapterOverhead|CommitEventDisabledAndUnsubscribed)$' -benchmem -benchtime 1s -count 5
```

Definitions were independently read with `git show` at original Dgraph `b87e967bc3c18d4c609e652d79c7ccb8e94ace1b` and merged candidate `bad4ced2c88e15076f73e3647a72e965f63be2b9`: all four files below are byte-identical. Accepted baseline patch `artifacts/final-baseline-prep-01/baseline.patch` changes none of them. This establishes shared name/fixture shape, not implementation identity: adapter/Gomap implementation differences are the comparison target. Accepted adapted baseline is `7f2720bf0d7ed90e82b04c52911bff92e9f22726` with Gomap `fc3c3db97ec2954d47a1adf731d501e7b73f181f`; the actual baseline micro preview is now available; its full-arm archive/source receipt remains pending.

| Shared file | SHA256 at both source revisions |
|---|---|
| `posting/treedb_store_bench_test.go` | `f209b121e0fb49196e4b8df99a628cdf45a07aa7cb619ffd6d77a1e9a3190405` |
| `posting/store_bench_test.go` | `e6f4255ebf0d07c3416bc202b9a56b5d9551e2f8a49dfcd9a6a2cc3fef2bfaea` |
| `posting/commit_events_bench_test.go` | `2afcdfa6c3f2cc4786b9aee782b374292e8b67c3edc564681d507b02b9991d63` |
| `posting/commit_events_test.go` | `b88ced4ccd52e342c4cf7c7db9c3753d957dd3f4d0923b2f8211ac0773d84169` |

The enclosing candidate v2 source manifest binds merged Dgraph `bad4ced2...`, published Gomap `v0.6.2-0.20261023204715-29a2fdf153ce`, Go 1.26.9, and server SHA256 `46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175`. That server identity is **not a separate hash of the transient `go test` microbenchmark binary**; this note does not invent such a receipt. See `filesystem-evidence/FILESYSTEM_ARM_AUDIT.md` for enclosing source/manifest verification.

## Shared benchmark mapping

Names below abbreviate only the common family prefix. The sample table retains every full cell name.

| Family / subcells | Fixed operation and shape | Relevant boundary |
|---|---|---|
| `BenchmarkTreeDBStoreAdapterOverhead/PointGet/{DirectMVCC,TreeDBStore}` | 256 keys, 128-byte values, one initial version at timestamp 1; cyclic point reads | Direct `GetAt` versus adapter read transaction and item callback; adapter transaction reused |
| `/AllVersionsScan/{DirectMVCC,TreeDBStore}` | Scan all 256 seeded keys each operation | New iterator per operation; adapter read transaction reused |
| `/RandomSeek/{DirectMVCC,TreeDBStore}` | Seek cyclically among 256 seeded keys | Iterator reused; direct loop also advances past the logical version group to match adapter semantics |
| `/WriteBatch{1,16}/{DirectMVCC,TreeDBStore}` | Distinct new keys each operation, 128-byte values, commit timestamp `i+1`; precomputed `b.N` batches | Direct relaxed `CommitAt` versus adapter transaction/SetEntry/CommitAt. Setup excluded by ResetTimer; StopTimer precedes Close/disk accounting |
| `/ExactKeyAllVersions/{DirectMVCC,TreeDBStore}` | Eight target versions, 32 prefix siblings at each version; consume exactly target history | New bounded/key iterator each operation; adapter read transaction reused |
| `/Reopen/{DirectMVCC,TreeDBStore}` | One seeded key; repeated Close/Open | Actual repeated storage lifecycle, not a read or write operation |
| `/Reopen/PairedLatency` | Same two separate databases; direct/adapter order alternates per iteration | One ns/op/bytes/op/allocs/op includes **both** reopens. Custom direct/adapter ns metrics are submeasurements, not independent allocation cells |
| `BenchmarkBadgerStoreSeam/point-read/{direct,seam}` | 64 seeded keys, four versions; cyclic one-key read | New transaction per operation, UserMeta read, then discard |
| `/bounded-iterator/{direct,seam}` | Full prefix of 64 seeded keys/four versions | New transaction/iterator, prefetch disabled, default visible-version semantics |
| `/exact-key-all-versions/{direct,seam}` | One selected key, all four versions | New transaction/key iterator; prefetch disabled |
| `/write/{direct,seam}` | Repeated cyclic 64-key updates, `benchmark-value` | New transaction/commit each operation; retained history grows |
| `/batch-write/{direct,seam}` | Repeated updates of the first 16 keys | New transaction/commit each operation; retained history grows |
| `BenchmarkCommitEventDisabledAndUnsubscribed/{direct,disabled,unsubscribed}` | Fake store: NewWriteTxn then Discard | No actual commit, storage, fsync, Raft, or Dgraph mutation. Map iteration can change cell order |

The TreeDB helper explicitly selects `ProfileCommandWALRelaxed`, disables side stores and periodic checkpoint interval, and opens the ordinary adapter. It does **not** select `cow_ordered` or Raft-owned recovery. The outer filesystem/raft_checkpoint matrix environment does not override these explicit benchmark options. Exact activation trace is below. Badger explicitly uses SyncWrites=false and retains all versions. Neither the durable callback/group benchmark nor the interleaved Badger/commit-event benchmarks is selected by the anchored regex.

## Candidate observations (all 28 cells)

These are candidate medians only. Each cell has all five original samples in the retained raw input; no sample was dropped. N ranges are deliberately visible because the benchmark is adaptive.

| Full cell | N min–max | median ns/op | median B/op | median allocs/op |
|---|---:|---:|---:|---:|
| `BenchmarkCommitEventDisabledAndUnsubscribed/direct` | 828171837–829896690 | 1.447 | 0 | 0 |
| `BenchmarkCommitEventDisabledAndUnsubscribed/disabled` | 737302158–829951671 | 1.447 | 0 | 0 |
| `BenchmarkCommitEventDisabledAndUnsubscribed/unsubscribed` | 364633330–414874231 | 3.046 | 0 | 0 |
| `BenchmarkBadgerStoreSeam/point-read/direct` | 3060472–3259135 | 364.8 | 408 | 7 |
| `BenchmarkBadgerStoreSeam/point-read/seam` | 3023505–3171265 | 362.6 | 408 | 7 |
| `BenchmarkBadgerStoreSeam/bounded-iterator/direct` | 169177–175675 | 7013 | 904 | 17 |
| `BenchmarkBadgerStoreSeam/bounded-iterator/seam` | 169525–171778 | 7165 | 904 | 17 |
| `BenchmarkBadgerStoreSeam/exact-key-all-versions/direct` | 1938210–2143731 | 574 | 872 | 16 |
| `BenchmarkBadgerStoreSeam/exact-key-all-versions/seam` | 1965906–2122210 | 589.8 | 872 | 16 |
| `BenchmarkBadgerStoreSeam/write/direct` | 558062–580875 | 2262 | 1456 | 33 |
| `BenchmarkBadgerStoreSeam/write/seam` | 556971–577354 | 2295 | 1440 | 33 |
| `BenchmarkBadgerStoreSeam/batch-write/direct` | 110055–117147 | 10971 | 10964 | 238 |
| `BenchmarkBadgerStoreSeam/batch-write/seam` | 113019–120270 | 11053 | 10949 | 238 |
| `BenchmarkTreeDBStoreAdapterOverhead/PointGet/DirectMVCC` | 792820–1000000 | 1459 | 488 | 7 |
| `BenchmarkTreeDBStoreAdapterOverhead/PointGet/TreeDBStore` | 792997–805486 | 1512 | 592 | 9 |
| `BenchmarkTreeDBStoreAdapterOverhead/AllVersionsScan/DirectMVCC` | 44442–45424 | 25814 | 45497 | 535 |
| `BenchmarkTreeDBStoreAdapterOverhead/AllVersionsScan/TreeDBStore` | 38252–39050 | 30238 | 45737 | 538 |
| `BenchmarkTreeDBStoreAdapterOverhead/RandomSeek/DirectMVCC` | 1672316–1692459 | 700.9 | 399 | 4 |
| `BenchmarkTreeDBStoreAdapterOverhead/RandomSeek/TreeDBStore` | 1627878–1641568 | 721.4 | 399 | 4 |
| `BenchmarkTreeDBStoreAdapterOverhead/WriteBatch1/DirectMVCC` | 739942–754815 | 1634 | 867 | 8 |
| `BenchmarkTreeDBStoreAdapterOverhead/WriteBatch1/TreeDBStore` | 663976–696697 | 1776 | 918 | 9 |
| `BenchmarkTreeDBStoreAdapterOverhead/WriteBatch16/DirectMVCC` | 107886–124368 | 14321 | 19416 | 61 |
| `BenchmarkTreeDBStoreAdapterOverhead/WriteBatch16/TreeDBStore` | 111956–116158 | 13902 | 19597 | 62 |
| `BenchmarkTreeDBStoreAdapterOverhead/ExactKeyAllVersions/DirectMVCC` | 328993–341196 | 3606 | 4600 | 55 |
| `BenchmarkTreeDBStoreAdapterOverhead/ExactKeyAllVersions/TreeDBStore` | 307960–313154 | 3908 | 4840 | 58 |
| `BenchmarkTreeDBStoreAdapterOverhead/Reopen/DirectMVCC` | 45–51 | 2.28348e+07 | 4.65488e+07 | 3855 |
| `BenchmarkTreeDBStoreAdapterOverhead/Reopen/TreeDBStore` | 44–46 | 2.30156e+07 | 4.65525e+07 | 3861 |
| `BenchmarkTreeDBStoreAdapterOverhead/Reopen/PairedLatency` | 22–25 | 4.71059e+07 | 9.2367e+07 | 7771 |

In this **filesystem** raw artifact, WriteBatch16/TreeDBStore is median **19,597 B/op, 62 allocs/op, 13,902 ns/op** (B/op 19,298–20,061; ns/op 13,567–14,947; N 111,956–116,158). This is not the approximately 22 KB observation from another/partial arm, and the actual baseline comparison is below. Direct WriteBatch16 is median 19,416 B/op/61 allocations; the direct/adapter difference inside this one candidate run is not the candidate-versus-baseline change. Reopen allocates about 46.5 MB per individual lifecycle, and paired reopen about 92.4 MB for both; those units must remain separate from posting writes or server operation allocations.

## Actual micro activation (both exact Gomap revisions)

This is an **ordinary adaptive mutable-memtable** comparison, not a COW microbenchmark. `posting/treedb_store_bench_test.go:581` constructs ProfileCommandWALRelaxed without setting MemtableMode; `posting/treedb_store.go:116` passes options directly to `treedb.Open` without consuming REQUIRE_TREEDB_MEMTABLE. At both Gomap fc3 and 29a2, `TreeDB/profiles.go:278` starts Options with only Dir and ApplyProfile does not set MemtableMode. Public Open defaults the empty mode to `adaptive` (fc3 public.go:1130; 29a2 public.go:1152). Caching Open converts adaptive to initial `append_only` (fc3 caching/db.go:12572; 29a2:12609). Adaptive subsequent choices remain ordinary modes; no transition from these options admits a COW owner. There is no micro runtime Stats witness of exactly which later ordinary mode was selected, so this note does not assert all operations remained append_only, btree, or skiplist.

In the identical launcher source, REQUIRE_TREEDB_MEMTABLE feeds `runtime_args` at run_durability_ab.sh:66, used for livebench invocations at :162/:190; the separate micro `go test` command at :90/:92 has no such selector. No matching TreeDB core environment consumer changes that conclusion. The final live-server COW witness remains independently valid; it is a different execution path.

## Actual baseline-preview comparison (all samples retained)

Baseline raw SHA256 `aa6be31e723d072e86ec7d5cf746c8ef6571862fda8c302667f1e13d7fd113ca`, path `baseline-micro-preview/raw.txt`; 28 cells × five samples, CPU suffix 2, successful posting package output. `final-micro-comparison-01.json` retains **all 280 exact raw lines, line numbers, N, and every metric**, including custom disk and paired reopen metrics. `final-micro-comparison-01.md` is the unabridged 28-cell standard-metric table. No rows were picked or dropped. Percentage changes below are ratios of medians from conventional sequential count-5 samples, not paired cross-source samples and not a new acceptance gate.

Material findings requiring review:

- **WriteBatch16/TreeDBStore B/op grows 18,597 → 19,597 (+5.377%)**, with all five baseline and candidate samples retaining 62 allocs/op. Median ns/op improves 14,415 → 13,902 (−3.559%). Baseline N is 95,941–102,958; candidate N 111,956–116,158, so final number of new keys, tree/flush shape, precomputed live input, and background attribution differ. The size growth is retained, not explained away. Baseline B/op samples are [18597,20686,18767,18576,18352]; candidate [19597,19601,19298,19326,20061]. Direct WriteBatch16 instead drops B/op 20,346 → 19,416 (−4.571%) with unchanged 61 allocations and +3.237% median time; this prevents attributing adapter-byte growth to a proven uniform engine cost.
- **Individual Reopen/DirectMVCC and Reopen/TreeDBStore median time grow +12.867% and +13.752%** (20.232 → 22.835 ms and 20.233 → 23.016 ms). Allocations increase 3,781 → 3,855 (+74, +1.957%) and 3,785 → 3,861 (+76, +2.008%). Median B/op slightly decreases (−0.071% and −0.086%). These are genuine reported lifecycle differences needing source/profile attribution, not proof of a timed mutation regression.
- **PairedLatency total median time is effectively unchanged**: 47.112907 → 47.105898 ms (−0.0149%). Its allocations rise 7,631 → 7,771 (+140, +1.835%) and B/op rises 92,349,980 → 92,366,996 (+17,016, +0.0184%). Custom paired direct medians are 23.454047 → 23.598654 ms; adapter 23.511791 → 23.506358 ms. This separate paired-lifecycle cell does not confirm the +13% individual-cell latency increase, and it does not erase those individual observations. Both are preserved.
- WriteBatch1 median B/op grows direct 855 → 867 (+1.404%) and adapter 902 → 918 (+1.774%), with unchanged 8/9 allocations and +1.114/+1.197% time. All point/seek/scan/exact-history TreeDB cells have unchanged median allocations and bytes. Their times range −6.401% to +1.427%. All Badger cells have unchanged median allocation counts; B/op rises at most 12 bytes (+0.831%, direct write), and time changes range −0.966% to +2.236%. All three fake commit-event cells remain zero B/op/allocations.

These results surface the micro-level regressions honestly. They do not estimate Dgraph operation allocations, diagnose a COW workspace allocation regression, establish causal time effects, or reopen/retry the failed process-arm campaign. Full archive binding remains the outstanding evidence condition.

## Comparison preparation and interpretation limits

`analyze_final_micro.py` is a stdlib retained-artifact reader, now executed once against the actual completed baseline micro preview and retained filesystem candidate. It requires complete successful package logs with exactly the shared 28 cells × five samples each and CPU suffix 2 on both sides. It preserves all 280 raw lines, line numbers, adaptive N, metric values, custom disk/reopen metrics, and input hashes in JSON. It emits every cell and median change for ns/op/B/op/allocs/op, plus custom logical disk deltas in JSON. Every positive median change is flagged for review without inventing a numerical microbenchmark acceptance gate; zero baseline makes percentage unavailable rather than dividing by zero. There is no overall PASS, and original matrix STOP remains locked.

The completed artifact-only parser invocation was:

```sh
python3 artifacts/final-native111-runtime-01/analyze_final_micro.py \
  --candidate artifacts/final-native111-runtime-01/filesystem-evidence/filesystem-12/microbench/raw.stdout \
  --baseline artifacts/final-native111-runtime-01/baseline-micro-preview/raw.txt \
  --output-prefix artifacts/final-native111-runtime-01/final-micro-comparison-01
```

Before interpreting output, verify baseline command, Go/env/source/module receipts and hashes against its accepted baseline packet. This parser verifies log shape/sample conservation, not independent binary/source authentication. A complete descriptive review must call out material allocation **and** time increases across all common cells, including unaffected Badger/fake-store controls, rather than selecting favorable rows. Five conventional count samples are not interleaved baseline/candidate pairs; ordinal samples must not be treated as paired AB timing observations.

Adaptive `b.N` changes the number of distinct TreeDB write keys and Badger retained versions, precomputed input size/heap pressure, tree height, flush/maintenance frequency, and final disk density. Thus larger write-cell B/op or time changes need source/shape attribution; fixed per-operation intent does not imply fixed final database size. TreeDB custom `disk-B/op` and `disk-B/item` are regular-file **logical size change after Close**, divided by N/items (negative deltas clamped to zero); they are not allocated blocks, bytes physically written, steady-state amplification, or sustained resource equilibrium. Reopen allocation/timing includes repeated lifecycle work and adaptive small iteration counts. Timer-scoped `-benchmem` counts include concurrent runtime/background work during the interval and do not isolate a Dgraph transaction.

Dgraph operation-level allocations remain unavailable. CPU profiles are sampled diagnostics and do not turn these microbenchmarks into server-operation allocation attribution or quantify fsync wall time. The full baseline archive/source binding remains **PENDING**. The descriptive differences below are real retained observations, not causal attribution or final qualification; all failed original matrix evidence remains retained.

## Final archive binding completed

The baseline preview's earlier pending archive condition is now **CLOSED** by independent retained-byte audit. Final archive `baseline-filesystem-12-EVIDENCE.tar` is 8,089,600 bytes, SHA256 `8c84c738db0119e90dacef38663222144e80edcf549b81e8049b79dc7da993e9`; all161 members and160 selected hashes match. Archived `baseline-evidence/baseline-filesystem-12/microbench/raw.stdout` equals the entire preview byte for byte, SHA256 `aa6be31e723d072e86ec7d5cf746c8ef6571862fda8c302667f1e13d7fd113ca`. The original parser inputs/results therefore need no replacement or rerun. V1 exact source/build/replacement identity, all live row witnesses, and profile bindings are independently accepted in `baseline-evidence/BASELINE_ARM_AUDIT.md`. The candidate/accepted-baseline benchmark source equivalence and ordinary adaptive activation conclusions above remain unchanged. All280 raw samples, adverse micro differences and whole-campaign STOP remain retained; this closes evidence identity, not performance qualification.
