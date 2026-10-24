# Final matrix supplementary analysis

**Whole campaign: STOP locked. No overall PASS or promotion.**

All original reports and rows remain authoritative. This reader preserves the numerical failures and is not a replacement gate.

## Within-arm original throughput rule

| Arm | Class | Badger median ops/s | TreeDB median ops/s | TreeDB/Badger | Delta % | Original 0.97 gate |
|---|---|---:|---:|---:|---:|---|
| process | relaxed | 9938.1542 | 9564.2564 | 0.962378 | -3.7622 | FAIL |
| process | durable | 837.2059 | 337.2918 | 0.402878 | -59.7122 | FAIL |
| filesystem | relaxed | 273.2701 | 334.9164 | 1.225587 | 22.5587 | PASS |
| filesystem | durable | 270.1905 | 429.0842 | 1.588080 | 58.8080 | PASS |
| baseline | relaxed | 270.2638 | 475.7794 | 1.760426 | 76.0426 | PASS |
| baseline | durable | 267.0481 | 195.5688 | 0.732335 | -26.7665 | FAIL |

## Filesystem source-paired ancillary changes

All three changes are candidate/baseline minus one, in percent. A missing repeat makes the median UNQUALIFIED. <=5% is review-only.

| Backend | Class | Metric | r1 % | r2 % | r3 % | Paired median % | Disposition |
|---|---|---|---:|---:|---:|---:|---|
| badger | relaxed | p95 | -14.4918 | -0.8143 | 35.7050 | -0.8143 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | p99 | -23.3813 | 1.8334 | 35.5475 | 1.8334 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | cpu_seconds | -8.4507 | 0.0000 | 6.2500 | 0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | rss_peak_bytes | 0.0712 | 0.1320 | 0.0977 | 0.0977 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | disk_logical_bytes | 0.0000 | 0.0000 | 0.0000 | 0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | disk_allocated_bytes | 0.0000 | 0.0000 | 0.0000 | 0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | recovery_read_seconds | -23.1679 | 0.0607 | 0.1024 | 0.0607 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | relaxed | recovery_read_write_seconds | -23.0009 | 0.0600 | 0.4074 | 0.0600 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | p95 | 0.9500 | -47.4535 | 1.8606 | 0.9500 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | p99 | 8.0011 | -63.7112 | 1.9309 | 1.9309 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | cpu_seconds | 0.0000 | -8.8608 | -2.8169 | -2.8169 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | rss_peak_bytes | -0.0473 | 0.0372 | 0.3195 | 0.0372 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | disk_logical_bytes | 0.0000 | 0.0000 | 0.0000 | 0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | disk_allocated_bytes | 0.0000 | 0.0000 | 0.0000 | 0.0000 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | recovery_read_seconds | 0.1934 | -23.1326 | -29.9747 | -23.1326 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| badger | durable | recovery_read_write_seconds | 0.0978 | -23.0521 | -29.9237 | -23.0521 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | p95 | 45.7026 | 45.0524 | 0.0320 | 45.0524 | FAIL_REVIEW_REQUIRED |
| treedb | relaxed | p99 | 38.2301 | 43.8688 | 0.2100 | 38.2301 | FAIL_REVIEW_REQUIRED |
| treedb | relaxed | cpu_seconds | 2.7027 | 4.1096 | 0.0000 | 2.7027 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | rss_peak_bytes | 0.0008 | -0.0644 | -0.4730 | -0.0644 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | disk_logical_bytes | 0.0225 | -0.1495 | -0.3181 | -0.1495 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | disk_allocated_bytes | 0.0853 | -0.0851 | -0.2551 | -0.0851 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | recovery_read_seconds | -0.5592 | -0.1945 | 29.6006 | -0.1945 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | relaxed | recovery_read_write_seconds | -0.2786 | -0.1156 | 29.3125 | -0.1156 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | p95 | -33.7642 | -23.0782 | -29.3899 | -29.3899 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | p99 | -24.8130 | -21.2831 | -29.3151 | -24.8130 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | cpu_seconds | -13.2530 | -22.3529 | -17.8571 | -17.8571 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | rss_peak_bytes | 1.9610 | 1.0303 | 1.8852 | 1.8852 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | disk_logical_bytes | -7.3690 | -7.0735 | -7.0860 | -7.0860 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | disk_allocated_bytes | -7.0145 | -6.8376 | -6.6781 | -6.8376 | WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE |
| treedb | durable | recovery_read_seconds | -18.5988 | 27.9597 | 20.4990 | 20.4990 | FAIL_REVIEW_REQUIRED |
| treedb | durable | recovery_read_write_seconds | -1.3130 | 27.8502 | 27.9478 | 27.8502 | FAIL_REVIEW_REQUIRED |

## Filesystem source-paired throughput

No new 5% throughput allowance is authorized. Any observed decline requires review; non-decline is not acceptance.

| Backend | Class | r1 % | r2 % | r3 % | Paired median % | Review |
|---|---|---:|---:|---:|---:|---|
| badger | relaxed | 22.1473 | 1.1124 | -21.2026 | 1.1124 | DECLINE_REVIEW_REQUIRED |
| badger | durable | 4.5803 | 42.2693 | 1.1767 | 4.5803 | NO_OBSERVED_DECLINE_NOT_ACCEPTANCE |
| treedb | relaxed | -29.4479 | -29.6068 | -2.0947 | -29.4479 | DECLINE_REVIEW_REQUIRED |
| treedb | durable | 101.2494 | 123.2680 | 120.1594 | 120.1594 | NO_OBSERVED_DECLINE_NOT_ACCEPTANCE |

## Every original repeat

| Arm | Run | Ops/s | p95 ms | p99 ms | Result SHA256 |
|---|---|---:|---:|---:|---|
| process | badger-durable-r1 | 812.9609 | 12.4610 | 15.5970 | 20c0484de9a4573349e3d7e0082e851a79c96483b614e92faff0fe78d4cf5009 |
| process | badger-durable-r2 | 892.6470 | 10.8670 | 15.9150 | ac99f69a1672b64f5a2ba0f00b1ad3ec19cbcaa408fd6a9914352b84665be5bf |
| process | badger-durable-r3 | 837.2059 | 12.5650 | 35.7390 | 3c0584aee3f98fe7eb3e93b8bed255512af3c7f2c73ced9e6f7852964272f510 |
| process | badger-relaxed-r1 | 9911.1132 | 0.7080 | 0.9780 | 8ae8f45c9035f65111542735110b1ebf426fa918b46ef9536a0ec95d0a82c3be |
| process | badger-relaxed-r2 | 9938.1542 | 0.6790 | 0.8630 | 67ed42a1953a3a86849c55ba625ea54fd537aede391fbcacd736c341dad727b0 |
| process | badger-relaxed-r3 | 10238.2372 | 0.6970 | 0.9080 | 13c549ba448851c3aca48579f897bd928f04f6185d007d55e8d4c892d26639a7 |
| process | treedb-durable-r1 | 331.5756 | 29.2760 | 39.3100 | e66e7343c70490a7eb2ec294ae8c3ae572d3a9fb3f6b3fb8cdd02c87c037b44c |
| process | treedb-durable-r2 | 337.2918 | 29.3950 | 35.9940 | 3d049ea7916fd238c161f2ee53c29702d1357c7ad435976b3f4da971a48145e4 |
| process | treedb-durable-r3 | 437.5675 | 26.1000 | 34.4770 | ed839b63857adfa9ae08937daa5df9a579735d7fbe9fbb624c2ef1527779d02f |
| process | treedb-relaxed-r1 | 9564.2564 | 0.7170 | 0.8510 | 030b7b1070a758897e6ecbff5f4b7cc3a1a6943c25986c64fc727e306a1533a6 |
| process | treedb-relaxed-r2 | 9512.1644 | 0.7110 | 0.8500 | 8c1c06eef973a8fb8b99621ce9eb7d470ccf3fff8ad7f9ccc773bc73a3c0208f |
| process | treedb-relaxed-r3 | 9590.1858 | 0.7310 | 0.8670 | f7f6b3e3a92589769300187eec0e2ddedbdd7f0e9353b73be830c4552f1aff56 |
| filesystem | badger-durable-r1 | 280.0514 | 26.3540 | 32.8280 | 060dec1f8e03757f78b451f0b1daeb84d2e86172c6fbbe3c8cd2e752e41c6e8c |
| filesystem | badger-durable-r2 | 269.1832 | 25.5970 | 31.2410 | 71a44b50c034d3fe93cbad4d830248787b6b3087888b6507d372aefcbcaaec62 |
| filesystem | badger-durable-r3 | 270.1905 | 25.6210 | 30.3010 | f17c9c3333cf7412c66389ca6df6e396b901b49d506b67e29b4224094d73729a |
| filesystem | badger-relaxed-r1 | 273.6757 | 27.5020 | 32.5530 | 5e3613b18b76ac6a00e09ae9fd5e9c747902b55ce637bddf0d544087e40d229d |
| filesystem | badger-relaxed-r2 | 273.2701 | 25.9430 | 31.2150 | 141da7aff1ed97726c41888a42dfa2ed56f0ab40307daad3c8eeff1477091f3e |
| filesystem | badger-relaxed-r3 | 273.0843 | 27.2170 | 32.0990 | 6ffd797a50d630f6e795e91a9631bcf0f3307e41a6e5c65ef6f0b897b1560f5b |
| filesystem | treedb-durable-r1 | 398.0025 | 22.4950 | 29.5470 | 17cd7250fd8df6b65240a48976146466ce3771bade1888f1adc4e75c8f767ad7 |
| filesystem | treedb-durable-r2 | 436.6426 | 26.9280 | 31.9630 | dad07b78e20d05f64181d561f05b008fb7802ccb16ad186f9eedf358328d6354 |
| filesystem | treedb-durable-r3 | 429.0842 | 25.4740 | 30.1160 | 9f894c7d794d77c0cb7a31952d1bd9f5d0f62d6e7ce241c81dbb764e2efc8859 |
| filesystem | treedb-relaxed-r1 | 335.9413 | 21.7840 | 25.2740 | 9c22361aa168160be7e3610ad38b8afcbeb203372dbaf5e205b71bb6cb0eaee2 |
| filesystem | treedb-relaxed-r2 | 334.9164 | 21.7390 | 25.7410 | 9916022e3d1fa9ffb5cdb53236c861744d869fbf6c6713d6f0d102fe9005be6b |
| filesystem | treedb-relaxed-r3 | 329.6847 | 21.8580 | 26.2450 | 7b552cfb5db3adc8d32352a3843b661dede2fc40a0e10b91d084a8524117a664 |
| baseline | badger-durable-r1 | 267.7861 | 26.1060 | 30.3960 | 5c5167ce1476af2c8659686294bf368b1e50dd2d5c53706779718d0eae9d7016 |
| baseline | badger-durable-r2 | 189.2068 | 48.7130 | 86.0900 | bf9763f06dbf00d808cd79f17cdf2eb3a13ba90de13c1caa7e4ee74700de68be |
| baseline | badger-durable-r3 | 267.0481 | 25.1530 | 29.7270 | 61f9ca70ea2897909a5816abd7764da9ab7dd69380eb1544207a0941cda51671 |
| baseline | badger-relaxed-r1 | 224.0537 | 32.1630 | 42.4870 | 6090ae53712d78ba712c4ee50b519f015fe56b448c417ef557e48efc8db9e533 |
| baseline | badger-relaxed-r2 | 270.2638 | 26.1560 | 30.6530 | cbc6dfb2c4830eef568d1a681af8ff6a256b28d4b89d0aa9854bf679232dea6c |
| baseline | badger-relaxed-r3 | 346.5652 | 20.0560 | 23.6810 | 4c1ec0b62575b09d4bd8d704de78ff0b97afe35d76abfb552c9ff1aa13696d68 |
| baseline | treedb-durable-r1 | 197.7658 | 33.9620 | 39.2980 | b21f5f79dcf34f13c6251b80e1362b4d142f1c0552119721f967c0dbd69ab8ce |
| baseline | treedb-durable-r2 | 195.5688 | 35.0070 | 40.6050 | d7187661b10281ecd1c0762091cb3518d76f7350eb10f222afda18f71a6877fe |
| baseline | treedb-durable-r3 | 194.8971 | 36.0770 | 42.6060 | 6c824f1304b51066b4c7d11b37c364fb6c608ee94ce8f06b0589c4a229c62071 |
| baseline | treedb-relaxed-r1 | 476.1609 | 14.9510 | 18.2840 | d2c33eed8d9ea2d91ff6a9027b524f98bbad55f0c9ce23b61ea47251c53cd76b |
| baseline | treedb-relaxed-r2 | 475.7794 | 14.9870 | 17.8920 | 34ce809b4f5296dd90669a454d784e0f9f6443be7783de3baa8e0d2e0d4c0647 |
| baseline | treedb-relaxed-r3 | 336.7384 | 21.8510 | 26.1900 | f94038b3a4e26e7baf5ce32a7c9614efd0cd4410019edc0b066a883b84e4df23 |

## Excluded profile rows

- process: `process-evidence/process-12/profiles/treedb-durable-run/result.json` SHA256 `f2e2595499ed3089289cd8064de0462c6ff31016807a8ea5abe2769cad5b4eca`; excluded diagnostic, never a decision repeat.
- process: `process-evidence/process-12/profiles/treedb-relaxed-run/result.json` SHA256 `51984cae5700156e3d13194608379ca9942e3666aaed11788053595d792b0537`; excluded diagnostic, never a decision repeat.
- filesystem: `filesystem-evidence/filesystem-12/profiles/treedb-durable-run/result.json` SHA256 `02a38e121614b397badd0c7ea4c79a2efeddfeb3793c667828252795bf1e6078`; excluded diagnostic, never a decision repeat.
- filesystem: `filesystem-evidence/filesystem-12/profiles/treedb-relaxed-run/result.json` SHA256 `4066d54dc7c8498b8c2ae5c5bbbb1c613cf54b04defc831246857b9cee484b38`; excluded diagnostic, never a decision repeat.
- baseline: `baseline-evidence/baseline-filesystem-12/profiles/treedb-durable-run/result.json` SHA256 `1b18d595fca8a435c8c450abbfc2410831e2fe9c3f41b301f10b078f26facba4`; excluded diagnostic, never a decision repeat.
- baseline: `baseline-evidence/baseline-filesystem-12/profiles/treedb-relaxed-run/result.json` SHA256 `52f09e31e184a2906f4936e81071ed1b6e0708e0811c5c459749ed6b51fb3648`; excluded diagnostic, never a decision repeat.

## Operational observations and bounds

JSON retains every absolute metric observation (availability, value, source, reason), all-three medians only, and legacy HTTP-health recovery descriptively. No missing metric becomes zero.

- Campaign STOP is locked by the original process arm. No overall PASS is emitted.
- Profiles are excluded diagnostic rows, never pooled with the original 36.
- The original 0.97 throughput parity rule uses a ratio of independent medians.
- Filesystem source comparisons pair the same backend/class/repeat; Badger drift is not normalized away.
- Ancillary >5% paired-median growth requires FAIL/review. <=5% is not automatic acceptance.
- Missing, unavailable, nonfinite, or nonpositive ratio operands are UNQUALIFIED; no imputation or selected-repeat median.
- No original process baseline exists; process/filesystem contrasts are not source-regression qualification.
- p95/p99 are aggregate mixed-operation latency; CPU is timed Alpha /proc, not whole-system CPU.
- RSS is Alpha VmHWM including setup, not timed live RSS or equilibrium.
- Disk covers postings only; read logical and allocated bytes together, not full Raft/Zero/temp storage.
- Aligned recovery is clean Alpha restart with Zero alive, not crash/power-loss latency.
- Legacy recovery_seconds is HTTP health and remains descriptive only.
- Operational counters retain per-repeat availability and source; zero can be a valid measured counter.
- GC/flush/checkpoint definitions differ by backend and cannot establish equal work or equilibrium.
- Whole-system CPU, per-op allocations, equivalent physical write amplification, physical I/O wait/bytes, live Raft debt, and complete ACK-phase timing are unavailable.
- This artifact reader checks stored source/manifest bindings and original reports; it does not re-execute binary/module or runtime-sidecar verification.

## Input bindings

- `DIAGNOSTIC_CONTINUATION_AMENDMENT.json`: `33a7fbd763a7ab4326381a758561522163b46f629552dfd9f5e68584424a83ac`
- `MATRIX_COMPARISON_MAPPING.md`: `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`
- `baseline-evidence/baseline-filesystem-12-invocation.json`: `6651a82356c352a171a239ae2c9ea1eed55a0eeebb4c168f56e8db1a86934873`
- `baseline-evidence/baseline-filesystem-12-source-manifest.json`: `7dcaa8e87a9b1a772eb7094d675f85dc4211c1aa0e66f67ab502f5538a4ff445`
- `baseline-evidence/baseline-filesystem-12/live/badger-durable-r1/result.json`: `5c5167ce1476af2c8659686294bf368b1e50dd2d5c53706779718d0eae9d7016`
- `baseline-evidence/baseline-filesystem-12/live/badger-durable-r2/result.json`: `bf9763f06dbf00d808cd79f17cdf2eb3a13ba90de13c1caa7e4ee74700de68be`
- `baseline-evidence/baseline-filesystem-12/live/badger-durable-r3/result.json`: `61f9ca70ea2897909a5816abd7764da9ab7dd69380eb1544207a0941cda51671`
- `baseline-evidence/baseline-filesystem-12/live/badger-relaxed-r1/result.json`: `6090ae53712d78ba712c4ee50b519f015fe56b448c417ef557e48efc8db9e533`
- `baseline-evidence/baseline-filesystem-12/live/badger-relaxed-r2/result.json`: `cbc6dfb2c4830eef568d1a681af8ff6a256b28d4b89d0aa9854bf679232dea6c`
- `baseline-evidence/baseline-filesystem-12/live/badger-relaxed-r3/result.json`: `4c1ec0b62575b09d4bd8d704de78ff0b97afe35d76abfb552c9ff1aa13696d68`
- `baseline-evidence/baseline-filesystem-12/live/treedb-durable-r1/result.json`: `b21f5f79dcf34f13c6251b80e1362b4d142f1c0552119721f967c0dbd69ab8ce`
- `baseline-evidence/baseline-filesystem-12/live/treedb-durable-r2/result.json`: `d7187661b10281ecd1c0762091cb3518d76f7350eb10f222afda18f71a6877fe`
- `baseline-evidence/baseline-filesystem-12/live/treedb-durable-r3/result.json`: `6c824f1304b51066b4c7d11b37c364fb6c608ee94ce8f06b0589c4a229c62071`
- `baseline-evidence/baseline-filesystem-12/live/treedb-relaxed-r1/result.json`: `d2c33eed8d9ea2d91ff6a9027b524f98bbad55f0c9ce23b61ea47251c53cd76b`
- `baseline-evidence/baseline-filesystem-12/live/treedb-relaxed-r2/result.json`: `34ce809b4f5296dd90669a454d784e0f9f6443be7783de3baa8e0d2e0d4c0647`
- `baseline-evidence/baseline-filesystem-12/live/treedb-relaxed-r3/result.json`: `f94038b3a4e26e7baf5ce32a7c9614efd0cd4410019edc0b066a883b84e4df23`
- `baseline-evidence/baseline-filesystem-12/profiles/treedb-durable-run/result.json`: `1b18d595fca8a435c8c450abbfc2410831e2fe9c3f41b301f10b078f26facba4`
- `baseline-evidence/baseline-filesystem-12/profiles/treedb-relaxed-run/result.json`: `52f09e31e184a2906f4936e81071ed1b6e0708e0811c5c459749ed6b51fb3648`
- `baseline-evidence/baseline-filesystem-12/report.md`: `c01ccc04b4d766041676817517db9410c2566a642744c58f2ac878398acf7242`
- `filesystem-evidence/filesystem-12-invocation.json`: `1d885200020de6967fc43cdc425afdf4ba2e3ca8328daa491c0b37a901e286cf`
- `filesystem-evidence/filesystem-12-source-manifest.json`: `008eda694c391c88f23af0cad4b06ac145668a38eec7affc27378ea0813c518c`
- `filesystem-evidence/filesystem-12/live/badger-durable-r1/result.json`: `060dec1f8e03757f78b451f0b1daeb84d2e86172c6fbbe3c8cd2e752e41c6e8c`
- `filesystem-evidence/filesystem-12/live/badger-durable-r2/result.json`: `71a44b50c034d3fe93cbad4d830248787b6b3087888b6507d372aefcbcaaec62`
- `filesystem-evidence/filesystem-12/live/badger-durable-r3/result.json`: `f17c9c3333cf7412c66389ca6df6e396b901b49d506b67e29b4224094d73729a`
- `filesystem-evidence/filesystem-12/live/badger-relaxed-r1/result.json`: `5e3613b18b76ac6a00e09ae9fd5e9c747902b55ce637bddf0d544087e40d229d`
- `filesystem-evidence/filesystem-12/live/badger-relaxed-r2/result.json`: `141da7aff1ed97726c41888a42dfa2ed56f0ab40307daad3c8eeff1477091f3e`
- `filesystem-evidence/filesystem-12/live/badger-relaxed-r3/result.json`: `6ffd797a50d630f6e795e91a9631bcf0f3307e41a6e5c65ef6f0b897b1560f5b`
- `filesystem-evidence/filesystem-12/live/treedb-durable-r1/result.json`: `17cd7250fd8df6b65240a48976146466ce3771bade1888f1adc4e75c8f767ad7`
- `filesystem-evidence/filesystem-12/live/treedb-durable-r2/result.json`: `dad07b78e20d05f64181d561f05b008fb7802ccb16ad186f9eedf358328d6354`
- `filesystem-evidence/filesystem-12/live/treedb-durable-r3/result.json`: `9f894c7d794d77c0cb7a31952d1bd9f5d0f62d6e7ce241c81dbb764e2efc8859`
- `filesystem-evidence/filesystem-12/live/treedb-relaxed-r1/result.json`: `9c22361aa168160be7e3610ad38b8afcbeb203372dbaf5e205b71bb6cb0eaee2`
- `filesystem-evidence/filesystem-12/live/treedb-relaxed-r2/result.json`: `9916022e3d1fa9ffb5cdb53236c861744d869fbf6c6713d6f0d102fe9005be6b`
- `filesystem-evidence/filesystem-12/live/treedb-relaxed-r3/result.json`: `7b552cfb5db3adc8d32352a3843b661dede2fc40a0e10b91d084a8524117a664`
- `filesystem-evidence/filesystem-12/profiles/treedb-durable-run/result.json`: `02a38e121614b397badd0c7ea4c79a2efeddfeb3793c667828252795bf1e6078`
- `filesystem-evidence/filesystem-12/profiles/treedb-relaxed-run/result.json`: `4066d54dc7c8498b8c2ae5c5bbbb1c613cf54b04defc831246857b9cee484b38`
- `filesystem-evidence/filesystem-12/report.md`: `b72931b85dc2011fadb956ada0370944bb03e46d6b610bb5e6d01828f82b32a6`
- `process-evidence/process-12-invocation.json`: `c0b1263174ba8dfdb80f42d3eb19aeb114b74aa05c4853b86e271cb86da26fe9`
- `process-evidence/process-12-source-manifest.json`: `008eda694c391c88f23af0cad4b06ac145668a38eec7affc27378ea0813c518c`
- `process-evidence/process-12/live/badger-durable-r1/result.json`: `20c0484de9a4573349e3d7e0082e851a79c96483b614e92faff0fe78d4cf5009`
- `process-evidence/process-12/live/badger-durable-r2/result.json`: `ac99f69a1672b64f5a2ba0f00b1ad3ec19cbcaa408fd6a9914352b84665be5bf`
- `process-evidence/process-12/live/badger-durable-r3/result.json`: `3c0584aee3f98fe7eb3e93b8bed255512af3c7f2c73ced9e6f7852964272f510`
- `process-evidence/process-12/live/badger-relaxed-r1/result.json`: `8ae8f45c9035f65111542735110b1ebf426fa918b46ef9536a0ec95d0a82c3be`
- `process-evidence/process-12/live/badger-relaxed-r2/result.json`: `67ed42a1953a3a86849c55ba625ea54fd537aede391fbcacd736c341dad727b0`
- `process-evidence/process-12/live/badger-relaxed-r3/result.json`: `13c549ba448851c3aca48579f897bd928f04f6185d007d55e8d4c892d26639a7`
- `process-evidence/process-12/live/treedb-durable-r1/result.json`: `e66e7343c70490a7eb2ec294ae8c3ae572d3a9fb3f6b3fb8cdd02c87c037b44c`
- `process-evidence/process-12/live/treedb-durable-r2/result.json`: `3d049ea7916fd238c161f2ee53c29702d1357c7ad435976b3f4da971a48145e4`
- `process-evidence/process-12/live/treedb-durable-r3/result.json`: `ed839b63857adfa9ae08937daa5df9a579735d7fbe9fbb624c2ef1527779d02f`
- `process-evidence/process-12/live/treedb-relaxed-r1/result.json`: `030b7b1070a758897e6ecbff5f4b7cc3a1a6943c25986c64fc727e306a1533a6`
- `process-evidence/process-12/live/treedb-relaxed-r2/result.json`: `8c1c06eef973a8fb8b99621ce9eb7d470ccf3fff8ad7f9ccc773bc73a3c0208f`
- `process-evidence/process-12/live/treedb-relaxed-r3/result.json`: `f7f6b3e3a92589769300187eec0e2ddedbdd7f0e9353b73be830c4552f1aff56`
- `process-evidence/process-12/profiles/treedb-durable-run/result.json`: `f2e2595499ed3089289cd8064de0462c6ff31016807a8ea5abe2769cad5b4eca`
- `process-evidence/process-12/profiles/treedb-relaxed-run/result.json`: `51984cae5700156e3d13194608379ca9942e3666aaed11788053595d792b0537`
- `process-evidence/process-12/report.md`: `1d2899e0ddd88343570130a4ccb49062807366035a90f9bdeb752558e69295ba`
