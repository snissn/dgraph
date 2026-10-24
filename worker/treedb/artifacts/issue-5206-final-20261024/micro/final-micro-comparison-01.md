# Final supplementary microbenchmark comparison

Descriptive only. Whole campaign STOP remains locked; no acceptance verdict. All five samples per cell are retained in JSON. Count-5 samples are not paired AB observations.

| Cell | baseline N range | candidate N range | ns/op baseline → candidate (change) | B/op baseline → candidate (change) | allocs/op baseline → candidate (change) |
|---|---|---|---|---|---|
| BenchmarkBadgerStoreSeam/batch-write/direct | 112993–118669 | 110055–117147 | 11078 → 10971 (-0.966%) | 10959 → 10964 (+0.046%) | 238 → 238 (+0.000%) |
| BenchmarkBadgerStoreSeam/batch-write/seam | 112720–119151 | 113019–120270 | 11018 → 11053 (+0.318%) | 10961 → 10949 (-0.109%) | 238 → 238 (+0.000%) |
| BenchmarkBadgerStoreSeam/bounded-iterator/direct | 168974–175333 | 169177–175675 | 6953 → 7013 (+0.863%) | 904 → 904 (+0.000%) | 17 → 17 (+0.000%) |
| BenchmarkBadgerStoreSeam/bounded-iterator/seam | 167878–171699 | 169525–171778 | 7166 → 7165 (-0.014%) | 904 → 904 (+0.000%) | 17 → 17 (+0.000%) |
| BenchmarkBadgerStoreSeam/exact-key-all-versions/direct | 2024480–2117787 | 1938210–2143731 | 569.4 → 574 (+0.808%) | 872 → 872 (+0.000%) | 16 → 16 (+0.000%) |
| BenchmarkBadgerStoreSeam/exact-key-all-versions/seam | 1952596–2030053 | 1965906–2122210 | 576.9 → 589.8 (+2.236%) | 872 → 872 (+0.000%) | 16 → 16 (+0.000%) |
| BenchmarkBadgerStoreSeam/point-read/direct | 3081832–3196360 | 3060472–3259135 | 359.2 → 364.8 (+1.559%) | 408 → 408 (+0.000%) | 7 → 7 (+0.000%) |
| BenchmarkBadgerStoreSeam/point-read/seam | 3076050–3223818 | 3023505–3171265 | 366 → 362.6 (-0.929%) | 408 → 408 (+0.000%) | 7 → 7 (+0.000%) |
| BenchmarkBadgerStoreSeam/write/direct | 530224–581677 | 558062–580875 | 2264 → 2262 (-0.088%) | 1444 → 1456 (+0.831%) | 33 → 33 (+0.000%) |
| BenchmarkBadgerStoreSeam/write/seam | 516927–565446 | 556971–577354 | 2305 → 2295 (-0.434%) | 1440 → 1440 (+0.000%) | 33 → 33 (+0.000%) |
| BenchmarkCommitEventDisabledAndUnsubscribed/direct | 603479414–829523979 | 828171837–829896690 | 1.447 → 1.447 (+0.000%) | 0 → 0 (+0; zero baseline) | 0 → 0 (+0; zero baseline) |
| BenchmarkCommitEventDisabledAndUnsubscribed/disabled | 663683930–829472890 | 737302158–829951671 | 1.808 → 1.447 (-19.967%) | 0 → 0 (+0; zero baseline) | 0 → 0 (+0; zero baseline) |
| BenchmarkCommitEventDisabledAndUnsubscribed/unsubscribed | 368568392–387092071 | 364633330–414874231 | 3.254 → 3.046 (-6.392%) | 0 → 0 (+0; zero baseline) | 0 → 0 (+0; zero baseline) |
| BenchmarkTreeDBStoreAdapterOverhead/AllVersionsScan/DirectMVCC | 42423–44160 | 44442–45424 | 26441 → 25814 (-2.371%) | 45497 → 45497 (+0.000%) | 535 → 535 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/AllVersionsScan/TreeDBStore | 35950–36678 | 38252–39050 | 32306 → 30238 (-6.401%) | 45737 → 45737 (+0.000%) | 538 → 538 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/ExactKeyAllVersions/DirectMVCC | 332413–342704 | 328993–341196 | 3593 → 3606 (+0.362%) | 4600 → 4600 (+0.000%) | 55 → 55 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/ExactKeyAllVersions/TreeDBStore | 311863–317360 | 307960–313154 | 3853 → 3908 (+1.427%) | 4840 → 4840 (+0.000%) | 58 → 58 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/PointGet/DirectMVCC | 797116–1000000 | 792820–1000000 | 1504 → 1459 (-2.992%) | 488 → 488 (+0.000%) | 7 → 7 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/PointGet/TreeDBStore | 731677–779595 | 792997–805486 | 1558 → 1512 (-2.953%) | 592 → 592 (+0.000%) | 9 → 9 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/RandomSeek/DirectMVCC | 1611492–1639839 | 1672316–1692459 | 730.4 → 700.9 (-4.039%) | 399 → 399 (+0.000%) | 4 → 4 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/RandomSeek/TreeDBStore | 1560783–1569758 | 1627878–1641568 | 757.8 → 721.4 (-4.803%) | 399 → 399 (+0.000%) | 4 → 4 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/Reopen/DirectMVCC | 50–55 | 45–51 | 2.02315e+07 → 2.28348e+07 (+12.867%) | 4.65821e+07 → 4.65488e+07 (-0.071%) | 3781 → 3855 (+1.957%) |
| BenchmarkTreeDBStoreAdapterOverhead/Reopen/PairedLatency | 24–26 | 22–25 | 4.71129e+07 → 4.71059e+07 (-0.015%) | 9.235e+07 → 9.2367e+07 (+0.018%) | 7631 → 7771 (+1.835%) |
| BenchmarkTreeDBStoreAdapterOverhead/Reopen/TreeDBStore | 49–58 | 44–46 | 2.02332e+07 → 2.30156e+07 (+13.752%) | 4.65927e+07 → 4.65525e+07 (-0.086%) | 3785 → 3861 (+2.008%) |
| BenchmarkTreeDBStoreAdapterOverhead/WriteBatch1/DirectMVCC | 722404–759626 | 739942–754815 | 1616 → 1634 (+1.114%) | 855 → 867 (+1.404%) | 8 → 8 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/WriteBatch1/TreeDBStore | 696147–704150 | 663976–696697 | 1755 → 1776 (+1.197%) | 902 → 918 (+1.774%) | 9 → 9 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/WriteBatch16/DirectMVCC | 102481–107293 | 107886–124368 | 13872 → 14321 (+3.237%) | 20346 → 19416 (-4.571%) | 61 → 61 (+0.000%) |
| BenchmarkTreeDBStoreAdapterOverhead/WriteBatch16/TreeDBStore | 95941–102958 | 111956–116158 | 14415 → 13902 (-3.559%) | 18597 → 19597 (+5.377%) | 62 → 62 (+0.000%) |
