# Final analyzer pre-baseline source review

Source-only review; analyzer not executed. Full baseline archive has not arrived locally. The only baseline preview currently present is microbench raw text, not live result JSON; no anticipated matrix numbers were invented.

- Analyzer SHA256: `0375c0eb19e9f8b3eac258b134e376bfa19dde8f1e2335b247d2e10bd875d892`.
- Frozen mapping SHA256: `4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2`.

The current arithmetic matches the selected mapping. Each arm requires all 12 original rows, exactly three repeats per backend/class, plus exactly two separately excluded profiles. Candidate/baseline manifest identities remain distinct. Actual TreeDB COW witnesses are required at startup and restart; Badger is not required to carry TreeDB-only selector fields.

Within-arm throughput uses ratio of independent medians, TreeDB/Badger >=0.97. Filesystem source comparisons match identical backend/class/repeat and keep every `100*(candidate/baseline-1)` value. Ancillary paired median >5% is FAIL/review; smaller changes are not acceptance. Throughput has no new 5% allowance: any negative repeat is marked for review. Zero/missing/unavailable/unit-mismatched ratio operands make the full three-pair statistic UNQUALIFIED. No partial median or Badger-drift normalization occurs.

The final review must show both TreeDB and Badger source-paired outcomes even if they drift in opposite directions. Relative TreeDB/Badger improvement in a later arm cannot erase absolute TreeDB throughput or recovery regression against the original source. Process/filesystem contrasts differ in survival/owner and remain descriptive. Legacy HTTP-health recovery remains separate from aligned read/read-write phases.

The analyzer retains all 36 complete results, six excluded profile results, original report/receipt hashes, absolute operational metrics and availability. Its output is supplementary: whole-campaign STOP is hard-coded and it emits no overall PASS. Later original ADVANCE labels cannot change that disposition.

No concrete arithmetic/input-shape blocker found in this read. The analyzer deliberately does not replace independent archive integrity, runtime-sidecar/log hashing, clean source/build/module authenticity, or original report review. The upcoming baseline audit must verify those and compare logical checksums/node counts across arms; the script currently enforces logical parity within each arm, not cross-arm equality. Its stored profile result check also does not authenticate profile data itself. Those are audit responsibilities, not grounds to infer acceptance from script success.

Ready for the final baseline archive integrity/all-12-row/runtime audit and independent offline arithmetic review once the coordinator releases the actual archived packet. No runtime, SSH, Go, benchmark, GitHub, source or state operation was performed.
