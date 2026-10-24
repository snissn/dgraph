#!/usr/bin/env python3
"""Read retained, complete microbench logs; no execution or acceptance verdict."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

EXPECTED = {
    *("BenchmarkCommitEventDisabledAndUnsubscribed/" + x for x in
      ("direct", "disabled", "unsubscribed")),
    *("BenchmarkBadgerStoreSeam/" + shape + "/" + mode for shape in
      ("point-read", "bounded-iterator", "exact-key-all-versions", "write", "batch-write")
      for mode in ("direct", "seam")),
    *("BenchmarkTreeDBStoreAdapterOverhead/" + shape + "/" + mode for shape in
      ("PointGet", "AllVersionsScan", "RandomSeek", "WriteBatch1", "WriteBatch16",
       "ExactKeyAllVersions", "Reopen") for mode in ("DirectMVCC", "TreeDBStore")),
    "BenchmarkTreeDBStoreAdapterOverhead/Reopen/PairedLatency",
}


def read_log(path):
    data = path.read_bytes()
    lines = data.decode("utf-8").splitlines()
    if "PASS" not in lines or not any(x.startswith("ok") and
            "github.com/dgraph-io/dgraph/v25/posting" in x for x in lines):
        raise ValueError(f"incomplete/unsuccessful package output: {path}")
    if any(x.startswith(("FAIL", "--- FAIL", "panic:")) for x in lines):
        raise ValueError(f"failed package output: {path}")
    groups = {}
    for number, line in enumerate(lines, 1):
        if not line.startswith("Benchmark"):
            continue
        fields = line.split()
        match = re.fullmatch(r"(.+)-(\d+)", fields[0])
        if not match or len(fields) < 8 or len(fields) % 2:
            raise ValueError(f"malformed sample: {path}:{number}")
        name, cpu = match[1], int(match[2])
        n = int(fields[1])
        if name not in EXPECTED or cpu != 2 or n <= 0:
            raise ValueError(f"unexpected cell/CPU/N: {path}:{number}")
        metrics = {}
        for index in range(2, len(fields), 2):
            value, unit = float(fields[index]), fields[index + 1]
            if not math.isfinite(value) or unit in metrics:
                raise ValueError(f"invalid metric: {path}:{number}")
            metrics[unit] = value
        if not {"ns/op", "B/op", "allocs/op"} <= metrics.keys():
            raise ValueError(f"missing standard metrics: {path}:{number}")
        if any(metrics[k] < 0 for k in ("ns/op", "B/op", "allocs/op")):
            raise ValueError(f"negative standard metric: {path}:{number}")
        groups.setdefault(name, []).append({"line": number, "raw": line,
            "iterations": n, "cpu_suffix": cpu, "metrics": metrics})
    if set(groups) != EXPECTED or any(len(v) != 5 for v in groups.values()):
        raise ValueError(f"expected all 28 cells with five samples each: {path}")
    for name, rows in groups.items():
        if any(row["metrics"].keys() != rows[0]["metrics"].keys() for row in rows):
            raise ValueError(f"inconsistent metric set: {name}")
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(data).hexdigest(),
            "sample_count": 140, "cells": groups}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--baseline", type=Path, required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--output-prefix", type=Path, required=True)
    args = p.parse_args()
    outputs = [Path(str(args.output_prefix) + suffix) for suffix in (".json", ".md")]
    if any(path.exists() for path in outputs):
        raise ValueError("refusing existing output")
    baseline, candidate = read_log(args.baseline), read_log(args.candidate)
    cells = []
    markdown = ["# Final supplementary microbenchmark comparison", "",
        "Descriptive only. Whole campaign STOP remains locked; no acceptance verdict. "
        "All five samples per cell are retained in JSON. Count-5 samples are not paired AB observations.", "",
        "| Cell | baseline N range | candidate N range | ns/op baseline → candidate (change) | "
        "B/op baseline → candidate (change) | allocs/op baseline → candidate (change) |", "|---|---|---|---|---|---|"]
    for name in sorted(EXPECTED):
        a, b = baseline["cells"][name], candidate["cells"][name]
        if a[0]["metrics"].keys() != b[0]["metrics"].keys():
            raise ValueError(f"cross-source metric set differs: {name}")
        entry = {"name": name, "baseline_samples": a, "candidate_samples": b,
                 "descriptive_comparison": {}, "review_increases": []}
        rendered = []
        for unit in ("ns/op", "B/op", "allocs/op", "disk-B/op", "disk-B/item"):
            if unit not in a[0]["metrics"]:
                continue
            av = statistics.median(row["metrics"][unit] for row in a)
            bv = statistics.median(row["metrics"][unit] for row in b)
            delta = bv - av
            percent = (bv / av - 1) * 100 if av > 0 else None
            entry["descriptive_comparison"][unit] = {"baseline_median": av,
                "candidate_median": bv, "absolute_change": delta, "percent_change": percent,
                "ratio_status": "AVAILABLE" if av > 0 else "UNQUALIFIED_ZERO_BASELINE"}
            if delta > 0:
                entry["review_increases"].append(unit)
            if unit in ("ns/op", "B/op", "allocs/op"):
                change = f"{percent:+.3f}%" if percent is not None else f"{delta:+g}; zero baseline"
                rendered.append(f"{av:g} → {bv:g} ({change})")
        ranges = [f"{min(r['iterations'] for r in rows)}–{max(r['iterations'] for r in rows)}" for rows in (a, b)]
        markdown.append("| " + " | ".join([name, *ranges, *rendered]) + " |")
        cells.append(entry)
    result = {"schema_version": 1, "overall_pass": None,
        "campaign_disposition": "STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE",
        "scope": "supplementary microbenchmarks; no Dgraph operation allocation inference",
        "baseline": baseline, "candidate": candidate, "comparisons": cells}
    with outputs[0].open("x") as f:
        json.dump(result, f, indent=2, allow_nan=False)
        f.write("\n")
    with outputs[1].open("x") as f:
        f.write("\n".join(markdown) + "\n")


if __name__ == "__main__":
    main()
