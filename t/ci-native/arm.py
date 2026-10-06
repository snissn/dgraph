# SPDX-License-Identifier: Apache-2.0
"""Exact integration-package selection and the stable ARM CI gate."""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import signal
import subprocess
import sys
from datetime import datetime


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def parse_packages(text):
    tasks = []
    for line in text.splitlines():
        if "Found valid task:" in line:
            match = re.fullmatch(
                r"Found valid task: (github\.com/dgraph-io/dgraph/v25/[\w./-]+) isCommon:(true|false)",
                line,
            )
            if not match:
                raise ValueError("malformed discovery record")
            tasks.append(match[1])
    totals = re.findall(r"^Running tests for (\d+) packages\.$", text, re.M)
    if not tasks or len(tasks) != len(set(tasks)) or totals != [str(len(tasks))]:
        raise ValueError("empty, duplicate or incomplete package discovery")
    return sorted(tasks)


def selection(universe, shard):
    if not universe or len(universe) != len(set(universe)):
        raise ValueError("invalid universe")
    selected = [p for p in universe if shard == "full" or p.endswith("/vector") == (shard == "vectors")]
    if shard not in ("full", "vectors", "remainder") or not selected:
        raise ValueError("empty or unknown required shard")
    return selected


def identity(root):
    return {
        "checkout": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "pr_head": os.environ["PR_HEAD"],
        "tags": os.environ.get("GO_RUN_TAGS", ""),
        "inputs": {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in (
            "t/t.go", "t/hooks.go", "t/ci-native/arm.py", "go.mod", "go.sum"
        )},
    }


def discover(root, output, packages=None):
    argv = ["./t", "--list-packages", "--suite=integration"]
    if packages is not None:
        argv.append("--pkg=" + ",".join(packages))
    result = subprocess.run(argv, cwd=root / "t", capture_output=True, timeout=120)
    output.with_suffix(".stdout").write_bytes(result.stdout)
    output.with_suffix(".stderr").write_bytes(result.stderr)
    if result.returncode:
        raise ValueError("runner discovery failed: " + str(result.returncode))
    return parse_packages(result.stdout.decode())


def run(shard, destination):
    root = pathlib.Path.cwd()
    destination.mkdir(parents=True, exist_ok=False)
    source = identity(root)
    if source["checkout"] != os.environ["GITHUB_SHA"] or platform.machine() not in ("aarch64", "arm64"):
        raise ValueError("checkout or architecture mismatch")
    universe = discover(root, destination / "universe")
    selected = selection(universe, shard)
    if discover(root, destination / "selected", selected) != selected:
        raise ValueError("full package IDs did not select the exact shard")
    command = ["./t"] if shard == "full" else ["./t", "--suite=integration", "--pkg=" + ",".join(selected)]
    receipt = {"schema": 1, "shard": shard, "source": source, "universe": universe,
               "coverage_sha256": digest(universe), "selected": selected, "command": command,
               "raws": {q.name: hashlib.sha256(q.read_bytes()).hexdigest() for q in destination.iterdir()},
               "run_id": os.environ["GITHUB_RUN_ID"], "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
               "exit": None, "signal": None}
    path = destination / "manifest.json"
    path.write_text(json.dumps(receipt, indent=2) + "\n")
    child = None
    received = []

    def forward(number, _frame):
        received.append(number)
        try:
            os.killpg(child.pid, number) if child is not None else None
        except ProcessLookupError:
            pass

    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, forward)
    if received:
        return 128 + received[0]
    child = subprocess.Popen(command, cwd=root / "t", start_new_session=True)
    if received:
        forward(received[0], None)
    status = child.wait()
    receipt["signal"] = received[0] if received else None
    receipt["exit"] = 128 + received[0] if received else (128 - status if status < 0 else status)
    if identity(root) != source:
        receipt["exit"] = 1
        receipt["error"] = "source changed during test execution"
    path.write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt["exit"]


def validate_jobs(jobs, expected, source, run_id, attempt, prefix, deadline):
    """Bind every required successful native job and its complete wall-clock span."""
    relevant = [j for j in jobs if j["name"].startswith(prefix)]
    if {j["name"] for j in relevant} != {prefix + s for s in expected} or len(relevant) != len(expected):
        raise ValueError("missing or duplicate shard jobs")
    starts, ends = [], []
    for shard in expected:
        j = next(j for j in relevant if j["name"] == prefix + shard)
        if (j["status"] != "completed" or j["conclusion"] != "success"
                or j["head_sha"] != source["pr_head"] or str(j["run_id"]) != str(run_id)
                or str(j["run_attempt"]) != str(attempt)):
            raise ValueError("failed, skipped, cancelled or wrong-head shard job")
        start = datetime.fromisoformat(j["started_at"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(j["completed_at"].replace("Z", "+00:00"))
        if start.tzinfo is None or end.tzinfo is None or end < start:
            raise ValueError("invalid job clock")
        starts.append(start)
        ends.append(end)
    span = (max(ends) - min(starts)).total_seconds()
    if span > deadline:
        raise ValueError("whole native gate exceeded original deadline")
    return span


def validate_gate(manifests, jobs, source, public, run_id, attempt):
    expected = {"vectors", "remainder"} if public else {"full"}
    if {m["shard"] for m in manifests} != expected or len(manifests) != len(expected):
        raise ValueError("missing or duplicate shard manifests")
    first = manifests[0]["universe"]
    if not first or len(set(first)) != len(first):
        raise ValueError("invalid full package universe")
    combined = []
    span = validate_jobs(jobs, expected, source, run_id, attempt, "arm-", 3600)
    for m in manifests:
        if (m["schema"] != 1 or m["source"] != source or m["universe"] != first
                or m["coverage_sha256"] != digest(first) or m["selected"] != selection(first, m["shard"])
                or m["exit"] != 0 or m["signal"] is not None or "error" in m
                or str(m["run_id"]) != str(run_id) or str(m["attempt"]) != str(attempt)):
            raise ValueError("shard failed or source/coverage/run binding mismatch")
        command = ["./t"] if m["shard"] == "full" else ["./t", "--suite=integration", "--pkg=" + ",".join(m["selected"])]
        if m["command"] != command:
            raise ValueError("test command mismatch")
        combined.extend(m["selected"])
    if sorted(combined) != first or len(combined) != len(set(combined)):
        raise ValueError("shards are not a disjoint complete union")
    return {"status": "PASS", "packages": len(first), "shards": sorted(expected), "whole_gate_seconds": span}


def load_manifests(directory):
    manifests = []
    for path in directory.glob("*/manifest.json"):
        manifest = json.loads(path.read_text())
        expected_raws = {"universe.stdout", "universe.stderr", "selected.stdout", "selected.stderr"}
        if set(manifest["raws"]) != expected_raws:
            raise ValueError("missing query bindings")
        for name, checksum in manifest["raws"].items():
            if hashlib.sha256((path.parent / name).read_bytes()).hexdigest() != checksum:
                raise ValueError("query artifact drift")
        if (parse_packages((path.parent / "universe.stdout").read_text()) != manifest["universe"]
                or parse_packages((path.parent / "selected.stdout").read_text()) != manifest["selected"]):
            raise ValueError("archived query coverage mismatch")
        manifests.append(manifest)
    return manifests


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--shard", choices=("vectors", "remainder", "full"), required=True)
    run_parser.add_argument("--output", type=pathlib.Path, required=True)
    gate = sub.add_parser("gate")
    gate.add_argument("--artifacts", type=pathlib.Path, required=True)
    gate.add_argument("--jobs", type=pathlib.Path, required=True)
    gate.add_argument("--public", choices=("true", "false"), required=True)
    args = parser.parse_args()
    if args.mode == "run":
        return run(args.shard, args.output)
    manifests = load_manifests(args.artifacts)
    pages = json.loads(args.jobs.read_text())
    jobs = [job for page in pages for job in page["jobs"]]
    source = identity(pathlib.Path.cwd())
    if source["checkout"] != os.environ["GITHUB_SHA"]:
        raise ValueError("aggregate checkout mismatch")
    result = validate_gate(manifests, jobs, source, args.public == "true", os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"])
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print("ARM gate refused: " + str(error), file=sys.stderr)
        sys.exit(1)
