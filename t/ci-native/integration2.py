# SPDX-License-Identifier: Apache-2.0
"""Full integration2 package execution and the original 90-minute native gate."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import shlex
import signal
import subprocess
import sys
import time

import arm

MODULE = "github.com/dgraph-io/dgraph/v25"
NAMESPACE = MODULE + "/systest/online-restore/namespace-aware"
QUERY = ["go", "list", "-mod=readonly", "-json", "-tags=integration2"]
PUBLIC_SECONDS = 45 * 60


def identity(root):
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")[:-1]
    files = {p: {"sha256": hashlib.sha256((root / p).read_bytes()).hexdigest(),
                 "mode": (root / p).stat().st_mode & 0o777} for p in paths}
    return {"checkout": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
            "pr_head": os.environ["PR_HEAD"], "tags": "integration2", "inputs_sha256": arm.digest(files),
            "go_version": "go" + re.search(r"^go ([0-9.]+)$", (root / "go.mod").read_text(), re.M)[1]}


def validate_tool(tool, source):
    flags = shlex.split(tool["GOFLAGS"])
    if (tool["GOVERSION"] != source["go_version"] or tool["GOOS"] != "linux" or tool["GOARCH"] != "amd64"
            or tool["CGO_ENABLED"] != "1" or tool["GOEXPERIMENT"]
            or any(flag != "-mod=readonly" and not re.fullmatch(r"-p=[1-9][0-9]*", flag) for flag in flags)):
        raise ValueError("native toolchain, architecture or ambient flag mismatch")


def parse_packages(text):
    packages = []
    decoder = json.JSONDecoder()
    text = text.lstrip()
    while text:
        record, end = decoder.raw_decode(text)
        if not isinstance(record, dict):
            raise ValueError("malformed package discovery record")
        module = record.get("Module", {})
        if not isinstance(module, dict):
            raise ValueError("malformed main-module record")
        name = record.get("ImportPath", "")
        if (record.get("Error") or record.get("DepsErrors") or record.get("Incomplete")
                or not module.get("Main") or module.get("Path") != MODULE
                or not isinstance(name, str) or not (name == MODULE or name.startswith(MODULE + "/"))):
            raise ValueError("failed or foreign full-package discovery")
        packages.append(name)
        text = text[end:].lstrip()
    if not packages or len(packages) != len(set(packages)):
        raise ValueError("empty or duplicate full-package discovery")
    return sorted(packages)


def selection(universe, shard):
    if not universe or len(universe) != len(set(universe)) or NAMESPACE not in universe:
        raise ValueError("invalid full universe or missing namespace package")
    if shard == "full":
        selected = universe
    elif shard == "namespace":
        selected = [NAMESPACE]
    elif shard == "remainder":
        selected = [p for p in universe if p != NAMESPACE]
    else:
        raise ValueError("unknown shard")
    if not selected:
        raise ValueError("empty required shard")
    return selected


def command(selected, public):
    if public:
        return ["go", "test", "-p=1", "-v", "-timeout=40m", "-failfast", "-tags=integration2", *selected]
    return ["go", "test", "-v", "-timeout=90m", "-failfast", "-tags=integration2", "./..."]


def discover(root, destination, name, packages):
    argv = [*QUERY, *packages]
    result = None
    error = None
    try:
        result = subprocess.run(argv, cwd=root, capture_output=True, timeout=120)
        stdout, stderr = result.stdout, result.stderr
    except subprocess.TimeoutExpired as exc:
        stdout, stderr = exc.stdout or b"", exc.stderr or b""
        error = "package query timed out"
    destination.joinpath(name + ".stdout").write_bytes(stdout)
    destination.joinpath(name + ".stderr").write_bytes(stderr)
    receipt = {"argv": argv, "exit": None if result is None else result.returncode, "error": error}
    destination.joinpath(name + ".json").write_text(json.dumps(receipt, indent=2) + "\n")
    if error or result.returncode:
        raise ValueError(error or "package query failed: " + str(result.returncode))
    return parse_packages(stdout.decode())


def execute(argv, destination, public):
    """Regular-file native output; cancellation and cutoff can never become success."""
    received = []
    child = sampler = None
    previous = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}
    timed_out = False

    def forward(number, _frame):
        received.append(number)
        if child is not None:
            try:
                os.killpg(child.pid, number)
            except ProcessLookupError:
                pass

    for number in previous:
        signal.signal(number, forward)
    try:
        with (destination / "test.stdout").open("xb") as stdout, (destination / "test.stderr").open("xb") as stderr:
            with (destination / "observer.stdout").open("xb") as obsout, (destination / "observer.stderr").open("xb") as obserr:
                if public:
                    sampler = subprocess.Popen([sys.executable, str(pathlib.Path(__file__).with_name("observe.py")),
                                                "--sample", "--ticks", "180", "--output", str(destination / "resources.jsonl")],
                                               stdout=obsout, stderr=obserr, start_new_session=True)
                if received:
                    return {"exit": 128 + received[0], "signal": received[0], "timed_out": False}
                child = subprocess.Popen(argv, stdout=stdout, stderr=stderr, start_new_session=True)
                if received:
                    forward(received[0], None)
                try:
                    status = child.wait(timeout=PUBLIC_SECONDS if public else None)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    os.killpg(child.pid, signal.SIGQUIT)
                    try:
                        status = child.wait(timeout=90)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        status = child.wait()
        return {"exit": 124 if timed_out else (128 + received[0] if received else (128 - status if status < 0 else status)),
                "signal": received[0] if received else (-status if status < 0 else None), "timed_out": timed_out}
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        if sampler is not None:
            sampler.terminate()
            try:
                sampler.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(sampler.pid, signal.SIGKILL)
                sampler.wait()
        for number, handler in previous.items():
            signal.signal(number, handler)


def run(shard, destination, public):
    root = pathlib.Path.cwd()
    destination.mkdir(parents=True, exist_ok=False)
    receipt = {"schema": 1, "shard": shard, "public": public,
               "run_id": os.environ["GITHUB_RUN_ID"], "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
               "exit": None, "signal": None, "source_stable": False}
    path = destination / "manifest.json"
    start = time.monotonic()
    try:
        source = identity(root)
        receipt["source"] = source
        if source["checkout"] != os.environ["GITHUB_SHA"]:
            raise ValueError("checkout mismatch")
        subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", "go.mod", "go.sum"], check=True)
        profile = os.environ.get("DGRAPH_CI_LOCAL_ALPHA_CACHE_MB", "")
        if profile != ("512" if public else "") or shard not in (("namespace", "remainder") if public else ("full",)):
            raise ValueError("public fixture profile or shard mismatch")
        tool_argv = ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOEXPERIMENT"]
        tool = subprocess.run(tool_argv, capture_output=True, timeout=30)
        (destination / "tool.stdout").write_bytes(tool.stdout)
        (destination / "tool.stderr").write_bytes(tool.stderr)
        (destination / "tool.json").write_text(json.dumps({"argv": tool_argv, "exit": tool.returncode}) + "\n")
        if tool.returncode:
            raise ValueError("Go environment query failed")
        receipt["go_env"] = json.loads(tool.stdout)
        validate_tool(receipt["go_env"], source)
        universe = discover(root, destination, "universe", ["./..."])
        selected = selection(universe, shard)
        if discover(root, destination, "selected", selected) != selected:
            raise ValueError("package arguments did not select the exact shard")
        receipt.update(universe=universe, selected=selected, coverage_sha256=arm.digest(universe),
                       command=command(selected, public), cache_profile=profile)
        if identity(root) != source:
            raise ValueError("source changed during discovery")
        path.write_text(json.dumps(receipt, indent=2) + "\n")
        receipt.update(execute(receipt["command"], destination, public))
        receipt["source_stable"] = identity(root) == source
        if not receipt["source_stable"]:
            raise ValueError("source changed during test execution")
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        receipt.update(exit=receipt["exit"] or 1, error=str(error))
        print("integration2 refused: " + str(error), file=sys.stderr)
    finally:
        receipt["native_seconds"] = time.monotonic() - start
        receipt["raws"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in destination.iterdir() if p.name != "manifest.json"}
        path.write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt["exit"]


def validate_gate(manifests, jobs, source, public, run_id, attempt):
    expected = {"namespace", "remainder"} if public else {"full"}
    if {m["shard"] for m in manifests} != expected or len(manifests) != len(expected):
        raise ValueError("missing or duplicate full-execution manifests")
    universe = manifests[0]["universe"]
    selected = []
    go_env = manifests[0]["go_env"]
    for manifest in manifests:
        packages = selection(universe, manifest["shard"])
        validate_tool(manifest["go_env"], source)
        if (manifest["schema"] != 1 or manifest["source"] != source or manifest["public"] != public
                or manifest["go_env"] != go_env or manifest["cache_profile"] != ("512" if public else "")
                or manifest["universe"] != universe or manifest["coverage_sha256"] != arm.digest(universe)
                or manifest["selected"] != packages or manifest["command"] != command(packages, public)
                or manifest["exit"] != 0 or manifest["signal"] is not None or manifest.get("timed_out")
                or not manifest["source_stable"] or "error" in manifest
                or str(manifest["run_id"]) != str(run_id) or str(manifest["attempt"]) != str(attempt)):
            raise ValueError("native failure or source/coverage/profile/run mismatch")
        selected.extend(packages)
    if sorted(selected) != universe or len(selected) != len(set(selected)):
        raise ValueError("native shards are not a disjoint complete full universe")
    span = arm.validate_jobs(jobs, expected, source, run_id, attempt, "integration2-", 5400)
    return {"status": "PASS", "packages": len(universe), "shards": sorted(expected), "whole_gate_seconds": span}


def load_manifests(directory):
    manifests = []
    required = {"universe.stdout", "universe.stderr", "universe.json", "selected.stdout", "selected.stderr", "selected.json",
                "test.stdout", "test.stderr", "observer.stdout", "observer.stderr", "tool.stdout", "tool.stderr", "tool.json"}
    for path in directory.glob("*/manifest.json"):
        manifest = json.loads(path.read_text())
        if not required <= set(manifest["raws"]) or set(manifest["raws"]) - required - {"resources.jsonl"}:
            raise ValueError("missing or unknown raw execution bindings")
        for name, checksum in manifest["raws"].items():
            if hashlib.sha256((path.parent / name).read_bytes()).hexdigest() != checksum:
                raise ValueError("raw execution artifact drift")
        tool_query = json.loads((path.parent / "tool.json").read_text())
        if (tool_query != {"argv": ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOEXPERIMENT"], "exit": 0}
                or json.loads((path.parent / "tool.stdout").read_text()) != manifest["go_env"]):
            raise ValueError("failed or rebound toolchain query")
        for name, arguments, packages in (("universe", ["./..."], manifest["universe"]),
                                         ("selected", manifest["selected"], manifest["selected"])):
            query = json.loads((path.parent / (name + ".json")).read_text())
            if query != {"argv": [*QUERY, *arguments], "exit": 0, "error": None}:
                raise ValueError("failed or rebound archived discovery command")
            if parse_packages((path.parent / (name + ".stdout")).read_text()) != packages:
                raise ValueError("archived full package coverage mismatch")
        manifests.append(manifest)
    return manifests


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_subparsers(dest="mode", required=True)
    run_parser = modes.add_parser("run")
    run_parser.add_argument("--shard", choices=("namespace", "remainder", "full"), required=True)
    run_parser.add_argument("--output", type=pathlib.Path, required=True)
    run_parser.add_argument("--public", choices=("true", "false"), required=True)
    gate = modes.add_parser("gate")
    gate.add_argument("--artifacts", type=pathlib.Path, required=True)
    gate.add_argument("--jobs", type=pathlib.Path, required=True)
    gate.add_argument("--public", choices=("true", "false"), required=True)
    args = parser.parse_args()
    if args.mode == "run":
        return run(args.shard, args.output, args.public == "true")
    source = identity(pathlib.Path.cwd())
    if source["checkout"] != os.environ["GITHUB_SHA"]:
        raise ValueError("aggregate checkout mismatch")
    pages = json.loads(args.jobs.read_text())
    jobs = [job for page in pages for job in page["jobs"]]
    result = validate_gate(load_manifests(args.artifacts), jobs, source, args.public == "true",
                           os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"])
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print("integration2 gate refused: " + str(error), file=sys.stderr)
        sys.exit(1)
