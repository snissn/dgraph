#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Public authenticated floor ACK, immediate crash, and certified recovery proof.

Correctness diagnostic only; no performance or power-loss qualification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.error
import urllib.request


def request(base, path, body, token="", content="application/json", allow_error=False):
    data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data,
        headers={"Content-Type": content, "X-Dgraph-AuthToken": token})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        result = {"errors": [error.read().decode()]}
    if not allow_error and result.get("errors"):
        raise RuntimeError(str(result))
    return result


def healthy(process, base):
    until = time.monotonic() + 90
    while time.monotonic() < until:
        if process.poll() is not None:
            raise RuntimeError(f"server exited {process.returncode}")
        try:
            with urllib.request.urlopen(base + "/health", timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, ValueError):
            pass
        time.sleep(.1)
    raise RuntimeError("health timeout")


def stop(process):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dgraph-bin", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--zero-offset", type=int, default=39000)
    parser.add_argument("--alpha-offset", type=int, default=40000)
    args = parser.parse_args()
    binary, out = Path(args.dgraph_bin).resolve(), Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    token = "floor-diagnostic-token"
    result = {"success": False, "diagnostic_only": True, "commands": [],
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "contract": "authenticated explicit S; ordered apply+certificate before ACK; immediate both-server SIGKILL"}
    logs = []
    zero = alpha = None
    base = f"http://localhost:{8080 + args.alpha_offset}"
    zero_args = ["zero", "--wal", str(out / "zw"), "--replicas=1", f"--port_offset={args.zero_offset}", "--survive=filesystem"]
    alpha_args = ["alpha", "--zero", f"localhost:{5080 + args.zero_offset}", "--postings", str(out / "p"),
                  "--wal", str(out / "w"), f"--port_offset={args.alpha_offset}", "--survive=filesystem",
                  "--posting-store", "backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true",
                  "--security", f"token={token}", "--badger", "syncwrites=false"]
    def start(label, command):
        log = (out / (label + ".log")).open("xb")
        logs.append(log)
        result["commands"].append([str(binary)] + command)
        return subprocess.Popen([str(binary)] + command, stdout=log, stderr=log)
    def start_pair(label):
        nonlocal zero, alpha
        zero = start("zero-" + label, zero_args)
        healthy(zero, f"http://localhost:{6080 + args.zero_offset}")
        alpha = start("alpha-" + label, alpha_args)
        healthy(alpha, base)
    def floor_request(floor, auth=token, allow_error=False):
        return request(base, "/admin", {"query": "mutation($s:UInt64!){retirePostingHistory(floor:$s){floor response{code message}}}",
                                       "variables": {"s": floor}}, auth, allow_error=allow_error)
    try:
        start_pair("initial")
        result["unauthenticated"] = floor_request(1, "", True)
        result["wrong_token"] = floor_request(1, "wrong", True)
        for key in ["unauthenticated", "wrong_token"]:
            if "AuthToken" not in str(result[key]):
                raise RuntimeError(f"missing auth refusal: {result[key]}")
        request(base, "/alter", "floor_value: string @index(exact) .", token, "application/rdf")
        mutation = request(base, "/mutate?commitNow=true", {"set": [{"uid": "_:floor", "floor_value": "preserved"}]}, token)
        result["mutation"] = mutation
        floor = int(mutation["extensions"]["txn"]["start_ts"])
        uid = mutation["data"]["uids"]["floor"]
        result["too_high"] = floor_request((1 << 64) - 1, allow_error=True)
        if "precede assigned" not in str(result["too_high"]):
            raise RuntimeError(f"missing high-floor refusal: {result['too_high']}")
        # CommitNow's Zero ACK can precede Alpha's ordered Delta. Retry the
        # same operator-selected floor; no later local frontier chooses it.
        result["floor_retries"] = []
        deadline = time.monotonic() + 30
        while True:
            acknowledged = floor_request(floor, allow_error=True)
            if not acknowledged.get("errors"):
                break
            result["floor_retries"].append(acknowledged)
            if "precede assigned" not in str(acknowledged) or time.monotonic() >= deadline:
                raise RuntimeError(f"floor never became eligible: {acknowledged}")
            time.sleep(.1)
        # No filesystem/network work before these adjacent termination syscalls.
        alpha.send_signal(signal.SIGKILL)
        zero.send_signal(signal.SIGKILL)
        alpha.wait(timeout=15)
        zero.wait(timeout=15)
        result["floor_response"], result["floor"] = acknowledged, floor
        start_pair("restart")
        recovered = (out / "alpha-restart.log").read_text()
        match = re.search(r"recovery: certified=(\d+) oracle=(\d+)", recovered)
        if not match or int(match[1]) == 0:
            raise RuntimeError("missing nonzero certified recovery")
        result["recovered_index"] = int(match[1])
        stale = request(base, f"/query?startTs={floor}", "{q(func:uid(" + uid + ")){uid floor_value}}", token,
                        "application/dql", allow_error=True)
        result["stale_read"] = stale
        if "replicated posting floor" not in str(stale):
            raise RuntimeError(f"old read was not rejected after recovery: {stale}")
        result["stale_floor"] = floor_request(floor - 1, allow_error=True)
        if "regression" not in str(result["stale_floor"]):
            raise RuntimeError(f"missing regressing-floor refusal: {result['stale_floor']}")
        result["idempotent"] = floor_request(floor)
        got = request(base, "/query", '{q(func:eq(floor_value,"preserved")){uid floor_value}}', token, "application/dql")
        if got["data"]["q"] != [{"uid": uid, "floor_value": "preserved"}]:
            raise RuntimeError(f"admissible indexed read mismatch: {got}")
        result["success"] = True
    except Exception as error:
        result["error"] = repr(error)
    finally:
        stop(alpha)
        stop(zero)
        for log in logs:
            log.close()
        (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
