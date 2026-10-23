#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Correctness only: terminate both servers immediately after an HTTP response.

No read, validation drain, checkpoint poll, or artifact write occurs between the
CommitNow response and SIGKILL. This tests process-crash recovery, not power loss.
"""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from run import file_hash, healthy, request, stop, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dgraph-bin", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--zero-offset", type=int, default=34000)
    parser.add_argument("--alpha-offset", type=int, default=35000)
    parser.add_argument("--ack-cuts", type=int, default=3)
    args = parser.parse_args()
    if args.ack_cuts < 1:
        parser.error("ack-cuts must be positive")
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = Path(args.dgraph_bin).resolve()
    result = {"success": False, "diagnostic_only": True, "binary_sha256": file_hash(binary),
              "contract": "HTTP CommitNow then immediate both-server SIGKILL; no intermediate drain",
              "config": vars(args), "commands": [], "cuts": [], "host_load_start": os.getloadavg()}
    zero = alpha = None
    logs = []
    zero_args = ["zero", "--wal", str(out / "zw"), "--replicas=1",
                 f"--port_offset={args.zero_offset}", "--survive=filesystem"]
    alpha_args = ["alpha", "--zero", f"localhost:{5080 + args.zero_offset}",
                  "--postings", str(out / "p"), "--wal", str(out / "w"),
                  f"--port_offset={args.alpha_offset}", "--survive=filesystem",
                  "--posting-store", "backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true",
                  "--badger", "syncwrites=false"]
    base = f"http://localhost:{8080 + args.alpha_offset}"

    def start(name, command):
        log = (out / f"{name}.log").open("xb")
        logs.append(log)
        result["commands"].append([str(binary)] + command)
        return subprocess.Popen([str(binary)] + command, stdout=log, stderr=log)

    def start_pair(label):
        nonlocal zero, alpha
        zero = start(f"zero-{label}", zero_args)
        healthy(zero, f"http://localhost:{6080 + args.zero_offset}")
        alpha = start(f"alpha-{label}", alpha_args)
        healthy(alpha, base)

    def abrupt_cut(label, response_at):
        # Keep these syscalls adjacent to the response: no network or filesystem
        # work may be inserted before termination in this correctness driver.
        alpha.send_signal(signal.SIGKILL)
        zero.send_signal(signal.SIGKILL)
        killed_at = time.perf_counter_ns()
        alpha.wait(timeout=15)
        zero.wait(timeout=15)
        result["cuts"].append({"label": label, "response_to_kill_ns": killed_at - response_at})
        write_json(out / "result.json", result)

    expected = {}
    try:
        start_pair("initial")
        # This reaches admission before any schema exists for either mixed edge.
        rejection = None
        try:
            request(base, "/mutate?commitNow=true",
                    '{ set {\n_:first <recovery_mixed> _:second .\n_:first <recovery_mixed> "scalar" .\n} }',
                    "application/rdf")
        except RuntimeError as err:
            rejection = str(err)
        response_at = time.perf_counter_ns()
        if rejection is None or "scalar" not in rejection or "uid" not in rejection:
            raise RuntimeError(f"expected mixed UID/scalar semantic rejection, got {rejection!r}")
        result["rejection"] = rejection
        abrupt_cut("rejection", response_at)
        start_pair("after-rejection")
        schema = request(base, "/query", "schema(pred: [recovery_mixed]) { predicate type }", "application/dql")
        if schema["data"].get("schema"):
            raise RuntimeError("rejected mutation left an inferred schema after replay")

        request(base, "/alter", "recovery_value: string @index(exact) .", "application/rdf")

        for cut in range(args.ack_cuts):
            value = f"immediate-ack-{cut}"
            response = request(base, "/mutate?commitNow=true",
                               {"set": [{"uid": "_:ack", "recovery_value": value}]})
            response_at = time.perf_counter_ns()
            abrupt_cut(f"ack-{cut}", response_at)
            uid = response["data"]["uids"]["ack"]
            expected[uid] = value
            write_json(out / "acknowledged.json", expected)
            start_pair(f"after-ack-{cut}")
            for expected_uid, expected_value in expected.items():
                got = request(base, "/query", "{ q(func: uid(" + expected_uid +
                              ")) { uid recovery_value } }", "application/dql")
                if got["data"]["q"] != [{"uid": expected_uid, "recovery_value": expected_value}]:
                    raise RuntimeError(f"acknowledged value lost after {cut}: {got}")
                indexed = request(base, "/query", '{ q(func: eq(recovery_value, "' + expected_value +
                                  '")) { uid recovery_value } }', "application/dql")
                if indexed["data"]["q"] != got["data"]["q"]:
                    raise RuntimeError(f"fresh exact index mismatch after {cut}: {indexed}")
            log = (out / f"alpha-after-ack-{cut}.log").read_text()
            recovered = re.search(r"recovery: certified=(\d+) oracle=(\d+)", log)
            replayed = re.search(r"committed replay completed through (\d+) before health", log)
            if not recovered or not replayed:
                raise RuntimeError("missing certified-recovery/before-health replay evidence")
            result["cuts"][-1]["recovered_index"] = int(recovered[1])
            result["cuts"][-1]["replayed_through"] = int(replayed[1])
        result["success"] = True
    except Exception as err:
        result["error"] = repr(err)
    finally:
        stop(alpha)
        stop(zero)
        for log in logs:
            log.close()
        result["host_load_end"] = os.getloadavg()
        write_json(out / "result.json", result)
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
