#!/usr/bin/env python3
"""Diagnostic real-server workload and abrupt recovery proof, never promotion evidence.

Uses the same client contract in all arms: HTTP CommitNow, UID point/one-hop
reads, one Zero and one Alpha, both with filesystem survival. The candidate has
a different *recovery owner*: retained Raft plus an exact certified TreeDB image.
Every attempt writes its own result, including failures. No third-party modules.
"""
import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def request(base, path, body=None, content_type="application/json"):
    data = None if body is None else (body.encode() if isinstance(body, str) else json.dumps(body).encode())
    req = urllib.request.Request(base + path, data=data, headers={"Content-Type": content_type})
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            result = json.load(response)
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"{path}: HTTP {err.code}: {err.read().decode()}") from err
    if isinstance(result, dict) and result.get("errors"):
        raise RuntimeError(f"{path}: {result['errors']}")
    return result


def healthy(process, base):
    start = time.monotonic()
    while time.monotonic() - start < 90:
        if process.poll() is not None:
            raise RuntimeError(f"server exited {process.returncode}; inspect server logs")
        try:
            with urllib.request.urlopen(base + "/health", timeout=2) as response:
                response.read()
                if response.status == 200:
                    return time.monotonic() - start
        except (OSError, ValueError, RuntimeError):
            pass
        time.sleep(.1)
    raise RuntimeError("server health timeout")


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
    parser.add_argument("--mode", choices=["raft_checkpoint", "durable", "badger"], required=True)
    parser.add_argument("--ops", type=int, default=5000)
    parser.add_argument("--dataset", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--zero-offset", type=int, default=32000)
    parser.add_argument("--alpha-offset", type=int, default=33000)
    parser.add_argument("--seed", type=int, default=20260711)
    args = parser.parse_args()
    if min(args.ops, args.dataset, args.concurrency) < 1:
        parser.error("ops/dataset/concurrency must be positive")
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = Path(args.dgraph_bin).resolve()
    result = {"diagnostic_only": True, "config": vars(args),
              "contract": "HTTP CommitNow; Zero+Alpha filesystem survival; candidate recovers from retained Raft and exact certified image",
              "started_unix": time.time(), "success": False, "commands": [], "host_load_start": os.getloadavg()}
    zero = alpha = None
    logs = []
    try:
        result["binary_sha256"] = file_hash(binary)
        for port in [5080 + args.zero_offset, 6080 + args.zero_offset, 7080 + args.alpha_offset, 8080 + args.alpha_offset, 9080 + args.alpha_offset]:
            with socket.socket() as check:
                # Match the Go listener's reuse policy: recently stopped servers
                # can leave TIME_WAIT connections without an active listener.
                check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                check.bind(("127.0.0.1", port))
        def start(name, command):
            log = (out / (name + ".log")).open("xb")
            logs.append(log)
            result["commands"].append([str(binary)] + command)
            return subprocess.Popen([str(binary)] + command, stdout=log, stderr=log)
        zero_args = ["zero", "--wal", str(out / "zw"), "--replicas=1", f"--port_offset={args.zero_offset}", "--survive=filesystem"]
        zero = start("zero", zero_args)
        healthy(zero, f"http://localhost:{6080 + args.zero_offset}")
        selector = "backend=badger;tier=production;durability=durable;events=false" if args.mode == "badger" else f"backend=treedb;tier=benchmark_minimal;durability={args.mode};events=true"
        alpha_args = ["alpha", "--zero", f"localhost:{5080 + args.zero_offset}", "--postings", str(out / "p"), "--wal", str(out / "w"),
                      f"--port_offset={args.alpha_offset}", "--survive=filesystem", "--posting-store", selector, "--badger", "syncwrites=true" if args.mode == "badger" else "syncwrites=false"]
        base = f"http://localhost:{8080 + args.alpha_offset}"
        alpha = start("alpha", alpha_args)
        healthy(alpha, base)
        schema = "bench_value: string .\nbench_next: uid ."
        request(base, "/alter", schema, "application/rdf")
        data = [{"uid": f"_:fixture-{i}", "bench_value": f"base-{i}", "bench_next": {"uid": f"_:fixture-{(i+1)%args.dataset}"}} for i in range(args.dataset)]
        response = request(base, "/mutate?commitNow=true", {"set": data})
        fixture_uids = [response["data"]["uids"][f"fixture-{i}"] for i in range(args.dataset)]
        fixture_indices = {uid: i for i, uid in enumerate(fixture_uids)}
        expected = {uid: f"base-{i}" for i, uid in enumerate(fixture_uids)}
        def operation(i, kind, warmup=False):
            began = time.perf_counter()
            write = None
            if kind < 20:
                val = ("warmup-" if warmup else "write-") + str(i)
                response = request(base, "/mutate?commitNow=true", {"set": [{"uid": "_:new", "bench_value": val}]})
                uid = response["data"]["uids"]["new"]
                write = (uid, val)
                name = "write"
            else:
                uid = fixture_uids[i % args.dataset]
                fields = "uid bench_value" if kind < 80 else "uid bench_value bench_next { uid bench_value }"
                response = request(base, "/query", "{ q(func: uid(" + uid + ")) { " + fields + " } }", "application/dql")
                if len(response["data"]["q"]) != 1:
                    raise RuntimeError("read did not find one fixture node")
                name = "point_read" if kind < 80 else "one_hop_read"
            return name, (time.perf_counter() - began) * 1000, write
        for i in range(100):
            _, _, write = operation(i, i % 100, True)
            if write:
                expected[write[0]] = write[1]
        def status(name):
            with urllib.request.urlopen(base + "/debug/vars", timeout=30) as response:
                value = json.load(response)
            write_json(out / name, value)
            with urllib.request.urlopen(base + "/debug/store", timeout=30) as response:
                raw = response.read().decode()
            (out / name.replace(".json", ".store.txt")).write_text(raw)
            if args.mode != "badger" and "cow_ordered" not in raw:
                raise RuntimeError("runtime status did not confirm cow_ordered")
            return value
        status("status-before.json")
        kinds = ([0] * 20 + [20] * 60 + [80] * 20) * ((args.ops + 99) // 100)
        kinds = kinds[:args.ops]
        random.Random(args.seed).shuffle(kinds)
        write_json(out / "workload.json", kinds)
        write_json(out / "result.json", result)
        latencies = {"write": [], "point_read": [], "one_hop_read": []}
        began = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            for name, ms, write in pool.map(lambda pair: operation(*pair), enumerate(kinds)):
                latencies[name].append(ms)
                if write:
                    expected[write[0]] = write[1]
        elapsed = time.perf_counter() - began
        result["seconds"], result["ops_per_second"] = elapsed, args.ops / elapsed
        result["latency_ms"] = {name: {"count": len(values), **{f"p{p}": sorted(values)[min(len(values)-1, int(len(values)*p/100))] for p in [50, 95, 99]}} for name, values in latencies.items() if values}
        status("status-after.json")
        def validate():
            actual = {}
            uids = list(expected)
            for offset in range(0, len(uids), 100):
                response = request(base, "/query", "{ q(func: uid(" + ",".join(uids[offset:offset+100]) + ")) { uid bench_value bench_next { uid bench_value } } }", "application/dql")
                for node in response["data"]["q"]:
                    fixture_index = fixture_indices.get(node["uid"])
                    if fixture_index is not None:
                        next_index = (fixture_index + 1) % args.dataset
                        edges = node.get("bench_next", [])
                        if isinstance(edges, dict):
                            edges = [edges]
                        if len(edges) != 1 or edges[0]["uid"] != fixture_uids[next_index] or edges[0]["bench_value"] != f"base-{next_index}":
                            raise RuntimeError(f"one-hop posting mismatch: {node}")
                actual.update({hex(int(x["uid"], 16)): x["bench_value"] for x in response["data"]["q"]})
            if actual != expected:
                raise RuntimeError(f"posting mismatch: expected {len(expected)}, actual {len(actual)}, mismatches {sum(actual.get(k) != v for k,v in expected.items())}")
            return hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()
        def certificate():
            currents = list((out / "p").rglob("CURRENT"))
            if len(currents) != 1:
                raise RuntimeError(f"expected one certified image, found {currents}")
            pointer = json.loads(currents[0].read_text())
            cert = json.loads((currents[0].parent / "checkpoints" / f"{pointer['Generation']:020d}" / "certificate.json").read_text())
            metadata = json.loads(base64.b64decode(cert["metadata"]))
            metadata["AppliedProposalCount"] = len(metadata.pop("AppliedProposals", {}))
            return {"pointer": pointer, "metadata": metadata, "image_bytes": sum(x["bytes"] for x in cert["files"])}
        if args.mode == "raft_checkpoint":
            # Prove an acknowledged suffix outside the immutable image exists.
            first = certificate()
            for attempt in range(4):
                value = f"acknowledged-suffix-{attempt}"
                response = request(base, "/mutate?commitNow=true", {"set": [{"uid": "_:suffix", "bench_value": value}]})
                uid = response["data"]["uids"]["suffix"]
                expected[uid] = value
                # A read waits for the corresponding Oracle/apply visibility.
                validate()
                second = certificate()
                if first["pointer"] == second["pointer"]:
                    result["acknowledged_suffix_outside_certificate"] = True
                    break
                first = second
            else:
                raise RuntimeError("could not establish an acknowledged suffix after a certificate")
            result["certificate_before_kill"] = second
        result["checksum_before"] = validate()
        write_json(out / "expected.json", expected)
        write_json(out / "result.json", result)
        alpha.kill()  # Deliberate SIGKILL: no shutdown checkpoint or clean Close.
        alpha.wait(timeout=10)
        zero.kill()
        zero.wait(timeout=10)
        result["kill_signal"] = "SIGKILL Alpha and Zero"
        if args.mode == "raft_checkpoint":
            # Check after the writer has stopped: a checkpoint could otherwise
            # advance between the last validation query and SIGKILL.
            result["certificate_after_kill"] = certificate()
            if result["certificate_after_kill"]["pointer"] != result["certificate_before_kill"]["pointer"]:
                result["acknowledged_suffix_outside_certificate"] = False
                raise RuntimeError("certificate advanced before SIGKILL; suffix proof unavailable")
        restart_began = time.monotonic()
        zero = start("zero-restart", zero_args)
        healthy(zero, f"http://localhost:{6080 + args.zero_offset}")
        alpha = start("alpha-restart", alpha_args)
        healthy(alpha, base)
        result["recovery_seconds"] = time.monotonic() - restart_began
        result["checksum_after"] = validate()
        if result["checksum_before"] != result["checksum_after"]:
            raise RuntimeError("restart checksum mismatch")
        status("status-restarted.json")
        result["validated_nodes"] = len(expected)
        result["success"] = True
        print(json.dumps({k: result[k] for k in ["success", "ops_per_second", "seconds", "validated_nodes", "recovery_seconds"]}), flush=True)
    except BaseException as err:
        result["error"] = repr(err)
        raise
    finally:
        stop(alpha)
        stop(zero)
        for log in logs:
            log.close()
        result["finished_unix"] = time.time()
        result["host_load_end"] = os.getloadavg()
        write_json(out / "result.json", result)


if __name__ == "__main__":
    main()
