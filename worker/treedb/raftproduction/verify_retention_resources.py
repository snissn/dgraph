#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded actual-server resource/retirement diagnostic; not an equilibrium benchmark.

Uses public mutation/query/commit/admin HTTP and existing server helpers. Linux
/proc and local certificates are observations, not additional recovery authority.
No physical GC/compact endpoint exists in this source; those cycles stay unavailable.
"""
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import threading
import time
import urllib.request

from verify_history_floor import healthy, request, stop


MIB = 1 << 20
CAPS = {"pending_rows": 4096, "pending_charge_bytes": 64 * MIB,
        "application_metadata_bytes": MIB, "new_prewrite_soft_rows": 65536,
        "recovery_input_entries": 4096, "recovery_input_bytes": 64 * MIB}


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(MIB), b""):
            digest.update(block)
    return digest.hexdigest()


def fixture_value(round_no, operation, size, mode):
    prefix = f"{round_no:08d}-{operation:08d}-"
    if mode == "repeat":
        return prefix.ljust(size, "x")
    # Reproducible high-entropy ASCII avoids testing only the compressor's
    # treatment of a repeated byte. Each overwrite has independent content.
    count = size - len(prefix)
    seed = prefix.encode("ascii")
    body = base64.b64encode(hashlib.shake_256(seed).digest((count * 3 + 3) // 4))
    return prefix + body[:count].decode("ascii")


def storage_inventory(root, max_files=100000):
    """Non-atomic file identities for observing reuse/deletion between phases.

    An absent name does not prove a segment was unreferenced: only the engine
    authorizes reclamation. Keep device/inode to distinguish a replacement at
    the same pathname. The observer never opens or edits storage contents.
    """
    rows, vanished = [], 0
    for directory, _, names in os.walk(root):
        for name in names:
            path = Path(directory) / name
            try:
                item = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                vanished += 1
                continue
            if len(rows) >= max_files:
                raise RuntimeError("storage inventory exceeds observer file bound")
            rows.append({"path": str(path.relative_to(root)), "device": item.st_dev,
                         "inode": item.st_ino, "logical_bytes": item.st_size,
                         "allocated_bytes": item.st_blocks * 512})
    rows.sort(key=lambda row: row["path"])
    return {"non_atomic": True, "vanished_during_walk": vanished, "files": rows}


def disk_usage(path):
    """Allocated/logical file bytes; deduplicate hardlinks within this component.

    Concurrent file removal is expected. This is a non-atomic walk, not a disk
    quota or a record of bytes physically written; directory blocks are excluded.
    """
    logical = allocated = files = vanished = 0
    seen = set()
    for directory, _, names in os.walk(path):
        for name in names:
            try:
                item = (Path(directory) / name).stat(follow_symlinks=False)
            except FileNotFoundError:
                vanished += 1
                continue
            identity = (item.st_dev, item.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            files += 1
            logical += item.st_size
            allocated += item.st_blocks * 512
    return {"logical_bytes": logical, "allocated_bytes": allocated,
            "files": files, "vanished_during_walk": vanished}


def memory_usage(process):
    if process is None or process.poll() is not None:
        return None
    try:
        values = {}
        for line in Path(f"/proc/{process.pid}/status").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in ("VmRSS", "VmHWM", "VmSize"):
                values[key + "_bytes"] = int(value.split()[0]) * 1024
        return {"pid": process.pid, **values}
    except FileNotFoundError:
        return None


def certificate(postings, archive):
    """Read matching CURRENT+certificate with a bounded publication-race retry.

    The server validates the certificate cryptographically on open. Here we
    match its pointer and preserve the exact bytes; we do not reimplement that
    authority or pretend that certified counters describe the live suffix.
    """
    current = postings / "CURRENT"
    if not current.exists():
        return None
    last_error = None
    for _ in range(5):
        try:
            pointer_raw = current.read_bytes()
            pointer = json.loads(pointer_raw)
            raw = (postings / "checkpoints" / f'{int(pointer["Generation"]):020d}' /
                   "certificate.json").read_bytes()
            cert = json.loads(raw)
            if current.read_bytes() != pointer_raw:
                continue
            if cert["generation"] != pointer["Generation"] or cert["digest"] != pointer["Digest"]:
                raise RuntimeError("CURRENT/certificate mismatch")
            metadata_raw = base64.b64decode(cert["metadata"], validate=True)
            metadata = json.loads(metadata_raw)
            manifest = metadata["PendingOverlay"]
            if metadata["Version"] != "dgraph-normal-delta-v6" or manifest["Version"] != 2:
                raise RuntimeError("driver source caps require application v6/pending manifest v2")
            name = f'{cert["generation"]:020d}-{cert["digest"]}.json'
            saved = archive / name
            if not saved.exists():
                saved.write_bytes(raw)
            signatures = metadata.get("AppliedProposals") or {}
            return {"generation": cert["generation"], "digest": cert["digest"],
                    "root": cert["root"], "certificate_file": str(saved),
                    "certificate_bytes": len(raw), "metadata_bytes": len(metadata_raw),
                    "version": metadata["Version"], "index": metadata["Index"],
                    "oracle_max_assigned": metadata["OracleMaxAssigned"],
                    "checksum_sequence": metadata["ChecksumSequence"],
                    "semantic_floor": metadata["SemanticFloor"],
                    "physical_floor": metadata["PhysicalFloor"],
                    "pending_rows": manifest["Count"], "pending_charge_bytes": manifest["Charge"],
                    "terminal_rows": manifest["TerminalCount"], "retry_signatures": len(signatures),
                    "retry_input_bytes": sum(row["Size"] for row in signatures.values()),
                    "timestamp_less_retry_signatures": sum(row["StartTs"] == 0 for row in signatures.values()),
                    "live_recovery_input_entries": None, "live_recovery_input_bytes": None,
                    "live_commit_minus_certified_index": None}
        except (OSError, ValueError, KeyError, TypeError) as error:
            last_error = error
    raise RuntimeError(f"cannot observe a matching certificate: {last_error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dgraph-bin", required=True)
    parser.add_argument("--witness-bin", required=True, help="prebuilt resourcewitness observer; never built by this driver")
    parser.add_argument("--source-manifest", required=True, help="immutable resourcewitness manifest captured with the clean build source")
    parser.add_argument("--source-revision", help="optional expected revision, checked against actual Go buildinfo; not provenance authority")
    parser.add_argument("--out", required=True, help="new artifact/data directory, preferably under /mnt/fast4tb")
    parser.add_argument("--zero-offset", type=int, default=46000)
    parser.add_argument("--alpha-offset", type=int, default=47000)
    parser.add_argument("--rounds", type=int, default=8)
    parser.add_argument("--writes-per-round", type=int, default=128)
    parser.add_argument("--keyspace", type=int, default=32)
    parser.add_argument("--value-bytes", type=int, default=256)
    parser.add_argument("--payload-mode", choices=("repeat", "shake256"), default="repeat",
                        help="shake256 makes deterministic distinct high-entropy ASCII values; repeat preserves the original fixture")
    parser.add_argument("--round-pause-seconds", type=float, default=0)
    parser.add_argument("--low-traffic-seconds", type=float, default=2)
    parser.add_argument("--sample-seconds", type=float, default=2)
    parser.add_argument("--maintenance-wait-seconds", type=float, default=0,
                        help=">=310 observes one scheduled logical-history maintenance opportunity")
    parser.add_argument("--require-physical-floor-progress", action="store_true",
                        help="requires certified P AFTER wait > P BEFORE wait (wait>=310s); distinct from GC/compaction")
    parser.add_argument("--require-nonzero-physical-floor", action="store_true",
                        help="requires any completed certified logical maintenance pass, including before the wait")
    parser.add_argument("--max-wall-seconds", type=float, default=1200,
                        help="workflow budget checked between calls; startup/HTTP/cleanup also have bounded individual deadlines")
    parser.add_argument("--max-rss-mib", type=int, default=4096, help="combined sampled Alpha+Zero RSS budget")
    parser.add_argument("--max-disk-mib", type=int, default=8192, help="allocated p+w+zw+private TMPDIR file-byte budget")
    parser.add_argument("--max-floor-seconds", type=float, default=60)
    parser.add_argument("--max-close-seconds", type=float, default=30)
    parser.add_argument("--max-post-retire-terminal", type=int, default=4096)
    parser.add_argument("--max-post-retire-retries", type=int, default=128)
    args = parser.parse_args()
    for name, value in vars(args).items():
        if isinstance(value, float) and not math.isfinite(value):
            parser.error(f"{name} must be finite")
    for name in ("rounds", "writes_per_round", "keyspace", "value_bytes", "max_rss_mib", "max_disk_mib"):
        if getattr(args, name) <= 0:
            parser.error(f"{name} must be positive")
    if args.rounds * args.writes_per_round > 100000 or args.keyspace > 100000 or args.value_bytes > 16384:
        parser.error("bounded driver requires <=100000 writes/keys and <=16384 bytes/value")
    if args.value_bytes < 32 or args.sample_seconds < .1 or args.max_wall_seconds < 1:
        parser.error("value-bytes>=32, sample-seconds>=0.1 and max-wall-seconds>=1 required")
    for name in ("round_pause_seconds", "low_traffic_seconds", "maintenance_wait_seconds",
                 "max_post_retire_terminal", "max_post_retire_retries"):
        if getattr(args, name) < 0:
            parser.error(f"{name} must be nonnegative")
    if args.max_floor_seconds <= 0 or args.max_close_seconds <= 0:
        parser.error("floor/close deadlines must be positive")
    if args.require_physical_floor_progress and args.maintenance_wait_seconds < 310:
        parser.error("physical floor gate requires maintenance-wait-seconds>=310")
    if args.low_traffic_seconds * args.rounds + args.maintenance_wait_seconds >= args.max_wall_seconds:
        parser.error("declared waits exhaust max-wall-seconds before workload/startup")
    for offset, highest in ((args.zero_offset, 6080), (args.alpha_offset, 9080)):
        if offset < 0 or highest + offset > 65535:
            parser.error("offset ports must fit 1..65535")
    binary, out = Path(args.dgraph_bin).resolve(), Path(args.out).resolve()
    witness_binary = Path(args.witness_bin).resolve()
    out.mkdir(parents=True, exist_ok=False)
    archive = out / "certificates"
    archive.mkdir()
    scratch = out / "tmp"
    scratch.mkdir()
    source_manifest = out / "source-manifest.json"
    with source_manifest.open("xb") as saved:
        saved.write(Path(args.source_manifest).read_bytes())
    pending_age_seconds = math.ceil(args.max_wall_seconds) + 120
    result = {"success": False, "diagnostic_only": True, "equilibrium_qualified": False,
              "configuration": vars(args), "binary_sha256": file_sha256(binary),
              "driver_sha256": file_sha256(__file__), "witness_binary_sha256": file_sha256(witness_binary),
              "source_manifest_sha256": file_sha256(source_manifest), "runtime_witnesses": [],
              "pending_age_limit_seconds": pending_age_seconds,
              "configured_source_caps": CAPS, "commands": [], "phases": [], "rounds": [],
              "gates": {"combined_sampled_rss_bytes": args.max_rss_mib * MIB,
                        "allocated_server_and_scratch_file_bytes": args.max_disk_mib * MIB,
                        "post_retire_terminal_rows": args.max_post_retire_terminal,
                        "post_retire_retry_signatures": args.max_post_retire_retries,
                        "floor_ack_seconds": args.max_floor_seconds,
                        "orderly_close_seconds": args.max_close_seconds,
                        "physical_floor_progress_during_wait_required": args.require_physical_floor_progress,
                        "nonzero_physical_floor_required": args.require_nonzero_physical_floor},
              "limitations": ["No physical GC or CompactStorage public HTTP/admin endpoint exists in this source; neither cycle is invoked.",
                  "Live Raft recovery debt has no public metric; null is unavailable, not zero. Certified counters exclude the live suffix.",
                  "RSS is sampled; VmHWM is per-process kernel high water. Disk walks are non-atomic, include persistent value logs, exclude directory blocks.",
                  "No power-loss/crash qualification or steady-state/production-tier performance acceptance follows from this bounded run.",
                  "Client UID/value/latency buffers scale with configured input. Rebuild/baseline scratch and short-lived fanout RSS are not bounded by pending logical charge.",
                  "The workload updates fixed-key scalar values, not a schema/Unique rebuild or worst-case index/delete fanout resource qualification.",
                  "Private server TMPDIR is sampled separately; artifact/log/certificate-copy bytes are outside the server-and-scratch disk budget.",
                  "Resource sampling and /debug/store walks perturb the run; observer buffers scale with files/metadata. ACK samples are diagnostic, not throughput acceptance.",
                  "Per-phase file inventories are non-atomic observations of names/device/inode/size, not engine reachability or proof of a GC cause. Between-phase files may be missed.",
                  "Operator selects S from this isolated client's completed transactions; concurrent external clients are unsupported.",
                  "Go buildinfo attests Dgraph revision/modified and the Gomap replacement path, not the compiled local replacement tree. The clean-source manifest is a build-procedure observation, not reproducible-build proof.",
                  "An exact build/source manifest and immutable hashes bind observations; they do not authenticate who built the supplied binaries or prove compiler integrity.",
                  "S=0 history growth and the 65536-row new-prewrite SOFT limit are not hard terminal/heap/WAL caps."],
              "physical_gc": {"available": False, "invoked": False},
              "physical_compaction": {"available": False, "invoked": False},
              "logical_maintenance": {"schedule_seconds": 300, "wait_seconds": args.maintenance_wait_seconds}}
    token, base = "retention-resource-diagnostic", f"http://localhost:{8080 + args.alpha_offset}"
    zero = alpha = None
    alpha_state, alpha_config = "absent", None
    logs, failures = [], []
    lock, finished = threading.Lock(), threading.Event()
    began = time.monotonic()
    peak_rss = peak_allocated = samples = 0
    sample_log = (out / "samples.jsonl").open("x")

    def witness(*arguments):
        observed = subprocess.run([str(witness_binary), *arguments], capture_output=True,
                                  text=True, timeout=30, check=False)
        if observed.returncode:
            raise RuntimeError(f"resourcewitness rejected evidence: {observed.stderr.strip()}")
        return json.loads(observed.stdout)

    def check():
        if time.monotonic() - began > args.max_wall_seconds:
            raise RuntimeError("max-wall-seconds exceeded (individual HTTP calls have a 30s timeout)")
        with lock:
            if failures:
                raise RuntimeError(failures[0])
        for name, proc in (("alpha", alpha), ("zero", zero)):
            if proc is not None and proc.poll() is not None and not (name == "alpha" and alpha_state == "closing"):
                raise RuntimeError(f"{name} exited unexpectedly: {proc.returncode}")

    def sample(label):
        nonlocal peak_rss, peak_allocated, samples
        # Serialization keeps phase and periodic walks from duplicating work.
        with lock:
            started = time.monotonic()
            memory = {"alpha": memory_usage(alpha), "zero": memory_usage(zero)}
            disk = {name: disk_usage(out / name) for name in ("p", "w", "zw", "tmp")}
            cert = certificate(out / "p", archive)
            rss = sum((item or {}).get("VmRSS_bytes", 0) for item in memory.values())
            allocated = sum(item["allocated_bytes"] for item in disk.values())
            peak_rss, peak_allocated = max(peak_rss, rss), max(peak_allocated, allocated)
            row = {"phase": label, "elapsed_seconds": time.monotonic() - began,
                   "memory": memory, "alpha_state": alpha_state, "disk": disk, "certificate": cert,
                   "combined_rss_bytes": rss, "allocated_server_and_scratch_file_bytes": allocated,
                   "collection_seconds": time.monotonic() - started}
            sample_log.write(json.dumps(row) + "\n")
            sample_log.flush()
            samples += 1
            if rss > args.max_rss_mib * MIB:
                failures.append("combined sampled RSS budget exceeded")
            if allocated > args.max_disk_mib * MIB:
                failures.append("allocated server-and-scratch file-byte budget exceeded")
            if cert:
                for key in ("pending_rows", "pending_charge_bytes", "metadata_bytes"):
                    cap = CAPS["application_metadata_bytes" if key == "metadata_bytes" else key]
                    if cert[key] > cap:
                        failures.append(f"certified {key} exceeds source cap")
            return row

    def observe(label):
        check()
        row = sample(label)
        check()
        try:
            with urllib.request.urlopen(base + "/debug/store", timeout=10) as response:
                raw_stats = response.read(16 * MIB + 1)
            if len(raw_stats) > 16 * MIB:
                raise RuntimeError("/debug/store exceeds observer response bound; truncated witnesses are refused")
            stats = raw_stats.decode()
            store_path = out / (label + "-store.txt")
            with store_path.open("x") as saved:
                saved.write(stats)
            observed = witness("--mode=runtime", "--config-log", str(alpha_config), "--store", str(store_path))
            result["runtime_witnesses"].append({"phase": label, "role": "alpha", "pid": alpha.pid,
                                                 "command": alpha.args, **observed})
        except OSError as error:
            raise RuntimeError(f"/debug/store unavailable: {error}") from error
        inventory_path = out / (label + "-storage-files.json")
        inventory = storage_inventory(out / "p")
        with inventory_path.open("x") as saved:
            json.dump(inventory, saved, indent=2)
            saved.write("\n")
        row["posting_file_inventory"] = {"file": str(inventory_path),
            "sha256": file_sha256(inventory_path), "file_count": len(inventory["files"]),
            "vanished_during_walk": inventory["vanished_during_walk"]}
        result["phases"].append(row)
        return row

    def periodic():
        while not finished.wait(args.sample_seconds):
            try:
                sample("periodic")
            except Exception as error:
                with lock:
                    failures.append(f"sampling failed: {error!r}")
                return

    def launch(label, arguments):
        log = (out / (label + ".log")).open("xb")
        logs.append(log)
        result["commands"].append([str(binary)] + arguments)
        return subprocess.Popen([str(binary)] + arguments, stdout=log, stderr=log,
                                cwd=out, env={**os.environ, "TMPDIR": str(scratch)})

    def start_alpha(label):
        nonlocal alpha, alpha_state, alpha_config
        process = launch("alpha-" + label, ["alpha", "--zero", f"localhost:{5080 + args.zero_offset}",
            "--postings", str(out / "p"), "--wal", str(out / "w"), f"--port_offset={args.alpha_offset}",
            "--tmp", str(scratch / "alpha"), "--limit", f"txn-abort-after={pending_age_seconds}s",
            "--survive=filesystem", "--security", f"token={token}", "--posting-store",
            "backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true"])
        with lock:
            alpha, alpha_state = process, "running"
        healthy(alpha, base)
        for role, filename in (("zero", "zero.log"), ("alpha", "alpha-" + label + ".log")):
            retained = out / (label + "-" + role + ".config.log")
            with retained.open("xb") as saved:
                saved.write((out / filename).read_bytes())
            observed = witness("--mode=runtime", "--config-log", str(retained))
            process = alpha if role == "alpha" else zero
            result["runtime_witnesses"].append({"phase": label, "role": role, "pid": process.pid,
                                                 "command": process.args, **observed})
            if role == "alpha":
                alpha_config = retained

    def call(path, body, content="application/json", allow_error=False):
        check()
        return request(base, path, body, token, content, allow_error)

    def wait(seconds):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            check()
            time.sleep(min(.2, max(0, until - time.monotonic())))

    def retire(floor, label):
        retries = 0
        before = observe(label + "-before")
        started = time.monotonic()
        attempts = []
        while True:
            attempt_started = time.monotonic()
            response = call("/admin", {"query": "mutation($s:UInt64!){retirePostingHistory(floor:$s){floor response{code message}}}",
                                      "variables": {"s": floor}}, allow_error=True)
            attempts.append(time.monotonic() - attempt_started)
            if not response.get("errors"):
                break
            if "precede assigned" not in str(response) or time.monotonic() - started > args.max_floor_seconds:
                raise RuntimeError(f"operator floor refused: {response}")
            retries += 1
            wait(.1)
        elapsed = time.monotonic() - started
        if elapsed > args.max_floor_seconds:
            raise RuntimeError("floor ACK exceeded declared deadline")
        after = observe(label + "-after")
        cert = after["certificate"]
        if cert is None or cert["semantic_floor"] != floor:
            raise RuntimeError("floor ACK lacks matching published application certificate")
        if cert["terminal_rows"] > args.max_post_retire_terminal or cert["retry_signatures"] > args.max_post_retire_retries:
            raise RuntimeError("post-retirement retained history exceeds diagnostic budgets")
        return {"operator_selected_floor": floor, "ack_seconds_including_eligibility_retries": elapsed,
                "http_attempt_seconds": attempts, "eligibility_retries": retries,
                "response": response, "before": before["certificate"], "after": cert}

    def close_alpha(label):
        nonlocal alpha, alpha_state
        with lock:
            process, alpha_state = alpha, "closing"
        # Keep alpha observable throughout SIGINT/drain/Close. Clearing it here
        # would silently omit this process's potentially largest RSS interval.
        sample(label + "-begin")
        started = time.monotonic()
        process.send_signal(signal.SIGINT)
        try:
            code = process.wait(timeout=args.max_close_seconds)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait(timeout=10)
            raise RuntimeError("orderly Alpha Close exceeded deadline; forced cleanup is not success") from error
        elapsed = time.monotonic() - started
        result[label] = {"seconds": elapsed, "exit_code": code}
        with lock:
            alpha, alpha_state = None, "exited"
        if code != 0:
            raise RuntimeError(f"orderly Alpha Close returned {code}")
        return sample(label)["certificate"]

    def verify_values(expected):
        got = call("/query", "{q(func:has(retention_key)){uid retention_key retention_value}}", "application/dql")
        actual = {int(row["retention_key"]): (row["uid"], row["retention_value"]) for row in got["data"]["q"]}
        if actual != expected or len(got["data"]["q"]) != len(expected):
            raise RuntimeError(f"retained key/value mismatch: expected {len(expected)}, received {len(actual)}")

    sampler = threading.Thread(target=periodic, name="resource-sampler", daemon=True)
    try:
        result["source_packet"] = witness("--mode=verify", "--server", str(binary),
                                           "--manifest", str(source_manifest))
        if result["source_packet"]["server_sha256"] != result["binary_sha256"]:
            raise RuntimeError("server hash differs from verified source packet")
        if args.source_revision and args.source_revision != result["source_packet"]["vcs_revision"]:
            raise RuntimeError("expected source revision differs from actual server buildinfo")
        zero = launch("zero", ["zero", "--wal", str(out / "zw"), "--replicas=1",
                              f"--port_offset={args.zero_offset}", "--survive=filesystem"])
        healthy(zero, f"http://localhost:{6080 + args.zero_offset}")
        start_alpha("initial")
        sampler.start()
        observe("initial")
        call("/alter", "retention_key: int @index(int) .\nretention_value: string .", "application/rdf")
        expected, held = {}, None
        for round_no in range(args.rounds):
            latencies = []
            for operation in range(args.writes_per_round):
                key = (round_no * args.writes_per_round + operation) % args.keyspace
                value = fixture_value(round_no, operation, args.value_bytes, args.payload_mode)
                uid = expected[key][0] if key in expected else "_:key"
                started = time.monotonic()
                response = call("/mutate?commitNow=true", {"set": [{"uid": uid, "retention_key": key, "retention_value": value}]})
                latencies.append(time.monotonic() - started)
                if key not in expected:
                    uid = response["data"]["uids"]["key"]
                expected[key] = (uid, value)
                floor = int(response["extensions"]["txn"]["start_ts"])
            # Newer pending state remains above operator S through maintenance,
            # low traffic and (on the last round) orderly Alpha reopen.
            response = call("/mutate", {"set": [{"uid": "_:held", "retention_value": f"held-{round_no}"}]})
            held = {"txn": response["extensions"]["txn"], "uid": response["data"]["uids"]["held"],
                    "value": f"held-{round_no}"}
            retirement = retire(floor, f"round-{round_no:04d}-retire")
            if retirement["after"]["pending_rows"] != 1:
                raise RuntimeError("pending overlay was not present in the forced certificate")
            verify_values(expected)
            wait(args.low_traffic_seconds)
            observe(f"round-{round_no:04d}-low-traffic")
            result["rounds"].append({"round": round_no, "writes": len(latencies),
                "write_ack_seconds": {"min": min(latencies), "median": statistics.median(latencies),
                                      "max": max(latencies), "sum": sum(latencies)}, "retirement": retirement})
            if round_no + 1 < args.rounds:
                txn = held["txn"]
                call(f'/commit?startTs={txn["start_ts"]}', {"keys": txn["keys"], "preds": txn["preds"]})
                held = None
                wait(args.round_pause_seconds)
        before = observe("maintenance-before")
        wait(args.maintenance_wait_seconds)
        after = observe("maintenance-after")
        old_p = (before["certificate"] or {}).get("physical_floor", 0)
        new_p = (after["certificate"] or {}).get("physical_floor", 0)
        result["logical_maintenance"].update({"physical_floor_before": old_p, "physical_floor_after": new_p,
                                             "observed_progress_during_wait": new_p > old_p,
                                             "nonzero_physical_floor_observed": new_p > 0})
        if args.require_physical_floor_progress and new_p <= old_p:
            raise RuntimeError("certified P did not advance during the requested maintenance wait (including if already at S)")
        if args.require_nonzero_physical_floor and new_p == 0:
            raise RuntimeError("no completed certified logical posting maintenance pass was observed")
        closed_cert = close_alpha("orderly-close")
        if closed_cert is None or closed_cert["pending_rows"] != 1:
            raise RuntimeError("Close failed to preserve the pending overlay certificate")
        start_alpha("reopen")  # Zero stays alive: no fabricated survival of Zero's prior timestamp lease.
        recovered = re.search(r"recovery: certified=(\d+) oracle=(\d+)", (out / "alpha-reopen.log").read_text())
        if not recovered or int(recovered[1]) != closed_cert["index"]:
            raise RuntimeError("reopen did not report the exact orderly-close certified frontier")
        result["recovered_index"] = int(recovered[1])
        observe("reopen")
        verify_values(expected)
        txn = held["txn"]
        call(f'/commit?startTs={txn["start_ts"]}', {"keys": txn["keys"], "preds": txn["preds"]})
        until = time.monotonic() + 30
        while True:
            got = call("/query", '{q(func:uid(' + held["uid"] + ')){retention_value}}', "application/dql")
            if got["data"]["q"] == [{"retention_value": held["value"]}]:
                break
            if time.monotonic() >= until:
                raise RuntimeError("pending transaction did not commit after orderly reopen")
            wait(.1)
        result["final_retirement"] = retire(int(txn["start_ts"]), "final-retire")
        verify_values(expected)
        close_alpha("final-orderly-close")
        check()
        result["success"] = True
    except Exception as error:
        result["error"] = repr(error)
    finally:
        finished.set()
        if sampler.is_alive():
            sampler.join(timeout=30)
        try:
            if file_sha256(witness_binary) != result["witness_binary_sha256"]:
                raise RuntimeError("witness binary changed during the diagnostic")
            if file_sha256(source_manifest) != result["source_manifest_sha256"]:
                raise RuntimeError("retained source manifest changed during the diagnostic")
            final_source = witness("--mode=verify", "--server", str(binary), "--manifest", str(source_manifest))
            if final_source != result.get("source_packet"):
                raise RuntimeError("source/binary packet changed or was never verified")
            result["source_packet_verified_after_run"] = True
        except Exception as error:
            result["source_packet_verified_after_run"] = False
            failures.append(f"final provenance check failed: {error!r}")
        # Cleanup fallback may kill a hung process; it never establishes a
        # successful orderly-close gate or a power-loss durability result.
        stop(alpha)
        stop(zero)
        for log in logs:
            log.close()
        with lock:
            result["sampling_failures"] = failures.copy()
            result["observed_peaks"] = {"combined_sampled_rss_bytes": peak_rss,
                "allocated_server_and_scratch_file_bytes": peak_allocated, "samples": samples}
        if failures:
            result["success"] = False
        result["elapsed_seconds"] = time.monotonic() - began
        sample_log.close()
        (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("success", "diagnostic_only", "equilibrium_qualified", "elapsed_seconds")}, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
