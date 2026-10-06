# SPDX-License-Identifier: Apache-2.0
"""Bounded best-effort CI resource records; test exit and signals remain failures."""
import argparse
import json
import itertools
import os
import pathlib
import platform
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

LIMIT = 8192
OUTPUT = None


def emit(record):
    record["utc"] = datetime.now(timezone.utc).isoformat()
    encoded = json.dumps(record, separators=(",", ":"))
    if len(encoded.encode()) + len("CI_RESOURCE ") + 1 > LIMIT:
        encoded = json.dumps({"utc": record["utc"], "kind": record.get("kind"), "unknown": "record size limit exceeded"})
    print("CI_RESOURCE " + encoded, flush=True)
    if OUTPUT is not None:
        try:
            with open(OUTPUT, "a") as stream:
                stream.write(encoded + "\n")
        except OSError:
            pass


def read(path, limit=1024):
    try:
        with open(path) as stream:
            return stream.read(limit)
    except OSError:
        return "unknown"


def command(argv):
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=0.75)
        return result.stdout[:2048] if result.returncode == 0 else "unknown"
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"


def snapshot(proc=pathlib.Path("/proc"), cgroups=pathlib.Path("/sys/fs/cgroup")):
    mem = dict(re.findall(r"^(MemAvailable|SwapFree):\s+(\d+) kB$", read(proc / "meminfo", 8192), re.M))
    oom = re.search(r"^oom_kill (\d+)$", read(proc / "vmstat", 16384), re.M)
    record = {"kind": "sample", "memory_kib": mem or "unknown", "oom_kill": int(oom[1]) if oom else "unknown",
              "pressure": {n: read(proc / "pressure" / n, 300) for n in ("memory", "cpu", "io")}}
    group = read(proc / "self/cgroup")
    record["cgroup"] = "unknown"
    for line in group.splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        version = 2 if parts[0] == "0" and parts[1] == "" else 1
        if version == 1 and "memory" not in parts[1].split(","):
            continue
        base = cgroups if version == 2 else cgroups / "memory"
        path = (base / parts[2].lstrip("/")).resolve()
        if not path.is_relative_to(base.resolve()):
            continue
        files = ("memory.current", "memory.max", "memory.events") if version == 2 else ("memory.usage_in_bytes", "memory.limit_in_bytes", "memory.failcnt", "memory.oom_control")
        record["cgroup"] = {"version": version, "path": parts[2][:160], "memory": {n: read(path / n, 300) for n in files}}
        break
    record["filesystems"] = {}
    for name in ("/", str(pathlib.Path.cwd())):
        try:
            stats = os.statvfs(name)
            record["filesystems"][name] = {"available_bytes": stats.f_bavail * stats.f_frsize, "free_inodes": stats.f_favail}
        except OSError:
            record["filesystems"][name] = "unknown"
    processes = []
    for path in itertools.islice(proc.glob("[0-9]*/status"), 512):
        text = read(path, 4096)
        pid, name, rss = (re.search(pattern, text, re.M) for pattern in (r"^Pid:\s+(\d+)$", r"^Name:\s+([^\n]+)$", r"^VmRSS:\s+(\d+) kB$"))
        if pid and name and rss:
            processes.append({"pid": int(pid[1]), "name": name[1][:40], "rss_kib": int(rss[1])})
    record["top_processes"] = sorted(processes, key=lambda p: p["rss_kib"], reverse=True)[:8] or "unknown"
    ids = command(["docker", "ps", "-aq", "--no-trunc"]).splitlines()[:8]
    ids = [i for i in ids if re.fullmatch(r"[0-9a-f]{12,64}", i)]
    record["docker"] = "unknown"
    if ids:
        states = command(["docker", "inspect", "--format", "{{.Name}} {{.State.Status}} {{.State.OOMKilled}} {{.State.ExitCode}}", *ids])
        memory = command(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}", *ids])
        record["docker"] = {"states": states[:900], "memory": memory[:900]}
    return record


def shutdown(start):
    # Retain safe OOM victim fields only, never raw kernel messages/command lines.
    text = command(["journalctl", "-k", "--since", start, "--no-pager", "-n", "80", "-o", "cat"])
    victims = re.findall(r"Killed process (\d+) \(([^)]+)\)", text)
    service = command(["systemctl", "show", "docker.service", "--property=ActiveState,SubState,Result"])
    states = [line for line in service.splitlines() if re.fullmatch(r"(ActiveState|SubState|Result)=[a-z-]+", line)]
    runner = command(["systemctl", "list-units", "--type=service", "--all", "--no-legend", "actions.runner.*"])
    units = [line.split()[0] for line in runner.splitlines() if line.split() and re.fullmatch(r"[\w.@:-]+\.service", line.split()[0])][:2]
    runner_states = {}
    for unit in units:
        state = command(["systemctl", "show", unit, "--property=ActiveState,SubState,Result"])
        runner_states[unit] = [line for line in state.splitlines() if re.fullmatch(r"(ActiveState|SubState|Result)=[a-z-]+", line)] or "unknown"
    return {"kind": "shutdown", "oom_victims": [{"pid": int(p), "name": n[:40]} for p, n in victims[:8]] if text != "unknown" else "unknown",
            "docker_service": states or "unknown", "runner_service": runner_states or "unknown"}


def sample(ticks):
    started = datetime.now(timezone.utc).isoformat()
    stop = False

    def finish(_number, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, finish)
    signal.signal(signal.SIGINT, finish)
    first = time.monotonic()
    for index in range(ticks):
        try:
            emit(snapshot())
        except Exception:
            emit({"kind": "sample", "unknown": "observer reading failed"})
        target = first + (index + 1) * 30
        while not stop and time.monotonic() < target:
            time.sleep(min(0.2, max(0, target - time.monotonic())))
        if stop:
            break
    emit(shutdown(started))


def observe(ticks, argv):
    observer, child = None, None
    received = []

    def forward(number, _frame):
        if not received:
            received.append(number)
            emit({"kind": "signal", "signal": number})
        if child is not None:
            try:
                os.killpg(child.pid, number)
            except ProcessLookupError:
                pass

    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, forward)
    status = 1
    try:
        emit({"kind": "start", "head": os.environ.get("GITHUB_SHA", "unknown"), "arch": platform.machine(), "cpu_count": os.cpu_count(), "monotonic_start": time.monotonic(), "maximum_ticks": ticks})
        sample_command = [sys.executable, __file__, "--sample", "--ticks", str(ticks)]
        if OUTPUT is not None:
            sample_command.extend(["--output", str(OUTPUT)])
        observer = subprocess.Popen(sample_command, start_new_session=True)
        if not received:
            child = subprocess.Popen(argv, start_new_session=True)
            if received:
                forward(received[0], None)
            result = child.wait()
            status = 128 - result if result < 0 else result
    finally:
        if observer is not None:
            observer.terminate()
            try:
                observer.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(observer.pid, signal.SIGKILL)
                observer.wait()
                emit({"kind": "observer_cleanup", "unknown": "final readings exceeded bounded cleanup"})
            if observer.returncode != 0:
                emit({"kind": "observer_exit", "exit": observer.returncode, "unknown": "observer did not complete final readings"})
    status = 128 + received[0] if received else status
    emit({"kind": "exit", "test_exit": status, "signal": received[0] if received else None})
    return status


def main():
    global OUTPUT
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--ticks", type=int, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    OUTPUT = args.output
    if OUTPUT is not None and not args.sample:
        try:
            with OUTPUT.open("x"):
                pass
        except OSError:
            OUTPUT = None
            emit({"kind": "artifact", "unknown": "cannot create exclusive observation artifact"})
    if not 1 <= args.ticks <= 180:
        parser.error("ticks must be within 1..180")
    if args.sample:
        sample(args.ticks)
        return 0
    argv = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not argv:
        parser.error("test command required")
    return observe(args.ticks, argv)


if __name__ == "__main__":
    sys.exit(main())
