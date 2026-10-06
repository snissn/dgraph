# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import io
import itertools
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import arm
import observe

REAL_POPEN = subprocess.Popen


def quiet_sampler(argv, **kwargs):
    if "--sample" in argv:
        argv = [sys.executable, "-c", "import time; time.sleep(300)"]
    return REAL_POPEN(argv, **kwargs)


def records(packages):
    return "".join("Found valid task: " + p + " isCommon:true\n" for p in packages) + "Running tests for " + str(len(packages)) + " packages.\n"


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.universe = ["github.com/dgraph-io/dgraph/v25/query/vector", "github.com/dgraph-io/dgraph/v25/worker"]
        self.source = {"checkout": "merge", "pr_head": "head", "tags": "", "inputs": {"t/t.go": "sha"}}
        self.manifests, self.jobs = [], []
        for shard in ("vectors", "remainder"):
            selected = arm.selection(self.universe, shard)
            self.manifests.append({"schema": 1, "shard": shard, "source": self.source, "universe": self.universe,
                                   "coverage_sha256": arm.digest(self.universe), "selected": selected,
                                   "command": ["./t", "--suite=integration", "--pkg=" + ",".join(selected)],
                                   "exit": 0, "signal": None, "run_id": "7", "attempt": "2"})
            self.jobs.append({"name": "arm-" + shard, "status": "completed", "conclusion": "success", "head_sha": "head", "run_id": 7, "run_attempt": 2,
                              "started_at": "2026-10-06T00:00:00Z", "completed_at": "2026-10-06T00:50:00Z"})

    def gate(self, manifests=None, jobs=None):
        return arm.validate_gate(self.manifests if manifests is None else manifests, self.jobs if jobs is None else jobs, self.source, True, 7, 2)

    def test_exact_union_and_deadline(self):
        self.assertEqual(self.gate()["packages"], 2)
        self.jobs[1]["completed_at"] = "2026-10-06T01:00:00Z"
        self.assertEqual(self.gate()["whole_gate_seconds"], 3600)
        self.jobs[1]["started_at"] = "2026-10-06T00:30:00Z"
        self.jobs[1]["completed_at"] = "2026-10-06T01:00:01Z"
        with self.assertRaises(ValueError):
            self.gate()

    def test_nonpublic_full_command_and_gate(self):
        manifest = copy.deepcopy(self.manifests[0])
        manifest.update(shard="full", selected=self.universe, command=["./t"])
        job = copy.deepcopy(self.jobs[0])
        job["name"] = "arm-full"
        result = arm.validate_gate([manifest], [job], self.source, False, 7, 2)
        self.assertEqual(result["shards"], ["full"])
        self.assertEqual(result["packages"], len(self.universe))

    def test_negative_gate_contracts(self):
        cases = [
            ("missing manifest", lambda m, j: m.pop()),
            ("duplicate manifest", lambda m, j: m.append(copy.deepcopy(m[0]))),
            ("missing job", lambda m, j: j.pop()),
            ("duplicate job", lambda m, j: j.append(copy.deepcopy(j[0]))),
            ("wrong source", lambda m, j: m[0]["source"].update(checkout="other")),
            ("wrong coverage", lambda m, j: m[0].update(coverage_sha256="wrong")),
            ("empty selected", lambda m, j: m[0].update(selected=[])),
            ("overlap", lambda m, j: m[1].update(selected=m[0]["selected"])),
            ("failed command", lambda m, j: m[0].update(exit=3)),
            ("signalled command", lambda m, j: m[0].update(signal=15)),
            ("wrong command", lambda m, j: m[0].update(command=["./t", "--suite=unit"])),
            ("wrong attempt", lambda m, j: m[0].update(attempt="1")),
            ("wrong job head", lambda m, j: j[0].update(head_sha="other")),
        ]
        for state in ("failure", "cancelled", "skipped", "timed_out"):
            cases.append((state, lambda m, j, state=state: j[0].update(conclusion=state)))
        for name, mutate in cases:
            with self.subTest(name=name):
                manifests, jobs = copy.deepcopy(self.manifests), copy.deepcopy(self.jobs)
                mutate(manifests, jobs)
                with self.assertRaises((ValueError, KeyError)):
                    self.gate(manifests, jobs)

    def test_empty_duplicate_partial_or_failed_discovery(self):
        self.assertEqual(arm.parse_packages(records(self.universe)), self.universe)
        for text in ("", records(self.universe * 2), records(self.universe).split("Running tests")[0], records(self.universe).replace("2 packages", "3 packages"), records(self.universe).replace("isCommon:true", "isCommon:maybe")):
            with self.assertRaises(ValueError):
                arm.parse_packages(text)
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(arm.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, records(self.universe).encode(), b"query error")):
                with self.assertRaises(ValueError):
                    arm.discover(pathlib.Path(directory), pathlib.Path(directory) / "raw")
        with self.assertRaises(ValueError):
            arm.selection([self.universe[1]], "vectors")

    def test_archived_raw_hash_and_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "vectors"
            path.mkdir()
            manifest = copy.deepcopy(self.manifests[0])
            for name, packages in (("universe", self.universe), ("selected", manifest["selected"])):
                (path / (name + ".stdout")).write_text(records(packages))
                (path / (name + ".stderr")).write_text("nonempty retained stderr\n")
            manifest["raws"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in path.iterdir()}
            (path / "manifest.json").write_text(json.dumps(manifest))
            self.assertEqual(len(arm.load_manifests(path.parent)), 1)
            (path / "selected.stdout").write_text(records(self.universe))
            with self.assertRaises(ValueError):
                arm.load_manifests(path.parent)
            manifest["raws"]["selected.stdout"] = hashlib.sha256((path / "selected.stdout").read_bytes()).hexdigest()
            (path / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                arm.load_manifests(path.parent)


class ObserverTests(unittest.TestCase):
    def test_artifact_collision_cannot_mask_test_exit(self):
        previous = observe.OUTPUT
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "existing.jsonl"
                path.write_text("retained original\n")
                with mock.patch.object(sys, "argv", ["observe.py", "--ticks", "1", "--output", str(path), "--", "false"]), mock.patch.object(observe, "observe", return_value=7) as execution, mock.patch.object(observe, "emit") as emit:
                    self.assertEqual(observe.main(), 7)
                    execution.assert_called_once_with(1, ["false"])
                    self.assertIsNone(observe.OUTPUT)
                    emit.assert_called_once()
                self.assertEqual(path.read_text(), "retained original\n")
        finally:
            observe.OUTPUT = previous

    def test_unknown_and_bounded_records(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(observe, "command", return_value="unknown"), mock.patch.object(observe.os, "statvfs", side_effect=OSError):
                record = observe.snapshot(pathlib.Path(directory), pathlib.Path(directory))
                self.assertEqual(record["memory_kib"], "unknown")
                self.assertEqual(record["cgroup"], "unknown")
                self.assertEqual(record["docker"], "unknown")
                self.assertEqual(observe.shutdown("now")["oom_victims"], "unknown")
        output = io.StringIO()
        with mock.patch.object(sys, "stdout", output):
            observe.emit({"kind": "sample", "big": "x" * 20000})
        self.assertLess(len(output.getvalue()), observe.LIMIT)
        self.assertIn("record size limit", output.getvalue())
        with mock.patch.object(observe, "snapshot", return_value={"kind": "sample"}) as sample, mock.patch.object(observe, "shutdown", return_value={"kind": "shutdown"}), mock.patch.object(observe, "emit"), mock.patch.object(observe.time, "monotonic", side_effect=itertools.count(0, 31)):
            observe.sample(2)
            self.assertEqual(sample.call_count, 2)

    def test_nonzero_and_success_preserved_and_sampler_reaped(self):
        previous = {n: signal.getsignal(n) for n in (signal.SIGINT, signal.SIGTERM)}
        children = []
        def spawn(argv, **kwargs):
            process = quiet_sampler(argv, **kwargs)
            children.append(process)
            return process
        try:
            for status in (0, 7):
                with mock.patch.object(observe.subprocess, "Popen", side_effect=spawn), mock.patch.object(observe, "emit"):
                    self.assertEqual(observe.observe(1, [sys.executable, "-c", "raise SystemExit(" + str(status) + ")"]), status)
            self.assertTrue(all(child.poll() is not None for child in children))
        finally:
            for n, handler in previous.items():
                signal.signal(n, handler)

    def test_signal_cannot_become_success(self):
        for number, expected in ((signal.SIGTERM, 143), (signal.SIGINT, 130)):
            child = REAL_POPEN([sys.executable, __file__, "--signal-child"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                while "READY" not in child.stdout.readline():
                    self.assertIsNone(child.poll())
                child.send_signal(number)
                stdout, stderr = child.communicate(timeout=5)
                self.assertEqual(child.returncode, expected, stdout + stderr)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()


if __name__ == "__main__":
    if sys.argv[1:] == ["--signal-child"]:
        with mock.patch.object(observe.subprocess, "Popen", side_effect=quiet_sampler):
            # Child deliberately reports success after cancellation: wrapper must refuse it.
            sys.exit(observe.observe(1, [sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM,lambda *a:exit(0)); signal.signal(signal.SIGINT,lambda *a:exit(0)); print('READY',flush=True); time.sleep(300)"]))
    unittest.main()
