# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

import arm
import integration2 as native


def records(packages):
    return "\n".join(json.dumps({"ImportPath": p, "Module": {"Main": True, "Path": native.MODULE}}) for p in packages)


class FullExecutionTests(unittest.TestCase):
    def setUp(self):
        self.universe = sorted([native.NAMESPACE, native.MODULE + "/x", native.MODULE + "/dgraphtest"])
        self.source = {"checkout": "merge", "pr_head": "head", "tags": "integration2", "inputs_sha256": "bound", "go_version": "go1.26.4"}
        self.manifests, self.jobs = [], []
        for shard in ("namespace", "remainder"):
            selected = native.selection(self.universe, shard)
            self.manifests.append({"schema": 1, "shard": shard, "source": self.source, "public": True,
                                   "universe": self.universe, "coverage_sha256": arm.digest(self.universe),
                                   "selected": selected, "command": native.command(selected, True), "cache_profile": "512",
                                   "exit": 0, "signal": None, "source_stable": True, "timed_out": False,
                                   "go_env": {"GOVERSION": "go1.26.4", "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "1", "GOFLAGS": "", "GOEXPERIMENT": ""}, "run_id": "7", "attempt": "2"})
            self.jobs.append({"name": "integration2-" + shard, "status": "completed", "conclusion": "success",
                              "head_sha": "head", "run_id": 7, "run_attempt": 2,
                              "started_at": "2026-10-06T00:00:00Z", "completed_at": "2026-10-06T01:20:00Z"})

    def gate(self, manifests=None, jobs=None):
        return native.validate_gate(self.manifests if manifests is None else manifests, self.jobs if jobs is None else jobs,
                                    self.source, True, 7, 2)

    def test_full_universe_includes_ordinary_packages_and_original_deadline(self):
        self.assertEqual(self.gate()["packages"], 3)
        self.assertIn(native.MODULE + "/x", self.manifests[1]["selected"])
        self.jobs[1]["completed_at"] = "2026-10-06T01:30:00Z"
        self.assertEqual(self.gate()["whole_gate_seconds"], 5400)
        self.jobs[1]["started_at"] = "2026-10-06T00:40:00Z"
        self.jobs[1]["completed_at"] = "2026-10-06T01:30:01Z"
        with self.assertRaises(ValueError):
            self.gate()
        self.assertEqual(native.command(self.universe, False),
                         ["go", "test", "-v", "-timeout=90m", "-failfast", "-tags=integration2", "./..."])
        self.assertNotIn("-run", native.command([native.NAMESPACE], True))
        self.assertIn("-p=1", native.command(self.universe, True))
        full = copy.deepcopy(self.manifests[0])
        full.update(shard="full", public=False, selected=self.universe, cache_profile="", command=native.command(self.universe, False))
        job = copy.deepcopy(self.jobs[0]); job["name"] = "integration2-full"
        self.assertEqual(native.validate_gate([full], [job], self.source, False, 7, 2)["packages"], 3)

    def test_negative_full_gate_contracts(self):
        cases = [
            ("missing manifest", lambda m, j: m.pop()),
            ("duplicate manifest", lambda m, j: m.append(copy.deepcopy(m[0]))),
            ("missing job", lambda m, j: j.pop()),
            ("duplicate job", lambda m, j: j.append(copy.deepcopy(j[0]))),
            ("wrong source", lambda m, j: m[0]["source"].update(inputs_sha256="drift")),
            ("incomplete coverage", lambda m, j: m[1]["selected"].pop()),
            ("overlap", lambda m, j: m[1]["selected"].append(native.NAMESPACE)),
            ("wrong universe", lambda m, j: m[0]["universe"].remove(native.MODULE + "/x")),
            ("wrong coverage sha", lambda m, j: m[0].update(coverage_sha256="other")),
            ("empty coverage", lambda m, j: m[0].update(selected=[])),
            ("failed", lambda m, j: m[0].update(exit=7)),
            ("cancelled exit zero", lambda m, j: m[0].update(signal=15)),
            ("deadline exit zero", lambda m, j: m[0].update(timed_out=True)),
            ("wrong command", lambda m, j: m[0]["command"].append("-run=OnlyOne")),
            ("wrong profile", lambda m, j: m[0].update(cache_profile="4096")),
            ("wrong tool", lambda m, j: m[0]["go_env"].update(GOVERSION="other")),
            ("unstable source", lambda m, j: m[0].update(source_stable=False)),
            ("wrong manifest run", lambda m, j: m[0].update(run_id="8")),
            ("wrong manifest attempt", lambda m, j: m[0].update(attempt="3")),
            ("wrong job head", lambda m, j: j[0].update(head_sha="other")),
            ("wrong job run", lambda m, j: j[0].update(run_id=8)),
            ("wrong job attempt", lambda m, j: j[0].update(run_attempt=3)),
            ("clock reversal", lambda m, j: j[0].update(completed_at="2025-10-06T00:00:00Z")),
        ]
        for status in ("failure", "cancelled", "skipped", "timed_out"):
            cases.append((status, lambda m, j, status=status: j[0].update(conclusion=status)))
        for name, mutate in cases:
            with self.subTest(name=name):
                m, j = copy.deepcopy(self.manifests), copy.deepcopy(self.jobs)
                mutate(m, j)
                with self.assertRaises((ValueError, KeyError)):
                    self.gate(m, j)

    def test_discovery_stdout_only_and_failed_partial_retention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            result = subprocess.CompletedProcess([], 0, records(self.universe).encode(), b"retained nonempty stderr")
            with mock.patch.object(native.subprocess, "run", return_value=result):
                self.assertEqual(native.discover(root, root, "valid", ["./..."]), self.universe)
            self.assertEqual((root / "valid.stderr").read_bytes(), result.stderr)
            for result in (subprocess.CompletedProcess([], 3, records(self.universe[:1]).encode(), b"actual error"),
                           subprocess.TimeoutExpired(native.QUERY, 120, output=b"partial", stderr=b"timed out")):
                with mock.patch.object(native.subprocess, "run", **({"side_effect": result} if isinstance(result, Exception) else {"return_value": result})):
                    with self.assertRaises(ValueError):
                        native.discover(root, root, "failed", ["./..."])
                self.assertTrue((root / "failed.stdout").exists())
                self.assertTrue((root / "failed.stderr").exists())
                self.assertNotEqual(json.loads((root / "failed.json").read_text())["exit"], 0)
        for value in ("", "partial", records(self.universe)[:-1], records(self.universe * 2),
                      records(self.universe).replace('"Main": true', '"Main": false'),
                      json.dumps({"ImportPath": native.NAMESPACE, "Module": {"Main": True, "Path": native.MODULE}, "Incomplete": True})):
            with self.subTest(value=value[:60]):
                with self.assertRaises(ValueError):
                    native.parse_packages(value)
        with self.assertRaises(ValueError):
            native.selection([native.NAMESPACE], "remainder")
        with self.assertRaises(ValueError):
            native.selection([native.MODULE + "/x"], "namespace")

    def test_archived_query_and_execution_drift_missing_rebound_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "namespace"; path.mkdir()
            m = copy.deepcopy(self.manifests[0])
            for name, packages, arguments in (("universe", self.universe, ["./..."]), ("selected", m["selected"], m["selected"])):
                (path / (name + ".stdout")).write_text(records(packages))
                (path / (name + ".stderr")).write_text("nonempty diagnostic\n")
                (path / (name + ".json")).write_text(json.dumps({"argv": [*native.QUERY, *arguments], "exit": 0, "error": None}))
            for name in ("test.stdout", "test.stderr", "observer.stdout", "observer.stderr"):
                (path / name).write_text("")
            (path / "tool.stdout").write_text(json.dumps(m["go_env"]))
            (path / "tool.stderr").write_text("retained tool diagnostics")
            (path / "tool.json").write_text(json.dumps({"argv": ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOEXPERIMENT"], "exit": 0}))
            def save():
                m["raws"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in path.iterdir() if p.name != "manifest.json"}
                (path / "manifest.json").write_text(json.dumps(m))
            save(); self.assertEqual(len(native.load_manifests(path.parent)), 1)
            original = (path / "selected.stdout").read_text()
            (path / "selected.stdout").write_text("drift")
            with self.assertRaises(ValueError): native.load_manifests(path.parent)
            (path / "selected.stdout").write_text(records(self.universe)); save()
            with self.assertRaises(ValueError): native.load_manifests(path.parent)
            (path / "selected.stdout").write_text(original); save()
            (path / "test.stderr").unlink(); save()
            with self.assertRaises(ValueError): native.load_manifests(path.parent)
            (path / "test.stderr").write_text(""); save()
            q = json.loads((path / "universe.json").read_text()); q["argv"][-1] = "./systest/..."
            (path / "universe.json").write_text(json.dumps(q)); save()
            with self.assertRaises(ValueError): native.load_manifests(path.parent)

    def test_toolchain_and_ambient_filter_refusals(self):
        tool = self.manifests[0]["go_env"]
        native.validate_tool(tool, self.source)
        for key, value in (("GOVERSION", "go1.25.0"), ("GOARCH", "arm64"), ("CGO_ENABLED", "0"),
                           ("GOFLAGS", "-run=OnlyOne"), ("GOFLAGS", "-tags=other"), ("GOFLAGS", "-race"),
                           ("GOFLAGS", "-short"), ("GOEXPERIMENT", "experimental")):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                native.validate_tool(dict(tool, **{key: value}), self.source)

    def test_actual_capture_entrypoint_retains_failed_receipt_without_success(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = pathlib.Path(directory) / "capture"
            tool = self.manifests[0]["go_env"]
            results = [subprocess.CompletedProcess([], 0, b"", b""),
                       subprocess.CompletedProcess([], 0, json.dumps(tool).encode(), b"tool diagnostic"),
                       subprocess.CompletedProcess([], 0, records(self.universe).encode(), b"query diagnostic"),
                       subprocess.CompletedProcess([], 0, records([native.NAMESPACE]).encode(), b"selected diagnostic")]
            def failed_execute(argv, output, public):
                self.assertEqual(argv, native.command([native.NAMESPACE], True))
                for name in ("test.stdout", "test.stderr", "observer.stdout", "observer.stderr"):
                    (output / name).write_text("failed child raw\n")
                return {"exit": 7, "signal": None, "timed_out": False}
            env = dict(GITHUB_SHA="merge", GITHUB_RUN_ID="7", GITHUB_RUN_ATTEMPT="2", DGRAPH_CI_LOCAL_ALPHA_CACHE_MB="512")
            with mock.patch.dict(os.environ, env), mock.patch.object(native, "identity", return_value=self.source), \
                    mock.patch.object(native.subprocess, "run", side_effect=results), mock.patch.object(native, "execute", side_effect=failed_execute):
                self.assertEqual(native.run("namespace", destination, True), 7)
            manifest = json.loads((destination / "manifest.json").read_text())
            self.assertEqual(manifest["exit"], 7)
            self.assertTrue(manifest["source_stable"])
            self.assertEqual(len(native.load_manifests(destination.parent)), 1)
            self.assertEqual((destination / "universe.stderr").read_text(), "query diagnostic")
            self.assertEqual((destination / "test.stderr").read_text(), "failed child raw\n")
            with self.assertRaises(ValueError):
                self.gate([manifest, self.manifests[1]])

    def test_native_status_and_timeout_reaped(self):
        previous = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}
        for status in (0, 7):
            with tempfile.TemporaryDirectory() as directory:
                result = native.execute([sys.executable, "-c", "raise SystemExit(" + str(status) + ")"], pathlib.Path(directory), False)
                self.assertEqual(result["exit"], status)
        self.assertEqual(previous, {n: signal.getsignal(n) for n in previous})
        with tempfile.TemporaryDirectory() as directory:
            child = mock.Mock(pid=1234, returncode=-signal.SIGQUIT)
            child.wait.side_effect = [subprocess.TimeoutExpired("native", 2700), -signal.SIGQUIT]
            child.poll.return_value = -signal.SIGQUIT
            sampler = mock.Mock()
            with mock.patch.object(native.subprocess, "Popen", side_effect=[sampler, child]), mock.patch.object(native.os, "killpg") as kill:
                result = native.execute(["controlled native"], pathlib.Path(directory), True)
            self.assertEqual(result["exit"], 124)
            self.assertTrue(result["timed_out"])
            kill.assert_called_once_with(1234, signal.SIGQUIT)
            self.assertEqual(child.wait.call_args_list, [mock.call(timeout=2700), mock.call(timeout=90)])
            sampler.terminate.assert_called_once(); sampler.wait.assert_called_once_with(timeout=8)

    def test_signal_cannot_become_success(self):
        for number in (signal.SIGINT, signal.SIGTERM):
            with tempfile.TemporaryDirectory() as directory:
                child = subprocess.Popen([sys.executable, __file__, "--signal-child", directory], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    ready = pathlib.Path(directory) / "ready"
                    import time
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        self.assertIsNone(child.poll()); time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child.send_signal(number); stdout, stderr = child.communicate(timeout=5)
                    self.assertEqual(child.returncode, 128 + number, stdout + stderr)
                finally:
                    if child.poll() is None: child.kill(); child.wait()

    def test_workflow_independent_cleanup_module_admission_and_public_profile(self):
        repo = pathlib.Path(__file__).resolve().parents[2]
        text = (repo / ".github/workflows/ci-dgraph-integration2-tests.yml").read_text()
        step = text.split("      - name: Run Integration2 Tests\n", 1)[1].split("\n      - ", 1)[0]
        cleanup = text.split("      - name: Clean Up Environment After Tests\n", 1)[1].split("\n      - ", 1)[0]
        self.assertIn("        if: always()\n", cleanup)
        self.assertNotIn("continue-on-error", step + cleanup)
        self.assertLess(text.index("git diff --exit-code HEAD -- go.mod go.sum"), text.index("      - name: Run Integration2 Tests"))
        self.assertIn("DGRAPH_CI_LOCAL_ALPHA_CACHE_MB:", text)
        self.assertIn("'[\"namespace\",\"remainder\"]'", text)
        self.assertEqual(text.count("timeout-minutes: 90"), 1)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for name in ("bin", "home/go/bin", "dgraph", "results"): (root / name).mkdir(parents=True)
            (root / "dgraph/dgraph").write_text("fixture binary")
            for name, value in {"python3": "exit 7", "go": 'printf "cleaned\\n" >> "$RUNNER_TEMP/cleanup"', "sleep": "exit 0"}.items():
                path = root / "bin" / name; path.write_text("#!/bin/bash\n" + value + "\n"); path.chmod(0o755)
            env = dict(os.environ, HOME=str(root / "home"), PATH=str(root / "bin") + os.pathsep + os.environ["PATH"], RUNNER_TEMP=str(root / "results"))
            script = textwrap.dedent(step.split("        run: |\n", 1)[1])
            import re
            script = re.sub(r"\$\{\{.*?\}\}", "namespace", script)
            result = subprocess.run(["bash", "-eo", "pipefail", "-c", script], cwd=root, env=env, capture_output=True)
            self.assertEqual(result.returncode, 7, result.stderr)
            cleaned = subprocess.run(["bash", "-eo", "pipefail", "-c", textwrap.dedent(cleanup.split("        run: |\n", 1)[1])], cwd=root, env=env, capture_output=True)
            self.assertEqual(cleaned.returncode, 0, cleaned.stderr)
            self.assertEqual((root / "results/cleanup").read_text(), "cleaned\n")
            self.assertEqual(result.returncode, 7)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--signal-child"]:
        directory = pathlib.Path(sys.argv[2])
        program = "import pathlib,signal,time;signal.signal(signal.SIGTERM,lambda *a:exit(0));signal.signal(signal.SIGINT,lambda *a:exit(0));pathlib.Path(" + repr(str(directory / "ready")) + ").touch();time.sleep(300)"
        sys.exit(native.execute([sys.executable, "-c", program], directory, False)["exit"])
    unittest.main()
