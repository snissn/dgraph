#!/usr/bin/env bash
# Opt-in CI prerequisite only; never publish or replace product source.
set -euo pipefail
python3 - "$@" <<'PY'
import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys
import tarfile
import urllib.request

PINS = (
    ("2020", "3595cb1267b3a9df009978c9f8f0a13a77f7e84b",
     "c3ae92366759dfff6f15d8c229021cabb3f36d7c340e20d76b5cc6aaba93b877",
     "2020-11-13T20:10:18Z", "RELEASE.2020-11-13T20-10-18Z"),
    ("2025", "07c3a429bfed433e49018cb0f78a52145d4bedeb",
     "8819e3e7817e46b7b3798f8f200ead208562e571563c2e040352378031abe9f2",
     "2025-09-07T16:13:09Z", "RELEASE.2025-09-07T16-13-09Z"),
)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def inventory(path):
    return {str(p.relative_to(path)): digest(p)
            for p in sorted(path.rglob("*")) if p.is_file()}

def output(argv):
    return subprocess.check_output(argv, text=True).strip()

if len(sys.argv) != 2 or platform.system() != "Linux":
    sys.exit("usage: build.sh NEW_ABSOLUTE_EVIDENCE_DIRECTORY (native Linux only)")
arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
if arch is None:
    sys.exit("unsupported native architecture")
go = pathlib.Path(shutil.which("go") or "").resolve()
if not go.is_file() or output([str(go), "version"]) != "go version go1.26.4 linux/" + arch:
    sys.exit("requires native Go1.26.4")
tool_hash = digest(go)
daemon_arch = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}.get(
    output(["docker", "info", "--format", "{{.Architecture}}"] ))
if daemon_arch != arch:
    sys.exit("native Docker daemon architecture required")
base = json.loads(output(["docker", "image", "inspect", "dgraph/dgraph:local"]))[0]
if base["Os"] != "linux" or base["Architecture"] != arch:
    sys.exit("native source-bound dgraph/dgraph:local image required")
root = pathlib.Path.cwd()
helper = root / "contrib/ci-minio"
if not all((helper / ("Dockerfile." + year)).is_file() for year, *_ in PINS):
    sys.exit("run from the repository root")
out = pathlib.Path(sys.argv[1])
if not out.is_absolute() or out.exists() or out.resolve() != out:
    sys.exit("evidence directory must be a new absolute path without aliases")
out.mkdir(parents=True)
env = {"HOME": str(pathlib.Path.home()), "PATH": str(go.parent) + os.pathsep + os.defpath,
       "GOENV": "off", "GOWORK": "off", "GOTOOLCHAIN": "local", "GOFLAGS": "-p=2",
       "GOMAXPROCS": "2", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": arch}
for key, override in (("GOCACHE", "CI_MINIO_GOCACHE"), ("GOMODCACHE", "CI_MINIO_GOMODCACHE")):
    value = pathlib.Path(os.environ.get(override, str(out / key.lower())))
    if not value.is_absolute() or value.resolve() != value:
        sys.exit("cache path must be absolute without aliases")
    env[key] = str(value)
receipt = {"state": "ATTEMPTED", "architecture": arch, "go": str(go),
           "go_sha256": tool_hash, "runtime_image": base["Id"], "env": env,
           "helper_inputs": inventory(helper), "commands": [], "versions": []}

def save():
    (out / "receipt.json").write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")

def run(name, argv, cwd, controlled_env=None):
    row = {"name": name, "argv": argv, "cwd": str(cwd)}
    receipt["commands"].append(row)
    save()
    with (out / (name + ".log")).open("wb") as log:
        proc = subprocess.run(argv, cwd=cwd, env=controlled_env,
                              stdout=log, stderr=subprocess.STDOUT)
    row.update(returncode=proc.returncode, raw_sha256=digest(out / (name + ".log")))
    save()
    if proc.returncode:
        raise RuntimeError(name + " failed")

try:
    for year, commit, archive_hash, version, release in PINS:
        archive = out / (year + ".tar.gz")
        url = "https://codeload.github.com/minio/minio/tar.gz/" + commit
        with urllib.request.urlopen(url, timeout=60) as response, archive.open("wb") as dest:
            shutil.copyfileobj(response, dest)
        if digest(archive) != archive_hash:
            raise ValueError("pinned archive checksum mismatch: " + year)
        src = out / ("minio-" + commit)
        with tarfile.open(archive, "r:gz") as tar:
            members = tar.getmembers()
            for member in members:
                parts = pathlib.PurePosixPath(member.name).parts
                if (not parts or parts[0] != src.name or ".." in parts or
                        pathlib.PurePosixPath(member.name).is_absolute() or
                        not (member.isfile() or member.isdir())):
                    raise ValueError("unexpected archive member")
            tar.extractall(out, members=members)
        before = inventory(src)
        context = out / (year + "-image")
        context.mkdir()
        flags = "-s -w"
        for key, value in (("Version", version), ("ReleaseTag", release),
                           ("CommitID", commit), ("ShortCommitID", commit[:12])):
            flags += " -X github.com/minio/minio/cmd." + key + "=" + value
        if year == "2025":
            flags += " -X github.com/minio/minio/cmd.CopyrightYear=2025"
        argv = [str(go), "build", "-mod=readonly", "-trimpath", "-ldflags", flags,
                "-o", str(context / "minio"), "."]
        run(year + "-build", argv, src, env)
        if inventory(src) != before or digest(go) != tool_hash:
            raise ValueError("source or tool drift")
        for name, source in (("docker-entrypoint.sh", "dockerscripts/docker-entrypoint.sh"),
                             ("LICENSE", "LICENSE"), ("CREDITS", "CREDITS")):
            shutil.copy2(src / source, context / name)
        shutil.copy2(helper / ("Dockerfile." + year), context / "Dockerfile")
        tag = "dgraph/ci-minio:" + year + "-source"
        run(year + "-package", ["docker", "build", "--build-arg", "RUNTIME_IMAGE=dgraph/dgraph:local",
                                "-t", tag, str(context)], root)
        image = json.loads(output(["docker", "image", "inspect", tag]))[0]
        if (image["Architecture"] != arch or digest(go) != tool_hash or
                image["RootFS"]["Layers"][:len(base["RootFS"]["Layers"])] != base["RootFS"]["Layers"]):
            raise ValueError("packaged architecture, tool, or runtime layer drift")
        receipt["versions"].append({"year": year, "commit": commit, "release": release,
                                    "archive_sha256": archive_hash, "source": before,
                                    "context": inventory(context), "image": image})
        save()
    if inventory(helper) != receipt["helper_inputs"]:
        raise ValueError("helper source drift")
    if json.loads(output(["docker", "image", "inspect", "dgraph/dgraph:local"]))[0]["Id"] != base["Id"]:
        raise ValueError("runtime image drift")
    # Existing legacy fixtures and the t/ ARM alias continue to select genuine2020.
    legacy = "dgraph/ci-minio:2020-source"
    for tag in ("minio/minio:RELEASE.2020-11-13T20-10-18Z",
                "minio/minio:RELEASE.2020-11-13T20-10-18Z-arm64"):
        run("legacy-tag-" + tag.rsplit(":", 1)[1], ["docker", "tag", legacy, tag], root)
    receipt["state"] = "PACKAGED_CI_PREREQUISITE"
except Exception as error:
    receipt.update(state="FAILED", error=str(error))
    save()
    raise
save()
PY
