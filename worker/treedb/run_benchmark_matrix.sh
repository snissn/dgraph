#!/usr/bin/env bash
# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

usage() {
	cat <<'USAGE'
Run the Dgraph Badger-vs-TreeDB benchmark matrix and write artifacts.

Usage:
  worker/treedb/run_benchmark_matrix.sh [--smoke] [--artifact-dir DIR]

Environment:
  ARTIFACT_DIR   Artifact directory. Defaults to /tmp/dgraph-treedb-bench/<UTC timestamp>.
  BENCHTIME      Go -benchtime value. Defaults to 500ms, or 1x with --smoke.
  COUNT          Go -count value. Defaults to 1.
  TIMEOUT        Go -timeout value. Defaults to 10m.
  BENCH          Benchmark regexp. Defaults to ^BenchmarkDgraphTreeDBMatrix$.
  BASELINE_REF   Reference recorded as baseline context. Defaults to origin/main.
  CANDIDATE_REF  Reference recorded as candidate context. Defaults to HEAD.

Examples:
  worker/treedb/run_benchmark_matrix.sh --smoke
  BENCHTIME=1s COUNT=5 worker/treedb/run_benchmark_matrix.sh --artifact-dir /tmp/dgraph-treedb-bench/full
USAGE
}

smoke=0
artifact_dir="${ARTIFACT_DIR:-}"
bench_key_count=256
bench_version_count=4
bench_value_size=128
bench_batch_size=16

while [[ $# -gt 0 ]]; do
	case "$1" in
	--smoke)
		smoke=1
		shift
		;;
	--artifact-dir)
		if [[ $# -lt 2 ]]; then
			echo "--artifact-dir requires a value" >&2
			exit 2
		fi
		artifact_dir="$2"
		shift 2
		;;
	--help | -h)
		usage
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

repo_root=$(git rev-parse --show-toplevel)
cd "${repo_root}"

if [[ -z "${artifact_dir}" ]]; then
	artifact_dir="/tmp/dgraph-treedb-bench/$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "${artifact_dir}"

bench="${BENCH:-^BenchmarkDgraphTreeDBMatrix$}"
benchtime="${BENCHTIME:-500ms}"
count="${COUNT:-1}"
timeout="${TIMEOUT:-10m}"
if [[ "${smoke}" -eq 1 ]]; then
	benchtime="${BENCHTIME:-1x}"
	count="${COUNT:-1}"
fi

baseline_ref="${BASELINE_REF:-origin/main}"
candidate_ref="${CANDIDATE_REF:-HEAD}"
baseline_sha="unresolved"
if baseline_sha_resolved=$(git rev-parse --verify "${baseline_ref}" 2>/dev/null); then
	baseline_sha="${baseline_sha_resolved}"
fi
candidate_sha=$(git rev-parse --verify "${candidate_ref}")
raw_file="${artifact_dir}/raw.txt"
context_file="${artifact_dir}/context.txt"
summary_file="${artifact_dir}/summary.md"

cmd=(go test ./worker/treedb -run '^$' -bench "${bench}" -benchtime "${benchtime}" -count "${count}" -timeout "${timeout}" -benchmem -v)

utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
repo_url=$(git config --get remote.origin.url || true)
branch_name=$(git branch --show-current || true)
gocache=$(GOWORK=off go env GOCACHE)
gomodcache=$(GOWORK=off go env GOMODCACHE)
go_version=$(go version)
goos=$(go env GOOS)
goarch=$(go env GOARCH)
cpu=$(awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null || true)
kernel=$(uname -srvmo)

{
	echo "Dgraph TreeDB benchmark matrix context"
	echo "======================================"
	echo "utc: ${utc}"
	echo "repo: ${repo_url}"
	echo "branch: ${branch_name}"
	echo "baseline_ref: ${baseline_ref}"
	echo "baseline_sha: ${baseline_sha}"
	echo "candidate_ref: ${candidate_ref}"
	echo "candidate_sha: ${candidate_sha}"
	echo "GOWORK: off"
	echo "TMPDIR: ${TMPDIR:-/tmp}"
	echo "GOCACHE: ${gocache}"
	echo "GOMODCACHE: ${gomodcache}"
	echo "go_version: ${go_version}"
	echo "goos: ${goos}"
	echo "goarch: ${goarch}"
	echo "cpu: ${cpu}"
	echo "kernel: ${kernel}"
	echo "benchmark_package: ./worker/treedb"
	echo "benchmark_regex: ${bench}"
	echo "benchtime: ${benchtime}"
	echo "count: ${count}"
	echo "timeout: ${timeout}"
	echo "fixture_shape: keys=${bench_key_count} versions=${bench_version_count} value_bytes=${bench_value_size} batch_size=${bench_batch_size}"
	echo "timed_boundary: Go benchmark timers exclude fixture setup, database open, and benchmark artifact generation; each row times only the named Badger/TreeDB operation loop."
	echo "exact_command: GOWORK=off ${cmd[*]}"
} >"${context_file}"

set +e
GOWORK=off "${cmd[@]}" 2>&1 | tee "${raw_file}"
status=${PIPESTATUS[0]}
set -e

{
	echo "# Dgraph Badger-vs-TreeDB Benchmark Matrix"
	echo
	echo "- Artifact directory: \`${artifact_dir}\`"
	echo "- Context: \`${context_file}\`"
	echo "- Raw output: \`${raw_file}\`"
	echo "- Baseline: \`${baseline_ref}\` / \`${baseline_sha}\`"
	echo "- Candidate: \`${candidate_ref}\` / \`${candidate_sha}\`"
	echo "- Exact command: \`GOWORK=off ${cmd[*]}\`"
	echo "- Measurement boundary: fixture setup, DB open, and artifact generation are outside Go benchmark timers."
	echo
	echo "## Timed rows"
	echo
	awk '
    function esc(s) { gsub(/\|/, "\\|", s); return s }
    function base_name(s) { sub(/-[0-9]+$/, "", s); return s }
    BEGIN {
      print "| Row | Iterations | ns/op | ops/sec | B/op | allocs/op | Other metrics |"
      print "| --- | ---: | ---: | ---: | ---: | ---: | --- |"
    }
    /^Benchmark/ && $2 ~ /^[0-9]+$/ {
      name = base_name($1)
      iterations = $2
      ns = ""
      bytes = ""
      allocs = ""
      other = ""
      for (i = 3; i <= NF; i++) {
        if (i < NF && $(i+1) == "ns/op") { ns = $i; i++; continue }
        if (i < NF && $(i+1) == "B/op") { bytes = $i; i++; continue }
        if (i < NF && $(i+1) == "allocs/op") { allocs = $i; i++; continue }
        if (i < NF && $i ~ /^[-+0-9.eE]+$/) {
          metric = $i " " $(i+1)
          if (other != "") { other = other "<br>" }
          other = other metric
          i++
        }
      }
      ops = ""
      if (ns + 0 > 0) { ops = sprintf("%.2f", 1000000000 / (ns + 0)) }
      print "| `" esc(name) "` | " iterations " | " ns " | " ops " | " bytes " | " allocs " | " esc(other) " |"
    }
  ' "${raw_file}"
	echo
	echo "## Explicit blocker / skip rows"
	echo
	echo "| Row | Status | Reason |"
	echo "| --- | --- | --- |"
	echo "| \`Blocked/ManagedTimestampTransactions\` | skipped | TreeDB does not expose Badger-compatible OpenManaged/NewTransactionAt/CommitAt/SetEntryAt semantics required by Dgraph posting stores. |"
	echo "| \`Blocked/EntryMetadataAndTTL\` | skipped | TreeDB primitives do not yet provide Badger Entry.UserMeta/Item.UserMeta/Entry.ExpiresAt compatibility for Dgraph posting metadata. |"
	echo "| \`Blocked/AllVersionKeyIterator\` | skipped | TreeDB native revisions are not a Badger IteratorOptions.AllVersions/NewKeyIterator substitute for Dgraph posting-list version scans. |"
	echo "| \`Blocked/StreamBackupExport\` | skipped | TreeDB does not yet provide Dgraph's Badger NewStreamAt/Stream.Orchestrate backup-export contract. |"
	echo "| \`Blocked/StreamWriterImport\` | skipped | TreeDB does not yet provide Dgraph's Badger NewStreamWriter import/restore contract. |"
	echo "| \`Blocked/Subscriptions\` | skipped | TreeDB does not yet provide the Badger Subscribe API used by worker.SubscribeForUpdates. |"
	echo "| \`Blocked/EncryptionKeyRegistry\` | skipped | TreeDB Dgraph scaffold intentionally fails closed for Badger-compatible encryption and key registry APIs. |"
	echo
	echo "## Result"
	echo
	if [[ "${status}" -eq 0 ]]; then
		echo "go test benchmark command exited successfully."
	else
		echo "go test benchmark command failed with exit status ${status}."
	fi
} >"${summary_file}"

printf 'artifact_dir=%s\ncontext=%s\nraw=%s\nsummary=%s\nstatus=%s\n' \
	"${artifact_dir}" "${context_file}" "${raw_file}" "${summary_file}" "${status}"

exit "${status}"
