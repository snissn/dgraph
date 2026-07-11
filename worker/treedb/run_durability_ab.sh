#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -Eeuo pipefail

usage() {
	printf '%s\n' 'usage: worker/treedb/run_durability_ab.sh [--smoke] --artifact-dir NEW_DIR' \
		'Runs adapter microbenchmarks and the four-cell live durability matrix.' \
		'Decision runs also collect separate relaxed/durable TreeDB CPU profiles.' \
		'Environment overrides: REPEATS DATASET_NODES WARMUP_OPS TIMED_OPS CONCURRENCY COUNT BENCHTIME PROFILE_RELAXED_OPS PROFILE_DURABLE_OPS PROFILE_SECONDS.'
}

smoke=0
artifact_dir=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--smoke)
		smoke=1
		shift
		;;
	--artifact-dir)
		artifact_dir=${2:?missing artifact directory}
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
done
[[ -n ${artifact_dir} ]] || {
	usage >&2
	exit 2
}
[[ ! -e ${artifact_dir} ]] || {
	echo "artifact directory already exists: ${artifact_dir}" >&2
	exit 1
}

repo_root=$(git rev-parse --show-toplevel)
cd "${repo_root}"
mkdir -p "${artifact_dir}/bin" "${artifact_dir}/microbench"

repeats=${REPEATS:-3}
dataset=${DATASET_NODES:-500}
warmup=${WARMUP_OPS:-100}
timed=${TIMED_OPS:-2000}
concurrency=${CONCURRENCY:-4}
count=${COUNT:-5}
benchtime=${BENCHTIME:-1s}
if [[ ${smoke} -eq 1 ]]; then
	repeats=${REPEATS:-1}
	dataset=${DATASET_NODES:-50}
	warmup=${WARMUP_OPS:-10}
	timed=${TIMED_OPS:-50}
	concurrency=${CONCURRENCY:-2}
	count=${COUNT:-1}
	benchtime=${BENCHTIME:-1x}
fi

GOWORK=off go build -o "${artifact_dir}/bin/dgraph" ./dgraph
GOWORK=off go build -o "${artifact_dir}/bin/livebench" ./worker/treedb/livebench/cmd
GOWORK=off go build -o "${artifact_dir}/bin/report" ./worker/treedb/livebench/reportcmd

micro_cmd=(go test ./posting -run '^$' -bench 'Benchmark(BadgerStoreSeam|TreeDBStoreAdapterOverhead|CommitEventDisabledAndUnsubscribed)$' -benchmem -benchtime "${benchtime}" -count "${count}")
GOWORK=off "${micro_cmd[@]}" 2>&1 | tee "${artifact_dir}/microbench/raw.txt"

storage_source=$(findmnt -no SOURCE -T "${artifact_dir}")
storage_filesystem=$(findmnt -no FSTYPE -T "${artifact_dir}")
storage_mountpoint=$(findmnt -no TARGET -T "${artifact_dir}")
storage_device=${storage_source%%\[*}
storage_parent=$(lsblk -ndo PKNAME "${storage_device}" 2>/dev/null || true)
if [[ -n ${storage_parent} ]]; then
	storage_device="/dev/${storage_parent%%$'\n'*}"
fi
storage_model=$(lsblk -dn -o MODEL "${storage_device}")
storage_size_bytes=$(lsblk -dn -b -o SIZE "${storage_device}")
ram_total_bytes=$(awk '/^MemTotal:/ { print $2 * 1024 }' /proc/meminfo)
cpu_model=$(awk -F ': ' '/^model name/ { print $2; exit }' /proc/cpuinfo)
context_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
dgraph_sha=$(git rev-parse HEAD)
dirty_status=$(git status --porcelain)
dirty=false
if [[ -n ${dirty_status} ]]; then
	dirty=true
fi
gomap_version=$(GOWORK=off go list -m github.com/snissn/gomap)
host=$(hostname)
kernel=$(uname -srvmo)
go_version=$(go version)
construction_audit=$(pgrep -af construction_audit.py || true)
read -r loadavg </proc/loadavg

{
	echo "utc=${context_utc}"
	echo "dgraph_sha=${dgraph_sha}"
	echo "dirty=${dirty}"
	echo "gomap=${gomap_version}"
	echo "host=${host}"
	echo "kernel=${kernel}"
	echo "go=${go_version}"
	echo "cpu=${cpu_model}"
	echo "ram_total_bytes=${ram_total_bytes}"
	echo "storage_scope=artifact_and_posting"
	echo "storage_source=${storage_source}"
	echo "storage_model=${storage_model}"
	echo "storage_size_bytes=${storage_size_bytes}"
	echo "storage_filesystem=${storage_filesystem}"
	echo "storage_mountpoint=${storage_mountpoint}"
	echo "environment_GOWORK=${GOWORK-}"
	echo "environment_TMPDIR=${TMPDIR-}"
	echo "environment_GOMAXPROCS=${GOMAXPROCS-}"
	echo "environment_GOFLAGS=${GOFLAGS-}"
	echo "construction_audit=${construction_audit}"
	echo "loadavg=${loadavg}"
	echo "micro_command=GOWORK=off ${micro_cmd[*]}"
	echo "live_shape=repeats=${repeats} dataset=${dataset} warmup=${warmup} timed=${timed} concurrency=${concurrency} seed=20260711 topology=single-zero-single-alpha mix=60-point/20-one-hop/20-write"
	echo "measurement_boundary=database creation, schema, load, and warmup precede timed operations; restart recovery is measured separately"
} >"${artifact_dir}/context.txt"

result_paths=()
cell=0
for class in relaxed durable; do
	for backend in badger treedb; do
		for repeat in $(seq 1 "${repeats}"); do
			run_dir="${artifact_dir}/live/${backend}-${class}-r${repeat}"
			offset=$((23000 + cell * 100 + repeat * 2))
			"${artifact_dir}/bin/livebench" \
				--dgraph-bin "${artifact_dir}/bin/dgraph" --artifact-dir "${run_dir}" \
				--backend "${backend}" --durability "${class}" --repeat "${repeat}" \
				--dataset-nodes "${dataset}" --warmup-ops "${warmup}" --timed-ops "${timed}" \
				--concurrency "${concurrency}" --zero-port-offset "${offset}" --alpha-port-offset "$((offset + 40))"
			result_paths+=("${run_dir}/result.json")
		done
		cell=$((cell + 1))
	done
done

profile_args=()
if [[ ${smoke} -eq 0 ]]; then
	profile_relaxed_ops=${PROFILE_RELAXED_OPS:-100000}
	profile_durable_ops=${PROFILE_DURABLE_OPS:-10000}
	profile_seconds=${PROFILE_SECONDS:-5}
	profile_dir="${artifact_dir}/profiles"
	mkdir -p "${profile_dir}"
	for profile_class in relaxed durable; do
		profile_ops=${profile_relaxed_ops}
		profile_offset=26000
		if [[ ${profile_class} == durable ]]; then
			profile_ops=${profile_durable_ops}
			profile_offset=26200
		fi
		profile_run="${profile_dir}/treedb-${profile_class}-run"
		profile_path="${profile_dir}/treedb-${profile_class}.pprof"
		"${artifact_dir}/bin/livebench" \
			--dgraph-bin "${artifact_dir}/bin/dgraph" --artifact-dir "${profile_run}" \
			--backend treedb --durability "${profile_class}" --repeat 1 \
			--dataset-nodes "${dataset}" --warmup-ops "${warmup}" --timed-ops "${profile_ops}" \
			--concurrency "${concurrency}" --zero-port-offset "${profile_offset}" --alpha-port-offset "$((profile_offset + 40))" \
			--cpu-profile "${profile_path}" --profile-seconds "${profile_seconds}"
		go tool pprof -top -nodecount=25 "${artifact_dir}/bin/dgraph" "${profile_path}" |
			tee "${profile_dir}/treedb-${profile_class}-top.txt"
	done
	profile_args=(--profile-dir "${profile_dir}")
fi

"${artifact_dir}/bin/report" --repeats "${repeats}" --out "${artifact_dir}/report.md" "${profile_args[@]}" "${result_paths[@]}"
printf 'report=%s\ncontext=%s\nmicrobench=%s\n' "${artifact_dir}/report.md" "${artifact_dir}/context.txt" "${artifact_dir}/microbench/raw.txt"
if [[ ${smoke} -eq 0 ]]; then
	printf 'profiles=%s\n' "${artifact_dir}/profiles"
fi
