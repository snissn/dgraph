// SPDX-License-Identifier: Apache-2.0
package main

import (
	"fmt"
	"time"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
)

type progressSample struct {
	completed time.Duration
	latencyMS float64
}

// Only the existing sample collector mutates this state. Completion timestamps
// reuse each RPC's existing start and elapsed measurement: no extra RPC, clock
// read, lock, or server observer is added inside the timed workload.
type workloadProgress struct {
	started time.Time
	elapsed time.Duration
	samples []progressSample
}

func (p *workloadProgress) addMetrics(out map[string]livebench.Metric) {
	if p.elapsed <= 0 {
		return
	}
	const windows = 10
	var latencies [windows][]float64
	for _, s := range p.samples {
		bucket := int(float64(s.completed) / float64(p.elapsed) * windows)
		bucket = max(0, min(windows-1, bucket))
		latencies[bucket] = append(latencies[bucket], s.latencyMS)
	}
	for i, values := range latencies {
		prefix := fmt.Sprintf("workload_window_%02d_", i+1)
		source := "successful RPC completions in equal wall-time windows within exercise; includes client validation"
		out[prefix+"completions"] = availability(float64(len(values)), true, "operations", source, "")
		out[prefix+"ops_s"] = availability(float64(len(values))*windows/p.elapsed.Seconds(), true, "operations/second", source, "")
		if len(values) == 0 {
			out[prefix+"p99_ms"] = availability(0, false, "milliseconds", source, "no RPC completed in this window")
		} else {
			out[prefix+"p99_ms"] = availability(livebench.Percentiles(values)["p99"], true, "milliseconds", source, "")
		}
	}
}

// These observations use the existing before/after status requests outside the
// timed phase. In particular, backend apply calls are distinct from WAL flushes.
func addMaintenanceMetrics(out map[string]livebench.Metric, backend string, before, after map[string]float64) {
	for _, item := range []struct{ metric, stat string }{
		{"treedb_backend_apply_calls", "treedb.flush_apply.apply_calls_total"},
		{"treedb_backend_apply_ops", "treedb.flush_apply.apply_ops_total"},
	} {
		v, ok := delta(after, before, item.stat)
		out[item.metric] = availability(v, backend == "treedb" && ok && v >= 0, "count", "TreeDB /debug/store delta "+item.stat, "counter unavailable or reset")
	}
	for _, item := range []struct{ metric, stat, unit string }{
		{"treedb_queue_backlog", "treedb.cache.queue_backlog_bytes", "bytes"},
		{"treedb_mutable", "treedb.cache.mutable_bytes", "bytes"},
		{"treedb_queue_length", "treedb.cache.queue_len", "count"},
		{"treedb_dependency_debt", "treedb.command_wal.dependency_debt.pending_bytes", "bytes"},
	} {
		for _, boundary := range []struct {
			name   string
			values map[string]float64
		}{{"before", before}, {"after", after}} {
			v, ok := boundary.values[item.stat]
			out[item.metric+"_"+boundary.name] = availability(v, backend == "treedb" && ok && v >= 0, item.unit, "TreeDB /debug/store "+boundary.name+" "+item.stat, "gauge unavailable")
		}
	}
}
