// SPDX-License-Identifier: Apache-2.0
package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
)

func TestProgressWindowsUseCompletionTimeAndPreserveEmptyWindows(t *testing.T) {
	p := workloadProgress{elapsed: 10 * time.Second, samples: []progressSample{
		{completed: 9500 * time.Millisecond, latencyMS: 9},
		{completed: 100 * time.Millisecond, latencyMS: 1},
		{completed: 10 * time.Second, latencyMS: 10},
	}}
	out := map[string]livebench.Metric{}
	p.addMetrics(out)
	if out["workload_window_01_completions"].Value != 1 || out["workload_window_10_completions"].Value != 2 || out["workload_window_10_ops_s"].Value != 2 {
		t.Fatalf("wrong completion windows: %v", out)
	}
	if out["workload_window_02_p99_ms"].Available || !out["workload_window_10_p99_ms"].Available {
		t.Fatal("empty window must not invent a latency")
	}
	var total float64
	for i := 1; i <= 10; i++ {
		total += out[fmt.Sprintf("workload_window_%02d_completions", i)].Value
	}
	if total != 3 {
		t.Fatalf("lost samples: %v", total)
	}
}

func TestMaintenanceMetricsSeparateBackendApplyFromWALFlush(t *testing.T) {
	out := map[string]livebench.Metric{}
	addMaintenanceMetrics(out, "treedb", map[string]float64{"treedb.flush_apply.apply_calls_total": 4}, map[string]float64{"treedb.flush_apply.apply_calls_total": 7, "treedb.command_wal.flush.count_total": 999, "treedb.cache.queue_backlog_bytes": 42})
	if m := out["treedb_backend_apply_calls"]; !m.Available || m.Value != 3 {
		t.Fatalf("apply metric: %v", m)
	}
	if out["treedb_queue_backlog_before"].Available || out["treedb_queue_backlog_after"].Value != 42 {
		t.Fatal("missing gauge must remain unavailable")
	}
	addMaintenanceMetrics(out, "badger", nil, nil)
	if out["treedb_backend_apply_calls"].Available {
		t.Fatal("TreeDB metrics must not be available for Badger")
	}
}
