// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validResult(backend, class string) Result {
	now := time.Now()
	posting := "backend=badger; tier=production; durability=durable; events=false"
	badger := "syncwrites=false"
	observed := "syncwrites=false"
	if backend == "treedb" {
		posting = "backend=treedb; tier=benchmark_minimal; durability=" + class + "; events=true"
		badger = "syncwrites=false"
		observed = "wal_on_sync+no_read_checksum"
		if class == "relaxed" {
			observed = "wal_on_relaxed_sync+no_read_checksum"
		}
	}
	if backend == "badger" && class == "durable" {
		badger = "syncwrites=true"
		observed = "syncwrites=true"
	}
	metrics := map[string]Metric{}
	for _, name := range requiredMetrics {
		metrics[name] = Metric{Available: true, Value: 1, Unit: "count", Source: "test"}
	}
	return Result{SchemaVersion: 1, RunID: backend + "-" + class, Repeat: 1,
		Config:       Config{Backend: backend, DurabilityClass: class, PostingStore: posting, Badger: badger, DatasetNodes: 10, Concurrency: 1, WarmupOps: 1, TimedOps: 10, QueryMix: map[string]int{"read": 80, "write": 20}, Topology: "single-zero-single-alpha", Seed: 42},
		Context:      Context{DgraphSHA: "abc", GomapVersion: "v1", GoVersion: "go1", Host: "host", ExactCommand: []string{"bench"}, RawPath: "/raw"},
		SetupStarted: now, SetupFinished: now.Add(time.Second), TimedStarted: now.Add(2 * time.Second), TimedFinished: now.Add(3 * time.Second), Throughput: 10, LatencyMS: map[string]float64{"p50": 1, "p95": 2, "p99": 3}, Metrics: metrics,
		Validation: Validation{BackendObserved: backend, DurabilityObserved: observed, SchemaOK: true, PostingChecksum: "same", NodeCount: 12, RestartOK: true, UnsupportedOK: true}}
}

func TestResultRejectsWrongBackendDurabilityMissingMetricAndSetupLeakage(t *testing.T) {
	tests := []struct {
		name, want string
		mutate     func(*Result)
	}{
		{"backend", "wrong backend", func(r *Result) { r.Validation.BackendObserved = "badger" }},
		{"durability", "wrong durability", func(r *Result) { r.Validation.DurabilityObserved = "durable" }},
		{"metric", "missing metric", func(r *Result) { delete(r.Metrics, "cpu_seconds") }},
		{"setup", "setup leaked", func(r *Result) { r.SetupFinished = r.TimedStarted.Add(time.Second) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := validResult("treedb", "relaxed")
			tc.mutate(&r)
			if err := r.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateSetRejectsIncompleteAndLogicalMismatch(t *testing.T) {
	if err := ValidateSet([]Result{validResult("badger", "relaxed")}, 1); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("got %v", err)
	}
	set := []Result{validResult("badger", "relaxed"), validResult("treedb", "relaxed"), validResult("badger", "durable"), validResult("treedb", "durable")}
	set[1].Validation.PostingChecksum = "different"
	if err := ValidateSet(set, 1); err == nil || !strings.Contains(err.Error(), "parity mismatch") {
		t.Fatalf("got %v", err)
	}
}

func TestWriteImmutableRefusesOverwrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "result.json")
	if err := WriteImmutable(p, map[string]int{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if err := WriteImmutable(p, map[string]int{"x": 2}); !os.IsExist(err) {
		t.Fatalf("got %v", err)
	}
}
