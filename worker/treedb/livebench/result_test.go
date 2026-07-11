// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"math"
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
		observed = "wal_on_sync"
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
	return Result{SchemaVersion: SchemaVersion, RunID: backend + "-" + class, Repeat: 1,
		Config:       Config{Backend: backend, DurabilityClass: class, PostingStore: posting, Badger: badger, DatasetNodes: 10, Concurrency: 1, WarmupOps: 1, TimedOps: 10, QueryMix: map[string]int{"read": 80, "write": 20}, Topology: "single-zero-single-alpha", Seed: 42},
		Context:      Context{DgraphSHA: "abc", GomapVersion: "v1", GoVersion: "go1", Host: "host", Kernel: "kernel", CPU: "cpu", TotalRAMBytes: 1024, Storage: StorageContext{Scope: "artifact_and_posting", Source: "/dev/test", Model: "test", SizeBytes: 2048, Filesystem: "ext4", Mountpoint: "/mnt"}, Environment: map[string]string{"GOWORK": "off", "TMPDIR": "/tmp", "GOMAXPROCS": "", "GOFLAGS": ""}, ExactCommand: []string{"bench"}, RawPath: "/raw"},
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

func TestResultRejectsUnavailableOrMalformedCoreMetrics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{"unavailable CPU", func(r *Result) { r.Metrics["cpu_seconds"] = Metric{Unit: "seconds", Source: "test", Reason: "failed"} }},
		{"zero RSS", func(r *Result) { m := r.Metrics["rss_peak_bytes"]; m.Value = 0; r.Metrics["rss_peak_bytes"] = m }},
		{"negative disk", func(r *Result) {
			m := r.Metrics["disk_logical_bytes"]
			m.Value = -1
			r.Metrics["disk_logical_bytes"] = m
		}},
		{"NaN disk", func(r *Result) {
			m := r.Metrics["disk_allocated_bytes"]
			m.Value = math.NaN()
			r.Metrics["disk_allocated_bytes"] = m
		}},
		{"zero recovery", func(r *Result) { m := r.Metrics["recovery_seconds"]; m.Value = 0; r.Metrics["recovery_seconds"] = m }},
		{"zero CPU in decision run", func(r *Result) {
			r.Config.TimedOps = 2000
			m := r.Metrics["cpu_seconds"]
			m.Value = 0
			r.Metrics["cpu_seconds"] = m
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := validResult("treedb", "relaxed")
			tc.mutate(&r)
			if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "core metric") {
				t.Fatalf("got %v, want core metric failure", err)
			}
		})
	}
}

func TestResultRejectsSubstringAndContradictorySelectors(t *testing.T) {
	for _, badger := range []string{"notsyncwrites=false", "syncwrites=false; syncwrites=true"} {
		r := validResult("badger", "relaxed")
		r.Config.Badger = badger
		if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "invalid config") {
			t.Fatalf("badger=%q got %v, want invalid config", badger, err)
		}
	}
	r := validResult("treedb", "durable")
	r.Config.PostingStore += "; durability=relaxed"
	if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("got %v, want invalid config", err)
	}
}

func TestResultRejectsIncompleteRAMStorageAndEnvironmentContext(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Result)
	}{
		{"RAM", func(r *Result) { r.Context.TotalRAMBytes = 0 }},
		{"storage", func(r *Result) { r.Context.Storage.Model = "" }},
		{"environment", func(r *Result) { delete(r.Context.Environment, "TMPDIR") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validResult("badger", "relaxed")
			tc.mutate(&r)
			if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "reproduction") {
				t.Fatalf("got %v, want reproduction context failure", err)
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

func TestValidateSetRejectsCrossDurabilityChecksumAndCountMismatch(t *testing.T) {
	base := []Result{validResult("badger", "relaxed"), validResult("treedb", "relaxed"), validResult("badger", "durable"), validResult("treedb", "durable")}

	checksum := append([]Result(nil), base...)
	checksum[2].Validation.PostingChecksum = "durable-only"
	checksum[3].Validation.PostingChecksum = "durable-only"
	if err := ValidateSet(checksum, 1); err == nil || !strings.Contains(err.Error(), "parity mismatch") {
		t.Fatalf("checksum mismatch got %v", err)
	}

	counts := append([]Result(nil), base...)
	counts[2].Validation.NodeCount++
	counts[3].Validation.NodeCount++
	if err := ValidateSet(counts, 1); err == nil || !strings.Contains(err.Error(), "node-count parity mismatch") {
		t.Fatalf("count mismatch got %v", err)
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
