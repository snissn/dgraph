// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validSet(repeats int) []Result {
	set := make([]Result, 0, repeats*4)
	for _, class := range []string{"relaxed", "durable"} {
		for _, backend := range []string{"badger", "treedb"} {
			for repeat := 1; repeat <= repeats; repeat++ {
				r := validResult(backend, class)
				r.Repeat = repeat
				r.RunID = fmt.Sprintf("%s-%s-r%d", backend, class, repeat)
				set = append(set, r)
			}
		}
	}
	return set
}

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
			observed = "wal_on_relaxed_sync"
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
	pointCoverage := metrics[pointAppendCoverageMetric]
	pointCoverage.Value = 0
	metrics[pointAppendCoverageMetric] = pointCoverage
	if backend == "badger" {
		for _, name := range requiredTreeDBMetrics {
			metrics[name] = Metric{Unit: "count", Source: "TreeDB test diagnostic", Reason: "TreeDB-only diagnostic"}
		}
	}
	return Result{SchemaVersion: SchemaVersion, RunID: backend + "-" + class, Repeat: 1,
		Config:       Config{Backend: backend, DurabilityClass: class, PostingStore: posting, Badger: badger, DatasetNodes: 10, Concurrency: 1, WarmupOps: 1, TimedOps: 10, QueryMix: map[string]int{"read": 80, "write": 20}, Topology: "single-zero-single-alpha", Seed: 42},
		Context:      Context{DgraphSHA: "abc", GomapVersion: "v1", GoVersion: "go1", Host: "host", Kernel: "kernel", CPU: "cpu", TotalRAMBytes: 1024, Storage: StorageContext{Scope: "artifact_and_posting", Source: "/dev/test", Model: "test", SizeBytes: 2048, Filesystem: "ext4", Mountpoint: "/mnt"}, Environment: map[string]string{"GOWORK": "off", "TMPDIR": "/tmp", "GOMAXPROCS": "", "GOFLAGS": ""}, ExactCommand: []string{"bench"}, RawPath: "/raw"},
		SetupStarted: now, SetupFinished: now.Add(time.Second), TimedStarted: now.Add(2 * time.Second), TimedFinished: now.Add(3 * time.Second), Throughput: 10, LatencyMS: map[string]float64{"p50": 1, "p95": 2, "p99": 3}, Metrics: metrics,
		Validation: Validation{BackendObserved: backend, DurabilityObserved: observed, SchemaOK: true, PostingChecksum: "same", NodeCount: 12, RestartOK: true, UnsupportedOK: true}}
}

func TestExpectedObservedUsesCanonicalTreeDBProfileIntegrity(t *testing.T) {
	config := validResult("treedb", "relaxed").Config
	backend, durability, err := expectedObserved(config)
	if err != nil {
		t.Fatal(err)
	}
	if backend != "treedb" || durability != "wal_on_relaxed_sync" {
		t.Fatalf("observed contract=(%q, %q), want (treedb, wal_on_relaxed_sync)", backend, durability)
	}
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

func TestResultFailsClosedOnTreeDBDiagnosticAvailability(t *testing.T) {
	tree := validResult("treedb", "relaxed")
	metric := tree.Metrics["treedb_group_commit_commits"]
	metric.Available = false
	metric.Reason = "debug sample failed"
	tree.Metrics["treedb_group_commit_commits"] = metric
	if err := tree.Validate(); err == nil || !strings.Contains(err.Error(), "TreeDB diagnostic") {
		t.Fatalf("TreeDB unavailable diagnostic got %v", err)
	}

	badger := validResult("badger", "relaxed")
	badger.Metrics["treedb_group_commit_commits"] = Metric{Available: true, Value: 1, Unit: "count", Source: "fabricated"}
	if err := badger.Validate(); err == nil || !strings.Contains(err.Error(), "must be unavailable for Badger") {
		t.Fatalf("Badger fabricated TreeDB diagnostic got %v", err)
	}
}

func TestResultRequiresPointAppendCoverageDiagnostic(t *testing.T) {
	tree := validResult("treedb", "relaxed")
	delete(tree.Metrics, pointAppendCoverageMetric)
	if err := tree.Validate(); err == nil || !strings.Contains(err.Error(), pointAppendCoverageMetric) {
		t.Fatalf("missing point-append coverage diagnostic got %v", err)
	}
}

func TestResultAllowsLegacySchemaWithoutPointAppendCoverageDiagnostic(t *testing.T) {
	tree := validResult("treedb", "relaxed")
	tree.SchemaVersion = legacySchemaVersion
	delete(tree.Metrics, pointAppendCoverageMetric)
	if err := tree.Validate(); err != nil {
		t.Fatalf("legacy result rejected: %v", err)
	}

	delete(tree.Metrics, "treedb_group_commit_commits")
	if err := tree.Validate(); err == nil || !strings.Contains(err.Error(), "treedb_group_commit_commits") {
		t.Fatalf("legacy result accepted without non-legacy diagnostic: %v", err)
	}
}

func TestResultRejectsLogicalBytesWhenPointAppendsArePresent(t *testing.T) {
	tree := validResult("treedb", "relaxed")
	pointAppends := tree.Metrics[pointAppendCoverageMetric]
	pointAppends.Value = 1
	tree.Metrics[pointAppendCoverageMetric] = pointAppends
	if err := tree.Validate(); err == nil || !strings.Contains(err.Error(), "logical write bytes must be unavailable") {
		t.Fatalf("point appends with available logical bytes got %v", err)
	}

	logicalBytes := tree.Metrics["write_bytes"]
	logicalBytes.Available = false
	logicalBytes.Reason = "direct-point appends bypass public-batch logical bytes"
	tree.Metrics["write_bytes"] = logicalBytes
	if err := tree.Validate(); err != nil {
		t.Fatalf("fail-closed point-append result rejected: %v", err)
	}
}

func TestMetricSerializesMeasuredZero(t *testing.T) {
	b, err := json.Marshal(Metric{Available: true, Value: 0, Unit: "count", Source: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"value":0`) {
		t.Fatalf("measured zero omitted from metric: %s", b)
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

func TestValidateSetRejectsDirtyOrMismatchedReproductionContext(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func(*Result)
	}{
		{"dirty", "dirty reproduction context", func(r *Result) { r.Context.Dirty = true }},
		{"Dgraph revision", "DgraphSHA", func(r *Result) { r.Context.DgraphSHA = "different" }},
		{"gomap revision", "GomapVersion", func(r *Result) { r.Context.GomapVersion = "different" }},
		{"Go version", "GoVersion", func(r *Result) { r.Context.GoVersion = "different" }},
		{"host", "Host", func(r *Result) { r.Context.Host = "different" }},
		{"kernel", "Kernel", func(r *Result) { r.Context.Kernel = "different" }},
		{"CPU", "CPU", func(r *Result) { r.Context.CPU = "different" }},
		{"RAM", "TotalRAMBytes", func(r *Result) { r.Context.TotalRAMBytes++ }},
		{"storage", "storage differs", func(r *Result) { r.Context.Storage.Source = "/dev/different" }},
		{"environment", "environment differs", func(r *Result) { r.Context.Environment["TMPDIR"] = "/different" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			set := validSet(1)
			tc.mutate(&set[1])
			if err := ValidateSet(set, 1); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want context failure containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateSetAllowsPerRunReproductionDetailsToVary(t *testing.T) {
	set := validSet(1)
	set[1].Context.ExactCommand = []string{"different", "command"}
	set[1].Context.RawPath = "/different/result.json"
	set[1].Context.Profiles = []string{"/different/cpu.pprof"}
	set[1].Context.Contaminants = []string{}
	if err := ValidateSet(set, 1); err != nil {
		t.Fatalf("per-run reproduction details were equality-gated: %v", err)
	}
}

func TestValidateSetRejectsInvalidRepeatCoverageAndDuplicateRunID(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func([]Result)
	}{
		{"repeat above range", "outside 1..1", func(set []Result) { set[0].Repeat = 2 }},
		{"duplicate repeat", "duplicate repeat ordinal", func(set []Result) { set[1].Repeat = 1 }},
		{"duplicate run ID", "duplicate run_id", func(set []Result) { set[1].RunID = set[0].RunID }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repeats := 2
			if tc.name == "repeat above range" {
				repeats = 1
			}
			set := validSet(repeats)
			tc.mutate(set)
			if err := ValidateSet(set, repeats); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want repeat/run ID failure containing %q", err, tc.want)
			}
		})
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
