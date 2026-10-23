// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func validSet(t *testing.T, repeats int) []Result {
	set := make([]Result, 0, repeats*4)
	for _, class := range []string{"relaxed", "durable"} {
		for _, backend := range []string{"badger", "treedb"} {
			for repeat := 1; repeat <= repeats; repeat++ {
				r := validResult(t, backend, class)
				r.Repeat = repeat
				r.RunID = fmt.Sprintf("%s-%s-r%d", backend, class, repeat)
				set = append(set, r)
			}
		}
	}
	return set
}

func validResult(t *testing.T, backend, class string) Result {
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
	r := Result{SchemaVersion: SchemaVersion, RunID: backend + "-" + class, Repeat: 1,
		Config:       Config{SurvivalMode: "process", Backend: backend, DurabilityClass: class, PostingStore: posting, Badger: badger, DatasetNodes: 10, Concurrency: 1, WarmupOps: 1, TimedOps: 10, QueryMix: map[string]int{"read": 80, "write": 20}, Topology: "single-zero-single-alpha", Seed: 42},
		Context:      Context{BinarySHA256: strings.Repeat("a", 64), BinaryBuildInfo: "test build identity", DgraphSHA: "abc", GomapVersion: "v1", GoVersion: "go1", Host: "host", Kernel: "kernel", CPU: "cpu", TotalRAMBytes: 1024, Storage: StorageContext{Scope: "artifact_and_posting", Source: "/dev/test", Model: "test", SizeBytes: 2048, Filesystem: "ext4", Mountpoint: "/mnt"}, Environment: map[string]string{"GOWORK": "off", "TMPDIR": "/tmp", "GOMAXPROCS": "", "GOFLAGS": ""}, ExactCommand: []string{"bench"}, RawPath: "/raw"},
		SetupStarted: now, SetupFinished: now.Add(time.Second), TimedStarted: now.Add(2 * time.Second), TimedFinished: now.Add(3 * time.Second), Throughput: 10, LatencyMS: map[string]float64{"p50": 1, "p95": 2, "p99": 3}, Metrics: metrics,
		Validation: Validation{BackendObserved: backend, DurabilityObserved: observed, SchemaOK: true, PostingChecksum: "same", NodeCount: 12, RestartOK: true, UnsupportedOK: true}}
	if backend == "treedb" {
		r.Config.TreeDBPolicy = "store_wal"
	}
	r.Validation.Runtime = runtimeFixture(r.Config)
	writeRuntimeFixture(t, &r)
	return r

}

func TestExpectedObservedUsesCanonicalTreeDBProfileIntegrity(t *testing.T) {
	config := validResult(t, "treedb", "relaxed").Config
	backend, durability, err := expectedObserved(config)
	if err != nil {
		t.Fatal(err)
	}
	if backend != "treedb" || durability != "wal_on_relaxed_sync" {
		t.Fatalf("observed contract=(%q, %q), want (treedb, wal_on_relaxed_sync)", backend, durability)
	}
}

func TestAlignedRecoveryProtocolRejectsMissingOrReversedReadiness(t *testing.T) {
	for _, bad := range []string{"", "missing read", "missing write", "before health", "before read", "nan", "unknown"} {
		t.Run(bad, func(t *testing.T) {
			r := validResult(t, "badger", "durable")
			legacyFingerprint := r.Config.Fingerprint()
			r.Config.RecoveryProbe = RecoveryReadCommitRead
			r.Metrics["recovery_read_seconds"] = Metric{Available: true, Value: 2, Unit: "seconds", Source: "validated read"}
			r.Metrics["recovery_read_write_seconds"] = Metric{Available: true, Value: 3, Unit: "seconds", Source: "committed write then read"}
			switch bad {
			case "missing read":
				delete(r.Metrics, "recovery_read_seconds")
			case "missing write":
				delete(r.Metrics, "recovery_read_write_seconds")
			case "before health":
				m := r.Metrics["recovery_read_seconds"]
				m.Value = 0.5
				r.Metrics["recovery_read_seconds"] = m
			case "before read":
				m := r.Metrics["recovery_read_write_seconds"]
				m.Value = 1.5
				r.Metrics["recovery_read_write_seconds"] = m
			case "nan":
				m := r.Metrics["recovery_read_seconds"]
				m.Value = math.NaN()
				r.Metrics["recovery_read_seconds"] = m
			case "unknown":
				r.Config.RecoveryProbe = "unknown"
			}
			if legacyFingerprint == r.Config.Fingerprint() {
				t.Fatal("new recovery protocol must not pool with health-only results")
			}
			if err := r.Validate(); (err == nil) != (bad == "") {
				t.Fatalf("invalid result disposition for %q: %v", bad, err)
			}
		})
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
			r := validResult(t, "treedb", "relaxed")
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
			r := validResult(t, "treedb", "relaxed")
			tc.mutate(&r)
			if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "core metric") {
				t.Fatalf("got %v, want core metric failure", err)
			}
		})
	}
}

func TestResultFailsClosedOnTreeDBDiagnosticAvailability(t *testing.T) {
	tree := validResult(t, "treedb", "relaxed")
	metric := tree.Metrics["treedb_group_commit_commits"]
	metric.Available = false
	metric.Reason = "debug sample failed"
	tree.Metrics["treedb_group_commit_commits"] = metric
	if err := tree.Validate(); err == nil || !strings.Contains(err.Error(), "TreeDB diagnostic") {
		t.Fatalf("TreeDB unavailable diagnostic got %v", err)
	}

	badger := validResult(t, "badger", "relaxed")
	badger.Metrics["treedb_group_commit_commits"] = Metric{Available: true, Value: 1, Unit: "count", Source: "fabricated"}
	if err := badger.Validate(); err == nil || !strings.Contains(err.Error(), "must be unavailable for Badger") {
		t.Fatalf("Badger fabricated TreeDB diagnostic got %v", err)
	}
}

func TestResultRequiresPointAppendCoverageDiagnostic(t *testing.T) {
	tree := validResult(t, "treedb", "relaxed")
	delete(tree.Metrics, pointAppendCoverageMetric)
	if err := tree.Validate(); err == nil || !strings.Contains(err.Error(), pointAppendCoverageMetric) {
		t.Fatalf("missing point-append coverage diagnostic got %v", err)
	}
}

func TestResultAllowsLegacySchemaWithoutPointAppendCoverageDiagnostic(t *testing.T) {
	tree := validResult(t, "treedb", "relaxed")
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
	tree := validResult(t, "treedb", "relaxed")
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
		r := validResult(t, "badger", "relaxed")
		r.Config.Badger = badger
		if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "invalid config") {
			t.Fatalf("badger=%q got %v, want invalid config", badger, err)
		}
	}
	r := validResult(t, "treedb", "durable")
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
			r := validResult(t, "badger", "relaxed")
			tc.mutate(&r)
			if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "reproduction") {
				t.Fatalf("got %v, want reproduction context failure", err)
			}
		})
	}
}

func TestValidateSetRejectsIncompleteAndLogicalMismatch(t *testing.T) {
	if err := ValidateSet([]Result{validResult(t, "badger", "relaxed")}, 1); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("got %v", err)
	}
	set := []Result{validResult(t, "badger", "relaxed"), validResult(t, "treedb", "relaxed"), validResult(t, "badger", "durable"), validResult(t, "treedb", "durable")}
	set[1].Validation.PostingChecksum = "different"
	if err := ValidateSet(set, 1); err == nil || !strings.Contains(err.Error(), "parity mismatch") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateSetRejectsCrossDurabilityChecksumAndCountMismatch(t *testing.T) {
	base := []Result{validResult(t, "badger", "relaxed"), validResult(t, "treedb", "relaxed"), validResult(t, "badger", "durable"), validResult(t, "treedb", "durable")}

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
			set := validSet(t, 1)
			tc.mutate(&set[1])
			if err := ValidateSet(set, 1); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want context failure containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateSetAllowsPerRunReproductionDetailsToVary(t *testing.T) {
	set := validSet(t, 1)
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
			set := validSet(t, repeats)
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

func TestValidateSetRejectsEndpointDiagnosticEvenWithOnlyCPUProfile(t *testing.T) {
	set := validSet(t, 1)
	set[0].Context.Profiles = []string{"cpu.pprof"}
	if err := ValidateSet(set, 1); err != nil {
		t.Fatalf("ordinary profile changed matrix acceptance: %v", err)
	}
	set[0].Context.Excluded = true
	set[0].Context.Contaminants = []string{"endpoint diagnostic observations and passive wait; excluded from performance acceptance"}
	set[0].Context.ExclusionReason = set[0].Context.Contaminants[0]
	if err := set[0].Validate(); err != nil {
		t.Fatalf("diagnostic raw result is invalid: %v", err)
	}
	if err := ValidateSet(set, 1); err == nil || !strings.Contains(err.Error(), "is excluded: endpoint diagnostic") {
		t.Fatalf("CPU-only endpoint result admitted to matrix: %v", err)
	}
}

func runtimeFixture(c Config) []RuntimeIdentity {
	var result []RuntimeIdentity
	for _, phase := range []string{"startup", "after_alpha_restart"} {
		for _, role := range []string{"zero", "alpha"} {
			hardSync := c.SurvivalMode == "filesystem"
			r := RuntimeIdentity{Process: role, Phase: phase, PID: 1, Command: []string{"dgraph", role, "--survive=" + c.SurvivalMode}, SurvivalMode: c.SurvivalMode, HardSync: hardSync, ConfigWitness: "x.WorkerConfig: {HardSync:" + strconv.FormatBool(hardSync) + "}", ConfigLogSHA256: strings.Repeat("a", 64)}
			if role == "alpha" {
				r.Backend, r.Durability, _ = expectedObserved(c)
				if c.Backend == "treedb" {
					r.Memtable = "cow_ordered"
				}
				r.StoreWitnessSHA256 = strings.Repeat("b", 64)
			}
			result = append(result, r)
		}
	}
	return result
}

func writeRuntimeFixture(t *testing.T, r *Result) {
	t.Helper()
	dir := t.TempDir()
	for i := range r.Validation.Runtime {
		identity := &r.Validation.Runtime[i]
		prefix := filepath.Join(dir, "runtime-"+identity.Phase+"-"+identity.Process)
		write := func(suffix, body string) string {
			t.Helper()
			if err := os.WriteFile(prefix+suffix, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256([]byte(body))
			return hex.EncodeToString(sum[:])
		}
		identity.ConfigLogSHA256 = write(".config.log", identity.ConfigWitness+"\n")
		if identity.Process == "alpha" {
			identity.StoreWitnessSHA256 = write(".store.txt", fmt.Sprintf("status=map[backend:%s profile:%s]\nstats=map[treedb.cache.memtable_mode:%s]\n", identity.Backend, identity.Durability, identity.Memtable))
		}
	}
	if err := r.VerifyRuntimeArtifacts(dir); err != nil {
		t.Fatal(err)
	}
}
