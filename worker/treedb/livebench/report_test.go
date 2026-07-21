// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscoverProfileArtifactsRequiresCompleteNonEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := DiscoverProfileArtifacts(dir); err == nil {
		t.Fatal("missing profiles unexpectedly accepted")
	}
	for _, name := range []string{"treedb-relaxed.pprof", "treedb-relaxed-top.txt", "treedb-durable.pprof", "treedb-durable-top.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("evidence"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	profiles, err := DiscoverProfileArtifacts(dir)
	if err != nil {
		t.Fatal(err)
	}
	if profiles.Relaxed.PPROF != "profiles/treedb-relaxed.pprof" || profiles.Durable.Top != "profiles/treedb-durable-top.txt" {
		t.Fatalf("unexpected report links: %+v", profiles)
	}
}

func TestSummarizeAveragesEvenMiddleValues(t *testing.T) {
	if got := summarize([]float64{100, 1, 10, 20}).median; got != 15 {
		t.Fatalf("median=%v, want 15", got)
	}
}

func TestRenderReportSeparatesDurabilityClassesAndMakesDecision(t *testing.T) {
	set := []Result{validResult("badger", "relaxed"), validResult("treedb", "relaxed"), validResult("badger", "durable"), validResult("treedb", "durable")}
	for i := range set {
		set[i].Context.RawPath = "/raw/" + set[i].RunID
		set[i].Throughput = 100
	}
	set[1].Throughput = 98
	report, err := RenderReport(set, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"## Relaxed durability", "## Durable durability", "Alpha CPU median", "disk allocated median", "write amp", "ADVANCE experimental integration", "keep Badger as the production default", "live/badger-relaxed/result.json", "sparse preallocated files", "RSS is Alpha `VmHWM`", "GOWORK=off"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q", want)
		}
	}
	for _, absent := range []string{"## Profile artifacts", "profiles/treedb-relaxed.pprof", "syscall/allocation-heavy", "consistent with I/O wait"} {
		if strings.Contains(report, absent) {
			t.Fatalf("generic report unexpectedly contains %q", absent)
		}
	}
}

func TestRenderReportIncludesTreeDBDurabilityAndPointSourceDiagnostics(t *testing.T) {
	set := validSet(1)
	for i := range set {
		if set[i].Config.Backend != "treedb" {
			continue
		}
		set[i].Metrics["treedb_group_commit_commits"] = Metric{Available: true, Value: 8, Unit: "count", Source: "test"}
		set[i].Metrics["treedb_group_commit_groups"] = Metric{Available: true, Value: 4, Unit: "count", Source: "test"}
		set[i].Metrics["treedb_point_successor_sources"] = Metric{Available: true, Value: 6, Unit: "count", Source: "test"}
		set[i].Metrics["treedb_point_successor_calls"] = Metric{Available: true, Value: 3, Unit: "count", Source: "test"}
	}
	report, err := RenderReport(set, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"TreeDB durability diagnostics", "group commits / groups", "point sources", "sources/call", "2.000 (1/1)"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestRenderReportIncludesOnlySuppliedProfileArtifactsWithoutFindings(t *testing.T) {
	set := []Result{validResult("badger", "relaxed"), validResult("treedb", "relaxed"), validResult("badger", "durable"), validResult("treedb", "durable")}
	dir := t.TempDir()
	for _, name := range []string{"treedb-relaxed.pprof", "treedb-relaxed-top.txt", "treedb-durable.pprof", "treedb-durable-top.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("evidence"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	profiles, err := DiscoverProfileArtifacts(dir)
	if err != nil {
		t.Fatal(err)
	}
	report, err := RenderReportWithProfiles(set, 1, profiles)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"## Profile artifacts", profiles.Relaxed.PPROF, profiles.Relaxed.Top, profiles.Durable.PPROF, profiles.Durable.Top, "do not by themselves attribute"} {
		if !strings.Contains(report, want) {
			t.Fatalf("profile report missing %q", want)
		}
	}
	for _, absent := range []string{"syscall/allocation-heavy", "consistent with I/O wait"} {
		if strings.Contains(report, absent) {
			t.Fatalf("generic profile report invented finding %q", absent)
		}
	}
}

func TestRenderReportUsesExplicitStopOutcome(t *testing.T) {
	set := []Result{validResult("badger", "relaxed"), validResult("treedb", "relaxed"), validResult("badger", "durable"), validResult("treedb", "durable")}
	for i := range set {
		set[i].Throughput = 100
	}
	set[1].Throughput = 90
	report, err := RenderReport(set, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(report, "STOP advancement/integration at this phase; keep Badger as the production default") {
		t.Fatalf("missing explicit stop outcome:\n%s", report)
	}
}
