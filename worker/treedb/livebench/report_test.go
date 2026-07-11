// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"strings"
	"testing"
)

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
	for _, want := range []string{"## Relaxed durability", "## Durable durability", "Alpha CPU median", "disk allocated median", "write amp", "ADVANCE experimental integration", "keep Badger as the production default", "live/badger-relaxed/result.json", "## Profile attribution", "profiles/treedb-relaxed.pprof", "profiles/treedb-durable-top.txt", "cannot attribute", "consistent with I/O wait", "sparse preallocated files", "RSS is Alpha `VmHWM`", "GOWORK=off"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q", want)
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
