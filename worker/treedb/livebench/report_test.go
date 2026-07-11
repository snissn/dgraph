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
	for _, want := range []string{"## Relaxed durability", "## Durable durability", "Alpha CPU median", "disk allocated median", "write amp", "PROCEED", "Badger remains the production default", "live/badger-relaxed/result.json"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q", want)
		}
	}
}
