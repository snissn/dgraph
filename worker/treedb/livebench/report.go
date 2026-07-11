// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

func LoadResults(paths []string) ([]Result, error) {
	results := make([]Result, 0, len(paths))
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		results = append(results, r)
	}
	return results, nil
}

type distribution struct{ median, min, max, cv float64 }

func summarize(xs []float64) distribution {
	sort.Float64s(xs)
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	variance := 0.0
	for _, x := range xs {
		variance += (x - mean) * (x - mean)
	}
	return distribution{median: xs[len(xs)/2], min: xs[0], max: xs[len(xs)-1], cv: math.Sqrt(variance/float64(len(xs))) / mean * 100}
}

// RenderReport validates the complete matrix and keeps relaxed and durable
// comparisons in separate headline sections.
func RenderReport(results []Result, repeats int) (string, error) {
	if err := ValidateSet(results, repeats); err != nil {
		return "", err
	}
	by := map[string][]Result{}
	for _, r := range results {
		by[r.Config.Backend+"/"+r.Config.DurabilityClass] = append(by[r.Config.Backend+"/"+r.Config.DurabilityClass], r)
	}
	var b strings.Builder
	b.WriteString("# Dgraph Badger vs TreeDB live durability A/B\n\n")
	fmt.Fprintf(&b, "- Repeats per cell: %d\n- Workload fingerprint: `%s`\n- Logical parity and restart gate: **PASS**\n- Comparisons never mix durability classes.\n\n", repeats, results[0].Config.Fingerprint())
	context := results[0].Context
	fmt.Fprintf(&b, "- Host: `%s`; CPU: `%s`; RAM: `%.1f GiB`\n- Artifact/posting storage: `%s` (`%s`, %.1f GB), `%s` mounted at `%s`\n- Environment: `GOWORK=%s TMPDIR=%s GOMAXPROCS=%s GOFLAGS=%s`\n\n",
		context.Host, context.CPU, float64(context.TotalRAMBytes)/(1024*1024*1024), context.Storage.Source,
		context.Storage.Model, float64(context.Storage.SizeBytes)/1e9, context.Storage.Filesystem,
		context.Storage.Mountpoint, context.Environment["GOWORK"], context.Environment["TMPDIR"],
		context.Environment["GOMAXPROCS"], context.Environment["GOFLAGS"])
	proceed := true
	for _, class := range []string{"relaxed", "durable"} {
		title := class[:1]
		title = strings.ToUpper(title) + class[1:]
		fmt.Fprintf(&b, "## %s durability\n\n", title)
		b.WriteString("| Backend | Throughput median (ops/s) | min-max | CV | p50 median (ms) | p95 median (ms) | p99 median (ms) | restart median (s) |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		med := map[string]float64{}
		for _, backend := range []string{"badger", "treedb"} {
			rs := by[backend+"/"+class]
			through, p50, p95, p99, recovery := vectors(rs)
			d := summarize(through)
			med[backend] = d.median
			fmt.Fprintf(&b, "| %s | %.2f | %.2f-%.2f | %.2f%% | %.3f | %.3f | %.3f | %.3f |\n", backend, d.median, d.min, d.max, d.cv, summarize(p50).median, summarize(p95).median, summarize(p99).median, summarize(recovery).median)
		}
		b.WriteString("\n| Backend | Alpha CPU median (s) | RSS/HWM median (MiB) | disk logical median (MiB) | disk allocated median (MiB) | logical write median (KiB) | write amp | GC cycles | flushes | checkpoints |\n| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |\n")
		for _, backend := range []string{"badger", "treedb"} {
			rs := by[backend+"/"+class]
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", backend,
				metricSummary(rs, "cpu_seconds", 1), metricSummaryScaled(rs, "rss_peak_bytes", 1024*1024, 1),
				metricSummaryScaled(rs, "disk_logical_bytes", 1024*1024, 1), metricSummaryScaled(rs, "disk_allocated_bytes", 1024*1024, 1),
				metricSummaryScaled(rs, "write_bytes", 1024, 1), metricSummary(rs, "write_amplification", 2),
				metricSummary(rs, "gc_cycles", 1), metricSummary(rs, "flushes", 1), metricSummary(rs, "checkpoints", 1))
		}
		delta := (med["treedb"]/med["badger"] - 1) * 100
		if delta < -3 {
			proceed = false
		}
		fmt.Fprintf(&b, "\nTreeDB throughput delta versus durability-matched Badger: **%+.2f%%** (gate: no worse than -3%%).\n\n", delta)
	}
	decision := "ADVANCE experimental integration to the next phase; keep Badger as the production default"
	if !proceed {
		decision = "STOP advancement/integration at this phase; keep Badger as the production default"
	}
	fmt.Fprintf(&b, "## Decision\n\n**%s.** Logical parity, schema, posting, unsupported-feature, and recovery gates passed. The performance decision applies only to this benchmark-minimal topology and workload.\n\n", decision)
	b.WriteString("## Profile attribution\n\n")
	b.WriteString("- Relaxed TreeDB: [`profiles/treedb-relaxed.pprof`](profiles/treedb-relaxed.pprof) and [`profiles/treedb-relaxed-top.txt`](profiles/treedb-relaxed-top.txt). The profile is syscall/allocation-heavy, but without matched substrate-only and adapter/runtime profiles it cannot attribute the split between gomap itself and Dgraph integration overhead.\n")
	b.WriteString("- Durable TreeDB: [`profiles/treedb-durable.pprof`](profiles/treedb-durable.pprof) and [`profiles/treedb-durable-top.txt`](profiles/treedb-durable-top.txt). The five-second wall-clock profile collected little CPU, which is consistent with I/O wait; it is not causal proof of the durable throughput gap.\n\n")
	b.WriteString("## Raw artifacts and reproduction\n\nReproduce from the recorded Dgraph SHA with `TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir NEW_DIR`. Paths below are relative to the artifact root; each JSON retains its exact original absolute command and raw path.\n\n")
	for _, r := range results {
		fmt.Fprintf(&b, "- `%s`: `live/%s/result.json`; Dgraph `%s`; gomap `%s`; dirty `%t`\n", r.RunID, r.RunID, r.Context.DgraphSHA, r.Context.GomapVersion, r.Context.Dirty)
	}
	b.WriteString("\nExcluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha `VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large logical size with small allocated size comes from sparse preallocated files, so logical and allocated bytes must be read together. TreeDB logical write bytes use its public-batch counter, but write amplification remains unavailable because an equivalent physical-byte counter is not exposed. Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are exposed; vlog writes are not relabeled as flushes.\n")
	return b.String(), nil
}

func metricSummary(results []Result, name string, precision int) string {
	return metricSummaryScaled(results, name, 1, precision)
}
func metricSummaryScaled(results []Result, name string, scale float64, precision int) string {
	values := make([]float64, 0, len(results))
	for _, r := range results {
		m := r.Metrics[name]
		if m.Available {
			values = append(values, m.Value/scale)
		}
	}
	if len(values) == 0 {
		return fmt.Sprintf("unavailable (0/%d)", len(results))
	}
	return fmt.Sprintf("%.*f (%d/%d)", precision, summarize(values).median, len(values), len(results))
}

func vectors(rs []Result) (through, p50, p95, p99, recovery []float64) {
	for _, r := range rs {
		through = append(through, r.Throughput)
		p50 = append(p50, r.LatencyMS["p50"])
		p95 = append(p95, r.LatencyMS["p95"])
		p99 = append(p99, r.LatencyMS["p99"])
		recovery = append(recovery, r.Metrics["recovery_seconds"].Value)
	}
	return
}
