// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
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

type ProfileArtifact struct {
	PPROF string
	Top   string
}

type ProfileArtifacts struct {
	Relaxed  ProfileArtifact
	Durable  ProfileArtifact
	verified bool
}

func (p ProfileArtifacts) Validate() error {
	if !p.verified {
		return errors.New("profile artifacts were not verified on disk")
	}
	for class, artifact := range map[string]ProfileArtifact{"relaxed": p.Relaxed, "durable": p.Durable} {
		if artifact.PPROF == "" || artifact.Top == "" {
			return fmt.Errorf("%s profile artifact paths are incomplete", class)
		}
	}
	return nil
}

// DiscoverProfileArtifacts verifies the separately generated profile files and
// returns report-relative links. Supplying a profile directory is an explicit
// contract: partial or empty profile output is an error, never a report claim.
func DiscoverProfileArtifacts(dir string) (ProfileArtifacts, error) {
	profiles := ProfileArtifacts{
		Relaxed: ProfileArtifact{PPROF: "profiles/treedb-relaxed.pprof", Top: "profiles/treedb-relaxed-top.txt"},
		Durable: ProfileArtifact{PPROF: "profiles/treedb-durable.pprof", Top: "profiles/treedb-durable-top.txt"},
	}
	for _, name := range []string{"treedb-relaxed.pprof", "treedb-relaxed-top.txt", "treedb-durable.pprof", "treedb-durable-top.txt"} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			return ProfileArtifacts{}, fmt.Errorf("profile artifact %s: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return ProfileArtifacts{}, fmt.Errorf("profile artifact %s is empty or not regular", path)
		}
	}
	profiles.verified = true
	return profiles, nil
}

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
	middle := len(xs) / 2
	median := xs[middle]
	if len(xs)%2 == 0 {
		median = (xs[middle-1] + xs[middle]) / 2
	}
	return distribution{median: median, min: xs[0], max: xs[len(xs)-1], cv: math.Sqrt(variance/float64(len(xs))) / mean * 100}
}

// RenderReport validates the complete matrix and keeps relaxed and durable
// comparisons in separate headline sections.
func RenderReport(results []Result, repeats int) (string, error) {
	return renderReport(results, repeats, nil)
}

func RenderReportWithProfiles(results []Result, repeats int, profiles ProfileArtifacts) (string, error) {
	if err := profiles.Validate(); err != nil {
		return "", err
	}
	return renderReport(results, repeats, &profiles)
}

func renderReport(results []Result, repeats int, profiles *ProfileArtifacts) (string, error) {
	if err := ValidateSet(results, repeats); err != nil {
		return "", err
	}
	legacyPointCoverageGap := hasLegacyPointAppendCoverageGap(results)
	results = normalizeLegacyPointAppendCoverage(results)
	sort.SliceStable(results, func(i, j int) bool {
		left, right := results[i], results[j]
		leftKey := reportOrderKey(left)
		rightKey := reportOrderKey(right)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return left.Repeat < right.Repeat
	})
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
		if class == "relaxed" && legacyPointCoverageGap {
			b.WriteString("\nThe public-batch call counters below are valid measured zeros. The schema-v3 rows predate the point-append coverage diagnostic, so they cannot establish whether direct-point appends bypassed the public-batch logical-byte counter. As a conservative compatibility erratum, this report therefore treats the raw relaxed logical-byte zero as unavailable rather than as evidence of zero logical writes. This is a later counter-coverage inference, not a fact independently established by the retained raw rows. The engine flush median cannot establish publications per application write.\n")
		}
		treeResults := by["treedb/"+class]
		fmt.Fprintf(&b, "\nTreeDB durability diagnostics (timed-phase deltas unless marked high-water):\n\nEach value below is an independent per-metric median across the %d accepted repeats. The row is not one observed repeat.\n\n", repeats)
		b.WriteString("| public batch writes | public batch durable writes | point appends | group commits / groups | participants | group syncs | max group size (high-water) | command-WAL file syncs | value-log logical syncs | value-log file syncs |\n| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		fmt.Fprintf(&b, "| %s | %s | %s | %s / %s | %s | %s | %s | %s | %s | %s |\n",
			metricSummary(treeResults, "treedb_public_batch_write_calls", 0),
			metricSummary(treeResults, "treedb_public_batch_write_sync_calls", 0),
			metricSummary(treeResults, "treedb_command_wal_append_point_calls", 0),
			metricSummary(treeResults, "treedb_group_commit_commits", 0),
			metricSummary(treeResults, "treedb_group_commit_groups", 0),
			metricSummary(treeResults, "treedb_group_commit_participants", 0),
			metricSummary(treeResults, "treedb_group_commit_syncs", 0),
			metricSummary(treeResults, "treedb_group_commit_group_size_max", 0),
			metricSummary(treeResults, "treedb_command_wal_file_syncs", 0),
			metricSummary(treeResults, "treedb_value_log_syncs", 0),
			metricSummary(treeResults, "treedb_value_log_file_syncs", 0))
		b.WriteString("\n| point-successor calls | point sources | sources/call | source high-water median | iterator snapshot rotations | leaf-log segment rotations |\n| ---: | ---: | ---: | ---: | ---: | ---: |\n")
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n",
			metricSummary(treeResults, "treedb_point_successor_calls", 0),
			metricSummary(treeResults, "treedb_point_successor_sources", 0),
			metricRatioSummary(treeResults, "treedb_point_successor_sources", "treedb_point_successor_calls", 3),
			metricSummary(treeResults, "treedb_point_successor_sources_max", 0),
			metricSummary(treeResults, "treedb_iterator_snapshot_rotations", 0),
			metricSummary(treeResults, "treedb_leaf_log_segment_rotations", 0))
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
	if profiles != nil {
		b.WriteString("## Profile artifacts\n\n")
		b.WriteString("Separate TreeDB profile runs were collected after the decision matrix; their throughput is diagnostic and is not part of the A/B decision.\n\n")
		fmt.Fprintf(&b, "- Relaxed TreeDB: [`%s`](%s) and [`%s`](%s).\n", profiles.Relaxed.PPROF, profiles.Relaxed.PPROF, profiles.Relaxed.Top, profiles.Relaxed.Top)
		fmt.Fprintf(&b, "- Durable TreeDB: [`%s`](%s) and [`%s`](%s).\n\n", profiles.Durable.PPROF, profiles.Durable.PPROF, profiles.Durable.Top, profiles.Durable.Top)
		b.WriteString("These artifacts do not by themselves attribute cost between gomap and Dgraph integration, establish I/O wait, or prove a causal explanation for either throughput delta.\n\n")
	}
	b.WriteString("## Raw artifacts and reproduction\n\nReproduce from the recorded Dgraph SHA with `TMPDIR=/mnt/fast4tb/tmp GOWORK=off worker/treedb/run_durability_ab.sh --artifact-dir /absolute/path/outside/repository/NEW_DIR`. Paths below are relative to the artifact root; each JSON retains its exact original absolute command and raw path.\n\n")
	for _, r := range results {
		fmt.Fprintf(&b, "- `%s`: `live/%s/result.json`; Dgraph `%s`; gomap `%s`; dirty `%t`\n", r.RunID, r.RunID, r.Context.DgraphSHA, r.Context.GomapVersion, r.Context.Dirty)
	}
	if legacyPointCoverageGap {
		b.WriteString("\nExcluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha `VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large logical size with small allocated size comes from sparse preallocated files, so logical and allocated bytes must be read together. TreeDB durable logical write bytes use the durable route's public-batch set-byte counter. The relaxed public-batch call counters remain valid measured zeros. The schema-v3 relaxed rows lack the point-append coverage counter, so their raw logical-byte zero is conservatively normalized above as unavailable and explicitly treated as a later counter-coverage inference. Write amplification remains unavailable because an equivalent physical-byte counter is not exposed. Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are exposed; vlog writes are not relabeled as flushes.\n")
	} else {
		b.WriteString("\nExcluded runs are rejected by aggregation. Alpha CPU is a timed-phase `/proc` delta; RSS is Alpha `VmHWM` and therefore includes setup. Disk metrics cover the postings directory. Badger's large logical size with small allocated size comes from sparse preallocated files, so logical and allocated bytes must be read together. TreeDB logical write bytes use its public-batch counter only when the point-append counter proves no direct-point writes occurred; otherwise they fail closed as unavailable. Write amplification remains unavailable because an equivalent physical-byte counter is not exposed. Badger flush and checkpoint counts are unavailable because no equivalent semantic counters are exposed; vlog writes are not relabeled as flushes.\n")
	}
	return b.String(), nil
}

func reportOrderKey(r Result) int {
	class, backend := 1, 1
	if r.Config.DurabilityClass == "relaxed" {
		class = 0
	}
	if r.Config.Backend == "badger" {
		backend = 0
	}
	return class*2 + backend
}

func hasLegacyPointAppendCoverageGap(results []Result) bool {
	for _, r := range results {
		coverage, ok := r.Metrics[pointAppendCoverageMetric]
		if r.SchemaVersion == legacySchemaVersion && r.Config.Backend == "treedb" && r.Config.DurabilityClass == "relaxed" && (!ok || !coverage.Available) {
			return true
		}
	}
	return false
}

// normalizeLegacyPointAppendCoverage keeps frozen schema-v3 matrices reportable without allowing
// their public-batch logical-byte counter to masquerade as total logical-write coverage. Schema v4
// requires the point-append diagnostic and therefore never takes this compatibility path.
func normalizeLegacyPointAppendCoverage(results []Result) []Result {
	normalized := append([]Result(nil), results...)
	for i := range normalized {
		r := &normalized[i]
		if r.SchemaVersion != legacySchemaVersion || r.Config.Backend != "treedb" || r.Config.DurabilityClass != "relaxed" {
			continue
		}
		coverage, ok := r.Metrics[pointAppendCoverageMetric]
		if ok && coverage.Available {
			continue
		}
		r.Metrics = maps.Clone(r.Metrics)
		logical := r.Metrics["write_bytes"]
		logical.Available = false
		logical.Reason = "legacy schema lacks the point-append counter needed for total logical-write coverage"
		r.Metrics["write_bytes"] = logical
	}
	return normalized
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

func metricRatioSummary(results []Result, numerator, denominator string, precision int) string {
	values := make([]float64, 0, len(results))
	for _, r := range results {
		n, nok := r.Metrics[numerator]
		d, dok := r.Metrics[denominator]
		if nok && dok && n.Available && d.Available && d.Value > 0 {
			values = append(values, n.Value/d.Value)
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
