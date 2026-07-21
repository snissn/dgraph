// SPDX-License-Identifier: Apache-2.0

// Package livebench defines the immutable result contract for the live
// single-Zero/single-Alpha posting-store benchmark.
package livebench

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 3

type Config struct {
	Backend         string         `json:"backend"`
	DurabilityClass string         `json:"durability_class"`
	PostingStore    string         `json:"posting_store"`
	Badger          string         `json:"badger"`
	DatasetNodes    int            `json:"dataset_nodes"`
	Concurrency     int            `json:"concurrency"`
	WarmupOps       int            `json:"warmup_ops"`
	TimedOps        int            `json:"timed_ops"`
	QueryMix        map[string]int `json:"query_mix_percent"`
	Topology        string         `json:"topology"`
	Seed            int64          `json:"seed"`
}

type Metric struct {
	Available bool    `json:"available"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit"`
	Source    string  `json:"source"`
	Reason    string  `json:"reason,omitempty"`
}

type StorageContext struct {
	Scope      string `json:"scope"`
	Source     string `json:"source"`
	Model      string `json:"model"`
	SizeBytes  uint64 `json:"size_bytes"`
	Filesystem string `json:"filesystem"`
	Mountpoint string `json:"mountpoint"`
}

type Context struct {
	DgraphSHA       string            `json:"dgraph_sha"`
	GomapVersion    string            `json:"gomap_version"`
	Dirty           bool              `json:"dirty"`
	GoVersion       string            `json:"go_version"`
	Host            string            `json:"host"`
	Kernel          string            `json:"kernel"`
	CPU             string            `json:"cpu"`
	TotalRAMBytes   uint64            `json:"total_ram_bytes"`
	Storage         StorageContext    `json:"storage"`
	Environment     map[string]string `json:"environment"`
	ExactCommand    []string          `json:"exact_command"`
	RawPath         string            `json:"raw_path"`
	Profiles        []string          `json:"profiles,omitempty"`
	Contaminants    []string          `json:"contaminants"`
	Excluded        bool              `json:"excluded"`
	ExclusionReason string            `json:"exclusion_reason,omitempty"`
}

type Validation struct {
	BackendObserved    string `json:"backend_observed"`
	DurabilityObserved string `json:"durability_observed"`
	SchemaOK           bool   `json:"schema_ok"`
	PostingChecksum    string `json:"posting_checksum"`
	NodeCount          int    `json:"node_count"`
	RestartOK          bool   `json:"restart_ok"`
	UnsupportedOK      bool   `json:"unsupported_ok"`
}

type Result struct {
	SchemaVersion int                `json:"schema_version"`
	RunID         string             `json:"run_id"`
	Repeat        int                `json:"repeat"`
	Config        Config             `json:"config"`
	Context       Context            `json:"context"`
	SetupStarted  time.Time          `json:"setup_started"`
	SetupFinished time.Time          `json:"setup_finished"`
	TimedStarted  time.Time          `json:"timed_started"`
	TimedFinished time.Time          `json:"timed_finished"`
	Throughput    float64            `json:"throughput_ops_s"`
	LatencyMS     map[string]float64 `json:"latency_ms"`
	Metrics       map[string]Metric  `json:"metrics"`
	Validation    Validation         `json:"validation"`
}

var requiredTreeDBMetrics = []string{
	"treedb_public_batch_write_calls", "treedb_public_batch_write_sync_calls",
	"treedb_command_wal_append_point_calls",
	"treedb_group_commit_groups", "treedb_group_commit_commits", "treedb_group_commit_participants",
	"treedb_group_commit_syncs", "treedb_group_commit_group_size_max",
	"treedb_command_wal_file_syncs", "treedb_value_log_syncs", "treedb_value_log_file_syncs",
	"treedb_point_successor_calls", "treedb_point_successor_sources", "treedb_point_successor_sources_max",
	"treedb_iterator_snapshot_rotations", "treedb_leaf_log_segment_rotations",
}

var requiredMetrics = append([]string{
	"cpu_seconds", "rss_peak_bytes", "disk_logical_bytes", "disk_allocated_bytes",
	"write_bytes", "write_amplification", "gc_cycles", "flushes", "checkpoints", "recovery_seconds",
}, requiredTreeDBMetrics...)

func (c Config) Fingerprint() string {
	copy := c
	copy.Backend, copy.DurabilityClass, copy.PostingStore, copy.Badger = "", "", "", ""
	b, _ := json.Marshal(copy)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (r Result) Validate() error {
	var errs []error
	if r.SchemaVersion != SchemaVersion {
		errs = append(errs, fmt.Errorf("schema_version=%d, want %d", r.SchemaVersion, SchemaVersion))
	}
	if r.RunID == "" || r.Repeat < 1 {
		errs = append(errs, errors.New("run_id and positive repeat are required"))
	}
	if r.Config.Topology != "single-zero-single-alpha" {
		errs = append(errs, fmt.Errorf("wrong topology %q", r.Config.Topology))
	}
	if r.Config.DatasetNodes < 1 || r.Config.Concurrency < 1 || r.Config.WarmupOps < 1 || r.Config.TimedOps < 1 {
		errs = append(errs, errors.New("dataset, concurrency, warmup, and timed operation counts must be positive"))
	}
	mix := 0
	for _, v := range r.Config.QueryMix {
		mix += v
	}
	if mix != 100 {
		errs = append(errs, fmt.Errorf("query mix sums to %d, want 100", mix))
	}
	if !r.SetupStarted.Before(r.SetupFinished) || r.SetupFinished.After(r.TimedStarted) || !r.TimedStarted.Before(r.TimedFinished) {
		errs = append(errs, errors.New("setup leaked into or overlapped timed phase"))
	}
	wantBackend, wantDurability, configErr := expectedObserved(r.Config)
	if configErr != nil {
		errs = append(errs, fmt.Errorf("invalid config: %w", configErr))
	} else {
		if r.Validation.BackendObserved != wantBackend {
			errs = append(errs, fmt.Errorf("wrong backend: observed %q want %q", r.Validation.BackendObserved, wantBackend))
		}
		if r.Validation.DurabilityObserved != wantDurability {
			errs = append(errs, fmt.Errorf("wrong durability: observed %q want %q", r.Validation.DurabilityObserved, wantDurability))
		}
	}
	if !r.Validation.SchemaOK || r.Validation.PostingChecksum == "" || r.Validation.NodeCount < r.Config.DatasetNodes || !r.Validation.RestartOK || !r.Validation.UnsupportedOK {
		errs = append(errs, errors.New("logical, schema, posting, restart, or unsupported-feature validation incomplete"))
	}
	if r.Throughput <= 0 {
		errs = append(errs, errors.New("throughput must be positive"))
	}
	for _, q := range []string{"p50", "p95", "p99"} {
		if r.LatencyMS[q] <= 0 {
			errs = append(errs, fmt.Errorf("missing latency %s", q))
		}
	}
	for _, name := range requiredMetrics {
		m, ok := r.Metrics[name]
		if !ok {
			errs = append(errs, fmt.Errorf("missing metric %q", name))
			continue
		}
		if m.Unit == "" || m.Source == "" || (!m.Available && m.Reason == "") {
			errs = append(errs, fmt.Errorf("metric %q lacks unit/source or unavailable reason", name))
		}
		if m.Available && (math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Value < 0) {
			errs = append(errs, fmt.Errorf("metric %q has invalid available value %v", name, m.Value))
		}
	}
	for _, name := range requiredTreeDBMetrics {
		m, ok := r.Metrics[name]
		if r.Config.Backend == "treedb" && (!ok || !m.Available) {
			errs = append(errs, fmt.Errorf("TreeDB diagnostic %q must be available", name))
		}
		if r.Config.Backend == "badger" && ok && m.Available {
			errs = append(errs, fmt.Errorf("TreeDB-only diagnostic %q must be unavailable for Badger", name))
		}
	}
	for _, name := range []string{"cpu_seconds", "rss_peak_bytes", "disk_logical_bytes", "disk_allocated_bytes", "recovery_seconds"} {
		m, ok := r.Metrics[name]
		invalidZero := name != "cpu_seconds" && m.Value <= 0
		if name == "cpu_seconds" && r.Config.TimedOps >= 1000 && m.Value <= 0 {
			invalidZero = true
		}
		if !ok || !m.Available || math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Value < 0 || invalidZero {
			errs = append(errs, fmt.Errorf("core metric %q must be available and valid", name))
		}
	}
	if r.Context.DgraphSHA == "" || r.Context.GomapVersion == "" || r.Context.GoVersion == "" || r.Context.Host == "" || r.Context.Kernel == "" || r.Context.CPU == "" || r.Context.TotalRAMBytes == 0 || len(r.Context.ExactCommand) == 0 || r.Context.RawPath == "" {
		errs = append(errs, errors.New("reproduction context incomplete"))
	}
	if r.Context.Storage.Scope != "artifact_and_posting" || r.Context.Storage.Source == "" || r.Context.Storage.Model == "" || r.Context.Storage.SizeBytes == 0 || r.Context.Storage.Filesystem == "" || r.Context.Storage.Mountpoint == "" {
		errs = append(errs, errors.New("reproduction storage context incomplete"))
	}
	for _, name := range []string{"GOWORK", "TMPDIR", "GOMAXPROCS", "GOFLAGS"} {
		if _, ok := r.Context.Environment[name]; !ok {
			errs = append(errs, fmt.Errorf("reproduction environment missing %s", name))
		}
	}
	if r.Context.Excluded && r.Context.ExclusionReason == "" {
		errs = append(errs, errors.New("excluded result lacks reason"))
	}
	return errors.Join(errs...)
}

func expectedObserved(c Config) (string, string, error) {
	if c.DurabilityClass != "relaxed" && c.DurabilityClass != "durable" {
		return "", "", fmt.Errorf("unknown durability class %q", c.DurabilityClass)
	}
	badgerSync := "false"
	if c.Backend == "badger" && c.DurabilityClass == "durable" {
		badgerSync = "true"
	}
	if err := requireExactSelector(c.Badger, map[string]string{"syncwrites": badgerSync}); err != nil {
		return "", "", fmt.Errorf("badger selector: %w", err)
	}
	switch c.Backend {
	case "badger":
		if err := requireExactSelector(c.PostingStore, map[string]string{"backend": "badger", "tier": "production", "durability": "durable", "events": "false"}); err != nil {
			return "", "", fmt.Errorf("posting-store selector: %w", err)
		}
		return "badger", "syncwrites=" + badgerSync, nil
	case "treedb":
		if err := requireExactSelector(c.PostingStore, map[string]string{"backend": "treedb", "tier": "benchmark_minimal", "durability": c.DurabilityClass, "events": "true"}); err != nil {
			return "", "", fmt.Errorf("posting-store selector: %w", err)
		}
		if c.DurabilityClass == "relaxed" {
			return "treedb", "wal_on_relaxed_sync", nil
		}
		return "treedb", "wal_on_sync", nil
	default:
		return "", "", fmt.Errorf("unknown backend %q", c.Backend)
	}
}

func requireExactSelector(raw string, expected map[string]string) error {
	actual := map[string]string{}
	for _, token := range strings.Split(raw, ";") {
		token = strings.TrimSpace(token)
		parts := strings.Split(token, "=")
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return fmt.Errorf("malformed token %q", token)
		}
		key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if _, duplicate := actual[key]; duplicate {
			return fmt.Errorf("duplicate token %q", key)
		}
		actual[key] = value
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("got %d tokens, want %d", len(actual), len(expected))
	}
	for key, want := range expected {
		if got, ok := actual[key]; !ok || got != want {
			return fmt.Errorf("%s=%q, want %q", key, got, want)
		}
	}
	return nil
}

func WriteImmutable(path string, value any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func WriteImmutableBytes(path string, value []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(value); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func ValidateSet(results []Result, repeats int) error {
	if repeats < 1 {
		return errors.New("repeats must be positive")
	}
	if len(results) != repeats*4 {
		return fmt.Errorf("incomplete matrix: got %d results, want %d", len(results), repeats*4)
	}
	want := map[string]int{"badger/relaxed": repeats, "badger/durable": repeats, "treedb/relaxed": repeats, "treedb/durable": repeats}
	fingerprint := ""
	checksum := ""
	nodeCount := 0
	seenRunIDs := make(map[string]struct{}, len(results))
	seenRepeats := make(map[string]map[int]string, 4)
	var contextIdentity *Context
	for _, r := range results {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("%s: %w", r.RunID, err)
		}
		if r.Context.Dirty {
			return fmt.Errorf("%s has dirty reproduction context", r.RunID)
		}
		if r.Context.Excluded {
			return fmt.Errorf("%s is excluded: %s", r.RunID, r.Context.ExclusionReason)
		}
		if _, duplicate := seenRunIDs[r.RunID]; duplicate {
			return fmt.Errorf("duplicate run_id %q", r.RunID)
		}
		seenRunIDs[r.RunID] = struct{}{}
		if contextIdentity == nil {
			identity := r.Context
			contextIdentity = &identity
		} else if err := sameReproductionContext(*contextIdentity, r.Context); err != nil {
			return fmt.Errorf("reproduction context mismatch for %s: %w", r.RunID, err)
		}
		key := r.Config.Backend + "/" + r.Config.DurabilityClass
		if r.Repeat < 1 || r.Repeat > repeats {
			return fmt.Errorf("repeat ordinal %d for %s is outside 1..%d", r.Repeat, key, repeats)
		}
		if seenRepeats[key] == nil {
			seenRepeats[key] = make(map[int]string, repeats)
		}
		if firstRunID, duplicate := seenRepeats[key][r.Repeat]; duplicate {
			return fmt.Errorf("duplicate repeat ordinal %d for %s (%s and %s); matrix is missing another ordinal", r.Repeat, key, firstRunID, r.RunID)
		}
		seenRepeats[key][r.Repeat] = r.RunID
		want[key]--
		if fingerprint == "" {
			fingerprint = r.Config.Fingerprint()
		} else if fingerprint != r.Config.Fingerprint() {
			return fmt.Errorf("workload mismatch for %s", r.RunID)
		}
		if checksum != "" && checksum != r.Validation.PostingChecksum {
			return fmt.Errorf("logical parity mismatch across matrix")
		}
		checksum = r.Validation.PostingChecksum
		if nodeCount != 0 && nodeCount != r.Validation.NodeCount {
			return fmt.Errorf("node-count parity mismatch across matrix")
		}
		nodeCount = r.Validation.NodeCount
	}
	for key, n := range want {
		if n != 0 {
			return fmt.Errorf("matrix count mismatch for %s: %d", key, n)
		}
		for repeat := 1; repeat <= repeats; repeat++ {
			if _, ok := seenRepeats[key][repeat]; !ok {
				return fmt.Errorf("missing repeat ordinal %d for %s", repeat, key)
			}
		}
	}
	return nil
}

// sameReproductionContext compares only the matrix-wide identity fields. Per-run
// commands, paths, profiles, contaminants, and exclusion details intentionally vary.
func sameReproductionContext(want, got Context) error {
	for _, field := range []struct {
		name      string
		want, got string
	}{
		{"DgraphSHA", want.DgraphSHA, got.DgraphSHA},
		{"GomapVersion", want.GomapVersion, got.GomapVersion},
		{"GoVersion", want.GoVersion, got.GoVersion},
		{"Host", want.Host, got.Host},
		{"Kernel", want.Kernel, got.Kernel},
		{"CPU", want.CPU, got.CPU},
	} {
		if field.want != field.got {
			return fmt.Errorf("%s differs: got %q, want %q", field.name, field.got, field.want)
		}
	}
	if want.TotalRAMBytes != got.TotalRAMBytes {
		return fmt.Errorf("TotalRAMBytes differs: got %d, want %d", got.TotalRAMBytes, want.TotalRAMBytes)
	}
	if want.Storage != got.Storage {
		return fmt.Errorf("storage differs: got %+v, want %+v", got.Storage, want.Storage)
	}
	if !maps.Equal(want.Environment, got.Environment) {
		return fmt.Errorf("environment differs: got %v, want %v", got.Environment, want.Environment)
	}
	return nil
}

func Percentiles(values []float64) map[string]float64 {
	sort.Float64s(values)
	at := func(p float64) float64 { i := int(float64(len(values)-1)*p + .5); return values[i] }
	return map[string]float64{"p50": at(.50), "p95": at(.95), "p99": at(.99)}
}
