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
	"os"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1

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
	Value     float64 `json:"value,omitempty"`
	Unit      string  `json:"unit"`
	Source    string  `json:"source"`
	Reason    string  `json:"reason,omitempty"`
}

type Context struct {
	DgraphSHA       string   `json:"dgraph_sha"`
	GomapVersion    string   `json:"gomap_version"`
	Dirty           bool     `json:"dirty"`
	GoVersion       string   `json:"go_version"`
	Host            string   `json:"host"`
	Kernel          string   `json:"kernel"`
	CPU             string   `json:"cpu"`
	ExactCommand    []string `json:"exact_command"`
	RawPath         string   `json:"raw_path"`
	Profiles        []string `json:"profiles,omitempty"`
	Contaminants    []string `json:"contaminants"`
	Excluded        bool     `json:"excluded"`
	ExclusionReason string   `json:"exclusion_reason,omitempty"`
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

var requiredMetrics = []string{
	"cpu_seconds", "rss_peak_bytes", "disk_logical_bytes", "disk_allocated_bytes",
	"write_bytes", "write_amplification", "gc_cycles", "flushes", "checkpoints", "recovery_seconds",
}

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
	wantBackend, wantDurability := expectedObserved(r.Config)
	if r.Validation.BackendObserved != wantBackend {
		errs = append(errs, fmt.Errorf("wrong backend: observed %q want %q", r.Validation.BackendObserved, wantBackend))
	}
	if !durabilityMatches(r.Config.Backend, r.Validation.DurabilityObserved, wantDurability) {
		errs = append(errs, fmt.Errorf("wrong durability: observed %q want %q", r.Validation.DurabilityObserved, wantDurability))
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
	}
	if r.Context.DgraphSHA == "" || r.Context.GomapVersion == "" || r.Context.GoVersion == "" || r.Context.Host == "" || len(r.Context.ExactCommand) == 0 || r.Context.RawPath == "" {
		errs = append(errs, errors.New("reproduction context incomplete"))
	}
	if r.Context.Excluded && r.Context.ExclusionReason == "" {
		errs = append(errs, errors.New("excluded result lacks reason"))
	}
	return errors.Join(errs...)
}

func expectedObserved(c Config) (string, string) {
	switch {
	case c.Backend == "badger" && c.DurabilityClass == "relaxed" && strings.Contains(c.Badger, "syncwrites=false"):
		return "badger", "syncwrites=false"
	case c.Backend == "badger" && c.DurabilityClass == "durable" && strings.Contains(c.Badger, "syncwrites=true"):
		return "badger", "syncwrites=true"
	case c.Backend == "treedb" && c.DurabilityClass == "relaxed" && strings.Contains(c.PostingStore, "durability=relaxed"):
		return "treedb", "wal_on_relaxed_sync"
	case c.Backend == "treedb" && c.DurabilityClass == "durable" && strings.Contains(c.PostingStore, "durability=durable"):
		return "treedb", "wal_on_sync"
	default:
		return "invalid-config", "invalid-config"
	}
}

func durabilityMatches(backend, observed, expected string) bool {
	if backend == "treedb" {
		return strings.HasPrefix(observed, expected)
	}
	return observed == expected
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
	if len(results) != repeats*4 {
		return fmt.Errorf("incomplete matrix: got %d results, want %d", len(results), repeats*4)
	}
	want := map[string]int{"badger/relaxed": repeats, "badger/durable": repeats, "treedb/relaxed": repeats, "treedb/durable": repeats}
	fingerprint := ""
	checksums := map[string]string{}
	for _, r := range results {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("%s: %w", r.RunID, err)
		}
		if r.Context.Excluded {
			return fmt.Errorf("%s is excluded: %s", r.RunID, r.Context.ExclusionReason)
		}
		key := r.Config.Backend + "/" + r.Config.DurabilityClass
		want[key]--
		if fingerprint == "" {
			fingerprint = r.Config.Fingerprint()
		} else if fingerprint != r.Config.Fingerprint() {
			return fmt.Errorf("workload mismatch for %s", r.RunID)
		}
		if old := checksums[r.Config.DurabilityClass]; old != "" && old != r.Validation.PostingChecksum {
			return fmt.Errorf("logical parity mismatch in %s class", r.Config.DurabilityClass)
		}
		checksums[r.Config.DurabilityClass] = r.Validation.PostingChecksum
	}
	for key, n := range want {
		if n != 0 {
			return fmt.Errorf("matrix count mismatch for %s: %d", key, n)
		}
	}
	return nil
}

func Percentiles(values []float64) map[string]float64 {
	sort.Float64s(values)
	at := func(p float64) float64 { i := int(float64(len(values)-1)*p + .5); return values[i] }
	return map[string]float64{"p50": at(.50), "p95": at(.95), "p99": at(.99)}
}
