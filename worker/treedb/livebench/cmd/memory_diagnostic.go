// SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
)

const memoryDiagnosticReason = "paired memory observations and forced GC; excluded from performance acceptance"

var memoryDiagnosticBoundaries = []string{"after_warmup_before_counters", "after_native_metrics_before_validation"}
var memoryDiagnosticNames = []string{
	"pre_gc.status.txt", "pre_gc.smaps_rollup.txt", "pre_gc.vars.json", "heap.gc1.pprof",
	"post_gc.status.txt", "post_gc.smaps_rollup.txt", "post_gc.vars.json", "manifest.json",
}

type memoryArtifact struct {
	Name       string    `json:"name"`
	Source     string    `json:"source"`
	Path       string    `json:"path"`
	SHA256     string    `json:"sha256"`
	Bytes      int       `json:"bytes"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

type memoryObservation struct {
	Version           int              `json:"version"`
	DiagnosticOnly    bool             `json:"diagnostic_only"`
	RunID             string           `json:"run_id"`
	Backend           string           `json:"backend"`
	Durability        string           `json:"durability_class"`
	BinarySHA256      string           `json:"server_binary_sha256"`
	PID               int              `json:"alpha_pid"`
	Boundary          string           `json:"boundary"`
	ForcedGCRequested bool             `json:"forced_gc_request_attempted"`
	StartedAt         time.Time        `json:"started_at"`
	FinishedAt        time.Time        `json:"finished_at"`
	TimedFinished     time.Time        `json:"timed_finished"`
	Complete          bool             `json:"complete"`
	Error             string           `json:"error,omitempty"`
	Artifacts         []memoryArtifact `json:"artifacts"`
}

func memoryDiagnosticPath(prefix, boundary, name string) string {
	return prefix + "." + boundary + "." + name
}

func validateMemoryDiagnosticOption(o options) error {
	if o.memoryDiagnostic == "" {
		return nil
	}
	if o.cpuProfile != "" || o.endpointDiagnostic != "" || o.operationDiagnostic != "" || o.storageDiagnostic != "" {
		return errors.New("--memory-diagnostic requires a separate run without CPU/endpoint/operation/storage diagnostics")
	}
	for _, boundary := range memoryDiagnosticBoundaries {
		for _, name := range memoryDiagnosticNames {
			if _, err := diagnosticOutputPath(memoryDiagnosticPath(o.memoryDiagnostic, boundary, name),
				filepath.Join(o.artifactDir, "cluster", "p"), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// The two raw-state envelopes bracket a GC-assisted heap sample, not an atomic
// process snapshot. Pre-GC state is captured before requesting GC; post-GC state
// also includes the cost of serving/serializing the heap profile. No forced-GC
// measurement is used as an unperturbed RSS or workload acceptance result.
func captureMemoryDiagnostic(prefix, boundary, postings, base string, pid int, r livebench.Result) (profiles []string, err error) {
	d := memoryObservation{Version: 1, DiagnosticOnly: true, RunID: r.RunID,
		Backend: r.Config.Backend, Durability: r.Config.DurabilityClass, BinarySHA256: r.Context.BinarySHA256,
		PID: pid, Boundary: boundary, StartedAt: time.Now().UTC(), TimedFinished: r.TimedFinished}
	if pid <= 0 || r.RunID == "" || r.Context.BinarySHA256 == "" || !r.Context.Excluded ||
		!strings.Contains(r.Context.ExclusionReason, memoryDiagnosticReason) ||
		(boundary != memoryDiagnosticBoundaries[0] && boundary != memoryDiagnosticBoundaries[1]) {
		return nil, errors.New("invalid memory diagnostic process/boundary")
	}
	defer func() {
		d.FinishedAt = time.Now().UTC()
		d.Complete = err == nil
		if err != nil {
			d.Error = err.Error()
		}
		output, pathErr := diagnosticOutputPath(memoryDiagnosticPath(prefix, boundary, "manifest.json"), postings, nil)
		if pathErr != nil {
			err = errors.Join(err, pathErr)
			return
		}
		err = errors.Join(err, livebench.WriteImmutable(output, d))
	}()
	read := func(name, source string, get func() ([]byte, error)) error {
		output, err := diagnosticOutputPath(memoryDiagnosticPath(prefix, boundary, name), postings, nil)
		if err != nil {
			return err
		}
		a := memoryArtifact{Name: name, Source: source, Path: output, StartedAt: time.Now().UTC()}
		raw, err := get()
		a.FinishedAt = time.Now().UTC()
		if err != nil {
			return fmt.Errorf("memory observation %s: %w", name, err)
		}
		if len(raw) == 0 {
			return fmt.Errorf("empty memory observation %s", name)
		}
		if err := livebench.WriteImmutableBytes(output, raw); err != nil {
			return err
		}
		h := sha256.Sum256(raw)
		a.SHA256, a.Bytes = hex.EncodeToString(h[:]), len(raw)
		d.Artifacts = append(d.Artifacts, a)
		if name == "heap.gc1.pprof" {
			profiles = append(profiles, output)
		}
		if strings.HasSuffix(name, ".vars.json") && !json.Valid(raw) {
			return errors.New("retained /debug/vars response is not valid JSON")
		}
		return nil
	}
	state := func(phase string) error {
		for _, name := range []string{"status", "smaps_rollup"} {
			path := fmt.Sprintf("/proc/%d/%s", pid, name)
			if err := read(phase+"."+name+".txt", path, func() ([]byte, error) { return os.ReadFile(path) }); err != nil {
				return err
			}
		}
		return read(phase+".vars.json", base+"/debug/vars", func() ([]byte, error) {
			return fetch(base+"/debug/vars", 30*time.Second)
		})
	}
	if err = state("pre_gc"); err != nil {
		return profiles, err
	}
	url := base + "/debug/pprof/heap?gc=1"
	if err = read("heap.gc1.pprof", url, func() ([]byte, error) {
		d.ForcedGCRequested = true
		return fetch(url, 30*time.Second)
	}); err != nil {
		return profiles, err
	}
	err = state("post_gc")
	return profiles, err
}
