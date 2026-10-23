// SPDX-License-Identifier: Apache-2.0
package main

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
)

func normalizeRuntimeOptions(o *options) error {
	if o.survival == "" {
		o.survival = "process"
	}
	if o.survival != "process" && o.survival != "filesystem" {
		return errors.New("--survive must be process or filesystem")
	}
	return nil
}

func runtimeConfig(o options) livebench.Config {
	posting, badger := selectors(o.backend, o.class)
	c := livebench.Config{SurvivalMode: o.survival, Backend: o.backend, DurabilityClass: o.class,
		PostingStore: posting, Badger: badger, DatasetNodes: o.dataset, Concurrency: o.concurrency,
		WarmupOps: o.warmup, TimedOps: o.timed, QueryMix: map[string]int{"point_read": 60, "one_hop_read": 20, "write": 20},
		Topology: "single-zero-single-alpha", Seed: o.seed}
	if o.backend == "treedb" {
		c.TreeDBPolicy, c.TreeDBMemtable = "store_wal", o.treeDBMemtable
	}
	return c
}

func measuredBinary(path string) (string, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	if err := errors.Join(copyErr, f.Close()); err != nil {
		return "", "", err
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("read measured Go binary identity: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), info.String(), nil
}

// Source identity comes from the executed binary, not go list in an unrelated
// checkout. A local replacement remains explicit in this record and build info;
// final qualification additionally pins that replacement's exact source tree.
func measuredSources(path string) (revision, gomap, goVersion string, dirty bool, err error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", "", "", false, err
	}
	if info.Path != "github.com/dgraph-io/dgraph/v25/dgraph" {
		return "", "", "", false, fmt.Errorf("unexpected measured main package %q", info.Path)
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			dirty = setting.Value == "true"
		}
	}
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/snissn/gomap" {
			gomap = dependency.Version
			if dependency.Replace != nil {
				gomap += " => " + dependency.Replace.Path + "@" + dependency.Replace.Version
			}
		}
	}
	if len(revision) != 40 || gomap == "" {
		return "", "", "", false, errors.New("measured binary lacks VCS revision or Gomap module identity; build with VCS metadata")
	}
	return revision, gomap, info.GoVersion, dirty, nil
}

func verifyMeasuredBinary(path, expected string) error {
	actual, _, err := measuredBinary(path)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("measured binary changed during run")
	}
	return nil
}

func captureRuntime(o options, phase string, zero, alpha *process, base string) ([]livebench.RuntimeIdentity, error) {
	var result []livebench.RuntimeIdentity
	for _, entry := range []struct {
		role    string
		process *process
	}{{"zero", zero}, {"alpha", alpha}} {
		p := entry.process
		if p == nil || p.cmd == nil || p.cmd.Process == nil {
			return nil, errors.New("runtime process missing")
		}
		raw, err := os.ReadFile(p.log.Name())
		if err != nil {
			return nil, err
		}
		witness, hardSync, err := livebench.ResolvedConfig(raw)
		if err != nil {
			return nil, fmt.Errorf("%s resolved configuration: %w", entry.role, err)
		}
		if hardSync != (o.survival == "filesystem") {
			return nil, fmt.Errorf("%s resolved survival mismatch", entry.role)
		}
		prefix := filepath.Join(o.artifactDir, "runtime-"+phase+"-"+entry.role)
		if err := livebench.WriteImmutableBytes(prefix+".config.log", raw); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(raw)
		identity := livebench.RuntimeIdentity{Process: entry.role, Phase: phase, PID: p.cmd.Process.Pid,
			Command: append([]string(nil), p.cmd.Args...), SurvivalMode: o.survival, HardSync: hardSync,
			ConfigWitness: witness, ConfigLogSHA256: hex.EncodeToString(sum[:])}
		if entry.role == "alpha" {
			store, err := fetch(base+"/debug/store", 30*time.Second)
			if err != nil {
				return nil, err
			}
			if err := livebench.WriteImmutableBytes(prefix+".store.txt", store); err != nil {
				return nil, err
			}
			identity.Backend, identity.Durability, identity.Memtable, err = livebench.ResolvedStore(store)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(store)
			identity.StoreWitnessSHA256 = hex.EncodeToString(sum[:])
			backend, profile, err := livebench.ExpectedObserved(runtimeConfig(o))
			if err != nil {
				return nil, err
			}
			if identity.Backend != backend || identity.Durability != profile || (o.backend == "treedb" && (identity.Memtable == "" || (o.treeDBMemtable != "" && identity.Memtable != o.treeDBMemtable))) {
				return nil, errors.New("observed posting backend/profile/memtable mismatch")
			}
		}
		result = append(result, identity)
	}
	return result, nil
}
