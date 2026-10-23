// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ResolvedConfig extracts the single resolved configuration from a retained log.
func ResolvedConfig(raw []byte) (string, bool, error) {
	witness := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "x.WorkerConfig:") {
			if witness != "" {
				return "", false, errors.New("ambiguous resolved configuration")
			}
			witness = line
		}
	}
	hardSync, err := ResolvedHardSync(witness)
	return witness, hardSync, err
}

// ResolvedStore extracts the route from the retained /debug/store response.
// Duplicate sections or keys are refused rather than choosing one witness.
func ResolvedStore(raw []byte) (backend, profile, memtable string, err error) {
	section := func(prefix string) (map[string]string, error) {
		s := string(raw)
		if strings.Count(s, prefix) != 1 {
			return nil, fmt.Errorf("missing or ambiguous %s witness", prefix)
		}
		_, tail, _ := strings.Cut(s, prefix)
		body, _, ok := strings.Cut(tail, "]")
		if !ok {
			return nil, errors.New("truncated store witness")
		}
		fields := make(map[string]string)
		for _, field := range strings.Fields(body) {
			key, value, ok := strings.Cut(field, ":")
			if !ok {
				continue // Other diagnostic values may contain spaces.
			}
			if _, duplicate := fields[key]; duplicate {
				return nil, fmt.Errorf("duplicate store witness key %q", key)
			}
			fields[key] = value
		}
		return fields, nil
	}
	status, err := section("status=map[")
	if err != nil {
		return "", "", "", err
	}
	stats, err := section("stats=map[")
	if err != nil {
		return "", "", "", err
	}
	backend, profile, memtable = status["backend"], status["profile"], stats["treedb.cache.memtable_mode"]
	if backend == "" || profile == "" || (backend == "treedb" && memtable == "") {
		return "", "", "", errors.New("incomplete resolved store witness")
	}
	return backend, profile, memtable, nil
}

// VerifyRuntimeArtifacts binds schema-v5 claims to their retained sidecars. The
// directory comes from the result's loaded path, never its JSON raw_path field.
// ValidateSet rechecks these files so deletion or replacement after load fails.
func (r *Result) VerifyRuntimeArtifacts(dir string) error {
	if r.SchemaVersion != SchemaVersion {
		return nil
	}
	if err := validateRuntime(*r); err != nil {
		return err
	}
	if dir == "" {
		return errors.New("runtime artifacts were not verified on disk")
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	read := func(name, expected string) ([]byte, error) {
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("runtime artifact %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("runtime artifact %s is not regular", path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != expected {
			return nil, fmt.Errorf("runtime artifact %s checksum mismatch", path)
		}
		return raw, nil
	}
	for _, identity := range r.Validation.Runtime {
		prefix := "runtime-" + identity.Phase + "-" + identity.Process
		raw, err := read(prefix+".config.log", identity.ConfigLogSHA256)
		if err != nil {
			return err
		}
		witness, hardSync, err := ResolvedConfig(raw)
		if err != nil {
			return fmt.Errorf("runtime artifact %s configuration mismatch: %w", prefix, err)
		}
		if witness != identity.ConfigWitness || hardSync != identity.HardSync {
			return fmt.Errorf("runtime artifact %s configuration mismatch", prefix)
		}
		if identity.Process == "alpha" {
			raw, err := read(prefix+".store.txt", identity.StoreWitnessSHA256)
			if err != nil {
				return err
			}
			backend, profile, memtable, err := ResolvedStore(raw)
			if err != nil {
				return fmt.Errorf("runtime artifact %s posting route mismatch: %w", prefix, err)
			}
			if backend != identity.Backend || profile != identity.Durability || memtable != identity.Memtable {
				return fmt.Errorf("runtime artifact %s posting route mismatch", prefix)
			}
		}
	}
	r.runtimeArtifactDir = dir
	return nil
}

// RuntimeIdentity binds the actual process configuration and posting route to
// retained raw artifacts. The restart phase restarts Alpha only; Zero is
// observed again, not implicitly claimed to have restarted or lost power.
type RuntimeIdentity struct {
	Process            string   `json:"process"`
	Phase              string   `json:"phase"`
	PID                int      `json:"pid"`
	Command            []string `json:"command"`
	SurvivalMode       string   `json:"survival_mode"`
	HardSync           bool     `json:"hard_sync"`
	ConfigWitness      string   `json:"config_witness"`
	ConfigLogSHA256    string   `json:"config_log_sha256"`
	StoreWitnessSHA256 string   `json:"store_witness_sha256,omitempty"`
	Backend            string   `json:"backend,omitempty"`
	Durability         string   `json:"durability,omitempty"`
	Memtable           string   `json:"memtable,omitempty"`
}

var hardSyncField = regexp.MustCompile(`(?:^|[ {])HardSync:(true|false)(?:[ }]|$)`)

// ResolvedHardSync accepts the redacted resolved worker configuration, never a
// command-line mode name or an unrelated occurrence of a sync option.
func ResolvedHardSync(line string) (bool, error) {
	if !strings.Contains(line, "x.WorkerConfig:") {
		return false, errors.New("missing resolved x.WorkerConfig witness")
	}
	fields := hardSyncField.FindAllStringSubmatch(line, -1)
	if len(fields) != 1 {
		return false, errors.New("missing or ambiguous resolved HardSync witness")
	}
	return fields[0][1] == "true", nil
}

func isSHA256(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && s == strings.ToLower(s)
}

func validateRuntime(r Result) error {
	c := r.Config
	if c.SurvivalMode != "process" && c.SurvivalMode != "filesystem" {
		return errors.New("explicit valid survival_mode is required")
	}
	if !isSHA256(r.Context.BinarySHA256) || r.Context.BinaryBuildInfo == "" {
		return errors.New("measured binary hash/build identity missing")
	}
	if c.Backend == "badger" && (c.TreeDBPolicy != "" || c.TreeDBMemtable != "") {
		return errors.New("badger config contains TreeDB selection")
	}
	if c.Backend == "treedb" && c.TreeDBPolicy != "store_wal" && c.TreeDBPolicy != "raft_checkpoint" {
		return errors.New("explicit TreeDB recovery policy is required")
	}
	backend, profile, err := expectedObserved(c)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, runtime := range r.Validation.Runtime {
		key := runtime.Process + "/" + runtime.Phase
		if (runtime.Process != "zero" && runtime.Process != "alpha") ||
			(runtime.Phase != "startup" && runtime.Phase != "after_alpha_restart") || seen[key] {
			return fmt.Errorf("unexpected/duplicate runtime identity %q", key)
		}
		seen[key] = true
		hardSync, err := ResolvedHardSync(runtime.ConfigWitness)
		if err != nil || runtime.HardSync != hardSync || hardSync != (c.SurvivalMode == "filesystem") || runtime.SurvivalMode != c.SurvivalMode {
			return fmt.Errorf("runtime survival/HardSync mismatch for %s", key)
		}
		if runtime.PID <= 0 || !isSHA256(runtime.ConfigLogSHA256) || len(runtime.Command) < 3 || runtime.Command[1] != runtime.Process {
			return fmt.Errorf("runtime process/command binding incomplete for %s", key)
		}
		flags := 0
		for _, arg := range runtime.Command {
			if strings.HasPrefix(arg, "--survive") {
				if arg != "--survive="+c.SurvivalMode {
					return fmt.Errorf("runtime survival argument mismatch for %s", key)
				}
				flags++
			}
		}
		if flags != 1 {
			return fmt.Errorf("one explicit survival argument required for %s", key)
		}
		if runtime.Process == "alpha" {
			if !isSHA256(runtime.StoreWitnessSHA256) || runtime.Backend != backend || runtime.Durability != profile {
				return fmt.Errorf("runtime posting route mismatch for %s", key)
			}
			if c.Backend == "treedb" && (runtime.Memtable == "" || (c.TreeDBMemtable != "" && runtime.Memtable != c.TreeDBMemtable)) {
				return fmt.Errorf("runtime TreeDB memtable mismatch for %s", key)
			}
		}
	}
	if len(seen) != 4 {
		return errors.New("startup/restart identity required for Zero and Alpha")
	}
	return nil
}
