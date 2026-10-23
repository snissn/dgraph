// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

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
