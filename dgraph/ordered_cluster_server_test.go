// SPDX-License-Identifier: Apache-2.0
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
)

// TestOrderedDeltaClusterServer starts the real CLI only in an explicitly
// spawned test subprocess. No gate or configuration is compiled into dgraph.
func TestOrderedDeltaClusterServer(t *testing.T) {
	encoded := os.Getenv("DGRAPH_ORDERED_CLUSTER_CHILD_ARGS")
	if encoded == "" {
		t.Skip("clusterqualification subprocess helper")
	}
	var args []string
	if err := json.Unmarshal([]byte(encoded), &args); err != nil {
		t.Fatal(err)
	}
	if len(args) == 0 || args[0] != "alpha" && args[0] != "zero" {
		t.Fatal("helper accepts only Alpha/Zero")
	}
	gate := os.Getenv("DGRAPH_ORDERED_CLUSTER_CHILD_GATE")
	if gate != "" {
		if !filepath.IsAbs(gate) || args[0] != "alpha" {
			t.Fatal("gate requires absolute Alpha test directory")
		}
		// Bind the selected public cached route to its exact main DB root. The
		// parent independently verifies the exported WAL-dir stat for this layout.
		var postings string
		for i, arg := range args {
			if arg == "--postings" && i+1 < len(args) {
				postings = args[i+1]
			}
		}
		if !filepath.IsAbs(postings) {
			t.Fatal("gate requires absolute posting directory")
		}
		expectedRoot := filepath.Join(postings, "maindb")
		var mu sync.Mutex
		var target *durabilitycut.Event
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Resource != durabilitycut.ResourceCommandWAL || e.LSN == 0 || filepath.Clean(e.Root) != expectedRoot {
				return nil
			}
			switch e.Point {
			case durabilitycut.BeforeDependencyFileSync:
				// Rename is the single-use claim. Other sync callers keep their original
				// scheduling and may not release or impersonate the held callback.
				if err := os.Rename(filepath.Join(gate, "arm"), filepath.Join(gate, "claimed")); os.IsNotExist(err) {
					return nil
				} else if err != nil {
					return err
				}
				identity := durabilitycut.Event{Root: e.Root, Path: e.Path, LSN: e.LSN}
				mu.Lock()
				target = &identity
				mu.Unlock()
				b, err := json.Marshal(map[string]any{"root": e.Root, "path": e.Path, "lsn": e.LSN})
				if err != nil {
					return err
				}
				if err = os.WriteFile(filepath.Join(gate, "held"), b, 0600); err != nil {
					return err
				}
				deadline := time.NewTimer(30 * time.Second)
				defer deadline.Stop()
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					if _, err := os.Stat(filepath.Join(gate, "release")); err == nil {
						return nil
					} else if !os.IsNotExist(err) {
						return err
					}
					select {
					case <-ticker.C:
					case <-deadline.C:
						return fmt.Errorf("test native sync gate expired without release")
					}
				}
			case durabilitycut.AfterDependencyFileSync:
				mu.Lock()
				matches := target != nil && target.Root == e.Root && target.Path == e.Path && target.LSN == e.LSN
				mu.Unlock()
				if matches {
					return os.WriteFile(filepath.Join(gate, "durable"), []byte("successful physical sync after release\n"), 0600)
				}
			}
			return nil
		})
		defer restore()
	}
	os.Args = append([]string{"dgraph"}, args...)
	main()
}
