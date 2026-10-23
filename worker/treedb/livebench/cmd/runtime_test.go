// SPDX-License-Identifier: Apache-2.0
package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeCaptureRefusesWrongResolvedConfigurationAndRoute(t *testing.T) {
	for _, tc := range []struct {
		name, config, profile, memtable string
		fail                            bool
	}{
		{"valid", "x.WorkerConfig: {HardSync:true}", "raft_checkpoint", "cow_ordered", false},
		{"wrong sync", "x.WorkerConfig: {HardSync:false}", "raft_checkpoint", "cow_ordered", true},
		{"missing config", "flags survive=filesystem", "raft_checkpoint", "cow_ordered", true},
		{"wrong profile", "x.WorkerConfig: {HardSync:true}", "wal_on_sync", "cow_ordered", true},
		{"wrong memtable", "x.WorkerConfig: {HardSync:true}", "raft_checkpoint", "btree", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			process := func(role string) *process {
				f, err := os.Create(filepath.Join(dir, role+".log"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.Close() })
				if _, err := f.WriteString(tc.config + "\n"); err != nil {
					t.Fatal(err)
				}
				return &process{cmd: &exec.Cmd{Args: []string{"dgraph", role, "--survive=filesystem"}, Process: &os.Process{Pid: os.Getpid()}}, log: f}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("status=map[backend:treedb profile:" + tc.profile + "] stats=map[treedb.cache.memtable_mode:" + tc.memtable + "]"))
			}))
			defer server.Close()
			o := options{artifactDir: dir, backend: "treedb", class: "durable", survival: "filesystem", treeDBPolicy: "raft_checkpoint", treeDBMemtable: "cow_ordered"}
			identities, err := captureRuntime(o, "startup", process("zero"), process("alpha"), server.URL)
			if (err != nil) != tc.fail {
				t.Fatalf("identities=%v err=%v", identities, err)
			}
			if !tc.fail {
				if len(identities) != 2 || !identities[0].HardSync {
					t.Fatal(identities)
				}
				if _, err := os.Stat(filepath.Join(dir, "runtime-startup-alpha.store.txt")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRuntimeOptionsRejectWeakCheckpointBeforeCreatingArtifacts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-not-exist")
	err := run(options{artifactDir: dir, survival: "process", treeDBPolicy: "raft_checkpoint", treeDBMemtable: "cow_ordered"})
	if err == nil || !strings.Contains(err.Error(), "filesystem") {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("artifacts created: %v", err)
	}
}

func TestBinaryBindingRefusesChangedOrNonGoBinary(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	hash, info, err := measuredBinary(executable)
	if err != nil || len(hash) != 64 || info == "" {
		t.Fatalf("%q %q %v", hash, info, err)
	}
	if err := verifyMeasuredBinary(executable, strings.Repeat("0", 64)); err == nil {
		t.Fatal("binary drift accepted")
	}
	path := filepath.Join(t.TempDir(), "fake")
	if err := os.WriteFile(path, []byte("not a Go binary"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := measuredBinary(path); err == nil {
		t.Fatal("unbound executable accepted")
	}
}
