// SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeWitnessUsesMaintainedExactParsers(t *testing.T) {
	dir := t.TempDir()
	config, store := filepath.Join(dir, "config"), filepath.Join(dir, "store")
	write := func(path, text string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const valid = "x.WorkerConfig: {HardSync:true}\n"
	write(config, valid)
	write(store, "status=map[backend:treedb profile:raft_checkpoint] stats=map[treedb.cache.memtable_mode:cow_ordered]")
	if _, err := runtimeWitness(config, store); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{valid + valid, "x.WorkerConfig: {HardSync:false}\n", "HardSync:true\n"} {
		write(config, bad)
		if _, err := runtimeWitness(config, store); err == nil {
			t.Fatalf("accepted invalid or ambiguous configuration %q", bad)
		}
	}
	write(config, valid)
	for _, bad := range []string{
		"status=map[backend:treedb profile:raft_checkpoint profile:raft_checkpoint] stats=map[treedb.cache.memtable_mode:cow_ordered]",
		"status=map[backend:treedb profile:raft_checkpoint] stats=map[treedb.cache.memtable_mode:skiplist]",
	} {
		write(store, bad)
		if _, err := runtimeWitness(config, store); err == nil {
			t.Fatalf("accepted invalid or ambiguous store %q", bad)
		}
	}
}
