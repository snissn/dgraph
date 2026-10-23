// SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"golang.org/x/mod/module"
	"golang.org/x/mod/sumdb/dirhash"
	modulezip "golang.org/x/mod/zip"
)

const testGomapVersion = "v0.6.2-0.20261023065356-2cb705090aa7"

func writeWitnessFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func publishedFixture(t *testing.T) (manifest, *debug.Module) {
	t.Helper()
	dir := t.TempDir()
	m := manifest{Version: 1, Dgraph: source{Directory: filepath.Join(dir, "dgraph")},
		Gomap: source{Directory: filepath.Join(dir, "gomap")}}
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "go.mod"), "module "+gomapModulePath+"\n\ngo 1.26\n")
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "TreeDB", "public.go"), "package treedb\n")
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "docs", "evidence", "go.mod"), "module example.com/archive\n")
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "docs", "evidence", "raw.json"), "archived evidence\n")
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, ".git", "config"), "repository metadata\n")
	// Independent archive oracle: production hashes CheckDir's file list, while
	// this fixture creates the official module zip and hashes its actual entries.
	archive, err := os.Create(filepath.Join(dir, "module.zip"))
	if err != nil {
		t.Fatal(err)
	}
	err = modulezip.CreateFromDir(archive, module.Version{Path: gomapModulePath, Version: testGomapVersion}, m.Gomap.Directory)
	closeErr := archive.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("module zip: %v; close: %v", err, closeErr)
	}
	sum, err := dirhash.HashZip(archive.Name(), dirhash.Hash1)
	if err != nil {
		t.Fatal(err)
	}
	dep := &debug.Module{Path: gomapModulePath, Version: testGomapVersion, Sum: sum}
	writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.mod"),
		"module example.com/dgraph\n\ngo 1.26\n\nrequire "+gomapModulePath+" "+dep.Version+"\n")
	writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.sum"), gomapModulePath+" "+dep.Version+" "+dep.Sum+"\n")
	return m, dep
}

func TestPublishedGomapWitnessBindsOfficialModuleContent(t *testing.T) {
	m, dep := publishedFixture(t)
	if err := bindGomap(&m, dep); err != nil {
		t.Fatal(err)
	}
	if m.Version != 2 || m.GomapSum != dep.Sum || m.GomapReplacement != "" || m.GomapVersion != dep.Version {
		t.Fatalf("incorrect published identity: %+v", m)
	}
	// Nested modules and VCS metadata remain bound by the clean source tree in
	// inspect, but the published module checksum correctly excludes their bytes.
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "docs", "evidence", "raw.json"), "different archive\n")
	writeWitnessFile(t, filepath.Join(m.Gomap.Directory, ".git", "config"), "different metadata\n")
	if err := bindGomap(&m, dep); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedGomapWitnessRejectsIdentityDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *manifest, *debug.Module)
	}{
		{"changed_module_bytes", func(t *testing.T, m *manifest, _ *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "TreeDB", "public.go"), "package changed\n")
		}},
		{"extra_build_output", func(t *testing.T, m *manifest, _ *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Gomap.Directory, "bin", "ignored-build-output"), "not published\n")
		}},
		{"linked_worktree_metadata", func(t *testing.T, m *manifest, _ *debug.Module) {
			if err := os.RemoveAll(filepath.Join(m.Gomap.Directory, ".git")); err != nil {
				t.Fatal(err)
			}
			writeWitnessFile(t, filepath.Join(m.Gomap.Directory, ".git"), "gitdir: /another/worktree\n")
		}},
		{"missing_build_sum", func(_ *testing.T, _ *manifest, dep *debug.Module) { dep.Sum = "" }},
		{"different_build_sum", func(_ *testing.T, _ *manifest, dep *debug.Module) { dep.Sum = "h1:wrong" }},
		{"missing_direct_pin", func(t *testing.T, m *manifest, _ *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.mod"), "module example.com/dgraph\n")
		}},
		{"different_direct_pin", func(t *testing.T, m *manifest, _ *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.mod"), "module example.com/dgraph\nrequire "+gomapModulePath+" v0.6.1\n")
		}},
		{"source_replacement", func(t *testing.T, m *manifest, dep *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.mod"), "module example.com/dgraph\nrequire "+gomapModulePath+" "+dep.Version+"\nreplace "+gomapModulePath+" => ../gomap\n")
		}},
		{"indirect_pin", func(t *testing.T, m *manifest, dep *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.mod"), "module example.com/dgraph\nrequire "+gomapModulePath+" "+dep.Version+" // indirect\n")
		}},
		{"missing_source_sum", func(t *testing.T, m *manifest, _ *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.sum"), "")
		}},
		{"different_source_sum", func(t *testing.T, m *manifest, dep *debug.Module) {
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.sum"), gomapModulePath+" "+dep.Version+" h1:wrong\n")
		}},
		{"duplicate_source_sum", func(t *testing.T, m *manifest, dep *debug.Module) {
			line := gomapModulePath + " " + dep.Version + " " + dep.Sum + "\n"
			writeWitnessFile(t, filepath.Join(m.Dgraph.Directory, "go.sum"), line+line)
		}},
		{"invalid_module_version", func(_ *testing.T, _ *manifest, dep *debug.Module) { dep.Version = "(devel)" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, dep := publishedFixture(t)
			test.mutate(t, &m, dep)
			if err := bindGomap(&m, dep); err == nil {
				t.Fatal("accepted identity drift")
			}
		})
	}
}

func TestGomapBuildDependencyMustBeUnique(t *testing.T) {
	dep := &debug.Module{Path: gomapModulePath, Version: testGomapVersion}
	if got, err := gomapDependency([]*debug.Module{{Path: "unrelated"}, dep}); err != nil || got != dep {
		t.Fatalf("unique dependency: %v, %v", got, err)
	}
	for _, deps := range [][]*debug.Module{nil, {dep, dep}, {{Path: gomapModulePath}}} {
		if _, err := gomapDependency(deps); err == nil {
			t.Fatal("accepted missing, duplicate or unversioned dependency")
		}
	}
}

func TestLocalReplacementWitnessRemainsStrictAndVersionOne(t *testing.T) {
	m, dep := publishedFixture(t)
	dep.Replace = &debug.Module{Path: "../gomap"}
	if err := bindGomap(&m, dep); err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 || m.GomapReplacement != "../gomap" || m.GomapSum != "" {
		t.Fatalf("local manifest changed: %+v", m)
	}
	dep.Replace.Path = "../wrong-source"
	if err := bindGomap(&m, dep); err == nil {
		t.Fatal("accepted wrong replacement directory")
	}
	dep.Replace = &debug.Module{Path: gomapModulePath, Version: "v0.6.1"}
	if err := bindGomap(&m, dep); err == nil {
		t.Fatal("accepted versioned replacement")
	}
}

func TestVersionOneManifestCanonicalBytesRemainCompatible(t *testing.T) {
	const legacy = `{
  "version": 1,
  "server_sha256": "",
  "build_info": "",
  "go_version": "",
  "vcs_revision": "",
  "vcs_modified": "",
  "gomap_module_version": "v0.6.1",
  "gomap_replacement": "../gomap",
  "dgraph_source": {
    "directory": "",
    "revision": "",
    "tree": "",
    "go_mod_sha256": ""
  },
  "gomap_source": {
    "directory": "",
    "revision": "",
    "tree": "",
    "go_mod_sha256": ""
  }
}`
	var m manifest
	if err := json.Unmarshal([]byte(legacy), &m); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil || string(raw) != legacy || strings.Contains(string(raw), "gomap_module_sum") {
		t.Fatalf("version-one canonical bytes changed: %s; %v", raw, err)
	}
}

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
