// SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
// SPDX-License-Identifier: Apache-2.0

// resourcewitness is an offline observer, not a recovery or build authority.
package main

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
)

type source struct {
	Directory string `json:"directory"`
	Revision  string `json:"revision"`
	Tree      string `json:"tree"`
	GoModSHA  string `json:"go_mod_sha256"`
}

type manifest struct {
	Version          int    `json:"version"`
	ServerSHA        string `json:"server_sha256"`
	BuildInfo        string `json:"build_info"`
	GoVersion        string `json:"go_version"`
	Revision         string `json:"vcs_revision"`
	Modified         string `json:"vcs_modified"`
	GomapVersion     string `json:"gomap_module_version"`
	GomapReplacement string `json:"gomap_replacement"`
	Dgraph           source `json:"dgraph_source"`
	Gomap            source `json:"gomap_source"`
}

func digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	if err = errors.Join(err, f.Close()); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func git(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %v: %w: %s", args, err, output)
	}
	return strings.TrimSpace(string(output)), nil
}

func canonical(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(absolute)
}

func inspectSource(dir string) (source, error) {
	var s source
	var err error
	if s.Directory, err = canonical(dir); err != nil {
		return s, err
	}
	top, err := git(s.Directory, "rev-parse", "--show-toplevel")
	if err != nil {
		return s, err
	}
	if top != s.Directory {
		return s, fmt.Errorf("source must name its repository root: %s", dir)
	}
	status, err := git(s.Directory, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return s, err
	}
	if status != "" {
		return s, fmt.Errorf("source is dirty (including untracked files): %s", dir)
	}
	if s.Revision, err = git(s.Directory, "rev-parse", "HEAD"); err != nil {
		return s, err
	}
	if s.Tree, err = git(s.Directory, "rev-parse", "HEAD^{tree}"); err != nil {
		return s, err
	}
	s.GoModSHA, err = digest(filepath.Join(s.Directory, "go.mod"))
	return s, err
}

func inspect(server, dgraph, gomap string) (manifest, error) {
	m := manifest{Version: 1}
	var err error
	if m.Dgraph, err = inspectSource(dgraph); err != nil {
		return m, err
	}
	if m.Gomap, err = inspectSource(gomap); err != nil {
		return m, err
	}
	if m.ServerSHA, err = digest(server); err != nil {
		return m, err
	}
	info, err := buildinfo.ReadFile(server)
	if err != nil {
		return m, err
	}
	if info.Path != "github.com/dgraph-io/dgraph/v25/dgraph" {
		return m, fmt.Errorf("unexpected server main package %q", info.Path)
	}
	m.BuildInfo, m.GoVersion = info.String(), info.GoVersion
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			m.Revision = setting.Value
		case "vcs.modified":
			m.Modified = setting.Value
		}
	}
	if len(m.Revision) != 40 || m.Revision != m.Dgraph.Revision || m.Modified != "false" {
		return m, errors.New("server requires exact clean Dgraph VCS build identity matching source")
	}
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/snissn/gomap" {
			if m.GomapVersion != "" || dependency.Replace == nil || dependency.Replace.Version != "" {
				return m, errors.New("one local Gomap replacement is required")
			}
			m.GomapVersion, m.GomapReplacement = dependency.Version, dependency.Replace.Path
		}
	}
	if m.GomapVersion == "" || m.GomapReplacement == "" {
		return m, errors.New("missing Gomap module/replacement build identity")
	}
	replacement := m.GomapReplacement
	if !filepath.IsAbs(replacement) {
		replacement = filepath.Join(m.Dgraph.Directory, replacement)
	}
	resolved, err := canonical(replacement)
	if err != nil || resolved != m.Gomap.Directory {
		return m, fmt.Errorf("binary Gomap replacement does not resolve to recorded source: %s", replacement)
	}
	// Hash again to detect replacement of the observed binary during inspection.
	if current, err := digest(server); err != nil || current != m.ServerSHA {
		return m, errors.New("server binary changed during inspection")
	}
	for _, before := range []source{m.Dgraph, m.Gomap} {
		after, err := inspectSource(before.Directory)
		if err != nil || after != before {
			return m, fmt.Errorf("source changed during inspection: %s", before.Directory)
		}
	}
	return m, nil
}

func runtimeWitness(configPath, storePath string) (any, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	witness, hardSync, err := livebench.ResolvedConfig(raw)
	if err != nil {
		return nil, err
	}
	if !hardSync {
		return nil, errors.New("resolved filesystem HardSync is false")
	}
	h := sha256.Sum256(raw)
	result := map[string]any{"config_witness": witness, "hard_sync": hardSync,
		"config_log_sha256": hex.EncodeToString(h[:])}
	if storePath != "" {
		raw, err = os.ReadFile(storePath)
		if err != nil {
			return nil, err
		}
		backend, profile, memtable, err := livebench.ResolvedStore(raw)
		if err != nil {
			return nil, err
		}
		c := livebench.Config{SurvivalMode: "filesystem", Backend: "treedb", DurabilityClass: "durable",
			TreeDBPolicy: "raft_checkpoint", TreeDBMemtable: "cow_ordered",
			PostingStore: "backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true", Badger: "syncwrites=false"}
		expectedBackend, expectedProfile, err := livebench.ExpectedObserved(c)
		if err != nil {
			return nil, err
		}
		if backend != expectedBackend || profile != expectedProfile || memtable != c.TreeDBMemtable {
			return nil, fmt.Errorf("resolved posting route mismatch: %s/%s/%s", backend, profile, memtable)
		}
		h = sha256.Sum256(raw)
		result["store_sha256"] = hex.EncodeToString(h[:])
		result["backend"], result["profile"], result["memtable"] = backend, profile, memtable
	}
	return result, nil
}

func run() error {
	mode := flag.String("mode", "", "manifest, verify, or runtime")
	server := flag.String("server", "", "observed prebuilt Dgraph server")
	dgraph := flag.String("dgraph-source", "", "clean Dgraph repository root")
	gomap := flag.String("gomap-source", "", "clean local replacement repository root")
	packet := flag.String("manifest", "", "immutable build/source observation manifest")
	config := flag.String("config-log", "", "retained single-process resolved configuration log")
	store := flag.String("store", "", "optional retained Alpha /debug/store response")
	flag.Parse()
	var result any
	switch *mode {
	case "manifest":
		m, err := inspect(*server, *dgraph, *gomap)
		if err != nil {
			return err
		}
		raw, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}
		if err := livebench.WriteImmutableBytes(*packet, append(raw, '\n')); err != nil {
			return err
		}
		result = m
	case "verify":
		raw, err := os.ReadFile(*packet)
		if err != nil {
			return err
		}
		var expected manifest
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&expected); err != nil {
			return err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return errors.New("manifest contains trailing JSON")
		}
		canonical, err := json.MarshalIndent(expected, "", "  ")
		if err != nil || !bytes.Equal(raw, append(canonical, '\n')) {
			return errors.New("manifest is not in canonical observer format")
		}
		actual, err := inspect(*server, expected.Dgraph.Directory, expected.Gomap.Directory)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, expected) {
			return errors.New("server or clean source differs from retained build/source manifest")
		}
		result = actual
	case "runtime":
		var err error
		result, err = runtimeWitness(*config, *store)
		if err != nil {
			return err
		}
	default:
		return errors.New("--mode must be manifest, verify, or runtime")
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
