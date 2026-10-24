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
	"runtime/debug"
	"strings"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/sumdb/dirhash"
	modulezip "golang.org/x/mod/zip"
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
	GomapSum         string `json:"gomap_module_sum,omitempty"`
	Dgraph           source `json:"dgraph_source"`
	Gomap            source `json:"gomap_source"`
}

const gomapModulePath = "github.com/snissn/gomap"

func gomapDependency(deps []*debug.Module) (*debug.Module, error) {
	var found *debug.Module
	for _, dep := range deps {
		if dep.Path == gomapModulePath {
			if found != nil {
				return nil, errors.New("duplicate Gomap build dependencies")
			}
			found = dep
		}
	}
	if found == nil || found.Version == "" {
		return nil, errors.New("missing Gomap module build identity")
	}
	return found, nil
}

// publishedSourceSum uses exactly the maintained module-zip file selection,
// including nested-module exclusions, without constructing a full zip archive.
func publishedSourceSum(dir, version string) (string, error) {
	if err := module.Check(gomapModulePath, version); err != nil {
		return "", err
	}
	checked, err := modulezip.CheckDir(dir)
	if err != nil {
		return "", err
	}
	files := make([]string, 0, len(checked.Valid))
	paths := make(map[string]string, len(checked.Valid))
	for _, path := range checked.Valid {
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return "", err
		}
		name := gomapModulePath + "@" + version + "/" + filepath.ToSlash(relative)
		files = append(files, name)
		paths[name] = path
	}
	return dirhash.Hash1(files, func(name string) (io.ReadCloser, error) {
		return os.Open(paths[name])
	})
}

func bindGomap(m *manifest, dep *debug.Module) error {
	m.GomapVersion = dep.Version
	if dep.Replace != nil {
		if dep.Replace.Path == "" || (dep.Replace.Version != "" && dep.Replace.Version != "(devel)") {
			return errors.New("gomap replacement must name a local source directory")
		}
		m.GomapReplacement = dep.Replace.Path
		replacement := dep.Replace.Path
		if !filepath.IsAbs(replacement) {
			replacement = filepath.Join(m.Dgraph.Directory, replacement)
		}
		resolved, err := canonical(replacement)
		if err != nil || resolved != m.Gomap.Directory {
			return fmt.Errorf("binary Gomap replacement does not resolve to recorded source: %s", replacement)
		}
		return nil
	}
	// Buildinfo records the published module content hash. Require the committed
	// direct pin and checksum as well, rather than trusting a checkout label.
	if !strings.HasPrefix(dep.Sum, "h1:") {
		return errors.New("published Gomap build dependency lacks an h1 content checksum")
	}
	raw, err := os.ReadFile(filepath.Join(m.Dgraph.Directory, "go.mod"))
	if err != nil {
		return err
	}
	mod, err := modfile.Parse("go.mod", raw, nil)
	if err != nil {
		return err
	}
	for _, replace := range mod.Replace {
		if replace.Old.Path == gomapModulePath {
			return errors.New("published Gomap witness refuses a source module replacement")
		}
	}
	matched := false
	for _, require := range mod.Require {
		if require.Mod.Path == gomapModulePath {
			if matched || require.Indirect || require.Mod.Version != dep.Version {
				return errors.New("published Gomap build version differs from the direct source pin")
			}
			matched = true
		}
	}
	if !matched {
		return errors.New("missing direct published Gomap source pin")
	}
	raw, err = os.ReadFile(filepath.Join(m.Dgraph.Directory, "go.sum"))
	if err != nil {
		return err
	}
	matched = false
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == gomapModulePath && fields[1] == dep.Version {
			if matched || len(fields) != 3 || fields[2] != dep.Sum {
				return errors.New("published Gomap checksum differs from the source go.sum")
			}
			matched = true
		}
	}
	if !matched {
		return errors.New("missing published Gomap content checksum in source go.sum")
	}
	actual, err := publishedSourceSum(m.Gomap.Directory, dep.Version)
	if err != nil {
		return err
	}
	if actual != dep.Sum {
		return errors.New("clean Gomap source module content differs from the server build checksum")
	}
	m.Version, m.GomapSum = 2, actual
	return nil
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
	dependency, err := gomapDependency(info.Deps)
	if err != nil {
		return m, err
	}
	if err := bindGomap(&m, dependency); err != nil {
		return m, err
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
	gomap := flag.String("gomap-source", "", "clean Gomap repository root matching local replacement or published module content")
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
