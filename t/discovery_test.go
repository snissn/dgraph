/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/dgraph/v25/testutil"
)

func TestListPackagesReturnsBeforeRunnerWork(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module github.com/dgraph-io/dgraph/v25/discoveryfixture\n\ngo 1.26\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fixture.go"), []byte("package discoveryfixture\n"), 0644); err != nil {
		t.Fatal(err)
	}
	oldList, oldBase, oldSuite, oldBuild, oldCompose := *listPackages, *baseDir, testsuite, testutil.BuildPlugins, ComposeFileArgs
	defer func() {
		*listPackages, *baseDir, testsuite, testutil.BuildPlugins, ComposeFileArgs = oldList, oldBase, oldSuite, oldBuild, oldCompose
	}()
	t.Chdir(dir)
	*listPackages, *baseDir, testsuite = true, ".", []string{"integration"}
	testutil.BuildPlugins = func(bool) { t.Fatal("discovery generated plugins") }
	ComposeFileArgs = func(string, string) []string { t.Fatal("discovery started/stopped cluster"); return nil }
	if err := run(); err != nil {
		t.Fatal(err)
	}
	for _, option := range []*bool{clear, rebuildBinary, race, runCoverage, keepCluster} {
		old := *option
		*option = true
		if err := run(); err == nil {
			t.Fatal("discovery admitted mutating flag")
		}
		*option = old
	}
	for _, option := range []*string{tmp, useExisting} {
		old := *option
		*option = "must-not-be-used"
		if err := run(); err == nil {
			t.Fatal("discovery admitted a runner workspace or cluster")
		}
		*option = old
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatalf("discovery created runner files: %v, %v", files, err)
	}
}
