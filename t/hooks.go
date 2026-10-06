/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"os"
	"path/filepath"
)

// This file declares public extensibility hooks for the t test runner.
// See testutil/hooks.go for the full convention.

// EnvForCompose returns extra KEY=VALUE entries to inject into the
// environment of docker-compose subprocesses that t spawns. Default:
// nil. Forks may inject UID/GID so `${UID:-65532}` in generated compose
// files resolves to the host UID rather than the image's nonroot user.
var EnvForCompose = defaultEnvForCompose

// ComposeFileArgs returns the file-selector args docker-compose receives
// for the given compose-file path. The baseDir argument is the repo
// root (t's --base flag), passed explicitly so the hook shares no
// package state with t/t.go. Default: returns "-f <path>", except the
// explicit public native CI cache profile below.
//
// A fork may override this to layer additional `-f <overlay>` files,
// switch the compose-file path, or add `--project-directory <dir>` so
// relative bind-mount sources resolve against the pristine test package
// dir rather than wherever the override emits its base file.
var ComposeFileArgs = defaultComposeFileArgs

func defaultEnvForCompose() []string { return nil }

// The public native CI profile changes only this load fixture's Alpha cache.
// The workflow enables it only under the existing public-personal runner guard.
func defaultComposeFileArgs(path, baseDir string) []string {
	args := []string{"-f", path}
	if os.Getenv("DGRAPH_CI_PUBLIC_NATIVE") == "1" && filepath.Clean(path) ==
		filepath.Join(filepath.Clean(baseDir), "systest", "21million", "live", "docker-compose.yml") {
		args = append(args, "-f", filepath.Join(baseDir, "t", "ci-native", "21million-live-cache.yml"))
	}
	return args
}
