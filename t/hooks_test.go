/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPublicNativeCacheOverlay(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "systest", "21million", "live", "docker-compose.yml")
	overlay := filepath.Join(base, "t", "ci-native", "21million-live-cache.yml")
	for _, value := range []string{"", "0", "true", "1"} {
		t.Setenv("DGRAPH_CI_PUBLIC_NATIVE", value)
		for _, path := range []string{target, filepath.Join(base, "systest", "21million", "bulk", "docker-compose.yml"), filepath.Join(base, "t", "docker-compose.yml")} {
			want := []string{"-f", path}
			if value == "1" && path == target {
				want = append(want, "-f", overlay)
			}
			if got := ComposeFileArgs(path, base); !reflect.DeepEqual(got, want) {
				t.Fatalf("profile %q, path %q: got %v, want %v", value, path, got, want)
			}
		}
	}
}

func TestPublicNativeCacheRenderedEnvironment(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Compose rendering requires the Docker CLI")
	}
	base, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, "systest", "21million", "live", "docker-compose.yml")
	t.Setenv("GOPATH", t.TempDir())
	t.Setenv("LINUX_GOBIN", t.TempDir())
	render := func(profile string) map[string]interface{} {
		t.Setenv("DGRAPH_CI_PUBLIC_NATIVE", profile)
		args := append([]string{"compose"}, ComposeFileArgs(path, base)...)
		args = append(args, "config", "--format", "json")
		out, err := exec.Command("docker", args...).Output()
		if err != nil {
			t.Fatalf("render profile %q: %v\n%s", profile, err, out)
		}
		var config map[string]interface{}
		if err := json.Unmarshal(out, &config); err != nil {
			t.Fatalf("decode profile %q: %v\n%s", profile, err, out)
		}
		return config
	}
	ordinary, public := render(""), render("1")
	alpha := public["services"].(map[string]interface{})["alpha1"].(map[string]interface{})
	env := alpha["environment"].(map[string]interface{})
	if env["DGRAPH_ALPHA_CACHE"] != "size-mb=1024;percentage=40,40,20;remove-on-update=false" {
		t.Fatalf("wrong Alpha cache environment: %v", env)
	}
	delete(alpha, "environment")
	if !reflect.DeepEqual(ordinary, public) {
		t.Fatalf("profile changed more than the Alpha cache environment: ordinary=%v public=%v", ordinary, public)
	}
}
