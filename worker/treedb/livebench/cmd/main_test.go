// SPDX-License-Identifier: Apache-2.0
package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureCPUProfileWritesImmutableArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("seconds"); got != "3" {
			t.Errorf("seconds=%q", got)
		}
		_, _ = w.Write([]byte("profile"))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	if err := captureCPUProfile(server.URL, path, 3); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "profile" {
		t.Fatalf("got %q err=%v", got, err)
	}
	if err := captureCPUProfile(server.URL, path, 3); !os.IsExist(err) {
		t.Fatalf("got %v", err)
	}
}
