// SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
)

func memoryTestResult() livebench.Result {
	r := livebench.Result{RunID: "treedb-durable-r1", Config: livebench.Config{Backend: "treedb", DurabilityClass: "durable"},
		Context: livebench.Context{BinarySHA256: strings.Repeat("a", 64)}, TimedFinished: time.Now().UTC()}
	markDiagnostics(&r, options{memoryDiagnostic: "memory"})
	return r
}

func TestMemoryDiagnosticPairsRawStateWithGCHeapAndRetainsHashes(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux /proc observer")
	}
	requests := make(chan string, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.RequestURI()
		if r.URL.Path == "/debug/vars" {
			_, _ = w.Write([]byte(`{"memstats":{"HeapAlloc":42}}`))
		} else {
			_, _ = w.Write([]byte("retained heap profile bytes"))
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	prefix, postings := filepath.Join(dir, "memory"), filepath.Join(dir, "run", "cluster", "p")
	for _, backend := range []string{"treedb", "badger"} {
		r := memoryTestResult()
		r.Config.Backend = backend
		boundary := memoryDiagnosticBoundaries[1]
		prefix := prefix + backend
		profiles, err := captureMemoryDiagnostic(prefix, boundary, postings, server.URL, os.Getpid(), r)
		if err != nil {
			t.Fatal(err)
		}
		var observed []string
		for len(requests) != 0 {
			observed = append(observed, <-requests)
		}
		if !reflect.DeepEqual(observed, []string{"/debug/vars", "/debug/pprof/heap?gc=1", "/debug/vars"}) {
			t.Fatalf("unexpected observation/GC order: %v", observed)
		}
		if len(profiles) != 1 || profiles[0] != memoryDiagnosticPath(prefix, boundary, "heap.gc1.pprof") {
			t.Fatalf("heap profile not forwarded: %v", profiles)
		}
		raw, err := os.ReadFile(memoryDiagnosticPath(prefix, boundary, "manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		var manifest memoryObservation
		if err := json.Unmarshal(raw, &manifest); err != nil {
			t.Fatal(err)
		}
		if !manifest.Complete || !manifest.DiagnosticOnly || !manifest.ForcedGCRequested || manifest.PID != os.Getpid() ||
			manifest.Backend != backend || manifest.BinarySHA256 != r.Context.BinarySHA256 || len(manifest.Artifacts) != 7 ||
			manifest.StartedAt.Before(r.TimedFinished) || manifest.FinishedAt.Before(manifest.StartedAt) {
			t.Fatalf("incomplete memory manifest: %+v", manifest)
		}
		previous := manifest.StartedAt
		for _, a := range manifest.Artifacts {
			bytes, err := os.ReadFile(a.Path)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(bytes)
			if len(bytes) != a.Bytes || hex.EncodeToString(hash[:]) != a.SHA256 || a.Source == "" ||
				a.StartedAt.Before(previous) || a.FinishedAt.Before(a.StartedAt) || a.FinishedAt.After(manifest.FinishedAt) {
				t.Fatalf("invalid retained observation: %+v", a)
			}
			previous = a.FinishedAt
		}
		if _, err := captureMemoryDiagnostic(prefix, boundary, postings, server.URL, os.Getpid(), r); err == nil {
			t.Fatal("existing observations overwritten")
		}
	}
}

func TestMemoryDiagnosticFailureRetainsPartialEnvelopeWithoutGCClaim(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux /proc observer")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer server.Close()
	dir := t.TempDir()
	prefix, boundary := filepath.Join(dir, "memory"), memoryDiagnosticBoundaries[0]
	profiles, err := captureMemoryDiagnostic(prefix, boundary, filepath.Join(dir, "run", "cluster", "p"), server.URL, os.Getpid(), memoryTestResult())
	if err == nil || len(profiles) != 0 {
		t.Fatal("failed raw state was accepted")
	}
	raw, err := os.ReadFile(memoryDiagnosticPath(prefix, boundary, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest memoryObservation
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Complete || manifest.ForcedGCRequested || manifest.Error == "" || len(manifest.Artifacts) != 2 {
		t.Fatalf("failure envelope lost: %+v", manifest)
	}
}

func TestMemoryDiagnosticPostGCFailureKeepsHeapForInspection(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux /proc observer")
	}
	vars := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/vars" {
			vars++
			if vars == 2 {
				_, _ = w.Write([]byte("malformed post-GC runtime state"))
				return
			}
			_, _ = w.Write([]byte(`{"memstats":{}}`))
			return
		}
		_, _ = w.Write([]byte("heap bytes retained before later failure"))
	}))
	defer server.Close()
	dir := t.TempDir()
	prefix, boundary := filepath.Join(dir, "memory"), memoryDiagnosticBoundaries[1]
	profiles, err := captureMemoryDiagnostic(prefix, boundary, filepath.Join(dir, "run", "cluster", "p"), server.URL, os.Getpid(), memoryTestResult())
	if err == nil || len(profiles) != 1 {
		t.Fatalf("later failure lost completed heap: profiles=%v err=%v", profiles, err)
	}
	raw, err := os.ReadFile(memoryDiagnosticPath(prefix, boundary, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest memoryObservation
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Complete || !manifest.ForcedGCRequested || manifest.Error == "" || len(manifest.Artifacts) != 7 {
		t.Fatalf("later failure envelope lost: %+v", manifest)
	}
}

func TestMemoryDiagnosticPreflightAndNativeBoundaryPlacement(t *testing.T) {
	dir := t.TempDir()
	o := options{artifactDir: filepath.Join(dir, "run"), memoryDiagnostic: filepath.Join(dir, "memory")}
	if err := validateMemoryDiagnosticOption(o); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*options){
		func(o *options) { o.cpuProfile = "profile" }, func(o *options) { o.operationDiagnostic = "operations" },
		func(o *options) { o.endpointDiagnostic = "endpoints" }, func(o *options) { o.storageDiagnostic = "storage" },
		func(o *options) { o.memoryDiagnostic = filepath.Join(o.artifactDir, "cluster", "w", "memory") },
	} {
		bad := o
		mutate(&bad)
		if err := validateMemoryDiagnosticOption(bad); err == nil {
			t.Fatalf("unsafe/confounded memory observation accepted: %+v", bad)
		}
	}
	if err := os.WriteFile(memoryDiagnosticPath(o.memoryDiagnostic, memoryDiagnosticBoundaries[1], "manifest.json"), []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateMemoryDiagnosticOption(o); err == nil {
		t.Fatal("existing second boundary artifact accepted before process launch")
	}
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	ordered := []string{"r.SetupFinished =", "captureMemoryDiagnostic(o.memoryDiagnostic, memoryDiagnosticBoundaries[0]", "cpuBefore, err := procCPU(",
		"r.TimedStarted =", "r.TimedFinished =", "cpuAfter, err := procCPU(", "hwm, err := procHWM(",
		"diskUsageObserved(filepath.Join(runDir, \"p\"), storage)", "r.Metrics = metrics(",
		"captureMemoryDiagnostic(o.memoryDiagnostic, memoryDiagnosticBoundaries[1]", "validateSchema(ctx, dg)"}
	previous := -1
	for _, needle := range ordered {
		index := strings.Index(s, needle)
		if index <= previous {
			t.Fatalf("memory hook/native counter order violated at %q", needle)
		}
		previous = index
	}
}
