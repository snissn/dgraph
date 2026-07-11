// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateReadResponseChecksPointAndOneHopContents(t *testing.T) {
	good, _ := json.Marshal(map[string]any{"q": []any{map[string]any{
		"uid": "0x1", "bench.value": "source", "bench.next": []any{map[string]any{"uid": "0x2", "bench.value": "target"}},
	}}})
	want := expectedNode{Value: "source", NextValue: "target"}
	if err := validateReadResponse(good, "0x1", want, true); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"wrong point value": `{"q":[{"uid":"0x1","bench.value":"wrong"}]}`,
		"missing one hop":   `{"q":[{"uid":"0x1","bench.value":"source"}]}`,
		"wrong one hop":     `{"q":[{"uid":"0x1","bench.value":"source","bench.next":[{"uid":"0x2","bench.value":"wrong"}]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			oneHop := name != "wrong point value"
			if err := validateReadResponse([]byte(raw), "0x1", want, oneHop); err == nil {
				t.Fatal("expected response validation failure")
			}
		})
	}
}

func TestCanonicalPostingRowIncludesAndValidatesEdgeTopology(t *testing.T) {
	want := expectedNode{Value: "source", NextValue: "target"}
	row, err := canonicalPostingRow(queryNode{UID: "0x1", Value: "source", Next: queryNodes{{UID: "0x2", Value: "target"}}}, want)
	if err != nil {
		t.Fatal(err)
	}
	if row != "source\x00target" {
		t.Fatalf("row=%q", row)
	}
	for _, tc := range []struct {
		name     string
		node     queryNode
		expected expectedNode
	}{
		{"missing expected edge", queryNode{UID: "0x1", Value: "source"}, want},
		{"wrong expected target", queryNode{UID: "0x1", Value: "source", Next: queryNodes{{UID: "0x2", Value: "wrong"}}}, want},
		{"multiple expected edges", queryNode{UID: "0x1", Value: "source", Next: queryNodes{{UID: "0x2", Value: "target"}, {UID: "0x3", Value: "other"}}}, want},
		{"unexpected edge without target value", queryNode{UID: "0x4", Value: "write", Next: queryNodes{{UID: "0x999"}}}, expectedNode{Value: "write"}},
		{"unexpected edge with target value", queryNode{UID: "0x4", Value: "write", Next: queryNodes{{UID: "0x2", Value: "target"}}}, expectedNode{Value: "write"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := canonicalPostingRow(tc.node, tc.expected); err == nil {
				t.Fatalf("expected edge failure for %+v", tc.node)
			}
		})
	}
	if row, err := canonicalPostingRow(queryNode{UID: "0x4", Value: "write"}, expectedNode{Value: "write"}); err != nil || row != "write\x00" {
		t.Fatalf("valid edge-free row rejected: row=%q err=%v", row, err)
	}
}

func TestValidateSchemaJSONRequiresExactPredicateTypes(t *testing.T) {
	good := []byte(`{"schema":[{"predicate":"bench.next","type":"uid"},{"predicate":"bench.value","type":"string"}]}`)
	if err := validateSchemaJSON(good); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"wrong value type":    `{"schema":[{"predicate":"bench.next","type":"uid"},{"predicate":"bench.value","type":"uid"}]}`,
		"wrong edge type":     `{"schema":[{"predicate":"bench.next","type":"string"},{"predicate":"bench.value","type":"string"}]}`,
		"missing predicate":   `{"schema":[{"predicate":"bench.value","type":"string"}]}`,
		"duplicate predicate": `{"schema":[{"predicate":"bench.next","type":"uid"},{"predicate":"bench.next","type":"uid"},{"predicate":"bench.value","type":"string"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSchemaJSON([]byte(raw)); err == nil {
				t.Fatal("expected schema validation failure")
			}
		})
	}
}

func TestFetchHonorsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	if _, err := fetch(server.URL, 10*time.Millisecond); err == nil {
		t.Fatal("timed-out fetch unexpectedly succeeded")
	}
}

func TestWaitHTTPBoundsEachReadinessProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	started := time.Now()
	if err := waitHTTP(context.Background(), server.URL, 50*time.Millisecond); err == nil || !strings.Contains(err.Error(), "readiness timeout") {
		t.Fatalf("waitHTTP error=%v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("readiness timeout took %s", elapsed)
	}
}

func TestWithDgraphRPCTimeoutCancelsStalledCall(t *testing.T) {
	started := time.Now()
	_, err := withDgraphRPCTimeout(context.Background(), 10*time.Millisecond, func(ctx context.Context) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled RPC error=%v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stalled RPC timeout took %s", elapsed)
	}
}

func TestValidatePostingRowsRejectsUnexpectedDuplicateAndOmittedNodes(t *testing.T) {
	expected := map[string]expectedNode{
		"0x1": {Value: "one"},
		"0x2": {Value: "two"},
	}
	valid := []queryNode{{UID: "0x1", Value: "one"}, {UID: "0x2", Value: "two"}}
	rows, err := validatePostingRows(valid, expected)
	if err != nil || len(rows) != len(expected) {
		t.Fatalf("valid rows rejected: rows=%v err=%v", rows, err)
	}

	tests := []struct {
		name  string
		nodes []queryNode
		want  string
	}{
		{"unexpected extra UID", append(append([]queryNode{}, valid...), queryNode{UID: "0x999", Value: "extra"}), "unexpected posting row for 0x999"},
		{"duplicate expected UID", append(append([]queryNode{}, valid...), valid[0]), "duplicate posting row for 0x1"},
		{"omitted expected UID", valid[:1], "omitted posting row for 0x2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := validatePostingRows(tc.nodes, expected); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
	if query := postingValidationQuery(len(expected)); !strings.Contains(query, "func: has(bench.value), first: 3") {
		t.Fatalf("validation query does not enumerate all benchmark nodes plus one: %s", query)
	}
}

func TestBadgerFlushMetricIsUnavailable(t *testing.T) {
	m := metrics("badger", 1, 2, 3, 4,
		map[string]float64{"badger_write_num_vlog": 1},
		map[string]float64{"badger_write_num_vlog": 10}, nil, nil)["flushes"]
	if m.Available || !strings.Contains(m.Reason, "not a semantic flush counter") {
		t.Fatalf("flush metric = %+v", m)
	}
}

func TestUnsupportedOKRequiresExactExpectedTokenSet(t *testing.T) {
	want := "backup,export,import,restore,encryption,in_memory,ttl,badger_subscribe,sort,count,inequality"
	for _, tokens := range []string{want, "inequality,count,sort,badger_subscribe,ttl,in_memory,encryption,restore,import,export,backup"} {
		if !unsupportedOK("treedb", map[string]string{"unsupported": tokens}) {
			t.Fatalf("exact TreeDB unsupported set rejected: %q", tokens)
		}
	}
	for name, tokens := range map[string]string{
		"missing": "backup,import,restore,encryption,in_memory,ttl,badger_subscribe,sort,count,inequality",
		"spoofed": "notbackup,export,import,restore,encryption,in_memory,ttl,badger_subscribe,sort,count,inequality",
		"extra":   want + ",future",
	} {
		t.Run(name, func(t *testing.T) {
			if unsupportedOK("treedb", map[string]string{"unsupported": tokens}) {
				t.Fatalf("invalid unsupported set accepted: %q", tokens)
			}
		})
	}
	if !unsupportedOK("badger", map[string]string{"unsupported": ""}) || unsupportedOK("badger", map[string]string{"unsupported": "backup"}) {
		t.Fatal("Badger unsupported set must be exactly empty")
	}
	if unsupportedOK("other", map[string]string{"unsupported": want}) {
		t.Fatal("unknown backend accepted")
	}
}

func TestRecordExpectedWriteRejectsMissingAndDuplicateUID(t *testing.T) {
	writes := map[string]expectedNode{}
	if err := recordExpectedWrite(writes, "", "value"); err == nil || !strings.Contains(err.Error(), "omitted uid") {
		t.Fatalf("missing UID got %v", err)
	}
	if err := recordExpectedWrite(writes, "0x1", "first"); err != nil {
		t.Fatal(err)
	}
	if err := recordExpectedWrite(writes, "0x1", "second"); err == nil || !strings.Contains(err.Error(), "duplicate write uid") {
		t.Fatalf("duplicate UID got %v", err)
	}
	if got := writes["0x1"].Value; got != "first" {
		t.Fatalf("duplicate overwrote first oracle value: %q", got)
	}
}

func TestCoreCollectorsReturnErrorsInsteadOfSyntheticValues(t *testing.T) {
	if _, err := procCPU(-1); err == nil {
		t.Fatal("procCPU accepted missing process")
	}
	if _, err := procHWM(-1); err == nil {
		t.Fatal("procHWM accepted missing process")
	}
	if _, _, err := diskUsage(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("diskUsage accepted missing posting directory")
	}
}

func TestRunRejectsNonPositiveWorkloadOptionsBeforeSetup(t *testing.T) {
	base := options{
		dgraphBin: "/does/not/exist", artifactDir: filepath.Join(t.TempDir(), "new"),
		backend: "badger", class: "relaxed", repeat: 1, dataset: 1,
		concurrency: 1, warmup: 1, timed: 1, profileSeconds: 1,
	}
	tests := []struct {
		name   string
		mutate func(*options)
	}{
		{"repeat", func(o *options) { o.repeat = 0 }},
		{"dataset", func(o *options) { o.dataset = 0 }},
		{"concurrency", func(o *options) { o.concurrency = -1 }},
		{"warmup", func(o *options) { o.warmup = 0 }},
		{"timed", func(o *options) { o.timed = -1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := base
			o.artifactDir = filepath.Join(t.TempDir(), "new")
			tc.mutate(&o)
			if err := run(o); err == nil || !strings.Contains(err.Error(), "must be positive") {
				t.Fatalf("got %v, want positive-option error", err)
			}
			if _, err := os.Stat(o.artifactDir); !os.IsNotExist(err) {
				t.Fatalf("invalid options created artifact directory: %v", err)
			}
		})
	}
}

func TestRunRejectsNonPositiveProfileSecondsBeforeSetup(t *testing.T) {
	o := options{
		dgraphBin: "/does/not/exist", artifactDir: filepath.Join(t.TempDir(), "new"),
		backend: "treedb", class: "relaxed", repeat: 1, dataset: 1,
		concurrency: 1, warmup: 1, timed: 1, cpuProfile: "cpu.pprof",
	}
	if err := run(o); err == nil || !strings.Contains(err.Error(), "--profile-seconds must be positive") {
		t.Fatalf("got %v, want positive profile duration error", err)
	}
	if _, err := os.Stat(o.artifactDir); !os.IsNotExist(err) {
		t.Fatalf("invalid profile options created artifact directory: %v", err)
	}
}

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
