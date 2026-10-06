// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dgo "github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestTreeDBDiagnosticMetricsPreserveDeltasAndHighWaterGauges(t *testing.T) {
	before := map[string]float64{
		"treedb.command_wal.group_commit.commits_total":  10,
		"treedb.command_wal.group_commit.group_size_max": 2,
		"treedb.cache.point_successor.calls_total":       20,
	}
	after := map[string]float64{
		"treedb.command_wal.group_commit.commits_total":  17,
		"treedb.command_wal.group_commit.group_size_max": 4,
		"treedb.cache.point_successor.calls_total":       25,
	}
	got := metrics("treedb", 1, 2, 3, 4, nil, nil, before, after)
	if metric := got["treedb_group_commit_commits"]; !metric.Available || metric.Value != 7 || !strings.Contains(metric.Source, "timed-phase delta") {
		t.Fatalf("group commits = %+v", metric)
	}
	if metric := got["treedb_group_commit_group_size_max"]; !metric.Available || metric.Value != 4 || !strings.Contains(metric.Source, "process-lifetime high-water") {
		t.Fatalf("group size high-water = %+v", metric)
	}
	if metric := got["treedb_point_successor_calls"]; !metric.Available || metric.Value != 5 {
		t.Fatalf("point successor calls = %+v", metric)
	}
	if metric := got["treedb_value_log_syncs"]; metric.Available || metric.Reason == "" {
		t.Fatalf("missing TreeDB diagnostic did not fail closed: %+v", metric)
	}
}

func TestTreeDBLogicalWriteBytesFailClosedWhenDirectPointRouteIsActive(t *testing.T) {
	before := map[string]float64{
		"treedb.command_wal.public_batch.set.bytes_total": 10,
		"treedb.command_wal.append.point.count_total":     5,
	}
	after := map[string]float64{
		"treedb.command_wal.public_batch.set.bytes_total": 20,
		"treedb.command_wal.append.point.count_total":     6,
	}
	got := metrics("treedb", 1, 2, 3, 4, nil, nil, before, after)
	if metric := got["write_bytes"]; metric.Available || !strings.Contains(metric.Reason, "direct-point") {
		t.Fatalf("logical write bytes = %+v", metric)
	}
	if metric := got["treedb_command_wal_append_point_calls"]; !metric.Available || metric.Value != 1 {
		t.Fatalf("point append calls = %+v", metric)
	}
}

func TestBadgerTreeDBDiagnosticsAreExplicitlyUnavailable(t *testing.T) {
	got := metrics("badger", 1, 2, 3, 4, nil, nil, nil, nil)
	for _, diagnostic := range treeDBDiagnostics {
		metric := got[diagnostic.metric]
		if metric.Available || metric.Reason != "TreeDB-only diagnostic" {
			t.Fatalf("%s = %+v", diagnostic.metric, metric)
		}
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

func TestEndpointDiagnosticCapturesOrderedFilesAndStatusWithoutOverwrite(t *testing.T) {
	base := t.TempDir()
	postings := filepath.Join(base, "cluster", "p")
	if err := os.MkdirAll(filepath.Join(postings, "maindb"), 0o755); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(postings, "maindb", "index.db")
	if err := os.WriteFile(index, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "wal"), make([]byte, 9999), 0o644); err != nil {
		t.Fatal(err)
	}
	// Walk/stat accounting follows diskUsage: no symlink targets or files outside p.
	if err := os.Symlink(filepath.Join(base, "wal"), filepath.Join(postings, "link")); err != nil {
		t.Fatal(err)
	}
	initialLogical, initialAllocated, err := diskUsage(postings)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/debug/store" {
			t.Errorf("unexpected observation request: %s", r.URL.Path)
		}
		count := calls.Add(1)
		if count == 1 {
			// This occurs after the first file observation: a background publication
			// changes both the index and the set of files before the second cut.
			if err := os.WriteFile(index, make([]byte, 8192), 0o644); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(filepath.Join(postings, "maindb", "value.log"), []byte("persistent"), 0o644); err != nil {
				t.Error(err)
			}
		}
		fmt.Fprintf(w, "status=map[backend:treedb profile:command_wal_relaxed] stats=map[treedb.cache.flush.flushes:%d treedb.cache.iterator.sources_total:%d]", count, count*3)
	}))
	defer server.Close()
	path := filepath.Join(base, "endpoints.json")
	// A past target avoids a five-second unit-test sleep while retaining the
	// exact +5s deadline calculation and observation ordering.
	finished := time.Now().UTC().Add(-10 * time.Second)
	if err := captureEndpointDiagnostic(context.Background(), path, postings, server.URL, "test-run", finished); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got endpointDiagnostic
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || !got.DiagnosticOnly || got.RunID != "test-run" ||
		got.PostingDir != postings || !got.TimedFinished.Equal(finished) || got.QuiescenceSeconds != 5 || len(got.Observations) != 2 || calls.Load() != 2 {
		t.Fatalf("unexpected sidecar header/cuts: %+v calls=%d", got, calls.Load())
	}
	first, second := got.Observations[0], got.Observations[1]
	if first.Boundary != "timed_end" || second.Boundary != "quiescent" ||
		!first.TargetAt.Equal(finished) || !second.TargetAt.Equal(finished.Add(5*time.Second)) ||
		first.FinishedAt.After(second.StartedAt) {
		t.Fatalf("wrong endpoint order/targets: %+v", got.Observations)
	}
	for _, observation := range got.Observations {
		if observation.StartedAt.After(observation.FilesFinishedAt) ||
			observation.FilesFinishedAt.After(observation.StatusStartedAt) ||
			observation.StatusStartedAt.After(observation.FinishedAt) ||
			observation.StartDelayNS != observation.StartedAt.Sub(observation.TargetAt).Nanoseconds() ||
			observation.EndDelayNS != observation.FinishedAt.Sub(observation.TargetAt).Nanoseconds() {
			t.Fatalf("inconsistent observation timestamps: %+v", observation)
		}
		var logical, allocated int64
		for _, file := range observation.Files {
			if filepath.IsAbs(file.Path) || strings.HasPrefix(file.Path, "../") || file.Path == "link" ||
				file.AllocatedBytes != file.StatBlocks*512 || file.ModifiedAt.IsZero() {
				t.Fatalf("wrong filename/stat accounting: %+v", file)
			}
			logical += file.LogicalBytes
			allocated += file.AllocatedBytes
		}
		if logical != observation.LogicalBytes || allocated != observation.AllocatedBytes || observation.Status["backend"] != "treedb" {
			t.Fatalf("wrong totals/status: %+v", observation)
		}
	}
	if len(first.Files) != 1 || first.Files[0].Path != "maindb/index.db" ||
		float64(first.LogicalBytes) != initialLogical || float64(first.AllocatedBytes) != initialAllocated ||
		first.Counters["treedb.cache.flush.flushes"] != 1 || second.Counters["treedb.cache.iterator.sources_total"] != 6 {
		t.Fatalf("first boundary changed or counters lost: %+v", got.Observations)
	}
	finalLogical, finalAllocated, err := diskUsage(postings)
	if err != nil || len(second.Files) != 2 || float64(second.LogicalBytes) != finalLogical || float64(second.AllocatedBytes) != finalAllocated {
		t.Fatalf("second boundary accounting: %+v disk=%f/%f err=%v", second, finalLogical, finalAllocated, err)
	}
	if err := captureEndpointDiagnostic(context.Background(), path, postings, server.URL, "overwrite", finished); err == nil {
		t.Fatal("existing sidecar was overwritten")
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != string(raw) {
		t.Fatalf("immutable sidecar changed: %v", err)
	}
}

func TestEndpointDiagnosticRejectsMissingProfileAndCollidingPaths(t *testing.T) {
	base := t.TempDir()
	o := options{artifactDir: filepath.Join(base, "run"), backend: "treedb", endpointDiagnostic: filepath.Join(base, "endpoint.json")}
	if err := validateEndpointDiagnostic(o); err == nil {
		t.Fatal("endpoint observation without CPU-profile diagnostic accepted")
	}
	o.cpuProfile = filepath.Join(base, "profile.pprof")
	if err := validateEndpointDiagnostic(o); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{o.cpuProfile, filepath.Join(o.artifactDir, "result.json"), filepath.Join(o.artifactDir, "cluster", "p", "observed.json")} {
		o.endpointDiagnostic = path
		if err := validateEndpointDiagnostic(o); err == nil {
			t.Fatalf("colliding/self-accounted path accepted: %s", path)
		}
	}
}

type operationTestServer struct {
	api.UnimplementedDgraphServer
	query func(context.Context, *api.Request) (*api.Response, error)
}

func (s *operationTestServer) CheckVersion(context.Context, *api.Check) (*api.Version, error) {
	return &api.Version{}, nil
}
func (s *operationTestServer) Query(ctx context.Context, req *api.Request) (*api.Response, error) {
	return s.query(ctx, req)
}

func operationClient(t *testing.T, query func(context.Context, *api.Request) (*api.Response, error)) *dgo.Dgraph {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	api.RegisterDgraphServer(server, &operationTestServer{query: query})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dg, err := client(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dg.Close)
	return dg
}

func TestOperationDiagnosticRecordsIdentityPhasesAndFullWall(t *testing.T) {
	var writes atomic.Int64
	dg := operationClient(t, func(ctx context.Context, req *api.Request) (*api.Response, error) {
		resp := &api.Response{Txn: &api.TxnContext{StartTs: 1},
			Latency: &api.Latency{TotalNs: 11, AssignTimestampNs: 2, ParsingNs: 3, ProcessingNs: 4, EncodingNs: 5}}
		if len(req.Mutations) != 0 {
			resp.Uids = map[string]string{"w": fmt.Sprintf("0x%x", writes.Add(1)+10)}
		} else if strings.Contains(req.Query, "bench.next") {
			resp.Json = []byte(`{"q":[{"uid":"0x1","bench.value":"source","bench.next":[{"uid":"0x2","bench.value":"target"}]}]}`)
		} else {
			resp.Json = []byte(`{"q":[{"uid":"0x1","bench.value":"source"}]}`)
		}
		return resp, nil
	})
	rows := make([]operationRow, 100)
	values, expected, err := exerciseWithOperations(context.Background(), dg,
		map[string]expectedNode{"0x1": {Value: "source", NextValue: "target"}}, 100, 4, 0, 1000, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 100 || len(expected) != 20 {
		t.Fatalf("values=%d writes=%d", len(values), len(expected))
	}
	kinds := map[string]int{}
	// Channel completion order is independent of per-index diagnostic storage.
	wantMS := make([]float64, len(rows))
	for i, row := range rows {
		wantKind := "write"
		kind := i * 37 % 100
		if kind < 60 {
			wantKind = "point_read"
		} else if kind < 80 {
			wantKind = "one_hop_read"
		}
		if row.Index != i || row.Worker != i%4 || row.Kind != wantKind || row.Outcome != "ok" {
			t.Fatalf("identity row %d: %+v", i, row)
		}
		if row.StartedAt.IsZero() || row.RPCFinishedAt.Before(row.StartedAt) || row.FinishedAt.Before(row.RPCFinishedAt) ||
			row.RPCNS < 0 || row.WallNS < row.RPCNS ||
			row.RPCFinishedAt.Sub(row.StartedAt).Nanoseconds() != row.RPCNS ||
			row.FinishedAt.Sub(row.StartedAt).Nanoseconds() != row.WallNS {
			t.Fatalf("timing row %d: %+v", i, row)
		}
		if !row.ServerLatency.Available || row.ServerLatency.TotalNS != 11 || row.ServerLatency.AssignTimestampNS != 2 ||
			row.ServerLatency.ParsingNS != 3 || row.ServerLatency.ProcessingNS != 4 || row.ServerLatency.EncodingNS != 5 {
			t.Fatalf("latency row %d: %+v", i, row.ServerLatency)
		}
		kinds[row.Kind]++
		wantMS[i] = float64(row.WallNS/1000) / 1000
	}
	if kinds["point_read"] != 60 || kinds["one_hop_read"] != 20 || kinds["write"] != 20 {
		t.Fatal(kinds)
	}
	sort.Float64s(values)
	sort.Float64s(wantMS)
	for i := range values {
		if values[i] != wantMS[i] {
			t.Fatalf("original ms producer changed: got=%v want=%v", values[i], wantMS[i])
		}
	}
}

func TestOperationDiagnosticDrainsFailureAndSeparatesRPCFromValidation(t *testing.T) {
	var calls atomic.Int64
	dg := operationClient(t, func(ctx context.Context, req *api.Request) (*api.Response, error) {
		if calls.Add(1) == 1 {
			// Successful RPC, expensive client decoding, then a semantic mismatch.
			return &api.Response{Txn: &api.TxnContext{StartTs: 1}, Latency: &api.Latency{TotalNs: 7},
				Json: []byte(strings.Repeat(" ", 1<<20) + `{"q":[{"uid":"0x1","bench.value":"wrong"}]}`)}, nil
		}
		return nil, status.Error(codes.Unavailable, "test RPC failure")
	})
	rows := make([]operationRow, 10)
	started := time.Now()
	_, _, err := exerciseWithOperations(context.Background(), dg, map[string]expectedNode{"0x1": {Value: "source"}}, 10, 1, 0, 1000, rows)
	if err == nil || !strings.Contains(err.Error(), "point read mismatch") {
		t.Fatalf("err=%v", err)
	}
	row := rows[0]
	if row.Outcome != "validation_error" || !row.ServerLatency.Available || row.ServerLatency.TotalNS != 7 ||
		row.WallNS <= row.RPCNS || !row.FinishedAt.After(row.RPCFinishedAt) {
		t.Fatalf("validation boundary: %+v", row)
	}
	for i, row := range rows {
		if row.StartedAt.IsZero() || row.FinishedAt.IsZero() || row.Index != i {
			t.Fatalf("not drained: row %d %+v", i, row)
		}
		if i > 0 && (row.Outcome != "rpc_error" || row.ServerLatency.Available) {
			t.Fatalf("failure row %d %+v", i, row)
		}
	}
	d := operationDiagnostic{SchemaVersion: 1, DiagnosticOnly: true, ExpectedOperations: 10,
		Concurrency: 1, TimedStarted: started, TimedFinished: time.Now(), Rows: rows}
	path := filepath.Join(t.TempDir(), "failed-operations.json")
	if err := writeOperationDiagnostic(path, &d, filepath.Join(t.TempDir(), "cluster", "p")); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got operationDiagnostic
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.WorkloadSucceeded || len(got.Rows) != 10 || !got.After.Store.StartedAt.IsZero() {
		t.Fatalf("failure sidecar: %+v", got)
	}
}

func TestOperationDiagnosticRetainsLabelledBoundariesAndWritesAfterTimedFinish(t *testing.T) {
	var requests atomic.Int64
	rawStore := "status=map[backend:badger profile:durable] stats=map[one:1]\n"
	rawProm := "dgraph_latency_bucket{method=\"query\",status=\"ok\",le=\"1\"} 3\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/debug/store":
			fmt.Fprint(w, rawStore)
		case "/debug/prometheus_metrics":
			fmt.Fprint(w, rawProm)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	d := operationDiagnostic{SchemaVersion: 1, DiagnosticOnly: true, RunID: "badger-durable-r1", ExpectedOperations: 1, Concurrency: 1, WorkloadSucceeded: true}
	if _, _, err := storeStatusObserved(server.URL, &d.Before.Store); err != nil {
		t.Fatal(err)
	}
	if _, err := prometheusObserved(server.URL+"/debug/prometheus_metrics", &d.Before.Prometheus); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || d.Before.Store.Body != rawStore || d.Before.Prometheus.Body != rawProm {
		t.Fatalf("boundary: %+v", d.Before)
	}
	if d.Before.Store.FinishedAt.Before(d.Before.Store.StartedAt) || d.Before.Prometheus.FinishedAt.Before(d.Before.Prometheus.StartedAt) {
		t.Fatal("boundary timestamps")
	}
	start := time.Now().Add(-time.Second)
	d.TimedStarted = start
	d.TimedFinished = time.Now()
	d.Rows = []operationRow{{Index: 0, Worker: 0, Kind: "point_read", Outcome: "ok", StartedAt: start, RPCFinishedAt: start.Add(time.Millisecond), FinishedAt: start.Add(2 * time.Millisecond), RPCNS: int64(time.Millisecond), WallNS: int64(2 * time.Millisecond)}}
	path := filepath.Join(t.TempDir(), "operations.json")
	if err := writeOperationDiagnostic(path, &d, filepath.Join(t.TempDir(), "cluster", "p")); err != nil {
		t.Fatal(err)
	}
	if d.WriteStartedAt.Before(d.TimedFinished) {
		t.Fatal("serialized inside timed-work boundary")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got operationDiagnostic
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 1 || got.Before.Prometheus.Body != rawProm || got.WriteStartedAt.Before(got.TimedFinished) {
		t.Fatalf("sidecar: %+v", got)
	}
	if err := writeOperationDiagnostic(path, &d, filepath.Join(t.TempDir(), "cluster", "p")); !os.IsExist(err) {
		t.Fatalf("overwrite err=%v", err)
	}
	again, _ := os.ReadFile(path)
	if string(b) != string(again) {
		t.Fatal("immutable sidecar overwritten")
	}
	d.TimedFinished = start
	if err := writeOperationDiagnostic(filepath.Join(t.TempDir(), "invalid.json"), &d, filepath.Join(t.TempDir(), "cluster", "p")); err == nil {
		t.Fatal("accepted rows beyond timed finish")
	}
}

func TestOperationDiagnosticMarkerAndPathGuard(t *testing.T) {
	r := livebench.Result{}
	markDiagnostics(&r, options{operationDiagnostic: "operations.json"})
	if !r.Context.Excluded || len(r.Context.Contaminants) != 1 || !strings.Contains(r.Context.ExclusionReason, "diagnostic") || len(r.Context.Profiles) != 0 {
		t.Fatalf("marker %+v", r.Context)
	}
	dir := t.TempDir()
	o := options{artifactDir: filepath.Join(dir, "native"), backend: "treedb", endpointDiagnostic: filepath.Join(dir, "endpoints.json")}
	if err := validateEndpointDiagnostic(o); err == nil {
		t.Fatal("unmarked endpoint allowed without CPU")
	}
	o.operationDiagnostic = filepath.Join(dir, "operations.json")
	if err := validateEndpointDiagnostic(o); err != nil {
		t.Fatal(err)
	}
	if err := validateOperationDiagnostic(o); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{o.endpointDiagnostic, filepath.Join(o.artifactDir, "result.json"), filepath.Join(o.artifactDir, "cluster", "p", "bad.json")} {
		o.operationDiagnostic = path
		if err := validateOperationDiagnostic(o); err == nil {
			t.Fatalf("collision %s", path)
		}
	}
}

func TestOperationDiagnosticAbsentServerLatencyAndNativePath(t *testing.T) {
	dg := operationClient(t, func(context.Context, *api.Request) (*api.Response, error) {
		return &api.Response{Txn: &api.TxnContext{StartTs: 1}, Json: []byte(`{"q":[{"uid":"0x1","bench.value":"source"}]}`)}, nil
	})
	dataset := map[string]expectedNode{"0x1": {Value: "source"}}
	rows := make([]operationRow, 1)
	values, _, err := exerciseWithOperations(context.Background(), dg, dataset, 1, 1, 0, 1000, rows)
	if err != nil || len(values) != 1 || rows[0].ServerLatency.Available || rows[0].Outcome != "ok" {
		t.Fatalf("optional latency rows=%+v err=%v", rows, err)
	}
	values, _, err = exercise(context.Background(), dg, dataset, 1, 1, 0, 1000)
	if err != nil || len(values) != 1 {
		t.Fatalf("native path values=%v err=%v", values, err)
	}
}

func storageTestResult(root string) livebench.Result {
	return livebench.Result{RunID: "treedb-relaxed-r1", Config: livebench.Config{Backend: "treedb", DurabilityClass: "relaxed"},
		TimedFinished: time.Now().UTC().Add(-time.Second), Context: livebench.Context{RawPath: filepath.Join(filepath.Dir(filepath.Dir(root)), "result.json"), Excluded: true, ExclusionReason: storageDiagnosticReason}, Metrics: map[string]livebench.Metric{}}
}
func TestStorageDiagnosticSameWalkAndDefaultOff(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cluster", "p")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "index.db")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	old, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	d := newStorageDiagnostic(root, storageTestResult(root))
	calls := 0
	walk := func(root string, fn filepath.WalkFunc) error {
		calls++
		return filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if p == path && err == nil {
				if e := os.WriteFile(path, make([]byte, 8192), 0644); e != nil {
					return e
				}
			}
			return fn(p, info, err)
		})
	}
	logical, allocated, err := diskUsageWalk(root, d, walk)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || logical != float64(old.Size()) || len(d.Files) != 1 || d.Files[0].LogicalBytes != old.Size() || d.LogicalBytes != logical || d.AllocatedBytes != allocated {
		t.Fatalf("same-pass capture: calls=%d totals=%v/%v d=%+v", calls, logical, allocated, d)
	}
	r := storageTestResult(root)
	r.TimedFinished = d.TimedFinished
	r.Metrics = metrics("treedb", 0, 0, logical, allocated, nil, nil, nil, nil)
	sidecar := filepath.Join(filepath.Dir(filepath.Dir(root)), "storage.json")
	if err := writeStorageDiagnostic(sidecar, d, r); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	var saved storageDiagnostic
	if err := json.Unmarshal(b, &saved); err != nil {
		t.Fatal(err)
	}
	if err := validateStorageDiagnostic(&saved, r); err != nil {
		t.Fatal(err)
	}
	if err := writeStorageDiagnostic(sidecar, d, r); err == nil {
		t.Fatal("overwrite accepted")
	}
	l, a, err := diskUsage(root)
	if err != nil || l != 8192 || a < 0 {
		t.Fatalf("default walk: %v %v %v", l, a, err)
	}
	if err := validateStorageDiagnosticOption(options{}); err != nil {
		t.Fatal(err)
	}
}
func TestStorageDiagnosticRejectsInvalidAndRetainsPartial(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cluster", "p")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "index.db")
	if err := os.WriteFile(file, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "empty"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	r := storageTestResult(root)
	d := newStorageDiagnostic(root, r)
	l, a, err := diskUsageObserved(root, d)
	if err != nil {
		t.Fatal(err)
	}
	r.Metrics = metrics("treedb", 0, 0, l, a, nil, nil, nil, nil)
	d.WriteStartedAt = time.Now().UTC()
	raw, _ := json.Marshal(d)
	for name, mutate := range map[string]func(*storageDiagnostic){
		"missing rows": func(x *storageDiagnostic) { x.Files = nil }, "missing identity": func(x *storageDiagnostic) { x.RunID = "" },
		"wrong boundary": func(x *storageDiagnostic) { x.Boundary = "after_restart" }, "wrong posting dir": func(x *storageDiagnostic) { x.PostingDir = filepath.Dir(root) },
		"wrong finish":       func(x *storageDiagnostic) { x.TimedFinished = x.TimedFinished.Add(time.Second) },
		"walk before finish": func(x *storageDiagnostic) { x.WalkStartedAt = x.TimedFinished.Add(-time.Second) },
		"inverted walk":      func(x *storageDiagnostic) { x.WalkFinishedAt = x.WalkStartedAt.Add(-time.Second) },
		"write before walk":  func(x *storageDiagnostic) { x.WriteStartedAt = x.WalkStartedAt.Add(-time.Second) },
		"logical sum":        func(x *storageDiagnostic) { x.LogicalBytes++ }, "allocated sum": func(x *storageDiagnostic) { x.AllocatedBytes += 512 },
		"blocks mismatch": func(x *storageDiagnostic) { x.Files[0].StatBlocks++ }, "negative size": func(x *storageDiagnostic) { x.Files[0].LogicalBytes = -1 },
		"overflow blocks": func(x *storageDiagnostic) { x.Files[0].StatBlocks = 1 << 62 }, "escape path": func(x *storageDiagnostic) { x.Files[0].Path = "../index.db" },
		"duplicate path": func(x *storageDiagnostic) { x.Files = append(x.Files, x.Files[0]); x.RegularFiles++ }, "missing completion": func(x *storageDiagnostic) { x.WalkComplete = false },
		"missing count":  func(x *storageDiagnostic) { x.RegularFiles = 0 },
		"retained error": func(x *storageDiagnostic) { x.Error = "failed" },
	} {
		t.Run(name, func(t *testing.T) {
			var x storageDiagnostic
			if err := json.Unmarshal(raw, &x); err != nil {
				t.Fatal(err)
			}
			mutate(&x)
			if err := validateStorageDiagnostic(&x, r); err == nil {
				t.Fatal("invalid sidecar accepted")
			}
		})
	}
	var missing storageDiagnostic
	if err := json.Unmarshal(raw, &missing); err != nil {
		t.Fatal(err)
	}
	missing.Files = missing.Files[1:] // empty sorts first; both sums stay unchanged.
	if err := validateStorageDiagnostic(&missing, r); err == nil || !strings.Contains(err.Error(), "file count mismatch") {
		t.Fatalf("missing zero-byte row was not refused by the count witness: %v", err)
	}
	wrong := r
	wrong.Context.Excluded = false
	if err := validateStorageDiagnostic(d, wrong); err == nil {
		t.Fatal("acceptance result accepted")
	}
	wrong = r
	wrong.Metrics = metrics("treedb", 0, 0, l+1, a, nil, nil, nil, nil)
	if err := validateStorageDiagnostic(d, wrong); err == nil {
		t.Fatal("native total mismatch accepted")
	}
	sentinel := errors.New("injected interrupted walk")
	partial := newStorageDiagnostic(root, r)
	_, _, err = diskUsageWalk(root, partial, func(_ string, fn filepath.WalkFunc) error {
		info, e := os.Stat(file)
		if e != nil {
			return e
		}
		if e = fn(file, info, nil); e != nil {
			return e
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) || partial.WalkComplete || len(partial.Files) != 1 {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}
	out := filepath.Join(filepath.Dir(filepath.Dir(root)), "partial.json")
	if err := writeStorageDiagnostic(out, partial, r); err == nil {
		t.Fatal("partial walk accepted")
	}
	if b, err := os.ReadFile(out); err != nil || !strings.Contains(string(b), sentinel.Error()) {
		t.Fatalf("partial raw not retained: %s %v", b, err)
	}
	if err := validateStorageDiagnostic(partial, r); err == nil {
		t.Fatal("retained partial accepted")
	}
}
func TestStorageDiagnosticOptionAndOriginalPosition(t *testing.T) {
	base := t.TempDir()
	o := options{artifactDir: filepath.Join(base, "run"), backend: "treedb", storageDiagnostic: filepath.Join(base, "storage.json")}
	if err := validateStorageDiagnosticOption(o); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*options){"endpoint wait": func(x *options) { x.endpointDiagnostic = filepath.Join(base, "end.json") },
		"CPU wait": func(x *options) { x.cpuProfile = filepath.Join(base, "cpu.pprof") }, "posting pollution": func(x *options) { x.storageDiagnostic = filepath.Join(x.artifactDir, "cluster", "p", "diag.json") },
		"native collision": func(x *options) { x.storageDiagnostic = filepath.Join(x.artifactDir, "result.json") }, "operation collision": func(x *options) { x.operationDiagnostic = x.storageDiagnostic },
	} {
		t.Run(name, func(t *testing.T) {
			x := o
			mutate(&x)
			if err := validateStorageDiagnosticOption(x); err == nil {
				t.Fatal("invalid option accepted")
			}
		})
	}
	if err := os.WriteFile(o.storageDiagnostic, []byte("preserve"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := validateStorageDiagnosticOption(o); err == nil {
		t.Fatal("existing sidecar accepted")
	}
	r := livebench.Result{}
	markDiagnostics(&r, options{storageDiagnostic: "storage.json"})
	if !r.Context.Excluded || !strings.Contains(r.Context.ExclusionReason, storageDiagnosticReason) {
		t.Fatal("diagnostic not excluded")
	}
	b, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	start := strings.Index(s, "func run(o options)")
	end := strings.Index(s[start:], "func selectors(")
	run := s[start : start+end]
	walk := strings.Index(run, "diskUsageObserved(filepath.Join(runDir, \"p\"), storage)")
	before := strings.Index(run, "hwm, err := procHWM(")
	after := strings.Index(run, "promAfter, promErr := prometheusObserved(")
	validation := strings.Index(run, "if err := validateSchema(")
	write := strings.LastIndex(run, "writeStorageDiagnostic(o.storageDiagnostic, storage, r)")
	if before < 0 || walk < before || after < walk || write < after || validation < write {
		t.Fatal("storage moved from native prevalidation/restart position")
	}
}

func TestStorageDiagnosticPreflightRefusesBeforeNativeProcesses(t *testing.T) {
	root := t.TempDir()
	for _, backend := range []string{"badger", "treedb"} {
		o := options{dgraphBin: "/must-not-run-dgraph", artifactDir: filepath.Join(root, backend), backend: backend, class: "relaxed", repeat: 1, dataset: 1, concurrency: 1, warmup: 1, timed: 1, storageDiagnostic: filepath.Join(root, backend, "cluster", "p", "storage.json")}
		err := run(o)
		if err == nil || !strings.Contains(err.Error(), "outside the posting directory") {
			t.Fatalf("preflight error=%v", err)
		}
		if _, err := os.Stat(o.artifactDir); !os.IsNotExist(err) {
			t.Fatalf("preflight created artifacts: %v", err)
		}
	}
	o := options{artifactDir: filepath.Join(root, "run"), backend: "badger", storageDiagnostic: filepath.Join(root, "storage.json"), operationDiagnostic: filepath.Join(root, "operations.json")}
	if err := validateStorageDiagnosticOption(o); err != nil {
		t.Fatal(err)
	}
	if err := validateOperationDiagnostic(o); err != nil {
		t.Fatal(err)
	}
	o.operationDiagnostic = filepath.Join(root, "child", "..", "storage.json")
	if err := validateOperationDiagnostic(o); err == nil {
		t.Fatal("canonical sidecar collision accepted")
	}
	if err := validateStorageDiagnostic(nil, livebench.Result{}); err == nil {
		t.Fatal("missing diagnostic accepted")
	}
}

func TestDiagnosticPathAliases(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(real, "future")
	inside := filepath.Join(alias, "future", "cluster", "p", "missing", "sidecar.json")
	for _, kind := range []string{"storage", "operation", "endpoint"} {
		t.Run(kind, func(t *testing.T) {
			o := options{artifactDir: artifact, backend: "treedb", class: "relaxed", dgraphBin: "/must-not-run", repeat: 1, dataset: 1, concurrency: 1, warmup: 1, timed: 1}
			switch kind {
			case "storage":
				o.storageDiagnostic = inside
			case "operation":
				o.operationDiagnostic = inside
			case "endpoint":
				o.endpointDiagnostic = inside
				o.operationDiagnostic = filepath.Join(base, "operation.json")
			}
			var admission error
			switch kind {
			case "storage":
				admission = validateStorageDiagnosticOption(o)
			case "operation":
				admission = validateOperationDiagnostic(o)
			case "endpoint":
				admission = validateEndpointDiagnostic(o)
			}
			if admission == nil {
				t.Fatal("alias admitted by preflight")
			}
			if err := run(o); err == nil || !strings.Contains(err.Error(), "outside the posting directory") {
				t.Fatalf("alias admitted or wrong refusal: %v", err)
			}
			if _, err := os.Lstat(artifact); !os.IsNotExist(err) {
				t.Fatalf("preflight created artifacts: %v", err)
			}
		})
	}
	// Resolving the posting root matters as much as resolving the sidecar.
	if err := validateStorageDiagnosticOption(options{artifactDir: filepath.Join(alias, "future"), storageDiagnostic: filepath.Join(real, "future", "cluster", "p", "sidecar.json")}); err == nil {
		t.Fatal("aliased posting root admitted")
	}
	// A benign parent alias remains usable, including a missing leaf.
	outside := filepath.Join(alias, "sidecar.json")
	if err := validateStorageDiagnosticOption(options{artifactDir: artifact, storageDiagnostic: filepath.Join(alias, "future-outside", "missing", "sidecar.json")}); err != nil {
		t.Fatal(err)
	}
	if err := validateStorageDiagnosticOption(options{artifactDir: artifact, storageDiagnostic: outside}); err != nil {
		t.Fatal(err)
	}
	// Canonical collisions with both native artifacts and other observers refuse.
	for _, target := range []string{filepath.Join(alias, "future", "result.json"), outside} {
		o := options{artifactDir: artifact, storageDiagnostic: target}
		if target == outside {
			o.operationDiagnostic = filepath.Join(real, "sidecar.json")
		}
		if err := validateStorageDiagnosticOption(o); err == nil {
			t.Fatalf("alias collision admitted: %s", target)
		}
	}
}

func TestDiagnosticWriteRechecksAliases(t *testing.T) {
	for _, kind := range []string{"storage", "operation", "endpoint"} {
		t.Run(kind, func(t *testing.T) {
			base := t.TempDir()
			postings := filepath.Join(base, "artifacts", "cluster", "p")
			outside := filepath.Join(base, "outside")
			for _, path := range []string{postings, outside} {
				if err := os.MkdirAll(path, 0755); err != nil {
					t.Fatal(err)
				}
			}
			alias := filepath.Join(base, "alias")
			if err := os.Symlink(outside, alias); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(alias, "sidecar.json")
			if _, err := diagnosticOutputPath(path, postings, nil); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(alias); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(postings, alias); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "storage":
				err = writeStorageDiagnostic(path, &storageDiagnostic{}, storageTestResult(postings))
			case "operation":
				err = writeOperationDiagnostic(path, &operationDiagnostic{}, postings)
			case "endpoint":
				err = captureEndpointDiagnostic(context.Background(), path, postings, "http://must-not-request", "run", time.Now())
			}
			if err == nil || !strings.Contains(err.Error(), "outside the posting directory") {
				t.Fatalf("write bypassed confinement: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(postings, "sidecar.json")); !os.IsNotExist(err) {
				t.Fatalf("posting mutated: %v", err)
			}
		})
	}
}

func TestDiagnosticResolvedWritesAndFailures(t *testing.T) {
	base := t.TempDir()
	postings := filepath.Join(base, "artifacts", "cluster", "p")
	outside := filepath.Join(base, "outside")
	for _, path := range []string{postings, outside} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(postings, "index.db"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(outside, alias); err != nil {
		t.Fatal(err)
	}
	r := storageTestResult(postings)
	d := newStorageDiagnostic(postings, r)
	logical, allocated, err := diskUsageObserved(postings, d)
	if err != nil {
		t.Fatal(err)
	}
	r.Metrics = metrics("treedb", 0, 0, logical, allocated, nil, nil, nil, nil)
	path := filepath.Join(alias, "storage.json")
	if err := writeStorageDiagnostic(path, d, r); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(outside, "storage.json")); err != nil {
		t.Fatal(err)
	}
	if err := writeStorageDiagnostic(path, d, r); !os.IsExist(err) {
		t.Fatalf("immutable refusal: %v", err)
	}
	d.WalkComplete = false
	d.Error = "interrupted walk"
	partial := filepath.Join(alias, "partial.json")
	if err := writeStorageDiagnostic(partial, d, r); err == nil {
		t.Fatal("partial accepted")
	}
	if b, err := os.ReadFile(filepath.Join(outside, "partial.json")); err != nil || !strings.Contains(string(b), "interrupted walk") {
		t.Fatalf("partial raw lost: %s %v", b, err)
	}
	dangling := filepath.Join(base, "dangling")
	if err := os.Symlink(filepath.Join(base, "absent"), dangling); err != nil {
		t.Fatal(err)
	}
	if _, err := diagnosticOutputPath(filepath.Join(dangling, "output.json"), postings, nil); err == nil {
		t.Fatal("dangling parent admitted")
	}
	cycle := filepath.Join(base, "cycle")
	if err := os.Symlink(cycle, cycle); err != nil {
		t.Fatal(err)
	}
	if _, err := diagnosticOutputPath(filepath.Join(cycle, "output.json"), postings, nil); err == nil {
		t.Fatal("cyclic parent admitted")
	}
}

func TestDiagnosticExcludesRunCluster(t *testing.T) {
	for _, kind := range []string{"storage", "operation", "endpoint"} {
		for _, location := range []string{"artifact", "cluster", "w", "zw", "aliased-w"} {
			t.Run(kind+"/"+location, func(t *testing.T) {
				base := t.TempDir()
				artifact := filepath.Join(base, "future-run")
				alias := filepath.Join(base, "alias")
				if err := os.Symlink(base, alias); err != nil {
					t.Fatal(err)
				}
				target := artifact
				switch location {
				case "cluster":
					target = filepath.Join(artifact, "cluster")
				case "w", "zw":
					target = filepath.Join(artifact, "cluster", location, "observer.json")
				case "aliased-w":
					target = filepath.Join(alias, "future-run", "cluster", "w", "observer.json")
				}
				o := options{artifactDir: artifact, backend: "treedb", class: "relaxed", dgraphBin: "/must-not-run", repeat: 1, dataset: 1, concurrency: 1, warmup: 1, timed: 1}
				switch kind {
				case "storage":
					o.storageDiagnostic = target
				case "operation":
					o.operationDiagnostic = target
				case "endpoint":
					o.endpointDiagnostic = target
					o.operationDiagnostic = filepath.Join(base, "operation.json")
				}
				if err := run(o); err == nil || !strings.Contains(err.Error(), "cluster directory") {
					t.Fatalf("wrong cluster admission: %v", err)
				}
				if _, err := os.Lstat(artifact); !os.IsNotExist(err) {
					t.Fatalf("preflight created run directory: %v", err)
				}
			})
		}
	}
	// The resolved parent of postings defines the entire run cluster, including
	// aliases of the posting root and existing ancestor directories.
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0755); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	postings := filepath.Join(alias, "future", "cluster", "p")
	for _, output := range []string{real, filepath.Join(real, "future"), filepath.Join(real, "future", "cluster", "zw", "observer.json")} {
		if _, err := diagnosticOutputPath(output, postings, nil); err == nil {
			t.Fatalf("cluster ancestor or root alias admitted: %s", output)
		}
	}
	// Ordinary sidecars within artifactDir but outside cluster remain valid.
	if _, err := diagnosticOutputPath(filepath.Join(real, "future", "observer.json"), postings, nil); err != nil {
		t.Fatal(err)
	}
}

func TestDiagnosticWritersRecheckRunCluster(t *testing.T) {
	for _, kind := range []string{"storage", "operation", "endpoint"} {
		t.Run(kind, func(t *testing.T) {
			base := t.TempDir()
			postings := filepath.Join(base, "run", "cluster", "p")
			wal := filepath.Join(base, "run", "cluster", "w")
			outside := filepath.Join(base, "outside")
			for _, dir := range []string{postings, wal, outside} {
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
			}
			alias := filepath.Join(base, "alias")
			if err := os.Symlink(outside, alias); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(alias, "observer.json")
			if _, err := diagnosticOutputPath(path, postings, nil); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(alias); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(wal, alias); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "storage":
				err = writeStorageDiagnostic(path, &storageDiagnostic{}, storageTestResult(postings))
			case "operation":
				err = writeOperationDiagnostic(path, &operationDiagnostic{}, postings)
			case "endpoint":
				err = captureEndpointDiagnostic(context.Background(), path, postings, "http://must-not-request", "run", time.Now())
			}
			if err == nil || !strings.Contains(err.Error(), "cluster directory") {
				t.Fatalf("write bypassed cluster confinement: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(wal, "observer.json")); !os.IsNotExist(err) {
				t.Fatalf("WAL mutated: %v", err)
			}
		})
	}
}

func TestDiagnosticExclusionCoversEndpointWithoutOperations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options options
		want    []string
	}{
		{"ordinary", options{}, nil},
		{"CPU profile only", options{cpuProfile: "cpu.pprof"}, nil},
		{"CPU-only endpoint", options{cpuProfile: "cpu.pprof", endpointDiagnostic: "endpoints.json"}, []string{"endpoint diagnostic"}},
		{"operation-only endpoint", options{operationDiagnostic: "operations.json", endpointDiagnostic: "endpoints.json"}, []string{"operation diagnostic", "endpoint diagnostic"}},
		{"operation", options{operationDiagnostic: "operations.json"}, []string{"operation diagnostic"}},
		{"storage", options{storageDiagnostic: "storage.json"}, []string{storageDiagnosticReason}},
		{"storage and operation", options{storageDiagnostic: "storage.json", operationDiagnostic: "operations.json"}, []string{storageDiagnosticReason, "operation diagnostic"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := livebench.Result{Context: livebench.Context{Profiles: []string{"existing.pprof"}}}
			markDiagnostics(&r, tc.options)
			if r.Context.Excluded != (len(tc.want) != 0) || len(r.Context.Contaminants) != len(tc.want) {
				t.Fatalf("exclusion context: %+v", r.Context)
			}
			for _, reason := range tc.want {
				if !strings.Contains(r.Context.ExclusionReason, reason) {
					t.Fatalf("missing %q in %+v", reason, r.Context)
				}
			}
			if len(r.Context.Profiles) != 1 || r.Context.Profiles[0] != "existing.pprof" {
				t.Fatal("diagnostic marking changed profile artifacts")
			}
		})
	}
	r := livebench.Result{Context: livebench.Context{Contaminants: []string{"existing contamination"}}}
	markDiagnostics(&r, options{cpuProfile: "cpu.pprof", endpointDiagnostic: "endpoints.json"})
	if !r.Context.Excluded || len(r.Context.Contaminants) != 2 || !strings.HasPrefix(r.Context.ExclusionReason, "existing contamination; ") {
		t.Fatalf("lost existing contamination: %+v", r.Context)
	}
}
