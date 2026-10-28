package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// A supplied FileInfo is the observation authority: replacing its path before
// callback processing must not change the captured inode or byte counts.
func TestStorageAttributionIdentityFromSameFileInfo(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "index.db")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	d := newStorageDiagnostic(root, storageTestResult(root))
	calls := 0
	_, _, err = diskUsageWalk(root, d, func(_ string, fn filepath.WalkFunc) error {
		calls++
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
			return err
		}
		return fn(path, info, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	f := d.Files[0]
	if calls != 1 || !f.IdentityAvailable || f.Inode != strconv.FormatUint(st.Ino, 10) || f.Device != strconv.FormatUint(uint64(st.Dev), 10) || f.LinkCount != uint64(st.Nlink) || f.Mode != uint32(info.Mode()) || f.ObservedAt.Before(d.WalkStartedAt) || f.ObservedAt.After(d.WalkFinishedAt) {
		t.Fatalf("wrong observation: %+v", f)
	}
}

func TestStorageAttributionAliasesAndSparse(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(8 << 20); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(path, filepath.Join(root, "b")); err != nil {
		t.Skip(err)
	}
	d := newStorageDiagnostic(root, storageTestResult(root))
	l, _, err := diskUsageObserved(root, d)
	if err != nil {
		t.Fatal(err)
	}
	if l != 16<<20 || len(d.Aliases) != 1 || d.Aliases[0].FirstPath != "a" || d.Aliases[0].Path != "b" {
		t.Fatalf("path semantics lost: %+v", d)
	}
	if d.Files[0].AllocatedBytes >= d.Files[0].LogicalBytes {
		t.Log("filesystem did not expose sparse allocation")
	}
}

func TestStorageAttributionRawStatusAndTiming(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cluster", "p")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.db"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	r := storageTestResult(root)
	d := newStorageDiagnostic(root, r)
	l, a, err := diskUsageObserved(root, d)
	if err != nil {
		t.Fatal(err)
	}
	r.Metrics = metrics("treedb", "store_wal", 0, 0, l, a, nil, nil, nil, nil)
	completeStorageTestObservation(d)
	d.Store.Body = "status=map[backend:treedb profile:wal_on_sync post_commit_events:true] stats=map[treedb.root_page:18446744073709551615 treedb.command_wal.mode:store_wal bool:false]"
	d.resolveStorageFields()
	d.WriteStartedAt = time.Now().UTC()
	if err := validateStorageDiagnostic(d, r); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "18446744073709551615") {
		t.Fatal("uint64 identity lost")
	}
	for _, mutate := range []func(*storageDiagnostic){
		func(x *storageDiagnostic) { x.Store.StartedAt = time.Time{} },
		func(x *storageDiagnostic) { x.Store.FinishedAt = x.Store.StartedAt.Add(-time.Second) },
		func(x *storageDiagnostic) { x.Prometheus.StartedAt = x.WalkFinishedAt.Add(-time.Second) },
		func(x *storageDiagnostic) { x.Store.Error = "request failed" },
		func(x *storageDiagnostic) {
			x.Store.Body = "status=map[backend:badger profile:syncwrites=true] stats=map[]"
		},
		func(x *storageDiagnostic) { x.Context.BinarySHA256 = "wrong" },
		func(x *storageDiagnostic) { x.Config.Concurrency++ },
		func(x *storageDiagnostic) { x.Files[0].ObservedAt = time.Time{} },
		func(x *storageDiagnostic) { x.Files[0].IdentityAvailable = false },
		func(x *storageDiagnostic) {
			x.Files[0].LogicalBytes = 1<<53 + 1
			x.LogicalBytes = float64(x.Files[0].LogicalBytes)
		},
	} {
		var x storageDiagnostic
		if err := json.Unmarshal(b, &x); err != nil {
			t.Fatal(err)
		}
		mutate(&x)
		if validateStorageDiagnostic(&x, r) == nil {
			t.Fatalf("invalid attribution accepted: %+v", x)
		}
	}
}

func completeStorageTestObservation(d *storageDiagnostic) {
	d.HarnessBinarySHA256 = strings.Repeat("b", 64)
	d.HarnessBuildInfo = "harness build"
	now := time.Now().UTC()
	d.Prometheus = operationRequest{HTTPStatus: 200, StartedAt: now, FinishedAt: now}
	d.Store = operationRequest{HTTPStatus: 200, StartedAt: now, FinishedAt: now, Body: "status=map[backend:" + d.Backend + " profile:wal_on_sync post_commit_events:true] stats=map[]"}
	d.WalkToStatusGap = d.Store.StartedAt.Sub(d.WalkFinishedAt)
	d.resolveStorageFields()
}

type missingStatFileInfo struct{ os.FileInfo }

func (missingStatFileInfo) Sys() any { return nil }
func TestStorageAttributionMissingStatAndOverflow(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	d := newStorageDiagnostic(root, storageTestResult(root))
	_, _, err = diskUsageWalk(root, d, func(_ string, fn filepath.WalkFunc) error { return fn(path, missingStatFileInfo{info}, nil) })
	if err == nil || d.WalkComplete || d.Error == "" || len(d.Files) != 1 || d.Files[0].IdentityAvailable {
		t.Fatalf("missing stat accepted: %+v %v", d, err)
	}
	st := *info.Sys().(*syscall.Stat_t)
	st.Blocks = 1 << 62
	_, _, err = diskUsageWalk(root, d, func(_ string, fn filepath.WalkFunc) error { return fn(path, injectedStatFileInfo{info, &st}, nil) })
	if err == nil || !strings.Contains(err.Error(), "overflowing") {
		t.Fatalf("overflow blocks accepted: %v", err)
	}
}

type injectedStatFileInfo struct {
	os.FileInfo
	stat *syscall.Stat_t
}

func (i injectedStatFileInfo) Sys() any { return i.stat }

func TestStorageAttributionBadgerAvailabilityAndErrors(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cluster", "p")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "badger.sst"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	r := storageTestResult(root)
	r.Config.Backend = "badger"
	d := newStorageDiagnostic(root, r)
	l, a, err := diskUsageObserved(root, d)
	if err != nil {
		t.Fatal(err)
	}
	r.Metrics = metrics("badger", "", 0, 0, l, a, nil, nil, nil, nil)
	completeStorageTestObservation(d)
	d.Store.Body = "status=map[backend:badger profile:syncwrites=true post_commit_events:false] stats=map[backend:badger]"
	d.resolveStorageFields()
	d.WriteStartedAt = time.Now().UTC()
	if err := validateStorageDiagnostic(d, r); err != nil {
		t.Fatal(err)
	}
	if d.EngineStatsAvailability.Available || d.EngineStatsAvailability.Reason == "" || d.EngineStatsAvailability.Error != "" || d.CommandWALMode.Available || !d.EventsPolicy.Available || d.EventsPolicy.Value != "false" {
		t.Fatalf("missing/bool confused: %+v", d)
	}
	d.Store.Body = "status=map[backend:badger profile:syncwrites=true] stats_error=injected failure"
	d.resolveStorageFields()
	if validateStorageDiagnostic(d, r) == nil || d.EngineStatsAvailability.Error != "injected failure" {
		t.Fatalf("engine error lost: %+v", d)
	}
	out := filepath.Join(filepath.Dir(filepath.Dir(root)), "partial.json")
	if writeStorageDiagnostic(out, d, r) == nil {
		t.Fatal("engine error succeeded")
	}
	raw, err := os.ReadFile(out)
	if err != nil || !strings.Contains(string(raw), "injected failure") {
		t.Fatalf("partial not retained: %s %v", raw, err)
	}
}

func TestStorageAttributionExactNativeBoundary(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cluster", "p")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	r := storageTestResult(root)
	d := newStorageDiagnostic(root, r)
	if _, _, err := diskUsageObserved(root, d); err != nil {
		t.Fatal(err)
	}
	completeStorageTestObservation(d)
	d.WriteStartedAt = time.Now().UTC()
	d.Files[0].LogicalBytes = 1 << 53
	d.LogicalBytes = 1 << 53
	r.Metrics = metrics("treedb", "store_wal", 0, 0, d.LogicalBytes, d.AllocatedBytes, nil, nil, nil, nil)
	if err := validateStorageDiagnostic(d, r); err != nil {
		t.Fatal(err)
	}
	d.Files[0].LogicalBytes++ // float64 would still round to 2^53.
	if err := validateStorageDiagnostic(d, r); err == nil {
		t.Fatal("rounded total accepted")
	}
	d.Files[0].LogicalBytes = 1 << 53
	extra := d.Files[0]
	extra.Path = "other"
	extra.LogicalBytes = 1
	extra.AllocatedBytes = 0
	extra.StatBlocks = 0
	d.Files = append(d.Files, extra)
	d.RegularFiles++
	if err := validateStorageDiagnostic(d, r); err == nil {
		t.Fatal("rounded sum accepted")
	}
}

func TestStorageAttributionDefaultMetricsUnchanged(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.db"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	l, a, err := diskUsage(root)
	if err != nil {
		t.Fatal(err)
	}
	d := newStorageDiagnostic(root, storageTestResult(root))
	dl, da, err := diskUsageObserved(root, d)
	if err != nil || l != dl || a != da {
		t.Fatalf("optional observation changed totals: %v/%v %v/%v %v", l, a, dl, da, err)
	}
	// Endpoint diagnostic schema remains unchanged when row enrichment is off.
	b, err := json.Marshal(endpointFile{Path: "index.db"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "observed_at") || strings.Contains(string(b), "identity_available") {
		t.Fatalf("default endpoint row changed: %s", b)
	}
}

func TestStorageAttributionPreservesHTTPFailureBytes(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		for _, store := range []bool{false, true} {
			t.Run(fmt.Sprintf("truncated=%t/store=%t", truncated, store), func(t *testing.T) {
				body := "failure evidence"
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if truncated {
						w.Header().Set("Content-Length", "100")
					} else {
						w.WriteHeader(http.StatusInternalServerError)
					}
					_, _ = w.Write([]byte(body))
				}))
				defer srv.Close()
				obs := operationRequest{}
				var err error
				if store {
					_, _, err = storeStatusObserved(srv.URL, &obs)
				} else {
					_, err = prometheusObserved(srv.URL, &obs)
				}
				if calls.Load() != 1 || err == nil || obs.Body != body || obs.Error == "" || (truncated && obs.HTTPStatus != 200) || (!truncated && obs.HTTPStatus != 500) || obs.StartedAt.IsZero() || obs.FinishedAt.Before(obs.StartedAt) {
					t.Fatalf("failure bytes lost: %+v err=%v", obs, err)
				}
			})
		}
	}
}
