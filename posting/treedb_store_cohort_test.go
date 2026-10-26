/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
)

// Distinct timestamps of the same logical key do not need separate durable
// publications. This public adapter test holds the first admitted request so
// that its successors are definitely queued, without a batching timer or a
// TreeDB group-commit configuration override.
func TestTreeDBStoreDurableCohortSharesSyncAcrossVersions(t *testing.T) {
	for _, mode := range []string{"adaptive", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) { testTreeDBStoreDurableCohortSharesSyncAcrossVersions(t, mode) })
	}
}

func testTreeDBStoreDurableCohortSharesSyncAcrossVersions(t *testing.T, mode string) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.MemtableMode = mode
	store, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	before, err := store.Stats()
	require.NoError(t, err)
	if mode == "cow_ordered" {
		require.Equal(t, mode, before["treedb.cache.memtable_mode"])
	} else {
		require.NotEmpty(t, before["treedb.cache.memtable_mode"])
	}
	release := holdTreeDBCohortRequest(t, store, 1)
	const commits = 7
	done := make([]chan error, commits)
	for ts := uint64(1); ts <= commits; ts++ {
		txn := store.NewWriteTxn()
		require.NoError(t, txn.SetEntry(Entry{Key: []byte("shared"), Value: []byte(fmt.Sprint(ts))}))
		require.NoError(t, txn.SetEntry(Entry{Key: []byte(fmt.Sprintf("independent/%d", ts)), Value: []byte(fmt.Sprint(ts))}))
		result := make(chan error, 2)
		done[ts-1] = result
		if ts%2 == 0 {
			go func() { result <- txn.CommitAt(ts, nil) }()
		} else {
			require.NoError(t, txn.CommitAt(ts, func(err error) { result <- err }))
		}
		if ts == 1 {
			awaitTreeDBCohort(t, "first request admission", func() bool { return len(store.commitQueue) == 0 && store.durableRequests.Load() == 1 })
		} else {
			// The held first request cannot drain this queue. Seeing every successor
			// here proves synchronous callers joined the same queue as callbacks.
			awaitTreeDBCohort(t, "mixed request enqueue", func() bool { return len(store.commitQueue) == int(ts-1) })
		}
	}
	for _, result := range done {
		select {
		case err := <-result:
			t.Fatalf("acknowledged before durability barrier: %v", err)
		default:
		}
	}
	release()
	for _, result := range done {
		require.NoError(t, receiveTreeDBCohortResult(t, result))
	}
	after, err := store.Stats()
	require.NoError(t, err)
	syncs := treeDBStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total")
	if mode == "cow_ordered" {
		// This gate is before append, not a held physical sync. The staged
		// pipeline may append both records before the coordinator captures one
		// shared barrier. The physical-sync test checks separate certificates.
		require.GreaterOrEqual(t, syncs, uint64(1))
		require.LessOrEqual(t, syncs, uint64(2))
	} else {
		require.Equal(t, uint64(2), syncs, "one gate sync and one shared aggregate sync")
	}
	require.Equal(t, uint64(2), store.durableCohorts.Load())
	require.Equal(t, uint64(commits-1), store.durableCohortMax.Load())
	require.NoError(t, store.Close())
	reopened, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	for ts := uint64(1); ts <= commits; ts++ {
		for _, key := range []string{"shared", fmt.Sprintf("independent/%d", ts)} {
			snapshot := pointSnapshot(t, reopened, ts, []byte(key))
			require.Equal(t, ts, snapshot.Version)
			require.Equal(t, fmt.Sprint(ts), snapshot.Value)
		}
	}
}

func TestTreeDBStoreDurableCohortPreservesErrorsAndVersionOrder(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	store.commitStartedForTest = func(ts uint64, _ []mvcc.Mutation) {
		if ts == 1 {
			close(firstStarted)
			<-release
		}
	}
	type request struct {
		ts   uint64
		keys []string
		val  string
	}
	requests := []request{
		{1, []string{"gate"}, "first"},
		{2, []string{"x"}, "two"},
		{3, []string{"x", "y"}, "three"},
		{4, []string{"y"}, "four"},
		{0, []string{"invalid"}, "never"},
		{5, []string{"good"}, "five"},
		{6, []string{"x"}, "six-first"},
		{6, []string{"x"}, "six-last"},
	}
	done := make([]chan error, len(requests))
	for i, request := range requests {
		txn := store.NewWriteTxn()
		for _, key := range request.keys {
			require.NoError(t, txn.SetEntry(Entry{Key: []byte(key), Value: []byte(request.val)}))
		}
		done[i] = make(chan error, 1)
		result := done[i]
		require.NoError(t, txn.CommitAt(request.ts, func(err error) { result <- err }))
		if i == 0 {
			select {
			case <-firstStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("first commit did not reach its barrier")
			}
		}
	}
	once.Do(func() { close(release) })
	for i, result := range done {
		select {
		case err := <-result:
			if requests[i].ts == 0 {
				require.ErrorIs(t, err, mvcc.ErrZeroTimestamp)
			} else {
				require.NoError(t, err, "invalid neighbour must not reject a valid transaction")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("queued transaction did not complete")
		}
	}
	for _, expected := range []struct {
		ts       uint64
		key, val string
	}{{2, "x", "two"}, {3, "x", "three"}, {3, "y", "three"}, {4, "y", "four"}, {6, "x", "six-last"}, {5, "good", "five"}} {
		read := store.NewReadTxn(expected.ts)
		item, err := read.Get([]byte(expected.key))
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, expected.val, string(value))
		read.Discard()
	}
	read := store.NewReadTxn(math.MaxUint64)
	_, err := read.Get([]byte("invalid"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	read.Discard()
	require.NoError(t, store.Close())
}

// Deadlines below detect stalled admission/completion only. Cohort membership
// is established by barriers and queue contents, never an elapsed-time window.
func awaitTreeDBCohort(t *testing.T, operation string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatalf("stalled waiting for %s", operation)
		default:
			runtime.Gosched()
		}
	}
}

func receiveTreeDBCohortResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("admitted request did not complete")
		return nil
	}
}

func holdTreeDBCohortRequest(t *testing.T, store *TreeDBStore, timestamp uint64) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	store.commitStartedForTest = func(ts uint64, _ []mvcc.Mutation) {
		if ts == timestamp {
			<-release
		}
	}
	return unblock
}

func TestTreeDBStoreDurableCohortByteCarryAndClose(t *testing.T) {
	store, opts := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	gate := make(chan struct{})
	carry := make(chan struct{})
	var gateOnce, carryOnce sync.Once
	releaseGate := func() { gateOnce.Do(func() { close(gate) }) }
	releaseCarry := func() { carryOnce.Do(func() { close(carry) }) }
	t.Cleanup(releaseGate)
	t.Cleanup(releaseCarry)
	var started, finished []uint64
	store.commitStartedForTest = func(ts uint64, _ []mvcc.Mutation) {
		started = append(started, ts)
		if ts == 1 {
			<-gate
		}
		// By this point requests 2 and 3 share the selected cohort, and request
		// 4 has been removed from the channel into the carried successor slot.
		if ts == 2 {
			<-carry
		}
	}
	store.commitFinishedForTest = func(ts uint64, _ error) { finished = append(finished, ts) }
	type entry struct {
		key   string
		size  int
		value byte
	}
	entries := [][]entry{
		{{"gate", 1, 1}},
		{{"fits-first", 2 << 20, 2}},
		{{"fits-second", 1 << 20, 3}},
		{{"carried", 2 << 20, 4}},
		{{"oversize-a", 3 << 20, 5}, {"oversize-b", 3 << 20, 6}},
		{{"after-oversize", 1, 7}},
	}
	done := make([]chan error, len(entries))
	for i, request := range entries {
		txn := store.NewWriteTxn()
		for _, entry := range request {
			require.NoError(t, txn.SetEntry(Entry{Key: []byte(entry.key), Value: bytes.Repeat([]byte{entry.value}, entry.size)}))
		}
		done[i] = make(chan error, 2)
		result := done[i]
		require.NoError(t, txn.CommitAt(uint64(i+1), func(err error) { result <- err }))
		if i == 0 {
			awaitTreeDBCohort(t, "gate admission", func() bool { return store.durableRequests.Load() == 1 })
		}
	}
	require.Len(t, store.commitQueue, 5)
	releaseGate()
	awaitTreeDBCohort(t, "byte-limited cohort selection", func() bool { return store.durableRequests.Load() == 3 })
	require.Len(t, store.commitQueue, 2, "request 4 must already occupy the carried slot")
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	awaitTreeDBCohort(t, "Close admission fence", func() bool { return store.closed.Load() })
	releaseCarry()
	for _, result := range done {
		require.NoError(t, receiveTreeDBCohortResult(t, result))
	}
	require.NoError(t, receiveTreeDBCohortResult(t, closeDone))
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, started)
	require.Equal(t, started, finished)
	require.Equal(t, uint64(5), store.durableCohorts.Load(), "gate; fitting pair; carry; atomic oversize; tail")
	require.Equal(t, uint64(2), store.durableCohortMax.Load())
	require.Empty(t, store.commitSlots, "Close must release ownership of carried and oversized work")
	require.Empty(t, store.commitQueue)
	for _, result := range done {
		require.Empty(t, result, "one completion per request")
	}
	reopened, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	for i, request := range entries {
		for _, entry := range request {
			snapshot := pointSnapshot(t, reopened, uint64(i+1), []byte(entry.key))
			require.Equal(t, uint64(i+1), snapshot.Version)
			require.Equal(t, bytes.Repeat([]byte{entry.value}, entry.size), []byte(snapshot.Value))
		}
	}
}

func TestTreeDBStoreDurableCohortStorageErrorNeverRetries(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	release := holdTreeDBCohortRequest(t, store, 1)
	injected := errors.Join(mvcc.ErrStorage, mvcc.ErrInvalidKey, errors.New("ambiguous injected storage failure"))
	var attempts atomic.Int32
	store.commitGroupsForTest = func(groups []mvcc.CommitGroup, mode mvcc.CommitMode) error {
		attempts.Add(1)
		if len(groups) != 4 || mode != mvcc.CommitDurable {
			return fmt.Errorf("unexpected injected group shape: size=%d mode=%v", len(groups), mode)
		}
		// Model the ambiguous case: storage applied the whole cohort, then its
		// acknowledgement failed. Replaying it would be an extra real write.
		if err := store.mvcc.CommitGroupAt(groups, mode); err != nil {
			return errors.Join(injected, err)
		}
		return injected
	}
	done := make([]chan error, 5)
	callbacks := make([]atomic.Int32, len(done))
	for i := range done {
		txn := store.NewWriteTxn()
		require.NoError(t, txn.SetEntry(Entry{Key: []byte(fmt.Sprintf("ambiguous/%d", i)), Value: []byte("unacknowledged")}))
		done[i] = make(chan error, 2)
		result := done[i]
		if i == 2 {
			go func() { result <- txn.CommitAt(uint64(i+1), nil) }()
		} else {
			require.NoError(t, txn.CommitAt(uint64(i+1), func(err error) { callbacks[i].Add(1); result <- err }))
		}
		if i == 0 {
			awaitTreeDBCohort(t, "first request admission", func() bool { return store.durableRequests.Load() == 1 })
		} else {
			awaitTreeDBCohort(t, "failure cohort enqueue", func() bool { return len(store.commitQueue) == i })
		}
	}
	release()
	for i, result := range done {
		err := receiveTreeDBCohortResult(t, result)
		if i == 0 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, mvcc.ErrStorage)
			require.ErrorIs(t, err, mvcc.ErrInvalidKey)
		}
	}
	for i := 1; i < len(done); i++ {
		require.Equal(t, "unacknowledged", pointSnapshot(t, store, uint64(i+1), []byte(fmt.Sprintf("ambiguous/%d", i))).Value)
	}
	// A later transaction and Close must still progress after the whole cohort
	// has received the ambiguous error, with no retry or permanently owned slot.
	commitStoreEntry(t, store, 6, Entry{Key: []byte("after-error"), Value: []byte("progress")})
	require.Equal(t, int32(1), attempts.Load())
	require.Equal(t, "progress", pointSnapshot(t, store, 6, []byte("after-error")).Value)
	require.NoError(t, store.Close())
	require.Empty(t, store.commitSlots)
	for i, result := range done {
		require.Empty(t, result)
		if i != 2 {
			require.Equal(t, int32(1), callbacks[i].Load())
		}
	}
}

func TestTreeDBStoreDurableCohortInvalidKeyAndDiscardFloorIsolation(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	require.NoError(t, store.AdvanceDiscardFloor(10))
	release := holdTreeDBCohortRequest(t, store, 11)
	invalidKey := bytes.Repeat([]byte{'x'}, 1<<16)
	requests := []struct {
		ts   uint64
		key  []byte
		want error
	}{
		{11, []byte("gate"), nil},
		{12, []byte("before-invalid-key"), nil},
		{13, invalidKey, mvcc.ErrInvalidKey},
		{14, []byte("between-invalid"), nil},
		{9, []byte("below-floor"), mvcc.ErrVersionBelowDiscardFloor},
		{15, []byte("after-invalid-floor"), nil},
	}
	done := make([]chan error, len(requests))
	for i, request := range requests {
		txn := store.NewWriteTxn()
		if request.want == mvcc.ErrInvalidKey {
			require.NoError(t, txn.SetEntry(Entry{Key: []byte("invalid-transaction-partial"), Value: []byte("must-not-publish")}))
		}
		require.NoError(t, txn.SetEntry(Entry{Key: request.key, Value: []byte(fmt.Sprint(request.ts))}))
		done[i] = make(chan error, 2)
		result := done[i]
		require.NoError(t, txn.CommitAt(request.ts, func(err error) { result <- err }))
		if i == 0 {
			awaitTreeDBCohort(t, "first request admission", func() bool { return store.durableRequests.Load() == 1 })
		}
	}
	require.Len(t, store.commitQueue, len(requests)-1)
	release()
	for i, result := range done {
		err := receiveTreeDBCohortResult(t, result)
		if requests[i].want == nil {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, requests[i].want)
		}
	}
	require.Equal(t, uint64(len(requests)-1), store.durableCohortMax.Load(), "validation failures must actually share a selected cohort with valid neighbours")
	for _, request := range requests {
		if request.want == nil {
			snapshot := pointSnapshot(t, store, request.ts, request.key)
			require.Equal(t, request.ts, snapshot.Version)
			require.Equal(t, fmt.Sprint(request.ts), snapshot.Value)
		}
	}
	require.Equal(t, badger.ErrKeyNotFound.Error(), pointSnapshot(t, store, 15, []byte("below-floor")).Err)
	require.Equal(t, badger.ErrKeyNotFound.Error(), pointSnapshot(t, store, 15, []byte("invalid-transaction-partial")).Err)
	require.NoError(t, store.Close())
}
