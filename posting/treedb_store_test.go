/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/x"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
)

func TestTreeDBEnvelopeRoundTripAndCorruption(t *testing.T) {
	entry := Entry{
		Value: []byte{0, 1, 0xff}, UserMeta: 0xfe, DiscardEarlierVersions: true,
	}
	encoded := encodeTreeDBEnvelope(entry)
	value, meta, discard, err := decodeTreeDBEnvelope(encoded)
	require.NoError(t, err)
	require.Equal(t, entry.Value, value)
	require.Equal(t, entry.UserMeta, meta)
	require.True(t, discard)
	entry.Value[0] = 7
	require.Equal(t, []byte{0, 1, 0xff}, value, "encoded payload must be owned")

	for name, corrupt := range map[string][]byte{
		"empty":         nil,
		"short":         {treeDBEnvelopeMagic0},
		"magic":         {0, treeDBEnvelopeMagic1, treeDBEnvelopeVersion, 0, 0},
		"version":       {treeDBEnvelopeMagic0, treeDBEnvelopeMagic1, 2, 0, 0},
		"unknown flags": {treeDBEnvelopeMagic0, treeDBEnvelopeMagic1, treeDBEnvelopeVersion, 0x80, 0},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := decodeTreeDBEnvelope(corrupt)
			require.ErrorIs(t, err, ErrTreeDBEnvelope)
		})
	}
}

func TestTreeDBStoreWriteSemanticsTTLAndCallbacks(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	key := []byte("deep-copy")
	value := []byte("first")
	txn := store.NewWriteTxn()
	require.NoError(t, txn.SetEntry(Entry{Key: key, Value: value, UserMeta: BitDeltaPosting}))
	key[0] = 'X'
	value[0] = 'X'
	require.NoError(t, txn.SetEntry(Entry{
		Key: []byte("last-wins"), Value: []byte("old"), UserMeta: BitDeltaPosting,
	}))
	require.NoError(t, txn.Delete([]byte("last-wins")))
	require.NoError(t, txn.SetEntry(Entry{
		Key: []byte("last-wins"), Value: []byte("new"), UserMeta: BitCompletePosting,
		DiscardEarlierVersions: true,
	}))
	var callbacks atomic.Int32
	done := make(chan error, 1)
	require.NoError(t, txn.CommitAt(5, func(err error) {
		callbacks.Add(1)
		done <- err
	}))
	require.NoError(t, <-done)
	require.Equal(t, int32(1), callbacks.Load())

	read := store.NewReadTxn(5)
	item, err := read.Get([]byte("deep-copy"))
	require.NoError(t, err)
	got, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), got)
	item, err = read.Get([]byte("last-wins"))
	require.NoError(t, err)
	got, err = item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), got)
	require.Equal(t, BitCompletePosting, item.UserMeta())
	require.True(t, item.DiscardEarlierVersions())
	read.Discard()

	poisoned := store.NewWriteTxn()
	require.NoError(t, poisoned.SetEntry(Entry{Key: []byte("must-not-publish"), Value: []byte("staged")}))
	err = poisoned.SetEntry(Entry{Key: []byte("ttl"), Value: []byte("no"), ExpiresAt: 1})
	require.ErrorIs(t, err, ErrTreeDBTTLUnsupported)
	callbacks.Store(0)
	err = poisoned.CommitAt(6, func(cbErr error) {
		callbacks.Add(1)
		done <- cbErr
	})
	require.NoError(t, err)
	require.ErrorIs(t, <-done, ErrTreeDBTTLUnsupported)
	require.Equal(t, int32(1), callbacks.Load())
	read = store.NewReadTxn(6)
	_, err = read.Get([]byte("must-not-publish"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	read.Discard()

	failing := store.NewWriteTxn()
	require.NoError(t, failing.SetEntry(Entry{Key: []byte("zero-ts"), Value: []byte("no")}))
	callbacks.Store(0)
	err = failing.CommitAt(0, func(cbErr error) {
		callbacks.Add(1)
		done <- cbErr
	})
	require.NoError(t, err)
	require.ErrorIs(t, <-done, mvcc.ErrZeroTimestamp)
	require.Equal(t, int32(1), callbacks.Load())
}

func TestTreeDBStoreCommitCallbackIsAsyncAndReentrant(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	txn := store.NewWriteTxn()
	require.NoError(t, txn.SetEntry(Entry{Key: []byte("callback"), Value: []byte("value")}))
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	require.NoError(t, txn.CommitAt(1, func(err error) {
		close(started)
		<-release
		txn.Discard()
		done <- err
	}))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not start")
	}
	close(release)
	require.NoError(t, <-done)

	empty := store.NewWriteTxn()
	emptyStarted := make(chan struct{})
	emptyRelease := make(chan struct{})
	emptyDone := make(chan error, 1)
	var emptyCallbacks atomic.Int32
	require.NoError(t, empty.CommitAt(2, func(err error) {
		emptyCallbacks.Add(1)
		close(emptyStarted)
		<-emptyRelease
		emptyDone <- err
	}))
	select {
	case <-emptyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("empty-transaction callback did not start")
	}
	close(emptyRelease)
	require.NoError(t, <-emptyDone)
	require.Equal(t, int32(1), emptyCallbacks.Load())
	read := store.NewReadTxn(2)
	_, err := read.Get([]byte("empty-transaction-must-not-publish"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	read.Discard()

	failing := store.NewWriteTxn()
	require.NoError(t, failing.SetEntry(Entry{Key: []byte("sync-error"), Value: []byte("value")}))
	require.ErrorIs(t, failing.CommitAt(0, nil), mvcc.ErrZeroTimestamp)

	writable, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	writer := NewTxnWriterForStore(writable)
	require.NoError(t, writable.Close())
	require.NoError(t, writer.SetAt([]byte("wait-error"), []byte("value"), BitDeltaPosting, 9))
	require.ErrorIs(t, writer.Wait(), treedb.ErrClosed)

	closingStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	closingTxn := closingStore.NewWriteTxn()
	require.NoError(t, closingTxn.SetEntry(Entry{Key: []byte("callback-close"), Value: []byte("value")}))
	closeFromCallback := make(chan error, 1)
	require.NoError(t, closingTxn.CommitAt(10, func(err error) {
		if err != nil {
			closeFromCallback <- err
			return
		}
		closeFromCallback <- closingStore.Close()
	}))
	select {
	case err := <-closeFromCallback:
		require.NoError(t, err, "callback must be able to close the store without waiting on itself")
	case <-time.After(5 * time.Second):
		t.Fatal("callback deadlocked while closing its store")
	}
}

func TestTreeDBStoreTxnWriterPipelinesAndCloseWaitsForAdmittedCommit(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	store.beforeCommitForTest = func() {
		close(commitStarted)
		<-releaseCommit
	}

	writer := NewTxnWriterForStore(store)
	setDone := make(chan error, 1)
	go func() {
		setDone <- writer.SetAt([]byte("pipelined"), []byte("value"), BitDeltaPosting, 1)
	}()
	select {
	case <-commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("admitted commit did not start")
	}
	select {
	case err := <-setDone:
		require.NoError(t, err, "TxnWriter.SetAt must return while the admitted commit is in flight")
	case <-time.After(5 * time.Second):
		t.Fatal("TxnWriter.SetAt blocked on storage commit")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before its admitted commit completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseCommit)
	require.NoError(t, writer.Wait())
	require.NoError(t, <-closeDone)
}

func TestTreeDBStoreDurableSchedulerOverlapsIndependentCommitsForGroupCommit(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	before, err := store.Stats()
	require.NoError(t, err)

	const commits = 4
	started := make(chan struct{}, commits)
	release := make(chan struct{})
	store.commitStartedForTest = func(_ uint64, _ []mvcc.Mutation) {
		started <- struct{}{}
		<-release
	}

	done := make(chan error, commits)
	for i := 0; i < commits; i++ {
		txn := store.NewWriteTxn()
		require.NoError(t, txn.SetEntry(Entry{
			Key: []byte(fmt.Sprintf("independent-%d", i)), Value: []byte("value"),
		}))
		require.NoError(t, txn.CommitAt(uint64(i+1), func(err error) { done <- err }))
	}

	// This is a scheduler barrier, not a timing assertion: every independent
	// request must reach the MVCC admission hook before any is allowed to enter
	// Gomap's durable group-commit barrier. The timeout only detects a deadlock.
	for i := 0; i < commits; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d independent commits reached the admission barrier", i, commits)
		}
	}
	close(release)
	for i := 0; i < commits; i++ {
		require.NoError(t, <-done)
	}

	after, err := store.Stats()
	require.NoError(t, err)
	groups := treeDBStatDelta(t, before, after, "treedb.command_wal.group_commit.groups_total")
	participants := treeDBStatDelta(t, before, after, "treedb.command_wal.group_commit.participants_total")
	syncs := treeDBStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total")
	require.Greater(t, groups, uint64(0), "independent durable commits must form a Gomap group")
	require.Greater(t, participants, uint64(1), "the adapter must create a multi-commit group")
	require.Less(t, syncs, uint64(commits), "a grouped durable acknowledgement must share command-WAL syncs")
	require.Greater(t, treeDBStatUint(t, after, "treedb.command_wal.group_commit.group_size_max"), uint64(1))
	require.NoError(t, store.Close())
}

func TestTreeDBStoreSchedulerOnlyOvertakesIndependentMultiKeyBatches(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	started := make(chan uint64, 4)
	release := make(chan struct{})
	store.commitStartedForTest = func(timestamp uint64, _ []mvcc.Mutation) {
		started <- timestamp
		<-release
	}

	commit := func(timestamp uint64, keys ...string) <-chan error {
		t.Helper()
		txn := store.NewWriteTxn()
		for _, key := range keys {
			require.NoError(t, txn.SetEntry(Entry{Key: []byte(key), Value: []byte(key)}))
		}
		done := make(chan error, 1)
		require.NoError(t, txn.CommitAt(timestamp, func(err error) { done <- err }))
		return done
	}

	// The first admitted batch owns both a and b. Later batches touching either
	// key must remain behind it, while the independent c batch may enter the
	// bounded window. This defines the only permitted scheduler overtaking.
	first := commit(1, "a", "b")
	second := commit(2, "a")
	third := commit(3, "b")
	fourth := commit(4, "c")
	seen := map[uint64]bool{}
	for len(seen) != 2 {
		select {
		case timestamp := <-started:
			seen[timestamp] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("started commits=%v, want exactly first multi-key batch and independent batch", seen)
		}
	}
	require.Equal(t, map[uint64]bool{1: true, 4: true}, seen)
	select {
	case timestamp := <-started:
		t.Fatalf("dependent batch %d overtook its multi-key predecessor", timestamp)
	default:
	}
	close(release)
	for _, done := range []<-chan error{first, second, third, fourth} {
		require.NoError(t, <-done)
	}
	require.NoError(t, store.Close())
}

func treeDBStatDelta(t *testing.T, before, after map[string]string, key string) uint64 {
	t.Helper()
	return treeDBStatUint(t, after, key) - treeDBStatUint(t, before, key)
}

func treeDBStatUint(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	value, ok := stats[key]
	require.Truef(t, ok, "missing TreeDB stat %q", key)
	parsed, err := strconv.ParseUint(value, 10, 64)
	require.NoErrorf(t, err, "parse TreeDB stat %q=%q", key, value)
	return parsed
}

func TestTreeDBStoreCallbackQueueIsBoundedAndFIFO(t *testing.T) {
	t.Run("backpressure", func(t *testing.T) {
		store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
		commitStarted := make(chan struct{})
		releaseCommits := make(chan struct{})
		var started sync.Once
		store.beforeCommitForTest = func() {
			started.Do(func() { close(commitStarted) })
			<-releaseCommits
		}

		writer := NewTxnWriterForStore(store)
		require.NoError(t, writer.SetAt([]byte("active"), []byte("value"), BitDeltaPosting, 1))
		select {
		case <-commitStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("callback commit worker did not start")
		}

		for i := 0; i < treeDBCommitQueueCapacity; i++ {
			require.NoError(t, writer.SetAt([]byte(fmt.Sprintf("queued-%03d", i)), []byte("value"),
				BitDeltaPosting, uint64(i+2)))
		}
		overflowDone := make(chan error, 1)
		go func() {
			overflowDone <- writer.SetAt([]byte("backpressured"), []byte("value"), BitDeltaPosting,
				uint64(treeDBCommitQueueCapacity+2))
		}()
		select {
		case err := <-overflowDone:
			t.Fatalf("callback queue accepted more than its bounded capacity: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		close(releaseCommits)
		require.NoError(t, <-overflowDone)
		require.NoError(t, writer.Wait())
		require.NoError(t, store.Close())
	})

	t.Run("duplicate key and timestamp", func(t *testing.T) {
		store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
		firstStarted := make(chan struct{})
		secondStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		var calls atomic.Int32
		store.beforeCommitForTest = func() {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseFirst
			case 2:
				close(secondStarted)
			}
		}

		writer := NewTxnWriterForStore(store)
		require.NoError(t, writer.SetAt([]byte("same"), []byte("first"), BitDeltaPosting, 7))
		select {
		case <-firstStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("first callback commit did not start")
		}
		require.NoError(t, writer.SetAt([]byte("same"), []byte("second"), BitDeltaPosting, 7))
		select {
		case <-secondStarted:
			t.Fatal("second callback commit overtook the blocked first commit")
		case <-time.After(100 * time.Millisecond):
		}
		close(releaseFirst)
		require.NoError(t, writer.Wait())
		select {
		case <-secondStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("second callback commit did not run after the first")
		}

		read := store.NewReadTxn(7)
		item, err := read.Get([]byte("same"))
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("second"), value)
		read.Discard()
		require.NoError(t, store.Close())
	})

	t.Run("callback then synchronous", func(t *testing.T) {
		store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
		firstStarted := make(chan struct{})
		secondStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		var calls atomic.Int32
		store.beforeCommitForTest = func() {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseFirst
			case 2:
				close(secondStarted)
			}
		}

		first := store.NewWriteTxn()
		require.NoError(t, first.SetEntry(Entry{Key: []byte("same"), Value: []byte("callback")}))
		firstDone := make(chan error, 1)
		require.NoError(t, first.CommitAt(7, func(err error) { firstDone <- err }))
		select {
		case <-firstStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("callback commit did not start")
		}

		second := store.NewWriteTxn()
		require.NoError(t, second.SetEntry(Entry{Key: []byte("same"), Value: []byte("synchronous")}))
		secondDone := make(chan error, 1)
		go func() { secondDone <- second.CommitAt(7, nil) }()
		select {
		case <-secondStarted:
			t.Fatal("synchronous commit overtook the blocked callback commit")
		case err := <-secondDone:
			t.Fatalf("synchronous commit returned before the earlier callback commit: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		close(releaseFirst)
		require.NoError(t, <-firstDone)
		require.NoError(t, <-secondDone)
		select {
		case <-secondStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("synchronous commit did not run after the callback commit")
		}

		read := store.NewReadTxn(7)
		item, err := read.Get([]byte("same"))
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("synchronous"), value)
		read.Discard()
		require.NoError(t, store.Close())
	})

	t.Run("synchronous then first callback", func(t *testing.T) {
		store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
		firstStarted := make(chan struct{})
		secondStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		var calls atomic.Int32
		store.beforeCommitForTest = func() {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseFirst
			case 2:
				close(secondStarted)
			}
		}

		first := store.NewWriteTxn()
		require.NoError(t, first.SetEntry(Entry{Key: []byte("same"), Value: []byte("synchronous")}))
		firstDone := make(chan error, 1)
		go func() { firstDone <- first.CommitAt(7, nil) }()
		select {
		case <-firstStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("synchronous commit did not start")
		}

		second := store.NewWriteTxn()
		require.NoError(t, second.SetEntry(Entry{Key: []byte("same"), Value: []byte("callback")}))
		secondDone := make(chan error, 1)
		callbackDone := make(chan error, 1)
		go func() {
			secondDone <- second.CommitAt(7, func(err error) { callbackDone <- err })
		}()
		select {
		case <-secondStarted:
			t.Fatal("first callback commit overtook the blocked synchronous commit")
		case err := <-secondDone:
			t.Fatalf("first callback commit returned before the synchronous handoff: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		close(releaseFirst)
		require.NoError(t, <-firstDone)
		require.NoError(t, <-secondDone)
		select {
		case <-secondStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("first callback commit did not run after the synchronous commit")
		}
		require.NoError(t, <-callbackDone)

		read := store.NewReadTxn(7)
		item, err := read.Get([]byte("same"))
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("callback"), value)
		read.Discard()
		require.NoError(t, store.Close())
	})
}

func TestTreeDBMutationBatchReleaseScrubsOwnedBytes(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	batch := &treeDBMutationBatch{
		mutations: make([]mvcc.Mutation, 0, 2),
		arena:     make([]byte, 0, 8),
	}
	first := batch.ownBytes(8)
	require.Equal(t, len(first), cap(first), "owned slices must not expose spare arena capacity")
	copy(first, "secret-1")
	second := batch.ownBytes(8 << 10)
	copy(second, "secret-2")
	batch.mutations = append(batch.mutations, mvcc.Mutation{
		Key: first,
	}, mvcc.Mutation{
		Key: second,
	})

	store.releaseMutationBatch(batch)
	require.Equal(t, make([]byte, len(first)), first, "grown-away arena must not retain caller data")
	require.Equal(t, make([]byte, len(second)), second, "oversized final arena must not retain caller data")
	require.NoError(t, store.Close())
}

func TestTreeDBStorePointErrorMappingAndCorruption(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	require.NoError(t, commitTreeDBMutations(store, 2, mvcc.Mutation{Key: []byte("deleted"), Delete: true}))

	read := store.NewReadTxn(2)
	_, err := read.Get([]byte("absent"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	_, err = read.Get([]byte("deleted"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	read.Discard()

	require.NoError(t, commitTreeDBMutations(store, 3, mvcc.Mutation{Key: []byte("corrupt"), Value: []byte("not-an-envelope")}))
	read = store.NewReadTxn(3)
	_, err = read.Get([]byte("corrupt"))
	require.ErrorIs(t, err, ErrTreeDBEnvelope)
	read.Discard()

	read = store.NewReadTxn(0)
	_, err = read.Get([]byte("anything"))
	require.ErrorIs(t, err, mvcc.ErrZeroTimestamp, "wrapped gomap errors must remain inspectable")
	it := read.NewIterator(IteratorOptions{})
	it.Rewind()
	require.False(t, it.Valid())
	require.ErrorIs(t, it.Error(), mvcc.ErrZeroTimestamp)
	it.Close()
	read.Discard()
}

func TestTreeDBStoreMatchesBadgerGoldenIteratorTrace(t *testing.T) {
	badgerDB := openPostingStoreTestDB(t)
	badgerStore := NewBadgerStore(badgerDB)
	treeStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)

	trace := []struct {
		ts      uint64
		entries []Entry
		deletes [][]byte
	}{
		{1, []Entry{{Key: []byte("a"), Value: []byte("a1"), UserMeta: BitDeltaPosting}}, nil},
		{2, []Entry{{Key: []byte("b"), Value: []byte("b2"), UserMeta: BitSchemaPosting}}, nil},
		{3, []Entry{{Key: []byte("a"), Value: []byte("a3"), UserMeta: BitCompletePosting, DiscardEarlierVersions: true}}, nil},
		{4, []Entry{{Key: []byte{0, 0xff, 'x'}, Value: []byte{0xff, 0, 1}, UserMeta: BitDeltaPosting}}, nil},
		{5, nil, [][]byte{[]byte("a")}},
		{6, []Entry{{Key: []byte("b"), Value: []byte("b6"), UserMeta: BitEmptyPosting}}, nil},
	}
	for _, step := range trace {
		commitTraceStep(t, badgerStore, step.ts, step.entries, step.deletes)
		commitTraceStep(t, treeStore, step.ts, step.entries, step.deletes)
	}

	for _, readTs := range []uint64{1, 3, 4, 5, 6, math.MaxUint64} {
		for _, key := range [][]byte{[]byte("a"), []byte("b"), {0, 0xff, 'x'}, []byte("missing")} {
			require.Equal(t, pointSnapshot(t, badgerStore, readTs, key), pointSnapshot(t, treeStore, readTs, key))
		}
		for _, opts := range []IteratorOptions{
			{AllVersions: true, PrefetchValues: true},
			{AllVersions: true, Reverse: true},
			{Prefix: []byte("b"), AllVersions: true},
			{Prefix: []byte("b"), AllVersions: true, Reverse: true},
			{},
			{Reverse: true},
		} {
			want := iteratorSnapshot(t, badgerStore, readTs, nil, opts, nil)
			got := iteratorSnapshot(t, treeStore, readTs, nil, opts, nil)
			require.Equal(t, want, got, "readTs=%d opts=%+v", readTs, opts)
		}
		for _, reverse := range []bool{false, true} {
			opts := IteratorOptions{AllVersions: true, Reverse: reverse}
			want := iteratorSnapshot(t, badgerStore, readTs, []byte("b"), opts, []byte("b"))
			got := iteratorSnapshot(t, treeStore, readTs, []byte("b"), opts, []byte("b"))
			require.Equal(t, want, got, "key iterator readTs=%d reverse=%v", readTs, reverse)
		}
	}
}

func TestTreeDBStoreItemCopyAndTombstoneValueMatchBadger(t *testing.T) {
	badgerStore := NewBadgerStore(openPostingStoreTestDB(t))
	treeStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	for _, store := range []Store{badgerStore, treeStore} {
		commitTraceStep(t, store, 1, []Entry{{Key: []byte("key"), Value: []byte("value")}}, nil)
		commitTraceStep(t, store, 2, nil, [][]byte{[]byte("key")})
	}

	collect := func(t *testing.T, store Store) []storeItemSnapshot {
		t.Helper()
		txn := store.NewReadTxn(2)
		defer txn.Discard()
		it := txn.NewIterator(IteratorOptions{AllVersions: true, PrefetchValues: true})
		defer it.Close()
		var got []storeItemSnapshot
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy([]byte("non-empty-key-dst"))
			value, err := item.ValueCopy([]byte("non-empty-value-dst"))
			require.NoError(t, err)
			require.NoError(t, item.Value(nil))
			got = append(got, storeItemSnapshot{
				Key: string(key), Value: string(value), Version: item.Version(),
				Deleted: item.IsDeletedOrExpired(),
			})
		}
		require.NoError(t, it.Error())
		return got
	}
	require.Equal(t, collect(t, badgerStore), collect(t, treeStore))
}

func TestTreeDBStoreIteratorStartsInvalidAndKeyIteratorForcesAllVersions(t *testing.T) {
	badgerStore := NewBadgerStore(openPostingStoreTestDB(t))
	treeStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	for _, store := range []Store{badgerStore, treeStore} {
		commitStoreEntry(t, store, 1, Entry{Key: []byte("key"), Value: []byte("one")})
		commitStoreEntry(t, store, 2, Entry{Key: []byte("key"), Value: []byte("two")})
	}
	for _, backend := range []struct {
		name  string
		store Store
	}{{"badger", badgerStore}, {"treedb", treeStore}} {
		t.Run(backend.name, func(t *testing.T) {
			txn := backend.store.NewReadTxn(2)
			defer txn.Discard()
			it := txn.NewIterator(IteratorOptions{})
			require.False(t, it.Valid())
			it.Close()

			keyIt := txn.NewKeyIterator([]byte("key"), IteratorOptions{AllVersions: false})
			defer keyIt.Close()
			var versions []uint64
			for keyIt.Rewind(); keyIt.Valid(); keyIt.Next() {
				versions = append(versions, keyIt.Item().Version())
			}
			require.NoError(t, keyIt.Error())
			require.Equal(t, []uint64{2, 1}, versions)
		})
	}
}

func TestTreeDBStoreExactKeyAndSeekMatchBadger(t *testing.T) {
	badgerStore := NewBadgerStore(openPostingStoreTestDB(t))
	treeStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	keys := [][]byte{{0}, []byte("a"), []byte("a\x00"), []byte("aa"), []byte("b")}
	for _, store := range []Store{badgerStore, treeStore} {
		for idx, key := range keys {
			commitStoreEntry(t, store, uint64(idx+1), Entry{Key: key, Value: []byte{byte(idx)}})
			commitStoreEntry(t, store, uint64(idx+10), Entry{Key: key, Value: []byte{byte(idx + 10)}})
		}
	}
	for _, key := range keys {
		for _, reverse := range []bool{false, true} {
			opts := IteratorOptions{Reverse: reverse, AllVersions: false}
			require.Equal(t,
				iteratorSnapshot(t, badgerStore, math.MaxUint64, key, opts, key),
				iteratorSnapshot(t, treeStore, math.MaxUint64, key, opts, key),
				"exact key=%x reverse=%v", key, reverse,
			)
		}
	}
	for _, seek := range [][]byte{[]byte{}, {0}, []byte("a"), []byte("a\x00"), []byte("ab"), []byte("z")} {
		for _, reverse := range []bool{false, true} {
			opts := IteratorOptions{Reverse: reverse}
			require.Equal(t,
				iteratorSnapshot(t, badgerStore, math.MaxUint64, nil, opts, seek),
				iteratorSnapshot(t, treeStore, math.MaxUint64, nil, opts, seek),
				"seek=%x reverse=%v", seek, reverse,
			)
		}
	}
}

func TestTreeDBStoreExactKeyIteratorAcceptsCodecMaximumKey(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	const gomapMVCCV1FixedKeyBytes = 9 + 2 + 8
	// Gomap's v1 MVCC codec uses a 9-byte namespace, a 2-byte terminator, and
	// an 8-byte timestamp. A nonzero logical key of this length fills its uint16
	// envelope exactly; the one-byte-over assertion below makes any upstream
	// layout change fail loudly until this pinned contract is updated.
	maxLogicalKey := bytes.Repeat([]byte{'x'}, math.MaxUint16-gomapMVCCV1FixedKeyBytes)
	read := store.NewReadTxn(1)
	defer read.Discard()
	it := read.NewKeyIterator(maxLogicalKey, IteratorOptions{})
	defer it.Close()
	it.Rewind()
	require.False(t, it.Valid())
	require.NoError(t, it.Error())

	overflow := append(bytes.Clone(maxLogicalKey), 'x')
	overflowIt := read.NewKeyIterator(overflow, IteratorOptions{})
	defer overflowIt.Close()
	overflowIt.Rewind()
	require.False(t, overflowIt.Valid())
	require.ErrorIs(t, overflowIt.Error(), mvcc.ErrInvalidKey)
}

func TestTreeDBStoreIteratorErrorIsSticky(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	require.NoError(t, commitTreeDBMutations(store, 1, mvcc.Mutation{
		Key: []byte("corrupt-iterator"), Value: []byte("not-an-envelope"),
	}))
	txn := store.NewReadTxn(1)
	defer txn.Discard()
	it := txn.NewIterator(IteratorOptions{AllVersions: true})
	defer it.Close()
	it.Rewind()
	require.False(t, it.Valid())
	require.ErrorIs(t, it.Error(), ErrTreeDBEnvelope)
	it.Seek([]byte("corrupt-iterator"))
	it.Rewind()
	require.False(t, it.Valid())
	require.ErrorIs(t, it.Error(), ErrTreeDBEnvelope)
}

func TestReadPostingListPropagatesIteratorError(t *testing.T) {
	injected := errors.New("injected iterator failure")
	_, err := readPostingList(
		x.DataKey(x.AttrInRootNamespace("iterator-error"), 1),
		errorPostingIterator{err: injected},
	)
	require.ErrorIs(t, err, injected)
}

type errorPostingIterator struct{ err error }

func (errorPostingIterator) Rewind()                    {}
func (errorPostingIterator) Seek([]byte)                {}
func (errorPostingIterator) Valid() bool                { return false }
func (errorPostingIterator) ValidForPrefix([]byte) bool { return false }
func (errorPostingIterator) Item() Item                 { return &treeDBItem{} }
func (errorPostingIterator) Next()                      {}
func (it errorPostingIterator) Error() error            { return it.err }
func (errorPostingIterator) Close()                     {}

func TestTreeDBStoreIteratorSeekSnapshotRewindAndOwnedItems(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	for ts, key := range []string{"a", "b", "c"} {
		commitStoreEntry(t, store, uint64(ts+1), Entry{Key: []byte(key), Value: []byte(key)})
	}
	read := store.NewReadTxn(math.MaxUint64)
	defer read.Discard()
	it := read.NewIterator(IteratorOptions{})
	defer it.Close()
	it.Rewind()
	require.True(t, it.Valid())
	first := it.Item()
	firstKey := first.KeyCopy(nil)
	firstValue, err := first.ValueCopy(nil)
	require.NoError(t, err)

	commitStoreEntry(t, store, 4, Entry{Key: []byte("aa"), Value: []byte("late")})
	it.Seek([]byte("aa"))
	require.True(t, it.Valid())
	require.Equal(t, []byte("b"), it.Item().Key(), "ordinary seek remains on the iterator snapshot")
	it.Rewind()
	require.Equal(t, firstKey, it.Item().Key())
	require.Equal(t, []byte("a"), firstValue, "items copied before refresh remain owned")
	var keys [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("aa"), []byte("b"), []byte("c")}, keys)
	require.NoError(t, it.Error())
}

func TestTreeDBStoreReadTxnTimestampVisibilityMatchesBadger(t *testing.T) {
	treeStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	stores := []struct {
		name  string
		store Store
	}{
		{name: "badger", store: NewBadgerStore(openPostingStoreTestDB(t))},
		{name: "treedb", store: treeStore},
	}
	for _, backend := range stores {
		t.Run(backend.name, func(t *testing.T) {
			read := backend.store.NewReadTxn(10)
			defer read.Discard()
			_, err := read.Get([]byte("late-backdated"))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)

			commitStoreEntry(t, backend.store, 5, Entry{Key: []byte("late-backdated"), Value: []byte("must-stay-invisible")})
			item, err := read.Get([]byte("late-backdated"))
			require.NoError(t, err, "managed stores expose a late backdated commit at the transaction timestamp")
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("must-stay-invisible"), value)

			it := read.NewIterator(IteratorOptions{})
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid(), "a newly-created iterator observes the same timestamp-visible commit")
			require.Equal(t, []byte("late-backdated"), it.Item().Key())
			require.NoError(t, it.Error())
		})
	}
}

func TestTreeDBStoreDiscardFloorPruneAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, opts := openTreeDBPostingStore(t, dir, TreeDBCommitDurable)
	for ts := uint64(1); ts <= 4; ts++ {
		commitStoreEntry(t, store, ts, Entry{Key: []byte("history"), Value: []byte{byte(ts)}})
	}
	require.NoError(t, store.AdvanceDiscardFloor(2))
	stats, err := store.PruneVersions(16)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.Pruned, uint64(1))
	read := store.NewReadTxn(2)
	_, err = read.Get([]byte("history"))
	require.ErrorIs(t, err, mvcc.ErrReadBeforeDiscardFloor)
	read.Discard()
	require.NoError(t, store.Close())
	require.True(t, store.IsClosed())
	require.NoError(t, store.Close())

	reopened, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	floor, err := reopened.DiscardFloor()
	require.NoError(t, err)
	require.Equal(t, uint64(2), floor)
	read = reopened.NewReadTxn(4)
	item, err := read.Get([]byte("history"))
	require.NoError(t, err)
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte{4}, value)
	read.Discard()
}

func TestTreeDBStoreLifecycleStatusAndDurabilityModes(t *testing.T) {
	durable, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	status := durable.Status()
	require.False(t, status.Closed)
	require.Equal(t, TreeDBCommitDurable, status.CommitMode)
	require.True(t, status.DurableCommits)
	require.Contains(t, status.DurabilityMode, "wal_on_sync")
	stats, err := durable.Stats()
	require.NoError(t, err)
	require.Equal(t, status.DurabilityMode, stats["treedb.durability_mode"])
	stats["detached"] = "mutated"
	fresh, err := durable.Stats()
	require.NoError(t, err)
	require.NotContains(t, fresh, "detached")
	_, err = durable.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{DryRun: true})
	require.NoError(t, err)
	_, err = durable.CompactStorage(context.Background(), treedb.CompactStorageOptions{})
	require.NoError(t, err)
	require.NoError(t, durable.Close())
	require.True(t, durable.Status().Closed)
	_, err = durable.Stats()
	require.ErrorIs(t, err, treedb.ErrClosed)
	_, err = durable.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{DryRun: true})
	require.ErrorIs(t, err, treedb.ErrClosed)
	_, err = durable.CompactStorage(context.Background(), treedb.CompactStorageOptions{})
	require.ErrorIs(t, err, treedb.ErrClosed)
	_, err = durable.DiscardFloor()
	require.ErrorIs(t, err, treedb.ErrClosed)
	err = durable.AdvanceDiscardFloor(1)
	require.ErrorIs(t, err, treedb.ErrClosed)
	_, err = durable.PruneVersions(1)
	require.ErrorIs(t, err, treedb.ErrClosed)

	dir := t.TempDir()
	relaxed, opts := openTreeDBPostingStore(t, dir, TreeDBCommitRelaxed)
	commitStoreEntry(t, relaxed, 7, Entry{Key: []byte("relaxed"), Value: []byte("reopen")})
	require.NoError(t, relaxed.Close())
	reopened, err := OpenTreeDBStore(opts, TreeDBCommitRelaxed)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	read := reopened.NewReadTxn(7)
	defer read.Discard()
	item, err := read.Get([]byte("relaxed"))
	require.NoError(t, err)
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("reopen"), value)
	require.NoError(t, reopened.Close())

	durableOptUp, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	commitStoreEntry(t, durableOptUp, 8, Entry{Key: []byte("durable-opt-up"), Value: []byte("synced")})
	require.NoError(t, durableOptUp.Close())
	durableOptUp, err = OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	read = durableOptUp.NewReadTxn(8)
	item, err = read.Get([]byte("durable-opt-up"))
	require.NoError(t, err)
	value, err = item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("synced"), value)
	read.Discard()
	require.NoError(t, durableOptUp.Close())
	_, err = OpenTreeDBStore(opts, TreeDBCommitMode(99))
	require.ErrorIs(t, err, ErrTreeDBCommitMode)
}

func TestTreeDBStoreDurableCrashReopen(t *testing.T) {
	testTreeDBStoreDurableCrashReopen(t, treedb.ProfileCommandWALDurable)
}

func TestTreeDBStoreRelaxedProfileDurableOptUpCrashReopen(t *testing.T) {
	testTreeDBStoreDurableCrashReopen(t, treedb.ProfileCommandWALRelaxed)
}

func testTreeDBStoreDurableCrashReopen(t *testing.T, profile treedb.Profile) {
	if os.Getenv("DGRAPH_TREEDB_CRASH_CHILD") == t.Name() {
		if err := writeTreeDBCrashFixture(os.Getenv("DGRAPH_TREEDB_CRASH_DIR"), profile); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		// Deliberately skip Close and all deferred cleanup.
		os.Exit(0)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(),
		"DGRAPH_TREEDB_CRASH_CHILD="+t.Name(),
		"DGRAPH_TREEDB_CRASH_DIR="+dir,
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "crash writer failed: %s", output)

	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	store, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	read := store.NewReadTxn(41)
	defer read.Discard()
	item, err := read.Get([]byte("crash-fixture"))
	require.NoError(t, err)
	require.Equal(t, uint64(41), item.Version())
	require.Equal(t, byte(0xa5), item.UserMeta())
	require.True(t, item.DiscardEarlierVersions())
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("durable-envelope-payload")), sha256.Sum256(value))
}

func writeTreeDBCrashFixture(dir string, profile treedb.Profile) error {
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	store, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	if err != nil {
		return err
	}
	txn := store.NewWriteTxn()
	if err := txn.SetEntry(Entry{
		Key: []byte("crash-fixture"), Value: []byte("durable-envelope-payload"),
		UserMeta: 0xa5, DiscardEarlierVersions: true,
	}); err != nil {
		return err
	}
	return txn.CommitAt(41, nil)
}

func TestTreeDBStoreRandomizedPointTraceMatchesBadger(t *testing.T) {
	badgerStore := NewBadgerStore(openPostingStoreTestDB(t))
	treeStore, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	rng := rand.New(rand.NewSource(21))
	keys := [][]byte{[]byte("a"), []byte("a\x00b"), {0xff, 0}, []byte("prefix/one"), []byte("prefix/two")}
	for ts := uint64(1); ts <= 80; ts++ {
		entryCount := 1 + rng.Intn(3)
		entries := make([]Entry, 0, entryCount)
		deletes := make([][]byte, 0, entryCount)
		for i := 0; i < entryCount; i++ {
			key := append([]byte(nil), keys[rng.Intn(len(keys))]...)
			if rng.Intn(5) == 0 {
				deletes = append(deletes, key)
				continue
			}
			entries = append(entries, Entry{
				Key: key, Value: []byte{byte(ts), byte(i), byte(rng.Intn(256))},
				UserMeta: byte(rng.Intn(4)), DiscardEarlierVersions: rng.Intn(7) == 0,
			})
		}
		commitTraceStep(t, badgerStore, ts, entries, deletes)
		commitTraceStep(t, treeStore, ts, entries, deletes)
		for i := 0; i < 3; i++ {
			readTs := uint64(1 + rng.Intn(int(ts)))
			key := keys[rng.Intn(len(keys))]
			require.Equal(t, pointSnapshot(t, badgerStore, readTs, key), pointSnapshot(t, treeStore, readTs, key), "commit=%d read=%d key=%x", ts, readTs, key)
		}
	}
}

func TestTreeDBStoreConcurrentReaders(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	commitStoreEntry(t, store, 1, Entry{Key: []byte("key"), Value: []byte("one")})
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				read := store.NewReadTxn(math.MaxUint64)
				item, err := read.Get([]byte("key"))
				require.NoError(t, err)
				_, err = item.ValueCopy(nil)
				require.NoError(t, err)
				it := read.NewIterator(IteratorOptions{AllVersions: true})
				for it.Rewind(); it.Valid(); it.Next() {
					_, err = it.Item().ValueCopy(nil)
					require.NoError(t, err)
				}
				require.NoError(t, it.Error())
				it.Close()
				read.Discard()
			}
		}()
	}
	wg.Wait()
}

type storeItemSnapshot struct {
	Key     string
	Value   string
	Meta    byte
	Version uint64
	Deleted bool
	Discard bool
	Err     string
}

func pointSnapshot(t *testing.T, store Store, readTs uint64, key []byte) storeItemSnapshot {
	t.Helper()
	txn := store.NewReadTxn(readTs)
	defer txn.Discard()
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return storeItemSnapshot{Err: badger.ErrKeyNotFound.Error()}
	}
	require.NoError(t, err)
	return snapshotStoreItem(t, item)
}

func iteratorSnapshot(t *testing.T, store Store, readTs uint64, exactKey []byte, opts IteratorOptions, seek []byte) []storeItemSnapshot {
	t.Helper()
	txn := store.NewReadTxn(readTs)
	defer txn.Discard()
	var it Iterator
	if exactKey != nil {
		it = txn.NewKeyIterator(exactKey, opts)
	} else {
		it = txn.NewIterator(opts)
	}
	defer it.Close()
	if seek != nil {
		it.Seek(seek)
	} else {
		it.Rewind()
	}
	var out []storeItemSnapshot
	for ; it.Valid(); it.Next() {
		out = append(out, snapshotStoreItem(t, it.Item()))
	}
	require.NoError(t, it.Error())
	return out
}

func snapshotStoreItem(t *testing.T, item Item) storeItemSnapshot {
	t.Helper()
	snapshot := storeItemSnapshot{
		Key: string(item.KeyCopy(nil)), Meta: item.UserMeta(), Version: item.Version(),
		Deleted: item.IsDeletedOrExpired(), Discard: item.DiscardEarlierVersions(),
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		snapshot.Err = err.Error()
	} else {
		snapshot.Value = string(value)
	}
	return snapshot
}

func commitTraceStep(t *testing.T, store Store, timestamp uint64, entries []Entry, deletes [][]byte) {
	t.Helper()
	txn := store.NewWriteTxn()
	defer txn.Discard()
	for _, entry := range entries {
		require.NoError(t, txn.SetEntry(entry))
	}
	for _, key := range deletes {
		require.NoError(t, txn.Delete(key))
	}
	done := make(chan error, 1)
	require.NoError(t, txn.CommitAt(timestamp, func(err error) { done <- err }))
	require.NoError(t, <-done)
}

func commitTreeDBMutations(store *TreeDBStore, timestamp uint64, mutations ...mvcc.Mutation) error {
	return store.mvcc.CommitAt(timestamp, mutations, store.commitMode)
}

func openTreeDBPostingStore(t *testing.T, dir string, mode TreeDBCommitMode) (*TreeDBStore, treedb.Options) {
	t.Helper()
	profile := treedb.ProfileCommandWALDurable
	if mode == TreeDBCommitRelaxed {
		profile = treedb.ProfileCommandWALRelaxed
	}
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	store, err := OpenTreeDBStore(opts, mode)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, opts
}

func TestTreeDBStoreBinaryKeyIdentity(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitDurable)
	commitStoreEntry(t, store, 1, Entry{Key: nil, Value: []byte("nil")})
	commitStoreEntry(t, store, 2, Entry{Key: []byte{}, Value: []byte("empty-alias")})
	keys := [][]byte{{0}, {0, 0}, {0xff}, {0xff, 0}}
	for idx, key := range keys {
		commitStoreEntry(t, store, uint64(idx+3), Entry{Key: key, Value: bytes.Repeat([]byte{byte(idx)}, idx+1)})
	}
	read := store.NewReadTxn(math.MaxUint64)
	defer read.Discard()
	for _, key := range [][]byte{nil, {}} {
		item, err := read.Get(key)
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("empty-alias"), value)
	}
	for idx, key := range keys {
		item, err := read.Get(key)
		require.NoError(t, err)
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(idx)}, idx+1), value)
	}
	exact := read.NewKeyIterator([]byte{}, IteratorOptions{})
	defer exact.Close()
	var exactVersions []uint64
	for exact.Rewind(); exact.Valid(); exact.Next() {
		require.Empty(t, exact.Item().Key())
		exactVersions = append(exactVersions, exact.Item().Version())
	}
	require.Equal(t, []uint64{2, 1}, exactVersions, "empty exact-key bounds must exclude binary-prefix siblings")
	require.NoError(t, exact.Error())
}
