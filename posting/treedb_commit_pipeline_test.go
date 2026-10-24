/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/x"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
	"github.com/stretchr/testify/require"
)

func openPipelinePostingStore(t *testing.T, dir string) (*TreeDBStore, treedb.Options) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	store, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NotZero(t, store.writeWorkLimit)
	return store, opts
}

func TestTreeDBDurablePipelineStagesAndInstallsDuringPhysicalSync(t *testing.T) {
	for _, seal := range []string{"producer-fence", "ownership-full"} {
		t.Run(seal, func(t *testing.T) { testTreeDBDurablePipelinePhysicalSync(t, seal) })
	}
}

func testTreeDBDurablePipelinePhysicalSync(t *testing.T, seal string) {
	store, opts := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(128)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 128)
	require.NoError(t, err)
	wrapped := WithCommitEvents(store, bus)
	entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	released := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	release := func(i int) { once[i].Do(func() { close(released[i]) }) }
	var syncs, appends, stages, preparations atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if (e.Resource != durabilitycut.ResourceCommandWAL && e.Point != durabilitycut.BeforeCOWPreparation) ||
			!strings.HasPrefix(e.Root+string(filepath.Separator), opts.Dir+string(filepath.Separator)) {
			return nil
		}
		switch e.Point {
		case durabilitycut.AfterLogicalMVCCStaging:
			stages.Add(1)
		case durabilitycut.BeforeCOWPreparation:
			preparations.Add(1)
		case durabilitycut.AfterDependencyAppend:
			appends.Add(1)
		case durabilitycut.BeforeDependencyFileSync:
			i := int(syncs.Add(1)) - 1
			if i < 2 {
				close(entered[i])
				<-released[i]
			}
		}
		return nil
	})
	defer restore()
	defer release(0)
	defer release(1)
	first := NewTxnWriterForStore(wrapped)
	require.NoError(t, first.SetAt([]byte("shared"), []byte("first"), 0, 1))
	select {
	case <-entered[0]:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not enter the physical file sync")
	}
	stagesBefore, preparesBefore := stages.Load(), preparations.Load()
	successor := NewTxnWriterForStore(wrapped)
	count := 2
	if seal == "ownership-full" {
		count = treeDBCommitOwnershipLimit - 1
	}
	for i := 0; i < count; i++ {
		ts := uint64(i + 2)
		require.NoError(t, successor.SetAt([]byte("shared"), []byte(fmt.Sprint(ts)), 0, ts))
		if i < 2 {
			want := stagesBefore + int32(i+1)
			awaitTreeDBCohort(t, "successor native operands staged before seal", func() bool { return stages.Load() >= want })
			require.Equal(t, int32(1), appends.Load(), "an open successor must not append on queue emptiness")
			require.Equal(t, preparesBefore, preparations.Load(), "root-dependent COW work is built once after seal")
		}
	}
	flushed := make(chan error, 1)
	if seal == "producer-fence" {
		go func() { flushed <- successor.Flush() }()
	}
	awaitTreeDBCohort(t, "successor ordered append/install while predecessor sync is held", func() bool { return store.durableOverlap.Load() == 1 })
	require.Equal(t, int32(2), appends.Load())
	require.Equal(t, preparesBefore+1, preparations.Load())
	require.Equal(t, int32(1), syncs.Load())
	lastTs := uint64(count + 1)
	for ts := uint64(2); ts <= lastTs; ts++ {
		require.Equal(t, fmt.Sprint(ts), pointSnapshot(t, store, ts, []byte("shared")).Value)
	}
	select {
	case event := <-events:
		t.Fatalf("event acknowledged before physical sync: %+v", event)
	default:
	}
	release(0)
	select {
	case <-entered[1]:
	case <-time.After(5 * time.Second):
		t.Fatal("completion owner did not drive successor physical sync")
	}
	if seal == "ownership-full" {
		go func() { flushed <- successor.Flush() }()
	}
	require.NoError(t, first.Flush())
	select {
	case err := <-flushed:
		t.Fatalf("successor borrowed predecessor durability certificate: %v", err)
	default:
	}
	release(1)
	require.NoError(t, receiveTreeDBCohortResult(t, flushed))
	for sequence := uint64(1); sequence <= lastTs; sequence++ {
		select {
		case event := <-events:
			require.Equal(t, sequence, event.Sequence)
			require.Equal(t, sequence, event.CommitTs)
		case <-time.After(5 * time.Second):
			t.Fatal("durable completion event was lost")
		}
	}
	awaitTreeDBCohort(t, "both original record budgets drain", func() bool { return len(store.commitSlots) == 0 && store.commitStagedBytes.Load() == 0 })
	require.Equal(t, uint64(2), store.durableCohorts.Load())
	require.Equal(t, uint64(count), store.durableCohortMax.Load())
}

// Exercise CommitToDisk's real key cursor and ErrTxnTooBig protocol at the
// unchanged default engine limits. No accepted engine command is subdivided.
func TestTreeDBCommitToDiskLargeTransactionChunksAndReopens(t *testing.T) {
	store, opts := openPipelinePostingStore(t, t.TempDir())
	const keys = 100000
	const commitTs = uint64(20)
	txn := NewTxn(10)
	attr := x.AttrInRootNamespace("large_chunk_recovery")
	for i := 0; i < keys; i++ {
		key := x.DataKey(attr, uint64(i+1))
		value := bytes.Repeat([]byte{byte(i)}, 64)
		binary.BigEndian.PutUint64(value, uint64(i))
		txn.cache.deltas[string(key)] = value
	}
	writer := NewTxnWriterForStore(store)
	require.NoError(t, txn.CommitToDisk(writer, commitTs))
	require.NoError(t, writer.Flush())
	require.Greater(t, store.durableRequests.Load(), uint64(1), "the original 100k command must be bounded before engine admission")
	awaitTreeDBCohort(t, "all large-transaction chunk ownership drains", func() bool { return len(store.commitSlots) == 0 })
	verify := func(s *TreeDBStore) {
		read := s.NewReadTxn(commitTs)
		defer read.Discard()
		for i := 0; i < keys; i++ {
			item, err := read.Get(x.DataKey(attr, uint64(i+1)))
			if err != nil {
				t.Fatalf("key %d: %v", i, err)
			}
			value, err := item.ValueCopy(nil)
			if err != nil || len(value) != 64 || binary.BigEndian.Uint64(value) != uint64(i) ||
				item.Version() != commitTs || item.UserMeta() != BitDeltaPosting {
				t.Fatalf("key %d: value=%x version=%d meta=%d error=%v", i, value, item.Version(), item.UserMeta(), err)
			}
		}
	}
	verify(store)
	require.NoError(t, store.Close())
	reopened, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	verify(reopened)
	t.Logf("100k keys at original timestamp; chunks=%d groups=%d max_requests=%d", store.durableRequests.Load(), store.durableCohorts.Load(), store.durableCohortMax.Load())
}

func TestTreeDBChunkRefusalPreservesPrefixAndOversizedFirstIsTerminal(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	cost := store.db.EstimateMVCCWriteWork(8, treeDBEnvelopeHeader+8)
	// This focused boundary control narrows only the adapter's producer window;
	// the large-transaction control above exercises unchanged production limits.
	store.writeWorkLimit = cost
	txn := store.NewWriteTxn().(*treeDBWriteTxn)
	require.NoError(t, txn.SetEntry(Entry{Key: []byte("prefix01"), Value: []byte("value001")}))
	beforeWork, beforeBytes := txn.batch.work, txn.batch.ownedBytes()
	require.ErrorIs(t, txn.SetEntry(Entry{Key: []byte("prefix02"), Value: []byte("value002")}), badger.ErrTxnTooBig)
	require.Equal(t, beforeWork, txn.batch.work)
	require.Equal(t, beforeBytes, txn.batch.ownedBytes())
	require.Len(t, txn.batch.mutations, 1)
	require.NoError(t, txn.CommitAt(1, nil))
	require.Equal(t, "value001", pointSnapshot(t, store, 1, []byte("prefix01")).Value)
	oversized := store.NewWriteTxn()
	err := oversized.SetEntry(Entry{Key: []byte("oversize"), Value: bytes.Repeat([]byte("x"), 4096)})
	require.ErrorIs(t, err, ErrTreeDBEntryTooLarge)
	require.NotErrorIs(t, err, badger.ErrTxnTooBig, "an empty prefix must not enter CommitToDisk's retry loop")
	require.ErrorIs(t, oversized.CommitAt(2, nil), ErrTreeDBEntryTooLarge)
	oversized.Discard()
}

func TestTreeDBCommitStagingSharedAllowanceAndExclusiveOversize(t *testing.T) {
	store := &TreeDBStore{}
	require.True(t, store.reserveCommitStaging(3<<20))
	require.False(t, store.reserveCommitStaging(2<<20))
	require.True(t, store.reserveCommitStaging(1<<20))
	require.Equal(t, int64(treeDBCommitCohortBytes), store.commitStagedBytes.Load())
	require.False(t, store.reserveCommitStaging(1))
	store.commitStagedBytes.Store(0)
	require.True(t, store.reserveCommitStaging(6<<20))
	require.False(t, store.reserveCommitStaging(1), "oversized command must be exclusive")
}

// Write transactions own their buffers before commit admission. Shaping must
// therefore use immutable captured parameters, even if Close releases the DB.
func TestTreeDBEntryShapingConcurrentClose(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	const writers = 32
	txns := make([]WriteTxn, writers)
	for i := range txns {
		txns[i] = store.NewWriteTxn()
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(writers)
	for _, txn := range txns {
		go func() {
			defer workers.Done()
			defer txn.Discard()
			<-start
			for i := 0; i < 100; i++ {
				if err := txn.SetEntry(Entry{Key: []byte("owned-key"), Value: []byte("owned-value")}); err != nil {
					return
				}
			}
		}()
	}
	close(start)
	require.NoError(t, store.Close())
	workers.Wait()
	// The copied estimator remains independent of released cache ownership.
	require.Greater(t, store.writeWorkEstimator.Estimate(9, 16), uint64(1))
	require.LessOrEqual(t, store.writeWorkEstimator.Estimate(9, 16), store.writeWorkLimit)
}

func TestTreeDBDurablePipelineUnsupportedInstrumentationKeepsFIFO(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.PublicBatchWriteSyncPhaseStats = true
	store, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NotZero(t, store.writeWorkLimit, "physical work shaping is independent of staged-route eligibility")
	require.False(t, store.stagedCommitGroups)
	w := NewTxnWriterForStore(store)
	for ts := uint64(1); ts <= 3; ts++ {
		require.NoError(t, w.SetAt([]byte("history"), []byte(fmt.Sprint(ts)), 0, ts))
	}
	require.NoError(t, w.Flush())
	for ts := uint64(1); ts <= 3; ts++ {
		require.Equal(t, fmt.Sprint(ts), pointSnapshot(t, store, ts, []byte("history")).Value)
	}
	stats, err := store.Stats()
	require.NoError(t, err)
	require.Equal(t, "fifo", stats["dgraph.treedb.commit_pipeline"])
}
