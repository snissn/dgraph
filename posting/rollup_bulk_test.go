/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
	"github.com/stretchr/testify/require"
)

// This test exercises the original producer, real List.Rollup, production event
// wrapper, FIFO admission and native durable backend, not a concatenation helper.
func TestRollupProcessNativeRoute(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	oldStore, oldOracle := postingStore, o
	bus := NewCommitEventBus(1)
	postingStore = WithCommitEvents(store, bus)
	defer bus.Close()
	o = &oracle{}
	o.init()
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 1000})
	defer func() { postingStore = oldStore; o = oldOracle }()
	keys := make([][]byte, 16)
	for i := range keys {
		keys[i] = x.DataKey(x.AttrInRootNamespace("native-rollup"), uint64(i+1))
		pack := codec.Encode([]uint64{uint64(i + 1)}, blockSize)
		wire := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
		commitStoreEntry(t, store, 10, Entry{Key: keys[i], Value: wire, UserMeta: BitCompletePosting})
		codec.FreePack(pack)
	}
	before := store.durableRequests.Load()
	producedBefore, completedBefore := rollupWork.producedKVs.Load(), rollupWork.completedKVs.Load()
	ir := &incrRollupi{priorityKeys: make([]*pooledKeys, 2)}
	for i := range ir.priorityKeys {
		ir.priorityKeys[i] = &pooledKeys{keysCh: make(chan *[][]byte, 1), keysPool: &sync.Pool{New: func() interface{} { return new([][]byte) }}}
	}
	sent := append([][]byte(nil), keys...)
	ir.priorityKeys[0].keysCh <- &sent
	closeRun := z.NewCloser(1)
	var timestamps atomic.Int32
	go ir.Process(closeRun, func(readonly bool) uint64 {
		if readonly {
			panic("read-only rollup timestamp")
		}
		return 100 + uint64(timestamps.Add(1))
	})
	awaitTreeDBCohort(t, "all original per-key timestamps", func() bool { return timestamps.Load() == 16 })
	// Completion is visible only once the producer has returned its original batch.
	awaitTreeDBCohort(t, "completed original batch", func() bool { return len(store.commitSlots) == 0 && store.durableRequests.Load() > before })
	closeRun.SignalAndWait()
	require.Equal(t, int32(16), timestamps.Load())
	require.Equal(t, uint64(16), rollupWork.producedKVs.Load()-producedBefore)
	require.Equal(t, uint64(16), rollupWork.completedKVs.Load()-completedBefore)
	require.Equal(t, uint64(1), store.durableRequests.Load()-before, "16 rolled-up roots must be one native FIFO request")
	for _, key := range keys {
		snap := pointSnapshot(t, store, 1000, key)
		require.Equal(t, uint64(11), snap.Version)
	}
}

func TestRollupNativeVersionsDuplicatesAndReopen(t *testing.T) {
	for _, backend := range []string{"treedb", "badger"} {
		t.Run(backend, func(t *testing.T) {
			var store Store
			var reopen func()
			if backend == "treedb" {
				s, opts := openPipelinePostingStore(t, t.TempDir())
				store = s
				reopen = func() {
					require.NoError(t, s.Close())
					next, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
					require.NoError(t, err)
					store = next
					t.Cleanup(func() { require.NoError(t, next.Close()) })
				}
			} else {
				opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithSyncWrites(true).WithNumVersionsToKeep(math.MaxInt64)
				db, err := badger.OpenManaged(opts)
				require.NoError(t, err)
				store = NewBadgerStore(db)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				reopen = func() {
					require.NoError(t, db.Close())
					db, err = badger.OpenManaged(opts)
					require.NoError(t, err)
					store = NewBadgerStore(db)
				}
			}
			w := &rollupWriter{store: store}
			for _, kv := range []*bpb.KV{{Key: []byte("same"), Value: []byte("old"), Version: 10}, {Key: []byte("same"), Value: []byte("new"), Version: 20}, {Key: []byte("same"), Value: []byte("last"), Version: 20}, {Key: []byte("meta"), Value: []byte("full"), Version: 30, UserMeta: []byte{BitCompletePosting}}} {
				require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{kv}}))
			}
			require.NoError(t, w.Flush())
			reopen()
			for _, tc := range []struct {
				ts    uint64
				value string
			}{{10, "old"}, {20, "last"}} {
				txn := store.NewReadTxn(tc.ts)
				item, err := txn.Get([]byte("same"))
				require.NoError(t, err)
				value, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, tc.value, string(value))
				txn.Discard()
			}
			txn := store.NewReadTxn(30)
			defer txn.Discard()
			item, err := txn.Get([]byte("meta"))
			require.NoError(t, err)
			require.True(t, item.DiscardEarlierVersions())
			require.Equal(t, BitCompletePosting, item.UserMeta())
		})
	}
}

func TestRollupChunkBackpressureAndAmbiguousError(t *testing.T) {
	s := &rollupFakeBulk{Store: newFakeCommitStore(), entered: make(chan int, 2), release: make(chan struct{}), err: errors.New("ambiguous sync")}
	w := &rollupWriter{store: s}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 65; i++ {
			if err := w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte(fmt.Sprint(i)), Value: []byte("v"), Version: uint64(i + 1)}}}); err != nil {
				done <- err
				return
			}
		}
		done <- w.Flush()
	}()
	require.Equal(t, 64, <-s.entered)
	select {
	case err := <-done:
		t.Fatalf("early completion: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(s.release)
	require.ErrorIs(t, <-done, s.err)
	require.Equal(t, 1, s.calls)
	require.Nil(t, w.target)
	require.Zero(t, w.n)
}

type rollupFakeBulk struct {
	Store
	entered chan int
	release chan struct{}
	calls   int
	err     error
}

func (s *rollupFakeBulk) writeRollupBatch(kvs []*bpb.KV, _ uint64) error {
	s.calls++
	s.entered <- len(kvs)
	<-s.release
	return s.err
}

func TestRollupEventDepthOneAndCreationBoundary(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(1)
	defer bus.Close()
	w := &rollupWriter{store: WithCommitEvents(s, bus)}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("before"), Version: 1}}}))
	events, err := bus.Subscribe(context.Background(), 4)
	require.NoError(t, err)
	for i := 2; i <= 4; i++ {
		require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte(fmt.Sprint(i)), Value: []byte("v"), Version: uint64(i)}}}))
	}
	require.NoError(t, w.Flush())
	for i := 2; i <= 4; i++ {
		select {
		case e := <-events:
			require.Equal(t, uint64(i), e.CommitTs)
		case <-time.After(time.Second):
			t.Fatal("event admission deadlock")
		}
	}
	select {
	case e := <-events:
		t.Fatalf("retroactive or duplicate event: %+v", e)
	default:
	}
}

func TestRollupFIFOFailureNoRetry(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	want := errors.New("storage refused")
	var calls atomic.Int32
	s.commitGroupsForTest = func(groups []mvcc.CommitGroup, _ mvcc.CommitMode) error {
		calls.Add(1)
		return fmt.Errorf("%w: %w", mvcc.ErrStorage, want)
	}
	w := &rollupWriter{store: s}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("a"), Version: 1}, {Key: []byte("b"), Version: 2}}}))
	require.ErrorIs(t, w.Flush(), want)
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, len(s.commitSlots))
	require.Zero(t, s.commitStagedBytes.Load())
}

func TestRollupWorkBoundAndRootFence(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	s.writeWorkLimit = s.writeWorkEstimator.Estimate(1, treeDBEnvelopeHeader+1)
	w := &rollupWriter{store: s}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("a"), Value: []byte("v"), Version: 1}, {Key: []byte("b"), Value: []byte("v"), Version: 2}}}))
	require.NoError(t, w.Flush())
	require.Equal(t, uint64(2), s.durableRequests.Load())
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("c"), Value: []byte("v"), Version: 3}}}))
	s.rootEpoch.Add(1) // Same original root-install epoch admission discriminator.
	require.ErrorIs(t, w.Flush(), ErrTreeDBTxnClosed)
	require.Zero(t, len(s.commitSlots))
	require.Zero(t, s.commitStagedBytes.Load())
}

func TestRollupValidationPartialPrefixAndClose(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	w := &rollupWriter{store: s}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("good"), Value: []byte("v"), Version: 1}, {Key: make([]byte, 65536), Version: 2}, {Key: []byte("later"), Value: []byte("v"), Version: 3}}}))
	require.ErrorIs(t, w.Flush(), mvcc.ErrInvalidKey)
	// Validation-only subdivision preserves valid neighbours; storage never retries.
	require.Equal(t, "v", pointSnapshot(t, s, 3, []byte("good")).Value)
	require.Equal(t, "v", pointSnapshot(t, s, 3, []byte("later")).Value)
	require.NoError(t, s.Close())
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("closed"), Version: 4}}}))
	require.Error(t, w.Flush())
	require.Zero(t, len(s.commitSlots))
}

func TestRollupFIFOOwnCompletionAndClose(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	release := holdTreeDBCohortRequest(t, s, 1)
	done := make(chan error, 1)
	go func() {
		w := &rollupWriter{store: s}
		err := w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("same"), Value: []byte("one"), Version: 1}, {Key: []byte("same"), Value: []byte("two"), Version: 2}}})
		if err == nil {
			err = w.Flush()
		}
		done <- err
	}()
	awaitTreeDBCohort(t, "bulk original FIFO admission", func() bool { return s.durableRequests.Load() == 1 })
	foreground := s.NewWriteTxn()
	require.NoError(t, foreground.SetEntry(Entry{Key: []byte("same"), Value: []byte("three")}))
	ack := make(chan error, 1)
	require.NoError(t, foreground.CommitAt(3, func(err error) { ack <- err }))
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	awaitTreeDBCohort(t, "Close waits for admitted bulk", func() bool { return s.closed.Load() })
	select {
	case err := <-done:
		t.Fatalf("early bulk ACK %v", err)
	case err := <-closed:
		t.Fatalf("early Close %v", err)
	default:
	}
	release()
	require.NoError(t, <-done)
	require.NoError(t, <-ack)
	require.NoError(t, <-closed)
	require.Empty(t, s.commitSlots)
	require.Zero(t, s.commitStagedBytes.Load())
}

func TestRollupSplitRootsPreserveWireAndVersions(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	oldMax := maxListSize
	maxListSize = 64
	defer func() { maxListSize = oldMax }()
	l, _, _ := ownershipBorrowedList(t)
	posts := make([]*pb.Posting, 0, 2044)
	for uid := uint64(5); uid <= 2048; uid++ {
		posts = append(posts, &pb.Posting{Uid: uid, Op: Set})
	}
	l.mutationMap.insertCommittedPostings(&pb.PostingList{CommitTs: 11, Postings: posts})
	kvs, err := l.Rollup(nil, 100)
	require.NoError(t, err)
	require.Greater(t, len(kvs), 2)
	w := &rollupWriter{store: s}
	require.NoError(t, w.Write(&bpb.KVList{Kv: kvs}))
	require.NoError(t, w.Flush())
	for _, kv := range kvs {
		txn := s.NewReadTxn(100)
		item, err := txn.Get(kv.Key)
		require.NoError(t, err)
		wire, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, kv.Value, wire)
		require.Equal(t, kv.Version, item.Version())
		txn.Discard()
	}
}

func TestRollupUnsupportedScalarAndEventFailure(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	unsupported := &rollupUnsupported{Store: s}
	w := &rollupWriter{store: unsupported}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("one"), Version: 1}, {Key: []byte("two"), Version: 2}}}))
	require.NoError(t, w.Flush())
	require.Equal(t, int32(2), unsupported.created.Load())
	require.Equal(t, uint64(2), s.durableRequests.Load())
	bus := NewCommitEventBus(1)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	w = &rollupWriter{store: WithCommitEvents(unsupported, bus)}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("closed"), Version: 3}}}))
	require.Error(t, w.Flush(), "original scalar fallback reports asynchronous completion errors at Flush")
	select {
	case e := <-events:
		t.Fatalf("failed event published: %+v", e)
	default:
	}
}

type rollupUnsupported struct {
	Store
	created atomic.Int32
}

func (s *rollupUnsupported) NewWriteTxn() WriteTxn { s.created.Add(1); return s.Store.NewWriteTxn() }

func TestRollupEncodedByteBound(t *testing.T) {
	s := &rollupCountingBulk{Store: newFakeCommitStore()}
	w := &rollupWriter{store: s}
	value := make([]byte, rollupChunkPayloadBytes/2)
	for i := 0; i < 3; i++ {
		require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte(fmt.Sprint(i)), Value: value, Version: uint64(i + 1)}}}))
	}
	require.NoError(t, w.Flush())
	require.Equal(t, 3, s.chunks)
	require.LessOrEqual(t, s.maxBytes, rollupChunkPayloadBytes)
	// One oversized original entry is atomic and follows backend admission.
	large := make([]byte, rollupChunkBytes+1)
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("large"), Value: large, Version: 4}}}))
	require.Equal(t, 4, s.chunks)
	require.Zero(t, w.n)
}

type rollupCountingBulk struct {
	Store
	chunks, maxBytes int
}

func (s *rollupCountingBulk) writeRollupBatch(kvs []*bpb.KV, _ uint64) error {
	s.chunks++
	n := int(rollupBytes(kvs))
	if n > s.maxBytes {
		s.maxBytes = n
	}
	return nil
}

func TestRollupNativePhysicalSyncCompletion(t *testing.T) {
	s, opts := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 4)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var syncs atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && strings.HasPrefix(e.Root+string(filepath.Separator), opts.Dir+string(filepath.Separator)) {
			if syncs.Add(1) == 1 {
				close(entered)
				<-release
			}
		}
		return nil
	})
	defer restore()
	done := make(chan error, 1)
	go func() {
		w := &rollupWriter{store: WithCommitEvents(s, bus)}
		err := w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("shared"), Value: []byte("one"), Version: 1}, {Key: []byte("shared"), Value: []byte("two"), Version: 2}}})
		if err == nil {
			err = w.Flush()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native rollup did not reach actual sync")
	}
	select {
	case err := <-done:
		t.Fatalf("native rollup ACK before sync: %v", err)
	default:
	}
	select {
	case e := <-events:
		t.Fatalf("event before physical sync: %+v", e)
	default:
	}
	require.Equal(t, uint64(1), s.durableRequests.Load())
	require.NotZero(t, s.commitStagedBytes.Load())
	unblock()
	require.NoError(t, <-done)
	require.Equal(t, int32(1), syncs.Load())
	for _, ts := range []uint64{1, 2} {
		select {
		case e := <-events:
			require.Equal(t, ts, e.CommitTs)
		case <-time.After(time.Second):
			t.Fatal("missing durable rollup event")
		}
	}
	require.Equal(t, "two", pointSnapshot(t, s, 2, []byte("shared")).Value)
}

func TestRollupNativeGroupBoundWithForegroundNeighbours(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	release := holdTreeDBCohortRequest(t, s, 1)
	s.commitGroupsForTest = func(groups []mvcc.CommitGroup, mode mvcc.CommitMode) error {
		if len(groups) > treeDBCommitOwnershipLimit {
			return fmt.Errorf("unbounded native groups: %d", len(groups))
		}
		return s.mvcc.CommitGroupAt(groups, mode)
	}
	first := s.NewWriteTxn()
	require.NoError(t, first.SetEntry(Entry{Key: []byte("shared"), Value: []byte("first")}))
	firstDone := make(chan error, 1)
	require.NoError(t, first.CommitAt(1, func(err error) { firstDone <- err }))
	awaitTreeDBCohort(t, "held predecessor", func() bool { return s.durableRequests.Load() == 1 })
	kvs := make([]*bpb.KV, 64)
	for i := range kvs {
		kvs[i] = &bpb.KV{Key: []byte("shared"), Value: []byte(fmt.Sprint(i + 2)), Version: uint64(i + 2)}
	}
	bulkDone := make(chan error, 1)
	go func() { bulkDone <- s.writeRollupBatch(kvs, s.rootEpoch.Load()) }()
	awaitTreeDBCohort(t, "bulk in original input queue", func() bool { return len(s.commitQueue) == 1 })
	done := make([]chan error, 4)
	for i := range done {
		txn := s.NewWriteTxn()
		require.NoError(t, txn.SetEntry(Entry{Key: []byte("shared"), Value: []byte(fmt.Sprint(i + 66))}))
		done[i] = make(chan error, 1)
		ch := done[i]
		require.NoError(t, txn.CommitAt(uint64(i+66), func(err error) { ch <- err }))
	}
	release()
	require.NoError(t, <-firstDone)
	require.NoError(t, <-bulkDone)
	for _, ch := range done {
		require.NoError(t, <-ch)
	}
	for ts := uint64(2); ts < 70; ts++ {
		require.Equal(t, fmt.Sprint(ts), pointSnapshot(t, s, ts, []byte("shared")).Value)
	}
	require.NoError(t, s.Close())
	require.Empty(t, s.commitSlots)
}

func TestTreeDBRollupPreparationCancellationHasNoAdmission(t *testing.T) {
	oldOracle := o
	o = &oracle{}
	o.init()
	defer func() { o = oldOracle }()
	closeRun := z.NewCloser(0)
	ir := &incrRollupi{getNewTs: func(bool) uint64 { return 100 }, closer: closeRun}
	done := make(chan error, 1)
	go func() { _, err := ir.prepareRollup(); done <- err }()
	awaitTreeDBCohort(t, "real waiter", func() bool { o.RLock(); defer o.RUnlock(); return len(o.waiters[100]) > 0 })
	closeRun.Signal()
	require.ErrorIs(t, <-done, ErrHighPriorityOp)
}

func TestTreeDBRollupProcessSubscribedDelayedReadiness(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	oldStore, oldOracle := postingStore, o
	bus := NewCommitEventBus(256)
	postingStore = WithCommitEvents(store, bus)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 64)
	require.NoError(t, err)
	_ = events
	o = &oracle{}
	o.init()
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 10})
	defer func() { postingStore = oldStore; o = oldOracle }()
	keys := make([][]byte, 16)
	for i := range keys {
		keys[i] = x.DataKey(x.AttrInRootNamespace("native-rollup"), uint64(i+1))
		pack := codec.Encode([]uint64{uint64(i + 1)}, blockSize)
		wire := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
		commitStoreEntry(t, store, 10, Entry{Key: keys[i], Value: wire, UserMeta: BitCompletePosting})
		codec.FreePack(pack)
	}
	before := store.durableRequests.Load()
	producedBefore, completedBefore := rollupWork.producedKVs.Load(), rollupWork.completedKVs.Load()
	ir := &incrRollupi{priorityKeys: make([]*pooledKeys, 2)}
	for i := range ir.priorityKeys {
		ir.priorityKeys[i] = &pooledKeys{keysCh: make(chan *[][]byte, 1), keysPool: &sync.Pool{New: func() interface{} { return new([][]byte) }}}
	}
	sent := append([][]byte(nil), keys...)
	ir.priorityKeys[0].keysCh <- &sent
	closeRun := z.NewCloser(1)
	var timestamps atomic.Int32
	go ir.Process(closeRun, func(readonly bool) uint64 {
		if readonly {
			panic("read-only rollup timestamp")
		}
		return 100 + uint64(timestamps.Add(1))
	})
	for i := uint64(101); i <= 116; i++ {
		awaitTreeDBCohort(t, "actual per-key Oracle waiter", func() bool { o.RLock(); defer o.RUnlock(); return len(o.waiters[i]) > 0 })
		require.Empty(t, bus.admissionSlots, "readiness phase must reserve no event slots")
		require.Equal(t, before, store.durableRequests.Load(), "readiness phase must admit no maintenance storage")
		o.ProcessDelta(&pb.OracleDelta{MaxAssigned: i})
	}
	// Completion is visible only once the producer has returned its original batch.
	awaitTreeDBCohort(t, "completed original batch", func() bool { return len(store.commitSlots) == 0 && store.durableRequests.Load() > before })
	closeRun.SignalAndWait()
	require.Equal(t, int32(16), timestamps.Load())
	require.Equal(t, uint64(16), rollupWork.producedKVs.Load()-producedBefore)
	require.Equal(t, uint64(16), rollupWork.completedKVs.Load()-completedBefore)
	require.Equal(t, uint64(1), store.durableRequests.Load()-before, "16 rolled-up roots must be one native FIFO request")
	for _, key := range keys {
		snap := pointSnapshot(t, store, 1000, key)
		require.Equal(t, uint64(11), snap.Version)
	}
}
