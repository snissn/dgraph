/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
	"github.com/stretchr/testify/require"
)

func seedReadinessKeys(t *testing.T, store Store, n int) [][]byte {
	t.Helper()
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = x.DataKey(x.AttrInRootNamespace("readiness-scalar"), uint64(i+1))
		pack := codec.Encode([]uint64{uint64(i + 1)}, blockSize)
		wire := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
		commitStoreEntry(t, store, 10, Entry{Key: keys[i], Value: wire, UserMeta: BitCompletePosting})
		codec.FreePack(pack)
	}
	return keys
}

func useReadinessStore(t *testing.T, store Store) {
	t.Helper()
	oldStore, oldOracle := postingStore, o
	postingStore = store
	o = &oracle{}
	o.init()
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 10})
	t.Cleanup(func() { postingStore, o = oldStore, oldOracle })
}

// The received window is deterministic. The public lossy sync.Pool assembly is
// not assumed to retain16 keys under the race detector.
func startReadinessProcess(t *testing.T, keys [][]byte, getTs func(bool) uint64) *z.Closer {
	t.Helper()
	ir := &incrRollupi{priorityKeys: make([]*pooledKeys, 2)}
	for i := range ir.priorityKeys {
		ir.priorityKeys[i] = &pooledKeys{keysCh: make(chan *[][]byte, 1), keysPool: &sync.Pool{New: func() interface{} { return new([][]byte) }}}
	}
	sent := append([][]byte(nil), keys...)
	ir.priorityKeys[0].keysCh <- &sent
	closer := z.NewCloser(1)
	go ir.Process(closer, getTs)
	t.Cleanup(closer.SignalAndWait)
	return closer
}

func awaitReadinessTimestamp(t *testing.T, ts uint64) {
	t.Helper()
	awaitTreeDBCohort(t, "original distinct Oracle waiter", func() bool {
		o.RLock()
		defer o.RUnlock()
		return len(o.waiters[ts]) != 0
	})
}

func TestTreeDBRollupReadinessFeedsOriginalScalarCohort(t *testing.T) {
	store, opts := openPipelinePostingStore(t, t.TempDir())
	keys := seedReadinessKeys(t, store, 16)
	bus := NewCommitEventBus(256)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 32)
	require.NoError(t, err)
	useReadinessStore(t, WithCommitEvents(store, bus))
	entered, released := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	var physicalSyncs atomic.Int32
	var stages atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !strings.HasPrefix(e.Root+string(filepath.Separator), opts.Dir+string(filepath.Separator)) {
			return nil
		}
		if e.Point == durabilitycut.AfterLogicalMVCCStaging {
			stages.Add(1)
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && physicalSyncs.Add(1) == 1 {
			close(entered)
			<-released
		}
		return nil
	})
	defer restore()
	defer release()
	predecessor := NewTxnWriterForStore(store)
	require.NoError(t, predecessor.SetAt([]byte("prior"), []byte("held"), 0, 20))
	<-entered
	before := store.commitSequence.Load()
	var timestamps atomic.Uint64
	run := startReadinessProcess(t, keys, func(readOnly bool) uint64 {
		if readOnly {
			panic("rollup requires distinct write timestamps")
		}
		return 100 + timestamps.Add(1)
	})
	// Cleanup must release storage before waiting for the producer's final drain.
	t.Cleanup(release)
	for ts := uint64(101); ts <= 116; ts++ {
		awaitReadinessTimestamp(t, ts)
		require.Equal(t, before, store.commitSequence.Load(), "no current-window storage admission before every readiness wait completes")
		require.Empty(t, bus.admissionSlots, "no event slot may span readiness")
		require.Equal(t, uint64(1), bus.nextSequence.Load())
		require.Zero(t, stages.Load())
		o.ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
	}
	awaitTreeDBCohort(t, "16 original scalar requests staged behind physical sync", func() bool { return stages.Load() == 16 })
	require.Equal(t, before+16, store.commitSequence.Load(), "one original transaction per KV, without native aggregate")
	require.Len(t, bus.admissionSlots, 16)
	require.Equal(t, int32(1), physicalSyncs.Load(), "scalar writes do not drain each callback")
	run.Signal()
	release()
	run.SignalAndWait()
	require.NoError(t, predecessor.Flush())
	require.Equal(t, uint64(16), timestamps.Load())
	require.GreaterOrEqual(t, store.durableCohortMax.Load(), uint64(16))
	for i, key := range keys {
		require.Equal(t, uint64(11), pointSnapshot(t, store, 1000, key).Version)
		e := <-events
		require.Equal(t, uint64(i+1), e.Sequence)
		require.Equal(t, uint64(11), e.CommitTs)
		require.Equal(t, key, e.Mutations[0].Entry.Key)
	}
}

// Poll without retaining a read transaction across the asynchronous commit.
func readinessVersion(store Store, key []byte) uint64 {
	txn := store.NewReadTxn(1000)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return 0
	}
	return item.Version()
}

type readinessUnknownStore struct{ Store }

func TestRollupReadinessFallbackRetainsPerKeyProducer(t *testing.T) {
	for _, name := range []string{"badger", "badger-events", "unknown", "relaxed", "external-checkpoint", "unstaged"} {
		t.Run(name, func(t *testing.T) {
			var store Store
			switch name {
			case "badger", "badger-events":
				store = NewBadgerStore(openPostingStoreTestDB(t))
				if name == "badger-events" {
					bus := NewCommitEventBus(4)
					t.Cleanup(bus.Close)
					_, err := bus.Subscribe(context.Background(), 4)
					require.NoError(t, err)
					store = WithCommitEvents(store, bus)
				}
			case "unknown":
				s, _ := openPipelinePostingStore(t, t.TempDir())
				store = readinessUnknownStore{s}
			case "relaxed":
				store, _ = openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
			case "external-checkpoint":
				store = openRaftTransferStore(t, "readiness-fallback")
			case "unstaged":
				s, _ := openPipelinePostingStore(t, t.TempDir())
				s.stagedCommitGroups = false
				store = s
			}
			require.False(t, rollupReadinessStore(store))
			keys := seedReadinessKeys(t, store, 2)
			useReadinessStore(t, store)
			var timestamps atomic.Uint64
			run := startReadinessProcess(t, keys, func(bool) uint64 { return 100 + timestamps.Add(1) })
			awaitReadinessTimestamp(t, 101)
			o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 101})
			awaitReadinessTimestamp(t, 102)
			awaitTreeDBCohort(t, "original first-key write before second readiness", func() bool { return readinessVersion(store, keys[0]) == 11 })
			require.Equal(t, uint64(10), readinessVersion(store, keys[1]))
			o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 102})
			awaitTreeDBCohort(t, "original second-key write", func() bool { return readinessVersion(store, keys[1]) == 11 })
			run.SignalAndWait()
			require.Equal(t, uint64(2), timestamps.Load())
		})
	}
}

func TestTreeDBRollupReadinessCancellationDiscardsUnsubmittedWindow(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	keys := seedReadinessKeys(t, store, 2)
	bus := NewCommitEventBus(1)
	defer bus.Close()
	_, err := bus.Subscribe(context.Background(), 2)
	require.NoError(t, err)
	useReadinessStore(t, WithCommitEvents(store, bus))
	before := store.commitSequence.Load()
	var timestamps atomic.Uint64
	run := startReadinessProcess(t, keys, func(bool) uint64 { return 100 + timestamps.Add(1) })
	awaitReadinessTimestamp(t, 101)
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 101})
	awaitReadinessTimestamp(t, 102)
	run.SignalAndWait()
	require.Equal(t, before, store.commitSequence.Load())
	require.Equal(t, uint64(1), bus.nextSequence.Load())
	require.Empty(t, bus.admissionSlots)
	for _, key := range keys {
		require.Equal(t, uint64(10), readinessVersion(store, key), "even the ready prefix remains unsubmitted")
	}
}

func TestTreeDBRollupReadinessDoesNotHoldEventCapacityDuringWait(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	keys := seedReadinessKeys(t, store, 2)
	bus := NewCommitEventBus(1)
	defer bus.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := bus.Subscribe(ctx, 0)
	require.NoError(t, err)
	wrapped := WithCommitEvents(store, bus)
	useReadinessStore(t, wrapped)
	// An existing event fills depth1 while its unbuffered subscriber is slow.
	commitStoreEntry(t, wrapped, 20, Entry{Key: []byte("event-pressure"), Value: []byte("prior")})
	require.Len(t, bus.admissionSlots, 1)
	before := store.commitSequence.Load()
	var timestamps atomic.Uint64
	run := startReadinessProcess(t, keys, func(bool) uint64 { return 100 + timestamps.Add(1) })
	// On failure release the preexisting subscriber before producer cleanup.
	t.Cleanup(cancel)
	for ts := uint64(101); ts <= 102; ts++ {
		awaitReadinessTimestamp(t, ts)
		require.Equal(t, before, store.commitSequence.Load())
		require.Equal(t, uint64(2), bus.nextSequence.Load(), "the window has not reserved an event sequence")
		o.ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
	}
	cancel()
	awaitTreeDBCohort(t, "scalar writes progress after existing subscription cancellation", func() bool { return readinessVersion(store, keys[1]) == 11 })
	run.SignalAndWait()
	require.Equal(t, before+2, store.commitSequence.Load())
	awaitTreeDBCohort(t, "depth1 event reservations released", func() bool { return len(bus.admissionSlots) == 0 })
}

func TestTreeDBRollupReadinessMaterializationParity(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprint("deleted=", deleted), func(t *testing.T) {
			key := x.DataKey(x.AttrInRootNamespace("readiness-family"), 1)
			var wires [2][][]byte
			for arm := 0; arm < 2; arm++ {
				store, _ := openPipelinePostingStore(t, t.TempDir())
				useReadinessStore(t, store)
				o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 1000})
				var part []byte
				for _, start := range []uint64{1, 3} {
					k, err := x.SplitKey(key, start)
					require.NoError(t, err)
					part = k
					pack := codec.Encode([]uint64{start, start + 1}, blockSize)
					wire := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
					codec.FreePack(pack)
					commitStoreEntry(t, store, 10, Entry{Key: k, Value: wire, UserMeta: BitCompletePosting})
				}
				root := MarshalPostingList(&pb.PostingList{Splits: []uint64{1, 3}}, nil).Value
				commitStoreEntry(t, store, 10, Entry{Key: key, Value: root, UserMeta: BitCompletePosting})
				if deleted {
					commitStoreEntry(t, store, 20, Entry{Key: key, Value: []byte{}, UserMeta: BitEmptyPosting})
				}
				writer := NewTxnWriterForStore(store)
				ts := uint64(99)
				ir := &incrRollupi{closer: z.NewCloser(0), getNewTs: func(bool) uint64 { ts++; return ts }}
				if arm == 0 {
					require.NoError(t, ir.rollUpKey(writer, key))
					require.ErrorIs(t, ir.rollUpKey(writer, part), ErrInvalidKey)
					require.NoError(t, ir.rollUpKey(writer, key))
				} else {
					// A root and its part can share a received window. Repeated root
					// remains hash-throttled exactly as the original producer.
					last := make(map[uint64]int64)
					require.NoError(t, ir.rollUpReadyBatch(writer, [][]byte{key, part, key}, last, 100))
					require.NoError(t, ir.rollUpReadyBatch(writer, [][]byte{key}, last, 111))
					require.Equal(t, uint64(102), ts)
				}
				require.NoError(t, writer.Flush())
				l, err := GetNoStore(key, 1000)
				require.NoError(t, err)
				kvs, err := l.Rollup(nil, 1000)
				require.NoError(t, err)
				if deleted {
					require.Len(t, kvs, 1)
				} else {
					require.Len(t, kvs, 3)
				}
				for _, kv := range kvs {
					wires[arm] = append(wires[arm], kv.Value)
				}
				read := store.NewReadTxn(1000)
				item, err := read.Get(key)
				require.NoError(t, err)
				require.True(t, item.DiscardEarlierVersions())
				read.Discard()
				historical, err := GetNoStore(key, 10)
				require.NoError(t, err)
				old, err := historical.Rollup(nil, 10)
				require.NoError(t, err)
				require.Len(t, old, 3)
			}
			require.Equal(t, wires[0], wires[1])
		})
	}
}

func TestTreeDBRollupReadinessLateSubscriberUsesOriginalCreationBoundary(t *testing.T) {
	store, opts := openPipelinePostingStore(t, t.TempDir())
	keys := seedReadinessKeys(t, store, 1)
	bus := NewCommitEventBus(1)
	defer bus.Close()
	first, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	useReadinessStore(t, WithCommitEvents(store, bus))
	entered, released := make(chan struct{}), make(chan struct{})
	var once, enteredOnce sync.Once
	release := func() { once.Do(func() { close(released) }) }
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync &&
			strings.HasPrefix(e.Root+string(filepath.Separator), opts.Dir+string(filepath.Separator)) {
			enteredOnce.Do(func() { close(entered) })
			<-released
		}
		return nil
	})
	defer restore()
	defer release()
	run := startReadinessProcess(t, keys, func(bool) uint64 { return 101 })
	t.Cleanup(release)
	awaitReadinessTimestamp(t, 101)
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 101})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("scalar write did not reach physical sync")
	}
	require.Len(t, bus.admissionSlots, 1)
	// The first subscriber made the transaction event-eligible at creation.
	// A second subscriber joins after that creation, before completion; the
	// original publication owner must deliver to both without early filtering.
	late, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	release()
	run.SignalAndWait()
	for _, ch := range []<-chan CommitEvent{first, late} {
		select {
		case event := <-ch:
			require.Equal(t, uint64(1), event.Sequence)
			require.Equal(t, uint64(11), event.CommitTs)
			require.Equal(t, keys[0], event.Mutations[0].Entry.Key)
		case <-time.After(5 * time.Second):
			t.Fatal("existing publication owner missed a subscriber")
		}
	}
}

func TestTreeDBRollupReadinessFloorChangeRefusesWithoutRetry(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	keys := seedReadinessKeys(t, store, 2)
	bus := NewCommitEventBus(4)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 4)
	require.NoError(t, err)
	useReadinessStore(t, WithCommitEvents(store, bus))
	writer := NewTxnWriterForStore(postingStore)
	before := store.commitSequence.Load()
	var timestamps atomic.Uint64
	ir := &incrRollupi{closer: z.NewCloser(0), getNewTs: func(bool) uint64 { return 100 + timestamps.Add(1) }}
	done := make(chan error, 1)
	go func() { done <- ir.rollUpReadyBatch(writer, keys, make(map[uint64]int64), 100) }()
	awaitReadinessTimestamp(t, 101)
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 101})
	awaitReadinessTimestamp(t, 102)
	require.Equal(t, before, store.commitSequence.Load())
	// No List/root or write epoch was captured before this maintenance cut.
	// The read timestamps remain legal, but the rollup output version11 is now
	// below the floor and must fail through the original completion path.
	require.NoError(t, store.AdvanceDiscardFloor(20))
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 102})
	require.NoError(t, receiveTreeDBCohortResult(t, done))
	require.ErrorIs(t, writer.Flush(), mvcc.ErrVersionBelowDiscardFloor)
	require.Equal(t, uint64(2), timestamps.Load())
	require.Equal(t, before+2, store.commitSequence.Load(), "one attempt per original scalar write")
	awaitTreeDBCohort(t, "failed event reservations released", func() bool { return len(bus.admissionSlots) == 0 })
	select {
	case event := <-events:
		t.Fatalf("refused rollup published success: %+v", event)
	default:
	}
}

func TestTreeDBRollupReadinessCloseBeforeMaterialization(t *testing.T) {
	store, _ := openPipelinePostingStore(t, t.TempDir())
	keys := seedReadinessKeys(t, store, 2)
	useReadinessStore(t, store)
	writer := NewTxnWriterForStore(store)
	before := store.commitSequence.Load()
	var timestamps atomic.Uint64
	ir := &incrRollupi{closer: z.NewCloser(0), getNewTs: func(bool) uint64 { return 100 + timestamps.Add(1) }}
	done := make(chan error, 1)
	go func() { done <- ir.rollUpReadyBatch(writer, keys, make(map[uint64]int64), 100) }()
	awaitReadinessTimestamp(t, 101)
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 101})
	awaitReadinessTimestamp(t, 102)
	require.NoError(t, store.Close())
	o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 102})
	require.NoError(t, receiveTreeDBCohortResult(t, done), "ordinary per-key materialization errors retain continue semantics")
	require.NoError(t, writer.Flush(), "no write was admitted before the store closed")
	require.Equal(t, before, store.commitSequence.Load())
	require.Equal(t, uint64(2), timestamps.Load())
}
