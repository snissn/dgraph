/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
)

func TestTreeDBRollupEventLogicalOutcomes(t *testing.T) {
	for _, mode := range []string{"staged", "fifo", "relaxed", "external"} {
		t.Run(mode, func(t *testing.T) {
			var s *TreeDBStore
			if mode == "external" {
				s = openRaftTransferStore(t, "rollup-vector")
			} else {
				s, _ = openPipelinePostingStore(t, t.TempDir())
			}
			if mode == "fifo" {
				s.stagedCommitGroups = false
			}
			if mode == "relaxed" {
				s.mode = TreeDBCommitRelaxed
				s.commitMode = mvcc.CommitRelaxed
			}
			bus := NewCommitEventBus(64)
			defer bus.Close()
			events, err := bus.Subscribe(context.Background(), 8)
			require.NoError(t, err)
			w := &rollupWriter{store: WithCommitEvents(s, bus)}
			before := rollupWork.nativeCompletedKVs.Load()
			require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("good"), Value: []byte("one"), Version: 1}, {Key: make([]byte, 65536), Version: 2}, {Key: []byte("later"), Value: []byte("three"), Version: 3}}}))
			require.ErrorIs(t, w.Flush(), mvcc.ErrInvalidKey)
			// The failed sequence is discharged; a subsequent event cannot be stuck.
			require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("next"), Value: []byte("four"), Version: 4}}}))
			require.NoError(t, w.Flush())
			for _, want := range []uint64{1, 3, 4} {
				select {
				case e := <-events:
					require.Equal(t, want, e.Sequence)
					require.Equal(t, want, e.CommitTs)
				case <-time.After(time.Second):
					t.Fatal("missing per-logical success or unresolved error hole")
				}
			}
			require.Equal(t, uint64(3), rollupWork.nativeCompletedKVs.Load()-before)
			require.Equal(t, "one", pointSnapshot(t, s, 4, []byte("good")).Value)
			require.Equal(t, "three", pointSnapshot(t, s, 4, []byte("later")).Value)
			awaitTreeDBCohort(t, "all event slots released", func() bool { return len(bus.admissionSlots) == 0 })
		})
	}
}

func TestTreeDBRollupEventAmbiguityNoRetry(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 8)
	require.NoError(t, err)
	calls := 0
	s.commitGroupsForTest = func([]mvcc.CommitGroup, mvcc.CommitMode) error {
		calls++
		return fmt.Errorf("%w: %w", mvcc.ErrStorage, mvcc.ErrInvalidKey)
	}
	w := &rollupWriter{store: WithCommitEvents(s, bus)}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("a"), Version: 1}, {Key: []byte("b"), Version: 2}}}))
	require.ErrorIs(t, w.Flush(), mvcc.ErrStorage)
	require.Equal(t, 1, calls)
	awaitTreeDBCohort(t, "ambiguous event holes discharged", func() bool { return len(bus.admissionSlots) == 0 })
	select {
	case e := <-events:
		t.Fatalf("ambiguous success event %+v", e)
	default:
	}
	require.Empty(t, s.commitSlots)
	require.Zero(t, s.commitStagedBytes.Load())
}

func TestTreeDBRollupPublicationFilterLateJoin(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(8)
	defer bus.Close()
	filtered, err := bus.SubscribeMatching(context.Background(), 0, func(k []byte) bool { return bytes.HasPrefix(k, []byte("schema/")) })
	require.NoError(t, err)
	w := &rollupWriter{store: WithCommitEvents(s, bus)}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("ordinary"), Value: []byte("owned"), Version: 10, UserMeta: []byte{BitCompletePosting}}}}))
	raw, err := bus.Subscribe(context.Background(), 4)
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	select {
	case e := <-raw:
		require.Equal(t, uint64(10), e.CommitTs)
		require.Equal(t, []byte("owned"), e.Mutations[0].Entry.Value)
		require.True(t, e.Mutations[0].Entry.DiscardEarlierVersions)
	case <-time.After(time.Second):
		t.Fatal("creation-time negative filter lost late join")
	}
	awaitTreeDBCohort(t, "nonmatch did not block publication", func() bool { return len(bus.admissionSlots) == 0 })
	select {
	case e := <-filtered:
		t.Fatalf("nonmatching delivery %+v", e)
	default:
	}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("schema/key"), Version: 11}}}))
	done := make(chan error, 1)
	go func() { done <- w.Flush() }()
	require.NoError(t, <-done) // Storage completion never waits for subscriber delivery.
	select {
	case e := <-filtered:
		require.Equal(t, uint64(11), e.CommitTs)
	case <-time.After(time.Second):
		t.Fatal("missing matching event")
	}
}

func TestTreeDBRollupSaturatedDepthOneAndClose(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(1)
	defer bus.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := bus.Subscribe(ctx, 0)
	require.NoError(t, err)
	wrapped := WithCommitEvents(s, bus)
	txn := wrapped.NewWriteTxn()
	require.NoError(t, txn.SetEntry(Entry{Key: []byte("foreground")}))
	require.NoError(t, txn.CommitAt(1, nil))
	txn.Discard()
	awaitTreeDBCohort(t, "held foreground event slot", func() bool { return len(bus.admissionSlots) == 1 })
	done := make(chan error, 1)
	go func() {
		w := &rollupWriter{store: wrapped}
		err := w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("rollup"), Version: 2}, {Key: []byte("after"), Version: 3}}})
		if err == nil {
			err = w.Flush()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("ignored admission pressure %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("depth-one deadlock after cancellation")
	}
	require.NoError(t, s.Close())
	bus.Close()
	require.Empty(t, s.commitSlots)
}

func TestTreeDBRollupReadEpochSurvivesActualRootInstall(t *testing.T) {
	source := openRaftTransferStore(t, "rollup-source")
	receiver := openRaftTransferStore(t, "rollup-receiver")
	key := x.DataKey(x.AttrInRootNamespace("rollup-epoch"), 1)
	wire := func(uid uint64) []byte {
		pack := codec.Encode([]uint64{uid}, blockSize)
		defer codec.FreePack(pack)
		return MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
	}
	commitStoreEntry(t, source, 10, Entry{Key: key, Value: wire(2), UserMeta: BitCompletePosting})
	commitStoreEntry(t, receiver, 10, Entry{Key: key, Value: wire(1), UserMeta: BitCompletePosting})
	oldStore := postingStore
	postingStore = receiver
	defer func() { postingStore = oldStore }()
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 4)
	require.NoError(t, err)
	w := &rollupWriter{store: WithCommitEvents(receiver, bus)}
	w.bindReadEpoch() // Before any old-root bytes are read.
	l, err := GetNoStore(key, 100)
	require.NoError(t, err)
	kvs, err := l.Rollup(nil, 100)
	require.NoError(t, err)
	require.NoError(t, source.CheckpointRaft(context.Background(), []byte("source-cut")))
	cert, ok := source.RaftCheckpointCertificate()
	require.True(t, ok)
	var archive bytes.Buffer
	require.NoError(t, source.ExportRaftCheckpoint(context.Background(), &archive, cert.Generation, cert.Digest))
	_, err = receiver.InstallRaftCheckpoint(context.Background(), &archive, func(td.ExternalRecoveryCheckpoint) ([]byte, error) { return []byte("accepted"), nil })
	require.NoError(t, err)
	require.NoError(t, w.Write(&bpb.KVList{Kv: kvs}))
	require.ErrorIs(t, w.Flush(), ErrTreeDBTxnClosed)
	awaitTreeDBCohort(t, "refused root event hole", func() bool { return len(bus.admissionSlots) == 0 })
	select {
	case e := <-events:
		t.Fatalf("stale-root success event %+v", e)
	default:
	}
	require.Equal(t, wire(2), []byte(pointSnapshot(t, receiver, 100, key).Value))
}

func TestTreeDBRollupFloorAfterMaterialization(t *testing.T) {
	s, _ := openPipelinePostingStore(t, t.TempDir())
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 4)
	require.NoError(t, err)
	w := &rollupWriter{store: WithCommitEvents(s, bus)}
	require.NoError(t, w.Write(&bpb.KVList{Kv: []*bpb.KV{{Key: []byte("old"), Value: []byte("old"), Version: 11}}}))
	require.NoError(t, s.AdvanceDiscardFloor(20))
	require.ErrorIs(t, w.Flush(), mvcc.ErrVersionBelowDiscardFloor)
	awaitTreeDBCohort(t, "floor refusal discharged", func() bool { return len(bus.admissionSlots) == 0 })
	select {
	case e := <-events:
		t.Fatalf("below-floor success event %+v", e)
	default:
	}
}

// The same root/part/repeated-root sequence runs through the maintained async
// writer and native producer. Parts remain invalid direct rollup inputs; logical
// data, retained historical data and discard metadata agree after completion.
func TestTreeDBRollupSplitAndRepeatedReadParity(t *testing.T) {
	key := x.DataKey(x.AttrInRootNamespace("rollup-family"), 1)
	oldStore := postingStore
	defer func() { postingStore = oldStore }()
	var wires [2][][]byte
	for arm := 0; arm < 2; arm++ {
		s, _ := openPipelinePostingStore(t, t.TempDir())
		postingStore = s
		var part []byte
		for _, start := range []uint64{1, 3} {
			k, err := x.SplitKey(key, start)
			require.NoError(t, err)
			part = k
			pack := codec.Encode([]uint64{start, start + 1}, blockSize)
			value := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
			codec.FreePack(pack)
			commitStoreEntry(t, s, 10, Entry{Key: k, Value: value, UserMeta: BitCompletePosting})
		}
		root := MarshalPostingList(&pb.PostingList{Splits: []uint64{1, 3}}, nil).Value
		commitStoreEntry(t, s, 10, Entry{Key: key, Value: root, UserMeta: BitCompletePosting})
		var writer rollupOutputWriter = NewTxnWriterForStore(s)
		if arm == 1 {
			writer = &rollupWriter{store: s}
		}
		ir := &incrRollupi{}
		require.NoError(t, ir.materializeRollup(writer, key, 100))
		require.ErrorIs(t, ir.materializeRollup(writer, part, 101), ErrInvalidKey)
		require.NoError(t, ir.materializeRollup(writer, key, 102))
		require.NoError(t, writer.Flush())
		l, err := GetNoStore(key, 1000)
		require.NoError(t, err)
		kvs, err := l.Rollup(nil, 1000)
		require.NoError(t, err)
		require.Len(t, kvs, 3)
		for _, kv := range kvs {
			wires[arm] = append(wires[arm], kv.Value)
		}
		txn := s.NewReadTxn(1000)
		item, err := txn.Get(key)
		require.NoError(t, err)
		require.True(t, item.DiscardEarlierVersions())
		txn.Discard()
		// Old logical input must still resolve as before at its own timestamp.
		historical, err := GetNoStore(key, 10)
		require.NoError(t, err)
		old, err := historical.Rollup(nil, 10)
		require.NoError(t, err)
		require.Len(t, old, len(kvs))
		for i := range kvs {
			require.Equal(t, kvs[i].Value, old[i].Value)
		}
	}
	require.Equal(t, wires[0], wires[1])
}
