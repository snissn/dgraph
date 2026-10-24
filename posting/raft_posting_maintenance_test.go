/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRaftPostingMaintenanceDrainsRequestPinsAndRetiresPrivateHandles(t *testing.T) {
	s := openRaftTransferStore(t, "maintenance-admission")
	key := x.DataKey(x.AttrInRootNamespace("maintenance-empty"), 1)
	commitStoreEntry(t, s, 2, Entry{Key: key, UserMeta: BitEmptyPosting})
	require.NoError(t, s.CheckpointRaft(context.Background(), []byte("old-root")))
	release, err := s.PinRaftReadTimestamp(5)
	require.NoError(t, err)
	defer release()
	require.NoError(t, s.AdvanceRaftSemanticFloor(6))
	_, err = s.PinRaftReadTimestamp(5)
	require.Error(t, err, "S advances independently of the older request pin")
	result := make(chan *RaftPostingMaintenance, 1)
	errors := make(chan error, 1)
	go func() {
		cap, err := s.BeginRaftPostingMaintenance(context.Background(), 6)
		if err != nil {
			errors <- err
			return
		}
		result <- cap
	}()
	require.Eventually(t, func() bool {
		s.raftReadMu.Lock()
		defer s.raftReadMu.Unlock()
		return s.raftReadDraining
	}, 5*time.Second, time.Millisecond)
	select {
	case <-result:
		t.Fatal("maintenance escaped the request pin")
	case err := <-errors:
		t.Fatal(err)
	default:
	}
	_, err = s.PinRaftReadTimestamp(10)
	require.Error(t, err, "new request cannot enter while old requests drain")
	release()
	var cap *RaftPostingMaintenance
	select {
	case cap = <-result:
	case err := <-errors:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("released request did not permit physical progress")
	}
	defer cap.Close()
	_, err = cap.AdvanceAndPrune(4)
	require.Error(t, err, "generic prune cannot precede the complete baseline pass")
	require.NoError(t, cap.MaterializeBaselines(context.Background()))
	_, err = cap.AdvanceAndPrune(4)
	require.NoError(t, err)
	privateStore := cap.ReadStore()
	privateRead := privateStore.NewReadTxn(7)
	it := privateRead.NewIterator(IteratorOptions{})
	it.Rewind()
	require.True(t, it.Valid())
	require.NoError(t, s.CheckpointRaft(context.Background(), []byte("new-root-floor6")))
	require.NoError(t, cap.Finish())
	cap.Close()
	_, err = privateRead.Get(key)
	require.Error(t, err, "private transaction may not escape its capability")
	it.Rewind()
	require.False(t, it.Valid())
	require.True(t, privateStore.IsClosed())
	privateRead.Discard()
	release, err = s.PinRaftReadTimestamp(7)
	require.NoError(t, err)
	release()
	floor, err := s.DiscardFloor()
	require.NoError(t, err)
	require.Equal(t, uint64(6), floor)
}

func TestRaftPostingMaintenanceClearsPendingListsPreservingDeltas(t *testing.T) {
	oldStore, oldBadger := postingStore, pstore
	s, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	postingStore, pstore = s, nil
	Oracle().ResetTxns()
	t.Cleanup(func() { Oracle().ResetTxns(); postingStore, pstore = oldStore, oldBadger })
	key := x.DataKey(x.AttrInRootNamespace("maintenance-pending-splits"), 1)
	split, err := x.SplitKey(key, 1)
	require.NoError(t, err)
	pack := codec.Encode([]uint64{1, 2}, 256)
	data, err := proto.Marshal(&pb.PostingList{Pack: pack})
	codec.FreePack(pack)
	require.NoError(t, err)
	root, err := proto.Marshal(&pb.PostingList{Splits: []uint64{1}})
	require.NoError(t, err)
	commitStoreEntry(t, s, 2, Entry{Key: key, Value: root, UserMeta: BitCompletePosting})
	commitStoreEntry(t, s, 2, Entry{Key: split, Value: data, UserMeta: BitCompletePosting})
	txn := Oracle().RegisterStartTs(20)
	list, err := txn.Get(key)
	require.NoError(t, err)
	delta, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: 99, Op: Set, PostingType: pb.Posting_REF}}})
	require.NoError(t, err)
	list.setMutation(20, delta)
	require.NoError(t, materializeRaftPostingBaselines(context.Background(), s, 6))
	resetRaftPostingCaches()
	require.Empty(t, txn.cache.plists)
	require.Equal(t, delta, txn.cache.deltas[string(key)], "pending mutation must survive list invalidation")
	require.NoError(t, s.AdvanceDiscardFloor(6))
	_, err = s.PruneVersions(4)
	require.NoError(t, err)
	list, err = txn.Get(key)
	require.NoError(t, err)
	uids, err := list.Uids(ListOptions{ReadTs: 20})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 99}, uids.Uids)
}
