/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// One replica prunes and the other retains the original delta chains. Their
// posting views must agree at every still-admissible timestamp, including cold
// and split lists, after two independently chosen physical floors.
func TestRaftPostingBaselinesPreserveDeltaAndSplitHistory(t *testing.T) {
	stores := make([]*TreeDBStore, 2)
	for i := range stores {
		stores[i], _ = openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	}
	attr := x.AttrInRootNamespace("floor-history")
	main := x.DataKey(attr, 1)
	cold := x.DataKey(attr, 2)
	empty := x.DataKey(attr, 3)
	split, err := x.SplitKey(main, 1)
	require.NoError(t, err)
	pack := codec.Encode([]uint64{1, 2, 3}, 256)
	part, err := proto.Marshal(&pb.PostingList{Pack: pack})
	codec.FreePack(pack)
	require.NoError(t, err)
	root, err := proto.Marshal(&pb.PostingList{Splits: []uint64{1}})
	require.NoError(t, err)
	for _, s := range stores {
		commitStoreEntry(t, s, 2, Entry{Key: main, Value: root, UserMeta: BitCompletePosting})
		commitStoreEntry(t, s, 2, Entry{Key: split, Value: part, UserMeta: BitCompletePosting})
		commitStoreEntry(t, s, 2, Entry{Key: empty, UserMeta: BitEmptyPosting})
		for ts := uint64(3); ts <= 12; ts++ {
			data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: ts + 10, Op: Set, PostingType: pb.Posting_REF}}})
			require.NoError(t, err)
			commitStoreEntry(t, s, ts, Entry{Key: main, Value: data, UserMeta: BitDeltaPosting})
			if ts <= 4 {
				commitStoreEntry(t, s, ts, Entry{Key: cold, Value: data, UserMeta: BitDeltaPosting})
			}
		}
		commitStoreEntry(t, s, 2, Entry{Key: x.SchemaKey(attr), Value: []byte("schema-record"), UserMeta: BitSchemaPosting})
	}
	read := func(s Store, key []byte, ts uint64) []uint64 {
		txn := s.NewReadTxn(ts)
		defer txn.Discard()
		it := txn.NewKeyIterator(key, IteratorOptions{AllVersions: true})
		defer it.Close()
		it.Seek(key)
		list, err := readPostingListForStore(key, it, s)
		require.NoError(t, err)
		uids, err := list.Uids(ListOptions{ReadTs: ts})
		require.NoError(t, err)
		return uids.Uids
	}
	for _, floor := range []uint64{6, 9} {
		require.NoError(t, materializeRaftPostingBaselines(context.Background(), stores[0], floor))
		require.NoError(t, stores[0].AdvanceDiscardFloor(floor))
		_, err := stores[0].PruneVersions(3)
		require.NoError(t, err)
		for _, key := range [][]byte{main, cold, split, empty} {
			txn := stores[0].NewReadTxn(floor + 1)
			item, err := txn.Get(key)
			require.NoError(t, err)
			require.Equal(t, floor+1, item.Version(), "cold main and split baselines must be above the discard floor")
			txn.Discard()
		}
		for ts := floor + 1; ts <= 13; ts++ {
			for _, key := range [][]byte{main, cold, empty} {
				require.Equal(t, read(stores[1], key, ts), read(stores[0], key, ts), "floor=%d ts=%d key=%x", floor, ts, key)
			}
		}
		for ts := floor + 2; ts <= 12; ts++ {
			read := stores[0].NewReadTxn(ts)
			item, err := read.Get(main)
			require.NoError(t, err)
			require.Equal(t, ts, item.Version())
			require.Equal(t, BitDeltaPosting, item.UserMeta(), "newer deltas must remain unchanged")
			data, err := item.ValueCopy(nil)
			require.NoError(t, err)
			want, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: ts + 10, Op: Set, PostingType: pb.Posting_REF}}})
			require.NoError(t, err)
			require.Equal(t, want, data)
			read.Discard()
		}
		// A repeat without a floor change is idempotent, including cold keys
		// whose newest original delta predates the prior physical floor.
		require.NoError(t, materializeRaftPostingBaselines(context.Background(), stores[0], floor))
	}
}

func TestRaftPostingBaselinesDrainFailureAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "callback-error", true: "canceled-during-callback"}[cancel], func(t *testing.T) {
			s, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
			key := x.DataKey(x.AttrInRootNamespace("baseline-error"), 1)
			commitStoreEntry(t, s, 2, Entry{Key: key, UserMeta: BitEmptyPosting})
			failure := errors.New("baseline callback failed")
			gate := &baselineCallbackGate{Store: s, entered: make(chan struct{}), release: make(chan struct{}), err: failure}
			if cancel {
				gate.err = nil
			}
			var once sync.Once
			release := func() { once.Do(func() { close(gate.release) }) }
			t.Cleanup(release)
			ctx, cancelFn := context.WithCancel(context.Background())
			defer cancelFn()
			done := make(chan error, 1)
			go func() { done <- materializeRaftPostingBaselines(ctx, gate, 6) }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("baseline did not reach callback")
			}
			if cancel {
				cancelFn()
			}
			select {
			case err := <-done:
				t.Fatalf("baseline returned before callback drained: %v", err)
			default:
			}
			release()
			select {
			case err := <-done:
				if cancel {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, failure)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("baseline did not drain")
			}
			floor, err := s.DiscardFloor()
			require.NoError(t, err)
			require.Zero(t, floor, "failed baseline pass cannot publish a floor")
			require.NoError(t, materializeRaftPostingBaselines(context.Background(), s, 6))
			require.NoError(t, s.AdvanceDiscardFloor(6))
			txn := s.NewReadTxn(6)
			_, err = txn.Get(key)
			require.ErrorIs(t, err, mvcc.ErrReadBeforeDiscardFloor)
			txn.Discard()
		})
	}
}

type baselineCallbackGate struct {
	Store
	entered chan struct{}
	release chan struct{}
	err     error
}

func (s *baselineCallbackGate) NewWriteTxn() WriteTxn {
	return baselineCallbackTxn{WriteTxn: s.Store.NewWriteTxn(), gate: s}
}

type baselineCallbackTxn struct {
	WriteTxn
	gate *baselineCallbackGate
}

func (t baselineCallbackTxn) CommitAt(ts uint64, cb func(error)) error {
	return t.WriteTxn.CommitAt(ts, func(err error) {
		close(t.gate.entered)
		<-t.gate.release
		cb(errors.Join(err, t.gate.err))
	})
}
