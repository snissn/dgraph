/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestApplySnapshotKeepsTreeDBOlderReadsAndColdWrites(t *testing.T) {
	for _, mode := range []posting.TreeDBCommitMode{posting.TreeDBCommitDurable, posting.TreeDBCommitRelaxed} {
		name := "durable"
		profile := treedb.ProfileCommandWALDurable
		if mode == posting.TreeDBCommitRelaxed {
			name = "relaxed"
			profile = treedb.ProfileCommandWALRelaxed
		}
		t.Run(name, func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			store, err := posting.OpenTreeDBStore(opts, mode)
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()

			oldState, oldPstore := State, pstore
			State, pstore = ServerState{TreeDBStore: store}, nil
			defer func() { State, pstore = oldState, oldPstore }()

			key := []byte("cold-posting")
			writer := posting.NewTxnWriterForStore(store)
			require.NoError(t, writer.SetAt(key, []byte("old complete"), posting.BitCompletePosting, 2))
			require.NoError(t, writer.Flush())

			// Apply the actual Raft snapshot proposal, including CreateSnapshot. ReadTs
			// represents newer publication elsewhere, not expiration of this cold key.
			n, snap := snapshotDiscardTestNode(t)
			require.NoError(t, n.applyCommitted(&pb.Proposal{Snapshot: snap}, 0))
			assertAppliedSnapshot(t, n, snap)

			for _, readTs := range []uint64{2, snap.ReadTs} {
				t.Run("readTs="+strconv.FormatUint(readTs, 10), func(t *testing.T) {
					// Open after application: the adapter's timestamp-only read transaction
					// is not a retained snapshot or an admission pin.
					read := store.NewReadTxn(readTs)
					defer read.Discard()
					item, err := read.Get(key)
					require.NoError(t, err)
					value, err := item.ValueCopy(nil)
					require.NoError(t, err)
					require.Equal(t, []byte("old complete"), value)
				})
			}
			t.Run("cold-complete-write", func(t *testing.T) {
				// A cold rollup can publish its represented commit timestamp + 1,
				// even after unrelated newer commits supplied the snapshot's ReadTs.
				writer := posting.NewTxnWriterForStore(store)
				require.NoError(t, writer.SetAt(key, []byte("late complete"), posting.BitCompletePosting, 3))
				require.NoError(t, writer.Flush(), "submission alone does not acknowledge the write")
			})

			floor, err := store.DiscardFloor()
			require.NoError(t, err)
			require.Zero(t, floor, "Raft snapshot publication must not write a TreeDB admission floor")
			require.NoError(t, store.Close())
			reopened, err := posting.OpenTreeDBStore(opts, mode)
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			floor, err = reopened.DiscardFloor()
			require.NoError(t, err)
			require.Zero(t, floor)
			read := reopened.NewReadTxn(3)
			defer read.Discard()
			item, err := read.Get(key)
			require.NoError(t, err)
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("late complete"), value)
		})
	}
}

func TestApplySnapshotPreservesBadgerDiscardTs(t *testing.T) {
	for _, managed := range []bool{true, false} {
		t.Run("managed="+strconv.FormatBool(managed), func(t *testing.T) {
			open := badger.Open
			if managed {
				open = badger.OpenManaged
			}
			db, err := open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			oldPstore := pstore
			pstore = db
			defer func() { pstore = oldPstore }()
			n, snap := snapshotDiscardTestNode(t)
			if managed {
				require.NoError(t, n.applyCommitted(&pb.Proposal{Snapshot: snap}, 0))
			} else {
				// The setter's existing guard observes the real call without reading
				// private oracle fields or adding a production hook just for the test.
				require.PanicsWithValue(t, "Cannot use SetDiscardTs with managedDB=false.", func() {
					_ = n.applyCommitted(&pb.Proposal{Snapshot: snap}, 0)
				})
			}
			assertAppliedSnapshot(t, n, snap)
		})
	}
}

func snapshotDiscardTestNode(t *testing.T) (*node, *pb.Snapshot) {
	t.Helper()
	ds := raftwal.Init(t.TempDir())
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	require.NoError(t, ds.Save(&raftpb.HardState{}, []raftpb.Entry{{Index: 1, Term: 1}}, &raftpb.Snapshot{}))
	n := &node{Node: &conn.Node{Id: 1, Store: ds}, gid: 1}
	n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
	return n, &pb.Snapshot{Index: 1, ReadTs: 10}
}

func assertAppliedSnapshot(t *testing.T, n *node, expected *pb.Snapshot) {
	t.Helper()
	stored, err := n.Store.Snapshot()
	require.NoError(t, err)
	require.Equal(t, expected.Index, stored.Metadata.Index)
	var actual pb.Snapshot
	require.NoError(t, proto.Unmarshal(stored.Data, &actual))
	require.True(t, proto.Equal(expected, &actual))
}
