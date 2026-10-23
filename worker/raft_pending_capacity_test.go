/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Count admission is exercised at its real boundary, without inflated counters.
// The two replicas retain equal logical data through different physical roots:
// a flat complete list at P=0 and a split list actually baselined/pruned to P=6.
func TestRaftPendingCapacityAgreesAcrossPhysicalFloorsAndSplits(t *testing.T) {
	oldConfig, oldState, oldBadger, oldStore, oldNode := Config, State, pstore, postingStore, gr.Node
	oldHardSync, oldRetries, oldHealth := x.WorkerConfig.HardSync, x.Config.MaxRetries, x.HealthCheck() == nil
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		Config, State, pstore, postingStore, gr.Node = oldConfig, oldState, oldBadger, oldStore, oldNode
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		x.UpdateHealthStatus(oldHealth)
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	x.WorkerConfig.HardSync, x.Config.MaxRetries = true, 3
	x.UpdateHealthStatus(true)
	type outcome struct {
		first, full, replacement uint64
		rejection                string
	}
	var results []outcome
	for _, pruned := range []bool{false, true} {
		t.Run(map[bool]string{false: "flat-P0", true: "split-P6"}[pruned], func(t *testing.T) {
			posting.Oracle().ResetTxns()
			Config = snapshotInstallOptions(t.TempDir())
			require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
			wal := raftwal.Init(Config.WALDir)
			defer wal.Close()
			state := &ServerState{}
			require.NoError(t, state.openPostingStore())
			defer func() {
				posting.Cleanup()
				posting.Oracle().ResetTxns()
				state.closePostingStore()
			}()
			State, pstore, postingStore = *state, nil, state.PostingStore
			posting.InitForStore(postingStore, 0, false)
			schema.InitForStore(schemaPostingStore{store: postingStore})
			n := newNode(wal, 1, 1, "")
			gr.Node = n
			require.NoError(t, n.recoverRaftCheckpoint())
			n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
			attr := x.AttrInRootNamespace("capacity-uids")
			key := x.DataKey(attr, 1)
			definition := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_UID, List: true}
			definitionBytes, err := proto.Marshal(definition)
			require.NoError(t, err)
			pack := codec.Encode([]uint64{10, 20, 30}, 256)
			part, err := proto.Marshal(&pb.PostingList{Pack: pack})
			codec.FreePack(pack)
			require.NoError(t, err)
			entries := []posting.Entry{{Key: x.SchemaKey(attr), Value: definitionBytes, UserMeta: posting.BitSchemaPosting}}
			if pruned {
				root, err := proto.Marshal(&pb.PostingList{Splits: []uint64{1}})
				require.NoError(t, err)
				split, err := x.SplitKey(key, 1)
				require.NoError(t, err)
				entries = append(entries, posting.Entry{Key: key, Value: root, UserMeta: posting.BitCompletePosting}, posting.Entry{Key: split, Value: part, UserMeta: posting.BitCompletePosting})
			} else {
				entries = append(entries, posting.Entry{Key: key, Value: part, UserMeta: posting.BitCompletePosting})
			}
			// Seed equivalent certified input images; subsequent mutations and
			// decisions use ordinary tracked apply and retained Raft WAL.
			writer := postingStore.NewWriteTxn()
			for _, entry := range entries {
				require.NoError(t, writer.SetEntry(entry))
			}
			require.NoError(t, writer.CommitAt(2, nil))
			writer.Discard()
			require.NoError(t, schema.LoadFromDb(context.Background()))
			readUIDs := func(ts uint64) []uint64 {
				list, err := posting.GetNoStore(key, ts)
				require.NoError(t, err)
				uids, err := list.Uids(posting.ListOptions{ReadTs: ts})
				require.NoError(t, err)
				return uids.Uids
			}
			require.Equal(t, []uint64{10, 20, 30}, readUIDs(3))
			var index uint64
			apply := func(p *pb.Proposal) error {
				index++
				e := checkpointEntry(t, index, p)
				require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: index}, []raftpb.Entry{e}, &raftpb.Snapshot{}))
				require.NoError(t, wal.Sync())
				atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
				var committed pb.Proposal
				require.NoError(t, proto.Unmarshal(e.Data[8:], &committed))
				committed.Index = index
				updateStartTs(&committed)
				err := n.applyTrackedProposal(&committed, binary.BigEndian.Uint64(e.Data[:8]), e.Size())
				n.raftAppliedBytes += uint64(len(e.Data))
				n.raftSemanticIndex = index
				n.Applied.SetDoneUntil(index)
				return err
			}
			require.NoError(t, apply(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: 10}}))
			require.NoError(t, n.checkpointRaftCut(index, true))
			require.NoError(t, apply(&pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: 6}}))
			if pruned {
				_, err := n.pruneRaftPostingHistory(context.Background(), 6, 4)
				require.NoError(t, err)
			}
			floor, err := state.TreeDBStore.DiscardFloor()
			require.NoError(t, err)
			require.Equal(t, map[bool]uint64{false: 0, true: 6}[pruned], floor)
			readBase := postingStore.NewReadTxn(30)
			baseItem, err := readBase.Get(key)
			require.NoError(t, err)
			require.Equal(t, map[bool]uint64{false: 2, true: 7}[pruned], baseItem.Version(), "different physical histories must be real")
			readBase.Discard()
			mutate := func(ts, uid uint64, predicate string) *pb.Proposal {
				return &pb.Proposal{Mutations: &pb.Mutations{StartTs: ts, Edges: []*pb.DirectedEdge{{Entity: 1, Attr: predicate, ValueId: uid, ValueType: pb.Posting_UID, Op: pb.DirectedEdge_SET}}}}
			}
			require.NoError(t, apply(mutate(30, 40, attr)))
			first, err := state.TreeDBStore.RaftPendingManifest()
			require.NoError(t, err)
			for ts := uint64(1000); ts < 1000+posting.RaftPendingMaxCount-1; ts++ {
				txn := posting.NewTxn(ts)
				row, err := posting.MarshalRaftPendingTxn(txn)
				require.NoError(t, err)
				require.NoError(t, state.TreeDBStore.ReplaceRaftPendingTxn(ts, row))
				require.NoError(t, posting.InstallRaftPendingTxn(txn))
			}
			full, err := state.TreeDBStore.RaftPendingManifest()
			require.NoError(t, err)
			require.Equal(t, uint64(posting.RaftPendingMaxCount), full.Count)
			unseen := x.AttrInRootNamespace("capacity-unseen")
			err = apply(mutate(1000000, 50, unseen))
			require.True(t, isRaftSemanticRejection(err), "%v", err)
			rejection := err.Error()
			unchanged, err := state.TreeDBStore.RaftPendingManifest()
			require.NoError(t, err)
			require.Equal(t, full, unchanged)
			require.Nil(t, posting.Oracle().GetTxn(1000000))
			_, exists := schema.State().Get(context.Background(), unseen)
			require.False(t, exists, "capacity rejection cannot publish inferred schema")
			read := postingStore.NewReadTxn(^uint64(0))
			_, err = read.Get(x.SchemaKey(unseen))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			read.Discard()
			// An existing transaction can still grow at the count boundary.
			require.NoError(t, apply(mutate(30, 50, attr)))
			replaced, err := state.TreeDBStore.RaftPendingManifest()
			require.NoError(t, err)
			require.Equal(t, full.Count, replaced.Count)
			require.Greater(t, replaced.Charge, full.Charge)
			// The ordered decision has no capacity reservation and frees a row.
			require.NoError(t, apply(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: 1000001, Txns: []*pb.TxnStatus{{StartTs: 30, CommitTs: 31}}}}))
			require.Equal(t, []uint64{10, 20, 30, 40, 50}, readUIDs(31), "commit preserves the original flat or split posting baseline")
			require.NoError(t, apply(mutate(1000000, 50, unseen)))
			results = append(results, outcome{first: first.Charge, full: full.Charge, replacement: replaced.Charge, rejection: rejection})
		})
	}
	require.Len(t, results, 2)
	require.Equal(t, results[0].first, results[1].first)
	require.Equal(t, results[0].full, results[1].full)
	require.Equal(t, results[0].replacement, results[1].replacement)
	require.Equal(t, results[0].rejection, results[1].rejection)
}
