/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftLogicalDropCertifiedReplay(t *testing.T) {
	for _, kind := range []string{"all", "data", "namespace", "predicate", "ban"} {
		t.Run(kind, func(t *testing.T) {
			oldConfig, oldState, oldBadger, oldStore := Config, State, pstore, postingStore
			oldHardSync, oldNode, oldRetries := x.WorkerConfig.HardSync, gr.Node, x.Config.MaxRetries
			posting.Cleanup()
			posting.Oracle().ResetTxns()
			var state *ServerState
			t.Cleanup(func() {
				posting.Cleanup()
				posting.Oracle().ResetTxns()
				if state != nil {
					state.closePostingStore()
				}
				Config, State, pstore, postingStore = oldConfig, oldState, oldBadger, oldStore
				x.WorkerConfig.HardSync, gr.Node, x.Config.MaxRetries = oldHardSync, oldNode, oldRetries
				posting.Init(oldBadger, 0, false)
				schema.Init(oldBadger)
			})
			Config = snapshotInstallOptions(t.TempDir())
			x.WorkerConfig.HardSync, x.Config.MaxRetries = true, 3
			require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
			wal := raftwal.Init(Config.WALDir)
			defer wal.Close()
			open := func() *node {
				state = &ServerState{}
				require.NoError(t, state.openPostingStore())
				State, pstore, postingStore = *state, nil, state.PostingStore
				posting.InitForStore(postingStore, 0, false)
				schema.InitForStore(schemaPostingStore{store: postingStore})
				require.NoError(t, schema.LoadFromDb(context.Background()))
				n := newNode(wal, 1, 1, "")
				gr.Node = n
				require.NoError(t, n.recoverRaftCheckpoint())
				n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
				return n
			}
			reopen := func() *node {
				posting.Cleanup()
				posting.Oracle().ResetTxns()
				state.closePostingStore()
				return open()
			}
			n := open()
			base := posting.Oracle().MaxAssigned() + 10
			attr, other := x.AttrInRootNamespace("admin-value"), x.NamespaceAttr(7, "admin-value")
			if kind == "ban" {
				attr, other = other, attr
			}
			edge := func(attr, value string) *pb.DirectedEdge {
				return &pb.DirectedEdge{Entity: 1, Attr: attr, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}
			}
			drop := func(ts uint64) *pb.Proposal {
				if kind == "ban" {
					return &pb.Proposal{StartTs: ts, DeleteNs: &pb.DeleteNsRequest{Namespace: 7, GroupId: 1}}
				}
				m := &pb.Mutations{StartTs: ts}
				switch kind {
				case "all":
					m.DropOp = pb.Mutations_ALL
				case "data":
					m.DropOp, m.DropValue = pb.Mutations_DATA, "0x0"
				case "namespace":
					m.DropOp, m.DropValue = pb.Mutations_ALL_IN_NS, "0x0"
				case "predicate":
					m.Edges = []*pb.DirectedEdge{{Attr: attr, Value: []byte(x.Star), Op: pb.DirectedEdge_DEL}}
				}
				return &pb.Proposal{Mutations: m}
			}
			entries := []raftpb.Entry{
				checkpointEntry(t, 1, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base, Edges: []*pb.DirectedEdge{edge(attr, "before"), edge(other, "other")}}}),
				checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 1, Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}),
				checkpointEntry(t, 3, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 2, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}}}}),
				checkpointEntry(t, 4, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 3, Edges: []*pb.DirectedEdge{edge(attr, "pending")}}}),
				checkpointEntry(t, 5, drop(base+4)),
				checkpointEntry(t, 6, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5, Txns: []*pb.TxnStatus{{StartTs: base + 3}}}}),
			}
			retry := entries[4]
			retry.Index = 7
			unrelated := &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 6, Edges: []*pb.DirectedEdge{edge(other, "unrelated-pending")}}}
			wantPending := 1
			if kind == "all" {
				unrelated = &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 6}}
				wantPending = 0
			}
			entries = append(entries, retry, checkpointEntry(t, 8, unrelated), checkpointEntry(t, 9, drop(base+7)))
			require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 9}, entries, &raftpb.Snapshot{}))
			require.NoError(t, wal.Sync())
			apply := func(e raftpb.Entry) error {
				var p pb.Proposal
				require.NoError(t, proto.Unmarshal(e.Data[8:], &p))
				p.Index = e.Index
				updateStartTs(&p)
				err := n.applyTrackedProposal(&p, binary.BigEndian.Uint64(e.Data[:8]), e.Size())
				n.raftAppliedBytes += uint64(len(e.Data))
				n.raftSemanticIndex = e.Index
				n.Applied.SetDoneUntil(e.Index)
				return err
			}
			for _, e := range entries {
				atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
			}
			for _, e := range entries[:3] {
				require.NoError(t, apply(e))
			}
			require.NoError(t, n.checkpointRaftCut(3, true))
			check := func() {
				require.NoError(t, apply(entries[3]))
				before, manifestErr := State.TreeDBStore.RaftPendingManifest()
				require.NoError(t, manifestErr)
				err := apply(entries[4])
				require.True(t, isRaftSemanticRejection(err))
				require.Contains(t, err.Error(), "pending transactions")
				after, manifestErr := State.TreeDBStore.RaftPendingManifest()
				require.NoError(t, manifestErr)
				require.Equal(t, before, after)
				require.Equal(t, 1, posting.Oracle().NumPendingTxns())
				require.NoError(t, apply(entries[5]))
				require.EqualError(t, apply(entries[6]), err.Error(), "rejected identity stays rejected after pending abort")
				require.NoError(t, apply(entries[7]))
				unrelatedManifest, manifestErr := State.TreeDBStore.RaftPendingManifest()
				require.NoError(t, manifestErr)
				require.NoError(t, apply(entries[8]))
				afterDrop, manifestErr := State.TreeDBStore.RaftPendingManifest()
				require.NoError(t, manifestErr)
				require.Equal(t, unrelatedManifest, afterDrop, "unrelated canonical pending state must survive drop")
				require.Equal(t, wantPending, posting.Oracle().NumPendingTxns())
				list, err := posting.GetNoStore(x.DataKey(attr, 1), math.MaxUint64)
				require.NoError(t, err)
				_, err = list.Value(math.MaxUint64)
				require.ErrorIs(t, err, posting.ErrNoValue)
				historical, err := posting.GetNoStore(x.DataKey(attr, 1), base+1)
				require.NoError(t, err)
				value, err := historical.Value(base + 1)
				require.NoError(t, err)
				require.Equal(t, []byte("before"), value.Value)
				_, exists := schema.State().Get(context.Background(), attr)
				require.Equal(t, kind == "data", exists)
				if kind != "all" {
					list, err = posting.GetNoStore(x.DataKey(other, 1), math.MaxUint64)
					require.NoError(t, err)
					value, err = list.Value(math.MaxUint64)
					require.NoError(t, err)
					require.Equal(t, []byte("other"), value.Value)
				}
			}
			check()
			n = reopen()
			require.Equal(t, uint64(3), n.Cfg.Applied)
			check()
			require.NoError(t, n.checkpointRaftCut(9, true))
			n = reopen()
			require.Equal(t, uint64(9), n.Cfg.Applied)
			require.Equal(t, wantPending, posting.Oracle().NumPendingTxns())
			if kind == "ban" {
				for _, p := range []*pb.Proposal{
					{Mutations: &pb.Mutations{StartTs: base + 7, Edges: []*pb.DirectedEdge{edge(attr, "resurrect")}}},
					{Mutations: &pb.Mutations{StartTs: base + 7, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}},
					{Mutations: &pb.Mutations{StartTs: base + 7, Types: []*pb.TypeUpdate{{TypeName: x.NamespaceAttr(7, "Banned")}}}},
				} {
					require.True(t, isRaftSemanticRejection(checkRaftSupportedProposal(p)))
				}
				require.NoError(t, checkRaftSupportedProposal(drop(base+7)), "repeated namespace deletion is idempotent")
				err := checkRaftSupportedProposal(&pb.Proposal{StartTs: base + 7, DeleteNs: &pb.DeleteNsRequest{Namespace: 0}})
				require.True(t, isRaftSemanticRejection(err))
				require.Contains(t, err.Error(), "non-root")
				_, err = processTask(context.Background(), &pb.Query{Attr: attr, ReadTs: base + 100, Cache: UseTxnCache}, 1)
				require.ErrorContains(t, err, "namespace 7", "ban refuses before future Oracle wait or cache")
			}
			original := entries[0]
			original.Index = 10
			require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 10}, []raftpb.Entry{original}, &raftpb.Snapshot{}))
			require.NoError(t, wal.Sync())
			require.NoError(t, apply(original))
			require.Equal(t, wantPending, posting.Oracle().NumPendingTxns(), "old success retry cannot recreate dropped transaction")
		})
	}
}
