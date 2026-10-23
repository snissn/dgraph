/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
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

func TestRaftSemanticRejectionAndOrderedAbortReplay(t *testing.T) {
	oldConfig, oldState, oldPstore, oldStore := Config, State, pstore, postingStore
	oldHardSync, oldNode := x.WorkerConfig.HardSync, gr.Node
	oldRetries := x.Config.MaxRetries
	x.Config.MaxRetries = 3
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var current *ServerState
	dir := t.TempDir()
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if current != nil {
			current.closePostingStore()
		}
		Config, State, pstore, postingStore = oldConfig, oldState, oldPstore, oldStore
		x.WorkerConfig.HardSync, gr.Node = oldHardSync, oldNode
		x.Config.MaxRetries = oldRetries
		posting.Init(oldPstore, 0, false)
		schema.Init(oldPstore)
	})
	Config = Options{PostingDir: dir + "/p", WALDir: dir + "/w", PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier: PostingStoreTierBenchmarkMinimal, PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint,
		PostingStoreEvents: true}
	x.WorkerConfig.HardSync = true
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	wal := raftwal.Init(Config.WALDir)
	defer wal.Close()
	open := func() *node {
		current = &ServerState{}
		require.NoError(t, current.openPostingStore())
		State, pstore, postingStore = *current, nil, current.PostingStore
		posting.InitForStore(current.PostingStore, 0, false)
		schema.InitForStore(schemaPostingStore{store: current.PostingStore})
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
		current.closePostingStore()
		return open()
	}
	n := open()
	base := posting.Oracle().MaxAssigned() + 10
	pred, unseen := x.AttrInRootNamespace("number"), x.AttrInRootNamespace("unseen")
	edge := func(attr, value string) *pb.DirectedEdge {
		return &pb.DirectedEdge{Entity: 1, Attr: attr, Value: []byte(value), ValueType: pb.Posting_STRING,
			Op: pb.DirectedEdge_SET}
	}
	mutation := func(ts uint64, edges ...*pb.DirectedEdge) *pb.Proposal {
		return &pb.Proposal{Mutations: &pb.Mutations{StartTs: ts, Edges: edges}}
	}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, mutation(base, edge(pred, "0"))),
		checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 1,
			Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}),
		// Real proposal admission skips type checks while schema is absent for
		// both edges. Inference from the first edge makes the second invalid.
		// Rejection must precede both inferred schema and first-edge effects.
		checkpointEntry(t, 3, mutation(base+2, &pb.DirectedEdge{Entity: 1, Attr: unseen,
			ValueId: 2, ValueType: pb.Posting_UID, Op: pb.DirectedEdge_SET}, edge(unseen, "invalid scalar"))),
		checkpointEntry(t, 4, mutation(base+3, edge(pred, "1"))),
		checkpointEntry(t, 5, mutation(base+4, edge(pred, "2"))),
		// This is the real transaction-conflict authority: Zero's ordered abort.
		checkpointEntry(t, 6, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5,
			Txns: []*pb.TxnStatus{{StartTs: base + 3}, {StartTs: base + 4, CommitTs: base + 5}}}}),
	}
	duplicate := entries[2]
	duplicate.Index = 7
	entries = append(entries, duplicate)
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 7}, entries, &raftpb.Snapshot{}))
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
	require.NoError(t, apply(entries[0]))
	require.NoError(t, apply(entries[1]))
	require.NoError(t, n.checkpointRaftCut(2, true))
	var rejection string
	checkSuffix := func() {
		err := apply(entries[2])
		require.True(t, isRaftSemanticRejection(err))
		if rejection == "" {
			rejection = err.Error()
		}
		require.EqualError(t, err, rejection)
		_, exists := schema.State().Get(context.Background(), unseen)
		require.False(t, exists, "preflight rejection must precede inferred schema write")
		require.Zero(t, posting.Oracle().NumPendingTxns())
		require.NoError(t, apply(entries[3]))
		require.NoError(t, apply(entries[4]))
		require.Equal(t, 2, posting.Oracle().NumPendingTxns())
		require.NoError(t, apply(entries[5]))
		require.Zero(t, posting.Oracle().NumPendingTxns())
		require.EqualError(t, apply(entries[6]), rejection)
		list, err := posting.GetNoStore(x.DataKey(pred, 1), math.MaxUint64)
		require.NoError(t, err)
		value, err := list.Value(math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, []byte("2"), value.Value)
	}
	checkSuffix()
	n = reopen()
	require.Equal(t, uint64(2), n.Cfg.Applied)
	require.NotContains(t, n.appliedProposals, uint64(3), "rejection must be recomputed, not restored from metadata")
	checkSuffix()
	require.NoError(t, n.checkpointRaftCut(7, true))
	data, ok := current.TreeDBStore.RecoveredRaftCheckpoint()
	require.True(t, ok)
	var cert raftCheckpointMetadata
	require.NoError(t, json.Unmarshal(data, &cert))
	require.Equal(t, rejection, cert.AppliedProposals[3].Rejection)
	n = reopen()
	require.Equal(t, uint64(7), n.Cfg.Applied)
	duplicate.Index = 8
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 8}, []raftpb.Entry{duplicate}, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(duplicate.Data)))
	require.EqualError(t, apply(duplicate), rejection, "certified duplicate preserves rejection, never succeeds")

	// A long pending transaction must pause new mutation admission without
	// consuming the decision lane. Its ordered abort then permits a cut and
	// returns admission credits. Use actual retained entry debt, not a mocked
	// byte counter, and the production proposal caller for both admission paths.
	pending := checkpointEntry(t, 9, mutation(base+6, edge(pred, "pending")))
	debt := []raftpb.Entry{pending}
	for index := uint64(10); index <= 7+raftRecoveryMaxEntries; index++ {
		debt = append(debt, checkpointEntry(t, index, &pb.Proposal{Delta: &pb.OracleDelta{}}))
	}
	last := debt[len(debt)-1].Index
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: last}, debt, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range debt {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
		require.NoError(t, apply(e))
	}
	require.Equal(t, 1, posting.Oracle().NumPendingTxns())
	require.NoError(t, n.checkpointRaftCut(last, true))
	require.Equal(t, uint64(7), atomic.LoadUint64(&n.raftCertifiedIndex))
	require.ErrorContains(t, n.proposeAndWait(context.Background(), mutation(base+7, edge(pred, "blocked"))), "recovery debt limit")
	abortProposal := &pb.Proposal{Delta: &pb.OracleDelta{Txns: []*pb.TxnStatus{{StartTs: base + 6}}}}
	require.ErrorContains(t, n.proposeAndWait(context.Background(), abortProposal), "Raft isn't initialized",
		"decision passes debt admission and reaches the deliberately absent transport")
	abort := checkpointEntry(t, last+1, abortProposal)
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: last + 1}, []raftpb.Entry{abort}, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(abort.Data)))
	require.NoError(t, apply(abort))
	require.Zero(t, posting.Oracle().NumPendingTxns())
	require.NoError(t, n.checkpointRaftCut(last+1, true))
	require.Equal(t, last+1, atomic.LoadUint64(&n.raftCertifiedIndex))
	release, err := n.reserveRaftRecoveryDebt(1)
	require.NoError(t, err, "certified abort restores mutation capacity")
	release()
	// A runtime conflict or a joined storage failure is not proof of a pure rejection.
	require.False(t, isRaftSemanticRejection(x.ErrConflict))
	require.False(t, isRaftSemanticRejection(errors.Join(&raftSemanticRejection{"invalid"}, errors.New("I/O failure"))))
}
