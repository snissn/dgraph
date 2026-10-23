/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftPostingFloorOrderedReplayAndContentBoundRetries(t *testing.T) {
	oldConfig, oldState, oldBadger, oldStore, oldNode := Config, State, pstore, postingStore, gr.Node
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var current *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if current != nil {
			current.closePostingStore()
		}
		Config, State, pstore, postingStore, gr.Node = oldConfig, oldState, oldBadger, oldStore, oldNode
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	dir := t.TempDir()
	Config = snapshotInstallOptions(dir)
	x.WorkerConfig.HardSync = true
	x.Config.MaxRetries = 3
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	wal := raftwal.Init(Config.WALDir)
	defer wal.Close()
	open := func() *node {
		current = &ServerState{}
		require.NoError(t, current.openPostingStore())
		State, pstore, postingStore = *current, nil, current.PostingStore
		posting.InitForStore(postingStore, 0, false)
		schema.InitForStore(schemaPostingStore{store: postingStore})
		require.NoError(t, schema.LoadFromDb(context.Background()))
		n := newNode(wal, 1, 1, "")
		gr.Node = n
		require.NoError(t, n.recoverRaftCheckpoint())
		n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
		return n
	}
	n := open()
	base := posting.Oracle().MaxAssigned() + 20
	pred := x.AttrInRootNamespace("floor_value")
	mutation := func(ts uint64, value string) *pb.Proposal {
		return &pb.Proposal{Mutations: &pb.Mutations{StartTs: ts, Edges: []*pb.DirectedEdge{{Entity: 1, Attr: pred, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}}
	}
	floor := func(ts uint64) *pb.Proposal {
		return &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: ts}}
	}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, mutation(base, "first")),
		checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 3, Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}),
		checkpointEntry(t, 3, mutation(base+4, "pending")),
		checkpointEntry(t, 4, floor(base+1)),
	}
	expired := entries[0]
	expired.Index = 5
	entries = append(entries, expired)
	collision := checkpointEntry(t, 6, mutation(base+4, "changed"))
	binary.BigEndian.PutUint64(collision.Data[:8], 3)
	entries = append(entries, collision)
	duplicate := entries[2]
	duplicate.Index = 7
	entries = append(entries, duplicate,
		checkpointEntry(t, 8, floor(base+4)),
		checkpointEntry(t, 9, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5, Txns: []*pb.TxnStatus{{StartTs: base + 4, CommitTs: base + 4}}}}),
		checkpointEntry(t, 10, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5, Txns: []*pb.TxnStatus{{StartTs: base + 4}}}}),
		checkpointEntry(t, 11, floor(base+4)))
	expired = entries[2]
	expired.Index = 12
	entries = append(entries, expired)
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 12}, entries, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, entry := range entries {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(entry.Data)))
	}
	apply := func(entry raftpb.Entry) error {
		var p pb.Proposal
		require.NoError(t, proto.Unmarshal(entry.Data[8:], &p))
		p.Index = entry.Index
		updateStartTs(&p)
		err := n.applyTrackedProposal(&p, binary.BigEndian.Uint64(entry.Data[:8]), entry.Size())
		n.raftAppliedBytes += uint64(len(entry.Data))
		n.raftSemanticIndex = entry.Index
		n.Applied.SetDoneUntil(entry.Index)
		return err
	}
	require.NoError(t, apply(entries[0]))
	require.NoError(t, apply(entries[1]))
	require.NoError(t, n.checkpointRaftCut(2, true))
	replay := func() {
		require.NoError(t, apply(entries[2]))
		require.Equal(t, 1, posting.Oracle().NumPendingTxns())
		require.NoError(t, apply(entries[3]))
		require.Equal(t, base+1, n.raftPostingFloor)
		require.NotContains(t, n.appliedProposals, uint64(1))
		require.ErrorContains(t, apply(entries[4]), "expired at replicated posting floor")
		require.ErrorContains(t, apply(entries[5]), "different transaction/content")
		require.NoError(t, apply(entries[6]))
		require.Equal(t, 1, posting.Oracle().NumPendingTxns())
		require.ErrorContains(t, apply(entries[7]), "must precede assigned frontier")
		require.ErrorContains(t, apply(entries[8]), "commit timestamp must exceed")
		require.Equal(t, 1, posting.Oracle().NumPendingTxns(), "malformed decision leaves original pending txn untouched")
		require.Equal(t, base+3, n.raftOrderedAssigned)
		require.NoError(t, apply(entries[9]))
		require.Zero(t, posting.Oracle().NumPendingTxns())
		require.NoError(t, apply(entries[10]))
		require.Equal(t, base+4, n.raftPostingFloor)
		require.NotContains(t, n.appliedProposals, uint64(3))
		require.ErrorContains(t, apply(entries[11]), "expired at replicated posting floor")
		count := len(n.appliedProposals)
		require.NoError(t, n.expireRaftProposals(time.Now().Add(24*time.Hour), time.Minute))
		require.Len(t, n.appliedProposals, count)
	}
	replay()
	// Global Oracle is intentionally monotone across this in-process reopen. S
	// still uses its preceding ordered frontier, reconstructed from cert+WAL.
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	n = open()
	require.Equal(t, base+3, n.raftOrderedAssigned)
	require.Zero(t, n.raftPostingFloor)
	replay()
	require.NoError(t, n.checkpointRaftCut(12, true))
	metadata, _ := current.TreeDBStore.RecoveredRaftCheckpoint()
	var cert raftCheckpointMetadata
	require.NoError(t, json.Unmarshal(metadata, &cert))
	require.Equal(t, base+4, cert.SemanticFloor)
	require.Equal(t, uint64(11), cert.FloorTransitionIndex)
	require.Zero(t, cert.PhysicalFloor)
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	n = open()
	require.Equal(t, base+4, n.raftPostingFloor)
	require.Equal(t, uint64(11), n.raftFloorIndex)
	_, err := current.TreeDBStore.PinRaftReadTimestamp(base + 4)
	require.ErrorContains(t, err, "replicated posting floor")
	release, err := current.TreeDBStore.PinRaftReadTimestamp(base + 5)
	require.NoError(t, err)
	release()
	require.ErrorContains(t, apply(entries[11]), "expired at replicated posting floor", "old retry remains refused after signature retirement+cert/reopen")
}
