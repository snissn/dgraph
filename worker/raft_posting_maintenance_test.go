/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestRaftPostingHistoryPruneCertifiedAndInterruptedReplay(t *testing.T) {
	oldConfig, oldState, oldBadger, oldStore, oldNode := Config, State, pstore, postingStore, gr.Node
	oldHardSync, oldRetries, oldHealth := x.WorkerConfig.HardSync, x.Config.MaxRetries, x.HealthCheck() == nil
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
		x.UpdateHealthStatus(oldHealth)
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	Config = snapshotInstallOptions(t.TempDir())
	x.WorkerConfig.HardSync, x.Config.MaxRetries = true, 3
	x.UpdateHealthStatus(true)
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
	reopen := func() *node {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		current.closePostingStore()
		return open()
	}
	n := open()
	base := posting.Oracle().MaxAssigned() + 20
	pred := x.AttrInRootNamespace("prune-replay-value")
	mutation := func(ts uint64, value string) *pb.Proposal {
		return &pb.Proposal{Mutations: &pb.Mutations{StartTs: ts, Edges: []*pb.DirectedEdge{{Entity: 1, Attr: pred, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}}
	}
	floor := base + 5
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, mutation(base, "old")),
		checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 10, Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}),
		checkpointEntry(t, 3, &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: floor}}),
	}
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 3}, entries, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range entries {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
	}
	applyCheckpointEntry(t, n, entries[0])
	applyCheckpointEntry(t, n, entries[1])
	require.NoError(t, n.checkpointRaftCut(2, true))
	applyCheckpointEntry(t, n, entries[2])
	// Interrupt after the full physical mutation but before its certificate.
	// Close must seal request admission, and reopen must restore old P and S
	// from the retained exact root rather than bless the newer working seal.
	cap, err := current.TreeDBStore.BeginRaftPostingMaintenance(context.Background(), floor)
	require.NoError(t, err)
	require.NoError(t, cap.MaterializeBaselines(context.Background()))
	_, err = cap.AdvanceAndPrune(4)
	require.NoError(t, err)
	cap.Close()
	require.True(t, current.TreeDBStore.IsClosed())
	n = reopen()
	physical, err := current.TreeDBStore.DiscardFloor()
	require.NoError(t, err)
	require.Zero(t, physical)
	require.Zero(t, n.raftPostingFloor)
	applyCheckpointEntry(t, n, entries[2])
	_, err = n.pruneRaftPostingHistory(context.Background(), floor, 4)
	require.NoError(t, err)
	data, ok := current.TreeDBStore.RecoveredRaftCheckpoint()
	require.True(t, ok)
	var cert raftCheckpointMetadata
	require.NoError(t, json.Unmarshal(data, &cert))
	require.Equal(t, floor, cert.PhysicalFloor)
	require.Equal(t, floor, cert.SemanticFloor)
	require.Equal(t, uint64(3), cert.Index)
	n = reopen()
	physical, err = current.TreeDBStore.DiscardFloor()
	require.NoError(t, err)
	require.Equal(t, floor, physical)
	require.Equal(t, floor, n.raftPostingFloor)
	read := func(ts uint64) string {
		list, err := posting.GetNoStore(x.DataKey(pred, 1), ts)
		require.NoError(t, err)
		value, err := list.Value(ts)
		require.NoError(t, err)
		return string(value.Value.([]byte))
	}
	require.Equal(t, "old", read(floor+1))
	require.Equal(t, "old", read(floor+2))
	// A retained suffix applied above the physical floor behaves exactly as
	// normal prewrite/Delta input, and its next certificate binds the same P.
	suffix := []raftpb.Entry{
		checkpointEntry(t, 4, mutation(base+11, "new")),
		checkpointEntry(t, 5, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 12, Txns: []*pb.TxnStatus{{StartTs: base + 11, CommitTs: base + 12}}}}),
	}
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 5}, suffix, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range suffix {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
		applyCheckpointEntry(t, n, e)
	}
	require.Equal(t, "old", read(floor+1))
	require.Equal(t, "new", read(base+12))
	require.NoError(t, n.checkpointRaftCut(5, true))
	n = reopen()
	require.Equal(t, "new", read(base+12))
	require.Equal(t, floor, n.raftPostingFloor)
	oldCert, ok := current.TreeDBStore.RaftCheckpointCertificate()
	require.True(t, ok)
	working := []raftpb.Entry{
		checkpointEntry(t, 6, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 35}}),
		checkpointEntry(t, 7, mutation(base+36, "pending-newer")),
		checkpointEntry(t, 8, &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: base + 20}}),
	}
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 8}, working, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range working {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
		applyCheckpointEntry(t, n, e)
	}
	var before api.TxnContext
	posting.Oracle().GetTxn(base+36).FillContext(&before, 1, false)
	require.NotEmpty(t, before.Keys)
	_, err = n.pruneRaftPostingHistory(context.Background(), base+20, 4)
	require.NoError(t, err, "a newer pending transaction must not prevent physical progress")
	physical, err = current.TreeDBStore.DiscardFloor()
	require.NoError(t, err)
	require.Equal(t, base+20, physical)
	stillOld, ok := current.TreeDBStore.RaftCheckpointCertificate()
	require.True(t, ok)
	require.Equal(t, oldCert.Generation, stillOld.Generation)
	require.Equal(t, oldCert.Digest, stillOld.Digest)
	require.Equal(t, uint64(5), atomic.LoadUint64(&n.raftCertifiedIndex), "working progress cannot advance WAL retention")
	require.Equal(t, 1, posting.Oracle().NumPendingTxns())
	// Crash/reopen discards unlogged physical progress, then reconstructs the
	// exact same pending overlay and conflict keys from the retained WAL.
	n = reopen()
	physical, err = current.TreeDBStore.DiscardFloor()
	require.NoError(t, err)
	require.Equal(t, floor, physical)
	for _, e := range working {
		applyCheckpointEntry(t, n, e)
	}
	var after api.TxnContext
	posting.Oracle().GetTxn(base+36).FillContext(&after, 1, false)
	require.Equal(t, before.StartTs, after.StartTs)
	require.ElementsMatch(t, before.Keys, after.Keys)
	require.ElementsMatch(t, before.Preds, after.Preds)
	_, err = n.pruneRaftPostingHistory(context.Background(), base+20, 4)
	require.NoError(t, err)
	decisions := []raftpb.Entry{
		checkpointEntry(t, 9, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 37, Txns: []*pb.TxnStatus{{StartTs: base + 36, CommitTs: base + 37}}}}),
		checkpointEntry(t, 10, mutation(base+40, "must-abort")),
		checkpointEntry(t, 11, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 41, Txns: []*pb.TxnStatus{{StartTs: base + 40}}}}),
	}
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 11}, decisions, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range decisions {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
		applyCheckpointEntry(t, n, e)
	}
	require.Zero(t, posting.Oracle().NumPendingTxns())
	require.Equal(t, "new", read(base+21))
	require.Equal(t, "pending-newer", read(base+41))
	require.NoError(t, n.checkpointRaftCut(11, true))
	n = reopen()
	require.Equal(t, "pending-newer", read(base+41))
	require.Equal(t, base+20, n.raftPostingFloor)
}
