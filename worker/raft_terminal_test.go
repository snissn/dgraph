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

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftTerminalDispositionPreEffectAndReplay(t *testing.T) {
	for _, schedule := range []string{"old-cut", "every-entry"} {
		t.Run(schedule, func(t *testing.T) { testRaftTerminalDisposition(t, schedule) })
	}
}
func testRaftTerminalDisposition(t *testing.T, schedule string) {
	oldConfig, oldState, oldBadger, oldStore, oldNode := Config, State, pstore, postingStore, gr.Node
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var current *ServerState
	taskdir := t.TempDir()
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
	Config = snapshotInstallOptions(taskdir)
	x.WorkerConfig.HardSync, x.Config.MaxRetries = true, 3
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
	var index uint64
	rejections := make(map[uint64]string)
	apply := func(key uint64, p *pb.Proposal) error {
		index++
		e := checkpointEntry(t, index, p)
		binary.BigEndian.PutUint64(e.Data[:8], key)
		require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: index}, []raftpb.Entry{e}, &raftpb.Snapshot{}))
		require.NoError(t, wal.Sync())
		var q pb.Proposal
		require.NoError(t, proto.Unmarshal(e.Data[8:], &q))
		q.Index = index
		updateStartTs(&q)
		err := n.applyTrackedProposal(&q, key, e.Size())
		n.raftSemanticIndex = index
		n.Applied.SetDoneUntil(index)
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
		if err != nil {
			require.True(t, isRaftSemanticRejection(err), "unexpected uncertain error: %v", err)
			rejections[index] = err.Error()
		}
		if schedule == "every-entry" {
			require.NoError(t, n.checkpointRaftCut(index, true))
		}
		return err
	}
	mutation := func(ts uint64, v string) *pb.Proposal {
		return &pb.Proposal{Mutations: &pb.Mutations{StartTs: ts, Edges: []*pb.DirectedEdge{{Entity: 1, Attr: x.AttrInRootNamespace("terminal"), Value: []byte(v), ValueType: pb.Posting_STRING}}}}
	}
	decision := func(start, commit, max uint64) *pb.Proposal {
		return &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: max, Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}}}
	}

	// Abort-only traffic must not fabricate an assignment watermark, even if an
	// unrelated process-local Oracle has already observed a future notification.
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: 10000})
	require.NoError(t, apply(100, decision(1, 0, 0)))
	require.Zero(t, n.raftOrderedAssigned)
	require.NoError(t, apply(10, mutation(10, "one")))
	require.NoError(t, n.checkpointRaftCut(index, true))
	require.NoError(t, apply(11, decision(10, 11, 20)))
	err := apply(12, decision(10, 0, 20))
	require.True(t, isRaftSemanticRejection(err), "contradictory decision must refuse before effects: %v", err)
	manifest, err := State.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	// Intrinsic Delta keys can collide with a mutation key without replacing its
	// original binding, while the byte-identical successful retry has no effects.
	require.NoError(t, apply(10, decision(10, 11, 20)))
	require.NoError(t, apply(10, mutation(10, "one")))
	require.True(t, isRaftSemanticRejection(apply(13, mutation(10, "one"))))
	same, err := State.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, manifest, same)
	require.NoError(t, apply(30, mutation(30, "two")))
	require.NoError(t, apply(40, mutation(40, "three")))
	before, err := State.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	batch := decision(30, 31, 100)
	batch.Delta.Txns = append(batch.Delta.Txns, &pb.TxnStatus{StartTs: 10, CommitTs: 0})
	require.True(t, isRaftSemanticRejection(apply(50, batch)))
	batch = decision(40, 0, 100)
	batch.Delta.Txns = append(batch.Delta.Txns, &pb.TxnStatus{StartTs: 40, CommitTs: 41})
	require.True(t, isRaftSemanticRejection(apply(51, batch)))
	same, err = State.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, before, same)
	require.NotNil(t, posting.Oracle().GetTxn(30))
	require.NotNil(t, posting.Oracle().GetTxn(40))
	newer := &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: 100, ChecksumSequence: 20, GroupChecksums: map[uint32]uint64{1: 777}}}
	require.NoError(t, apply(60, newer))
	// Lower assignment and older checksum authority still carry a NEW abort.
	older := decision(30, 0, 20)
	older.Delta.ChecksumSequence = 10
	older.Delta.GroupChecksums = map[uint32]uint64{1: 555}
	require.NoError(t, apply(61, older))
	require.Nil(t, posting.Oracle().GetTxn(30))
	require.Equal(t, uint64(777), n.raftOrderedChecksum)
	equal := proto.Clone(newer).(*pb.Proposal)
	equal.Delta.ChecksumSequence = 21
	equal.Delta.GroupChecksums[1] = 888
	require.NoError(t, apply(62, equal))
	require.Equal(t, uint64(888), n.raftOrderedChecksum)
	conflicting := proto.Clone(equal).(*pb.Proposal)
	conflicting.Delta.GroupChecksums[2] = 99
	conflicting.Delta.Txns = []*pb.TxnStatus{{StartTs: 40}}
	require.True(t, isRaftSemanticRejection(apply(63, conflicting)))
	require.NotNil(t, posting.Oracle().GetTxn(40))
	unsequenced := proto.Clone(equal).(*pb.Proposal)
	unsequenced.Delta.ChecksumSequence = 0
	require.True(t, isRaftSemanticRejection(apply(64, unsequenced)))
	require.NoError(t, apply(65, &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: 20}}))
	require.NoError(t, apply(66, older))
	require.Equal(t, uint64(888), n.raftOrderedChecksum)
	require.NoError(t, apply(67, decision(40, 41, 100)))
	require.Equal(t, uint64(100), n.raftOrderedAssigned)
	// Controls never accumulate arbitrary retry keys in the certificate map.
	require.Len(t, n.appliedProposals, 2) // StartTs10 retired by S; pending30+40 identities remain.
	for key := uint64(1000); key < 1010; key++ {
		require.NoError(t, apply(key, newer))
		require.NoError(t, apply(key, &pb.Proposal{Snapshot: &pb.Snapshot{}}))
	}
	require.Len(t, n.appliedProposals, 2)
	expected, err := State.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(2), expected.TerminalCount)
	// Close can flush arbitrary live data; restart must restore exactly the cut
	// and recompute every uncertified semantic rejection before read admission.
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	current = nil
	n = open()
	certified := atomic.LoadUint64(&n.raftCertifiedIndex)
	entries, err := wal.Entries(certified+1, index+1, ^uint64(0))
	require.NoError(t, err)
	for _, entry := range entries {
		var p pb.Proposal
		require.NoError(t, proto.Unmarshal(entry.Data[8:], &p))
		p.Index = entry.Index
		updateStartTs(&p)
		err := n.applyTrackedProposal(&p, binary.BigEndian.Uint64(entry.Data[:8]), entry.Size())
		if message, ok := rejections[entry.Index]; ok {
			require.EqualError(t, err, message)
		} else {
			require.NoError(t, err)
		}
		n.raftSemanticIndex = entry.Index
		n.Applied.SetDoneUntil(entry.Index)
	}
	actual, err := State.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(888), n.raftOrderedChecksum)
	require.Equal(t, uint64(21), n.raftChecksumSequence)

	require.NoError(t, n.checkpointRaftCut(index, true))
	if schedule == "old-cut" {
		require.NoError(t, apply(5000, mutation(1000, "reserved")))
		decisions := make([]*pb.TxnStatus, posting.RaftTerminalAdmissionLimit)
		for i := range decisions {
			decisions[i] = &pb.TxnStatus{StartTs: uint64(i) + 10000}
		}
		require.NoError(t, apply(5001, &pb.Proposal{Delta: &pb.OracleDelta{Txns: decisions}}))
		require.True(t, isRaftSemanticRejection(apply(5002, mutation(2000, "paused"))))
		// Existing pending state and every decision bypass NEW-prewrite admission.
		require.NoError(t, apply(5003, mutation(1000, "existing growth")))
		require.NoError(t, apply(5004, decision(1000, 1001, 100001)))
		full, err := State.TreeDBStore.RaftPendingManifest()
		require.NoError(t, err)
		require.GreaterOrEqual(t, full.TerminalCount, uint64(posting.RaftTerminalAdmissionLimit))
		require.Zero(t, full.Count)
		require.NoError(t, n.checkpointRaftCut(index, true)) // fixed metadata despite 65k terminal rows
		require.NoError(t, apply(5005, &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: 90000}}))
		retired, err := State.TreeDBStore.RaftPendingManifest()
		require.NoError(t, err)
		require.Zero(t, retired.TerminalCount)
		require.NoError(t, apply(5006, mutation(90001, "resumed")))
		require.NoError(t, n.checkpointRaftCut(index, true))
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		current.closePostingStore()
		current = nil
		n = open()
		require.Equal(t, uint64(90000), n.raftPostingFloor)
		require.NotNil(t, posting.Oracle().GetTxn(90001))
	}
}

func TestSequencedOracleBatchKeepsExactMapAndIndependentDecisions(t *testing.T) {
	target := &pb.OracleDelta{MaxAssigned: 100, ChecksumSequence: 20, GroupChecksums: map[uint32]uint64{1: 222}}
	older := &pb.OracleDelta{MaxAssigned: 10, ChecksumSequence: 19, GroupChecksums: map[uint32]uint64{1: 111}, Txns: []*pb.TxnStatus{{StartTs: 5}}}
	require.NoError(t, mergeOracleDelta(target, older))
	require.Equal(t, uint64(20), target.ChecksumSequence)
	require.Equal(t, uint64(222), target.GroupChecksums[1])
	require.Len(t, target.Txns, 1)
	newer := &pb.OracleDelta{MaxAssigned: 100, ChecksumSequence: 21, GroupChecksums: map[uint32]uint64{1: 333}}
	require.NoError(t, mergeOracleDelta(target, newer))
	require.Equal(t, uint64(333), target.GroupChecksums[1])
	contradictory := proto.Clone(newer).(*pb.OracleDelta)
	contradictory.GroupChecksums[2] = 555
	require.Error(t, mergeOracleDelta(target, contradictory))
	require.Equal(t, newer.GroupChecksums, target.GroupChecksums)
	for _, p := range []*pb.Proposal{{State: &pb.MembershipState{}}, {Delta: &pb.OracleDelta{}}, {Snapshot: &pb.Snapshot{}}, {PostingFloor: &pb.PostingFloorRequest{Version: 1}}} {
		require.False(t, raftProposalNeedsIdentity(p))
	}
	require.True(t, raftProposalNeedsIdentity(&pb.Proposal{State: &pb.MembershipState{}, Mutations: &pb.Mutations{StartTs: 10}}))
}

func TestRaftNamespaceAdminUsesExistingDebtReservation(t *testing.T) {
	oldConfig := Config
	t.Cleanup(func() { Config = oldConfig })
	Config = snapshotInstallOptions(t.TempDir())
	oldState, oldHardSync := State, x.WorkerConfig.HardSync
	defer func() { State = oldState; x.WorkerConfig.HardSync = oldHardSync }()
	x.WorkerConfig.HardSync = true
	state := &ServerState{}
	require.NoError(t, state.openPostingStore())
	defer state.closePostingStore()
	State = *state
	wal := raftwal.Init(t.TempDir())
	defer wal.Close()
	n := newNode(wal, 1, 1, "")
	release, err := n.reserveRaftRecoveryDebt(raftRecoveryMaxBytes)
	require.NoError(t, err)
	defer release()
	err = n.proposeAndWait(context.Background(), &pb.Proposal{StartTs: 10, DeleteNs: &pb.DeleteNsRequest{Namespace: 7}})
	require.ErrorContains(t, err, "recovery debt limit")
	// Decisions and the explicitly authorized retirement control remain able to
	// progress to normal Raft admission, rather than sharing exhausted debt.
	require.ErrorContains(t, n.proposeAndWait(context.Background(), &pb.Proposal{Delta: &pb.OracleDelta{}}), "Raft isn't initialized")
	require.ErrorContains(t, n.proposeAndWait(context.Background(), &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: 5}}), "Raft isn't initialized")
}
