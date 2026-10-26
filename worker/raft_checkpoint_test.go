/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
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

func checkpointEntry(t *testing.T, index uint64, p *pb.Proposal) raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(p)
	require.NoError(t, err)
	framed := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(framed, index)
	copy(framed[8:], data)
	return raftpb.Entry{Index: index, Term: 1, Type: raftpb.EntryNormal, Data: framed}
}

func applyCheckpointEntry(t *testing.T, n *node, e raftpb.Entry) {
	t.Helper()
	var p pb.Proposal
	require.NoError(t, proto.Unmarshal(e.Data[8:], &p))
	p.Index = e.Index
	updateStartTs(&p)
	require.NoError(t, n.applyTrackedProposal(&p, binary.BigEndian.Uint64(e.Data[:8]), e.Size()))
	n.raftAppliedBytes += uint64(len(e.Data))
	n.raftSemanticIndex = e.Index
	n.Applied.SetDoneUntil(e.Index)
}

func TestRaftCheckpointRealMutationDeltaExactRootReplay(t *testing.T) {
	for _, cut := range []string{"after_retention", "before_retention", "failed_native_snapshot_restart", "pending_certificate", "pending_commit_certificate"} {
		t.Run(cut, func(t *testing.T) { testRaftCheckpointRealMutationDeltaExactRootReplay(t, cut) })
	}
}

func TestRaftCheckpointReadyCut(t *testing.T) {
	for _, cut := range []string{"after_retention", "pending_commit_certificate", "failed_native_snapshot_restart"} {
		t.Run(cut, func(t *testing.T) { testRaftCheckpointReplayWithFormat(t, cut, true) })
	}
}
func testRaftCheckpointRealMutationDeltaExactRootReplay(t *testing.T, cut string) {
	testRaftCheckpointReplayWithFormat(t, cut, false)
}
func testRaftCheckpointReplayWithFormat(t *testing.T, cut string, ready bool) {
	oldConfig, oldState, oldPstore, oldPostingStore, oldHardSync := Config, State, pstore, postingStore, x.WorkerConfig.HardSync
	oldNode := gr.Node
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
		Config, State, pstore, postingStore, x.WorkerConfig.HardSync = oldConfig, oldState, oldPstore, oldPostingStore, oldHardSync
		gr.Node = oldNode
		x.Config.MaxRetries = oldRetries
		posting.Init(oldPstore, 0, false)
		schema.Init(oldPstore)
	})
	Config = Options{PostingDir: dir + "/p", WALDir: dir + "/w", PostingStoreBackend: PostingStoreBackendTreeDB, PostingStoreTier: PostingStoreTierBenchmarkMinimal, PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint, PostingStoreEvents: true}
	x.WorkerConfig.HardSync = true
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	var wal *raftwal.DiskStorage
	if ready {
		var err error
		wal, err = raftwal.InitCheckpointEncrypted(Config.WALDir, nil)
		require.NoError(t, err)
	} else {
		wal = raftwal.Init(Config.WALDir)
	}
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
	n := open()
	pendingCertificate := cut == "pending_certificate" || cut == "pending_commit_certificate"
	certifiedIndex := uint64(2)
	if pendingCertificate {
		// Exercise the normal indexed posting pipeline, including its canonical
		// index deltas, rather than merely persisting raw input edges.
		su := &pb.SchemaUpdate{Predicate: x.AttrInRootNamespace("name"), ValueType: pb.Posting_STRING,
			Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}
		require.NoError(t, writeSchema(su, posting.Oracle().MaxAssigned()+1))
		schema.State().Set(su.Predicate, su)
	}
	base := posting.Oracle().MaxAssigned() + 10
	predicate := x.AttrInRootNamespace("name")
	mutation := func(ts uint64, value string) *pb.Proposal {
		return &pb.Proposal{Mutations: &pb.Mutations{StartTs: ts, Edges: []*pb.DirectedEdge{{Entity: 1, Attr: predicate, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}}
	}
	decision := func(start, commit, max uint64) *pb.Proposal {
		return &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: max, Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}}}
	}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, mutation(base, "certified")),
		checkpointEntry(t, 2, decision(base, base+1, base+1)),
		checkpointEntry(t, 3, mutation(base+2, "suffix")),
		checkpointEntry(t, 4, decision(base+2, base+3, base+3)),
		checkpointEntry(t, 5, mutation(base+4, "pending")),
	}
	duplicate := entries[0]
	duplicate.Index = 6 // Same proposal key and size, across the certified cut.
	entries = append(entries, duplicate)
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 6}, entries, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range entries {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
	}
	applyCheckpointEntry(t, n, entries[0])
	applyCheckpointEntry(t, n, entries[1])
	if cut == "before_retention" {
		c := raftCheckpointMetadata{Version: raftCheckpointVersion, PendingOverlay: posting.EmptyRaftPendingManifest(), Group: 1, RaftID: 1, Index: 2, Term: 1, OracleMaxAssigned: posting.Oracle().MaxAssigned(), ConfState: *n.ConfState()}
		overlay, err := current.TreeDBStore.RaftPendingManifest()
		require.NoError(t, err)
		c.PendingOverlay = overlay
		c.AppliedProposals = make(map[uint64]raftProposalSignature)
		for key, p := range n.appliedProposals {
			c.AppliedProposals[key] = raftProposalSignature{Size: p.size, StartTs: p.startTs, Digest: p.digest, SeenUnixNano: p.seen.UnixNano()}
		}
		data, err := json.Marshal(c)
		require.NoError(t, err)
		require.NoError(t, current.TreeDBStore.CheckpointRaft(context.Background(), data))
		// A crash after certificate publication but before retention authorization
		// must recover the new root. The old scalar hint may be arbitrarily ahead.
		wal.SetUint(raftwal.CheckpointIndex, 999)
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		current.closePostingStore()
		n = open()
	} else {
		require.NoError(t, n.checkpointRaftCut(2, true))
	}
	certBytes, ok := current.TreeDBStore.RecoveredRaftCheckpoint()
	require.True(t, ok)
	var cert raftCheckpointMetadata
	require.NoError(t, json.Unmarshal(certBytes, &cert))
	require.Equal(t, uint64(2), cert.Index)
	require.Equal(t, base+1, cert.OracleMaxAssigned)
	require.Contains(t, cert.AppliedProposals, uint64(1))
	first, err := wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), first)
	for _, e := range entries[2:] {
		applyCheckpointEntry(t, n, e)
	}
	require.Equal(t, 1, posting.Oracle().NumPendingTxns())
	if pendingCertificate {
		require.NoError(t, n.checkpointRaftCut(6, true))
		certifiedIndex = 6
		require.Equal(t, certifiedIndex, atomic.LoadUint64(&n.raftCertifiedIndex), "canonical pending state permits a bounded cut")
	}
	readValue := func() string {
		list, err := posting.GetNoStore(x.DataKey(predicate, 1), math.MaxUint64)
		require.NoError(t, err)
		value, err := list.Value(math.MaxUint64)
		require.NoError(t, err)
		return string(value.Value.([]byte))
	}
	require.Equal(t, "suffix", readValue())
	// Close deliberately flushes working suffix data. Recovery must still select
	// the certified root, never the latest accidentally durable working root.
	if cut == "failed_native_snapshot_restart" {
		// A stale native request cannot advance its accepted-but-unapplied
		// Ready. Preserve term/vote and the drained current application state,
		// restart at that same cut with its exact applied membership.
		before, err := wal.Snapshot()
		require.NoError(t, err)
		require.NoError(t, n.persistSnapshotElection(raftpb.HardState{Term: 2, Vote: 1, Commit: 999}))
		hs, err := wal.HardState()
		require.NoError(t, err)
		require.Equal(t, raftpb.HardState{Term: 2, Vote: 1, Commit: 6}, hs)
		require.NoError(t, n.restartAfterRaftSnapshotFailure(context.Background()))
		defer n.Raft().Stop()
		after, err := wal.Snapshot()
		require.NoError(t, err)
		require.Equal(t, before, after)
		last, err := wal.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(6), last)
		require.Equal(t, uint64(6), n.Cfg.Applied)
		require.Equal(t, 1, posting.Oracle().NumPendingTxns(), "runtime restart keeps the valid pending semantic state")
		require.Contains(t, n.appliedProposals, uint64(3))
		require.Equal(t, "suffix", readValue(), "failed preparation must never rewind admitted reads")
		// A later process restart still reconstructs from certificate+WAL only.
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		current.closePostingStore()
		n = open()
	} else {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		current.closePostingStore()
		n = open()
	}
	require.Equal(t, certifiedIndex, n.Cfg.Applied)
	if pendingCertificate {
		require.Equal(t, "suffix", readValue(), "certified pending prewrite remains invisible before its decision")
		require.Equal(t, 1, posting.Oracle().NumPendingTxns(), "pending state restored without its pruned input log")
	} else {
		require.Equal(t, "certified", readValue(), "uncertified working suffix must be discarded")
	}
	oldSignatures := len(n.appliedProposals)
	require.NoError(t, n.expireRaftProposals(time.Now().Add(time.Hour), 10*time.Minute))
	require.Len(t, n.appliedProposals, oldSignatures, "clock expiry must wait through initial committed replay")
	suffix, err := wal.Entries(certifiedIndex+1, 7, math.MaxUint64)
	require.NoError(t, err)
	for _, e := range suffix {
		applyCheckpointEntry(t, n, e)
	}
	require.Equal(t, "suffix", readValue())
	require.Equal(t, 1, posting.Oracle().NumPendingTxns(), "prewrite reconstructed from retained Raft")
	oldSignatures = len(n.appliedProposals)
	require.NoError(t, n.expireRaftProposals(time.Now().Add(time.Hour), 10*time.Minute))
	require.Len(t, n.appliedProposals, oldSignatures, "pending prewrites postpone dedup expiration")
	commitPending := uint64(0)
	expectedFinal := "suffix"
	if cut == "pending_commit_certificate" {
		commitPending, expectedFinal = base+5, "pending"
	}
	abort := checkpointEntry(t, 7, decision(base+4, commitPending, base+5))
	abortState, err := wal.HardState()
	require.NoError(t, err)
	abortState.Commit = 7
	require.NoError(t, wal.Save(&abortState, []raftpb.Entry{abort}, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(abort.Data)))
	applyCheckpointEntry(t, n, abort)
	// Metadata overflow must retain the last certificate and all replay inputs.
	for key := uint64(100); key < 30000; key++ {
		n.appliedProposals[key] = &appliedProposal{size: 128, seen: time.Now()}
	}
	require.ErrorContains(t, n.checkpointRaftCut(7, true), "1MiB")
	require.Equal(t, certifiedIndex, atomic.LoadUint64(&n.raftCertifiedIndex))
	first, err = wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, certifiedIndex+1, first)
	for key := range n.appliedProposals {
		if key >= 100 {
			delete(n.appliedProposals, key)
		}
	}
	require.NoError(t, n.checkpointRaftCut(7, true))
	require.Zero(t, posting.Oracle().NumPendingTxns())
	require.Equal(t, uint64(7), atomic.LoadUint64(&n.raftCertifiedIndex))
	require.Equal(t, expectedFinal, readValue())
	// Wall clocks cannot retire replicated outcomes, even after a pending-free cut.
	count := len(n.appliedProposals)
	require.NoError(t, n.expireRaftProposals(time.Now().Add(time.Hour), 10*time.Minute))
	require.Len(t, n.appliedProposals, count)
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	n = open()
	require.Equal(t, uint64(7), n.Cfg.Applied)
	require.Len(t, n.appliedProposals, count, "clock time must not change replicated dispositions")
	require.Equal(t, expectedFinal, readValue())
}

func TestRaftCheckpointValidationAndDebtFailClosed(t *testing.T) {
	wal := raftwal.Init(t.TempDir())
	defer wal.Close()
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 3}, []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}, &raftpb.Snapshot{}))
	c := raftCheckpointMetadata{Version: raftCheckpointVersion, PendingOverlay: posting.EmptyRaftPendingManifest(), Group: 1, RaftID: 1, Index: 2, Term: 1}
	require.NoError(t, validateRaftCheckpoint(c, wal, 1, 1))
	require.Error(t, validateRaftCheckpoint(c, wal, 2, 1))
	c.Term = 2
	require.Error(t, validateRaftCheckpoint(c, wal, 1, 1))
	c.Term = 1
	c.PendingTransactions = 1
	require.Error(t, validateRaftCheckpoint(c, wal, 1, 1))
	c.PendingTransactions = 0
	require.NoError(t, wal.CreateSnapshot(3, &raftpb.ConfState{}, nil))
	require.Error(t, validateRaftCheckpoint(c, wal, 1, 1), "compacted required replay is fatal")
	n := &node{Node: newNode(wal, 1, 1, "").Node}
	release, err := n.reserveRaftRecoveryDebt(raftRecoveryMaxBytes)
	require.NoError(t, err)
	_, err = n.reserveRaftRecoveryDebt(1)
	require.Error(t, err)
	release()
	release, err = n.reserveRaftRecoveryDebt(1)
	require.NoError(t, err)
	release()
	for _, p := range []*pb.Proposal{{Restore: &pb.RestoreRequest{}}, {Mutations: &pb.Mutations{DropOp: pb.Mutations_ALL}}, {CleanPredicate: "name"}} {
		require.Error(t, checkRaftPrototypeProposal(p))
	}
}
