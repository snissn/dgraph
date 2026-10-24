/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// chunkRecoveryStore exercises TxnWriter's existing pre-commit chunk boundary.
// The first chunk uses real durable TreeDB storage; subsequent chunks report a
// callback error before touching storage. No accepted engine command is split,
// retried or reclassified by this wrapper.
type chunkRecoveryStore struct {
	posting.Store
	mu       sync.Mutex
	commits  int
	readTxns int
	fail     error
}

func (s *chunkRecoveryStore) NewReadTxn(ts uint64) posting.ReadTxn {
	s.mu.Lock()
	s.readTxns++
	s.mu.Unlock()
	return s.Store.NewReadTxn(ts)
}

func (s *chunkRecoveryStore) NewWriteTxn() posting.WriteTxn {
	return &chunkRecoveryTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}

func (s *chunkRecoveryStore) SealCommitWindow() {
	if sealer, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		sealer.SealCommitWindow()
	}
}

type chunkRecoveryTxn struct {
	posting.WriteTxn
	owner *chunkRecoveryStore
	count int
}

func (txn *chunkRecoveryTxn) SetEntry(entry posting.Entry) error {
	if txn.count == 2 {
		return badger.ErrTxnTooBig // refuse the next entry without mutating it
	}
	if err := txn.WriteTxn.SetEntry(entry); err != nil {
		return err
	}
	txn.count++
	return nil
}

func (txn *chunkRecoveryTxn) CommitAt(ts uint64, cb func(error)) error {
	txn.owner.mu.Lock()
	txn.owner.commits++
	nth, failure := txn.owner.commits, txn.owner.fail
	txn.owner.mu.Unlock()
	if nth > 1 && failure != nil {
		cb(failure)
		return nil
	}
	return txn.WriteTxn.CommitAt(ts, cb)
}

// The ordinary store_wal path may advance the Raft attempted-apply marker after
// a failed Flush. Visibility and retention instead depend on the unresolved
// Oracle transaction. This test crosses the real mutation, chunk, durable
// callback, progress, snapshot and retained-log replay boundaries.
func TestTreeDBChunkFailureRetainsOracleAndRaftReplay(t *testing.T) {
	oldConfig, oldState, oldBadger, oldStore, oldGroup := Config, State, pstore, postingStore, gr
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	attr := x.AttrInRootNamespace("chunk-recovery")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}},
		blockDeletes: new(sync.Mutex), closer: oldGroup.closer}
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var current *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if current != nil {
			current.closePostingStore()
		}
		Config, State, pstore, postingStore, gr = oldConfig, oldState, oldBadger, oldStore, oldGroup
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	Config = snapshotInstallOptions(t.TempDir())
	Config.PostingStoreDurability = PostingStoreDurabilityDurable
	x.WorkerConfig.HardSync, x.Config.MaxRetries = false, 3
	require.False(t, raftCheckpointEnabled())
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	wal := raftwal.Init(Config.WALDir)
	defer func() { require.NoError(t, wal.Close()) }()
	open := func() *node {
		current = &ServerState{}
		require.NoError(t, current.openPostingStore())
		State, pstore, postingStore = *current, nil, current.PostingStore
		posting.InitForStore(postingStore, 1<<20, false)
		schema.InitForStore(schemaPostingStore{store: postingStore})
		require.NoError(t, schema.LoadFromDb(context.Background()))
		n := newNode(wal, 1, 1, "")
		gr.Node = n
		n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
		return n
	}
	n := open()
	applyEntry := func(entry raftpb.Entry) error {
		var input pb.Proposal
		require.NoError(t, proto.Unmarshal(entry.Data[8:], &input))
		input.Index = entry.Index
		updateStartTs(&input)
		err := n.applyTrackedProposal(&input, binary.BigEndian.Uint64(entry.Data[:8]), entry.Size())
		// This deliberately models processApplyCh's ordinary-path attempted
		// progress even on error, rather than inventing a stronger invariant.
		n.Applied.SetDoneUntil(entry.Index)
		return err
	}
	appendApply := func(index uint64, proposal *pb.Proposal) error {
		entry := checkpointEntry(t, index, proposal)
		require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: index}, []raftpb.Entry{entry}, &raftpb.Snapshot{}))
		require.NoError(t, wal.Sync())
		return applyEntry(entry)
	}
	base := posting.Oracle().MaxAssigned() + 100
	mutation := func(ts uint64, value string) *pb.Proposal {
		m := &pb.Mutations{StartTs: ts}
		for uid := uint64(1); uid <= 6; uid++ {
			m.Edges = append(m.Edges, &pb.DirectedEdge{Entity: uid, Attr: attr,
				Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
		}
		return &pb.Proposal{Mutations: m}
	}
	decision := func(start, commit, max uint64) *pb.Proposal {
		return &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: max,
			Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}}}
	}
	require.NoError(t, appendApply(1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}}))
	require.NoError(t, appendApply(2, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1,
		Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}}))
	require.NoError(t, appendApply(3, mutation(base+2, "before")))
	require.NoError(t, appendApply(4, decision(base+2, base+3, base+4)))
	require.NoError(t, appendApply(5, mutation(base+5, "after")))
	pending := posting.Oracle().GetTxn(base + 5)
	require.NotNil(t, pending)

	// Seed every shared cache entry before failure. UpdateCachedKeys(0) is an
	// existing no-op publication which also waits for cache admission, allowing
	// the next reads to prove these are actual cache hits without a sleep.
	for uid := uint64(1); uid <= 6; uid++ {
		_, err := posting.MemLayerInstance.ReadData(x.DataKey(attr, uid), postingStore, base+3, false)
		require.NoError(t, err)
	}
	pending.UpdateCachedKeys(0)
	failure := errors.New("injected later chunk callback failure")
	wrapped := &chunkRecoveryStore{Store: postingStore, fail: failure}
	for uid := uint64(1); uid <= 6; uid++ {
		_, err := posting.MemLayerInstance.ReadData(x.DataKey(attr, uid), wrapped, base+3, false)
		require.NoError(t, err)
	}
	require.Zero(t, wrapped.readTxns, "the cache-publication control must actually hit the cache")
	postingStore = wrapped
	err := appendApply(6, decision(base+5, base+6, base+7))
	require.ErrorIs(t, err, failure)
	require.Equal(t, 3, wrapped.commits)
	require.Equal(t, uint64(6), n.Applied.DoneUntil())
	require.ErrorIs(t, n.appliedProposals[6].err, failure, "failed proposal is not successful dedup authority")
	require.Same(t, pending, posting.Oracle().GetTxn(base+5))
	require.NotNil(t, posting.Oracle().CacheAt(base+5))
	require.Equal(t, base+5, posting.Oracle().MinPendingStartTs())
	require.Equal(t, base+4, posting.Oracle().MaxAssigned())
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, posting.Oracle().WaitForTs(cancelled, base+6), context.Canceled,
		"a read at the failed commit must remain behind the Oracle fence")

	// Probe below the Oracle only to characterize the physical partial write.
	// The cache must agree with those physical versions, rather than publishing
	// all six pending deltas after only the first durable chunk succeeded.
	read := wrapped.Store.NewReadTxn(base + 6)
	newCount := 0
	for uid := uint64(1); uid <= 6; uid++ {
		key := x.DataKey(attr, uid)
		item, err := read.Get(key)
		require.NoError(t, err)
		want := "before"
		if item.Version() == base+6 {
			want = "after"
			newCount++
		} else {
			require.Equal(t, base+3, item.Version())
		}
		list, err := posting.MemLayerInstance.ReadData(key, wrapped.Store, base+6, false)
		require.NoError(t, err)
		value, err := list.Value(base + 6)
		require.NoError(t, err)
		require.Equal(t, []byte(want), value.Value, "uid %d prematurely published in cache", uid)
	}
	read.Discard()
	require.Equal(t, 2, newCount, "one complete chunk must actually reach durable TreeDB storage")

	snap, err := n.calculateSnapshot(0, n.Applied.DoneUntil(), posting.Oracle().MinPendingStartTs())
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, uint64(4), snap.Index, "retain both mutation 5 and failed decision 6")
	require.NoError(t, n.updateRaftProgress())
	require.Equal(t, uint64(4), wal.Uint(raftwal.CheckpointIndex))
	require.NoError(t, n.applyCommitted(&pb.Proposal{Snapshot: snap}, 0))
	require.NoError(t, wal.Sync())
	retained, err := wal.Entries(5, 7, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, retained, 2)
	require.Equal(t, uint64(5), retained[0].Index)
	require.Equal(t, uint64(6), retained[1].Index)

	// Ordinary store_wal reopen and replay use the retained mutation/decision,
	// without the separate raft_checkpoint recovery route or manual Txn caches.
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	require.NoError(t, wal.Close())
	wal = raftwal.Init(Config.WALDir)
	retained, err = wal.Entries(5, 7, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, retained, 2, "both recovery records must survive reopening the actual Raft WAL")
	n = open()
	require.Equal(t, uint64(4), n.Cfg.Applied)
	wrapped = &chunkRecoveryStore{Store: postingStore}
	postingStore = wrapped
	for _, entry := range retained {
		require.NoError(t, applyEntry(entry))
	}
	require.Nil(t, posting.Oracle().GetTxn(base+5))
	require.Equal(t, base+7, posting.Oracle().MaxAssigned())
	require.NoError(t, posting.Oracle().WaitForTs(cancelled, base+6))
	require.GreaterOrEqual(t, wrapped.commits, 2, "replay must still honor bounded producer chunks")
	verify := func() {
		for uid := uint64(1); uid <= 6; uid++ {
			for _, point := range []struct {
				ts    uint64
				value string
			}{{base + 3, "before"}, {base + 6, "after"}} {
				list, err := posting.GetNoStore(x.DataKey(attr, uid), point.ts)
				require.NoError(t, err)
				value, err := list.Value(point.ts)
				require.NoError(t, err)
				require.Equal(t, []byte(point.value), value.Value, "uid %d at %d", uid, point.ts)
			}
		}
	}
	verify()
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	n = open()
	verify()
}
