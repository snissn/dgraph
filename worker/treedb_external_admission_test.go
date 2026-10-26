/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// Only the producer boundary and callback delivery are controlled. Successful
// chunks use the actual external owner and native MVCC publication. The forced
// two-entry prefix keeps semantic failure schedules small; the posting package
// separately proves default production shaping at 100000 entries.
type externalAdmissionStore struct {
	posting.Store
	commits       atomic.Int32
	failAt        int32
	failure       error
	held, release chan struct{}
	terminalLater bool
	injected      atomic.Bool
	callbacks     atomic.Int32
}

func (s *externalAdmissionStore) NewWriteTxn() posting.WriteTxn {
	return &externalAdmissionTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *externalAdmissionStore) SealCommitWindow() {
	if sealer, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		sealer.SealCommitWindow()
	}
}

type externalAdmissionTxn struct {
	posting.WriteTxn
	owner *externalAdmissionStore
	count int
}

func (t *externalAdmissionTxn) SetEntry(e posting.Entry) error {
	if t.count == 2 {
		return badger.ErrTxnTooBig
	}
	if t.owner.terminalLater && t.owner.commits.Load() > 0 && t.owner.injected.CompareAndSwap(false, true) {
		// This actual native producer rejection occurs after a real submitted prefix.
		// Retained Raft input is unchanged; replay removes the injected producer fault.
		e.Value = make([]byte, 8<<20)
	}
	if err := t.WriteTxn.SetEntry(e); err != nil {
		return err
	}
	t.count++
	return nil
}
func (t *externalAdmissionTxn) CommitAt(ts uint64, cb func(error)) error {
	nth := t.owner.commits.Add(1)
	if nth == t.owner.failAt {
		t.owner.callbacks.Add(1)
		cb(t.owner.failure)
		return nil
	}
	return t.WriteTxn.CommitAt(ts, func(err error) {
		if nth == 2 && t.owner.held != nil {
			close(t.owner.held)
			<-t.owner.release
		}
		t.owner.callbacks.Add(1)
		cb(err)
	})
}

func TestPostingProducerRetryClassification(t *testing.T) {
	old := x.Config.MaxRetries
	x.Config.MaxRetries = 3
	defer func() { x.Config.MaxRetries = old }()
	terminal := fmt.Errorf("later: %w", posting.ErrTreeDBEntryTooLarge)
	calls := 0
	require.ErrorIs(t, retryPostingProducer(func() error { calls++; return terminal }), terminal)
	require.Equal(t, 1, calls, "terminal producer error must not restart accepted prefixes")
	calls = 0
	transient := errors.New("unchanged unrelated retry")
	require.NoError(t, retryPostingProducer(func() error {
		calls++
		if calls < 3 {
			return transient
		}
		return nil
	}))
	require.Equal(t, 3, calls)
	calls = 0
	require.NoError(t, retryPostingProducer(func() error { calls++; return badger.ErrBannedKey }))
	require.Equal(t, 1, calls)
}

func TestRaftCheckpointChunkDecisionBoundaryAndReplay(t *testing.T) {
	for _, mode := range []string{"held_success", "middle_failure", "final_failure"} {
		t.Run(mode, func(t *testing.T) { testExternalAdmissionDecision(t, mode) })
	}
}
func TestRaftCheckpointTerminalProducerDoesNotRetryPrefix(t *testing.T) {
	testExternalAdmissionDecision(t, "terminal_later")
}

func testExternalAdmissionDecision(t *testing.T, mode string) {
	oldConfig, oldState, oldPstore, oldStore, oldGroup := Config, State, pstore, postingStore, gr
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	attr := x.AttrInRootNamespace("external-chunks")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}}, blockDeletes: new(sync.Mutex), closer: oldGroup.closer}
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var current *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if current != nil {
			current.closePostingStore()
		}
		Config, State, pstore, postingStore, gr = oldConfig, oldState, oldPstore, oldStore, oldGroup
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		posting.Init(oldPstore, 0, false)
		schema.Init(oldPstore)
	})
	dir := t.TempDir()
	Config = Options{PostingDir: dir + "/p", WALDir: dir + "/w", PostingStoreBackend: PostingStoreBackendTreeDB, PostingStoreTier: PostingStoreTierBenchmarkMinimal, PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint, PostingStoreEvents: true}
	x.WorkerConfig.HardSync, x.Config.MaxRetries = true, 3
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
		require.NoError(t, n.recoverRaftCheckpoint())
		n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
		return n
	}
	n := open()
	// Reuse processApplyCh's real semantic exclusion and success-only frontier.
	apply := func(e raftpb.Entry) error {
		n.raftSemanticMu.Lock()
		defer n.raftSemanticMu.Unlock()
		var p pb.Proposal
		if err := proto.Unmarshal(e.Data[8:], &p); err != nil {
			return err
		}
		p.Index = e.Index
		updateStartTs(&p)
		if err := n.applyTrackedProposal(&p, binary.BigEndian.Uint64(e.Data[:8]), e.Size()); err != nil {
			return err
		}
		n.raftAppliedBytes += uint64(len(e.Data))
		n.raftSemanticIndex = e.Index
		n.Applied.SetDoneUntil(e.Index)
		return nil
	}
	base := posting.Oracle().MaxAssigned() + 100
	mutation := func(ts uint64, value string) *pb.Proposal {
		m := &pb.Mutations{StartTs: ts}
		for uid := uint64(1); uid <= 6; uid++ {
			m.Edges = append(m.Edges, &pb.DirectedEdge{Entity: uid, Attr: attr, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
		}
		return &pb.Proposal{Mutations: m}
	}
	decision := func(start, commit, max uint64) *pb.Proposal {
		return &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: max, Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}}}
	}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}}),
		checkpointEntry(t, 2, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}}),
		checkpointEntry(t, 3, mutation(base+2, "before")), checkpointEntry(t, 4, decision(base+2, base+3, base+4)),
		checkpointEntry(t, 5, mutation(base+5, "after")), checkpointEntry(t, 6, decision(base+5, base+6, base+7)),
	}
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 6}, entries, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range entries {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
	}
	for _, e := range entries[:4] {
		require.NoError(t, apply(e))
	}
	n.raftSemanticMu.Lock()
	err := n.checkpointRaftCut(4, true)
	n.raftSemanticMu.Unlock()
	require.NoError(t, err)
	cert, ok := current.TreeDBStore.RaftCheckpointCertificate()
	require.True(t, ok)
	require.NoError(t, apply(entries[4]))
	pending := posting.Oracle().GetTxn(base + 5)
	require.NotNil(t, pending)
	beforeManifest, err := current.TreeDBStore.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), beforeManifest.Count)
	readValue := func(ts, uid uint64) string {
		list, err := posting.MemLayerInstance.ReadData(x.DataKey(attr, uid), current.PostingStore, ts, false)
		require.NoError(t, err)
		value, err := list.Value(ts)
		require.NoError(t, err)
		return string(value.Value.([]byte))
	}
	for uid := uint64(1); uid <= 6; uid++ {
		require.Equal(t, "before", readValue(base+3, uid))
	}
	pending.UpdateCachedKeys(0)
	failure := errors.New("external later chunk callback failure")
	wrapped := &externalAdmissionStore{Store: postingStore, failure: failure}
	switch mode {
	case "held_success":
		wrapped.held = make(chan struct{})
		wrapped.release = make(chan struct{})
	case "middle_failure":
		wrapped.failAt = 2
	case "final_failure":
		wrapped.failAt = 3
	case "terminal_later":
		wrapped.terminalLater = true
	}
	postingStore = wrapped
	verifyBlocked := func() {
		require.Same(t, pending, posting.Oracle().GetTxn(base+5))
		require.NotNil(t, posting.Oracle().CacheAt(base+5))
		require.Equal(t, base+4, posting.Oracle().MaxAssigned())
		require.Equal(t, base+5, posting.Oracle().MinPendingStartTs())
		require.Equal(t, uint64(5), n.raftSemanticIndex)
		require.Equal(t, uint64(5), n.Applied.DoneUntil())
		require.Equal(t, uint64(4), atomic.LoadUint64(&n.raftCertifiedIndex))
		require.Equal(t, uint64(4), wal.Uint(raftwal.CheckpointIndex))
		actual, ok := current.TreeDBStore.RaftCheckpointCertificate()
		require.True(t, ok)
		require.Equal(t, cert.Generation, actual.Generation)
		require.Equal(t, cert.Digest, actual.Digest)
		manifest, err := current.TreeDBStore.RaftPendingManifest()
		require.NoError(t, err)
		require.Equal(t, beforeManifest, manifest)
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, posting.Oracle().WaitForTs(cancelled, base+6), context.Canceled)
		for uid := uint64(1); uid <= 6; uid++ {
			require.Equal(t, "before", readValue(base+3, uid))
		}
	}
	if mode == "held_success" {
		done := make(chan error, 1)
		var releaseOnce sync.Once
		releaseHeld := func() { releaseOnce.Do(func() { close(wrapped.release) }) }
		t.Cleanup(releaseHeld)
		go func() { done <- apply(entries[5]) }()
		select {
		case <-wrapped.held:
		case <-time.After(5 * time.Second):
			t.Fatal("later real publication did not reach held callback")
		}
		// A raw read bypasses the Oracle solely to prove storage really progressed.
		raw := current.PostingStore.NewReadTxn(base + 6)
		published := 0
		for uid := uint64(1); uid <= 6; uid++ {
			item, err := raw.Get(x.DataKey(attr, uid))
			require.NoError(t, err)
			if item.Version() == base+6 {
				published++
			}
		}
		raw.Discard()
		require.GreaterOrEqual(t, published, 2)
		if n.raftSemanticMu.TryLock() {
			n.raftSemanticMu.Unlock()
			t.Fatal("checkpoint semantic gate passed held decision")
		}
		verifyBlocked()
		select {
		case err := <-done:
			t.Fatalf("decision completed before later callback: %v", err)
		default:
		}
		releaseHeld()
		require.NoError(t, <-done)
	} else {
		err := apply(entries[5])
		if mode == "terminal_later" {
			require.ErrorIs(t, err, posting.ErrTreeDBEntryTooLarge)
			require.Equal(t, int32(1), wrapped.commits.Load(), "terminal later entry must not restart its accepted prefix")
		} else {
			require.ErrorIs(t, err, failure)
			require.Equal(t, int32(3), wrapped.commits.Load())
		}
		require.Equal(t, wrapped.commits.Load(), wrapped.callbacks.Load(), "return must join every accepted callback")
		verifyBlocked()
		raw := current.PostingStore.NewReadTxn(base + 6)
		published := 0
		for uid := uint64(1); uid <= 6; uid++ {
			item, err := raw.Get(x.DataKey(attr, uid))
			require.NoError(t, err)
			if item.Version() == base+6 {
				published++
			}
		}
		raw.Discard()
		require.GreaterOrEqual(t, published, 2)
		// Controlled reopen selects the old real certificate even though Close can
		// flush uncertified physical prefixes. Replay uses retained original records.
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		current.closePostingStore()
		n = open()
		require.Equal(t, uint64(4), n.Cfg.Applied)
		require.Equal(t, base+4, posting.Oracle().MaxAssigned())
		for uid := uint64(1); uid <= 6; uid++ {
			require.Equal(t, "before", readValue(base+3, uid))
		}
		retained, err := wal.Entries(5, 7, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, retained, 2)
		wrapped = &externalAdmissionStore{Store: postingStore}
		postingStore = wrapped
		for _, e := range retained {
			require.NoError(t, apply(e))
		}
		require.Equal(t, int32(3), wrapped.commits.Load())
		before := wrapped.commits.Load()
		require.NoError(t, apply(entries[5]))
		require.Equal(t, before, wrapped.commits.Load(), "semantic replay deduplicates original decision")
	}
	require.Nil(t, posting.Oracle().GetTxn(base+5))
	require.Nil(t, posting.Oracle().CacheAt(base+5))
	require.Equal(t, base+7, posting.Oracle().MaxAssigned())
	require.Equal(t, uint64(6), n.raftSemanticIndex)
	for uid := uint64(1); uid <= 6; uid++ {
		require.Equal(t, "before", readValue(base+3, uid))
		require.Equal(t, "after", readValue(base+6, uid))
	}
	n.raftSemanticMu.Lock()
	err = n.checkpointRaftCut(6, true)
	n.raftSemanticMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, uint64(6), atomic.LoadUint64(&n.raftCertifiedIndex))
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	current.closePostingStore()
	n = open()
	require.Equal(t, uint64(6), n.Cfg.Applied)
	for uid := uint64(1); uid <= 6; uid++ {
		require.Equal(t, "before", readValue(base+3, uid))
		require.Equal(t, "after", readValue(base+6, uid))
	}
}
