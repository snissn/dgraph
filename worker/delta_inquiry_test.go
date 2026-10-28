/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type inquiryTestStore struct {
	posting.Store
	callback func(error)
	sealed   chan struct{}
	failure  error
}

func (s *inquiryTestStore) NewWriteTxn() posting.WriteTxn { return &inquiryTestTxn{owner: s} }
func (s *inquiryTestStore) SealCommitWindow()             { close(s.sealed) }

type inquiryTestTxn struct {
	posting.WriteTxn
	owner *inquiryTestStore
}

func (t *inquiryTestTxn) SetEntry(posting.Entry) error { return nil }
func (t *inquiryTestTxn) Discard()                     {}
func (t *inquiryTestTxn) CommitAt(_ uint64, cb func(error)) error {
	t.owner.callback = cb
	return t.owner.failure
}

// The inquiry wraps the actual TxnWriter contract: admission return is not a
// callback, sealing does not complete Flush, and callback errors survive intact.
func TestDeltaInquiryWriterBoundaries(t *testing.T) {
	var mu sync.Mutex
	var events []inquiryEvent
	d := &deltaInquiry{emit: func(e inquiryEvent) { mu.Lock(); defer mu.Unlock(); events = append(events, e) }}
	base := &inquiryTestStore{sealed: make(chan struct{})}
	s := &inquiryStore{Store: base, trace: d, index: 7, key: 9}
	w := posting.NewTxnWriterForStore(s)
	require.NoError(t, w.SetAt([]byte("k"), []byte("v"), 0, 11))
	require.Equal(t, "submit_return", events[len(events)-1].Phase)
	done := make(chan error, 1)
	go func() { done <- w.Flush() }()
	<-base.sealed
	select {
	case err := <-done:
		t.Fatalf("seal completed callback wait: %v", err)
	default:
	}
	failure := errors.New("callback failure")
	base.callback(failure)
	require.ErrorIs(t, <-done, failure)

	mu.Lock()
	defer mu.Unlock()
	phases := make([]string, len(events))
	for i, e := range events {
		phases[i] = e.Phase
		require.Equal(t, uint64(7), e.Index)
	}
	require.Contains(t, phases, "window_seal")
	require.Contains(t, phases, "callback")
	require.Equal(t, "callback failure", events[len(events)-1].Error)
}

func TestDeltaInquiryDisabledAndEntryKinds(t *testing.T) {
	t.Setenv("DGRAPH_DELTA_INQUIRY", "")
	require.Nil(t, newDeltaInquiry())
	var events []inquiryEvent
	d := &deltaInquiry{emit: func(e inquiryEvent) { events = append(events, e) }}
	a := checkpointEntry(t, 1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: 30, Txns: []*pb.TxnStatus{{StartTs: 10, CommitTs: 20}}}})
	b := checkpointEntry(t, 2, &pb.Proposal{Mutations: &pb.Mutations{StartTs: 11}})
	d.entries([]raftpb.Entry{a, b, {Index: 3, Type: raftpb.EntryConfChange}}, "enqueue_begin")
	require.Equal(t, []string{"delta", "mutation", "control"}, []string{events[0].Kind, events[1].Kind, events[2].Kind})
	require.Equal(t, uint64(10), events[0].Txns[0].StartTs)
	require.Equal(t, uint64(20), events[0].Txns[0].CommitTs)
	for i, e := range events {
		require.Equal(t, uint64(i+1), e.Sequence)
		require.NotEmpty(t, e.At)
	}
}

// Hold an actual native callback while a second Delta is queued. This inquiry
// characterizes the current serial production path; it enables no overlap.
func TestDeltaInquiryProductionSerialBoundary(t *testing.T) {
	oldConfig, oldState, oldBadger, oldStore, oldGroup := Config, State, pstore, postingStore, gr
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	attr := x.AttrInRootNamespace("delta-inquiry")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}}, blockDeletes: new(sync.Mutex), closer: oldGroup.closer}
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var state ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		state.closePostingStore()
		Config, State, pstore, postingStore, gr = oldConfig, oldState, oldBadger, oldStore, oldGroup
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	Config = snapshotInstallOptions(t.TempDir())
	Config.PostingStoreDurability = PostingStoreDurabilityDurable
	x.WorkerConfig.HardSync, x.Config.MaxRetries = false, 3
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	wal := raftwal.Init(Config.WALDir)
	defer func() { require.NoError(t, wal.Close()) }()
	require.NoError(t, state.openPostingStore())
	State, pstore, postingStore = state, nil, state.PostingStore
	posting.InitForStore(postingStore, 1<<20, false)
	schema.InitForStore(schemaPostingStore{store: postingStore})
	require.NoError(t, schema.LoadFromDb(context.Background()))
	n := newNode(wal, 1, 1, "")
	gr.Node = n
	base := posting.Oracle().MaxAssigned() + 100
	apply := func(p *pb.Proposal) { require.NoError(t, n.applyCommitted(p, 0)) }
	apply(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}})
	apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}})
	for i := uint64(2); i <= 3; i++ {
		apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + i, Edges: []*pb.DirectedEdge{{Entity: i, Attr: attr, Value: []byte("value"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}})
	}
	var mu sync.Mutex
	var events []inquiryEvent
	n.deltaInquiry = &deltaInquiry{emit: func(e inquiryEvent) { mu.Lock(); defer mu.Unlock(); events = append(events, e) }}
	n.deltaInquiry.event(inquiryEvent{Phase: "enabled", Detail: "deterministic held-callback fixture; not natural arrivals"})
	wrapped := &inquiryHeldStore{Store: postingStore, held: make(chan struct{}), release: make(chan struct{})}
	postingStore = wrapped
	a := checkpointEntry(t, 1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 4, Txns: []*pb.TxnStatus{{StartTs: base + 2, CommitTs: base + 4}}}})
	b := checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5, Txns: []*pb.TxnStatus{{StartTs: base + 3, CommitTs: base + 5}}}})
	n.closer = z.NewCloser(1)
	n.Applied.Begin(1)
	n.Applied.Begin(2)
	atomic.AddInt64(&n.pendingSize, int64(a.Size()+b.Size()))
	done := make(chan struct{})
	go func() { n.processApplyCh(); close(done) }()
	var finish sync.Once
	shutdown := func() { finish.Do(func() { close(n.applyCh); close(wrapped.release); <-done }) }
	defer shutdown()
	n.enqueueApply([]raftpb.Entry{a})
	<-wrapped.held
	n.enqueueApply([]raftpb.Entry{b})
	require.Equal(t, base, posting.Oracle().MaxAssigned())
	require.Zero(t, n.Applied.DoneUntil())
	mu.Lock()
	for _, e := range events {
		require.False(t, e.Index == 2 && e.Phase == "submit_begin")
		require.NotEqual(t, "oracle_end", e.Phase)
	}
	mu.Unlock()
	shutdown()
	require.Equal(t, base+5, posting.Oracle().MaxAssigned())
	mu.Lock()
	defer mu.Unlock()
	positions := map[string]int{}
	for i, e := range events {
		positions[fmt.Sprintf("%d/%s", e.Index, e.Phase)] = i
		if e.Phase == "delta_begin" {
			require.Equal(t, "ordinary_store_wal_candidate", e.Kind)
		}
	}
	for _, key := range []string{"2/enqueue_end", "1/flush_end", "1/callback", "1/oracle_end", "1/applied_done", "2/submit_begin"} {
		require.Contains(t, positions, key)
	}
	require.Less(t, positions["2/enqueue_end"], positions["1/flush_end"])
	require.Less(t, positions["1/callback"], positions["1/flush_end"])
	require.Less(t, positions["1/flush_end"], positions["1/oracle_end"])
	require.Less(t, positions["1/oracle_end"], positions["1/applied_done"])
	require.Less(t, positions["1/applied_done"], positions["2/submit_begin"])
	for _, event := range events {
		data, err := json.Marshal(event)
		require.NoError(t, err)
		t.Logf("DELTA_INQUIRY %s", data)
	}
}

type inquiryHeldStore struct {
	posting.Store
	held, release chan struct{}
	once          sync.Once
}

func (s *inquiryHeldStore) NewWriteTxn() posting.WriteTxn {
	return &inquiryHeldTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *inquiryHeldStore) SealCommitWindow() {
	if v, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		v.SealCommitWindow()
	}
}

type inquiryHeldTxn struct {
	posting.WriteTxn
	owner *inquiryHeldStore
}

func (t *inquiryHeldTxn) CommitAt(ts uint64, cb func(error)) error {
	return t.WriteTxn.CommitAt(ts, func(err error) { t.owner.once.Do(func() { close(t.owner.held); <-t.owner.release }); cb(err) })
}

// A synchronous producer rejection is not an accepted callback. The diagnostic
// must retain the original return and must never synthesize completion.
func TestDeltaInquiryImmediateSubmissionError(t *testing.T) {
	failure := errors.New("rejected admission")
	base := &inquiryTestStore{failure: failure}
	var events []inquiryEvent
	d := &deltaInquiry{emit: func(e inquiryEvent) { events = append(events, e) }}
	store := &inquiryStore{Store: base, trace: d, index: 3, key: 4}
	txn := store.NewWriteTxn()
	called := false
	err := txn.CommitAt(10, func(error) { called = true })
	require.ErrorIs(t, err, failure)
	require.False(t, called)
	require.Len(t, events, 2)
	require.Equal(t, "submit_begin", events[0].Phase)
	require.Equal(t, "submit_return", events[1].Phase)
	require.Equal(t, "rejected admission", events[1].Error)
}
