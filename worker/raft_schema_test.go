/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/tok"
	"github.com/dgraph-io/dgraph/v25/x"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftScalarSchemaRebuildCertifiedReplay(t *testing.T) {
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
	dir := t.TempDir()
	Config = Options{PostingDir: dir + "/p", WALDir: dir + "/w", PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier: PostingStoreTierBenchmarkMinimal, PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint, PostingStoreEvents: true}
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
	attr := x.AttrInRootNamespace("name")
	secondAttr := x.AttrInRootNamespace("name2")
	edges := make([]*pb.DirectedEdge, 600)
	for i := range edges {
		edges[i] = &pb.DirectedEdge{Entity: uint64(i + 1), Attr: attr, Value: []byte("shared"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}
	}
	blob := x.AttrInRootNamespace("schema-rebuild-blob")
	edges = append(edges, &pb.DirectedEdge{Entity: 1, Attr: blob, Value: []byte(strings.Repeat("old-blob", 1024)), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
	edges = append(edges, &pb.DirectedEdge{Entity: 1, Attr: secondAttr, Value: []byte("shared"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
	indexed := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}
	secondIndexed := proto.Clone(indexed).(*pb.SchemaUpdate)
	secondIndexed.Predicate = secondAttr
	typeName := x.AttrInRootNamespace("RaftSchemaPerson")
	typeUpdate := &pb.TypeUpdate{TypeName: typeName, Fields: []*pb.SchemaUpdate{{Predicate: attr}}}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base, Edges: edges}}),
		checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 1, Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}),
		checkpointEntry(t, 3, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 2, Schema: []*pb.SchemaUpdate{indexed, secondIndexed}, Types: []*pb.TypeUpdate{typeUpdate}}}),
		checkpointEntry(t, 4, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 3, Edges: []*pb.DirectedEdge{{Entity: 1, Attr: attr, Value: []byte("suffix"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}, {Entity: 1, Attr: blob, Value: []byte(strings.Repeat("new-blob", 1024)), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}}),
		checkpointEntry(t, 5, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 4, Txns: []*pb.TxnStatus{{StartTs: base + 3, CommitTs: base + 4}}}}),
	}
	duplicate := entries[2]
	duplicate.Index = 6
	entries = append(entries, duplicate)
	entries = append(entries, checkpointEntry(t, 7, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 5, DropOp: pb.Mutations_TYPE, DropValue: typeName}}))
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 7}, entries, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, e := range entries {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
	}
	applyCheckpointEntry(t, n, entries[0])
	applyCheckpointEntry(t, n, entries[1])
	require.NoError(t, n.checkpointRaftCut(2, true))
	gate := &schemaCallbackGate{Store: postingStore, entered: make(chan struct{}), release: make(chan struct{}), target: secondAttr}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(release)
	posting.Cleanup()
	postingStore = gate
	posting.InitForStore(gate, 0, false)
	done := make(chan struct{})
	go func() { defer close(done); applyCheckpointEntry(t, n, entries[2]) }()
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("index callback did not reach gate")
	}
	current, _ := schema.State().Get(context.Background(), attr)
	require.Empty(t, current.Tokenizer, "query schema must not expose an index with an outstanding write callback")
	secondCurrent, _ := schema.State().Get(context.Background(), secondAttr)
	require.Empty(t, secondCurrent.Tokenizer)
	require.True(t, schema.State().IndexingInProgress())
	select {
	case <-done:
		t.Fatal("ordered schema apply returned before index callbacks drained")
	default:
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("schema apply did not finish after callback release")
	}
	readIndex := func(value string, ts uint64) []uint64 {
		tokenizer, ok := tok.GetTokenizer("exact")
		require.True(t, ok)
		tokens, err := tok.BuildTokens(value, tokenizer)
		require.NoError(t, err)
		list, err := posting.GetNoStore(x.IndexKey(attr, tokens[0]), ts)
		require.NoError(t, err)
		uids, err := list.Uids(posting.ListOptions{ReadTs: ts})
		require.NoError(t, err)
		return uids.Uids
	}
	require.Len(t, readIndex("shared", math.MaxUint64), 600)
	require.False(t, schema.State().IndexingInProgress())
	// Closing writes a newer working root; recovery must still restore cut 2,
	// whose schema has no index, and reconstruct every token from retained input.
	n = reopen()
	current, _ = schema.State().Get(context.Background(), attr)
	require.Empty(t, current.Tokenizer)
	require.Empty(t, readIndex("shared", math.MaxUint64))
	applyCheckpointEntry(t, n, entries[2])
	require.Len(t, readIndex("shared", math.MaxUint64), 600)
	require.NoError(t, n.checkpointRaftCut(3, true))
	n = reopen()
	current, _ = schema.State().Get(context.Background(), attr)
	require.True(t, proto.Equal(indexed, &current))
	require.Len(t, readIndex("shared", math.MaxUint64), 600)
	// A retry already represented in the certificate cannot run the rebuild again.
	applyCheckpointEntry(t, n, entries[3])
	applyCheckpointEntry(t, n, entries[4])
	applyCheckpointEntry(t, n, entries[5])
	require.Len(t, readIndex("shared", math.MaxUint64), 599)
	require.Equal(t, []uint64{1}, readIndex("suffix", math.MaxUint64))
	require.NoError(t, state.RunPostingStoreGC(context.Background()))
	_, err := state.TreeDBStore.CompactStorage(context.Background(), td.CompactStorageOptions{Mode: td.CompactStorageFull, ValueLogRewriteBatchSize: 32, LeafPackMaxPasses: 1, LeafPackMaxBytesToCopyPerPass: 1 << 20})
	require.NoError(t, err)
	_, err = state.TreeDBStore.CompactStorage(context.Background(), td.CompactStorageOptions{UnsafeValueLogReclaimFencedUnreferenced: true})
	require.Error(t, err)
	floor, err := state.TreeDBStore.DiscardFloor()
	require.NoError(t, err)
	require.Zero(t, floor, "physical compaction cannot imply a semantic discard floor")
	n = reopen()
	// Physical maintenance of the newer working root must not replace the
	// semantic authority or lose large values belonging to either retained cut.
	require.Len(t, readIndex("shared", math.MaxUint64), 600)
	require.Empty(t, readIndex("suffix", math.MaxUint64))
	list, err := posting.GetNoStore(x.DataKey(blob, 1), math.MaxUint64)
	require.NoError(t, err)
	value, err := list.Value(math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, []byte(strings.Repeat("old-blob", 1024)), value.Value)
	for _, entry := range entries[3:] {
		applyCheckpointEntry(t, n, entry)
	}
	require.Len(t, readIndex("shared", math.MaxUint64), 599)
	data, ok := state.TreeDBStore.RecoveredRaftCheckpoint()
	require.True(t, ok)
	var cert raftCheckpointMetadata
	require.NoError(t, json.Unmarshal(data, &cert))
	require.Equal(t, uint64(3), cert.Index)
	_, exists := schema.State().GetType(typeName)
	require.False(t, exists, "retained type deletion must be applied after suffix replay")
	n = reopen()
	_, exists = schema.State().GetType(typeName)
	require.True(t, exists, "old certificate restores the timestamped type before replay")
	for _, entry := range entries[3:] {
		applyCheckpointEntry(t, n, entry)
	}
	_, exists = schema.State().GetType(typeName)
	require.False(t, exists)
	require.NoError(t, n.checkpointRaftCut(7, true))
	n = reopen()
	_, exists = schema.State().GetType(typeName)
	require.False(t, exists, "certified type tombstone survives reopen")
}

type schemaCallbackGate struct {
	posting.Store
	target           string
	entered, release chan struct{}
	once             sync.Once
}

func TestRaftSchemaSupportedBoundary(t *testing.T) {
	attr := x.AttrInRootNamespace("raft-schema-boundary")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING}
	schema.State().Set(attr, old)
	defer schema.State().DeleteMutSchema(attr)
	index := proto.Clone(old).(*pb.SchemaUpdate)
	index.Directive, index.Tokenizer = pb.SchemaUpdate_INDEX, []string{"exact"}
	require.NoError(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{Schema: []*pb.SchemaUpdate{index}}}))
	for _, change := range []func(*pb.SchemaUpdate){
		func(s *pb.SchemaUpdate) { s.Lang = true },
		func(s *pb.SchemaUpdate) { s.Unique = true },
		func(s *pb.SchemaUpdate) { s.Upsert = true },
		func(s *pb.SchemaUpdate) { s.Directive = pb.SchemaUpdate_REVERSE },
		func(s *pb.SchemaUpdate) { s.Directive = pb.SchemaUpdate_DELETE },
	} {
		update := proto.Clone(index).(*pb.SchemaUpdate)
		change(update)
		require.Error(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{Schema: []*pb.SchemaUpdate{update}}}))
	}
	for _, change := range []func(*pb.SchemaUpdate){
		func(s *pb.SchemaUpdate) { s.List = true },
		func(s *pb.SchemaUpdate) { s.ValueType = pb.Posting_INT },
		func(s *pb.SchemaUpdate) { s.Count = true },
	} {
		update := proto.Clone(index).(*pb.SchemaUpdate)
		change(update)
		require.NoError(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{Schema: []*pb.SchemaUpdate{update}}}))
	}
	require.Error(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{Schema: []*pb.SchemaUpdate{nil}}}))
	require.Error(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{DropOp: pb.Mutations_ALL}}))
	require.NoError(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{StartTs: 10, DropOp: pb.Mutations_TYPE, DropValue: "0-Type"}}))
	require.Error(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{DropOp: pb.Mutations_TYPE, DropValue: "0-Type"}}))
}

func TestRaftSchemaPendingTransitionIsPureAndNoopAllowed(t *testing.T) {
	posting.Oracle().ResetTxns()
	t.Cleanup(posting.Oracle().ResetTxns)
	attr, other := x.AttrInRootNamespace("pending-schema"), x.AttrInRootNamespace("other-schema")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING}
	schema.State().Set(attr, old)
	typeName := x.AttrInRootNamespace("PendingSchemaPerson")
	typeOld := &pb.TypeUpdate{TypeName: typeName, Fields: []*pb.SchemaUpdate{{Predicate: attr}}}
	schema.State().SetType(typeName, typeOld)
	txn := posting.NewTxn(posting.Oracle().MaxAssigned() + 10)
	list, err := txn.GetFromDelta(x.DataKey(attr, 1))
	require.NoError(t, err)
	require.NoError(t, list.AddMutationWithIndex(context.Background(), &pb.DirectedEdge{
		Attr: attr, Entity: 1, Value: []byte("pending"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET,
	}, txn))
	before, err := posting.MarshalRaftPendingTxn(txn)
	require.NoError(t, err)
	require.NoError(t, posting.InstallRaftPendingTxn(txn))
	indexed := proto.Clone(old).(*pb.SchemaUpdate)
	indexed.Directive, indexed.Tokenizer = pb.SchemaUpdate_INDEX, []string{"exact"}
	changedType := proto.Clone(typeOld).(*pb.TypeUpdate)
	changedType.Fields = append(changedType.Fields, &pb.SchemaUpdate{Predicate: other})
	for _, mutation := range []*pb.Mutations{
		{Schema: []*pb.SchemaUpdate{indexed}},
		{Types: []*pb.TypeUpdate{changedType}},
		{StartTs: txn.StartTs + 1, DropOp: pb.Mutations_TYPE, DropValue: typeName},
	} {
		proposal := &pb.Proposal{Mutations: mutation}
		err := checkRaftSupportedProposal(proposal)
		require.True(t, isRaftSemanticRejection(err), "%v", err)
		require.False(t, raftSchemaMutationIsNoop(mutation))
		after, encodeErr := posting.MarshalRaftPendingTxn(txn)
		require.NoError(t, encodeErr)
		require.Equal(t, before, after, "eligibility cannot abort or rewrite canonical pending state")
		current, exists := schema.State().Get(context.Background(), attr)
		require.True(t, exists)
		require.True(t, proto.Equal(old, &current))
		require.False(t, txn.ShouldAbort())
	}
	for _, mutation := range []*pb.Mutations{
		{Schema: []*pb.SchemaUpdate{proto.Clone(old).(*pb.SchemaUpdate)}},
		{Types: []*pb.TypeUpdate{proto.Clone(typeOld).(*pb.TypeUpdate)}},
		{StartTs: txn.StartTs + 1, DropOp: pb.Mutations_TYPE, DropValue: x.AttrInRootNamespace("absent-type")},
	} {
		require.NoError(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: mutation}))
		require.True(t, raftSchemaMutationIsNoop(mutation))
	}
	// The pending overlay pins its own predicate semantics, not unrelated schema.
	require.NoError(t, checkRaftSupportedProposal(&pb.Proposal{Mutations: &pb.Mutations{
		Schema: []*pb.SchemaUpdate{{Predicate: other, ValueType: pb.Posting_STRING}},
	}}))
	require.False(t, raftSchemaMutationIsNoop(&pb.Mutations{Edges: []*pb.DirectedEdge{{Attr: attr}}}))
}

func (s *schemaCallbackGate) NewWriteTxn() posting.WriteTxn {
	return &schemaGateWriteTxn{WriteTxn: s.Store.NewWriteTxn(), gate: s}
}

type schemaGateWriteTxn struct {
	posting.WriteTxn
	gate  *schemaCallbackGate
	index bool
}

func (t *schemaGateWriteTxn) SetEntry(e posting.Entry) error {
	if pk, err := x.Parse(e.Key); err == nil && pk.IsIndex() && pk.Attr == t.gate.target {
		t.index = true
	}
	return t.WriteTxn.SetEntry(e)
}
func (t *schemaGateWriteTxn) CommitAt(ts uint64, cb func(error)) error {
	if !t.index || cb == nil {
		return t.WriteTxn.CommitAt(ts, cb)
	}
	return t.WriteTxn.CommitAt(ts, func(err error) { t.gate.once.Do(func() { close(t.gate.entered) }); <-t.gate.release; cb(err) })
}
