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
	"github.com/dgraph-io/dgraph/v25/tok"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftSchemaVariantsCertifiedPendingAndRejection(t *testing.T) {
	dir := t.TempDir()
	oldConfig, oldState, oldBadger, oldStore, oldNode := Config, State, pstore, postingStore, gr.Node
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var state *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if state != nil {
			state.closePostingStore()
		}
		Config, State, pstore, postingStore, gr.Node = oldConfig, oldState, oldBadger, oldStore, oldNode
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	Config = snapshotInstallOptions(dir)
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
	n := open()
	reopen := func() { posting.Cleanup(); posting.Oracle().ResetTxns(); state.closePostingStore(); n = open() }
	apply := func(index uint64, p *pb.Proposal) error {
		e := checkpointEntry(t, index, p)
		require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: index}, []raftpb.Entry{e}, &raftpb.Snapshot{}))
		require.NoError(t, wal.Sync())
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
		var input pb.Proposal
		require.NoError(t, proto.Unmarshal(e.Data[8:], &input))
		input.Index = index
		updateStartTs(&input)
		err := n.applyTrackedProposal(&input, binary.BigEndian.Uint64(e.Data[:8]), e.Size())
		n.raftAppliedBytes += uint64(len(e.Data))
		n.raftSemanticIndex = index
		n.Applied.SetDoneUntil(index)
		return err
	}
	certify := func(index uint64) {
		require.NoError(t, n.checkpointRaftCut(index, true))
		reopen()
		require.Equal(t, index, n.Cfg.Applied)
	}
	base := posting.Oracle().MaxAssigned() + 10
	friend, numeric, names, bad := x.AttrInRootNamespace("variant-friend"), x.AttrInRootNamespace("variant-numeric"), x.AttrInRootNamespace("variant-names"), x.AttrInRootNamespace("variant-bad")
	uid := func(subject, target uint64) *pb.DirectedEdge {
		return &pb.DirectedEdge{Entity: subject, Attr: friend, ValueId: target, ValueType: pb.Posting_UID, Op: pb.DirectedEdge_SET}
	}
	scalar := func(attr, value string) *pb.DirectedEdge {
		return &pb.DirectedEdge{Entity: 1, Attr: attr, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}
	}
	require.NoError(t, apply(1, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base, Edges: []*pb.DirectedEdge{uid(1, 99), uid(2, 99), uid(3, 99), uid(4, 99), uid(1, 100), scalar(numeric, "42"), scalar(names, "value"), scalar(bad, "not-an-int")}}}))
	require.NoError(t, apply(2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 10, Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}))
	certify(2)
	friendIndexed := &pb.SchemaUpdate{Predicate: friend, ValueType: pb.Posting_UID, List: true, Directive: pb.SchemaUpdate_REVERSE, Count: true}
	numericIndexed := &pb.SchemaUpdate{Predicate: numeric, ValueType: pb.Posting_INT, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"int"}}
	namesIndexed := &pb.SchemaUpdate{Predicate: names, ValueType: pb.Posting_STRING, List: true, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}
	require.NoError(t, apply(3, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 11, Schema: []*pb.SchemaUpdate{friendIndexed, numericIndexed, namesIndexed}}}))
	read := func(key []byte, ts uint64) []uint64 {
		list, err := posting.GetNoStore(key, ts)
		require.NoError(t, err)
		values, err := list.Uids(posting.ListOptions{ReadTs: ts})
		require.NoError(t, err)
		return values.Uids
	}
	tokenKey := func(attr, tokenizer string, value any) []byte {
		impl, ok := tok.GetTokenizer(tokenizer)
		require.True(t, ok)
		tokens, err := tok.BuildTokens(value, impl)
		require.NoError(t, err)
		require.Len(t, tokens, 1)
		return x.IndexKey(attr, tokens[0])
	}
	verifyBuilt := func() {
		require.Equal(t, []uint64{1, 2, 3, 4}, read(x.ReverseKey(friend, 99), math.MaxUint64))
		require.Equal(t, []uint64{1}, read(x.ReverseKey(friend, 100), math.MaxUint64))
		require.Equal(t, []uint64{2, 3, 4}, read(x.CountKey(friend, 1, false), math.MaxUint64))
		require.Equal(t, []uint64{1}, read(x.CountKey(friend, 2, false), math.MaxUint64))
		require.Equal(t, []uint64{99}, read(x.CountKey(friend, 4, true), math.MaxUint64))
		require.Equal(t, []uint64{100}, read(x.CountKey(friend, 1, true), math.MaxUint64))
		require.Equal(t, []uint64{1}, read(tokenKey(numeric, "int", int64(42)), math.MaxUint64))
		require.Equal(t, []uint64{1}, read(tokenKey(names, "exact", "value"), math.MaxUint64))
		list, err := posting.GetNoStore(x.DataKey(names, 1), math.MaxUint64)
		require.NoError(t, err)
		count := 0
		require.NoError(t, list.Iterate(math.MaxUint64, 0, func(p *pb.Posting) error { count++; require.NotEqual(t, uint64(math.MaxUint64), p.Uid); return nil }))
		require.Equal(t, 1, count)
	}
	verifyBuilt()
	certify(3)
	verifyBuilt()
	require.NoError(t, apply(4, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 12, Edges: []*pb.DirectedEdge{uid(1, 101), scalar(numeric, "43"), scalar(names, "second")}}}))
	require.Equal(t, 1, posting.Oracle().NumPendingTxns())
	certify(4)
	require.Equal(t, 1, posting.Oracle().NumPendingTxns())
	require.NoError(t, apply(5, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 13, Txns: []*pb.TxnStatus{{StartTs: base + 12, CommitTs: base + 13}}}}))
	require.Equal(t, []uint64{1}, read(x.ReverseKey(friend, 101), math.MaxUint64))
	require.Equal(t, []uint64{1}, read(x.CountKey(friend, 3, false), math.MaxUint64))
	require.Empty(t, read(tokenKey(numeric, "int", int64(42)), math.MaxUint64))
	require.Equal(t, []uint64{1}, read(tokenKey(numeric, "int", int64(43)), math.MaxUint64))
	require.Equal(t, []uint64{1}, read(tokenKey(names, "exact", "second"), math.MaxUint64))
	friendPlain := &pb.SchemaUpdate{Predicate: friend, ValueType: pb.Posting_UID, List: true}
	invalid := &pb.SchemaUpdate{Predicate: bad, ValueType: pb.Posting_INT}
	err := apply(6, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 14, Schema: []*pb.SchemaUpdate{friendPlain, invalid}}})
	var rejected *raftSemanticRejection
	require.ErrorAs(t, err, &rejected)
	current, _ := schema.State().Get(context.Background(), friend)
	require.True(t, proto.Equal(friendIndexed, &current), "invalid later predicate must reject before earlier index removal")
	require.Equal(t, []uint64{1}, read(x.ReverseKey(friend, 101), math.MaxUint64))
	certify(6)
	require.Equal(t, []uint64{1}, read(x.ReverseKey(friend, 101), math.MaxUint64))
	require.NoError(t, apply(7, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 15, Schema: []*pb.SchemaUpdate{friendPlain}}}))
	require.Empty(t, read(x.ReverseKey(friend, 99), math.MaxUint64))
	require.Empty(t, read(x.CountKey(friend, 1, false), math.MaxUint64))
	require.Equal(t, []uint64{1, 2, 3, 4}, read(x.ReverseKey(friend, 99), base+10))
	certify(7)
	require.Empty(t, read(x.ReverseKey(friend, 99), math.MaxUint64))
	require.Equal(t, []uint64{1, 2, 3, 4}, read(x.ReverseKey(friend, 99), base+10))
	err = apply(8, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 16, Schema: []*pb.SchemaUpdate{{Predicate: names, ValueType: pb.Posting_STRING, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}}}})
	require.ErrorAs(t, err, &rejected)
	current, _ = schema.State().Get(context.Background(), names)
	require.True(t, current.List)
	certify(8)
}
