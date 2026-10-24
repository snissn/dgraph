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
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftSchemaConversionReplayAndDelayedUniqueAdmission(t *testing.T) {
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
	var entries []raftpb.Entry
	apply := func(index uint64, p *pb.Proposal) error {
		e := checkpointEntry(t, index, p)
		entries = append(entries, e)
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
	listAttr, scalarAttr, uniqueAttr := x.AttrInRootNamespace("repair-list"), x.AttrInRootNamespace("repair-scalar"), x.NamespaceAttr(9, "repair-unique")
	stringSchema := func(attr string, list bool) *pb.SchemaUpdate {
		return &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING, List: list, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}
	}
	oldList, oldScalar, oldUnique := stringSchema(listAttr, true), stringSchema(scalarAttr, false), stringSchema(uniqueAttr, false)
	oldList.Count = true
	require.NoError(t, apply(1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}}))
	require.NoError(t, apply(2, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1, Schema: []*pb.SchemaUpdate{oldList, oldScalar, oldUnique}}}))
	edge := func(attr string, owner uint64, value string) *pb.DirectedEdge {
		return &pb.DirectedEdge{Attr: attr, Entity: owner, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}
	}
	require.NoError(t, apply(3, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 2, Edges: []*pb.DirectedEdge{edge(listAttr, 1, "01"), edge(listAttr, 1, "1"), edge(scalarAttr, 1, "42"), edge(uniqueAttr, 1, "taken")}}}))
	require.NoError(t, apply(4, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 10, Txns: []*pb.TxnStatus{{StartTs: base + 2, CommitTs: base + 3}}}}))
	certify(4)
	// The frontend examined the non-unique schema, but the request has not yet
	// entered ordered application or registered a canonical pending transaction.
	before, _ := schema.State().Get(context.Background(), uniqueAttr)
	require.False(t, before.Unique)
	delayed := &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 5, Edges: []*pb.DirectedEdge{edge(uniqueAttr, 2, "taken")}}}
	newList := &pb.SchemaUpdate{Predicate: listAttr, ValueType: pb.Posting_INT, List: true, Count: true, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"int"}}
	newScalar := proto.Clone(newList).(*pb.SchemaUpdate)
	newScalar.Predicate, newScalar.Count = scalarAttr, false
	newUnique := proto.Clone(oldUnique).(*pb.SchemaUpdate)
	newUnique.Unique, newUnique.Upsert = true, true
	require.NoError(t, apply(5, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 11, Schema: []*pb.SchemaUpdate{newList, newScalar, newUnique}}}))
	readValues := func(attr string, owner, ts uint64) []*pb.Posting {
		list, err := posting.GetNoStore(x.DataKey(attr, owner), ts)
		require.NoError(t, err)
		var out []*pb.Posting
		require.NoError(t, list.Iterate(ts, 0, func(p *pb.Posting) error { out = append(out, proto.Clone(p).(*pb.Posting)); return nil }))
		return out
	}
	readUIDs := func(key []byte, ts uint64) []uint64 {
		list, err := posting.GetNoStore(key, ts)
		require.NoError(t, err)
		out, err := list.Uids(posting.ListOptions{ReadTs: ts})
		require.NoError(t, err)
		return out.Uids
	}
	verify := func() {
		values := readValues(listAttr, 1, math.MaxUint64)
		require.Len(t, values, 1)
		require.Equal(t, pb.Posting_INT, values[0].ValType)
		require.Equal(t, []uint64{1}, readUIDs(x.CountKey(listAttr, 1, false), math.MaxUint64))
		require.Empty(t, readUIDs(x.CountKey(listAttr, 2, false), math.MaxUint64))
		require.Len(t, readValues(listAttr, 1, base+9), 2)
		require.Equal(t, pb.Posting_STRING, readValues(scalarAttr, 1, base+9)[0].ValType)
	}
	verify()
	reopen()
	require.Equal(t, uint64(4), n.Cfg.Applied)
	require.Len(t, readValues(listAttr, 1, math.MaxUint64), 2)
	applyCheckpointEntry(t, n, entries[4])
	verify()
	certify(5)
	verify()
	err := apply(6, delayed)
	var rejected *raftSemanticRejection
	require.ErrorAs(t, err, &rejected)
	require.Zero(t, posting.Oracle().NumPendingTxns())
	require.Empty(t, readValues(uniqueAttr, 2, math.MaxUint64))
	certify(6)
	require.Empty(t, readValues(uniqueAttr, 2, math.MaxUint64))
	// A target-typed SET matches the converted fingerprint; a target-typed DEL
	// subsequently removes it. Both pending images survive exact certification.
	encoded := types.Val{Tid: types.BinaryID}
	require.NoError(t, types.Marshal(types.Val{Tid: types.IntID, Value: int64(1)}, &encoded))
	for i, op := range []pb.DirectedEdge_Op{pb.DirectedEdge_SET, pb.DirectedEdge_DEL} {
		index, start := uint64(7+i*2), base+uint64(12+i*2)
		require.NoError(t, apply(index, &pb.Proposal{Mutations: &pb.Mutations{StartTs: start, Edges: []*pb.DirectedEdge{{Attr: listAttr, Entity: 1, Value: encoded.Value.([]byte), ValueType: pb.Posting_INT, Op: op}}}}))
		certify(index)
		require.NoError(t, apply(index+1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: start + 1, Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: start + 1}}}}))
		certify(index + 1)
		if op == pb.DirectedEdge_SET {
			require.Len(t, readValues(listAttr, 1, math.MaxUint64), 1)
		} else {
			require.Empty(t, readValues(listAttr, 1, math.MaxUint64))
		}
	}
	require.Len(t, readValues(listAttr, 1, base+9), 2)
}
