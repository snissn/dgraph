/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"google.golang.org/protobuf/proto"
	"math"
	"strings"
	"testing"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestOrderedDeltaWholeWindowBudgetsAndIndexBarrier(t *testing.T) {
	testDeltaInquirySerialBoundary(t, true, "budgets")
}

// Use real pending mutation caches, not fabricated budget counters. Rejected
// candidates must leave the whole prior admitted window unchanged.
func testOrderedDeltaWindowBudgets(t *testing.T, n *node, apply func(*pb.Proposal), attr string, base uint64) {
	prewrite := func(start, first, count uint64, value string) {
		edges := make([]*pb.DirectedEdge, 0, count)
		for uid := first; uid < first+count; uid++ {
			edges = append(edges, &pb.DirectedEdge{Entity: uid, Attr: attr, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
		}
		apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: start, Edges: edges}})
	}
	prewrite(base+6, 100, 63, "v")
	prewrite(base+7, 200, 64, "v")
	prewrite(base+8, 300, 64, "v")
	prewrite(base+9, 400, 1, strings.Repeat("a", 600<<10))
	prewrite(base+10, 500, 1, strings.Repeat("b", 600<<10))
	entry := func(index, start, commit uint64, checksums map[uint32]uint64) raftpb.Entry {
		return checkpointEntry(t, index, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: commit, GroupChecksums: checksums, Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}}})
	}
	admit := func(t *testing.T, w *orderedDeltaWindow, e raftpb.Entry) {
		p, _, _ := w.admit(n, e)
		require.NotNil(t, p)
	}
	reject := func(t *testing.T, w *orderedDeltaWindow, e raftpb.Entry) {
		input, bytes, maps, keys, starts, last := w.inputBytes, w.snapshotBytes, w.mapEntries, len(w.proposalKeys), len(w.starts), w.lastIndex
		p, key, snapshots := w.admit(n, e)
		require.Nil(t, p)
		require.Zero(t, key)
		require.Nil(t, snapshots)
		require.Equal(t, input, w.inputBytes)
		require.Equal(t, bytes, w.snapshotBytes)
		require.Equal(t, maps, w.mapEntries)
		require.Len(t, w.proposalKeys, keys)
		require.Len(t, w.starts, starts)
		require.Equal(t, last, w.lastIndex)
	}
	t.Run("checksum_sequence_boundaries_preserve_originals", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			old, next uint64
			accept    bool
		}{
			{"equal_zero", 0, 0, true}, {"from_zero", 0, 1, true},
			{"equal_max", math.MaxUint64, math.MaxUint64, true},
			{"to_max", math.MaxUint64 - 1, math.MaxUint64, true},
			{"regression", 9, 8, false}, {"wraparound", math.MaxUint64, 0, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				a, b := entry(1, base+2, base+20, map[uint32]uint64{1: 11, 2: 22}), entry(2, base+3, base+21, map[uint32]uint64{1: 11, 2: 22})
				for i, e := range []*raftpb.Entry{&a, &b} {
					var p pb.Proposal
					require.NoError(t, proto.Unmarshal(e.Data[8:], &p))
					p.Delta.ChecksumSequence = []uint64{tc.old, tc.next}[i]
					*e = checkpointEntry(t, e.Index, &p)
				}
				w := new(orderedDeltaWindow)
				first, _, _ := w.admit(n, a)
				require.NotNil(t, first)
				if !tc.accept {
					reject(t, w, b)
					return
				}
				second, _, _ := w.admit(n, b)
				require.NotNil(t, second)
				require.Equal(t, tc.old, first.Delta.ChecksumSequence)
				require.Equal(t, tc.next, second.Delta.ChecksumSequence)
				require.Equal(t, base+20, first.Delta.MaxAssigned)
				require.Equal(t, base+21, second.Delta.MaxAssigned)
				require.NotSame(t, first.Delta, second.Delta)
				require.Equal(t, map[uint32]uint64{1: 11, 2: 22}, first.Delta.GroupChecksums)
				require.Equal(t, first.Delta.GroupChecksums, second.Delta.GroupChecksums)
			})
		}
	})
	t.Run("map_254_plus_128", func(t *testing.T) {
		w := new(orderedDeltaWindow)
		admit(t, w, entry(1, base+6, base+20, nil))
		admit(t, w, entry(2, base+7, base+21, nil))
		require.Equal(t, 254, w.mapEntries)
		reject(t, w, entry(3, base+8, base+22, nil))
	})
	t.Run("copied_payload_crosses_one_MiB", func(t *testing.T) {
		w := new(orderedDeltaWindow)
		admit(t, w, entry(1, base+9, base+20, nil))
		require.Greater(t, w.snapshotBytes, 600<<10)
		require.Less(t, w.snapshotBytes, orderedDeltaMaxSnapshots)
		reject(t, w, entry(2, base+10, base+21, nil))
	})
	t.Run("encoded_input_crosses_64KiB", func(t *testing.T) {
		checksums := make(map[uint32]uint64, 3000)
		for group := uint32(1); group <= 3000; group++ {
			checksums[group] = ^uint64(0)
		}
		a, b := entry(1, base+2, base+20, checksums), entry(2, base+3, base+21, checksums)
		require.LessOrEqual(t, len(a.Data), orderedDeltaMaxInput)
		require.Greater(t, len(a.Data)+len(b.Data), orderedDeltaMaxInput)
		w := new(orderedDeltaWindow)
		admit(t, w, a)
		reject(t, w, b)
	})
	t.Run("nonconsecutive_log_index", func(t *testing.T) {
		a, b := entry(1, base+2, base+20, nil), entry(3, base+3, base+21, nil)
		w := new(orderedDeltaWindow)
		admit(t, w, a)
		reject(t, w, b)
		b.Index = 2
		admit(t, w, b)
	})
}
