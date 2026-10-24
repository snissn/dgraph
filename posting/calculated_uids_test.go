/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"testing"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCalculatedUidsRespectHistoricalTimestamp(t *testing.T) {
	for _, deleteAll := range []bool{false, true} {
		name := "single-delete"
		if deleteAll {
			name = "delete-all"
		}
		t.Run(name, func(t *testing.T) {
			layer := newMutableLayer()
			deleted := &pb.Posting{Uid: 3, Op: Del, PostingType: pb.Posting_REF}
			if deleteAll {
				deleted = &pb.Posting{Value: []byte(x.Star), Op: Del}
			}
			// Disk loading encounters newest deltas first, retaining all old versions.
			layer.insertCommittedPostings(&pb.PostingList{CommitTs: 10, Postings: []*pb.Posting{deleted}})
			layer.insertCommittedPostings(&pb.PostingList{CommitTs: 4, Postings: []*pb.Posting{
				{Uid: 2, Op: Set, PostingType: pb.Posting_REF},
				{Uid: 3, Op: Set, PostingType: pb.Posting_REF},
			}})
			list := &List{plist: &pb.PostingList{}, mutationMap: layer, maxTs: 10}
			require.NoError(t, list.calculateUids())
			list.RLock()
			latestEligible := list.canUseCalculatedUids(10)
			list.RUnlock()
			require.True(t, latestEligible, "keep the current-view fast path")
			check := func(opts ListOptions, want []uint64) {
				t.Helper()
				got, err := list.Uids(opts)
				require.NoError(t, err)
				require.Equal(t, want, got.Uids)
			}
			check(ListOptions{ReadTs: 4}, []uint64{2, 3})
			check(ListOptions{ReadTs: 9, AfterUid: 2}, []uint64{3})
			check(ListOptions{ReadTs: 4, Intersect: &pb.List{Uids: []uint64{3}}}, []uint64{3})
			if deleteAll {
				got, err := list.Uids(ListOptions{ReadTs: 10})
				require.NoError(t, err)
				require.Empty(t, got.Uids)
			} else {
				check(ListOptions{ReadTs: 10}, []uint64{2})
				check(ListOptions{ReadTs: 11}, []uint64{2})
			}
		})
	}
}

func TestCalculatedUidsRespectImmutableTimestamp(t *testing.T) {
	pack := codec.Encode([]uint64{2, 3}, 256)
	defer codec.FreePack(pack)
	list := &List{plist: &pb.PostingList{Pack: pack}, mutationMap: newMutableLayer(), minTs: 4, maxTs: 4}
	require.NoError(t, list.calculateUids())
	for _, ts := range []uint64{4, 10} {
		list.RLock()
		eligible := list.canUseCalculatedUids(ts)
		list.RUnlock()
		require.True(t, eligible, "an immutable-only list has no latest-delta timestamp")
		got, err := list.Uids(ListOptions{ReadTs: ts})
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 3}, got.Uids)
	}
	_, err := list.Uids(ListOptions{ReadTs: 3})
	require.ErrorContains(t, err, "less than minTs")
}

// Both backends must use the same historical view even when their latest UID
// bitmap remains cached. No background rollup or cache invalidation may hide it.
func TestCalculatedUidsMemoryLayerHistoryAcrossStores(t *testing.T) {
	for _, backend := range []string{"badger", "raft_checkpoint"} {
		t.Run(backend, func(t *testing.T) {
			var store Store
			if backend == "badger" {
				store = NewBadgerStore(openPostingStoreTestDB(t))
			} else {
				store = openRaftTransferStore(t, "calculated-uids")
			}
			key := x.DataKey(x.AttrInRootNamespace("calculated-history"), 1)
			write := func(ts uint64, pl *pb.PostingList, meta byte) {
				data, err := proto.Marshal(pl)
				require.NoError(t, err)
				commitStoreEntry(t, store, ts, Entry{Key: key, Value: data, UserMeta: meta})
			}
			write(4, &pb.PostingList{Postings: []*pb.Posting{
				{Uid: 2, Op: Set, PostingType: pb.Posting_REF},
				{Uid: 3, Op: Set, PostingType: pb.Posting_REF},
			}}, BitDeltaPosting)
			write(10, &pb.PostingList{Postings: []*pb.Posting{
				{Uid: 3, Op: Del, PostingType: pb.Posting_REF},
			}}, BitDeltaPosting)
			cache, err := ristretto.NewCache(&ristretto.Config[[]byte, *CachePL]{
				NumCounters: 1000, MaxCost: 10000, BufferItems: 16,
				Cost: func(*CachePL) int64 { return 1 },
			})
			require.NoError(t, err)
			defer cache.Close()
			layer := &MemoryLayer{cache: &Cache{data: cache}}
			read := func(ml *MemoryLayer, ts uint64, want []uint64) {
				t.Helper()
				list, err := ml.ReadData(key, store, ts, true)
				require.NoError(t, err)
				got, err := list.Uids(ListOptions{ReadTs: ts})
				require.NoError(t, err)
				require.Equal(t, want, got.Uids)
			}
			read(layer, 10, []uint64{2})
			layer.wait()
			cached, ok := layer.cache.get(key)
			require.True(t, ok)
			require.True(t, cached.list.mutationMap.isUidsCalculated)
			require.Equal(t, uint64(0), cached.list.minTs)
			require.NotNil(t, layer.readFromCache(key, 4), "historical request must actually hit cache")
			read(layer, 4, []uint64{2, 3})
			// A cold historical request also loads latest state before selecting its view.
			read(&MemoryLayer{}, 4, []uint64{2, 3})
			// A rolled immutable anchor above the request instead uses historical disk state.
			pack := codec.Encode([]uint64{2}, 256)
			write(11, &pb.PostingList{Pack: pack}, BitCompletePosting)
			codec.FreePack(pack)
			read(&MemoryLayer{}, 4, []uint64{2, 3})
		})
	}
}
