/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"math"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStoreDataConversionCanonicalizesValuesAndFingerprints(t *testing.T) {
	for _, shape := range []struct {
		name             string
		oldList, newList bool
		cold             bool
		badger           bool
	}{{"scalar-scalar", false, false, false, false}, {"scalar-list", false, true, false, false}, {"list-list", true, true, false, false}, {"cold-list-list", true, true, true, false}, {"badger-list-list", true, true, false, true}} {
		t.Run(shape.name, func(t *testing.T) {
			oldStore, oldBadger := postingStore, pstore
			var store Store
			if shape.badger {
				store = NewBadgerStore(pstore)
			} else {
				store, _ = openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
				postingStore, pstore = store, nil
			}
			t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
			attr := x.AttrInRootNamespace("convert-" + shape.name)
			key := x.DataKey(attr, 1)
			old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING, List: shape.oldList}
			next := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_INT, List: shape.newList}
			values := []string{"01"}
			if shape.oldList {
				values = append(values, "1")
			}
			seed := NewList(key, &pb.PostingList{}, 0)
			txn := NewTxn(9)
			txn.cache.SetIfAbsent(string(key), seed)
			schema.State().Set(attr, old)
			for _, value := range values {
				require.NoError(t, seed.addMutation(context.Background(), txn, &pb.DirectedEdge{Attr: attr, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET,
					Facets: []*api.Facet{{Key: "source", ValType: api.Facet_STRING, Value: []byte(value)}}}))
			}
			txn.Update()
			writer := NewTxnWriterForStore(store)
			require.NoError(t, txn.CommitToDisk(writer, 10))
			require.NoError(t, writer.Flush())
			read := func(ts uint64) []*pb.Posting {
				list, err := getNewForStore(key, store, ts, false)
				require.NoError(t, err)
				var out []*pb.Posting
				require.NoError(t, list.Iterate(ts, 0, func(p *pb.Posting) error { out = append(out, proto.Clone(p).(*pb.Posting)); return nil }))
				return out
			}
			original := read(10)
			require.Len(t, original, len(values))
			if shape.cold {
				postings := read(10)
				uids := make([]uint64, len(postings))
				for i, p := range postings {
					uids[i] = p.Uid
				}
				pack := codec.Encode(uids, 256)
				data, err := proto.Marshal(&pb.PostingList{Pack: pack, Postings: postings})
				codec.FreePack(pack)
				require.NoError(t, err)
				part, err := x.SplitKey(key, 1)
				require.NoError(t, err)
				commitStoreEntry(t, store, 11, Entry{Key: part, Value: data, UserMeta: BitCompletePosting})
				data, err = proto.Marshal(&pb.PostingList{Splits: []uint64{1}})
				require.NoError(t, err)
				commitStoreEntry(t, store, 11, Entry{Key: key, Value: data, UserMeta: BitCompletePosting})
			}
			schema.State().Set(attr, next)
			rb := IndexRebuild{Attr: attr, StartTs: 20, OldSchema: old, CurrentSchema: next}
			require.NoError(t, rb.ValidateData(context.Background()))
			require.NoError(t, rb.BuildData(context.Background()))
			binaryValue := types.Val{Tid: types.BinaryID}
			require.NoError(t, types.Marshal(types.Val{Tid: types.IntID, Value: int64(1)}, &binaryValue))
			converted := read(20)
			require.Len(t, converted, 1, "equivalent converted values must collapse")
			require.Equal(t, pb.Posting_INT, converted[0].ValType)
			require.Equal(t, binaryValue.Value.([]byte), converted[0].Value)
			require.Equal(t, original[len(original)-1].Facets, converted[0].Facets, "equal targets retain facets from the last canonical source UID")
			expected := fingerprintEdgeWithList(&pb.DirectedEdge{Value: binaryValue.Value.([]byte)}, shape.newList)
			require.Equal(t, expected, converted[0].Uid)
			if shape.newList {
				require.NotEqual(t, uint64(math.MaxUint64), expected)
			}
			require.Len(t, read(19), len(values), "conversion must preserve the preceding image")
			// Repeating the schema timestamp must not accumulate old fingerprints.
			require.NoError(t, rb.BuildData(context.Background()))
			require.Len(t, read(20), 1)
			for i, op := range []pb.DirectedEdge_Op{pb.DirectedEdge_SET, pb.DirectedEdge_DEL} {
				start := uint64(21 + i*2)
				list, err := getNewForStore(key, store, start, false)
				require.NoError(t, err)
				txn := NewTxn(start)
				txn.cache.SetIfAbsent(string(key), list)
				require.NoError(t, list.addMutation(context.Background(), txn, &pb.DirectedEdge{Attr: attr, Value: binaryValue.Value.([]byte), ValueType: pb.Posting_INT, Op: op}))
				txn.Update()
				writer := NewTxnWriterForStore(store)
				require.NoError(t, txn.CommitToDisk(writer, start+1))
				require.NoError(t, writer.Flush())
				if op == pb.DirectedEdge_SET {
					require.Len(t, read(start+1), 1)
				} else {
					require.Empty(t, read(start+1))
				}
			}
			require.Len(t, read(19), len(values))
		})
	}
}
