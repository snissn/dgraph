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

func TestStoreScalarTypeRoundtripPreservesStoredEncoding(t *testing.T) {
	for _, backend := range []string{"badger", "treedb"} {
		t.Run(backend, func(t *testing.T) {
			oldStore, oldBadger := postingStore, pstore
			var store Store
			if backend == "badger" {
				pstore = openPostingStoreTestDB(t)
				store = NewBadgerStore(pstore)
			} else {
				store, _ = openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
				pstore = nil
			}
			postingStore = store
			t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
			attr := x.AttrInRootNamespace("scalar-roundtrip-" + backend)
			key := x.DataKey(attr, 1)
			floatSchema := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_FLOAT}
			intSchema := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_INT}
			schema.State().Set(attr, floatSchema)
			encoded := types.Val{Tid: types.BinaryID}
			require.NoError(t, types.Marshal(types.Val{Tid: types.FloatID, Value: float64(1.6)}, &encoded))
			originalBytes := append([]byte(nil), encoded.Value.([]byte)...)
			list := NewList(key, &pb.PostingList{}, 0)
			txn := NewTxn(9)
			txn.cache.SetIfAbsent(string(key), list)
			require.NoError(t, list.addMutation(context.Background(), txn, &pb.DirectedEdge{
				Attr: attr, Value: originalBytes, ValueType: pb.Posting_FLOAT, Op: pb.DirectedEdge_SET,
			}))
			txn.Update()
			writer := NewTxnWriterForStore(store)
			require.NoError(t, txn.CommitToDisk(writer, 10))
			require.NoError(t, writer.Flush())

			// The query path converts the stored value to the current schema type.
			// A scalar schema change must not replace its original physical value.
			read := func(ts uint64, logicalType types.TypeID) types.Val {
				t.Helper()
				list, err := getNewForStore(key, store, ts, false)
				require.NoError(t, err)
				raw, err := list.Value(ts)
				require.NoError(t, err)
				require.Equal(t, types.FloatID, raw.Tid, "stored type at read timestamp %d", ts)
				require.Equal(t, originalBytes, raw.Value, "stored bytes at read timestamp %d", ts)
				readTxn := store.NewReadTxn(ts)
				item, err := readTxn.Get(key)
				require.NoError(t, err)
				require.Equal(t, uint64(10), item.Version(), "scalar schema changes must not write DATA")
				readTxn.Discard()
				value, err := types.Convert(raw, logicalType)
				require.NoError(t, err)
				return value
			}
			require.Equal(t, float64(1.6), read(10, types.FloatID).Value)
			change := func(ts uint64, old, next *pb.SchemaUpdate) {
				t.Helper()
				schema.State().Set(attr, next)
				rb := IndexRebuild{Attr: attr, StartTs: ts, OldSchema: old, CurrentSchema: next}
				if backend == "treedb" {
					rb.Store = store
				}
				require.NoError(t, rb.ValidateData(context.Background()))
				require.NoError(t, rb.BuildData(context.Background()))
			}
			change(20, floatSchema, intSchema)
			require.Equal(t, int64(1), read(20, types.IntID).Value)
			require.Equal(t, float64(1.6), read(19, types.FloatID).Value)
			change(30, intSchema, floatSchema)
			for _, ts := range []uint64{10, 19, 20, 29, 30} {
				value := read(ts, types.FloatID)
				require.Equal(t, float64(1.6), value.Value)
				require.True(t, types.CompareVals("ge", value, types.Val{Tid: types.FloatID, Value: float64(1.6)}))
			}
		})
	}
}

func TestStoreDataConversionCanonicalizesValuesAndFingerprints(t *testing.T) {
	for _, shape := range []struct {
		name             string
		oldList, newList bool
		cold             bool
		badger           bool
	}{{"scalar-list", false, true, false, false}, {"list-list", true, true, false, false}, {"cold-list-list", true, true, true, false}, {"badger-list-list", true, true, false, true}} {
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
			if !shape.badger {
				rb.Store = store
			}
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
