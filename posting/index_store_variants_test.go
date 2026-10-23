/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func readStoreVariantList(t *testing.T, store Store, key []byte, ts uint64) *List {
	t.Helper()
	txn := store.NewReadTxn(ts)
	defer txn.Discard()
	it := txn.NewKeyIterator(key, IteratorOptions{AllVersions: true})
	defer it.Close()
	it.Seek(key)
	list, err := readPostingListForStore(key, it, store)
	require.NoError(t, err)
	return list
}

func TestStorePostingIterationPropagatesCallbackErrors(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	attr := x.AttrInRootNamespace("iteration-callback-error")
	for _, meta := range []byte{BitCompletePosting, BitDeltaPosting} {
		key := x.DataKey(attr, uint64(meta))
		pack := codec.Encode([]uint64{10, 20}, 256)
		data, err := proto.Marshal(&pb.PostingList{Pack: pack, Postings: []*pb.Posting{{Uid: 10, Op: Set}, {Uid: 20, Op: Set}}})
		codec.FreePack(pack)
		require.NoError(t, err)
		commitStoreEntry(t, store, uint64(meta)+10, Entry{Key: key, Value: data, UserMeta: meta})
		list := readStoreVariantList(t, store, key, 100)
		failure := errors.New("callback failed")
		calls := 0
		err = list.Iterate(100, 0, func(*pb.Posting) error { calls++; return failure })
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, calls)
		require.NoError(t, list.Iterate(100, 0, func(*pb.Posting) error { return ErrStopIteration }))
	}
}

func TestStoreCountReverseRebuildAggregatesAndPreservesHistory(t *testing.T) {
	oldStore, oldBadger := postingStore, pstore
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	postingStore, pstore = store, nil
	t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
	attr := x.AttrInRootNamespace("store-count-reverse")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_UID, List: true}
	next := proto.Clone(old).(*pb.SchemaUpdate)
	next.Directive, next.Count = pb.SchemaUpdate_REVERSE, true
	schema.State().Set(attr, next)
	for uid := uint64(1); uid <= 600; uid++ {
		uids := []uint64{9001}
		if uid == 1 {
			uids = []uint64{700, 9001}
		}
		pack := codec.Encode(uids, 256)
		data, err := proto.Marshal(&pb.PostingList{Pack: pack})
		codec.FreePack(pack)
		require.NoError(t, err)
		key := x.DataKey(attr, uid)
		if uid == 1 {
			part, err := x.SplitKey(key, 1)
			require.NoError(t, err)
			commitStoreEntry(t, store, 10, Entry{Key: part, Value: data, UserMeta: BitCompletePosting})
			data, err = proto.Marshal(&pb.PostingList{Splits: []uint64{1}})
			require.NoError(t, err)
		}
		commitStoreEntry(t, store, 10, Entry{Key: key, Value: data, UserMeta: BitCompletePosting})
	}
	read := func(key []byte, ts uint64) []uint64 {
		list := readStoreVariantList(t, store, key, ts)
		uids, err := list.Uids(ListOptions{ReadTs: ts})
		require.NoError(t, err)
		return uids.Uids
	}
	for _, ts := range []uint64{20, 30} {
		rb := &IndexRebuild{Attr: attr, StartTs: ts, OldSchema: old, CurrentSchema: next, Store: store}
		require.NoError(t, rb.DropIndexes(context.Background()))
		require.NoError(t, rb.BuildIndexes(schema.GetWriteContext(context.Background())))
		require.Len(t, read(x.ReverseKey(attr, 9001), ts), 600)
		require.Equal(t, []uint64{1}, read(x.ReverseKey(attr, 700), ts))
		require.Len(t, read(x.CountKey(attr, 1, false), ts), 599)
		require.Equal(t, []uint64{1}, read(x.CountKey(attr, 2, false), ts))
		require.Equal(t, []uint64{9001}, read(x.CountKey(attr, 600, true), ts))
		require.Equal(t, []uint64{700}, read(x.CountKey(attr, 1, true), ts))
		remove := &IndexRebuild{Attr: attr, StartTs: ts + 5, OldSchema: next, CurrentSchema: old, Store: store}
		require.NoError(t, remove.DropIndexes(context.Background()))
		require.NoError(t, remove.BuildIndexes(context.Background()))
		require.Empty(t, read(x.ReverseKey(attr, 9001), ts+5))
		require.Empty(t, read(x.CountKey(attr, 1, false), ts+5))
		require.Empty(t, read(x.CountKey(attr, 600, true), ts+5))
		require.Len(t, read(x.ReverseKey(attr, 9001), ts), 600)
	}
}

func TestStoreScalarListConversionPreservesTaggedValuesAndOldReads(t *testing.T) {
	oldStore, oldBadger := postingStore, pstore
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	postingStore, pstore = store, nil
	t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
	attr := x.AttrInRootNamespace("store-list-conversion")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING, Lang: true}
	next := proto.Clone(old).(*pb.SchemaUpdate)
	next.List = true
	schema.State().Set(attr, next)
	key := x.DataKey(attr, 1)
	pack := codec.Encode([]uint64{123, math.MaxUint64}, 256)
	data, err := proto.Marshal(&pb.PostingList{Pack: pack, Postings: []*pb.Posting{
		{Uid: 123, Value: []byte("tagged"), LangTag: []byte("en"), ValType: pb.Posting_STRING, PostingType: pb.Posting_VALUE_LANG},
		{Uid: math.MaxUint64, Value: []byte("untagged"), ValType: pb.Posting_STRING, PostingType: pb.Posting_VALUE},
	}})
	codec.FreePack(pack)
	require.NoError(t, err)
	commitStoreEntry(t, store, 10, Entry{Key: key, Value: data, UserMeta: BitCompletePosting})
	rb := &IndexRebuild{Attr: attr, StartTs: 20, OldSchema: old, CurrentSchema: next, Store: store}
	require.NoError(t, rb.BuildData(schema.GetWriteContext(context.Background())))
	read := func(ts uint64) map[uint64]string {
		list := readStoreVariantList(t, store, key, ts)
		values := make(map[uint64]string)
		require.NoError(t, list.Iterate(ts, 0, func(p *pb.Posting) error { values[p.Uid] = string(p.Value); return nil }))
		return values
	}
	require.Equal(t, map[uint64]string{123: "tagged", math.MaxUint64: "untagged"}, read(19))
	current := read(20)
	require.Len(t, current, 2)
	require.Equal(t, "tagged", current[123])
	require.NotContains(t, current, uint64(math.MaxUint64))
	require.Contains(t, current, fingerprintEdgeForContext(schema.GetWriteContext(context.Background()), &pb.DirectedEdge{Attr: attr, Value: []byte("untagged"), ValueType: pb.Posting_STRING}))
	// Repeating the same timestamp must preserve the complete converted list.
	require.NoError(t, rb.BuildData(schema.GetWriteContext(context.Background())))
	require.Equal(t, current, read(20))
}

func TestStoreSchemaValidationUsesLogicalDataAndPreservesErrorClass(t *testing.T) {
	attr := x.AttrInRootNamespace("schema-logical-empty")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_UID, List: true}
	next := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING}
	for _, prune := range []bool{false, true} {
		store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
		for ts, op := range map[uint64]uint32{2: Set, 3: Del} {
			data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: 10, Op: op, PostingType: pb.Posting_REF}}})
			require.NoError(t, err)
			commitStoreEntry(t, store, ts, Entry{Key: x.DataKey(attr, 1), Value: data, UserMeta: BitDeltaPosting})
		}
		if prune {
			require.NoError(t, materializeRaftPostingBaselines(context.Background(), store, 6))
			require.NoError(t, store.AdvanceDiscardFloor(6))
			_, err := store.PruneVersions(4)
			require.NoError(t, err)
		}
		rb := &IndexRebuild{Attr: attr, StartTs: 10, OldSchema: old, CurrentSchema: next, Store: store}
		require.NoError(t, rb.ValidateData(context.Background()), "empty logical data must permit shape changes despite different physical histories")
		data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: 10, Op: Set, PostingType: pb.Posting_REF}}})
		require.NoError(t, err)
		commitStoreEntry(t, store, 8, Entry{Key: x.DataKey(attr, 1), Value: data, UserMeta: BitDeltaPosting})
		list := readStoreVariantList(t, store, x.DataKey(attr, 1), 10)
		uids, err := list.Uids(ListOptions{ReadTs: 10})
		require.NoError(t, err)
		require.Equal(t, []uint64{10}, uids.Uids, "pruned=%v list=%s", prune, list.Print())
		seen := 0
		pk := x.ParsedKey{Attr: attr}
		require.NoError(t, walkStoreLists(context.Background(), store, 10, pk.DataPrefix(), func(_ []byte, list *List) error {
			return list.Iterate(10, 0, func(*pb.Posting) error { seen++; return nil })
		}))
		require.Equal(t, 1, seen, "pruned=%v", prune)
		require.ErrorIs(t, rb.ValidateData(context.Background()), ErrSchemaDataConflict)
	}
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	for _, value := range []string{"42", "not-an-int"} {
		pred := x.AttrInRootNamespace("schema-convert-" + value)
		data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: math.MaxUint64, Value: []byte(value), ValType: pb.Posting_STRING, PostingType: pb.Posting_VALUE}}})
		require.NoError(t, err)
		commitStoreEntry(t, store, 2, Entry{Key: x.DataKey(pred, 1), Value: data, UserMeta: BitDeltaPosting})
		rb := &IndexRebuild{Attr: pred, StartTs: 10, OldSchema: &pb.SchemaUpdate{ValueType: pb.Posting_STRING}, CurrentSchema: &pb.SchemaUpdate{ValueType: pb.Posting_INT}, Store: store}
		if value == "42" {
			require.NoError(t, rb.ValidateData(context.Background()))
		} else {
			require.ErrorIs(t, rb.ValidateData(context.Background()), ErrSchemaDataConflict)
		}
	}
	corrupt := x.AttrInRootNamespace("schema-corrupt-input")
	commitStoreEntry(t, store, 2, Entry{Key: x.DataKey(corrupt, 1), Value: []byte{0xff}, UserMeta: BitDeltaPosting})
	rb := &IndexRebuild{Attr: corrupt, StartTs: 10, OldSchema: &pb.SchemaUpdate{ValueType: pb.Posting_STRING}, CurrentSchema: &pb.SchemaUpdate{ValueType: pb.Posting_INT}, Store: store}
	err := rb.ValidateData(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSchemaDataConflict, "corrupt/unknown reads cannot become terminal semantic no-ops")
}
