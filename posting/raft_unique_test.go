/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/tok"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRaftUniqueOrderedValidationPreservesTransactionSemantics(t *testing.T) {
	oldStore, oldBadger := postingStore, pstore
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	postingStore, pstore = store, nil
	t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
	attr := x.NamespaceAttr(9, "unique-ordered")
	su := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING, Lang: true,
		Unique: true, Upsert: true, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}
	schema.State().Set(attr, su)
	edge := func(owner uint64, value string) *pb.DirectedEdge {
		return &pb.DirectedEdge{Attr: attr, Entity: owner, Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}
	}
	stage := func(ts uint64, edges ...*pb.DirectedEdge) *Txn {
		txn := NewTxn(ts)
		txn.cache.privateStore = store
		for _, e := range edges {
			list, err := txn.Get(x.DataKey(e.Attr, e.Entity))
			require.NoError(t, err)
			require.NoError(t, list.AddMutationWithIndex(schema.GetWriteContext(context.Background()), e, txn))
		}
		txn.Update()
		return txn
	}
	seed := stage(2, edge(1, "one"), edge(2, "two"))
	writer := NewTxnWriterForStore(store)
	require.NoError(t, seed.CommitToDisk(writer, 3))
	require.NoError(t, writer.Flush())
	validate := func(edges ...*pb.DirectedEdge) error {
		return ValidateRaftUniqueTxn(context.Background(), stage(10, edges...), 9)
	}
	require.ErrorIs(t, validate(edge(3, "one")), ErrSchemaDataConflict)
	require.NoError(t, validate(edge(1, "one")), "same owner may retain its value")
	require.NoError(t, validate(edge(1, "two"), edge(2, "one")), "atomic scalar swap")
	require.NoError(t, validate(edge(1, "fresh"), edge(3, "one")), "old owner overwritten before reassignment")
	remove := edge(1, "one")
	remove.Op = pb.DirectedEdge_DEL
	require.NoError(t, validate(remove, edge(3, "one")), "explicit removal before reassignment")
	require.ErrorIs(t, validate(edge(3, "fresh"), edge(4, "fresh")), ErrSchemaDataConflict)
	other := edge(3, "one")
	other.Attr = x.NamespaceAttr(10, "unique-ordered")
	otherSchema := proto.Clone(su).(*pb.SchemaUpdate)
	otherSchema.Predicate = other.Attr
	schema.State().Set(other.Attr, otherSchema)
	require.NoError(t, validate(other), "namespaces have independent unique values")
	lang := edge(3, "one")
	lang.Lang = "en"
	require.NoError(t, validate(lang), "language scope is part of value identity")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ValidateRaftUniqueTxn(ctx, stage(10, edge(3, "one")), 9), context.Canceled)
	// An index is a candidate source, including hash collisions/stale entries.
	// Actual canonical values decide equality, not index membership alone.
	impl, ok := tok.GetTokenizer("exact")
	require.True(t, ok)
	tokens, err := tok.BuildTokens("third", impl)
	require.NoError(t, err)
	data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: 1, Op: Set}}})
	require.NoError(t, err)
	commitStoreEntry(t, store, 4, Entry{Key: x.IndexKey(attr, tokens[0]), Value: data, UserMeta: BitDeltaPosting})
	require.NoError(t, validate(edge(3, "third")))
	// A projected index deletion must not hide another surviving logical
	// value in the same candidate bucket (e.g. list/hash-token collisions).
	tokens, err = tok.BuildTokens("one", impl)
	require.NoError(t, err)
	candidate := stage(10, edge(3, "one"))
	data, err = proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: 1, Op: Del, StartTs: 10}}})
	require.NoError(t, err)
	candidate.cache.deltas[string(x.IndexKey(attr, tokens[0]))] = data
	require.ErrorIs(t, ValidateRaftUniqueTxn(context.Background(), candidate, 9), ErrSchemaDataConflict)
	// Local reclamation changes neither the ordered decision nor collision
	// filtering. The complete baseline uses the maintained exclusive floor.
	require.NoError(t, materializeRaftPostingBaselines(context.Background(), store, 6))
	require.NoError(t, store.AdvanceDiscardFloor(6))
	_, err = store.PruneVersions(8)
	require.NoError(t, err)
	require.ErrorIs(t, validate(edge(3, "one")), ErrSchemaDataConflict)
	require.NoError(t, validate(edge(1, "two"), edge(2, "one")))
	require.NoError(t, validate(edge(3, "third")))
	unknown := errors.New("unique source read failed")
	require.ErrorIs(t, validateRaftUniqueTxn(context.Background(), uniqueErrorStore{Store: store, err: unknown}, stage(10, edge(3, "one")), 9), unknown)
}

type uniqueErrorStore struct {
	Store
	err error
}

func (s uniqueErrorStore) NewReadTxn(ts uint64) ReadTxn {
	return uniqueErrorRead{ReadTxn: s.Store.NewReadTxn(ts), err: s.err}
}

type uniqueErrorRead struct {
	ReadTxn
	err error
}

func (r uniqueErrorRead) NewKeyIterator(key []byte, opts IteratorOptions) Iterator {
	return uniqueErrorIterator{Iterator: r.ReadTxn.NewKeyIterator(key, opts), err: r.err}
}

type uniqueErrorIterator struct {
	Iterator
	err error
}

func (i uniqueErrorIterator) Valid() bool  { return false }
func (i uniqueErrorIterator) Error() error { return i.err }
