package posting

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/tok"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStoreScalarIndexRebuildAggregatesChunksAndPreservesHistory(t *testing.T) {
	oldStore, oldBadger := postingStore, pstore
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	postingStore, pstore = store, nil
	t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
	attr := x.AttrInRootNamespace("store-rebuild-name")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING}
	next := proto.Clone(old).(*pb.SchemaUpdate)
	next.Directive, next.Tokenizer = pb.SchemaUpdate_INDEX, []string{"exact"}
	schema.State().Set(attr, next)
	for uid := uint64(1); uid <= 600; uid++ {
		data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: math.MaxUint64, Value: []byte("shared"), ValType: pb.Posting_STRING, PostingType: pb.Posting_VALUE}}})
		require.NoError(t, err)
		commitStoreEntry(t, store, 10, Entry{Key: x.DataKey(attr, uid), Value: data, UserMeta: BitDeltaPosting})
	}
	rb := &IndexRebuild{Attr: attr, StartTs: 20, OldSchema: old, CurrentSchema: next}
	require.NoError(t, rb.DropIndexes(context.Background()))
	require.NoError(t, rb.BuildIndexes(context.Background()))
	tokenizer, ok := tok.GetTokenizer("exact")
	require.True(t, ok)
	tokens, err := tok.BuildTokens("shared", tokenizer)
	require.NoError(t, err)
	key := x.IndexKey(attr, tokens[0])
	read := func(ts uint64) []uint64 {
		txn := store.NewReadTxn(ts)
		defer txn.Discard()
		it := txn.NewKeyIterator(key, IteratorOptions{AllVersions: true})
		defer it.Close()
		it.Seek(key)
		list, err := readPostingList(key, it)
		require.NoError(t, err)
		uids, err := list.Uids(ListOptions{ReadTs: ts})
		require.NoError(t, err)
		return uids.Uids
	}
	require.Empty(t, read(19))
	require.Len(t, read(20), 600)
	// Logical removal must retain versions needed by existing readers and older cuts.
	remove := proto.Clone(old).(*pb.SchemaUpdate)
	rb = &IndexRebuild{Attr: attr, StartTs: 25, OldSchema: next, CurrentSchema: remove}
	require.NoError(t, rb.DropIndexes(context.Background()))
	require.Empty(t, read(25))
	require.Len(t, read(20), 600)
	// Replay/retry repeats exactly the same build timestamp, never accumulating UIDs.
	rb = &IndexRebuild{Attr: attr, StartTs: 30, OldSchema: old, CurrentSchema: next}
	require.NoError(t, rb.DropIndexes(context.Background()))
	require.NoError(t, rb.BuildIndexes(context.Background()))
	require.NoError(t, rb.DropIndexes(context.Background()))
	require.NoError(t, rb.BuildIndexes(context.Background()))
	require.Len(t, read(30), 600)
	require.Empty(t, read(25))
}

func TestStoreIndexRebuildPropagatesCallbackFailure(t *testing.T) {
	oldStore, oldBadger := postingStore, pstore
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	t.Cleanup(func() { postingStore, pstore = oldStore, oldBadger })
	attr := x.AttrInRootNamespace("store-rebuild-failure")
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING}
	next := proto.Clone(old).(*pb.SchemaUpdate)
	next.Directive, next.Tokenizer = pb.SchemaUpdate_INDEX, []string{"exact"}
	schema.State().Set(attr, next)
	data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: math.MaxUint64, Value: []byte("shared"), ValType: pb.Posting_STRING, PostingType: pb.Posting_VALUE}}})
	require.NoError(t, err)
	commitStoreEntry(t, store, 10, Entry{Key: x.DataKey(attr, 1), Value: data, UserMeta: BitDeltaPosting})
	failure := errors.New("injected index callback failure")
	postingStore, pstore = rebuildFailStore{Store: store, err: failure}, nil
	rb := &IndexRebuild{Attr: attr, StartTs: 20, OldSchema: old, CurrentSchema: next}
	require.ErrorIs(t, rb.BuildIndexes(context.Background()), failure)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, rb.BuildIndexes(ctx), context.Canceled)
}

type rebuildFailStore struct {
	Store
	err error
}

func (s rebuildFailStore) NewWriteTxn() WriteTxn {
	return rebuildFailTxn{WriteTxn: s.Store.NewWriteTxn(), err: s.err}
}

type rebuildFailTxn struct {
	WriteTxn
	err error
}

func (t rebuildFailTxn) CommitAt(ts uint64, cb func(error)) error {
	if cb == nil {
		return t.WriteTxn.CommitAt(ts, nil)
	}
	return t.WriteTxn.CommitAt(ts, func(err error) { cb(errors.Join(err, t.err)) })
}
