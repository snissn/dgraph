/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
)

func TestBadgerStorePreservesManagedTimestampsMetadataAndIteration(t *testing.T) {
	db := openPostingStoreTestDB(t)
	store := NewBadgerStore(db)

	key := []byte("posting/store/read")
	futureExpiry := uint64(time.Now().Add(time.Hour).Unix())
	commitStoreEntry(t, store, 5, Entry{
		Key:       key,
		Value:     []byte("at-five"),
		UserMeta:  BitDeltaPosting,
		ExpiresAt: futureExpiry,
	})
	commitStoreEntry(t, store, 9, Entry{
		Key:                    key,
		Value:                  []byte("at-nine"),
		UserMeta:               BitCompletePosting,
		DiscardEarlierVersions: true,
	})

	readAtFive := store.NewReadTxn(5)
	item, err := readAtFive.Get(key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), item.Version())
	require.Equal(t, BitDeltaPosting, item.UserMeta())
	require.Equal(t, futureExpiry, item.ExpiresAt())
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("at-five"), value)
	readAtFive.Discard()

	readAtNine := store.NewReadTxn(9)
	item, err = readAtNine.Get(key)
	require.NoError(t, err)
	require.Equal(t, key, item.Key())
	require.Equal(t, uint64(9), item.Version())
	require.Equal(t, BitCompletePosting, item.UserMeta())
	require.True(t, item.DiscardEarlierVersions())
	var callbackValue []byte
	require.NoError(t, item.Value(func(value []byte) error {
		callbackValue = append(callbackValue, value...)
		return nil
	}))
	require.Equal(t, []byte("at-nine"), callbackValue)
	value, err = item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("at-nine"), value)
	readAtNine.Discard()

	versionedKey := []byte("posting/store/versioned")
	commitStoreEntry(t, store, 3, Entry{
		Key:      versionedKey,
		Value:    []byte("v3"),
		UserMeta: BitDeltaPosting,
	})
	commitStoreEntry(t, store, 8, Entry{
		Key:      versionedKey,
		Value:    []byte("v8"),
		UserMeta: BitSchemaPosting,
	})

	iterTxn := store.NewReadTxn(8)
	iter := iterTxn.NewIterator(IteratorOptions{
		Prefix:         versionedKey,
		AllVersions:    true,
		PrefetchValues: true,
	})
	defer iterTxn.Discard()
	defer iter.Close()

	var gotVersions []uint64
	var gotMetas []byte
	var gotValues [][]byte
	for iter.Seek(versionedKey); iter.ValidForPrefix(versionedKey); iter.Next() {
		item := iter.Item()
		gotVersions = append(gotVersions, item.Version())
		gotMetas = append(gotMetas, item.UserMeta())
		value, err := item.ValueCopy(nil)
		require.NoError(t, err)
		gotValues = append(gotValues, value)
	}
	require.Equal(t, []uint64{8, 3}, gotVersions)
	require.Equal(t, []byte{BitSchemaPosting, BitDeltaPosting}, gotMetas)
	require.Equal(t, [][]byte{[]byte("v8"), []byte("v3")}, gotValues)

	keyIter := iterTxn.NewKeyIterator(versionedKey, IteratorOptions{AllVersions: true})
	defer keyIter.Close()
	gotVersions = gotVersions[:0]
	for keyIter.Seek(versionedKey); keyIter.Valid(); keyIter.Next() {
		require.Equal(t, versionedKey, keyIter.Item().Key())
		gotVersions = append(gotVersions, keyIter.Item().Version())
	}
	require.Equal(t, []uint64{8, 3}, gotVersions)
	require.False(t, store.IsClosed())
}

func TestBadgerStorePreservesAtomicDelete(t *testing.T) {
	db := openPostingStoreTestDB(t)
	store := NewBadgerStore(db)
	key := []byte("posting/store/delete")
	commitStoreEntry(t, store, 4, Entry{Key: key, Value: []byte("before-delete")})

	txn := store.NewWriteTxn()
	require.NoError(t, txn.Delete(key))
	done := make(chan error, 1)
	require.NoError(t, txn.CommitAt(7, func(err error) { done <- err }))
	require.NoError(t, <-done)
	txn.Discard()

	readBefore := store.NewReadTxn(6)
	item, err := readBefore.Get(key)
	require.NoError(t, err)
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("before-delete"), value)
	readBefore.Discard()

	readAfter := store.NewReadTxn(7)
	_, err = readAfter.Get(key)
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	readAfter.Discard()
}

func TestBadgerOperationalPathsFailClosedWithoutBadger(t *testing.T) {
	original := pstore
	pstore = nil
	t.Cleanup(func() { pstore = original })

	err := DeleteAll()
	require.ErrorIs(t, err, ErrBadgerOperationalPath)
	require.Contains(t, err.Error(), "drop all posting data")

	noop := &IndexRebuild{
		Attr:          x.AttrInRootNamespace("store-noop"),
		CurrentSchema: &pb.SchemaUpdate{},
	}
	require.NoError(t, noop.DropIndexes(t.Context()))
	require.NoError(t, noop.BuildData(t.Context()))

	drop := &IndexRebuild{
		Attr: x.AttrInRootNamespace("store-drop"),
		OldSchema: &pb.SchemaUpdate{
			Directive: pb.SchemaUpdate_INDEX,
			Tokenizer: []string{"term"},
		},
		CurrentSchema: &pb.SchemaUpdate{},
	}
	err = drop.DropIndexes(t.Context())
	require.ErrorIs(t, err, ErrBadgerOperationalPath)
	require.Contains(t, err.Error(), "drop index prefixes")

	listRebuild := &IndexRebuild{
		Attr:          x.AttrInRootNamespace("store-list-rebuild"),
		OldSchema:     &pb.SchemaUpdate{List: false},
		CurrentSchema: &pb.SchemaUpdate{List: true},
	}
	err = listRebuild.BuildData(t.Context())
	require.ErrorIs(t, err, ErrBadgerOperationalPath)
	require.Contains(t, err.Error(), "rebuild list data")
}

func TestTxnWriterForStorePreservesBadgerWriteBehavior(t *testing.T) {
	db := openPostingStoreTestDB(t)
	writer := NewTxnWriterForStore(NewBadgerStore(db))

	err := writer.Write(&bpb.KVList{Kv: []*bpb.KV{
		{
			Key:      []byte("posting/store/writer-a"),
			Value:    []byte("writer-a"),
			UserMeta: []byte{BitSchemaPosting},
			Version:  11,
		},
		{
			Key:      []byte("posting/store/writer-b"),
			Value:    []byte("writer-b"),
			UserMeta: []byte{BitEmptyPosting},
			Version:  12,
		},
	}})
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	require.NoError(t, writer.SetAt([]byte("posting/store/noop"), []byte("noop"), BitDeltaPosting, 0))
	require.NoError(t, writer.Flush())

	readBadgerItem(t, db, []byte("posting/store/writer-a"), 11, []byte("writer-a"), BitSchemaPosting)
	readBadgerItem(t, db, []byte("posting/store/writer-b"), 12, []byte("writer-b"), BitEmptyPosting)

	txn := db.NewTransactionAt(math.MaxUint64, false)
	_, err = txn.Get([]byte("posting/store/noop"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	txn.Discard()
}

func openPostingStoreTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir()).
		WithLogger(nil).
		WithSyncWrites(false).
		WithNumVersionsToKeep(math.MaxInt64).
		WithCompression(options.None)
	db, err := badger.OpenManaged(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func commitStoreEntry(t *testing.T, store Store, commitTs uint64, entry Entry) {
	t.Helper()
	txn := store.NewWriteTxn()
	defer txn.Discard()
	require.NoError(t, txn.SetEntry(entry))
	done := make(chan error, 1)
	require.NoError(t, txn.CommitAt(commitTs, func(err error) { done <- err }))
	require.NoError(t, <-done)
}

func readBadgerItem(t *testing.T, db *badger.DB, key []byte, readTs uint64, wantValue []byte, wantMeta byte) {
	t.Helper()
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()
	item, err := txn.Get(key)
	require.NoError(t, err)
	require.Equal(t, readTs, item.Version())
	require.Equal(t, wantMeta, item.UserMeta())
	gotValue, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, wantValue, gotValue)
}
