/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
)

// This control also compiles against the old adapter: its repeated initial
// traversal reports two visited records instead of one.
func TestTreeDBStoreNewExactSeekVisitsOnce(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	key := []byte("exact-seek")
	commitStoreEntry(t, store, 1, Entry{Key: key, Value: []byte("one")})
	read := store.NewReadTxn(1)
	defer read.Discard()
	it := read.NewKeyIterator(key, IteratorOptions{}).(*treeDBIterator)
	defer it.Close()
	require.False(t, it.Valid())
	it.Seek(key)
	require.NoError(t, it.Error())
	require.True(t, it.Valid())
	require.Equal(t, uint64(1), it.raw.Stats().Visited)
	before := it.raw.Stats().Visited
	it.Next()
	require.False(t, it.Valid())
	it.Seek(key)
	require.NoError(t, it.Error())
	require.True(t, it.Valid())
	require.Greater(t, it.raw.Stats().Visited, before)
	require.Equal(t, uint64(1), it.Item().Version())
}

func TestTreeDBStoreNewExactSeekPositionAndOwnership(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	for version := uint64(1); version <= 3; version++ {
		txn := store.NewWriteTxn()
		for _, key := range [][]byte{{}, {0}, []byte("exact-seek"), []byte("exact-seek\x00")} {
			require.NoError(t, txn.SetEntry(Entry{Key: key, Value: []byte{byte(version)}}))
		}
		require.NoError(t, txn.CommitAt(version, nil))
		txn.Discard()
	}
	for _, key := range [][]byte{{}, {0}, []byte("exact-seek")} {
		t.Run(string(key), func(t *testing.T) {
			read := store.NewReadTxn(2)
			defer read.Discard()
			it := read.NewKeyIterator(key, IteratorOptions{}).(*treeDBIterator)
			it.Seek(key)
			require.NoError(t, it.Error())
			require.True(t, it.Valid())
			require.Equal(t, uint64(2), it.Item().Version())
			// Entry() supplies owned bytes; movement and close cannot alter them.
			retained := it.current
			it.Next()
			require.True(t, it.Valid())
			require.Equal(t, uint64(1), it.Item().Version())
			it.Seek(key)
			require.True(t, it.Valid())
			require.Equal(t, uint64(2), it.Item().Version())
			it.Rewind()
			require.True(t, it.Valid())
			it.Close()
			require.True(t, bytes.Equal(key, retained.key))
			require.Equal(t, []byte{2}, retained.value)
		})
	}
	for _, target := range [][]byte{[]byte("a"), []byte("exact-seek"), []byte("z")} {
		read := store.NewReadTxn(2)
		it := read.NewKeyIterator([]byte("exact-seek"), IteratorOptions{})
		it.Seek(target)
		require.NoError(t, it.Error())
		require.Equal(t, bytes.Compare(target, []byte("exact-seek")) <= 0, it.Valid())
		it.Close()
		read.Discard()
	}
}

func TestTreeDBStoreNewExactSeekValidatesDifferentTarget(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	key := []byte("z")
	commitStoreEntry(t, store, 1, Entry{Key: key, Value: []byte("value")})
	// Both targets sort before z; the second overflows through zero escaping.
	for _, target := range [][]byte{bytes.Repeat([]byte{'a'}, math.MaxUint16), bytes.Repeat([]byte{0}, math.MaxUint16/2)} {
		read := store.NewReadTxn(1)
		it := read.NewKeyIterator(key, IteratorOptions{})
		it.Seek(target)
		require.False(t, it.Valid())
		require.ErrorIs(t, it.Error(), mvcc.ErrInvalidKey)
		it.Seek(key)
		require.ErrorIs(t, it.Error(), mvcc.ErrInvalidKey)
		it.Close()
		read.Discard()
	}
}
