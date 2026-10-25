/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/v25/x"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
)

// This is a deterministic producer-size control, not a reconstruction of the
// unknown transaction in the failed checkpoint workload.
func TestRaftTreeDBCommitToDiskWorkBoundAndRecovery(t *testing.T) {
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	store, err := OpenRaftTreeDBStore(opts, "work-bound")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	const count = 100000
	const commitTs = uint64(20)
	txn := NewTxn(10)
	attr := x.AttrInRootNamespace("external-work-bound")
	for i := 0; i < count; i++ {
		value := bytes.Repeat([]byte{byte(i)}, 64)
		binary.BigEndian.PutUint64(value, uint64(i))
		txn.cache.deltas[string(x.DataKey(attr, uint64(i+1)))] = value
	}
	var calls atomic.Uint64
	store.commitStartedForTest = func(ts uint64, _ []mvcc.Mutation) {
		if ts != commitTs {
			t.Errorf("chunk timestamp %d", ts)
		}
		calls.Add(1)
	}
	writer := NewTxnWriterForStore(store)
	producerErr := txn.CommitToDisk(writer, commitTs)
	callbackErr := writer.Flush() // Always join accepted ownership, including RED.
	require.NoError(t, producerErr)
	require.NoError(t, callbackErr)
	require.Greater(t, calls.Load(), uint64(1), "one unsafe native command must be shaped before submission")
	require.NotZero(t, store.writeWorkLimit)
	stats, err := store.Stats()
	require.NoError(t, err)
	require.Equal(t, "serial", stats["dgraph.treedb.commit_pipeline"])
	verify := func(s *TreeDBStore) {
		read := s.NewReadTxn(commitTs)
		defer read.Discard()
		for i := 0; i < count; i++ {
			item, err := read.Get(x.DataKey(attr, uint64(i+1)))
			require.NoError(t, err, "key %d", i)
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, value, 64)
			require.Equal(t, uint64(i), binary.BigEndian.Uint64(value))
			require.Equal(t, commitTs, item.Version())
			require.Equal(t, byte(BitDeltaPosting), item.UserMeta())
		}
	}
	verify(store)
	require.NoError(t, store.CheckpointRaft(context.Background(), []byte("complete-decision")))
	require.NoError(t, store.Close())
	reopened, err := OpenRaftTreeDBStore(opts, "work-bound")
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	verify(reopened)
	t.Logf("actual external calls=%d, worklimit=%d", calls.Load(), store.writeWorkLimit)
}

func TestRaftTreeDBEntryWorkRefusalAndLifetime(t *testing.T) {
	store := openRaftTransferStore(t, "entry-work")
	require.NotZero(t, store.writeWorkLimit)
	cost := store.writeWorkEstimator.Estimate(8, treeDBEnvelopeHeader+8)
	store.writeWorkLimit = cost // Existing private sizing seam: no engine limit changes.
	txn := store.NewWriteTxn().(*treeDBWriteTxn)
	key, value := []byte("prefix01"), []byte("value001")
	require.NoError(t, txn.SetEntry(Entry{Key: key, Value: value}))
	beforeWork, beforeBytes := txn.batch.work, txn.batch.ownedBytes()
	require.ErrorIs(t, txn.SetEntry(Entry{Key: []byte("prefix02"), Value: []byte("value002")}), badger.ErrTxnTooBig)
	require.Equal(t, beforeWork, txn.batch.work)
	require.Equal(t, beforeBytes, txn.batch.ownedBytes())
	clear(key)
	clear(value)
	require.NoError(t, txn.CommitAt(2, nil))
	require.Equal(t, "value001", pointSnapshot(t, store, 2, []byte("prefix01")).Value)
	for _, prefix := range []bool{false, true} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			next := store.NewWriteTxn().(*treeDBWriteTxn)
			if prefix {
				require.NoError(t, next.SetEntry(Entry{Key: []byte("prefix03"), Value: []byte("value003")}))
			}
			err := next.SetEntry(Entry{Key: []byte("oversize"), Value: bytes.Repeat([]byte("x"), 4096)})
			require.ErrorIs(t, err, ErrTreeDBEntryTooLarge)
			require.NotErrorIs(t, err, badger.ErrTxnTooBig)
			require.ErrorIs(t, next.CommitAt(3, nil), ErrTreeDBEntryTooLarge)
			require.Nil(t, next.batch, "terminal failure releases owned arena")
			next.Discard()
		})
	}
	require.Zero(t, len(store.commitSlots))
}

// Adapter shaping must not weaken the engine's atomic oversized-request refusal.
func TestRaftTreeDBRawAtomicOversizeStillRefuses(t *testing.T) {
	store := openRaftTransferStore(t, "raw-oversize")
	commitStoreEntry(t, store, 2, Entry{Key: []byte("prior"), Value: []byte("accepted")})
	const count = 100000
	mutations := make([]mvcc.Mutation, count)
	attr := x.AttrInRootNamespace("raw-oversize")
	for i := range mutations {
		mutations[i] = mvcc.Mutation{Key: x.DataKey(attr, uint64(i+1)), Value: bytes.Repeat([]byte("x"), 64)}
	}
	err := store.external.Apply(func() error { return store.mvcc.CommitAt(4, mutations, mvcc.CommitRelaxed) })
	require.Error(t, err)
	require.True(t, errors.Is(err, mvcc.ErrStorage))
	require.ErrorContains(t, err, "immutable memtable capacity exhausted")
	require.Equal(t, "accepted", pointSnapshot(t, store, 4, []byte("prior")).Value)
	read := store.NewReadTxn(4)
	defer read.Discard()
	for _, i := range []int{0, count / 2, count - 1} {
		_, err := read.Get(mutations[i].Key)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
	}
}
