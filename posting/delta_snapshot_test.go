/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
package posting

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDeltaSnapshotBoundsAndOwnership(t *testing.T) {
	txn := &Txn{StartTs: 1, cache: &LocalCache{startTs: 1, plists: map[string]*List{}, deltas: map[string][]byte{"a": []byte("value")}, maxVersions: map[string]uint64{"a": 1}}}
	snap, size, ok := txn.SnapshotDelta(1, 15, 256)
	require.True(t, ok)
	require.Equal(t, 15, size)
	_, _, ok = txn.SnapshotDelta(1, 14, 256)
	require.False(t, ok)
	_, _, ok = txn.SnapshotDelta(0, 15, 256)
	require.False(t, ok)
	localOracle := &oracle{}
	localOracle.init()
	localOracle.pendingTxns[1] = txn
	populated := make(chan struct{})
	go func() {
		defer close(populated)
		_, err := localOracle.CacheAt(1).GetFromDelta([]byte("reader"))
		if err != nil {
			panic(err)
		}
		txn.Update()
	}()
	<-populated
	require.Contains(t, txn.cache.maxVersions, "reader", "concurrent reader/FillContext population crossed snapshot admission")
	txn.cache.Lock()
	txn.cache.deltas["a"][0] = 'X'
	txn.cache.maxVersions["a"] = 100
	txn.cache.Unlock()
	store := &deltaSnapshotCaptureStore{}
	writer := NewTxnWriterForStore(store)
	require.NoError(t, snap.CommitToDisk(writer, 2))
	require.NoError(t, writer.Flush())
	require.Equal(t, []Entry{{Key: []byte("a"), Value: []byte("value"), UserMeta: BitDeltaPosting}}, store.entries)
	require.Equal(t, uint64(2), store.commitTs)
	require.NotContains(t, snap.txn.cache.maxVersions, "reader")
	require.False(t, snap.Disjoint(snap))
	txn.cache.maxVersions["other"] = 2
	_, _, ok = txn.SnapshotDelta(1, 100, 256)
	require.False(t, ok)
	txn.cache.plists = map[string]*List{"unfinished": {}}
	_, _, ok = txn.SnapshotDelta(3, 100, 256)
	require.False(t, ok)
	var missing *Txn
	_, _, ok = missing.SnapshotDelta(1, 100, 256)
	require.False(t, ok)
}

type deltaSnapshotCaptureStore struct {
	Store
	entries  []Entry
	commitTs uint64
}

func (s *deltaSnapshotCaptureStore) NewWriteTxn() WriteTxn { return &deltaSnapshotCaptureTxn{owner: s} }

type deltaSnapshotCaptureTxn struct {
	WriteTxn
	owner *deltaSnapshotCaptureStore
}

func (t *deltaSnapshotCaptureTxn) SetEntry(e Entry) error {
	t.owner.entries = append(t.owner.entries, e)
	return nil
}
func (t *deltaSnapshotCaptureTxn) Discard() {}
func (t *deltaSnapshotCaptureTxn) CommitAt(ts uint64, cb func(error)) error {
	t.owner.commitTs = ts
	cb(nil)
	return nil
}

// The remaining whole-window map budget must be checked under the same cache
// lock before allocating any rejected immutable custody.
func TestDeltaSnapshotMapBudgetRejectsBeforeAllocation(t *testing.T) {
	txn := &Txn{StartTs: 1, cache: &LocalCache{startTs: 1, deltas: map[string][]byte{"a": []byte("value")}, maxVersions: map[string]uint64{"a": 1}}}
	allocations := testing.AllocsPerRun(10, func() {
		snapshot, size, ok := txn.SnapshotDelta(64, 1<<20, 1)
		if ok || snapshot != nil || size != 0 {
			t.Fatal("snapshot exceeded remaining map budget")
		}
	})
	require.Zero(t, allocations, "rejected copied map entries must never be allocated")
	snapshot, _, ok := txn.SnapshotDelta(64, 1<<20, 2)
	require.True(t, ok)
	require.Equal(t, 2, snapshot.MapEntries())
}
