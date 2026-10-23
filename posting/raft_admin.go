/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"math"

	"github.com/dgraph-io/dgraph/v25/x"
)

// RaftPendingKeysInUse examines canonical nonempty deltas only. Query cache
// contents must never determine an ordered administration outcome.
func RaftPendingKeysInUse(matches func([]byte) bool) bool {
	o := Oracle()
	o.RLock()
	defer o.RUnlock()
	for _, txn := range o.pendingTxns {
		txn.Lock()
		txn.cache.RLock()
		found := false
		for key, value := range txn.cache.deltas {
			if len(value) != 0 && matches([]byte(key)) {
				found = true
				break
			}
		}
		txn.cache.RUnlock()
		txn.Unlock()
		if found {
			return true
		}
	}
	return false
}

// TombstoneRaftPostingKeys preserves historical versions and certified roots.
// The caller owns the logged semantic operation and exclusive reader barrier.
// Metadata keys get MVCC tombstones; lists get complete empty posting anchors.
func TombstoneRaftPostingKeys(ctx context.Context, store Store, ts uint64, matches func([]byte) bool) (retErr error) {
	reader := store.NewReadTxn(math.MaxUint64)
	defer reader.Discard()
	it := reader.NewIterator(IteratorOptions{})
	defer it.Close()
	writer := NewTxnWriterForStore(store)
	defer func() { retErr = errors.Join(retErr, writer.Flush()) }()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Item().KeyCopy(nil)
		if !matches(key) {
			continue
		}
		if key[0] == x.ByteSchema || key[0] == x.ByteType {
			if err := writer.update(ts, func(txn WriteTxn) error { return txn.Delete(key) }); err != nil {
				return err
			}
		} else if err := writer.SetAt(key, nil, BitEmptyPosting, ts); err != nil {
			return err
		}
	}
	return it.Error()
}
