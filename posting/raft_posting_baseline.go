/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
)

// materializeRaftPostingBaselines prepares complete posting anchors before
// generic MVCC pruning. The caller supplies a private epoch-bound Store while
// ordered apply, background writers and ALL query/read admissions are drained
// and excluded. This routine never changes a floor or publishes a certificate.
//
// Gomap disallows reads/commits <=physicalFloor. Every main and split anchor
// is therefore reconstructed at B=physicalFloor+1 and written at exactly B.
// Retaining Rollup's cold-list timestamps, or rebasing splits alone, would leave
// dependencies inaccessible after the floor advances. The caller must prove
// from the preceding replicated Oracle/pending state that no future commit can
// arrive at or below B, then advance/prune only after this complete pass drains.
func materializeRaftPostingBaselines(ctx context.Context, store Store, physicalFloor uint64) (retErr error) {
	if physicalFloor == math.MaxUint64 {
		return fmt.Errorf("posting baseline timestamp overflows")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	baseline := physicalFloor + 1
	txn := store.NewReadTxn(baseline)
	defer txn.Discard()
	it := txn.NewIterator(IteratorOptions{AllVersions: true})
	defer it.Close()
	writer := NewTxnWriterForStore(store)
	defer func() { retErr = errors.Join(retErr, writer.Flush(), ctx.Err()) }()
	for it.Rewind(); it.Valid(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := it.Item()
		key := item.KeyCopy(nil)
		// Schema/type records use generic MVCC anchors; their values are not
		// posting lists. Physical tombstones also need no posting reconstruction.
		if !item.IsDeletedOrExpired() && item.UserMeta() != BitSchemaPosting {
			list, err := readPostingListForStore(key, it, store)
			if err != nil && !errors.Is(err, ErrInvalidKey) {
				return err
			}
			if err == nil {
				kvs, err := list.Rollup(nil, baseline)
				if err != nil {
					return err
				}
				for _, kv := range kvs {
					if len(kv.UserMeta) != 1 || (kv.UserMeta[0] != BitCompletePosting && kv.UserMeta[0] != BitEmptyPosting) {
						return fmt.Errorf("invalid posting baseline metadata for %x", kv.Key)
					}
					if err := writer.SetAt(kv.Key, kv.Value, kv.UserMeta[0], baseline); err != nil {
						return err
					}
				}
			}
		}
		for it.Valid() && bytes.Equal(it.Item().Key(), key) {
			it.Next()
		}
	}
	return it.Error()
}

// resetRaftPostingCaches runs with the same complete request/apply exclusion as
// the baseline pass. Pending transaction caches can outlive individual tasks:
// merely clearing the global memory layer leaves old split anchors reachable.
// Use the maintained cache transition to preserve uncommitted deltas and
// conflict versions while discarding only materialized posting-list objects.
func resetRaftPostingCaches() {
	o := Oracle()
	o.RLock()
	for _, txn := range o.pendingTxns {
		txn.Update()
	}
	o.RUnlock()
	if MemLayerInstance != nil {
		ResetCache()
	}
}
