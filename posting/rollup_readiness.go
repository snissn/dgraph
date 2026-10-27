/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"sync/atomic"
	"time"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/golang/glog"
)

// Only the ordinary durable TreeDB route has the bounded scalar cohort owner.
// Unknown decorators deliberately retain the original producer. Subscription
// presence never changes eligibility or event creation/publication semantics.
func rollupReadinessStore(store Store) bool {
	switch s := store.(type) {
	case *TreeDBStore:
		return s != nil && s.mode == TreeDBCommitDurable && s.stagedCommitGroups
	case eventStore:
		return rollupReadinessStore(s.Store)
	case batchEventStore:
		return rollupReadinessStore(s.Store)
	default:
		return false
	}
}

// processReadiness retains the original received-window selection and timers.
// It changes only when that window's scalar writes become available to TreeDB.
func (ir *incrRollupi) processReadiness(closer *z.Closer, getNewTs func(bool) uint64) {
	ir.getNewTs = getNewTs
	ir.closer = closer

	defer closer.Done()

	writer := NewTxnWriterForStore(postingStore)
	defer writer.Flush()

	m := make(map[uint64]int64) // map hash(key) to ts. hash(key) to limit the size of the map.
	limiter := time.Tick(time.Millisecond)
	cleanupTick := time.Tick(5 * time.Minute)
	forceRollupTick := time.Tick(500 * time.Millisecond)

	doRollup := func(batch *[][]byte, priority int) bool {
		defer func() {
			*batch = (*batch)[:0]
			ir.priorityKeys[priority].keysPool.Put(batch)
		}()
		if err := ir.rollUpReadyBatch(writer, *batch, m, time.Now().Unix()); err != nil {
			return false // Cancellation abandons only unsubmitted maintenance.
		}
		return true
	}

	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case <-cleanupTick:
			currTs := time.Now().Unix()
			for hash, ts := range m {
				// Remove entries from map which have been there for there more than 10 seconds.
				if currTs-ts >= 10 {
					delete(m, hash)
				}
			}
		case <-forceRollupTick:
			batch := ir.priorityKeys[0].keysPool.Get().(*[][]byte)
			if len(*batch) > 0 {
				if !doRollup(batch, 0) {
					return
				}
			} else {
				ir.priorityKeys[0].keysPool.Put(batch)
			}
		case batch := <-ir.priorityKeys[0].keysCh:
			if !doRollup(batch, 0) {
				return
			}
			// We don't need a limiter here as we don't expect to call this function frequently.
		case batch := <-ir.priorityKeys[1].keysCh:
			if !doRollup(batch, 1) {
				return
			}
			// throttle to 1 batch = 16 rollups per 1 ms.
			<-limiter
		}
	}
}

// rollUpReadyBatch retains only descriptors while obtaining each original unique
// timestamp and waiting for its complete Oracle readiness. No List, payload,
// transaction, event reservation or storage admission spans this phase.
func (ir *incrRollupi) rollUpReadyBatch(writer *TxnWriter, keys [][]byte, last map[uint64]int64, now int64) error {
	const window = 16
	type readyKey struct {
		key []byte
		ts  uint64
	}
	// Public received batches contain at most 16 keys. Segment defensively if an
	// internal caller supplies more, preserving the same fixed descriptor bound.
	for len(keys) != 0 {
		var ready [window]readyKey
		n := 0
		end := min(len(keys), window)
		for _, key := range keys[:end] {
			select {
			case <-ir.closer.HasBeenClosed():
				return ErrHighPriorityOp
			default:
			}
			hash := z.MemHash(key)
			if now-last[hash] < 10 {
				continue
			}
			last[hash] = now
			ts := ir.getNewTs(false)
			waitCh, wait := o.addToWaiters(ts)
			if wait {
				select {
				case <-ir.closer.HasBeenClosed():
					return ErrHighPriorityOp
				case <-waitCh:
				}
			}
			ready[n] = readyKey{key: key, ts: ts}
			n++
		}
		for _, r := range ready[:n] {
			select {
			case <-ir.closer.HasBeenClosed():
				return ErrHighPriorityOp
			default:
			}
			if err := ir.rollUpReadyKey(writer, r.key, r.ts); err != nil {
				glog.Warningf("Error rolling up key [%v]: %v", err, r.key)
			}
		}
		keys = keys[end:]
	}
	return nil
}

// Materialization and writes remain in key order through the ordinary scalar
// TxnWriter. It owns the original per-KV callbacks and final completion drain.
func (ir *incrRollupi) rollUpReadyKey(writer *TxnWriter, key []byte, ts uint64) error {
	l, err := GetNoStore(key, ts)
	if err != nil {
		return err
	}
	kvs, err := l.Rollup(nil, ts)
	if err != nil {
		return err
	}
	RemoveCacheFor(key)
	const N = uint64(1000)
	if glog.V(2) {
		if count := atomic.AddUint64(&ir.count, 1); count%N == 0 {
			glog.V(2).Infof("Rolled up %d keys", count)
		}
	}
	return writer.Write(&bpb.KVList{Kv: kvs})
}
