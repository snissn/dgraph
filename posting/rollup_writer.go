/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	bpb "github.com/dgraph-io/badger/v4/pb"
)

// One producer-owned chunk. Completion is drained before another chunk is built.
// Oversized single entries go alone to the backend's original admission rule.
const rollupChunkKVs = 64
const rollupChunkBytes = 4 << 20

// Reserve metadata headroom even when a small chunk reuses a 64-group batch.
const rollupChunkPayloadBytes = rollupChunkBytes - rollupChunkKVs*treeDBCommitGroupBytes

type rollupBulkStore interface{ writeRollupBatch([]*bpb.KV, uint64) error }
type rollupRootBoundary interface{ rollupRootEpoch() uint64 }

type rollupCreationBoundary interface {
	rollupEntryTarget() (rollupBulkStore, WriteTxn)
}

type rollupWriter struct {
	store     Store
	pending   [rollupChunkKVs]*bpb.KV
	n, bytes  int
	target    rollupBulkStore
	rootEpoch uint64
}

func (w *rollupWriter) Flush() error {
	if w.n == 0 {
		return nil
	}
	rollupSubmitted(w.pending[:w.n])
	err := w.target.writeRollupBatch(w.pending[:w.n], w.rootEpoch)
	rollupCompleted(uint64(w.n), rollupBytes(w.pending[:w.n]), err)
	clear(w.pending[:w.n])
	w.n, w.bytes = 0, 0
	w.target = nil
	return err // No scalar retry after any native admission.
}

func rollupKVEntry(kv *bpb.KV) Entry {
	var meta byte
	if len(kv.UserMeta) > 0 {
		meta = kv.UserMeta[0]
	}
	return Entry{Key: kv.Key, Value: kv.Value, UserMeta: meta, DiscardEarlierVersions: meta == BitCompletePosting || meta == BitEmptyPosting}
}

func (w *rollupWriter) Write(kvs *bpb.KVList) error {
	for _, kv := range kvs.Kv {
		if kv.Version == 0 {
			continue
		} // TxnWriter.SetAt's existing no-op.
		size := len(kv.Key) + len(kv.Value) + treeDBEnvelopeHeader
		target, _ := w.store.(rollupBulkStore)
		var txn WriteTxn
		if boundary, ok := w.store.(rollupCreationBoundary); ok {
			target, txn = boundary.rollupEntryTarget()
		}
		if target == nil {
			// Capture instrumentation before draining, exactly at logical txn creation.
			if txn == nil {
				txn = w.store.NewWriteTxn()
			}
			err := w.Flush()
			submitted := err == nil
			if err == nil {
				rollupSubmitted([]*bpb.KV{kv})
				err = txn.SetEntry(rollupKVEntry(kv))
				if err == badger.ErrTxnTooBig {
					// Match the original TxnWriter.update scalar prefix rule.
					err = nil
				}
			}
			if err == nil {
				err = txn.CommitAt(kv.Version, nil)
			}
			if submitted {
				rollupCompleted(1, rollupBytes([]*bpb.KV{kv}), err)
			}
			txn.Discard()
			if err != nil {
				return err
			}
			continue
		}
		epoch := uint64(0)
		if boundary, ok := target.(rollupRootBoundary); ok {
			epoch = boundary.rollupRootEpoch()
		}
		if w.n > 0 && epoch != w.rootEpoch {
			if err := w.Flush(); err != nil {
				return err
			}
		}
		duplicate := false
		for _, prior := range w.pending[:w.n] {
			if prior.Version == kv.Version && bytes.Equal(prior.Key, kv.Key) {
				duplicate = true
				break
			}
		}
		if w.n > 0 && (w.n == rollupChunkKVs || size > rollupChunkPayloadBytes-w.bytes || duplicate) {
			if err := w.Flush(); err != nil {
				return err
			}
		}
		w.target = target
		w.rootEpoch = epoch
		w.pending[w.n] = kv
		w.n++
		w.bytes += size
		if w.n == rollupChunkKVs || w.bytes >= rollupChunkPayloadBytes {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Native managed batches retain per-entry versions and discard metadata. Badger
// may have committed a prefix on error; Cancel drains it and no retry is allowed.
func (s BadgerStore) writeRollupBatch(kvs []*bpb.KV, _ uint64) error {
	rollupWork.nativeChunks.Add(1)
	wb := s.db.NewManagedWriteBatch()
	wb.SetMaxPendingTxns(1)
	defer wb.Cancel()
	for _, kv := range kvs {
		e := rollupKVEntry(kv)
		be := &badger.Entry{Key: e.Key, Value: e.Value, UserMeta: e.UserMeta}
		if e.DiscardEarlierVersions {
			be = be.WithDiscard()
		}
		if err := wb.SetEntryAt(be, kv.Version); err != nil {
			return fmt.Errorf("rollup Badger batch: %w", err)
		}
	}
	err := wb.Flush()
	if err == nil {
		rollupWork.nativeCompleted.Add(1)
	}
	return err
}
