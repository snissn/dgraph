/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"unsafe"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgraph/v25/x"
)

// One producer-owned chunk. Completion is drained before another chunk is built.
// Oversized single entries go alone to the backend's original admission rule.
const rollupChunkKVs = 64
const rollupChunkBytes = 4 << 20

// Reserve metadata headroom even when a small chunk reuses a 64-group batch.
const rollupChunkPayloadBytes = rollupChunkBytes - rollupChunkKVs*(treeDBCommitGroupBytes+int(unsafe.Sizeof(error(nil)))+int(unsafe.Sizeof(CommitEvent{})))

type rollupBulkStore interface{ writeRollupBatch([]*bpb.KV, uint64) error }
type rollupRootBoundary interface{ rollupRootEpoch() uint64 }
type rollupResultStore interface {
	writeRollupBatchResults([]*bpb.KV, uint64, []error) error
}
type rollupEventStore interface{ rollupEvents() *CommitEventBus }

type rollupCreationBoundary interface {
	rollupEntryTarget() rollupBulkStore
}

type rollupWriter struct {
	store      Store
	pending    [rollupChunkKVs]*bpb.KV
	n, bytes   int
	target     rollupBulkStore
	rootEpoch  uint64
	events     [rollupChunkKVs]CommitEvent
	results    [rollupChunkKVs]error
	bus        *CommitEventBus
	fallback   *TxnWriter
	readEpoch  uint64
	epochBound bool
}

func (w *rollupWriter) Flush() error {
	if w.n == 0 {
		if w.fallback != nil {
			return w.fallback.Flush()
		}
		return nil
	}
	rollupSubmitted(w.pending[:w.n])
	var err error
	if target, ok := w.target.(rollupResultStore); ok {
		err = target.writeRollupBatchResults(w.pending[:w.n], w.rootEpoch, w.results[:w.n])
	} else {
		err = w.target.writeRollupBatch(w.pending[:w.n], w.rootEpoch)
		for i := range w.n {
			w.results[i] = err
		}
	}
	for i := range w.n {
		rollupCompleted(1, rollupBytes(w.pending[i:i+1]), w.results[i])
		if w.events[i].admitted {
			w.bus.complete(commitCompletion{event: w.events[i], err: w.results[i]})
		}
	}
	clear(w.pending[:w.n])
	clear(w.events[:w.n])
	clear(w.results[:w.n])
	w.n, w.bytes, w.target, w.bus = 0, 0, nil, nil
	return err // Never retry a submitted or ambiguous prefix.
}

// Reads of a root or any of its split parts must not overtake an earlier
// buffered write in the same physical family. No unrelated key forces a cut.
func (w *rollupWriter) beforeRead(key []byte) error {
	family := func(k []byte) []byte {
		if len(k) == 0 {
			return k
		}
		if k[0] == x.ByteSplit && len(k) > 8 {
			return k[1 : len(k)-8]
		}
		return k[1:]
	}
	for _, kv := range w.pending[:w.n] {
		if bytes.Equal(family(key), family(kv.Key)) {
			rollupWork.readDependencyCuts.Add(1)
			return w.Flush()
		}
	}
	return nil
}

// bindReadEpoch is called before GetNoStore, never after materialization. A root
// installed during the read must refuse the resulting old-root output.
func (w *rollupWriter) bindReadEpoch() {
	target, _ := w.store.(rollupBulkStore)
	if boundary, ok := w.store.(rollupCreationBoundary); ok {
		target = boundary.rollupEntryTarget()
	}
	w.epochBound = false
	if root, ok := target.(rollupRootBoundary); ok {
		w.readEpoch, w.epochBound = root.rollupRootEpoch(), true
	}
}

func (w *rollupWriter) native() bool {
	if _, ok := w.store.(rollupBulkStore); ok {
		return true
	}
	if boundary, ok := w.store.(rollupCreationBoundary); ok {
		target := boundary.rollupEntryTarget()
		return target != nil
	}
	return false
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
		if boundary, ok := w.store.(rollupCreationBoundary); ok {
			target = boundary.rollupEntryTarget()
		}
		if target == nil {
			// Restore the maintained asynchronous scalar owner (including Badger).
			// No additional per-entry callback or counter closure on this fallback.
			if w.n > 0 {
				if err := w.Flush(); err != nil {
					return err
				}
			}
			if w.fallback == nil {
				w.fallback = NewTxnWriterForStore(w.store)
			}
			if err := w.fallback.Write(&bpb.KVList{Kv: []*bpb.KV{kv}}); err != nil {
				return err
			}
			continue
		}
		var bus *CommitEventBus
		if wrapped, ok := w.store.(rollupEventStore); ok {
			bus = wrapped.rollupEvents()
		}
		instrument := bus != nil && bus.subscribers.Load() != 0
		epoch := uint64(0)
		if boundary, ok := target.(rollupRootBoundary); ok {
			epoch = boundary.rollupRootEpoch()
			if w.epochBound {
				epoch = w.readEpoch
			}
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

		// Charge storage and the independent event-owned payload against one bound.
		ownedSize := size
		if instrument {
			ownedSize += len(kv.Key) + len(kv.Value) + int(unsafe.Sizeof(CommitMutation{}))
		}
		if w.n > 0 && (w.n == rollupChunkKVs || ownedSize > rollupChunkPayloadBytes-w.bytes || duplicate) {
			if err := w.Flush(); err != nil {
				return err
			}
		}
		var event CommitEvent
		if instrument {
			entry := rollupKVEntry(kv)
			entry.Key = append([]byte(nil), entry.Key...)
			entry.Value = append([]byte(nil), entry.Value...)
			mutations := []CommitMutation{{Entry: entry}}
			var admitted bool
			event, admitted = bus.tryAdmit(kv.Version, mutations)
			if !admitted {
				rollupWork.eventPressureCuts.Add(1)
				if err := w.Flush(); err != nil {
					return err
				}
				event = bus.admit(kv.Version, mutations)
			}
		}
		w.events[w.n] = event
		w.bus = bus
		w.target = target
		w.rootEpoch = epoch
		w.pending[w.n] = kv
		w.n++
		w.bytes += ownedSize
		if w.n == rollupChunkKVs || w.bytes >= rollupChunkPayloadBytes {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}
