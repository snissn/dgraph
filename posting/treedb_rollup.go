/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"fmt"
	"unsafe"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

func (r *treeDBCommitRequest) ownedBytes() int {
	return r.batch.ownedBytes() + len(r.groupResults)*int(unsafe.Sizeof(error(nil)))
}
func (r *treeDBCommitRequest) setGroupResults(results []error) {
	copy(r.groupResults, results)
	r.groupResultsKnown = true
}
func (r *treeDBCommitRequest) groupCount() int {
	if len(r.batch.groups) > 0 {
		return len(r.batch.groups)
	}
	return 1
}
func (r *treeDBCommitRequest) copyGroups(dst []mvcc.CommitGroup, start int) int {
	if len(r.batch.groups) > 0 {
		return start + copy(dst[start:], r.batch.groups)
	}
	dst[start] = mvcc.CommitGroup{Timestamp: r.commitTs, Mutations: r.batch.mutations}
	return start + 1
}
func (s *TreeDBStore) commitRequestAdmitted(r *treeDBCommitRequest) error {
	if len(r.batch.groups) == 0 {
		return s.commitAtAdmitted(r.commitTs, r.batch.mutations)
	}
	results := r.groupResults
	var local [rollupChunkKVs]error
	if len(results) != len(r.batch.groups) {
		results = local[:len(r.batch.groups)]
	}
	apply := func() error {
		s.commitDurableGroups(r.batch.groups, results)
		r.setGroupResults(results)
		for _, err := range results {
			if err != nil {
				return err
			}
		}
		return nil
	}
	if s.external != nil {
		return s.external.Apply(apply)
	}
	return apply()
}

// Use the original FIFO admission, root epoch, slots, owned arena, shared stage
// budget and completion owner. A rollup is one bounded request with explicit
// versions, not direct producer access to the engine or a second scheduler.
func (s *TreeDBStore) rollupRootEpoch() uint64 { return s.rootEpoch.Load() }

func (s *TreeDBStore) writeRollupBatch(kvs []*bpb.KV, epoch uint64) error {
	var results [rollupChunkKVs]error
	if len(kvs) > len(results) {
		return fmt.Errorf("rollup chunk exceeds %d KVs", rollupChunkKVs)
	}
	return s.writeRollupBatchResults(kvs, epoch, results[:len(kvs)])
}

func (s *TreeDBStore) writeRollupBatchResults(kvs []*bpb.KV, epoch uint64, results []error) (err error) {
	// Until a part returns, all its entries are conservatively unconfirmed.
	for i := range results {
		results[i] = ErrTreeDBTxnClosed
	}
	defer func() {
		if err != nil {
			for i := range results {
				if results[i] == ErrTreeDBTxnClosed {
					results[i] = err
				}
			}
		}
	}()
	if len(kvs) == 0 {
		return nil
	}
	if len(kvs) > rollupChunkKVs {
		return fmt.Errorf("rollup chunk exceeds %d KVs", rollupChunkKVs)
	}
	// Shape native work before admission. A work-limit refusal splits only an
	// unsubmitted suffix; completed/ambiguous storage is never retried.
	for start := 0; start < len(kvs); {
		end := start
		var work uint64
		for end < len(kvs) {
			cost := s.writeWorkEstimator.Estimate(len(kvs[end].Key), treeDBEnvelopeHeader+len(kvs[end].Value))
			if s.writeWorkLimit != 0 && (cost > s.writeWorkLimit || work > s.writeWorkLimit-cost) {
				break
			}
			work += cost
			end++
		}
		if end == start {
			return ErrTreeDBEntryTooLarge
		}
		if err := s.writeRollupPart(kvs[start:end], epoch, results[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (s *TreeDBStore) writeRollupPart(kvs []*bpb.KV, epoch uint64, results []error) error {
	txn := s.NewWriteTxn().(*treeDBWriteTxn)
	txn.rootEpoch = epoch
	defer txn.Discard()
	// Preallocate once so every one-mutation group keeps a stable slice.
	if cap(txn.batch.mutations) < len(kvs) {
		txn.batch.mutations = make([]mvcc.Mutation, 0, len(kvs))
	}
	if cap(txn.batch.groups) < len(kvs) {
		txn.batch.groups = make([]mvcc.CommitGroup, 0, len(kvs))
	}
	bytes := 0
	for _, kv := range kvs {
		e := rollupKVEntry(kv)
		bytes += len(e.Key) + treeDBEnvelopeHeader + len(e.Value)
		if bytes > rollupChunkBytes && len(kvs) > 1 {
			return fmt.Errorf("rollup chunk exceeds byte bound")
		}
		if e.ExpiresAt != 0 {
			return ErrTreeDBTTLUnsupported
		}
		if err := txn.admitEntryWork(len(e.Key), treeDBEnvelopeHeader+len(e.Value)); err != nil {
			return err
		}
		owned := txn.batch.ownBytes(len(e.Key) + treeDBEnvelopeHeader + len(e.Value))
		key := owned[:len(e.Key)]
		copy(key, e.Key)
		value := owned[len(e.Key):]
		encodeTreeDBEnvelopeInto(value, e)
		i := len(txn.batch.mutations)
		txn.batch.mutations = append(txn.batch.mutations, mvcc.Mutation{Key: key, Value: value})
		txn.batch.groups = append(txn.batch.groups, mvcc.CommitGroup{Timestamp: kv.Version, Mutations: txn.batch.mutations[i : i+1 : i+1]})
	}
	batch, err := txn.prepareCommit()
	if err != nil {
		s.releaseMutationBatch(batch)
		return err
	}
	// Always enter the existing queue, including relaxed/external serial mode.
	done := make(chan error, 1)

	clear(results)
	err = s.enqueueCommit(treeDBCommitRequest{groupResults: results, commitTs: kvs[0].Version, batch: batch, done: done, rootEpoch: txn.rootEpoch})
	if err != nil {
		for i := range results {
			results[i] = err
		}
		s.releaseMutationBatch(batch)
		return err
	}
	rollupWork.nativeChunks.Add(1)
	rollupWork.nativeKVs.Add(uint64(len(kvs)))
	s.SealCommitWindow()
	err = <-done
	for _, result := range results {
		if result == nil {
			rollupWork.nativeCompletedKVs.Add(1)
		}
	}
	if err == nil {
		rollupWork.nativeCompleted.Add(1)
	}
	return err
}
