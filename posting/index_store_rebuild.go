/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/dgraph/v25/x"
)

func (rb *IndexRebuild) sourceStore() Store {
	if rb.Store != nil {
		return rb.Store
	}
	return postingStore
}

func (lc *LocalCache) sourceStore() Store {
	if lc.privateStore != nil {
		return lc.privateStore
	}
	return postingStore
}

func (lc *LocalCache) loadList(key []byte, readUids bool) (*List, error) {
	if lc.privateStore == nil {
		return getNewForStore(key, postingStore, lc.startTs, readUids)
	}
	store := lc.privateStore
	txn := store.NewReadTxn(lc.startTs)
	defer txn.Discard()
	it := txn.NewKeyIterator(key, IteratorOptions{AllVersions: true})
	defer it.Close()
	it.Seek(key)
	list, err := readPostingListForStore(key, it, store)
	if err != nil {
		return nil, err
	}
	list.mutationMap.setTs(lc.startTs)
	if readUids {
		if err := list.calculateUids(); err != nil {
			return nil, err
		}
	}
	return list, nil
}

// walkStoreLists retains one read snapshot for the entire traversal. A list
// consumes multiple versions itself; skipping the remainder here must not skip
// the following key. Split parts are accessed through their owning main list.
func walkStoreLists(ctx context.Context, store Store, ts uint64, prefix []byte, fn func([]byte, *List) error) error {
	txn := store.NewReadTxn(ts)
	defer txn.Discard()
	it := txn.NewIterator(IteratorOptions{Prefix: prefix, AllVersions: true})
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Item().KeyCopy(nil)
		list, err := readPostingListForStore(key, it, store)
		if err != nil && !errors.Is(err, ErrInvalidKey) {
			return err
		}
		if err == nil {
			if err := fn(key, list); err != nil {
				return err
			}
		}
		for it.Valid() && bytes.Equal(it.Item().Key(), key) {
			it.Next()
		}
	}
	return it.Error()
}

// Logical empty lists replace physical prefix deletion. Versions below ts stay
// available to timestamped readers and retained checkpoint roots. The caller
// owns the schema/apply barrier; this function never advances a discard floor.
func dropStoreIndexPrefixesForStore(ctx context.Context, store Store, ts uint64, prefixes [][]byte) (retErr error) {
	if ts == 0 {
		return nil
	}
	writer := NewTxnWriterForStore(store)
	defer func() { retErr = errors.Join(retErr, writer.Flush()) }()
	for _, prefix := range prefixes {
		if err := func() error {
			txn := store.NewReadTxn(ts)
			defer txn.Discard()
			it := txn.NewIterator(IteratorOptions{Prefix: prefix})
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				key := it.Item().KeyCopy(nil)
				RemoveCacheFor(key)
				if err := writer.SetAt(key, nil, BitEmptyPosting, ts); err != nil {
					return err
				}
			}
			return it.Error()
		}(); err != nil {
			return err
		}
	}
	return nil
}

// runForStore reuses the maintained per-list rebuild and rollup algorithms.
// The temporary Badger only aggregates independently generated deltas at unique
// scratch timestamps. It is ephemeral working space, never recovery authority:
// recovery reruns this whole synchronous operation from certified data and the
// retained schema proposal. No scratch path is recorded in a certificate.
func (r *rebuilder) runForStore(ctx context.Context) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.startTs == 0 {
		return nil
	}
	store := r.store
	if store == nil {
		store = postingStore
	}
	tmp, cleanup, err := openStoreRebuildScratch()
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, cleanup()) }()
	batch := tmp.NewManagedWriteBatch()
	defer batch.Cancel()
	newTxn := func() *Txn {
		txn := NewTxn(r.startTs)
		txn.cache.privateStore = store
		return txn
	}
	txn := newTxn()
	var counter uint64 = 1
	flushChunk := func() error {
		txn.Update()
		for key, data := range txn.cache.deltas {
			counter++
			if err := batch.SetEntryAt(&badger.Entry{Key: []byte(key), Value: data, UserMeta: BitDeltaPosting}, counter); err != nil {
				return err
			}
		}
		txn = newTxn()
		return nil
	}
	count := 0
	if err := walkStoreLists(ctx, store, r.startTs, r.prefix, func(key []byte, list *List) error {
		pk, err := x.Parse(key)
		if err != nil {
			return err
		}
		if _, err := r.fn(pk.Uid, list, txn); err != nil {
			return err
		}
		count++
		if count%256 == 0 {
			return flushChunk()
		}
		return nil
	}); err != nil {
		return err
	}
	if err := flushChunk(); err != nil {
		return err
	}
	if err := batch.Flush(); err != nil {
		return err
	}
	writer := NewTxnWriterForStore(store)
	defer func() { retErr = errors.Join(retErr, writer.Flush()) }()
	return walkStoreLists(ctx, NewBadgerStore(tmp), counter, nil, func(key []byte, list *List) error {
		kvs, err := list.Rollup(nil, math.MaxUint64)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			meta := BitCompletePosting
			if len(kv.Value) == 0 {
				meta = BitEmptyPosting
			}
			RemoveCacheFor(kv.Key)
			if err := writer.SetAt(kv.Key, kv.Value, meta, r.startTs); err != nil {
				return err
			}
		}
		return nil
	})
}

// openStoreRebuildScratch bounds in-memory aggregation while allowing a predicate-sized
// disposable working image. It is never referenced by a recovery certificate.
func openStoreRebuildScratch() (*badger.DB, func() error, error) {
	dir, err := os.MkdirTemp(x.WorkerConfig.TmpDir, "dgraph_store_index_")
	if err != nil {
		return nil, nil, err
	}
	opts := badger.DefaultOptions(dir).WithSyncWrites(false).
		WithNumVersionsToKeep(math.MaxInt32).WithCompression(options.None).
		WithLogger(&x.ToGlog{}).WithLoggingLevel(badger.WARNING).
		WithMetricsEnabled(false).WithDetectConflicts(false).
		WithMemTableSize(16 << 20).WithBaseTableSize(2 << 20).WithBaseLevelSize(8 << 20)
	if len(x.WorkerConfig.EncryptionKey) > 0 {
		opts.EncryptionKey = x.WorkerConfig.EncryptionKey
		opts.BlockCacheSize = 16 << 20
	}
	tmp, err := badger.OpenManaged(opts)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	return tmp, func() error { return errors.Join(tmp.Close(), os.RemoveAll(dir)) }, nil
}
