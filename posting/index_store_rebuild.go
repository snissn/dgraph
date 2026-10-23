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
		list, err := readPostingList(key, it)
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
func dropStoreIndexPrefixes(ctx context.Context, ts uint64, prefixes [][]byte) (retErr error) {
	if ts == 0 {
		return nil
	}
	writer := NewTxnWriterForStore(postingStore)
	defer func() { retErr = errors.Join(retErr, writer.Flush()) }()
	for _, prefix := range prefixes {
		if err := func() error {
			txn := postingStore.NewReadTxn(ts)
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
	dir, err := os.MkdirTemp(x.WorkerConfig.TmpDir, "dgraph_store_index_")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
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
		return err
	}
	defer func() { retErr = errors.Join(retErr, tmp.Close()) }()
	batch := tmp.NewManagedWriteBatch()
	defer batch.Cancel()
	txn := NewTxn(r.startTs)
	var counter uint64 = 1
	flushChunk := func() error {
		txn.Update()
		for key, data := range txn.cache.deltas {
			counter++
			if err := batch.SetEntryAt(&badger.Entry{Key: []byte(key), Value: data, UserMeta: BitDeltaPosting}, counter); err != nil {
				return err
			}
		}
		txn = NewTxn(r.startTs)
		return nil
	}
	count := 0
	if err := walkStoreLists(ctx, postingStore, r.startTs, r.prefix, func(key []byte, list *List) error {
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
	writer := NewTxnWriterForStore(postingStore)
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
