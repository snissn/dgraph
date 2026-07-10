/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"fmt"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

var benchmarkStoreSink byte

func BenchmarkBadgerStoreSeam(b *testing.B) {
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("posting/store-bench/%03d", i))
	}

	b.Run("point-read", func(b *testing.B) {
		b.Run("direct", func(b *testing.B) {
			db := openPopulatedBenchmarkDB(b, keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(math.MaxUint64, false)
				item, err := txn.Get(keys[i%len(keys)])
				if err != nil {
					b.Fatal(err)
				}
				benchmarkStoreSink = item.UserMeta()
				txn.Discard()
			}
		})
		b.Run("seam", func(b *testing.B) {
			store := NewBadgerStore(openPopulatedBenchmarkDB(b, keys))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := store.NewReadTxn(math.MaxUint64)
				item, err := txn.Get(keys[i%len(keys)])
				if err != nil {
					b.Fatal(err)
				}
				benchmarkStoreSink = item.UserMeta()
				txn.Discard()
			}
		})
	})

	b.Run("bounded-iterator", func(b *testing.B) {
		prefix := []byte("posting/store-bench/")
		b.Run("direct", func(b *testing.B) {
			db := openPopulatedBenchmarkDB(b, keys)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(math.MaxUint64, false)
				it := txn.NewIterator(opts)
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					benchmarkStoreSink = it.Item().UserMeta()
				}
				it.Close()
				txn.Discard()
			}
		})
		b.Run("seam", func(b *testing.B) {
			store := NewBadgerStore(openPopulatedBenchmarkDB(b, keys))
			opts := IteratorOptions{Prefix: prefix, PrefetchValues: false}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := store.NewReadTxn(math.MaxUint64)
				it := txn.NewIterator(opts)
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					benchmarkStoreSink = it.Item().UserMeta()
				}
				it.Close()
				txn.Discard()
			}
		})
	})

	b.Run("exact-key-all-versions", func(b *testing.B) {
		key := keys[len(keys)/2]
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false
		b.Run("direct", func(b *testing.B) {
			db := openPopulatedBenchmarkDB(b, keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(math.MaxUint64, false)
				it := txn.NewKeyIterator(key, opts)
				for it.Seek(key); it.Valid(); it.Next() {
					benchmarkStoreSink = it.Item().UserMeta()
				}
				it.Close()
				txn.Discard()
			}
		})
		b.Run("seam", func(b *testing.B) {
			store := NewBadgerStore(openPopulatedBenchmarkDB(b, keys))
			storeOpts := IteratorOptions{AllVersions: true, PrefetchValues: false}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := store.NewReadTxn(math.MaxUint64)
				it := txn.NewKeyIterator(key, storeOpts)
				for it.Seek(key); it.Valid(); it.Next() {
					benchmarkStoreSink = it.Item().UserMeta()
				}
				it.Close()
				txn.Discard()
			}
		})
	})

	b.Run("write", func(b *testing.B) {
		value := []byte("benchmark-value")
		b.Run("direct", func(b *testing.B) {
			db := openStoreBenchmarkDB(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(math.MaxUint64, true)
				if err := txn.SetEntry(&badger.Entry{Key: keys[i%len(keys)], Value: value}); err != nil {
					b.Fatal(err)
				}
				if err := txn.CommitAt(uint64(i+1), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("seam", func(b *testing.B) {
			store := NewBadgerStore(openStoreBenchmarkDB(b))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := store.NewWriteTxn()
				if err := txn.SetEntry(Entry{Key: keys[i%len(keys)], Value: value}); err != nil {
					b.Fatal(err)
				}
				if err := txn.CommitAt(uint64(i+1), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("batch-write", func(b *testing.B) {
		value := []byte("benchmark-value")
		b.Run("direct", func(b *testing.B) {
			db := openStoreBenchmarkDB(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(math.MaxUint64, true)
				for _, key := range keys[:16] {
					if err := txn.SetEntry(&badger.Entry{Key: key, Value: value}); err != nil {
						b.Fatal(err)
					}
				}
				if err := txn.CommitAt(uint64(i+1), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("seam", func(b *testing.B) {
			store := NewBadgerStore(openStoreBenchmarkDB(b))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := store.NewWriteTxn()
				for _, key := range keys[:16] {
					if err := txn.SetEntry(Entry{Key: key, Value: value}); err != nil {
						b.Fatal(err)
					}
				}
				if err := txn.CommitAt(uint64(i+1), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func openPopulatedBenchmarkDB(b *testing.B, keys [][]byte) *badger.DB {
	b.Helper()
	db := openStoreBenchmarkDB(b)
	txn := db.NewTransactionAt(math.MaxUint64, true)
	for _, key := range keys {
		entry := &badger.Entry{Key: key, Value: []byte("benchmark-value"), UserMeta: BitDeltaPosting}
		if err := txn.SetEntry(entry); err != nil {
			b.Fatal(err)
		}
	}
	if err := txn.CommitAt(1, nil); err != nil {
		b.Fatal(err)
	}
	for version := uint64(2); version <= 4; version++ {
		txn = db.NewTransactionAt(math.MaxUint64, true)
		for _, key := range keys {
			entry := &badger.Entry{Key: key, Value: []byte("benchmark-value"), UserMeta: BitDeltaPosting}
			if err := txn.SetEntry(entry); err != nil {
				b.Fatal(err)
			}
		}
		if err := txn.CommitAt(version, nil); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

func openStoreBenchmarkDB(b *testing.B) *badger.DB {
	b.Helper()
	opts := badger.DefaultOptions(b.TempDir()).
		WithLogger(nil).
		WithSyncWrites(false).
		WithNumVersionsToKeep(math.MaxInt64)
	db, err := badger.OpenManaged(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	return db
}
