/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"fmt"
	"math"
	"testing"
	"time"

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

// BenchmarkBadgerStoreSeamInterleaved is the comparison benchmark for the
// Badger seam overhead gate. BenchmarkBadgerStoreSeam keeps conventional
// per-mode allocation rows, but -count runs all direct samples before all seam
// samples. Here both modes share one database and their execution order swaps
// after every operation, producing same-process, same-state, same-time-span ratios.
func BenchmarkBadgerStoreSeamInterleaved(b *testing.B) {
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("posting/store-interleaved/%03d", i))
	}
	value := []byte("benchmark-value")

	b.Run("point-read", func(b *testing.B) {
		direct := openPopulatedBenchmarkDB(b, keys)
		seam := NewBadgerStore(direct)
		var elapsed [2]time.Duration
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for offset := 0; offset < len(elapsed); offset++ {
				mode := (i + offset) % len(elapsed)
				start := time.Now()
				if mode == 0 {
					txn := direct.NewTransactionAt(math.MaxUint64, false)
					item, err := txn.Get(keys[i%len(keys)])
					if err != nil {
						b.Fatal(err)
					}
					benchmarkStoreSink = item.UserMeta()
					txn.Discard()
				} else {
					txn := seam.NewReadTxn(math.MaxUint64)
					item, err := txn.Get(keys[i%len(keys)])
					if err != nil {
						b.Fatal(err)
					}
					benchmarkStoreSink = item.UserMeta()
					txn.Discard()
				}
				elapsed[mode] += time.Since(start)
			}
		}
		b.StopTimer()
		reportBadgerStoreSeamInterleaved(b, elapsed)
	})

	b.Run("write", func(b *testing.B) {
		direct := openStoreBenchmarkDB(b)
		seam := NewBadgerStore(direct)
		var elapsed [2]time.Duration
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for offset := 0; offset < len(elapsed); offset++ {
				mode := (i + offset) % len(elapsed)
				commitTs := uint64(i*len(elapsed) + offset + 1)
				start := time.Now()
				if mode == 0 {
					txn := direct.NewTransactionAt(math.MaxUint64, true)
					if err := txn.SetEntry(&badger.Entry{Key: keys[i%len(keys)], Value: value}); err != nil {
						b.Fatal(err)
					}
					if err := txn.CommitAt(commitTs, nil); err != nil {
						b.Fatal(err)
					}
				} else {
					txn := seam.NewWriteTxn()
					if err := txn.SetEntry(Entry{Key: keys[i%len(keys)], Value: value}); err != nil {
						b.Fatal(err)
					}
					if err := txn.CommitAt(commitTs, nil); err != nil {
						b.Fatal(err)
					}
				}
				elapsed[mode] += time.Since(start)
			}
		}
		b.StopTimer()
		reportBadgerStoreSeamInterleaved(b, elapsed)
	})

	b.Run("batch-write", func(b *testing.B) {
		direct := openStoreBenchmarkDB(b)
		seam := NewBadgerStore(direct)
		var elapsed [2]time.Duration
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for offset := 0; offset < len(elapsed); offset++ {
				mode := (i + offset) % len(elapsed)
				commitTs := uint64(i*len(elapsed) + offset + 1)
				start := time.Now()
				if mode == 0 {
					txn := direct.NewTransactionAt(math.MaxUint64, true)
					for _, key := range keys[:16] {
						if err := txn.SetEntry(&badger.Entry{Key: key, Value: value}); err != nil {
							b.Fatal(err)
						}
					}
					if err := txn.CommitAt(commitTs, nil); err != nil {
						b.Fatal(err)
					}
				} else {
					txn := seam.NewWriteTxn()
					for _, key := range keys[:16] {
						if err := txn.SetEntry(Entry{Key: key, Value: value}); err != nil {
							b.Fatal(err)
						}
					}
					if err := txn.CommitAt(commitTs, nil); err != nil {
						b.Fatal(err)
					}
				}
				elapsed[mode] += time.Since(start)
			}
		}
		b.StopTimer()
		reportBadgerStoreSeamInterleaved(b, elapsed)
	})
}

func reportBadgerStoreSeamInterleaved(b *testing.B, elapsed [2]time.Duration) {
	directNs := float64(elapsed[0].Nanoseconds()) / float64(b.N)
	seamNs := float64(elapsed[1].Nanoseconds()) / float64(b.N)
	b.ReportMetric(directNs, "direct-ns/op")
	b.ReportMetric(seamNs, "seam-ns/op")
	b.ReportMetric(seamNs/directNs, "seam/direct")
	b.ReportMetric((seamNs/directNs-1)*100, "seam-overhead-%")
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
