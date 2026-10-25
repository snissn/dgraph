/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

// BenchmarkTreeDBStorePersistentExactSeek includes a new read transaction and
// all eight versions per operation. Its separate cold-four-ns metric includes
// the first four reads after a real close/reopen, before the steady timer.
// Reopening clears process cache state; it does not clear OS/device caches.
func BenchmarkTreeDBStorePersistentExactSeek(b *testing.B) {
	b.StopTimer()
	opts := treeDBAdapterBenchOptions(b.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.IndexOuterLeavesInValueLog = true
	opts.LeafPageReadCacheEntries = 8192
	store, err := OpenTreeDBStore(opts, TreeDBCommitRelaxed)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	target := []byte("persistent-exact")
	var values [9][]byte
	for version := uint64(1); version <= 8; version++ {
		values[version] = bytes.Repeat([]byte{0x5a}, 64)
		values[version][0] = byte(version)
		txn := store.NewWriteTxn()
		if err := txn.SetEntry(Entry{Key: target, Value: values[version]}); err != nil {
			b.Fatal(err)
		}
		for sibling := 0; sibling < 32; sibling++ {
			if err := txn.SetEntry(Entry{Key: []byte(fmt.Sprintf("persistent-exact/%02d", sibling)), Value: values[version]}); err != nil {
				b.Fatal(err)
			}
		}
		if err := txn.CommitAt(version, nil); err != nil {
			b.Fatal(err)
		}
		txn.Discard()
	}
	if err := store.db.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	store, err = OpenTreeDBStore(opts, TreeDBCommitRelaxed)
	if err != nil {
		b.Fatal(err)
	}
	stats := func() map[string]string {
		s, err := store.Stats()
		if err != nil {
			b.Fatal(err)
		}
		return s
	}
	counter := func(s map[string]string, name string) uint64 {
		n, err := strconv.ParseUint(s["treedb.process.read_path.outer_leaf.cache."+name], 10, 64)
		if err != nil {
			b.Fatalf("cache %s: %v", name, err)
		}
		return n
	}
	cold := stats()
	if cold["treedb.cache.memtable_mode"] != "cow_ordered" || counter(cold, "capacity") != 8192 || counter(cold, "stores") != 0 {
		b.Fatal("fixture did not reopen with a cold, bounded ordered COW cache")
	}
	if _, ok := cold["treedb.process.read_path.outer_leaf.cache.backing_bytes"]; ok && counter(cold, "backing_bytes") != 0 {
		b.Fatal("reopened cache already owns page backing")
	}
	readExact := func() {
		read := store.NewReadTxn(math.MaxUint64)
		defer read.Discard()
		it := read.NewKeyIterator(target, IteratorOptions{})
		defer it.Close()
		version := uint64(8)
		for it.Seek(target); it.Valid(); it.Next() {
			if version == 0 || !bytes.Equal(it.Item().Key(), target) || it.Item().Version() != version {
				b.Fatal("exact history identity/order changed")
			}
			if err := it.Item().Value(captureTreeDBAdapterBenchValue); err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(treeDBAdapterBenchSink, values[version]) {
				b.Fatal("exact history payload changed")
			}
			version--
		}
		if err := it.Error(); err != nil {
			b.Fatal(err)
		}
		if version != 0 {
			b.Fatal("exact history lost a retained version")
		}
	}
	start := time.Now()
	for i := 0; i < 4; i++ {
		readExact()
	}
	coldNanos := time.Since(start).Nanoseconds()
	warm := stats()
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		readExact()
	}
	b.StopTimer()
	end := stats()
	b.ReportMetric(float64(coldNanos), "cold-four-ns")
	b.ReportMetric(float64(counter(warm, "misses")-counter(cold, "misses")), "cold-cache-misses")
	b.ReportMetric(float64(counter(end, "misses")-counter(warm, "misses"))/float64(b.N), "cache-misses/op")
	b.ReportMetric(float64(counter(end, "hits")-counter(warm, "hits"))/float64(b.N), "cache-hits/op")
	b.ReportMetric(float64(counter(warm, "read_miss_admission_stores")-counter(cold, "read_miss_admission_stores")), "cold-admissions")
	if _, ok := end["treedb.process.read_path.outer_leaf.cache.backing_bytes"]; ok {
		b.ReportMetric(float64(counter(cold, "backing_bytes")), "cold-cache-backing-B")
		b.ReportMetric(float64(counter(warm, "backing_bytes")), "warm-cache-backing-B")
		b.ReportMetric(float64(counter(end, "backing_bytes")), "cache-backing-B")
	}
}
