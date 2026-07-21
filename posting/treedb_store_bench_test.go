/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

const (
	treeDBAdapterBenchKeys  = 256
	treeDBAdapterBenchValue = 128
)

var treeDBAdapterBenchSink []byte

func captureTreeDBAdapterBenchValue(value []byte) error {
	treeDBAdapterBenchSink = value
	return nil
}

// BenchmarkTreeDBStoreAdapterOverhead compares the Dgraph semantic adapter to
// the same gomap MVCC owner. It deliberately uses relaxed commits so fsync does
// not hide adapter CPU/allocation overhead; durability is covered separately by
// the durable crash/reopen test and the A/B benchmark issue.
func BenchmarkTreeDBStoreAdapterOverhead(b *testing.B) {
	store := openTreeDBAdapterBenchStore(b)
	keys := make([][]byte, treeDBAdapterBenchKeys)
	mutations := make([]mvcc.Mutation, treeDBAdapterBenchKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("adapter-bench/%04d", i))
		mutations[i] = mvcc.Mutation{
			Key: keys[i], Value: encodeTreeDBEnvelope(Entry{Value: make([]byte, treeDBAdapterBenchValue)}),
		}
	}
	if err := store.mvcc.CommitAt(1, mutations, mvcc.CommitRelaxed); err != nil {
		b.Fatal(err)
	}

	b.Run("PointGet/DirectMVCC", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := store.mvcc.GetAt(keys[i%len(keys)], 1)
			if err != nil {
				b.Fatal(err)
			}
			treeDBAdapterBenchSink = result.Value
		}
	})
	b.Run("PointGet/TreeDBStore", func(b *testing.B) {
		read := store.NewReadTxn(1)
		defer read.Discard()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			item, err := read.Get(keys[i%len(keys)])
			if err != nil {
				b.Fatal(err)
			}
			if err := item.Value(captureTreeDBAdapterBenchValue); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("AllVersionsScan/DirectMVCC", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it, err := store.mvcc.IterateVersions(mvcc.VersionIteratorOptions{
				Prefix: []byte("adapter-bench/"), ReadTimestamp: 1,
			})
			if err != nil {
				b.Fatal(err)
			}
			for ; it.Valid(); it.Next() {
				treeDBAdapterBenchSink = it.Entry().Value
			}
			if err := it.Error(); err != nil {
				b.Fatal(err)
			}
			if err := it.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("AllVersionsScan/TreeDBStore", func(b *testing.B) {
		read := store.NewReadTxn(1)
		defer read.Discard()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := read.NewIterator(IteratorOptions{Prefix: []byte("adapter-bench/"), AllVersions: true})
			for it.Rewind(); it.Valid(); it.Next() {
				if err := it.Item().Value(captureTreeDBAdapterBenchValue); err != nil {
					b.Fatal(err)
				}
			}
			if err := it.Error(); err != nil {
				b.Fatal(err)
			}
			it.Close()
		}
	})

	b.Run("RandomSeek/DirectMVCC", func(b *testing.B) {
		it, err := store.mvcc.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 1})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = it.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.Seek(keys[i%len(keys)], math.MaxUint64)
			if !it.Valid() {
				b.Fatalf("seek %d: %v", i, it.Error())
			}
			entry := it.Entry()
			treeDBAdapterBenchSink = entry.Value
			// Match TreeDBStore's default-iterator contract, which must consume
			// the complete logical-key version group before returning the newest
			// visible non-tombstone item.
			it.Next()
			for it.Valid() && bytes.Equal(it.Entry().Key, entry.Key) {
				it.Next()
			}
		}
	})
	b.Run("RandomSeek/TreeDBStore", func(b *testing.B) {
		read := store.NewReadTxn(1)
		defer read.Discard()
		it := read.NewIterator(IteratorOptions{})
		defer it.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.Seek(keys[i%len(keys)])
			if !it.Valid() {
				b.Fatalf("seek %d: %v", i, it.Error())
			}
			if err := it.Item().Value(captureTreeDBAdapterBenchValue); err != nil {
				b.Fatal(err)
			}
		}
	})

	benchmarkTreeDBStoreWrites(b, 1)
	benchmarkTreeDBStoreWrites(b, 16)
	benchmarkTreeDBStoreExactKey(b, store)
	benchmarkTreeDBStoreReopen(b)
}

// BenchmarkTreeDBStoreCallbackPipeline compares synchronous one-at-a-time
// commits with the callback path used by TxnWriter at a bounded pipeline depth.
// It is separate from the adapter-overhead matrix so its concurrent write load
// cannot perturb read/iterator samples.
func BenchmarkTreeDBStoreCallbackPipeline(b *testing.B) {
	for _, depth := range []int{16, 128} {
		b.Run(fmt.Sprintf("SyncDepth%d", depth), func(b *testing.B) {
			store := openTreeDBAdapterBenchStore(b)
			b.ReportAllocs()
			b.ResetTimer()
			for operation := 0; operation < b.N; operation++ {
				for item := 0; item < depth; item++ {
					var key [16]byte
					binary.LittleEndian.PutUint64(key[:8], uint64(operation))
					binary.LittleEndian.PutUint64(key[8:], uint64(item))
					txn := store.NewWriteTxn()
					if err := txn.SetEntry(Entry{Key: key[:], Value: []byte("value")}); err != nil {
						b.Fatal(err)
					}
					timestamp := uint64(operation*depth + item + 1)
					if err := txn.CommitAt(timestamp, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run(fmt.Sprintf("TxnWriterDepth%d", depth), func(b *testing.B) {
			store := openTreeDBAdapterBenchStore(b)
			b.ReportAllocs()
			b.ResetTimer()
			for operation := 0; operation < b.N; operation++ {
				writer := NewTxnWriterForStore(store)
				for item := 0; item < depth; item++ {
					var key [16]byte
					binary.LittleEndian.PutUint64(key[:8], uint64(operation))
					binary.LittleEndian.PutUint64(key[8:], uint64(item))
					timestamp := uint64(operation*depth + item + 1)
					if err := writer.SetAt(key[:], []byte("value"), BitDeltaPosting, timestamp); err != nil {
						b.Fatal(err)
					}
				}
				if err := writer.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTreeDBStoreDurableCallbackGroupCommit is the Dgraph-shaped durable
// callback path: independent TxnWriter.SetAt calls share the adapter's bounded
// scheduler, and every writer drains its callbacks before submitting its next
// small window. It reports the public Gomap counters needed to distinguish
// scheduler overlap from a merely fast single-commit path.
func BenchmarkTreeDBStoreDurableCallbackGroupCommit(b *testing.B) {
	store := openTreeDBAdapterBenchStoreMode(b, TreeDBCommitDurable)
	before, err := store.Stats()
	if err != nil {
		b.Fatal(err)
	}

	var timestamp atomic.Uint64
	var submitted atomic.Uint64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, 1024)
	const window = 8
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		writer := NewTxnWriterForStore(store)
		pending := 0
		started := time.Now()
		for pb.Next() {
			sequence := timestamp.Add(1)
			var key [16]byte
			binary.LittleEndian.PutUint64(key[:8], sequence)
			binary.LittleEndian.PutUint64(key[8:], sequence^0x9e3779b97f4a7c15)
			if err := writer.SetAt(key[:], []byte("durable-group-value"), BitDeltaPosting, sequence); err != nil {
				b.Error(err)
				return
			}
			submitted.Add(1)
			pending++
			if pending != window {
				continue
			}
			if err := writer.Wait(); err != nil {
				b.Error(err)
				return
			}
			latencyMu.Lock()
			latencies = append(latencies, time.Since(started)/time.Duration(pending))
			latencyMu.Unlock()
			pending = 0
			started = time.Now()
		}
		if pending != 0 {
			if err := writer.Wait(); err != nil {
				b.Error(err)
				return
			}
			latencyMu.Lock()
			latencies = append(latencies, time.Since(started)/time.Duration(pending))
			latencyMu.Unlock()
		}
	})
	b.StopTimer()

	after, err := store.Stats()
	if err != nil {
		b.Fatal(err)
	}
	publicWrites := treeDBBenchStatDelta(b, before, after, "treedb.public.batch.write_sync.calls_total")
	groups := treeDBBenchStatDelta(b, before, after, "treedb.command_wal.group_commit.groups_total")
	participants := treeDBBenchStatDelta(b, before, after, "treedb.command_wal.group_commit.participants_total")
	groupSyncs := treeDBBenchStatDelta(b, before, after, "treedb.command_wal.group_commit.syncs_total")
	fileSyncs := treeDBBenchStatDelta(b, before, after, "treedb.command_wal.file_sync.calls_total")
	b.ReportMetric(float64(submitted.Load()), "public-writes")
	b.ReportMetric(float64(groups), "groups")
	b.ReportMetric(float64(participants), "participants")
	b.ReportMetric(float64(groupSyncs), "group-syncs")
	b.ReportMetric(float64(fileSyncs), "file-syncs")
	b.ReportMetric(float64(treeDBBenchStatUint(b, after, "treedb.command_wal.group_commit.group_size_max")), "group-size-max")
	if fileSyncs != 0 {
		b.ReportMetric(float64(publicWrites)/float64(fileSyncs), "commits/sync")
	}
	if len(latencies) != 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		b.ReportMetric(float64(latencies[len(latencies)/2].Microseconds()), "p50-us")
		b.ReportMetric(float64(latencies[(len(latencies)*95+99)/100-1].Microseconds()), "p95-us")
	}
}

func treeDBBenchStatDelta(b *testing.B, before, after map[string]string, key string) uint64 {
	b.Helper()
	return treeDBBenchStatUint(b, after, key) - treeDBBenchStatUint(b, before, key)
}

func treeDBBenchStatUint(b *testing.B, stats map[string]string, key string) uint64 {
	b.Helper()
	value, ok := stats[key]
	if !ok {
		b.Fatalf("missing TreeDB stat %q", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		b.Fatalf("parse TreeDB stat %q=%q: %v", key, value, err)
	}
	return parsed
}

func benchmarkTreeDBStoreWrites(b *testing.B, batchSize int) {
	b.Helper()
	name := fmt.Sprintf("WriteBatch%d", batchSize)
	b.Run(name+"/DirectMVCC", func(b *testing.B) {
		dir := b.TempDir()
		store := openTreeDBAdapterBenchStoreAt(b, dir)
		before := treeDBAdapterBenchDiskBytes(b, dir)
		batches := make([][]mvcc.Mutation, b.N)
		for operation := range batches {
			batch := make([]mvcc.Mutation, batchSize)
			for item := range batch {
				batch[item] = mvcc.Mutation{
					Key:   []byte(fmt.Sprintf("direct-write/%d/%d", operation, item)),
					Value: encodeTreeDBEnvelope(Entry{Value: make([]byte, treeDBAdapterBenchValue)}),
				}
			}
			batches[operation] = batch
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := store.mvcc.CommitAt(uint64(i+1), batches[i], mvcc.CommitRelaxed); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportTreeDBAdapterBenchDiskBytes(b, store, dir, before, batchSize)
	})
	b.Run(name+"/TreeDBStore", func(b *testing.B) {
		dir := b.TempDir()
		store := openTreeDBAdapterBenchStoreAt(b, dir)
		before := treeDBAdapterBenchDiskBytes(b, dir)
		batches := make([][]Entry, b.N)
		for operation := range batches {
			batch := make([]Entry, batchSize)
			for item := range batch {
				batch[item] = Entry{
					Key:   []byte(fmt.Sprintf("adapter-write/%d/%d", operation, item)),
					Value: make([]byte, treeDBAdapterBenchValue), UserMeta: BitDeltaPosting,
				}
			}
			batches[operation] = batch
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			txn := store.NewWriteTxn()
			for _, entry := range batches[i] {
				if err := txn.SetEntry(entry); err != nil {
					b.Fatal(err)
				}
			}
			if err := txn.CommitAt(uint64(i+1), nil); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportTreeDBAdapterBenchDiskBytes(b, store, dir, before, batchSize)
	})
}

func benchmarkTreeDBStoreExactKey(b *testing.B, store *TreeDBStore) {
	b.Helper()
	target := []byte("adapter-exact")
	for version := uint64(1); version <= 8; version++ {
		mutations := []mvcc.Mutation{{
			Key: target, Value: encodeTreeDBEnvelope(Entry{Value: []byte{byte(version)}}),
		}}
		for sibling := 0; sibling < 32; sibling++ {
			mutations = append(mutations, mvcc.Mutation{
				Key:   []byte(fmt.Sprintf("adapter-exact/%02d", sibling)),
				Value: encodeTreeDBEnvelope(Entry{Value: []byte{byte(version)}}),
			})
		}
		if err := store.mvcc.CommitAt(version, mutations, mvcc.CommitRelaxed); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("ExactKeyAllVersions/DirectMVCC", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it, err := store.mvcc.IterateVersions(mvcc.VersionIteratorOptions{
				ReadTimestamp: math.MaxUint64,
				LowerBound:    target,
				UpperBound:    append(append([]byte(nil), target...), 0),
			})
			if err != nil {
				b.Fatal(err)
			}
			for ; it.Valid(); it.Next() {
				treeDBAdapterBenchSink = it.Entry().Value
			}
			if err := it.Error(); err != nil {
				b.Fatal(err)
			}
			if err := it.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ExactKeyAllVersions/TreeDBStore", func(b *testing.B) {
		read := store.NewReadTxn(math.MaxUint64)
		defer read.Discard()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := read.NewKeyIterator(target, IteratorOptions{})
			for it.Rewind(); it.Valid(); it.Next() {
				if err := it.Item().Value(captureTreeDBAdapterBenchValue); err != nil {
					b.Fatal(err)
				}
			}
			if err := it.Error(); err != nil {
				b.Fatal(err)
			}
			it.Close()
		}
	})
}

func benchmarkTreeDBStoreReopen(b *testing.B) {
	b.Helper()
	direct := func(b *testing.B) {
		opts := treeDBAdapterBenchOptions(b.TempDir())
		db, err := treedb.Open(opts)
		if err != nil {
			b.Fatal(err)
		}
		owner := mvcc.New(db)
		if err := owner.CommitAt(1, []mvcc.Mutation{{Key: []byte("reopen"), Value: []byte("value")}}, mvcc.CommitRelaxed); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			db, err = treedb.Open(opts)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
	adapter := func(b *testing.B) {
		opts := treeDBAdapterBenchOptions(b.TempDir())
		store, err := OpenTreeDBStore(opts, TreeDBCommitRelaxed)
		if err != nil {
			b.Fatal(err)
		}
		txn := store.NewWriteTxn()
		if err := txn.SetEntry(Entry{Key: []byte("reopen"), Value: []byte("value")}); err != nil {
			b.Fatal(err)
		}
		if err := txn.CommitAt(1, nil); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			store, err = OpenTreeDBStore(opts, TreeDBCommitRelaxed)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("Reopen/DirectMVCC", direct)
	b.Run("Reopen/TreeDBStore", adapter)
	b.Run("Reopen/PairedLatency", func(b *testing.B) {
		directOpts := treeDBAdapterBenchOptions(b.TempDir())
		directDB, err := treedb.Open(directOpts)
		if err != nil {
			b.Fatal(err)
		}
		directOwner := mvcc.New(directDB)
		if err := directOwner.CommitAt(1, []mvcc.Mutation{{Key: []byte("reopen"), Value: []byte("value")}},
			mvcc.CommitRelaxed); err != nil {
			b.Fatal(err)
		}

		adapterOpts := treeDBAdapterBenchOptions(b.TempDir())
		store, err := OpenTreeDBStore(adapterOpts, TreeDBCommitRelaxed)
		if err != nil {
			b.Fatal(err)
		}
		txn := store.NewWriteTxn()
		if err := txn.SetEntry(Entry{Key: []byte("reopen"), Value: []byte("value")}); err != nil {
			b.Fatal(err)
		}
		if err := txn.CommitAt(1, nil); err != nil {
			b.Fatal(err)
		}

		var directNanos, adapterNanos int64
		runDirect := func() {
			start := time.Now()
			if err := directDB.Close(); err != nil {
				b.Fatal(err)
			}
			directDB, err = treedb.Open(directOpts)
			if err != nil {
				b.Fatal(err)
			}
			directNanos += time.Since(start).Nanoseconds()
		}
		runAdapter := func() {
			start := time.Now()
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			store, err = OpenTreeDBStore(adapterOpts, TreeDBCommitRelaxed)
			if err != nil {
				b.Fatal(err)
			}
			adapterNanos += time.Since(start).Nanoseconds()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				runDirect()
				runAdapter()
				continue
			}
			runAdapter()
			runDirect()
		}
		b.StopTimer()
		b.ReportMetric(float64(directNanos)/float64(b.N), "direct-ns/op")
		b.ReportMetric(float64(adapterNanos)/float64(b.N), "adapter-ns/op")
		b.ReportMetric(100*(float64(adapterNanos)/float64(directNanos)-1), "adapter-overhead-%")
		if err := directDB.Close(); err != nil {
			b.Fatal(err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	})
}

func openTreeDBAdapterBenchStore(b *testing.B) *TreeDBStore {
	b.Helper()
	return openTreeDBAdapterBenchStoreMode(b, TreeDBCommitRelaxed)
}

func openTreeDBAdapterBenchStoreMode(b *testing.B, mode TreeDBCommitMode) *TreeDBStore {
	b.Helper()
	return openTreeDBAdapterBenchStoreAtMode(b, b.TempDir(), mode)
}

func openTreeDBAdapterBenchStoreAt(b *testing.B, dir string) *TreeDBStore {
	return openTreeDBAdapterBenchStoreAtMode(b, dir, TreeDBCommitRelaxed)
}

func openTreeDBAdapterBenchStoreAtMode(b *testing.B, dir string, mode TreeDBCommitMode) *TreeDBStore {
	b.Helper()
	profile := treedb.ProfileCommandWALRelaxed
	if mode == TreeDBCommitDurable {
		profile = treedb.ProfileCommandWALDurable
	}
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	store, err := OpenTreeDBStore(opts, mode)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	return store
}

func reportTreeDBAdapterBenchDiskBytes(b *testing.B, store *TreeDBStore, dir string, before int64, batchSize int) {
	b.Helper()
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	after := treeDBAdapterBenchDiskBytes(b, dir)
	delta := after - before
	if delta < 0 {
		delta = 0
	}
	b.ReportMetric(float64(delta)/float64(b.N), "disk-B/op")
	b.ReportMetric(float64(delta)/float64(b.N*batchSize), "disk-B/item")
}

func treeDBAdapterBenchDiskBytes(b *testing.B, dir string) int64 {
	b.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return total
}

func treeDBAdapterBenchOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	return opts
}
