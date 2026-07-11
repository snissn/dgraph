/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/posting"
	td "github.com/snissn/gomap/TreeDB"
)

const (
	benchKeyCount     = 256
	benchVersionCount = 4
	benchValueSize    = 128
	benchBatchSize    = 16
)

var (
	benchValueSink []byte
	benchCountSink int
	benchStatsSink map[string]string
)

type dgraphTreeDBBlockerRow struct {
	name   string
	reason string
}

var dgraphTreeDBBlockerRows = []dgraphTreeDBBlockerRow{
	{
		name:   "EntryTTL",
		reason: "TreeDBStore intentionally rejects nonzero Badger Entry.ExpiresAt values until an operational-tier expiry contract exists.",
	},
	{
		name:   "StreamBackupExport",
		reason: "TreeDB does not yet provide Dgraph's Badger NewStreamAt/Stream.Orchestrate backup-export contract.",
	},
	{
		name:   "StreamWriterImport",
		reason: "TreeDB does not yet provide Dgraph's Badger NewStreamWriter import/restore contract.",
	},
	{
		name:   "Subscriptions",
		reason: "TreeDB does not yet provide the Badger Subscribe API used by worker.SubscribeForUpdates.",
	},
	{
		name:   "EncryptionKeyRegistry",
		reason: "TreeDB Dgraph scaffold intentionally fails closed for Badger-compatible encryption and key registry APIs.",
	},
}

// BenchmarkDgraphTreeDBMatrix captures the Dgraph posting-store comparison
// matrix used before adding any runtime TreeDB backend selector. It intentionally
// mixes Dgraph-shaped Badger baselines with TreeDB primitive rows plus explicit
// blocker rows for Dgraph-required Badger contracts TreeDB cannot run yet.
func BenchmarkDgraphTreeDBMatrix(b *testing.B) {
	b.Run("Badger/ManagedTxnWriterSetAt", benchmarkBadgerManagedTxnWriterSetAt)
	b.Run("Badger/NewTransactionAtPointRead", benchmarkBadgerNewTransactionAtPointRead)
	b.Run("Badger/AllVersionsPrefixScan", benchmarkBadgerAllVersionsPrefixScan)

	b.Run("TreeDB/Set", benchmarkTreeDBSet)
	b.Run("TreeDB/Get", benchmarkTreeDBGet)
	b.Run("TreeDB/GetVersioned", benchmarkTreeDBGetVersioned)
	b.Run("TreeDB/NewBatchWrite", benchmarkTreeDBNewBatchWrite)
	b.Run("TreeDB/NewBatchWriteSync", benchmarkTreeDBNewBatchWriteSync)
	b.Run("TreeDB/SnapshotGet", benchmarkTreeDBSnapshotGet)
	b.Run("TreeDB/SnapshotPrefixIterate", benchmarkTreeDBSnapshotPrefixIterate)
	b.Run("TreeDB/IteratorPrefixScan", benchmarkTreeDBIteratorPrefixScan)
	b.Run("TreeDB/ReverseIteratorPrefixScan", benchmarkTreeDBReverseIteratorPrefixScan)
	b.Run("TreeDB/Stats", benchmarkTreeDBStats)

	for _, row := range dgraphTreeDBBlockerRows {
		row := row
		b.Run("Blocked/"+row.name, func(b *testing.B) {
			b.Skip(row.reason)
		})
	}
}

func benchmarkBadgerManagedTxnWriterSetAt(b *testing.B) {
	db := openBadgerBenchDB(b)
	keys := benchKeys("dgraph/badger/managed-write/", b.N)
	value := benchValue(0)
	writer := posting.NewTxnWriter(db)

	b.ReportAllocs()
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writer.SetAt(keys[i], value, posting.BitSchemaPosting, uint64(i+1)); err != nil {
			b.Fatalf("TxnWriter.SetAt: %v", err)
		}
		if err := writer.Flush(); err != nil {
			b.Fatalf("TxnWriter.Flush: %v", err)
		}
	}
	b.StopTimer()
	reportPerSecond(b, float64(b.N), "writes/s")
}

func benchmarkBadgerNewTransactionAtPointRead(b *testing.B) {
	db := openBadgerBenchDB(b)
	keys := seedBadgerVersions(b, db, "dgraph/badger/point-read/", benchKeyCount, benchVersionCount)
	readTs := uint64(benchVersionCount)
	dst := make([]byte, 0, benchValueSize)

	b.ReportAllocs()
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.NewTransactionAt(readTs, false)
		item, err := txn.Get(keys[i%len(keys)])
		if err != nil {
			txn.Discard()
			b.Fatalf("Badger point read: %v", err)
		}
		benchValueSink, err = item.ValueCopy(dst[:0])
		txn.Discard()
		if err != nil {
			b.Fatalf("Badger ValueCopy: %v", err)
		}
	}
	b.StopTimer()
	reportPerSecond(b, float64(b.N), "reads/s")
}

func benchmarkBadgerAllVersionsPrefixScan(b *testing.B) {
	db := openBadgerBenchDB(b)
	prefix := []byte("dgraph/badger/all-versions/")
	seedBadgerVersions(b, db, string(prefix), benchKeyCount, benchVersionCount)
	readTs := uint64(benchVersionCount)
	dst := make([]byte, 0, benchValueSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.NewTransactionAt(readTs, false)
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		opts.Prefix = prefix
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		count := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var err error
			benchValueSink, err = item.ValueCopy(dst[:0])
			if err != nil {
				it.Close()
				txn.Discard()
				b.Fatalf("Badger all-version ValueCopy: %v", err)
			}
			count++
		}
		it.Close()
		txn.Discard()
		benchCountSink = count
	}
	b.StopTimer()
	versionsPerOp := float64(benchKeyCount * benchVersionCount)
	b.ReportMetric(versionsPerOp, "versions/op")
	reportPerSecond(b, float64(b.N)*versionsPerOp, "versions/s")
}

func benchmarkTreeDBSet(b *testing.B) {
	db := openTreeDBBenchDB(b)
	keys := benchKeys("dgraph/treedb/set/", b.N)
	value := benchValue(1)

	b.ReportAllocs()
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Set(keys[i], value); err != nil {
			b.Fatalf("TreeDB Set: %v", err)
		}
	}
	b.StopTimer()
	reportPerSecond(b, float64(b.N), "writes/s")
}

func benchmarkTreeDBGet(b *testing.B) {
	db := openTreeDBBenchDB(b)
	keys := seedTreeDBFixture(b, db, "dgraph/treedb/get/", benchKeyCount)

	b.ReportAllocs()
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := db.Get(keys[i%len(keys)])
		if err != nil {
			b.Fatalf("TreeDB Get: %v", err)
		}
		benchValueSink = value
	}
	b.StopTimer()
	reportPerSecond(b, float64(b.N), "reads/s")
}

func benchmarkTreeDBGetVersioned(b *testing.B) {
	db := openTreeDBBenchDB(b)
	keys := seedTreeDBFixture(b, db, "dgraph/treedb/get-versioned/", benchKeyCount)

	b.ReportAllocs()
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, revision, err := db.GetVersioned(keys[i%len(keys)])
		if err != nil {
			b.Fatalf("TreeDB GetVersioned: %v", err)
		}
		if revision == td.LegacyEntryRevision {
			b.Fatalf("TreeDB GetVersioned returned legacy revision for seeded key")
		}
		benchValueSink = value
	}
	b.StopTimer()
	reportPerSecond(b, float64(b.N), "reads/s")
}

func benchmarkTreeDBNewBatchWrite(b *testing.B) {
	benchmarkTreeDBBatchWrite(b, false)
}

func benchmarkTreeDBNewBatchWriteSync(b *testing.B) {
	benchmarkTreeDBBatchWrite(b, true)
}

func benchmarkTreeDBBatchWrite(b *testing.B, sync bool) {
	db := openTreeDBBenchDB(b)
	keys := benchKeys("dgraph/treedb/batch/", b.N*benchBatchSize)
	value := benchValue(2)

	b.ReportAllocs()
	b.SetBytes(benchBatchSize * benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatch()
		if batch == nil {
			b.Fatal("TreeDB NewBatch returned nil")
		}
		base := i * benchBatchSize
		for j := 0; j < benchBatchSize; j++ {
			if err := batch.Set(keys[base+j], value); err != nil {
				_ = batch.Close()
				b.Fatalf("TreeDB batch Set: %v", err)
			}
		}
		var err error
		if sync {
			err = batch.WriteSync()
		} else {
			err = batch.Write()
		}
		if closeErr := batch.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			b.Fatalf("TreeDB batch write: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(benchBatchSize), "writes/op")
	reportPerSecond(b, float64(b.N*benchBatchSize), "writes/s")
}

func benchmarkTreeDBSnapshotGet(b *testing.B) {
	db := openTreeDBBenchDB(b)
	keys := seedTreeDBFixture(b, db, "dgraph/treedb/snapshot-get/", benchKeyCount)
	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("TreeDB AcquireSnapshot returned nil")
	}
	b.Cleanup(func() {
		if err := snap.Close(); err != nil {
			b.Errorf("TreeDB snapshot close: %v", err)
		}
	})

	b.ReportAllocs()
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := snap.Get(keys[i%len(keys)])
		if err != nil {
			b.Fatalf("TreeDB snapshot Get: %v", err)
		}
		benchValueSink = value
	}
	b.StopTimer()
	reportPerSecond(b, float64(b.N), "reads/s")
}

func benchmarkTreeDBSnapshotPrefixIterate(b *testing.B) {
	db := openTreeDBBenchDB(b)
	prefix := []byte("dgraph/treedb/snapshot-iterate/")
	seedTreeDBFixture(b, db, string(prefix), benchKeyCount)
	end := benchPrefixEnd(prefix)
	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("TreeDB AcquireSnapshot returned nil")
	}
	b.Cleanup(func() {
		if err := snap.Close(); err != nil {
			b.Errorf("TreeDB snapshot close: %v", err)
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := snap.Iterate(prefix, end, func(key, value []byte) error {
			benchValueSink = value
			count++
			return nil
		}); err != nil {
			b.Fatalf("TreeDB snapshot Iterate: %v", err)
		}
		benchCountSink = count
	}
	b.StopTimer()
	b.ReportMetric(float64(benchKeyCount), "keys/op")
	reportPerSecond(b, float64(b.N*benchKeyCount), "keys/s")
}

func benchmarkTreeDBIteratorPrefixScan(b *testing.B) {
	benchmarkTreeDBIteratorScan(b, false)
}

func benchmarkTreeDBReverseIteratorPrefixScan(b *testing.B) {
	benchmarkTreeDBIteratorScan(b, true)
}

func benchmarkTreeDBIteratorScan(b *testing.B, reverse bool) {
	db := openTreeDBBenchDB(b)
	prefix := []byte("dgraph/treedb/iterator/")
	seedTreeDBFixture(b, db, string(prefix), benchKeyCount)
	end := benchPrefixEnd(prefix)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var it td.Iterator
		var err error
		if reverse {
			it, err = db.ReverseIterator(prefix, end)
		} else {
			it, err = db.Iterator(prefix, end)
		}
		if err != nil {
			b.Fatalf("TreeDB iterator open: %v", err)
		}
		count := 0
		for it.Valid() {
			benchValueSink = it.Value()
			if err := it.Error(); err != nil {
				_ = it.Close()
				b.Fatalf("TreeDB iterator: %v", err)
			}
			count++
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			b.Fatalf("TreeDB iterator final error: %v", err)
		}
		if err := it.Close(); err != nil {
			b.Fatalf("TreeDB iterator close: %v", err)
		}
		benchCountSink = count
	}
	b.StopTimer()
	b.ReportMetric(float64(benchKeyCount), "keys/op")
	reportPerSecond(b, float64(b.N*benchKeyCount), "keys/s")
}

func benchmarkTreeDBStats(b *testing.B) {
	db := openTreeDBBenchDB(b)
	seedTreeDBFixture(b, db, "dgraph/treedb/stats/", benchKeyCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchStatsSink = db.Stats()
	}
}

func openBadgerBenchDB(b *testing.B) *badger.DB {
	b.Helper()
	opts := badger.DefaultOptions(b.TempDir()).
		WithLogger(nil).
		WithSyncWrites(false).
		WithNumVersionsToKeep(math.MaxInt32)
	opts.DetectConflicts = false
	db, err := badger.OpenManaged(opts)
	if err != nil {
		b.Fatalf("open Badger managed DB: %v", err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Errorf("close Badger DB: %v", err)
		}
	})
	return db
}

func openTreeDBBenchDB(b *testing.B) *td.DB {
	b.Helper()
	handle, err := Open(OpenOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open TreeDB: %v", err)
	}
	b.Cleanup(func() {
		if err := handle.Close(); err != nil {
			b.Errorf("close TreeDB: %v", err)
		}
	})
	return handle.DB
}

func seedBadgerVersions(b *testing.B, db *badger.DB, prefix string, keyCount, versions int) [][]byte {
	b.Helper()
	keys := benchKeys(prefix, keyCount)
	value := benchValue(3)
	wb := db.NewManagedWriteBatch()
	flushed := false
	defer func() {
		if !flushed {
			wb.Cancel()
		}
	}()
	for version := 1; version <= versions; version++ {
		for _, key := range keys {
			entry := &badger.Entry{Key: key, Value: value, UserMeta: posting.BitSchemaPosting}
			if err := wb.SetEntryAt(entry, uint64(version)); err != nil {
				b.Fatalf("seed Badger version %d: %v", version, err)
			}
		}
	}
	if err := wb.Flush(); err != nil {
		b.Fatalf("flush Badger fixture: %v", err)
	}
	flushed = true
	return keys
}

func seedTreeDBFixture(b *testing.B, db *td.DB, prefix string, keyCount int) [][]byte {
	b.Helper()
	keys := benchKeys(prefix, keyCount)
	value := benchValue(4)
	batch := db.NewBatch()
	if batch == nil {
		b.Fatal("TreeDB NewBatch returned nil while seeding fixture")
	}
	closed := false
	defer func() {
		if !closed {
			_ = batch.Close()
		}
	}()
	for _, key := range keys {
		if err := batch.Set(key, value); err != nil {
			b.Fatalf("seed TreeDB fixture: %v", err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatalf("write TreeDB fixture: %v", err)
	}
	if err := batch.Close(); err != nil {
		b.Fatalf("close TreeDB fixture batch: %v", err)
	}
	closed = true
	return keys
}

func benchKeys(prefix string, n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = benchKey(prefix, i)
	}
	return keys
}

func benchKey(prefix string, index int) []byte {
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], uint64(index))
	return key
}

func benchValue(seed byte) []byte {
	value := make([]byte, benchValueSize)
	for i := range value {
		value[i] = seed + byte(i)
	}
	return value
}

func benchPrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func reportPerSecond(b *testing.B, total float64, unit string) {
	b.Helper()
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(total/elapsed.Seconds(), unit)
	}
}
