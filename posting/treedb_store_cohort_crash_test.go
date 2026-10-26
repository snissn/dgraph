/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/stretchr/testify/require"
)

func treeDBCohortCrashOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.DisableSideStores = true
	opts.MemtableMode = os.Getenv("DGRAPH_TREEDB_COHORT_CRASH_MEMTABLE")
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.FlushThreshold = 1 << 30
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	return opts
}

func treeDBCohortCrashValue(ts uint64) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("acknowledged-pointer-version-%d/", ts)), 1024)
}

// The child observes every successful public CommitAt acknowledgement then
// exits without Close, Checkpoint, or deferred cleanup. Pointer placement and
// cohort membership are checked before the final acknowledgement is delivered.
func TestTreeDBStoreDurableCohortAbruptCrashAfterACK(t *testing.T) {
	for _, mode := range []string{"adaptive", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) { testTreeDBStoreDurableCohortAbruptCrashAfterACK(t, mode) })
	}
}

func testTreeDBStoreDurableCohortAbruptCrashAfterACK(t *testing.T, mode string) {
	if os.Getenv("DGRAPH_TREEDB_COHORT_CRASH_CHILD") == t.Name() {
		if err := writeTreeDBCohortCrashFixture(os.Getenv("DGRAPH_TREEDB_COHORT_CRASH_DIR")); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), "DGRAPH_TREEDB_COHORT_CRASH_CHILD="+t.Name(), "DGRAPH_TREEDB_COHORT_CRASH_DIR="+dir, "DGRAPH_TREEDB_COHORT_CRASH_MEMTABLE="+mode)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "abrupt cohort writer failed: %s", output)
	opts := treeDBCohortCrashOptions(dir)
	opts.MemtableMode = mode
	store, err := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stats, err := store.Stats()
	require.NoError(t, err)
	if mode == "cow_ordered" {
		require.Equal(t, mode, stats["treedb.cache.memtable_mode"])
	} else {
		require.NotEmpty(t, stats["treedb.cache.memtable_mode"])
	}
	require.Equal(t, "gate", pointSnapshot(t, store, 41, []byte("gate")).Value)
	for ts := uint64(42); ts <= 45; ts++ {
		if ts == 44 {
			require.Equal(t, badger.ErrKeyNotFound.Error(), pointSnapshot(t, store, ts, []byte("shared")).Err)
		} else {
			snapshot := pointSnapshot(t, store, ts, []byte("shared"))
			require.Equal(t, ts, snapshot.Version)
			require.Equal(t, treeDBCohortCrashValue(ts), []byte(snapshot.Value))
		}
		snapshot := pointSnapshot(t, store, ts, []byte(fmt.Sprintf("independent/%d", ts)))
		require.Equal(t, ts, snapshot.Version)
		require.Equal(t, treeDBCohortCrashValue(ts), []byte(snapshot.Value))
	}
	versions := iteratorSnapshot(t, store, 45, []byte("shared"), IteratorOptions{AllVersions: true}, nil)
	require.Len(t, versions, 4)
	for i, ts := range []uint64{45, 44, 43, 42} {
		require.Equal(t, ts, versions[i].Version)
		require.Equal(t, ts == 44, versions[i].Deleted)
		if ts != 44 {
			require.Equal(t, treeDBCohortCrashValue(ts), []byte(versions[i].Value))
		}
	}
	require.NoError(t, verifyTreeDBCohortCrashPointers(store))
}

func verifyTreeDBCohortCrashPointers(store *TreeDBStore) error {
	snapshot := store.db.AcquireSnapshot()
	if snapshot == nil {
		return errors.New("pointer snapshot unavailable")
	}
	defer snapshot.Close()
	count := 0
	err := snapshot.Iterate(nil, nil, func(key, _ []byte) error {
		entry, err := snapshot.GetEntry(key)
		if err != nil {
			return err
		}
		if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
			return fmt.Errorf("acknowledged record %x is not backed by a persistent value-log pointer", key)
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	if count != 9 {
		return fmt.Errorf("pointer record count=%d want 9 (gate, four same-key versions and four independent keys)", count)
	}
	return nil
}

func writeTreeDBCohortCrashFixture(dir string) error {
	store, err := OpenTreeDBStore(treeDBCohortCrashOptions(dir), TreeDBCommitDurable)
	if err != nil {
		return err
	}
	gate := make(chan struct{})
	started := make(chan struct{})
	var beforeACK error
	store.commitStartedForTest = func(ts uint64, _ []mvcc.Mutation) {
		if ts == 41 {
			close(started)
			<-gate
		}
	}
	store.commitFinishedForTest = func(ts uint64, err error) {
		if ts != 45 || err != nil {
			return
		}
		if store.durableCohorts.Load() != 2 || store.durableCohortMax.Load() != 4 {
			beforeACK = fmt.Errorf("grouped cohort was not selected: cohorts=%d max=%d", store.durableCohorts.Load(), store.durableCohortMax.Load())
			return
		}
		beforeACK = verifyTreeDBCohortCrashPointers(store)
	}
	results := make([]chan error, 5)
	for i := range results {
		ts := uint64(i + 41)
		txn := store.NewWriteTxn()
		if ts == 41 {
			err = txn.SetEntry(Entry{Key: []byte("gate"), Value: []byte("gate")})
		} else {
			if ts == 44 {
				err = txn.Delete([]byte("shared"))
			} else {
				err = txn.SetEntry(Entry{Key: []byte("shared"), Value: treeDBCohortCrashValue(ts)})
			}
			if err == nil {
				err = txn.SetEntry(Entry{Key: []byte(fmt.Sprintf("independent/%d", ts)), Value: treeDBCohortCrashValue(ts)})
			}
		}
		if err != nil {
			return err
		}
		results[i] = make(chan error, 1)
		result := results[i]
		if err := txn.CommitAt(ts, func(err error) { result <- err }); err != nil {
			return err
		}
		if i == 0 {
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				return errors.New("gate admission stalled")
			}
		}
	}
	if len(store.commitQueue) != 4 {
		return fmt.Errorf("queued crash successors=%d want 4", len(store.commitQueue))
	}
	close(gate)
	for _, result := range results {
		select {
		case err := <-result:
			if err != nil {
				return err
			}
		case <-time.After(10 * time.Second):
			return errors.New("durable crash acknowledgement stalled")
		}
	}
	return beforeACK
}
