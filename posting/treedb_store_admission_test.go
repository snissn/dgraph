/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestTreeDBStoreMixedAdmissionReservesSlotsOutsideLifecycleGate(t *testing.T) {
	for _, mode := range []TreeDBCommitMode{TreeDBCommitRelaxed, TreeDBCommitExternallyRecoverable} {
		t.Run(map[TreeDBCommitMode]string{TreeDBCommitRelaxed: "relaxed", TreeDBCommitExternallyRecoverable: "external"}[mode], func(t *testing.T) {
			var store *TreeDBStore
			if mode == TreeDBCommitExternallyRecoverable {
				store = openRaftTransferStore(t, "mixed-admission")
			} else {
				store, _ = openTreeDBPostingStore(t, t.TempDir(), mode)
			}
			syncTxn, callbackTxn := store.NewWriteTxn(), store.NewWriteTxn()
			require.NoError(t, syncTxn.SetEntry(Entry{Key: []byte("sync"), Value: []byte("one")}))
			require.NoError(t, callbackTxn.SetEntry(Entry{Key: []byte("callback"), Value: []byte("two")}))

			// Represent ownership already retained by earlier requests, then
			// leave both new callers waiting at admission while the lifecycle
			// gate is held. Capacity reservation must precede that gate in BOTH
			// paths; the reverse order can deadlock all 65 real slot owners.
			for range treeDBCommitOwnershipLimit {
				store.acquireTreeDBCommitSlot()
			}
			store.lifecycleMu.Lock()
			// Retain 63 existing owners; two released slots must be reserved by
			// the synchronous and callback callers before this mutex opens.
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					store.lifecycleMu.Unlock()
					for range treeDBCommitOwnershipLimit - 2 {
						store.releaseTreeDBCommitSlot()
					}
				})
			}
			t.Cleanup(release)
			syncDone, callbackDone := make(chan error, 1), make(chan error, 1)
			go func() { syncDone <- syncTxn.CommitAt(1, nil) }()
			go func() {
				if err := callbackTxn.CommitAt(2, func(err error) { callbackDone <- err }); err != nil {
					callbackDone <- err
				}
			}()
			store.releaseTreeDBCommitSlot()
			store.releaseTreeDBCommitSlot()
			require.Eventually(t, func() bool { return len(store.commitSlots) == treeDBCommitOwnershipLimit }, 3*time.Second, time.Millisecond,
				"both commit paths must reserve capacity without acquiring lifecycleMu")
			release()
			require.NoError(t, receiveTreeDBCohortResult(t, syncDone))
			require.NoError(t, receiveTreeDBCohortResult(t, callbackDone))
			awaitTreeDBCohort(t, "all admission ownership drain", func() bool { return len(store.commitSlots) == 0 })
			require.Equal(t, "one", pointSnapshot(t, store, 2, []byte("sync")).Value)
			require.Equal(t, "two", pointSnapshot(t, store, 2, []byte("callback")).Value)
			require.NoError(t, store.Close())
		})
	}
}

// A rejected caller still owns its capacity reservation until validation has
// finished. Neither rejection may leak capacity to future writers.
func TestTreeDBStoreSyncAdmissionRejectionReleasesSlot(t *testing.T) {
	for _, reason := range []string{"closed", "root-replaced"} {
		t.Run(reason, func(t *testing.T) {
			store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
			txn := store.NewWriteTxn()
			require.NoError(t, txn.SetEntry(Entry{Key: []byte("rejected"), Value: []byte("value")}))
			expected := ErrTreeDBTxnClosed
			if reason == "closed" {
				require.NoError(t, store.Close())
				expected = treedb.ErrClosed
			} else {
				store.rootEpoch.Add(1)
			}
			require.ErrorIs(t, txn.CommitAt(1, nil), expected)
			require.Zero(t, len(store.commitSlots), "rejected admission leaked an ownership slot")
		})
	}
}
