/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

// A received batch is deterministic; the intentionally lossy public sync.Pool
// assembly is not assumed to preserve a particular number of keys. Both arms use
// actual managed Badger transactions and the ordinary concrete TxnWriter.
func TestRollupScalarProcessPreservesPerKeyRoute(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "badger"
		if wrapped {
			name = "unsupported-decorator"
		}
		t.Run(name, func(t *testing.T) {
			store := Store(NewBadgerStore(openPostingStoreTestDB(t)))
			if wrapped {
				store = struct{ Store }{store}
			}
			oldStore, oldOracle := postingStore, o
			postingStore = store
			o = &oracle{}
			o.init()
			o.ProcessDelta(&pb.OracleDelta{MaxAssigned: 10})
			defer func() { postingStore, o = oldStore, oldOracle }()
			keys := make([][]byte, 2)
			for i := range keys {
				keys[i] = x.DataKey(x.AttrInRootNamespace("scalar-rollup"), uint64(i+1))
				pack := codec.Encode([]uint64{uint64(i + 1)}, blockSize)
				wire := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
				commitStoreEntry(t, store, 10, Entry{Key: keys[i], Value: wire, UserMeta: BitCompletePosting})
				codec.FreePack(pack)
			}
			before := RollupStats()
			ir := &incrRollupi{priorityKeys: make([]*pooledKeys, 2)}
			for i := range ir.priorityKeys {
				ir.priorityKeys[i] = &pooledKeys{keysCh: make(chan *[][]byte, 1), keysPool: &sync.Pool{New: func() interface{} { return new([][]byte) }}}
			}
			sent := append([][]byte(nil), keys...)
			ir.priorityKeys[0].keysCh <- &sent
			closeRun := z.NewCloser(1)
			defer closeRun.SignalAndWait()
			var timestamps atomic.Int32
			go ir.Process(closeRun, func(readonly bool) uint64 {
				if readonly {
					panic("read-only scalar rollup timestamp")
				}
				return 100 + uint64(timestamps.Add(1))
			})
			for ts := uint64(101); ts <= 102; ts++ {
				awaitTreeDBCohort(t, "scalar per-key Oracle waiter", func() bool { o.RLock(); defer o.RUnlock(); return len(o.waiters[ts]) > 0 })
				require.Equal(t, before, RollupStats(), "ordinary scalar route must not enter native preparation or accounting")
				if ts == 102 {
					awaitTreeDBCohort(t, "first scalar write before next readiness finishes", func() bool {
						txn := store.NewReadTxn(1000)
						defer txn.Discard()
						item, err := txn.Get(keys[0])
						return err == nil && item.Version() == 11
					})
				}
				o.ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
			}
			awaitTreeDBCohort(t, "second ordinary scalar write", func() bool {
				txn := store.NewReadTxn(1000)
				defer txn.Discard()
				item, err := txn.Get(keys[1])
				return err == nil && item.Version() == 11
			})
			closeRun.SignalAndWait()
			require.Equal(t, int32(2), timestamps.Load())
			require.Equal(t, before, RollupStats(), "scalar completion must not run native accounting")
		})
	}
}
