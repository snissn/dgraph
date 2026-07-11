/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitEventsDisabledAndUnsubscribedAddNoTxnAllocation(t *testing.T) {
	base := newFakeCommitStore()
	bus := NewCommitEventBus(8)
	defer bus.Close()
	disabled := WithCommitEvents(base, nil)
	unsubscribed := WithCommitEvents(base, bus)
	var direct Store = base

	directAllocs := testing.AllocsPerRun(1000, func() { direct.NewWriteTxn().Discard() })
	disabledAllocs := testing.AllocsPerRun(1000, func() { disabled.NewWriteTxn().Discard() })
	unsubscribedAllocs := testing.AllocsPerRun(1000, func() { unsubscribed.NewWriteTxn().Discard() })
	require.Equal(t, directAllocs, disabledAllocs)
	require.Equal(t, directAllocs, unsubscribedAllocs)
}

func BenchmarkCommitEventWriteBatch(b *testing.B) {
	for _, mode := range []string{"direct", "disabled", "unsubscribed"} {
		b.Run(mode, func(b *testing.B) {
			base := openTreeDBAdapterBenchStore(b)
			var store Store = base
			switch mode {
			case "disabled":
				store = WithCommitEvents(base, nil)
			case "unsubscribed":
				bus := NewCommitEventBus(64)
				b.Cleanup(bus.Close)
				store = WithCommitEvents(base, bus)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for operation := 0; operation < b.N; operation++ {
				commitEventWriteBatch(b, store, operation)
			}
		})
	}
}

// BenchmarkCommitEventWriteBatchInterleaved is the comparison benchmark for
// the disabled-path overhead gate. BenchmarkCommitEventWriteBatch retains the
// conventional per-mode allocation results, but `go test -count=N` runs all N
// samples for one sub-benchmark before moving to the next mode. That ordering
// makes ratios near one percent sensitive to CPU drift. This benchmark gives
// each mode its own fresh, equally growing store and rotates their order after
// every batch, reporting ratios from the same benchmark process and time span.
func BenchmarkCommitEventWriteBatchInterleaved(b *testing.B) {
	direct := openTreeDBAdapterBenchStore(b)
	disabled := openTreeDBAdapterBenchStore(b)
	unsubscribed := openTreeDBAdapterBenchStore(b)
	bus := NewCommitEventBus(64)
	b.Cleanup(bus.Close)
	stores := [...]Store{
		direct,
		WithCommitEvents(disabled, nil),
		WithCommitEvents(unsubscribed, bus),
	}
	var elapsed [len(stores)]time.Duration

	b.ResetTimer()
	for operation := 0; operation < b.N; operation++ {
		for offset := 0; offset < len(stores); offset++ {
			mode := (operation + offset) % len(stores)
			start := time.Now()
			commitEventWriteBatch(b, stores[mode], operation)
			elapsed[mode] += time.Since(start)
		}
	}
	b.StopTimer()

	directNs := float64(elapsed[0].Nanoseconds()) / float64(b.N)
	disabledNs := float64(elapsed[1].Nanoseconds()) / float64(b.N)
	unsubscribedNs := float64(elapsed[2].Nanoseconds()) / float64(b.N)
	b.ReportMetric(directNs, "direct-ns/batch")
	b.ReportMetric(disabledNs, "disabled-ns/batch")
	b.ReportMetric(unsubscribedNs, "unsubscribed-ns/batch")
	b.ReportMetric((disabledNs/directNs-1)*100, "disabled-overhead-%")
	b.ReportMetric((unsubscribedNs/directNs-1)*100, "unsubscribed-overhead-%")
}

func commitEventWriteBatch(b *testing.B, store Store, operation int) {
	txn := store.NewWriteTxn()
	for item := 0; item < 16; item++ {
		var key [16]byte
		binary.LittleEndian.PutUint64(key[:8], uint64(operation))
		binary.LittleEndian.PutUint64(key[8:], uint64(item))
		if err := txn.SetEntry(Entry{Key: key[:], Value: []byte("value")}); err != nil {
			b.Fatal(err)
		}
	}
	if err := txn.CommitAt(uint64(operation+1), nil); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCommitEventDisabledAndUnsubscribed(b *testing.B) {
	base := newFakeCommitStore()
	bus := NewCommitEventBus(64)
	b.Cleanup(bus.Close)
	stores := map[string]Store{
		"direct":       base,
		"disabled":     WithCommitEvents(base, nil),
		"unsubscribed": WithCommitEvents(base, bus),
	}
	for name, store := range stores {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				store.NewWriteTxn().Discard()
			}
		})
	}
}
