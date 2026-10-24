/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type windowSealStore struct {
	*fakeCommitStore
	sealed chan struct{}
}

func (s *windowSealStore) SealCommitWindow() {
	select {
	case s.sealed <- struct{}{}:
	default:
	}
}

// A producer declares its complete input window before waiting for callbacks.
// The production event decorator must forward the scheduling fence, and the
// fence itself must not satisfy the durability wait.
func TestTxnWriterWaitSealsWindowThroughCommitEvents(t *testing.T) {
	base := &windowSealStore{fakeCommitStore: newFakeCommitStore(), sealed: make(chan struct{}, 1)}
	bus := NewCommitEventBus(8)
	defer bus.Close()
	w := NewTxnWriterForStore(WithCommitEvents(base, bus))
	require.NoError(t, w.SetAt([]byte("key"), []byte("value"), 0, 10))
	done := make(chan error, 1)
	go func() { done <- w.Flush() }()
	completed := false
	defer func() {
		if !completed {
			base.complete(0, nil)
			<-done
		}
	}()
	select {
	case <-base.sealed:
	case <-time.After(time.Second):
		t.Fatal("producer window was not sealed before callback wait")
	}
	select {
	case err := <-done:
		completed = true
		base.complete(0, nil)
		t.Fatalf("seal acknowledged an incomplete write: %v", err)
	default:
	}
	base.complete(0, nil)
	require.NoError(t, <-done)
	completed = true
}
