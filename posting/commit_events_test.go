/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitEventBusOrdersOutOfOrderCompletionsAndSkipsFailures(t *testing.T) {
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 3)
	require.NoError(t, err)

	first := bus.admit(10, []CommitMutation{{Entry: Entry{Key: []byte("a")}}})
	failed := bus.admit(11, []CommitMutation{{Entry: Entry{Key: []byte("bad")}}})
	third := bus.admit(12, []CommitMutation{{Entry: Entry{Key: []byte("c")}}})
	bus.complete(commitCompletion{event: third})
	bus.complete(commitCompletion{event: failed, err: errors.New("commit failed")})

	select {
	case event := <-events:
		t.Fatalf("published before first completion: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
	bus.complete(commitCompletion{event: first})
	require.Equal(t, first, <-events)
	require.Equal(t, third, <-events)
}

func TestCommitEventDecoratorCopiesMutationsAndPublishesSuccessfulCommit(t *testing.T) {
	base := newFakeCommitStore()
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	store := WithCommitEvents(base, bus)

	key, value := []byte("key"), []byte("value")
	txn := store.NewWriteTxn()
	require.NoError(t, txn.SetEntry(Entry{Key: key, Value: value, UserMeta: 7}))
	done := make(chan error, 1)
	require.NoError(t, txn.CommitAt(42, func(err error) { done <- err }))
	key[0], value[0] = 'X', 'X'
	base.complete(0, nil)
	require.NoError(t, <-done)

	event := <-events
	require.Equal(t, uint64(42), event.CommitTs)
	require.Equal(t, []byte("key"), event.Mutations[0].Entry.Key)
	require.Equal(t, []byte("value"), event.Mutations[0].Entry.Value)
	require.Equal(t, byte(7), event.Mutations[0].Entry.UserMeta)
}

func TestCommitEventDecoratorPreservesSynchronousCommitBlocking(t *testing.T) {
	base := newFakeCommitStore()
	base.syncRelease = make(chan struct{})
	bus := NewCommitEventBus(8)
	defer bus.Close()
	events, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	txn := WithCommitEvents(base, bus).NewWriteTxn()
	require.NoError(t, txn.SetEntry(Entry{Key: []byte("key"), Value: []byte("value")}))
	done := make(chan error, 1)
	go func() { done <- txn.CommitAt(9, nil) }()

	select {
	case err := <-done:
		t.Fatalf("synchronous commit returned before storage release: %v", err)
	case event := <-events:
		t.Fatalf("event published before storage release: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
	close(base.syncRelease)
	require.NoError(t, <-done)
	require.Equal(t, uint64(9), (<-events).CommitTs)
}

func TestCommitEventSubscriptionBoundaryIsWriteTxnCreation(t *testing.T) {
	base := newFakeCommitStore()
	bus := NewCommitEventBus(8)
	defer bus.Close()
	store := WithCommitEvents(base, bus)
	createdBeforeSubscribe := store.NewWriteTxn()

	events, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	require.NoError(t, createdBeforeSubscribe.CommitAt(8, nil))
	select {
	case event := <-events:
		t.Fatalf("published a transaction created before subscription: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}

	createdAfterSubscribe := store.NewWriteTxn()
	require.NoError(t, createdAfterSubscribe.SetEntry(Entry{Key: []byte("covered")}))
	done := make(chan error, 1)
	require.NoError(t, createdAfterSubscribe.CommitAt(9, func(err error) { done <- err }))
	base.complete(0, nil)
	require.NoError(t, <-done)
	require.Equal(t, uint64(9), (<-events).CommitTs)
}

func TestCommitEventSubscribeReturnEstablishesInstrumentationBoundary(t *testing.T) {
	base := newFakeCommitStore()
	bus := NewCommitEventBus(8)
	defer bus.Close()
	store := WithCommitEvents(base, bus)
	events, err := bus.Subscribe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), bus.SubscriberCount())

	txn := store.NewWriteTxn()
	require.NoError(t, txn.SetEntry(Entry{Key: []byte("first-after-subscribe")}))
	require.NoError(t, txn.CommitAt(10, nil))
	select {
	case event := <-events:
		require.Equal(t, uint64(10), event.CommitTs)
	case <-time.After(time.Second):
		t.Fatal("first transaction created after Subscribe returned was not instrumented")
	}
}

func TestCommitEventBusBackpressureCancellationAndShutdown(t *testing.T) {
	bus := NewCommitEventBus(1)
	ctx, cancel := context.WithCancel(context.Background())
	events, err := bus.Subscribe(ctx, 0)
	require.NoError(t, err)

	first := bus.admit(1, nil)
	go bus.complete(commitCompletion{event: first})
	secondAdmitted := make(chan CommitEvent, 1)
	go func() { secondAdmitted <- bus.admit(2, nil) }()
	select {
	case <-secondAdmitted:
		t.Fatal("second commit admitted while the subscriber held all bus capacity")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	second := <-secondAdmitted
	bus.complete(commitCompletion{event: second})

	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				goto closed
			}
		case <-deadline:
			t.Fatal("subscriber did not close after cancellation")
		}
	}
closed:
	done := make(chan struct{})
	go func() { bus.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown remained blocked by subscriber backpressure")
	}
}

func TestWithCommitEventsDisabledReturnsOriginalStore(t *testing.T) {
	store := newFakeCommitStore()
	require.Same(t, store, WithCommitEvents(store, nil))
}

type fakeCommitStore struct {
	mu          sync.Mutex
	commits     []func(error)
	syncRelease chan struct{}
	txn         fakeCommitTxn
}

func newFakeCommitStore() *fakeCommitStore {
	s := &fakeCommitStore{}
	s.txn.store = s
	return s
}
func (s *fakeCommitStore) NewReadTxn(uint64) ReadTxn { return nil }
func (s *fakeCommitStore) IsClosed() bool            { return false }
func (s *fakeCommitStore) NewWriteTxn() WriteTxn     { return &s.txn }
func (s *fakeCommitStore) complete(index int, err error) {
	s.mu.Lock()
	cb := s.commits[index]
	s.mu.Unlock()
	cb(err)
}

type fakeCommitTxn struct{ store *fakeCommitStore }

func (t *fakeCommitTxn) SetEntry(Entry) error { return nil }
func (t *fakeCommitTxn) Delete([]byte) error  { return nil }
func (t *fakeCommitTxn) Discard()             {}
func (t *fakeCommitTxn) CommitAt(_ uint64, cb func(error)) error {
	if cb == nil {
		if t.store.syncRelease != nil {
			<-t.store.syncRelease
		}
		return nil
	}
	t.store.mu.Lock()
	t.store.commits = append(t.store.commits, cb)
	t.store.mu.Unlock()
	return nil
}
