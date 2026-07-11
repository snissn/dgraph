/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrCommitEventBusClosed = errors.New("posting commit event bus is closed")

// CommitMutation is an owned copy of one mutation made durable by a successful
// posting-store commit. A nil Value denotes a delete.
type CommitMutation struct {
	Entry  Entry
	Delete bool
}

// CommitEvent describes one successful posting-store commit. Sequence follows
// CommitAt admission order, including across commits whose callbacks complete
// out of order.
type CommitEvent struct {
	Sequence  uint64
	CommitTs  uint64
	Mutations []CommitMutation
	admitted  bool
}

type commitCompletion struct {
	event CommitEvent
	err   error
}

type commitSubscriber struct {
	ctx        context.Context
	ch         chan CommitEvent
	registered chan struct{}
}

// CommitEventBus is an in-process, non-durable post-commit broadcaster owned by
// Dgraph. Subscriber backpressure is intentional and preserves commit order.
type CommitEventBus struct {
	ctx    context.Context
	cancel context.CancelFunc

	nextSequence   atomic.Uint64
	subscribers    atomic.Int64
	completions    chan commitCompletion
	admissionSlots chan struct{}
	add            chan *commitSubscriber
	remove         chan *commitSubscriber
	done           chan struct{}
	closeOnce      sync.Once
	inflight       sync.WaitGroup
	admissionMu    sync.Mutex
	closing        bool
}

func NewCommitEventBus(queueDepth int) *CommitEventBus {
	if queueDepth < 1 {
		queueDepth = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &CommitEventBus{
		ctx:            ctx,
		cancel:         cancel,
		completions:    make(chan commitCompletion, queueDepth),
		admissionSlots: make(chan struct{}, queueDepth),
		add:            make(chan *commitSubscriber),
		remove:         make(chan *commitSubscriber),
		done:           make(chan struct{}),
	}
	b.nextSequence.Store(1)
	go b.run()
	return b
}

// SubscriberCount reports the number of currently registered subscribers.
// It is intended for runtime diagnostics and readiness tests.
func (b *CommitEventBus) SubscriberCount() int64 {
	if b == nil {
		return 0
	}
	return b.subscribers.Load()
}

func (b *CommitEventBus) admit(commitTs uint64, mutations []CommitMutation) CommitEvent {
	select {
	case b.admissionSlots <- struct{}{}:
	case <-b.ctx.Done():
		return CommitEvent{}
	}
	b.admissionMu.Lock()
	defer b.admissionMu.Unlock()
	if b.closing {
		<-b.admissionSlots
		return CommitEvent{}
	}
	b.inflight.Add(1)
	return CommitEvent{
		Sequence:  b.nextSequence.Add(1) - 1,
		CommitTs:  commitTs,
		Mutations: mutations,
		admitted:  true,
	}
}

func (b *CommitEventBus) complete(completion commitCompletion) {
	if !completion.event.admitted {
		return
	}
	defer b.inflight.Done()
	select {
	case b.completions <- completion:
	case <-b.ctx.Done():
		<-b.admissionSlots
	}
}

// Subscribe registers an ordered subscriber. The returned channel is closed
// after ctx cancellation or bus shutdown.
func (b *CommitEventBus) Subscribe(ctx context.Context, buffer int) (<-chan CommitEvent, error) {
	if buffer < 0 {
		buffer = 0
	}
	sub := &commitSubscriber{
		ctx: ctx, ch: make(chan CommitEvent, buffer), registered: make(chan struct{}),
	}
	select {
	case b.add <- sub:
	case <-ctx.Done():
		close(sub.ch)
		return sub.ch, ctx.Err()
	case <-b.ctx.Done():
		close(sub.ch)
		return sub.ch, ErrCommitEventBusClosed
	}
	// Sending on add only hands the request to run; it does not by itself
	// guarantee that NewWriteTxn observes the incremented subscriber count.
	// A successful Subscribe return establishes that instrumentation boundary.
	select {
	case <-sub.registered:
	case <-b.done:
		return sub.ch, ErrCommitEventBusClosed
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-b.ctx.Done():
		}
		select {
		case b.remove <- sub:
		case <-b.done:
		}
	}()
	return sub.ch, nil
}

// Close cancels subscriber delivery (including backpressure), waits for every
// admitted storage callback to leave the bus, closes subscribers, and waits for
// the coordinator to exit. Events not yet delivered at shutdown are ephemeral
// by contract and may be dropped.
func (b *CommitEventBus) Close() {
	if b == nil {
		return
	}
	b.closeOnce.Do(func() {
		b.admissionMu.Lock()
		b.closing = true
		b.cancel()
		b.admissionMu.Unlock()
	})
	b.inflight.Wait()
	<-b.done
}

func (b *CommitEventBus) run() {
	defer close(b.done)
	subs := make(map[*commitSubscriber]struct{})
	pending := make(map[uint64]commitCompletion)
	next := uint64(1)
	defer func() {
		for sub := range subs {
			close(sub.ch)
		}
	}()

	remove := func(sub *commitSubscriber) {
		if _, ok := subs[sub]; ok {
			delete(subs, sub)
			b.subscribers.Add(-1)
			close(sub.ch)
		}
	}
	publish := func(event CommitEvent) bool {
		for sub := range subs {
			select {
			case sub.ch <- event:
			case <-sub.ctx.Done():
				remove(sub)
			case <-b.ctx.Done():
				return false
			}
		}
		return true
	}

	for {
		select {
		case <-b.ctx.Done():
			return
		case sub := <-b.add:
			subs[sub] = struct{}{}
			b.subscribers.Add(1)
			close(sub.registered)
		case sub := <-b.remove:
			remove(sub)
		case completion := <-b.completions:
			pending[completion.event.Sequence] = completion
			for {
				completion, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				next++
				if completion.err == nil && !publish(completion.event) {
					return
				}
				<-b.admissionSlots
			}
		}
	}
}

// WithCommitEvents decorates store with successful post-commit publication.
// A nil bus returns store unchanged, preserving the disabled hot path. Events
// cover transactions created while at least one subscriber is registered;
// subscribing after NewWriteTxn does not retroactively instrument that txn.
func WithCommitEvents(store Store, bus *CommitEventBus) Store {
	if bus == nil {
		return store
	}
	return eventStore{Store: store, bus: bus}
}

type eventStore struct {
	Store
	bus *CommitEventBus
}

func (s eventStore) NewWriteTxn() WriteTxn {
	// Define the subscription boundary at transaction creation so the enabled
	// but unsubscribed path remains allocation-free.
	if s.bus.subscribers.Load() == 0 {
		return s.Store.NewWriteTxn()
	}
	return &eventWriteTxn{WriteTxn: s.Store.NewWriteTxn(), bus: s.bus}
}

type eventWriteTxn struct {
	WriteTxn
	bus       *CommitEventBus
	mutations []CommitMutation
}

func (t *eventWriteTxn) SetEntry(entry Entry) error {
	if err := t.WriteTxn.SetEntry(entry); err != nil {
		return err
	}
	entry.Key = append([]byte(nil), entry.Key...)
	entry.Value = append([]byte(nil), entry.Value...)
	t.mutations = append(t.mutations, CommitMutation{Entry: entry})
	return nil
}

func (t *eventWriteTxn) Delete(key []byte) error {
	if err := t.WriteTxn.Delete(key); err != nil {
		return err
	}
	t.mutations = append(t.mutations, CommitMutation{
		Entry:  Entry{Key: append([]byte(nil), key...)},
		Delete: true,
	})
	return nil
}

func (t *eventWriteTxn) CommitAt(commitTs uint64, cb func(error)) error {
	event := t.bus.admit(commitTs, t.mutations)
	if !event.admitted {
		return t.WriteTxn.CommitAt(commitTs, cb)
	}
	if cb == nil {
		err := t.WriteTxn.CommitAt(commitTs, nil)
		t.bus.complete(commitCompletion{event: event, err: err})
		return err
	}
	var once sync.Once
	complete := func(err error) {
		once.Do(func() {
			t.bus.complete(commitCompletion{event: event, err: err})
			cb(err)
		})
	}
	err := t.WriteTxn.CommitAt(commitTs, complete)
	if err != nil {
		complete(err)
	}
	return err
}
