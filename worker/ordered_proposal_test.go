/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
)

// More than the old three dropped epochs must retain this exact registration.
// Transport/leadership errors are pre-commit retries, never apply failures.
func TestOrderedProposalRetriesDropsAndTransportBeforeFound(t *testing.T) {
	for _, transportError := range []bool{false, true} {
		t.Run(map[bool]string{false: "dropped", true: "leadership_transport"}[transportError], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pctx := &conn.ProposalCtx{ErrCh: make(chan error, 1)}
			calls := make(chan int, 16)
			var count atomic.Int32
			done := make(chan error, 1)
			go func() {
				done <- waitOrderedProposal(ctx, pctx, func() error {
					call := int(count.Add(1))
					calls <- call
					if call == 5 {
						atomic.StoreUint32(&pctx.Found, 1)
					}
					if transportError {
						return errors.New("leader changed")
					}
					return nil
				}, func(int) time.Duration { return time.Millisecond })
			}()
			for i := 1; i <= 5; i++ {
				select {
				case call := <-calls:
					require.Equal(t, i, call)
				case <-time.After(time.Second):
					t.Fatal("retry lost ownership")
				}
			}
			// Found confers ordering only: neither timeout nor leadership loss ACKs.
			select {
			case err := <-done:
				t.Fatalf("Found prematurely finished: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			require.Equal(t, int32(5), count.Load())
			pctx.ErrCh <- nil
			require.NoError(t, <-done)
		})
	}
}

func TestOrderedProposalCancellationAndOriginalApplyFailure(t *testing.T) {
	for _, found := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_found", true: "after_found"}[found], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			pctx := &conn.ProposalCtx{ErrCh: make(chan error, 1)}
			if found {
				atomic.StoreUint32(&pctx.Found, 1)
			}
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				close(started)
				done <- waitOrderedProposal(ctx, pctx, func() error { return nil }, func(int) time.Duration { return time.Hour })
			}()
			<-started
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
		})
	}
	pctx := &conn.ProposalCtx{Found: 1, ErrCh: make(chan error, 1)}
	failure := errors.New("accepted materialization failed")
	pctx.ErrCh <- failure
	require.ErrorIs(t, waitOrderedProposal(context.Background(), pctx, func() error { t.Fatal("reproposed committed entry"); return nil }, func(int) time.Duration { return time.Hour }), failure)
}

func TestOrderedAdmissionWaitsCommittedAndRetainsPredecessorOnStreamReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &orderedProposal{committed: make(chan uint64, 1), done: make(chan struct{})}
	w := &orderedDeltaAdmission{ctx: ctx, pending: []*orderedProposal{a}, sizes: []int{10}, bytes: 10}
	next := make(chan error, 1)
	go func() { next <- w.beforeNext() }()
	select {
	case <-next:
		t.Fatal("admitted before Found")
	case <-time.After(10 * time.Millisecond):
	}
	a.committed <- 7
	require.NoError(t, <-next)
	// Stream cancellation/reconnect uses group context; it cannot release accepted
	// ownership or allow a new stream's admission to bypass pending A's apply.
	drained := make(chan error, 1)
	go func() { drained <- w.drain() }()
	select {
	case <-drained:
		t.Fatal("reconnect passed pending A")
	case <-time.After(10 * time.Millisecond):
	}
	close(a.done)
	require.NoError(t, <-drained)
	require.Zero(t, w.bytes)
	require.Empty(t, w.pending)
}

// A Raft send may accept bytes before returning a transport error. Found must
// still find the original live registration, and apply must keep its operation
// context after stream cancellation or leadership replacement.
type orderedRetryRaft struct {
	raft.Node
	propose func(context.Context, []byte) error
}

func (r orderedRetryRaft) Propose(ctx context.Context, data []byte) error {
	return r.propose(ctx, data)
}
func TestOrderedProposalAcceptedThenTransportErrorRetainsRegistration(t *testing.T) {
	n := &node{Node: new(conn.Node)}
	accepted := make(chan *conn.ProposalCtx, 1)
	var calls atomic.Int32
	const key = 123
	data := []byte("immutable original")
	n.SetRaft(orderedRetryRaft{propose: func(operation context.Context, got []byte) error {
		require.Equal(t, data, got)
		calls.Add(1)
		pctx := n.Proposals.Get(key)
		require.NotNil(t, pctx)
		require.Equal(t, operation, pctx.Ctx)
		n.markProposalCommitted(key, 9)
		accepted <- pctx
		return errors.New("accepted before transport loss")
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	committed := make(chan uint64, 1)
	done := make(chan error, 1)
	go func() { done <- n.proposeOrderedBytes(ctx, key, data, committed) }()
	pctx := <-accepted
	require.Equal(t, uint64(9), <-committed)
	require.NoError(t, pctx.Ctx.Err())
	require.Same(t, pctx, n.Proposals.Get(key))
	n.Proposals.Done(key, nil)
	require.NoError(t, <-done)
	require.Equal(t, int32(1), calls.Load())
	require.Nil(t, n.Proposals.Get(key))
	require.ErrorIs(t, pctx.Ctx.Err(), context.Canceled)
}
