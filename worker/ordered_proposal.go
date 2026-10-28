/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/golang/glog"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"google.golang.org/protobuf/proto"
)

// markProposalCommitted belongs to the Ready owner, after the exact entry has
// reached CommittedEntries. A proposal send/return cannot substitute for it.
func (n *node) markProposalCommitted(key, index uint64) {
	if pctx := n.Proposals.Get(key); pctx != nil {
		atomic.AddUint32(&pctx.Found, 1)
		if pctx.Committed != nil {
			select {
			case pctx.Committed <- index:
			default:
			}
		}
	}
}

type orderedProposal struct {
	committed      chan uint64
	done           chan struct{}
	err            error  // read only after done; closing the channel transfers ownership.
	committedIndex uint64 // admission-owner only; waitCommitted is idempotent.
}

func (n *node) startOrderedProposal(ctx context.Context, proposal *pb.Proposal) *orderedProposal {
	p := &orderedProposal{committed: make(chan uint64, 1), done: make(chan struct{})}
	// One immutable original aggregate and its proposal key remain owned by
	// proposeAndWaitCommitted throughout retries. This goroutine never applies.
	go func() {
		p.err = n.proposeAndWaitCommitted(ctx, proposal, p.committed)
		close(p.done)
	}()
	return p
}
func (p *orderedProposal) waitCommitted(ctx context.Context) error {
	if p.committedIndex != 0 {
		return nil
	}
	select {
	case index := <-p.committed:
		p.committedIndex = index
		return nil
	case <-p.done:
		return p.err // successful apply implies prior commitment.
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *orderedProposal) wait() error { <-p.done; return p.err }

// orderedDeltaAdmission owns at most three original proposal operations. The
// stream's existing receiver/batching and the apply owner retain their roles.
type orderedDeltaAdmission struct {
	n       *node
	ctx     context.Context
	pending []*orderedProposal
	sizes   []int
	bytes   int
}

func (w *orderedDeltaAdmission) retireFirst() error {
	err := w.pending[0].wait()
	w.bytes -= w.sizes[0]
	w.pending[0] = nil
	w.sizes[0] = 0
	w.pending = w.pending[1:]
	w.sizes = w.sizes[1:]
	return err
}
func (w *orderedDeltaAdmission) beforeNext() error {
	if len(w.pending) > 0 {
		if err := w.pending[len(w.pending)-1].waitCommitted(w.ctx); err != nil {
			return err
		}
	}
	for len(w.pending) > 0 {
		select {
		case <-w.pending[0].done:
			if err := w.retireFirst(); err != nil {
				return err
			}
		default:
			if len(w.pending) < 3 {
				return nil
			}
			return w.retireFirst()
		}
	}
	return nil
}
func (w *orderedDeltaAdmission) drain() error {
	var err error
	for len(w.pending) > 0 {
		err = errors.Join(err, w.retireFirst())
	}
	return err
}
func (w *orderedDeltaAdmission) submit(delta *pb.OracleDelta) (bool, error) {
	return w.submitContext(w.ctx, delta)
}

func (w *orderedDeltaAdmission) submitContext(ctx context.Context, delta *pb.OracleDelta) (bool, error) {
	p := &pb.Proposal{Delta: delta}
	size := proto.Size(p) + 8
	if size > (64<<10)-w.bytes {
		if err := w.drain(); err != nil {
			return false, err
		}
	}
	if size > 64<<10 {
		return false, nil
	}
	w.pending = append(w.pending, w.n.startOrderedProposal(ctx, p))
	w.sizes = append(w.sizes, size)
	w.bytes += size
	return true, nil
}

func (n *node) orderedDeltaEnabled() bool {
	if !n.orderedDelta || raftCheckpointEnabled() || x.WorkerConfig.HardSync ||
		Config.PostingStoreBackend != PostingStoreBackendTreeDB || Config.PostingStoreDurability != PostingStoreDurabilityDurable || State.TreeDBStore == nil {
		return false
	}
	status := State.TreeDBStore.Status()
	return status.DurableCommits && status.DurabilityMode == "wal_on_sync" && !status.Closed
}

// Selected stream proposals retain their registration between retry epochs. A
// normal pre-commit leadership change must neither lose the original aggregate
// nor terminate the Alpha. Only the original apply result acknowledges it.
func (n *node) proposeOrderedBytes(ctx context.Context, key uint64, data []byte, committed chan<- uint64) error {
	proposalCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pctx := &conn.ProposalCtx{Ctx: proposalCtx, ErrCh: make(chan error, 1), Committed: committed}
	x.AssertTruef(n.Proposals.Store(key, pctx), "Found existing proposal with key: [%x]", key)
	defer n.Proposals.Delete(key)
	return waitOrderedProposal(ctx, pctx, func() error { return n.Raft().Propose(proposalCtx, data) }, func(retry int) time.Duration {
		return newTimeout(retry)
	})
}

// retryDelay is supplied explicitly so transport/drop tests can exercise the
// actual ownership loop without waiting for production timeout durations.
func waitOrderedProposal(ctx context.Context, pctx *conn.ProposalCtx, propose func() error, retryDelay func(int) time.Duration) error {
	for retry := 0; ; {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if atomic.LoadUint32(&pctx.Found) == 0 {
			if err := propose(); err != nil {
				glog.Warningf("Retrying ordered Delta proposal before commitment: %v", err)
			}
		}
		timer := time.NewTimer(retryDelay(retry))
		for {
			select {
			case err := <-pctx.ErrCh:
				timer.Stop()
				return err
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				if atomic.LoadUint32(&pctx.Found) == 0 {
					goto retryProposal
				}
				// Committed ownership cannot be retried or acknowledged by a timeout.
				// With the timer consumed, only original Done or shutdown can finish it.
			}
		}
	retryProposal:
		if retry < 2 {
			retry++
		}
	}
}
