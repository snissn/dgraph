/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

// Each record owns original requests and the engine's original completion.
// Only the submitter mutates an available/open record. Sending it to the one
// completion owner transfers exclusive custody until it is returned to free.
type treeDBCommitAggregate struct {
	requests   [treeDBCommitOwnershipLimit]treeDBCommitRequest
	groups     [treeDBCommitOwnershipLimit]mvcc.CommitGroup
	results    [treeDBCommitOwnershipLimit]error
	completion mvcc.CommitCompletion
	builder    mvcc.CommitBuilder
	buildErr   error
	building   bool
	n          int
	await      bool
	ownedBytes int
}

// reserveCommitStaging shares one allowance across the installed and open
// records. Original queue inputs retain their separate request ownership.
// An oversized atomic request is staged alone after the previous record drains.
func (s *TreeDBStore) reserveCommitStaging(size int) bool {
	for used := s.commitStagedBytes.Load(); ; used = s.commitStagedBytes.Load() {
		if used != 0 && (size > treeDBCommitCohortBytes || used > int64(treeDBCommitCohortBytes-size)) {
			return false
		}
		if s.commitStagedBytes.CompareAndSwap(used, used+int64(size)) {
			return true
		}
	}
}

func (s *TreeDBStore) wakeCommitSubmitter() {
	select {
	case s.commitWake <- struct{}{}:
	default:
	}
}

// runDurableCommitQueue preserves a ready-work reservoir while separating
// ordered append/install from the physical completion wait. There are exactly
// two reusable records, one submitter and one completion owner. An open
// successor is not sealed merely because its input queue is temporarily empty.
func (s *TreeDBStore) runDurableCommitQueue() {
	if !s.stagedCommitGroups {
		s.runDurableCommitQueueFIFO()
		return
	}
	var records [2]treeDBCommitAggregate
	free := make(chan *treeDBCommitAggregate, len(records))
	ready := make(chan *treeDBCommitAggregate, len(records))
	finished := make(chan struct{})
	for i := range records {
		free <- &records[i]
	}
	go func() {
		defer close(finished)
		for aggregate := range ready {
			s.finishDurableAggregate(aggregate)
			free <- aggregate
			s.wakeCommitSubmitter()
		}
	}()
	defer func() {
		close(ready)
		<-finished
	}()
	var carried treeDBCommitRequest
	hasCarried := false
	var predecessor uint64
	queue := s.commitQueue
	for {
		aggregate := <-free
		first := carried
		if !hasCarried {
			var ok bool
			first, ok = <-queue
			if !ok {
				return
			}
		}
		carried, hasCarried = treeDBCommitRequest{}, false
		aggregate.requests[0], aggregate.n = first, 1
		ownedBytes, work := first.batch.ownedBytes(), first.batch.work
		for !s.reserveCommitStaging(ownedBytes) {
			<-s.commitWake
		}
		aggregate.ownedBytes = ownedBytes
		// An idle cut already has all its ready operands available. Keep its
		// native allocation sized once at Submit. Only a successor overlaps
		// incremental owned staging with an outstanding durability barrier.
		incremental := predecessor != 0 && s.commitCompleted.Load() < predecessor
		if incremental {
			s.stageDurableRequest(aggregate, 0)
		}
	collect:
		for aggregate.n < len(aggregate.requests) && ownedBytes < treeDBCommitCohortBytes && work < s.writeWorkLimit {
			var request treeDBCommitRequest
			var ok bool
			select {
			case request, ok = <-queue:
			default:
				if predecessor == 0 || s.commitCompleted.Load() >= predecessor || s.closed.Load() ||
					len(s.commitSlots) == cap(s.commitSlots) {
					break collect
				}
				select {
				case request, ok = <-queue:
				case <-s.commitWake:
					continue
				}
			}
			if !ok {
				queue = nil
				break collect
			}
			size := request.batch.ownedBytes()
			if size > treeDBCommitCohortBytes-ownedBytes || work > s.writeWorkLimit || request.batch.work > s.writeWorkLimit-work || !s.reserveCommitStaging(size) {
				carried, hasCarried = request, true
				break collect
			}
			aggregate.requests[aggregate.n] = request
			if incremental {
				s.stageDurableRequest(aggregate, aggregate.n)
			}
			aggregate.n++
			aggregate.ownedBytes += size
			ownedBytes += size
			work += request.batch.work
		}
		s.submitDurableAggregate(aggregate)
		if aggregate.await && predecessor != 0 && s.commitCompleted.Load() < predecessor {
			s.durableOverlap.Add(1)
		}
		predecessor = aggregate.requests[aggregate.n-1].sequence
		ready <- aggregate
		if queue == nil && !hasCarried {
			return
		}
	}
}

// The builder owns only root-independent native operands while arrivals are
// collected. It constructs the root-dependent postimage once, after sealing.
func (s *TreeDBStore) stageDurableRequest(a *treeDBCommitAggregate, i int) {
	r := &a.requests[i]
	a.groups[i] = mvcc.CommitGroup{Timestamp: r.commitTs, Mutations: r.batch.mutations}
	if s.commitGroupsForTest != nil || a.buildErr != nil {
		return
	}
	if !a.building {
		a.buildErr = s.mvcc.BeginCommitGroup(&a.builder)
		if a.buildErr != nil {
			return
		}
		a.building = true
	}
	if err := a.builder.AppendGroup(a.groups[i]); err != nil {
		a.buildErr = errors.Join(err, a.builder.Abort())
		a.building = false
	}
}

func treeDBGroupValidationError(err error) bool {
	return !errors.Is(err, mvcc.ErrStorage) &&
		(errors.Is(err, mvcc.ErrZeroTimestamp) || errors.Is(err, mvcc.ErrInvalidKey) ||
			errors.Is(err, mvcc.ErrDuplicateKey) || errors.Is(err, mvcc.ErrVersionBelowDiscardFloor))
}

func (s *TreeDBStore) submitDurableAggregate(a *treeDBCommitAggregate) {
	s.durableCohorts.Add(1)
	s.durableRequests.Add(uint64(a.n))
	if uint64(a.n) > s.durableCohortMax.Load() {
		s.durableCohortMax.Store(uint64(a.n))
	}
	// Keep the existing test injection at its original synchronous ambiguity
	// boundary. Production always uses the actual native submit/completion seam.
	if s.commitGroupsForTest != nil && a.n == 1 {
		a.results[0] = s.commitAtAdmitted(a.requests[0].commitTs, a.requests[0].batch.mutations)
		return
	}
	for i := range a.n {
		r := &a.requests[i]
		if s.beforeCommitForTest != nil {
			s.beforeCommitForTest()
		}
		if s.commitStartedForTest != nil {
			s.commitStartedForTest(r.commitTs, r.batch.mutations)
		}
		a.groups[i] = mvcc.CommitGroup{Timestamp: r.commitTs, Mutations: r.batch.mutations}
	}
	if s.commitGroupsForTest != nil {
		s.commitDurableGroups(a.groups[:a.n], a.results[:a.n])
		return
	}
	err := a.buildErr
	if err == nil {
		if a.building {
			err = a.builder.Submit(s.commitMode, &a.completion)
			a.building = false // Submit transfers custody or releases it on error.
		} else {
			err = s.mvcc.SubmitCommitGroupAt(a.groups[:a.n], s.commitMode, &a.completion)
		}
	}
	if err == nil {
		a.await = true
		return
	}
	// Only a wholly pre-storage validation refusal permits the existing
	// sequential subdivision. Capacity and every storage/ambiguous result stay
	// terminal for these original requests; no assigned command is retried.
	if a.n > 1 && treeDBGroupValidationError(err) {
		s.commitDurableGroups(a.groups[:a.n], a.results[:a.n])
		return
	}
	err = fmt.Errorf("commit posting TreeDB transaction: %w", err)
	for i := range a.n {
		a.results[i] = err
	}
}

func (s *TreeDBStore) finishDurableAggregate(a *treeDBCommitAggregate) {
	if a.await {
		if err := a.completion.Complete(); err != nil {
			err = fmt.Errorf("commit posting TreeDB transaction: %w", err)
			for i := range a.n {
				a.results[i] = err
			}
		}
	}
	last := a.requests[a.n-1].sequence
	for i := range a.n {
		if s.commitFinishedForTest != nil && !(s.commitGroupsForTest != nil && a.n == 1) {
			s.commitFinishedForTest(a.requests[i].commitTs, a.results[i])
		}
		completion := treeDBCommitCompletion{request: a.requests[i], err: a.results[i]}
		s.completeTreeDBCommit(completion)
		s.deliverTreeDBCommit(completion)
		a.requests[i], a.groups[i], a.results[i] = treeDBCommitRequest{}, mvcc.CommitGroup{}, nil
	}
	s.commitStagedBytes.Add(-int64(a.ownedBytes))
	a.ownedBytes = 0
	a.buildErr, a.building = nil, false
	a.n, a.await = 0, false
	s.commitCompleted.Store(last)
}
