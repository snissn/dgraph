/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"encoding/binary"
	"errors"
	"maps"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var errOrderedDeltaIncomplete = errors.New("ordered Delta window incomplete; replay required")

// These bounds cover immutable speculative custody, not the original live
// Oracle caches or process memory. One apply owner retains all finalizers.
const (
	orderedDeltaMaxProposals  = 3
	orderedDeltaMaxInput      = 64 << 10
	orderedDeltaMaxSnapshots  = 1 << 20
	orderedDeltaMaxMapEntries = 256
)

type orderedDeltaWindow struct {
	previous                              *pb.Proposal
	lastIndex                             uint64
	proposalKeys                          map[uint64]bool
	starts                                map[uint64]bool
	snapshots                             []*posting.DeltaSnapshot
	inputBytes, snapshotBytes, mapEntries int
}

func (w *orderedDeltaWindow) admit(n *node, e raftpb.Entry) (*pb.Proposal, uint64, map[uint64]*posting.DeltaSnapshot) {
	if e.Type != raftpb.EntryNormal || len(e.Data) < 8 || len(e.Data) > orderedDeltaMaxInput-w.inputBytes {
		return nil, 0, nil
	}
	key := binary.BigEndian.Uint64(e.Data[:8])
	if _, seen := n.appliedProposals[key]; seen || key == 0 || w.proposalKeys[key] {
		return nil, 0, nil
	}
	var p pb.Proposal
	if proto.Unmarshal(e.Data[8:], &p) != nil || p.Delta == nil || len(p.Delta.Txns) == 0 {
		return nil, 0, nil
	}
	if !proto.Equal(&p, &pb.Proposal{Delta: p.Delta, Index: p.Index, StartTs: p.StartTs}) {
		return nil, 0, nil
	}
	d := p.Delta
	if !proto.Equal(d, &pb.OracleDelta{Txns: d.Txns, MaxAssigned: d.MaxAssigned, GroupChecksums: d.GroupChecksums, ChecksumSequence: d.ChecksumSequence}) {
		return nil, 0, nil
	}
	if d.MaxAssigned < posting.Oracle().MaxAssigned() {
		return nil, 0, nil
	}
	if w.previous != nil {
		old := w.previous.Delta
		// Zero's sequence is its Raft proposal counter, not a membership-change
		// counter. A later source sequence may carry the identical complete map.
		// Preparation touches only disjoint posting payloads; each original
		// finalizer still publishes its own metadata after its durable callback.
		// Preserve barriers for regressions and every map change (including
		// another group's checksum or contradictory same-sequence metadata).
		if e.Index != w.lastIndex+1 || d.MaxAssigned < old.MaxAssigned || d.ChecksumSequence < old.ChecksumSequence || !maps.Equal(d.GroupChecksums, old.GroupChecksums) {
			return nil, 0, nil
		}
	}
	// Each admitted transaction owns at least one payload-map entry. Reject
	// before allocating per-status custody beyond the whole-window map bound.
	if len(d.Txns) > orderedDeltaMaxMapEntries-w.mapEntries {
		return nil, 0, nil
	}
	owned := make(map[uint64]*posting.DeltaSnapshot)
	copies := make([]*posting.DeltaSnapshot, 0, len(d.Txns))
	bytes, entries := w.snapshotBytes, w.mapEntries
	var previousCommit uint64
	if w.previous != nil {
		previousCommit = w.previous.Delta.MaxAssigned
	}
	for _, tx := range d.Txns {
		if tx == nil || tx.StartTs == 0 || tx.CommitTs <= tx.StartTs || tx.CommitTs <= previousCommit || tx.CommitTs > d.MaxAssigned || w.starts[tx.StartTs] || owned[tx.StartTs] != nil {
			return nil, 0, nil
		}
		if !proto.Equal(tx, &pb.TxnStatus{StartTs: tx.StartTs, CommitTs: tx.CommitTs}) {
			return nil, 0, nil
		}
		snapshot, size, ok := posting.Oracle().GetTxn(tx.StartTs).SnapshotDelta(64, orderedDeltaMaxSnapshots-bytes, orderedDeltaMaxMapEntries-entries)
		if !ok {
			return nil, 0, nil
		}
		entries += snapshot.MapEntries()
		if entries > orderedDeltaMaxMapEntries {
			return nil, 0, nil
		}
		for _, other := range w.snapshots {
			if !snapshot.Disjoint(other) {
				return nil, 0, nil
			}
		}
		for _, other := range copies {
			if !snapshot.Disjoint(other) {
				return nil, 0, nil
			}
		}
		bytes += size
		previousCommit = tx.CommitTs
		owned[tx.StartTs] = snapshot
		copies = append(copies, snapshot)
	}
	if w.proposalKeys == nil {
		w.proposalKeys = make(map[uint64]bool)
		w.starts = make(map[uint64]bool)
	}
	w.proposalKeys[key] = true
	for start := range owned {
		w.starts[start] = true
	}
	w.snapshots = append(w.snapshots, copies...)
	w.inputBytes += len(e.Data)
	w.snapshotBytes, w.mapEntries = bytes, entries
	p.Index = e.Index
	w.previous, w.lastIndex = &p, e.Index
	return &p, key, owned
}

// beginCallbackWait cannot seal the native successor reservoir or publish any
// logical state. The bounded waiter owns the one destructive callback error
// result and transfers it back by closing callbacksDone.
func (p *preparedDelta) beginCallbackWait() {
	p.callbacksDone = make(chan struct{})
	go func() { p.callbacksErr = p.writer.DrainCallbacks(); close(p.callbacksDone) }()
}

func drainPreparedWindow(states []*preparedDelta) error {
	var err error
	for _, p := range states {
		err = errors.Join(err, p.drain())
	}
	return err
}

// pumpOrderedDeltas consumes only actual committed arrivals. Barriers remain
// pending until all accepted callbacks and original finalizers have completed.
// The callback waiter is not another apply owner.
func (n *node) pumpOrderedDeltas(input []raftpb.Entry, handle func([]raftpb.Entry, map[uint64]func() error)) (closed bool) {
	for len(input) > 0 {
		w := new(orderedDeltaWindow)
		proposal, key, snapshots := w.admit(n, input[0])
		if proposal == nil {
			handle(input[:1], nil)
			input = input[1:]
			continue
		}
		states := make([]*preparedDelta, 0, orderedDeltaMaxProposals)
		window := make([]raftpb.Entry, 0, orderedDeltaMaxProposals)
		add := func(e raftpb.Entry, p *pb.Proposal, key uint64, snapshots map[uint64]*posting.DeltaSnapshot) {
			prepared, err := n.prepareDeltaMode(key, p.Index, p.Delta, false, snapshots)
			if err != nil {
				panic(errors.Join(errOrderedDeltaIncomplete, err, drainPreparedWindow(states)))
			}
			states = append(states, &prepared)
			window = append(window, e)
			prepared.beginCallbackWait()
		}
		// Every panic after accepted preparation drains all reachable waiters.
		func() {
			defer func() {
				if v := recover(); v != nil {
					for _, p := range states {
						func() { defer func() { _ = recover() }(); _ = p.drain() }()
					}
					panic(v)
				}
			}()
			add(input[0], proposal, key, snapshots)
			input = input[1:]
		collect:
			for len(states) < orderedDeltaMaxProposals {
				select {
				case <-states[0].callbacksDone:
					break collect
				default:
				}
				if len(input) == 0 {
					select {
					case <-states[0].callbacksDone:
						break collect
					case next, ok := <-n.applyCh:
						if !ok {
							closed = true
							break collect
						}
						input = next
						if len(input) == 0 {
							continue
						}
					}
				}
				p, key, snapshots := w.admit(n, input[0])
				if p == nil {
					break
				}
				add(input[0], p, key, snapshots)
				input = input[1:]
			}
			if sealer, ok := postingStore.(interface{ SealCommitWindow() }); ok {
				sealer.SealCommitWindow()
			}
			finalizers := make(map[uint64]func() error, len(states))
			for i, p := range states {
				finalizers[p.index] = func() error {
					defer drainPreparedDeltaPanic(states...)
					if err := p.finish(); err != nil {
						return errors.Join(errOrderedDeltaIncomplete, err, drainPreparedWindow(states[i+1:]))
					}
					return nil
				}
			}
			handle(window, finalizers)
		}()
		if closed {
			return true
		}
	}
	return false
}
