/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

// PinRaftReadTimestamp owns a whole request, including Oracle waits and cache
// hits before its first storage transaction. Internal posting split reads are
// governed by the physical floor, not the semantic request floor.
func (s *TreeDBStore) PinRaftReadTimestamp(ts uint64) (func(), error) {
	if s == nil || s.IsClosed() {
		return nil, fmt.Errorf("posting store closed")
	}
	if s.mode != TreeDBCommitExternallyRecoverable {
		return func() {}, nil
	}
	s.raftReadMu.Lock()
	defer s.raftReadMu.Unlock()
	if err := s.checkRaftReadTimestampLocked(ts); err != nil {
		return nil, err
	}
	s.raftReadPins++
	var released atomic.Bool
	return func() {
		if released.Swap(true) {
			return
		}
		s.raftReadMu.Lock()
		s.raftReadPins--
		if s.raftReadChanged != nil {
			close(s.raftReadChanged)
			s.raftReadChanged = nil
		}
		s.raftReadMu.Unlock()
	}, nil
}

// CheckRaftReadTimestamp checks initial admission without pinning an Oracle
// waiter. The caller must acquire a real pin after waiting, before data/cache.
func (s *TreeDBStore) CheckRaftReadTimestamp(ts uint64) error {
	if s == nil || s.IsClosed() {
		return fmt.Errorf("posting store closed")
	}
	if s.mode != TreeDBCommitExternallyRecoverable {
		return nil
	}
	s.raftReadMu.Lock()
	defer s.raftReadMu.Unlock()
	return s.checkRaftReadTimestampLocked(ts)
}
func (s *TreeDBStore) checkRaftReadTimestampLocked(ts uint64) error {
	if s.raftReadDraining || s.IsClosed() {
		return fmt.Errorf("raft posting reader admission paused")
	}
	if ts <= s.raftSemanticFloor {
		return fmt.Errorf("read timestamp %d is at or below replicated posting floor %d", ts, s.raftSemanticFloor)
	}
	return nil
}

// RestoreRaftPostingFloor is called only by the ordered apply/recovery owner.
// The physical floor is checked independently against the certified root.
func (s *TreeDBStore) RestoreRaftPostingFloor(semantic, physical uint64) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return fmt.Errorf("external posting owner unavailable")
	}
	actual, err := s.mvcc.DiscardFloor()
	if err != nil {
		return err
	}
	if physical > semantic || actual != physical {
		return fmt.Errorf("certified posting floors mismatch: semantic=%d physical=%d root=%d", semantic, physical, actual)
	}
	s.raftReadMu.Lock()
	s.raftSemanticFloor = semantic
	s.raftReadMu.Unlock()
	return nil
}

// AdvanceRaftSemanticFloor changes request admission only. Its authority is an
// explicit preceding ordered Raft transition; local readers never select it.
func (s *TreeDBStore) AdvanceRaftSemanticFloor(floor uint64) error {
	s.raftReadMu.Lock()
	defer s.raftReadMu.Unlock()
	if s.IsClosed() || floor < s.raftSemanticFloor {
		return fmt.Errorf("invalid replicated posting floor transition")
	}
	s.raftSemanticFloor = floor
	return nil
}

func (s *TreeDBStore) drainRaftRequests(ctx context.Context) (func(), error) {
	s.raftReadMu.Lock()
	if s.raftReadDraining {
		s.raftReadMu.Unlock()
		return nil, fmt.Errorf("raft posting reader drain already active")
	}
	s.raftReadDraining = true
	for s.raftReadPins != 0 {
		if s.raftReadChanged == nil {
			s.raftReadChanged = make(chan struct{})
		}
		changed := s.raftReadChanged
		s.raftReadMu.Unlock()
		select {
		case <-ctx.Done():
			s.raftReadMu.Lock()
			s.raftReadDraining = false
			s.raftReadMu.Unlock()
			return nil, ctx.Err()
		case <-changed:
		}
		s.raftReadMu.Lock()
	}
	s.raftReadMu.Unlock()
	return func() { s.raftReadMu.Lock(); s.raftReadDraining = false; s.raftReadMu.Unlock() }, nil
}

// RaftPostingMaintenance is a private historical capability. The caller also
// owns Dgraph's semantic apply barrier for its entire lifetime. Request pins and
// raw read handles have drained before it is returned; no rootGate RLock can be
// recursively acquired by its private Store.
type RaftPostingMaintenance struct {
	mu           sync.Mutex
	reads        map[*treeDBReadTxn]struct{}
	store        *TreeDBStore
	epoch        uint64
	floor        uint64
	release      func()
	materialized bool
	started      bool
	pruned       bool
	finished     bool
	generation   uint64
	digest       string
	closed       atomic.Bool
}

func (s *TreeDBStore) BeginRaftPostingMaintenance(ctx context.Context, maxP uint64) (*RaftPostingMaintenance, error) {
	if s == nil || s.IsClosed() || s.mode != TreeDBCommitExternallyRecoverable {
		return nil, fmt.Errorf("external posting owner unavailable")
	}
	release, err := s.drainRaftRequests(ctx)
	if err != nil {
		return nil, err
	}
	s.rootGate.Lock()
	s.lifecycleMu.Lock()
	if s.IsClosed() {
		s.lifecycleMu.Unlock()
		s.rootGate.Unlock()
		release()
		return nil, fmt.Errorf("posting store closed")
	}
	s.commitWG.Wait()
	s.raftReadMu.Lock()
	floor := s.raftSemanticFloor
	s.raftReadMu.Unlock()
	if maxP < floor {
		floor = maxP
	}
	cert, _ := s.external.RecoveredCheckpoint()
	current, err := s.mvcc.DiscardFloor()
	s.lifecycleMu.Unlock()
	if err != nil || floor < current {
		s.rootGate.Unlock()
		release()
		if err == nil {
			err = fmt.Errorf("physical posting floor regression")
		}
		return nil, err
	}
	return &RaftPostingMaintenance{store: s, epoch: s.rootEpoch.Add(1), floor: floor, release: release, generation: cert.Generation, digest: cert.Digest}, nil
}

func (m *RaftPostingMaintenance) PhysicalFloor() uint64 { return m.floor }
func (m *RaftPostingMaintenance) ReadStore() Store      { return raftMaintenanceStore{m} }
func (m *RaftPostingMaintenance) valid() bool {
	return m != nil && !m.closed.Load() && !m.store.IsClosed() && m.epoch == m.store.rootEpoch.Load()
}
func (m *RaftPostingMaintenance) MaterializeBaselines(ctx context.Context) error {
	if !m.valid() {
		return fmt.Errorf("stale posting maintenance capability")
	}
	m.started = true
	if err := materializeRaftPostingBaselines(ctx, m.ReadStore(), m.floor); err != nil {
		return err
	}
	resetRaftPostingCaches()
	m.materialized = true
	return nil
}
func (m *RaftPostingMaintenance) AdvanceAndPrune(batchSize int) (mvcc.PruneStats, error) {
	if !m.valid() || !m.materialized {
		return mvcc.PruneStats{}, fmt.Errorf("posting baseline pass must finish before pruning")
	}
	s := m.store
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	var stats mvcc.PruneStats
	err := s.external.Apply(func() error {
		if err := s.mvcc.AdvanceDiscardFloor(m.floor, s.commitMode); err != nil {
			return err
		}
		var err error
		stats, err = s.mvcc.PruneVersions(mvcc.PruneOptions{BatchSize: batchSize, Mode: s.commitMode})
		return err
	})
	if err == nil {
		m.pruned = true
	}
	return stats, err
}

// Finish acknowledges a newly certified physical cut before read admission.
func (m *RaftPostingMaintenance) Finish() error {
	if !m.valid() || !m.materialized || !m.pruned {
		return fmt.Errorf("incomplete posting maintenance pass")
	}
	cert, ok := m.store.RaftCheckpointCertificate()
	if !ok || cert.Generation <= m.generation {
		return fmt.Errorf("posting maintenance must be certified before admission")
	}
	m.finished = true
	return nil
}

// FinishWorking releases a fully completed pass while keeping exactly the old
// recovery certificate. The Dgraph owner must verify/sync all replay history
// before calling; this capability cannot authorize Raft retention or progress.
func (m *RaftPostingMaintenance) FinishWorking() error {
	if !m.valid() || !m.materialized || !m.pruned {
		return fmt.Errorf("incomplete posting maintenance pass")
	}
	cert, ok := m.store.RaftCheckpointCertificate()
	if !ok || cert.Generation != m.generation || cert.Digest != m.digest {
		return fmt.Errorf("working maintenance lost its previous recovery certificate")
	}
	m.finished = true
	return nil
}
func (m *RaftPostingMaintenance) Close() {
	if m == nil || m.closed.Swap(true) {
		return
	}
	if m.started && !m.finished {
		m.store.closed.Store(true)
	}
	m.mu.Lock()
	for txn := range m.reads {
		txn.Discard()
	}
	m.reads = nil
	m.mu.Unlock()
	m.store.rootEpoch.Add(1)
	m.store.rootGate.Unlock()
	m.release()
}

type raftMaintenanceStore struct{ cap *RaftPostingMaintenance }

func (s raftMaintenanceStore) IsClosed() bool { return !s.cap.valid() }
func (s raftMaintenanceStore) NewReadTxn(ts uint64) ReadTxn {
	// A stale capability returns an already-discarded transaction, so neither Get
	// nor iterators may touch an installed replacement root.
	s.cap.mu.Lock()
	defer s.cap.mu.Unlock()
	txn := &treeDBReadTxn{store: s.cap.store, readTs: ts, iterators: make(map[*treeDBIterator]struct{})}
	if !s.cap.valid() {
		txn.discarded = true
	} else {
		if s.cap.reads == nil {
			s.cap.reads = make(map[*treeDBReadTxn]struct{})
		}
		s.cap.reads[txn] = struct{}{}
	}
	return &raftMaintenanceReadTxn{treeDBReadTxn: txn, cap: s.cap}
}

type raftMaintenanceReadTxn struct {
	*treeDBReadTxn
	cap *RaftPostingMaintenance
}

func (t *raftMaintenanceReadTxn) Discard() {
	t.cap.mu.Lock()
	delete(t.cap.reads, t.treeDBReadTxn)
	t.cap.mu.Unlock()
	t.treeDBReadTxn.Discard()
}
func (s raftMaintenanceStore) NewWriteTxn() WriteTxn {
	txn := s.cap.store.NewWriteTxn().(*treeDBWriteTxn)
	if !s.cap.valid() {
		txn.stickyErr = fmt.Errorf("stale posting maintenance capability")
	}
	return txn
}
