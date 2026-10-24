/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"fmt"
	"io"

	td "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// OpenRaftTreeDBStore is the explicit externally recoverable materialization
// path. Ordinary durable/relaxed stores cannot enter this policy by toggling WAL.
func OpenRaftTreeDBStore(opts td.Options, identity string) (*TreeDBStore, error) {
	owner, err := td.OpenExternalRecovery(opts, td.ExternalRecoveryConfig{Identity: identity, MaxCheckpointBytes: 8 << 30, MaxCheckpointFiles: 8192})
	if err != nil {
		return nil, err
	}
	s := &TreeDBStore{
		external: owner, db: owner.DB(), mvcc: mvcc.New(owner.DB()),
		commitMode: mvcc.CommitRelaxed, mode: TreeDBCommitExternallyRecoverable,
		durability: "raft_checkpoint", commitQueue: make(chan treeDBCommitRequest, treeDBCommitQueueCapacity),
		commitSlots: make(chan struct{}, treeDBCommitOwnershipLimit),
	}
	s.mutationPool.New = newTreeDBMutationBatch
	if err := s.loadRaftPendingManifest(); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return s, nil
}

func (s *TreeDBStore) RecoveredRaftCheckpoint() ([]byte, bool) {
	cert, ok := s.RaftCheckpointCertificate()
	return cert.Metadata, ok
}

func (s *TreeDBStore) RaftCheckpointCertificate() (td.ExternalRecoveryCheckpoint, bool) {
	if s == nil {
		return td.ExternalRecoveryCheckpoint{}, false
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.external == nil || s.IsClosed() {
		return td.ExternalRecoveryCheckpoint{}, false
	}
	return s.external.RecoveredCheckpoint()
}

func (s *TreeDBStore) ExportRaftCheckpoint(ctx context.Context, dst io.Writer, generation uint64, digest string) error {
	if s == nil {
		return fmt.Errorf("external posting store unavailable")
	}
	s.lifecycleMu.Lock()
	owner := s.external
	closed := s.IsClosed()
	s.lifecycleMu.Unlock()
	if owner == nil || closed {
		return fmt.Errorf("external posting store unavailable")
	}
	return owner.ExportCheckpoint(ctx, dst, generation, digest)
}

// InstallRaftCheckpoint drains every read transaction (and its iterators),
// blocks new readers, then closes write admission and drains accepted storage
// and acknowledgement dispatch. Dgraph's semantic apply/TxnWriter.Flush barrier
// must finish its callbacks before this call; arbitrary user callbacks may call
// Close and cannot be waited on while this adapter holds its lifecycle gate.
// Pre-install write handles become stale even if preparation subsequently fails.
// The caller must separately drain semantic apply and reload schema/cache before
// read health; this barrier owns only the storage handles and root publication.
func (s *TreeDBStore) InstallRaftCheckpoint(ctx context.Context, src io.Reader,
	authorize func(td.ExternalRecoveryCheckpoint) ([]byte, error)) (td.ExternalRecoveryCheckpoint, error) {
	if s == nil {
		return td.ExternalRecoveryCheckpoint{}, fmt.Errorf("external posting store unavailable")
	}
	release, err := s.drainRaftRequests(ctx)
	if err != nil {
		return td.ExternalRecoveryCheckpoint{}, err
	}
	defer release()
	s.rootGate.Lock()
	defer s.rootGate.Unlock()
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.external == nil || s.IsClosed() {
		return td.ExternalRecoveryCheckpoint{}, fmt.Errorf("external posting store unavailable")
	}
	s.commitWG.Wait()
	s.rootEpoch.Add(1)
	cert, err := s.external.InstallCheckpoint(ctx, src, authorize)
	if err != nil {
		// A no-op capability call distinguishes an intact old owner from an
		// ambiguous publication requiring reopen. Never serve through a poisoned
		// owner or guess whether CURRENT selected the old/new namespace.
		if probe := s.external.Apply(func() error { return nil }); probe != nil {
			s.closed.Store(true)
		}
		return cert, err
	}
	s.db = s.external.DB()
	s.mvcc = mvcc.New(s.db)
	return cert, nil
}

// CheckpointRaft closes admission and drains existing storage before asking
// the external owner to bind the exact physical root to this application cut.
func (s *TreeDBStore) CheckpointRaft(ctx context.Context, metadata []byte) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.external == nil || s.closed.Load() {
		return fmt.Errorf("external posting store unavailable")
	}
	s.commitWG.Wait()
	_, err := s.external.Checkpoint(ctx, metadata)
	return err
}
