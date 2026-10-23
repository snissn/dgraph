package posting

import (
	"context"
	"fmt"

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
	return s, nil
}

func (s *TreeDBStore) RecoveredRaftCheckpoint() ([]byte, bool) {
	if s == nil || s.external == nil {
		return nil, false
	}
	cert, ok := s.external.RecoveredCheckpoint()
	return cert.Metadata, ok
}

// CheckpointRaft closes admission and drains existing callbacks before asking
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
