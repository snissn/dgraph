/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// pruneRaftPostingHistory changes only local physical history. Semantic expiry
// must already be authorized by an explicit preceding replicated transition.
// A successful pass either certifies its exact root or retains the preceding
// certificate and every input needed to replay the newer working state. A
// started pass that fails seals adapter admission until that root is reopened.
func (n *node) pruneRaftPostingHistory(ctx context.Context, maxP uint64, batchSize int) (mvcc.PruneStats, error) {
	var stats mvcc.PruneStats
	if !raftCheckpointEnabled() || n == nil || State.TreeDBStore == nil {
		return stats, fmt.Errorf("Raft posting maintenance requires the external recovery owner")
	}
	n.raftSemanticMu.Lock()
	defer n.raftSemanticMu.Unlock()
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if err := x.HealthCheck(); err != nil {
		return stats, err
	}
	if n.raftSemanticIndex == 0 || n.raftSemanticIndex < n.raftReplayUntil || atomic.LoadInt32(&n.streaming) != 0 {
		return stats, fmt.Errorf("Raft posting maintenance requires completed recovery and snapshot installation")
	}
	// Keep B=P+1 strictly below every pending StartTs. Pending transaction
	// overlays and conflict versions survive cache invalidation; no later valid
	// decision can commit them at or below B (CommitTs must exceed StartTs).
	if pending := posting.Oracle().MinPendingStartTs(); pending != math.MaxUint64 {
		if pending <= 1 {
			return stats, fmt.Errorf("Raft posting maintenance waits for an older pending decision")
		}
		if bound := pending - 2; maxP > bound {
			maxP = bound
		}
	}
	// The old certificate remains authoritative if newer transactions prevent
	// certification. Verify its exact metadata and all retained replay input
	// before changing working history; FinishWorking never advances retention.
	if err := n.validateRaftMaintenanceRecovery(); err != nil {
		return stats, err
	}
	previous, err := State.TreeDBStore.DiscardFloor()
	if err != nil {
		return stats, err
	}
	cap, err := State.TreeDBStore.BeginRaftPostingMaintenance(ctx, maxP)
	if err != nil {
		return stats, err
	}
	defer cap.Close()
	if cap.PhysicalFloor() == previous {
		return stats, nil
	}
	if err := cap.MaterializeBaselines(ctx); err != nil {
		return stats, err
	}
	stats, err = cap.AdvanceAndPrune(batchSize)
	if err != nil {
		return stats, err
	}
	if posting.Oracle().NumPendingTxns() == 0 {
		if err := n.checkpointRaftCut(n.raftSemanticIndex, true); err != nil {
			return stats, err
		}
		return stats, cap.Finish()
	}
	return stats, cap.FinishWorking()
}

func (n *node) validateRaftMaintenanceRecovery() error {
	data, ok := State.TreeDBStore.RecoveredRaftCheckpoint()
	if !ok {
		return fmt.Errorf("Raft posting maintenance requires a retained exact certificate")
	}
	var c raftCheckpointMetadata
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	if err := validateRaftCheckpoint(c, n.Store, n.gid, n.Id); err != nil {
		return err
	}
	if c.Index != atomic.LoadUint64(&n.raftCertifiedIndex) || c.Index > n.raftSemanticIndex {
		return fmt.Errorf("Raft maintenance certificate frontier mismatch")
	}
	hs, err := n.Store.HardState()
	if err != nil {
		return err
	}
	if hs.Commit < n.raftSemanticIndex {
		return fmt.Errorf("Raft maintenance lacks committed replay authority")
	}
	for lo := c.Index + 1; lo <= n.raftSemanticIndex; {
		entries, err := n.Store.Entries(lo, n.raftSemanticIndex+1, 8<<20)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return fmt.Errorf("Raft maintenance lacks retained replay input %d", lo)
		}
		for _, entry := range entries {
			if entry.Index != lo {
				return fmt.Errorf("Raft maintenance replay gap at %d", lo)
			}
			lo++
		}
	}
	return n.Store.Sync()
}
