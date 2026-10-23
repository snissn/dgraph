/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"fmt"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
)

// RequestPostingHistoryFloor uses the existing ordered proposal/acknowledgment
// path. There is deliberately no automatic wall-clock semantic expiration.
func RequestPostingHistoryFloor(ctx context.Context, floor uint64) error {
	if !raftCheckpointEnabled() || groups().Node == nil {
		return fmt.Errorf("external posting recovery is not active")
	}
	return groups().Node.proposeAndWait(ctx, &pb.Proposal{PostingFloor: &pb.PostingFloorRequest{Version: 1, Floor: floor}})
}

// applyPostingFloor is a pure deterministic disposition followed by monotone
// admission state. Only the preceding ordered Oracle/pending state selects S;
// active readers and local physical reclamation never participate.
func (n *node) applyPostingFloor(p *pb.Proposal) error {
	request := p.PostingFloor
	if request.Version != 1 || request.Floor < n.raftPostingFloor {
		return &raftSemanticRejection{reason: "invalid posting floor version or regression"}
	}
	frontier, pending := n.raftOrderedAssigned, posting.Oracle().MinPendingStartTs()
	if request.Floor >= frontier || request.Floor >= pending {
		return &raftSemanticRejection{reason: "posting floor must precede assigned frontier and every pending start"}
	}
	if request.Floor == n.raftPostingFloor {
		return nil
	}
	if err := State.TreeDBStore.AdvanceRaftSemanticFloor(request.Floor); err != nil {
		return err
	}
	n.raftPostingFloor = request.Floor
	n.raftFloorIndex = p.Index
	for key, outcome := range n.appliedProposals {
		if outcome.startTs != 0 && outcome.startTs <= request.Floor {
			delete(n.appliedProposals, key)
		}
	}
	return nil
}
