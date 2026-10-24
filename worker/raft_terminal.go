/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"google.golang.org/protobuf/proto"
)

// Only these isolated control kinds are intrinsically idempotent. Their random
// transport keys do not establish an eternal collision contract with mutations.
func raftProposalNeedsIdentity(p *pb.Proposal) bool {
	controls := 0
	if p.State != nil {
		controls++
	}
	if p.Delta != nil {
		controls++
	}
	if p.Snapshot != nil {
		controls++
	}
	if p.PostingFloor != nil {
		controls++
	}
	return controls != 1 || p.Mutations != nil || p.DeleteNs != nil || p.Restore != nil || p.ExtSnapshotState != nil || p.CdcState != nil || p.CleanPredicate != "" || len(p.Kv) != 0
}

// Digest binds the complete checksum map at one source sequence. It is a
// bounded source-message identity, not an aggregate pending/history checksum.
func raftChecksumDigest(delta *pb.OracleDelta) string {
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&pb.OracleDelta{GroupChecksums: delta.GroupChecksums})
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func mergeOracleDelta(target, next *pb.OracleDelta) error {
	if next.ChecksumSequence != 0 && next.ChecksumSequence == target.ChecksumSequence && !maps.Equal(target.GroupChecksums, next.GroupChecksums) {
		return fmt.Errorf("same Zero checksum sequence carries different maps")
	}
	target.Txns = append(target.Txns, next.Txns...)
	if next.MaxAssigned > target.MaxAssigned {
		target.MaxAssigned = next.MaxAssigned
	}
	if next.ChecksumSequence >= target.ChecksumSequence {
		target.ChecksumSequence = next.ChecksumSequence
		target.GroupChecksums = next.GroupChecksums
	}
	return nil
}

// prepareRaftDelta is a PURE whole-batch validation. A stale watermark never
// suppresses unseen decisions; only retired S or an exact terminal row can.
func (n *node) prepareRaftDelta(input *pb.OracleDelta) (*pb.OracleDelta, error) {
	if input.ChecksumSequence == 0 && len(input.GroupChecksums) > 0 {
		return nil, &raftSemanticRejection{reason: "external checksum requires sequenced Zero authority"}
	}
	if input.ChecksumSequence != 0 && input.ChecksumSequence == n.raftChecksumSequence && raftChecksumDigest(input) != n.raftChecksumDigest {
		return nil, &raftSemanticRejection{reason: "same checksum sequence has contradictory checksum map"}
	}
	seen := make(map[uint64]uint64, len(input.Txns))
	delta := proto.Clone(input).(*pb.OracleDelta)
	delta.Txns = nil
	for _, decision := range input.Txns {
		if decision == nil || decision.StartTs == 0 || (decision.CommitTs != 0 && decision.CommitTs <= decision.StartTs) {
			return nil, &raftSemanticRejection{reason: "Oracle commit timestamp must exceed nonzero transaction start"}
		}
		if previous, exists := seen[decision.StartTs]; exists {
			if previous != decision.CommitTs {
				return nil, &raftSemanticRejection{reason: "contradictory decisions within Oracle batch"}
			}
			continue
		}
		seen[decision.StartTs] = decision.CommitTs
		if decision.StartTs <= n.raftPostingFloor {
			continue
		}
		commit, resolved, err := State.TreeDBStore.RaftTxnDisposition(decision.StartTs)
		if err != nil {
			return nil, err
		}
		if resolved {
			if commit != decision.CommitTs {
				return nil, &raftSemanticRejection{reason: fmt.Sprintf("contradictory terminal decision for transaction %d", decision.StartTs)}
			}
			continue
		}
		delta.Txns = append(delta.Txns, decision)
	}
	return delta, nil
}
