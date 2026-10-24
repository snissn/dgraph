/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"sort"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/types"
)

// stageRaftMutation runs the existing semantic/index pipeline exactly once in
// isolated canonical state. Capacity rejects BEFORE schema, overlay or Oracle
// publication, so a large index/delete fanout cannot become a restart loop.
// Temporary staging/list-read memory remains subject to the ordinary execution
// costs; the retained canonical pending state has the explicit charged bound.
func (n *node) stageRaftMutation(ctx context.Context, p *pb.Proposal, inferred map[string]types.TypeID) error {
	overrides := make(map[string]*pb.SchemaUpdate)
	for attr, typ := range inferred {
		if _, ok := schema.State().Get(schema.GetWriteContext(ctx), attr); !ok {
			su := inferredSchema(attr, typ, p.GetMutations().GetMetadata().GetPredHints()[attr])
			overrides[attr] = &su
		}
	}
	txn, err := posting.CloneRaftPendingTxn(p.Mutations.StartTs)
	if err != nil {
		return err
	}
	if txn.ShouldAbort() {
		return &raftSemanticRejection{reason: "transaction is canonically marked for abort"}
	}
	ctx = schema.WithStagedWriteSchema(ctx, overrides)
	edges := p.Mutations.Edges
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].Attr != edges[j].Attr {
			return edges[i].Attr < edges[j].Attr
		}
		return edges[i].Entity < edges[j].Entity
	})
	if err := applyRaftMutationEdges(ctx, p.Mutations, txn); err != nil {
		return err
	}
	data, err := posting.MarshalRaftPendingTxn(txn)
	if err != nil {
		// The canonical encoder's explicit size error describes ONLY private state.
		// All other failures retain their causes and cannot qualify as a no-op.
		if errors.Is(err, posting.ErrRaftPendingStateTooLarge) {
			return &raftSemanticRejection{reason: err.Error()}
		}
		return err
	}
	if err := State.TreeDBStore.CheckRaftPendingReplacement(txn.StartTs, data); err != nil {
		if errors.Is(err, posting.ErrRaftPendingCapacity) {
			return &raftSemanticRejection{reason: err.Error()}
		}
		return err
	}
	if err := posting.ValidateRaftUniqueTxn(ctx, txn, n.raftOrderedAssigned); err != nil {
		if errors.Is(err, posting.ErrSchemaDataConflict) {
			return &raftSemanticRejection{reason: err.Error()}
		}
		return err
	}
	// Every effect follows the final deterministic capacity decision. Unknown
	// failures here stop progress; the preceding exact certificate/WAL reconstructs
	// the proposal instead of guessing how much storage was published.
	attrs := make([]string, 0, len(overrides))
	for attr := range overrides {
		attrs = append(attrs, attr)
	}
	sort.Strings(attrs)
	for _, attr := range attrs {
		if err := writeSchema(overrides[attr], p.Mutations.StartTs); err != nil {
			return err
		}
	}
	if err := State.TreeDBStore.ReplaceRaftPendingTxn(txn.StartTs, data); err != nil {
		return err
	}
	if err := posting.InstallRaftPendingTxn(txn); err != nil {
		return err
	}
	for _, attr := range attrs {
		schema.State().Set(attr, overrides[attr])
	}
	return nil
}
