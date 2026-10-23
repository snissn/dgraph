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
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
	"google.golang.org/protobuf/proto"
)

// checkRaftSupportedProposal runs at both proposal admission and ordered apply.
// Admission is advisory: schema may have changed before the committed entry is
// applied. A failure after side effects is an uncertain apply failure, never a
// terminal normal-mutation rejection or a certifiable partial schema outcome.
func checkRaftSupportedProposal(p *pb.Proposal) error {
	if p.Kv != nil || p.CleanPredicate != "" || p.Restore != nil || p.DeleteNs != nil || p.ExtSnapshotState != nil || p.CdcState != nil {
		return fmt.Errorf("raft_checkpoint does not support external/admin materialization")
	}
	m := p.Mutations
	if m == nil {
		return nil
	}
	if m.DropOp == pb.Mutations_TYPE {
		// Unlike prefix/data deletion, this existing schema-store operation is
		// one timestamped tombstone and never rewrites posting data or Oracle.
		if m.StartTs == 0 || m.DropValue == "" || x.IsPreDefinedType(m.DropValue) || len(m.Edges)+len(m.Schema)+len(m.Types) != 0 {
			return fmt.Errorf("raft_checkpoint requires an isolated timestamped type deletion")
		}
		return nil
	}
	if m.DropOp != pb.Mutations_NONE {
		return fmt.Errorf("raft_checkpoint does not support drop")
	}
	for _, edge := range m.Edges {
		if edge == nil || isDeletePredicateEdge(edge) {
			return fmt.Errorf("raft_checkpoint does not support predicate deletion or nil edges")
		}
	}
	for _, update := range m.Schema {
		if err := validateRaftSchemaUpdate(update); err != nil {
			return err
		}
	}
	for _, update := range m.Types {
		if update == nil || update.TypeName == "" {
			return fmt.Errorf("raft_checkpoint requires a named type definition")
		}
	}
	return nil
}

func validateRaftSchemaUpdate(update *pb.SchemaUpdate) error {
	if update == nil {
		return fmt.Errorf("raft_checkpoint: nil schema update")
	}
	if update.Directive == pb.SchemaUpdate_DELETE || len(update.IndexSpecs) > 0 || update.Count || update.Directive == pb.SchemaUpdate_REVERSE || update.ValueType == pb.Posting_VFLOAT {
		return fmt.Errorf("raft_checkpoint requires scalar tokenizer schema; delete/vector/count/reverse need an owned lifecycle")
	}
	old, exists := schema.State().Get(context.Background(), update.Predicate)
	if !exists || proto.Equal(&old, update) {
		return nil
	}
	// This batch deliberately changes only tokenizer membership. Data/type/list,
	// language, uniqueness and conflict semantics stay with their existing schema.
	before, after := proto.Clone(&old).(*pb.SchemaUpdate), proto.Clone(update).(*pb.SchemaUpdate)
	before.Tokenizer, after.Tokenizer = nil, nil
	before.Directive, after.Directive = pb.SchemaUpdate_NONE, pb.SchemaUpdate_NONE
	if !types.TypeID(old.ValueType).IsScalar() || !proto.Equal(before, after) {
		return fmt.Errorf("raft_checkpoint supports only scalar tokenizer changes on existing predicate %s", update.Predicate)
	}
	return nil
}

// runRaftSchemaMutation keeps all setup, scratch aggregation, callback draining
// and final schema publication inside the single ordered apply operation. The
// caller excludes checkpoints until return; no asynchronous builder escapes it.
func runRaftSchemaMutation(ctx context.Context, updates []*pb.SchemaUpdate, startTs uint64) error {
	for _, update := range updates {
		if err := validateRaftSchemaUpdate(update); err != nil {
			return err
		}
		if err := checkSchema(update); err != nil {
			return err
		}
		// Committed input already passed ownership admission. As with normal
		// runMutation, replay must not query Zero's later tablet assignment.
	}
	for _, update := range updates {
		old, exists := schema.State().Get(ctx, update.Predicate)
		if exists && proto.Equal(&old, update) {
			continue
		}
		rebuild := posting.IndexRebuild{Attr: update.Predicate, StartTs: startTs, OldSchema: &old, CurrentSchema: update}
		querySchema := rebuild.GetQuerySchema()
		schema.State().Set(update.Predicate, querySchema)
		schema.State().SetMutSchema(update.Predicate, update)
		// On error leave the schema marked as indexing. Ordered apply must stop,
		// retain the old certificate/history and restart from its exact root.
		if exists {
			if err := rebuild.DropIndexes(ctx); err != nil {
				return err
			}
			if rebuild.NeedIndexRebuild() {
				if err := rebuild.BuildIndexes(schema.GetWriteContext(ctx)); err != nil {
					return err
				}
			}
		}
		if err := writeSchema(update, startTs); err != nil {
			return err
		}
	}
	// All predicates' materialization and schema writes have completed. A later
	// predicate failure cannot expose an earlier predicate's final query schema.
	for _, update := range updates {
		schema.State().Set(update.Predicate, update)
		schema.State().DeleteMutSchema(update.Predicate)
	}
	return nil
}
