/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
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
		return validateRaftPendingSchemaTransition(m)
	}
	if m.DropOp != pb.Mutations_NONE {
		return fmt.Errorf("raft_checkpoint does not support drop")
	}
	for _, edge := range m.Edges {
		if edge == nil || isDeletePredicateEdge(edge) {
			return fmt.Errorf("raft_checkpoint does not support predicate deletion or nil edges")
		}
	}
	seen := make(map[string]struct{}, len(m.Schema))
	for _, update := range m.Schema {
		if err := validateRaftSchemaUpdate(update); err != nil {
			return err
		}
		if _, exists := seen[update.Predicate]; exists {
			return &raftSemanticRejection{reason: "duplicate predicate schema in one proposal"}
		}
		seen[update.Predicate] = struct{}{}
	}
	for _, update := range m.Types {
		if update == nil || update.TypeName == "" {
			return fmt.Errorf("raft_checkpoint requires a named type definition")
		}
	}
	return validateRaftPendingSchemaTransition(m)
}

// validateRaftPendingSchemaTransition is a pure check against preceding ordered
// state. The canonical pending overlay must keep the schema under which its
// deltas and conflict keys were constructed. It never asks Zero to abort a
// transaction: a rejected schema request is a recorded no-op, and its caller may
// propose a fresh request after ordered transaction decisions have completed.
func validateRaftPendingSchemaTransition(m *pb.Mutations) error {
	if posting.Oracle().NumPendingTxns() == 0 {
		return nil
	}
	if m.DropOp == pb.Mutations_TYPE {
		if _, exists := schema.State().GetType(m.DropValue); exists {
			return &raftSemanticRejection{reason: "type deletion conflicts with pending transactions"}
		}
		return nil
	}
	for _, update := range m.Schema {
		old, exists := schema.State().Get(context.Background(), update.Predicate)
		if exists && proto.Equal(&old, update) {
			continue
		}
		if posting.RaftPendingPredicateInUse(update.Predicate) {
			return &raftSemanticRejection{reason: fmt.Sprintf("schema transition for %s conflicts with pending transactions", update.Predicate)}
		}
	}
	for _, update := range m.Types {
		old, exists := schema.State().GetType(update.TypeName)
		if !exists || !proto.Equal(&old, update) {
			return &raftSemanticRejection{reason: "type transition conflicts with pending transactions"}
		}
	}
	return nil
}

// raftSchemaMutationIsNoop lets ordered apply skip even timestamped duplicate
// writes. In particular, an exact retry must not launch legacy pending aborts
// or require a new Oracle frontier just to publish an identical definition.
func raftSchemaMutationIsNoop(m *pb.Mutations) bool {
	if m == nil || len(m.Edges) != 0 {
		return false
	}
	if m.DropOp == pb.Mutations_TYPE {
		_, exists := schema.State().GetType(m.DropValue)
		return !exists
	}
	if m.DropOp != pb.Mutations_NONE || len(m.Schema)+len(m.Types) == 0 {
		return false
	}
	for _, update := range m.Schema {
		if update == nil {
			return false
		}
		old, exists := schema.State().Get(context.Background(), update.Predicate)
		if !exists || !proto.Equal(&old, update) {
			return false
		}
	}
	for _, update := range m.Types {
		if update == nil {
			return false
		}
		old, exists := schema.State().GetType(update.TypeName)
		if !exists || !proto.Equal(&old, update) {
			return false
		}
	}
	return true
}

func validateRaftSchemaUpdate(update *pb.SchemaUpdate) error {
	if update == nil {
		return fmt.Errorf("raft_checkpoint: nil schema update")
	}
	if update.Directive == pb.SchemaUpdate_DELETE || len(update.IndexSpecs) > 0 || update.ValueType == pb.Posting_VFLOAT {
		return fmt.Errorf("raft_checkpoint does not own predicate deletion or vector lifecycle")
	}
	if update.NoConflict && (update.Upsert || update.Unique) {
		return &raftSemanticRejection{reason: "upsert/unique schema requires canonical conflict tracking"}
	}
	if update.Lang && (update.ValueType != pb.Posting_STRING || update.List) {
		return &raftSemanticRejection{reason: "language schema requires a non-list string predicate"}
	}
	if err := schema.ValidateTokenizers([]*pb.SchemaUpdate{update}); err != nil {
		return &raftSemanticRejection{reason: err.Error()}
	}
	if err := checkSchemaWithDataCheck(proto.Clone(update).(*pb.SchemaUpdate), func(string) bool { return false }); err != nil {
		return &raftSemanticRejection{reason: err.Error()}
	}
	old, exists := schema.State().Get(context.Background(), update.Predicate)
	if !exists || proto.Equal(&old, update) {
		return nil
	}
	// Data/type/list and derived indexes use maintained synchronous builders.
	// Upsert reuses canonical conflict keys; unique additionally validates all
	// existing logical values before any schema or index effects.
	before, after := proto.Clone(&old).(*pb.SchemaUpdate), proto.Clone(update).(*pb.SchemaUpdate)
	before.Tokenizer, after.Tokenizer = nil, nil
	before.Directive, after.Directive = pb.SchemaUpdate_NONE, pb.SchemaUpdate_NONE
	before.ValueType, after.ValueType = 0, 0
	before.List, after.List = false, false
	before.Count, after.Count = false, false
	before.Upsert, after.Upsert = false, false
	before.Unique, after.Unique = false, false
	before.Lang, after.Lang = false, false
	before.NoConflict, after.NoConflict = false, false
	if !proto.Equal(before, after) {
		return fmt.Errorf("raft_checkpoint unsupported schema metadata change for %s", update.Predicate)
	}
	return nil
}

// runRaftSchemaMutation keeps all setup, scratch aggregation, callback draining
// and final schema publication inside the single ordered apply operation. The
// caller excludes checkpoints until return; no asynchronous builder escapes it.
func runRaftSchemaMutation(ctx context.Context, updates []*pb.SchemaUpdate, startTs uint64) error {
	if len(updates) == 0 {
		return nil
	}
	prepared := make([]*pb.SchemaUpdate, 0, len(updates))
	for _, input := range updates {
		if input == nil {
			return fmt.Errorf("raft_checkpoint: nil schema update")
		}
		update := proto.Clone(input).(*pb.SchemaUpdate)
		if err := validateRaftSchemaUpdate(update); err != nil {
			return err
		}
		if err := checkSchemaWithDataCheck(update, func(string) bool { return false }); err != nil {
			return &raftSemanticRejection{reason: err.Error()}
		}
		old, exists := schema.State().Get(ctx, update.Predicate)
		if exists && proto.Equal(&old, update) {
			continue
		}
		rebuild := posting.IndexRebuild{Attr: update.Predicate, StartTs: startTs, OldSchema: &old, CurrentSchema: update}
		if err := rebuild.ValidateData(ctx); err != nil {
			if errors.Is(err, posting.ErrSchemaDataConflict) {
				return &raftSemanticRejection{reason: err.Error()}
			}
			return err
		}
		prepared = append(prepared, update)
		// Committed input already passed ownership admission. As with normal
		// runMutation, replay must not query Zero's later tablet assignment.
	}
	if len(prepared) == 0 {
		return nil
	}
	cap, err := State.TreeDBStore.BeginRaftSchemaTransition(ctx)
	if err != nil {
		return err
	}
	defer cap.Close()
	// Reads must use the epoch-bound capability under rootGate. The ordinary
	// writer wrapper retains callback/error instrumentation and creates writes
	// in this capability's epoch; every callback drains before cap.Complete.
	store := raftSchemaStore{Store: postingStore, reads: cap.Store()}
	for _, update := range prepared {
		old, exists := schema.State().Get(ctx, update.Predicate)
		if exists && proto.Equal(&old, update) {
			continue
		}
		rebuild := posting.IndexRebuild{Attr: update.Predicate, StartTs: startTs, OldSchema: &old, CurrentSchema: update, Store: store}
		querySchema := rebuild.GetQuerySchema()
		schema.State().Set(update.Predicate, querySchema)
		schema.State().SetMutSchema(update.Predicate, update)
		// On error leave the schema marked as indexing. Ordered apply must stop,
		// retain the old certificate/history and restart from its exact root.
		if exists {
			if err := rebuild.DropIndexes(ctx); err != nil {
				return err
			}
			// A list-to-scalar transition is eligible only after the pure scan
			// proved it empty, so there is no list representation to convert.
			if !old.List || update.List {
				if err := rebuild.BuildData(schema.GetWriteContext(ctx)); err != nil {
					return err
				}
			}
			if rebuild.NeedIndexRebuild() {
				if err := rebuild.BuildIndexes(schema.GetWriteContext(ctx)); err != nil {
					return err
				}
			}
		}
		if err := writeSchemaForStore(store, update, startTs); err != nil {
			return err
		}
	}
	// All predicates' materialization and schema writes have completed. A later
	// predicate failure cannot expose an earlier predicate's final query schema.
	for _, update := range prepared {
		schema.State().Set(update.Predicate, update)
		schema.State().DeleteMutSchema(update.Predicate)
	}
	return cap.Complete()
}

type raftSchemaStore struct {
	posting.Store
	reads posting.Store
}

func (s raftSchemaStore) NewReadTxn(ts uint64) posting.ReadTxn { return s.reads.NewReadTxn(ts) }
