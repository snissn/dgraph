/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
)

// ErrSchemaDataConflict denotes a deterministic schema/data incompatibility
// discovered before any materialization. Storage, decoding and cancellation
// errors retain their causes and cannot qualify as terminal no-op proposals.
var ErrSchemaDataConflict = errors.New("schema transition conflicts with existing logical data")

// ValidateData uses a complete logical posting view at the ordered transition
// timestamp, including all main/split/delta dependencies. Physical key presence
// and local pruning schedules never select a replicated schema outcome.
func (rb *IndexRebuild) ValidateData(ctx context.Context) error {
	old, next := rb.OldSchema, rb.CurrentSchema
	if old == nil || next == nil {
		return nil
	}
	oldType, newType := types.TypeID(old.ValueType), types.TypeID(next.ValueType)
	forbiddenWithData := (old.List && !next.List) || oldType.IsScalar() != newType.IsScalar()
	convert := oldType != newType && oldType.IsScalar() && newType.IsScalar()
	unique := next.Unique && (!old.Unique || convert)
	if !forbiddenWithData && !convert && !unique {
		return nil
	}
	if unique {
		return rb.validateUniqueData(ctx, forbiddenWithData)
	}
	pk := x.ParsedKey{Attr: rb.Attr}
	return walkStoreLists(ctx, rb.sourceStore(), rb.StartTs, pk.DataPrefix(), func(_ []byte, list *List) error {
		return list.Iterate(rb.StartTs, 0, func(p *pb.Posting) error {
			if forbiddenWithData {
				return fmt.Errorf("%w: cannot change list/scalar or UID/scalar shape of %s while it has values", ErrSchemaDataConflict, rb.Attr)
			}
			if _, err := types.Convert(types.Val{Tid: types.TypeID(p.ValType), Value: p.Value}, newType); err != nil {
				return fmt.Errorf("%w: cannot convert values of %s to %s", ErrSchemaDataConflict, rb.Attr, newType.Name())
			}
			return nil
		})
	})
}
