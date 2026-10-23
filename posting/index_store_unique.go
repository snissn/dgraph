/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
)

// validateUniqueData is a pure preflight over the complete logical source.
// The scratch digest groups candidates only: every alleged duplicate is
// compared byte-for-byte, and every occurrence retains its own scratch row.
// Neither a hash collision nor the local physical layout selects the outcome.
func (rb *IndexRebuild) validateUniqueData(ctx context.Context, forbiddenWithData bool) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, cleanup, err := openStoreRebuildScratch()
	if err != nil {
		return err
	}
	defer func() {
		if err := cleanup(); err != nil {
			// Unknown scratch I/O cannot inherit a deterministic-conflict tag
			// from an earlier successful data check.
			retErr = fmt.Errorf("closing unique schema scratch: %w", err)
		}
	}()
	batch := tmp.NewManagedWriteBatch()
	defer batch.Cancel()
	var sequence uint64
	pk := x.ParsedKey{Attr: rb.Attr}
	err = walkStoreLists(ctx, rb.sourceStore(), rb.StartTs, pk.DataPrefix(), func(key []byte, list *List) error {
		source, err := x.Parse(key)
		if err != nil {
			return err
		}
		return list.Iterate(rb.StartTs, 0, func(p *pb.Posting) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if forbiddenWithData {
				return fmt.Errorf("%w: cannot change list/scalar or UID/scalar shape of %s while it has values", ErrSchemaDataConflict, rb.Attr)
			}
			value, err := types.Convert(types.Val{Tid: types.TypeID(p.ValType), Value: p.Value}, types.TypeID(rb.CurrentSchema.ValueType))
			if err != nil {
				return fmt.Errorf("%w: cannot convert unique values of %s", ErrSchemaDataConflict, rb.Attr)
			}
			encoded := types.Val{Tid: types.BinaryID}
			if err := types.Marshal(value, &encoded); err != nil {
				return err
			}
			if len(p.LangTag) > math.MaxUint32 || sequence == math.MaxUint64 {
				return fmt.Errorf("unique schema scratch row overflow")
			}
			canonical := binary.BigEndian.AppendUint32(nil, uint32(len(p.LangTag)))
			canonical = append(canonical, p.LangTag...)
			canonical = append(canonical, encoded.Value.([]byte)...)
			digest := sha256.Sum256(canonical)
			sequence++
			rowKey := binary.BigEndian.AppendUint64(digest[:], sequence)
			rowValue := binary.BigEndian.AppendUint64(nil, source.Uid)
			rowValue = append(rowValue, canonical...)
			return batch.SetEntryAt(&badger.Entry{Key: rowKey, Value: rowValue}, 1)
		})
	})
	if err != nil {
		return err
	}
	if err := batch.Flush(); err != nil {
		return err
	}
	return checkUniqueScratchRows(ctx, tmp)
}

// Two iterators retain at most two full values and a fixed working budget.
// The usual bucket has one row; a duplicate owner is refused at its first
// exact match. Even deliberately colliding digests compare actual values.
func checkUniqueScratchRows(ctx context.Context, tmp *badger.DB) error {
	txn := tmp.NewTransactionAt(1, false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	outer, inner := txn.NewIterator(opts), txn.NewIterator(opts)
	defer outer.Close()
	defer inner.Close()
	for outer.Rewind(); outer.Valid(); outer.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := outer.Item().KeyCopy(nil)
		row, err := outer.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(key) != sha256.Size+8 || len(row) < 12 {
			return fmt.Errorf("invalid unique schema scratch row")
		}
		bucket := key[:sha256.Size]
		owner := binary.BigEndian.Uint64(row)
		for inner.Seek(bucket); inner.ValidForPrefix(bucket) && bytes.Compare(inner.Item().Key(), key) < 0; inner.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			other, err := inner.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(other) < 12 {
				return fmt.Errorf("invalid unique schema scratch value")
			}
			if binary.BigEndian.Uint64(other) != owner && bytes.Equal(other[8:], row[8:]) {
				return fmt.Errorf("%w: duplicate value on different subjects", ErrSchemaDataConflict)
			}
		}
	}
	return nil
}
