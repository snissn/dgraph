/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStoreUniqueSchemaValidationLogicalValuesAndHistory(t *testing.T) {
	for _, prune := range []bool{false, true} {
		store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
		attr := x.AttrInRootNamespace("unique-schema-data")
		write := func(uid, ts uint64, value string, op uint32) {
			data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: math.MaxUint64, Value: []byte(value), ValType: pb.Posting_STRING, PostingType: pb.Posting_VALUE, Op: op}}})
			require.NoError(t, err)
			commitStoreEntry(t, store, ts, Entry{Key: x.DataKey(attr, uid), Value: data, UserMeta: BitDeltaPosting})
		}
		write(1, 2, "42", Set)
		write(2, 2, "42", Set)
		if prune {
			require.NoError(t, materializeRaftPostingBaselines(context.Background(), store, 6))
			require.NoError(t, store.AdvanceDiscardFloor(6))
			_, err := store.PruneVersions(4)
			require.NoError(t, err)
		}
		old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING}
		next := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING, Unique: true, Upsert: true, Directive: pb.SchemaUpdate_INDEX, Tokenizer: []string{"exact"}}
		rb := &IndexRebuild{Attr: attr, StartTs: 10, Store: store, OldSchema: old, CurrentSchema: next}
		require.ErrorIs(t, rb.ValidateData(context.Background()), ErrSchemaDataConflict, "same duplicate outcome with pruned=%v", prune)
		write(2, 8, "42", Del)
		require.NoError(t, rb.ValidateData(context.Background()))
		write(2, 9, "042", Set)
		require.NoError(t, rb.ValidateData(context.Background()), "distinct string values remain distinct")
		next.ValueType, next.Tokenizer = pb.Posting_INT, []string{"int"}
		require.ErrorIs(t, rb.ValidateData(context.Background()), ErrSchemaDataConflict, "type conversion must precede uniqueness comparison")
	}
}

func TestStoreUniqueSchemaScratchComparesExactValues(t *testing.T) {
	tmp, cleanup, err := openStoreRebuildScratch()
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	// Deliberately put distinct canonical values in the same candidate bucket.
	// A digest collision cannot reject a valid transition or hide another row.
	write := func(sequence, owner uint64, value string) {
		key := make([]byte, 32)
		key = binary.BigEndian.AppendUint64(key, sequence)
		row := binary.BigEndian.AppendUint64(nil, owner)
		row = binary.BigEndian.AppendUint32(row, 0)
		row = append(row, value...)
		txn := tmp.NewTransactionAt(1, true)
		require.NoError(t, txn.SetEntry(&badger.Entry{Key: key, Value: row}))
		require.NoError(t, txn.CommitAt(1, nil))
	}
	write(1, 1, "one")
	write(2, 2, "two")
	write(3, 1, "one") // repeated same subject is not a duplicate owner.
	require.NoError(t, checkUniqueScratchRows(context.Background(), tmp))
	write(4, 3, "one")
	require.ErrorIs(t, checkUniqueScratchRows(context.Background(), tmp), ErrSchemaDataConflict)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, checkUniqueScratchRows(ctx, tmp), context.Canceled)
}
