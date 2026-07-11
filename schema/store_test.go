/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package schema

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
)

func TestSchemaStorePreservesLoadAndDelete(t *testing.T) {
	Init(ps)
	predicate := x.AttrInRootNamespace("schema-store-golden")
	key := x.SchemaKey(predicate)
	want := &pb.SchemaUpdate{Predicate: predicate, ValueType: pb.Posting_STRING}
	value, err := proto.Marshal(want)
	require.NoError(t, err)

	txn := ps.NewTransactionAt(math.MaxUint64, true)
	require.NoError(t, txn.Set(key, value))
	require.NoError(t, txn.CommitAt(5, nil))
	require.NoError(t, Load(predicate))
	got, ok := State().Get(context.Background(), predicate)
	require.True(t, ok)
	require.True(t, proto.Equal(want, &got))

	require.NoError(t, State().Delete(predicate, 7))
	readBefore := ps.NewTransactionAt(6, false)
	_, err = readBefore.Get(key)
	require.NoError(t, err)
	readBefore.Discard()
	readAfter := ps.NewTransactionAt(7, false)
	_, err = readAfter.Get(key)
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	readAfter.Discard()
}

func TestSchemaStoreSupportsNeutralIteratorBootstrap(t *testing.T) {
	InitForStore(newBadgerStore(ps))
	t.Cleanup(func() { Init(ps) })
	predicate := x.AttrInRootNamespace("schema-store-neutral-bootstrap")
	typeName := x.AttrInRootNamespace("SchemaStoreNeutralType")
	wantSchema := &pb.SchemaUpdate{Predicate: predicate, ValueType: pb.Posting_STRING}
	wantType := &pb.TypeUpdate{
		TypeName: typeName,
		Fields:   []*pb.SchemaUpdate{{Predicate: predicate, ValueType: pb.Posting_STRING}},
	}
	schemaValue, err := proto.Marshal(wantSchema)
	require.NoError(t, err)
	typeValue, err := proto.Marshal(wantType)
	require.NoError(t, err)
	txn := ps.NewTransactionAt(math.MaxUint64, true)
	require.NoError(t, txn.Set(x.SchemaKey(predicate), schemaValue))
	require.NoError(t, txn.Set(x.TypeKey(typeName), typeValue))
	require.NoError(t, txn.CommitAt(50, nil))

	err = LoadFromDb(context.Background())
	require.NoError(t, err)
	gotSchema, ok := State().Get(context.Background(), predicate)
	require.True(t, ok)
	require.True(t, proto.Equal(wantSchema, &gotSchema))
	gotType, ok := State().GetType(typeName)
	require.True(t, ok)
	require.True(t, proto.Equal(wantType, &gotType))
}

func TestLoadFromDBRejectsInvalidMode(t *testing.T) {
	err := loadFromDB(context.Background(), -1)
	require.EqualError(t, err, "invalid schema load mode: -1")
}
