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

func TestSchemaStoreFailsClosedForBadgerStreamBootstrap(t *testing.T) {
	InitForStore(newBadgerStore(ps))
	t.Cleanup(func() { Init(ps) })
	predicate := x.AttrInRootNamespace("schema-store-preserved-on-bootstrap-error")
	want := &pb.SchemaUpdate{Predicate: predicate, ValueType: pb.Posting_STRING}
	State().Set(predicate, want)

	err := LoadFromDb(context.Background())
	require.EqualError(t, err, "schema stream bootstrap requires the Badger operational backend")
	got, ok := State().Get(context.Background(), predicate)
	require.True(t, ok)
	require.True(t, proto.Equal(want, &got))
}
