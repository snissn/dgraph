/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
)

func TestInitForLiteInitializesPostingStoreForSchemaWrites(t *testing.T) {
	oldPstore, oldPostingStore := pstore, postingStore
	oldState, oldNode, oldGID := groups().state, groups().Node, groups().gid

	db, err := badger.OpenManaged(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() {
		pstore, postingStore = oldPstore, oldPostingStore
		groups().state, groups().Node, groups().gid = oldState, oldNode, oldGID
		schema.Init(oldPstore)
		require.NoError(t, db.Close())
	})

	schema.Init(db)
	InitForLite(db)
	require.NotNil(t, postingStore)

	predicate := x.AttrInRootNamespace("lite-schema")
	require.NoError(t, updateSchema(&pb.SchemaUpdate{
		Predicate: predicate,
		ValueType: pb.Posting_STRING,
	}, 1))

	txn := postingStore.NewReadTxn(1)
	defer txn.Discard()
	item, err := txn.Get(x.SchemaKey(predicate))
	require.NoError(t, err)
	require.Equal(t, posting.BitSchemaPosting, item.UserMeta())
}
