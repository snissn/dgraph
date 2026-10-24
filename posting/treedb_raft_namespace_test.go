/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestRaftNamespaceBanExactRootAndValidation(t *testing.T) {
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	open := func() *TreeDBStore {
		s, err := OpenRaftTreeDBStore(opts, "namespace-test")
		require.NoError(t, err)
		return s
	}
	s := open()
	defer func() { require.NoError(t, s.Close()) }()
	require.Error(t, s.BanRaftNamespace(0))
	require.NoError(t, s.CheckpointRaft(context.Background(), []byte("old-cut")))
	require.NoError(t, s.BanRaftNamespace(7))
	require.ErrorIs(t, s.CheckRaftNamespace(7), badger.ErrBannedKey)
	require.NoError(t, s.Close())
	s = open()
	require.NoError(t, s.CheckRaftNamespace(7), "uncertified ban is discarded with its working suffix")
	require.NoError(t, s.BanRaftNamespace(7))
	require.NoError(t, s.CheckpointRaft(context.Background(), []byte("banned-cut")))
	require.NoError(t, s.Close())
	s = open()
	require.NoError(t, s.ValidateRaftNamespaceBans())
	require.ErrorIs(t, s.CheckRaftNamespace(7), badger.ErrBannedKey)
	require.NoError(t, s.CheckRaftNamespace(8))
	require.NoError(t, s.external.Apply(func() error {
		b := s.db.NewBatch()
		defer b.Close()
		require.NoError(t, b.Set(raftNamespaceBanKey(8), []byte("corrupt")))
		return b.Write()
	}))
	require.ErrorContains(t, s.CheckRaftNamespace(8), "invalid persisted namespace ban")
	require.ErrorContains(t, s.ValidateRaftNamespaceBans(), "invalid persisted namespace ban")
}
