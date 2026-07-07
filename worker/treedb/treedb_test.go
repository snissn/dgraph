/*
 * SPDX-FileCopyrightText: 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

import (
	"errors"
	"testing"

	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestResolveOptionsUsesDgraphDefaults(t *testing.T) {
	dir := t.TempDir()

	opts, err := ResolveOptions(OpenOptions{Dir: dir})
	require.NoError(t, err)
	require.Equal(t, dir, opts.Dir)
	require.True(t, opts.CommandWAL)
	require.Equal(t, DefaultKeepRecent, opts.KeepRecent)
}

func TestResolveOptionsRejectsUnsupportedFeatures(t *testing.T) {
	_, err := ResolveOptions(OpenOptions{
		Dir:               t.TempDir(),
		RequireEncryption: true,
	})
	require.ErrorIs(t, err, ErrUnsupportedFeature)

	_, err = ResolveOptions(OpenOptions{
		Dir:             t.TempDir(),
		RequireInMemory: true,
	})
	require.ErrorIs(t, err, ErrUnsupportedFeature)
}

func TestResolveOptionsRejectsNonPublicProfile(t *testing.T) {
	_, err := ResolveOptions(OpenOptions{
		Dir:     t.TempDir(),
		Profile: td.Profile("durable"),
	})
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrUnsupportedFeature))
}

func TestOpenSmoke(t *testing.T) {
	handle, err := Open(OpenOptions{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, handle.Close())
	}()

	require.NoError(t, handle.DB.Set([]byte("alpha"), []byte("one")))
	value, err := handle.DB.Get([]byte("alpha"))
	require.NoError(t, err)
	require.Equal(t, []byte("one"), value)

	value, _, err = handle.DB.GetVersioned([]byte("alpha"))
	require.NoError(t, err)
	require.Equal(t, []byte("one"), value)

	batch := handle.DB.NewBatch()
	require.NoError(t, batch.Set([]byte("beta"), []byte("two")))
	require.NoError(t, batch.Write())
	require.NoError(t, batch.Close())

	_, err = handle.DB.NewConditionalTxn()
	require.Error(t, err)
	require.Contains(t, err.Error(), "conditional transactions unsupported")

	snap := handle.DB.AcquireSnapshot()
	require.NotNil(t, snap)
	defer func() {
		require.NoError(t, snap.Close())
	}()
	value, err = snap.Get([]byte("beta"))
	require.NoError(t, err)
	require.Equal(t, []byte("two"), value)

	iter, err := handle.DB.Iterator(nil, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, iter.Close())
	}()
	require.True(t, iter.Valid())
	require.NoError(t, iter.Error())
}
