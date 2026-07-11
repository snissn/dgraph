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

	custom, err := ResolveOptions(OpenOptions{Dir: dir, KeepRecent: 32})
	require.NoError(t, err)
	require.Equal(t, uint64(32), custom.KeepRecent)
}

func TestResolveOptionsRejectsUnsupportedFeatures(t *testing.T) {
	_, err := ResolveOptions(OpenOptions{
		Dir:               t.TempDir(),
		RequireEncryption: true,
	})
	require.ErrorIs(t, err, ErrUnsupportedFeature)
	require.Contains(t, err.Error(), "encryption_key_registry=unsupported")

	_, err = ResolveOptions(OpenOptions{
		Dir:             t.TempDir(),
		RequireInMemory: true,
	})
	require.ErrorIs(t, err, ErrUnsupportedFeature)
	require.Contains(t, err.Error(), "in_memory_posting_store=unsupported")
}

func TestResolveOptionsRejectsNonRuntimeProfiles(t *testing.T) {
	for _, profile := range []td.Profile{td.Profile("durable"), td.ProfileBench} {
		_, err := ResolveOptions(OpenOptions{
			Dir:     t.TempDir(),
			Profile: profile,
		})
		require.Error(t, err)
		require.False(t, errors.Is(err, ErrUnsupportedFeature))
		require.Contains(t, err.Error(), "command_wal_durable or command_wal_relaxed")
	}
}

func TestRuntimeProfilesUseCumulativeCapabilityTiers(t *testing.T) {
	profiles := []td.Profile{td.ProfileCommandWALDurable, td.ProfileCommandWALRelaxed}
	for _, profile := range profiles {
		for _, tier := range CapabilityTiers() {
			t.Run(string(profile)+"/"+string(tier), func(t *testing.T) {
				opts, err := ResolveOptions(OpenOptions{Dir: t.TempDir(), Profile: profile})
				require.NoError(t, err)
				require.True(t, opts.CommandWAL)

				err = CheckCapabilityTier(tier)
				if tier == TierBenchmarkMinimal {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, ErrUnsupportedFeature)
					require.Contains(t, err.Error(), "capability tier "+string(tier)+" is not ready")
				}
			})
		}
	}
}

func TestOpenReopenDurability(t *testing.T) {
	dir := t.TempDir()
	handle, err := Open(OpenOptions{Dir: dir})
	require.NoError(t, err)
	require.NoError(t, handle.DB.Set([]byte("durable"), []byte("value")))
	require.NoError(t, handle.Close())

	reopened, err := Open(OpenOptions{Dir: dir})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reopened.Close())
	}()
	value, err := reopened.DB.Get([]byte("durable"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
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
