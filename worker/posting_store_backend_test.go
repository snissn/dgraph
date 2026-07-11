/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/dgraph/v25/worker/treedb"
	"github.com/dgraph-io/dgraph/v25/x"
)

func TestNormalizePostingStoreBackend(t *testing.T) {
	for _, input := range []string{"", "badger", " BADGER "} {
		backend, err := NormalizePostingStoreBackend(input)
		require.NoError(t, err)
		require.Equal(t, PostingStoreBackendBadger, backend)
	}

	backend, err := NormalizePostingStoreBackend("TreeDB")
	require.NoError(t, err)
	require.Equal(t, PostingStoreBackendTreeDB, backend)

	_, err = NormalizePostingStoreBackend("rocksdb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected \"badger\" or \"treedb\"")
}

func TestSetConfigurationNormalizesPostingStoreBackend(t *testing.T) {
	oldConfig := Config
	oldTmpDir := x.WorkerConfig.TmpDir
	t.Cleanup(func() {
		Config = oldConfig
		x.WorkerConfig.TmpDir = oldTmpDir
	})

	root := t.TempDir()
	x.WorkerConfig.TmpDir = filepath.Join(root, "tmp")

	treeDBFlag := ParsePostingStoreSuperFlag("backend=TreeDB")
	SetConfiguration(&Options{
		PostingDir:             filepath.Join(root, "p"),
		WALDir:                 filepath.Join(root, "w"),
		PostingStoreBackend:    treeDBFlag.Backend,
		PostingStoreTier:       treeDBFlag.Tier,
		PostingStoreDurability: treeDBFlag.Durability,
		PostingStoreEvents:     treeDBFlag.Events,
		PostingStoreEventsSet:  treeDBFlag.EventsConfigured,
	})
	require.Equal(t, PostingStoreBackendTreeDB, Config.PostingStoreBackend)
	require.Equal(t, PostingStoreTierBenchmarkMinimal, Config.PostingStoreTier)
	require.True(t, Config.PostingStoreEvents)

	badgerFlag := ParsePostingStoreSuperFlag("")
	SetConfiguration(&Options{
		PostingDir:             filepath.Join(root, "p2"),
		WALDir:                 filepath.Join(root, "w2"),
		PostingStoreBackend:    badgerFlag.Backend,
		PostingStoreTier:       badgerFlag.Tier,
		PostingStoreDurability: badgerFlag.Durability,
		PostingStoreEvents:     badgerFlag.Events,
		PostingStoreEventsSet:  badgerFlag.EventsConfigured,
	})
	require.Equal(t, PostingStoreBackendBadger, Config.PostingStoreBackend)
	require.Equal(t, PostingStoreTierProduction, Config.PostingStoreTier)
	require.False(t, Config.PostingStoreEvents)

	disabled := ParsePostingStoreSuperFlag("backend=treedb; events=false")
	require.True(t, disabled.EventsConfigured)
	require.False(t, disabled.Events)
}

func TestCheckPostingStoreBackendReadyAllowsBenchmarkMinimalTreeDB(t *testing.T) {
	require.NoError(t, CheckPostingStoreBackendReady(PostingStoreBackendBadger))
	require.NoError(t, CheckPostingStoreBackendReady(PostingStoreBackendTreeDB))
	require.NoError(t, ValidatePostingStoreSelection("treedb", "benchmark_minimal", "durable", false, true))
	require.NoError(t, ValidatePostingStoreSelection("treedb", "benchmark_minimal", "relaxed", false, true))

	err := ValidatePostingStoreSelection("treedb", "production", "durable", false, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "restricted to tier")
	err = ValidatePostingStoreSelection("treedb", "benchmark_minimal", "unknown", false, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "durability must be")
	err = ValidatePostingStoreSelection("treedb", "benchmark_minimal", "durable", false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires events=true")
}

func TestCheckPostingStoreBackendReadyRejectsEncryptedTreeDBStartup(t *testing.T) {
	require.NoError(t, CheckPostingStoreBackendReadyForConfig(PostingStoreBackendBadger, true))

	err := CheckPostingStoreBackendReadyForConfig(PostingStoreBackendTreeDB, true)
	require.ErrorIs(t, err, treedb.ErrUnsupportedFeature)
	require.Contains(t, err.Error(), "cannot satisfy the configured encryption requirement")
	require.Contains(t, err.Error(), string(treedb.FeatureEncryptionKeyRegistry))
	require.NotContains(t, err.Error(), string(treedb.FeatureBadgerManagedTransactions))
}

func TestPostingStoreBackendStatus(t *testing.T) {
	require.Equal(t, "posting-store backend badger: default production backend",
		PostingStoreBackendStatus(""))
	require.Contains(t, PostingStoreBackendStatus(PostingStoreBackendTreeDB),
		"posting-store backend treedb: experimental, tier=benchmark_minimal")
	require.Contains(t, PostingStoreBackendStatus("unknown"), "posting-store backend \"unknown\" is not supported")
}
