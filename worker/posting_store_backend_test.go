/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"errors"
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

	SetConfiguration(&Options{
		PostingDir:          filepath.Join(root, "p"),
		WALDir:              filepath.Join(root, "w"),
		PostingStoreBackend: "TreeDB",
	})
	require.Equal(t, PostingStoreBackendTreeDB, Config.PostingStoreBackend)

	SetConfiguration(&Options{
		PostingDir: filepath.Join(root, "p2"),
		WALDir:     filepath.Join(root, "w2"),
	})
	require.Equal(t, PostingStoreBackendBadger, Config.PostingStoreBackend)
}

func TestCheckPostingStoreBackendReadyFailsClosedForTreeDB(t *testing.T) {
	require.NoError(t, CheckPostingStoreBackendReady(PostingStoreBackendBadger))

	err := CheckPostingStoreBackendReady(PostingStoreBackendTreeDB)
	require.ErrorIs(t, err, treedb.ErrUnsupportedFeature)
	require.Contains(t, err.Error(), "posting-store backend \"treedb\" is experimental and not ready")
	require.Contains(t, err.Error(), string(treedb.TierBenchmarkMinimal))
	require.Contains(t, err.Error(), string(treedb.FeatureBadgerManagedTransactions))
	require.Contains(t, err.Error(), string(treedb.FeatureBadgerEntryMetadata))
	require.Contains(t, err.Error(), string(treedb.FeatureBadgerAllVersionIterators))
	require.NotContains(t, err.Error(), string(treedb.FeatureBadgerStreamImportExport))
	require.NotContains(t, err.Error(), string(treedb.FeatureEncryptionKeyRegistry))

	var readinessErr *treedb.FeatureReadinessError
	require.True(t, errors.As(err, &readinessErr))
	require.NotEmpty(t, readinessErr.Blockers)
}

func TestPostingStoreBackendStatus(t *testing.T) {
	require.Equal(t, "posting-store backend badger: default production backend",
		PostingStoreBackendStatus(""))
	require.Contains(t, PostingStoreBackendStatus(PostingStoreBackendTreeDB),
		"posting-store backend treedb: experimental, benchmark_minimal disabled until")
	require.Contains(t, PostingStoreBackendStatus("unknown"), "posting-store backend \"unknown\" is not supported")
}
