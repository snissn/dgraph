/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostingCompatibilityMatrixClassifiesDgraphBlockers(t *testing.T) {
	matrix := PostingCompatibilityMatrix()
	require.NotEmpty(t, matrix)

	byID := make(map[CompatibilityFamilyID]CompatibilityRecord)
	for _, record := range matrix {
		require.NotEmpty(t, record.ID)
		require.NotEmpty(t, record.Feature)
		require.NotEmpty(t, record.Status)
		require.NotEmpty(t, record.Decision)
		require.NotEmpty(t, record.OperatorMessage)
		require.NotEmpty(t, record.RequiredAPIs)
		require.NotEmpty(t, record.DgraphCallSites)
		require.NotEmpty(t, record.Evidence)
		require.False(t, strings.Contains(strings.ToLower(record.OperatorMessage), "silently"),
			"operator message should be direct, not silently permissive: %s", record.ID)
		_, ok := FeatureForID(record.Feature)
		require.True(t, ok, "compatibility row %s references unknown feature %s", record.ID, record.Feature)
		require.NotContains(t, byID, record.ID)
		byID[record.ID] = record
	}

	require.Equal(t, StatusDisabledNeedBlocker, byID[CompatibilityManagedTimestampTransactions].Status)
	require.Contains(t, byID[CompatibilityManagedTimestampTransactions].RequiredAPIs, "(*badger.Txn).CommitAt")
	require.Contains(t, byID[CompatibilityManagedTimestampTransactions].DgraphCallSites, "posting.TxnWriter.SetAt")

	require.Equal(t, StatusDisabledNeedBlocker, byID[CompatibilityEntryMetadata].Status)
	require.Equal(t, TierBenchmarkMinimal, byID[CompatibilityEntryMetadata].RequiredTier)
	require.Contains(t, byID[CompatibilityEntryMetadata].RequiredAPIs, "badger.Entry.UserMeta")
	require.Contains(t, byID[CompatibilityEntryMetadata].RequiredAPIs, "(*badger.Entry).WithDiscard")
	require.Equal(t, StatusUnsupported, byID[CompatibilityEntryTTL].Status)
	require.Equal(t, TierOperational, byID[CompatibilityEntryTTL].RequiredTier)

	require.Equal(t, StatusDisabledNeedBlocker, byID[CompatibilityAllVersionIteration].Status)
	require.Contains(t, byID[CompatibilityAllVersionIteration].RequiredAPIs, "badger.IteratorOptions.AllVersions")

	require.Equal(t, StatusDisabledNeedBlocker, byID[CompatibilityStreamImportExport].Status)
	require.Equal(t, StatusDisabledNeedBlocker, byID[CompatibilitySubscriptions].Status)
	require.Equal(t, StatusUnsupported, byID[CompatibilityEncryptionKeyRegistry].Status)
	require.Equal(t, StatusDisabledWant, byID[CompatibilityMetricsCache].Status)
}

func TestPostingBackendRequiredFeaturesFailClosed(t *testing.T) {
	required := PostingBackendRequiredFeatures()
	require.Contains(t, required, FeaturePostingStoreAdapterContract)
	require.Contains(t, required, FeatureBadgerManagedTransactions)
	require.Contains(t, required, FeatureBadgerEntryMetadata)
	require.Contains(t, required, FeatureBadgerAllVersionIterators)
	require.NotContains(t, required, FeatureBadgerEntryTTL)
	require.NotContains(t, required, FeatureBadgerStreamImportExport)
	require.NotContains(t, required, FeatureBadgerSubscriptions)
	require.NotContains(t, required, FeatureEncryptionKeyRegistry)
	require.NotContains(t, required, FeatureCommandWALConditionalTransactions)

	blockers := PostingBackendBlockers()
	require.NotEmpty(t, blockers)
	blockerIDs := make([]CompatibilityFamilyID, 0, len(blockers))
	for _, blocker := range blockers {
		blockerIDs = append(blockerIDs, blocker.ID)
		require.NotEqual(t, StatusSupported, blocker.Status)
		require.NotEmpty(t, blocker.OperatorMessage)
	}
	require.Contains(t, blockerIDs, CompatibilityManagedTimestampTransactions)
	require.Contains(t, blockerIDs, CompatibilityEntryMetadata)
	require.Contains(t, blockerIDs, CompatibilityAllVersionIteration)
	require.NotContains(t, blockerIDs, CompatibilityEntryTTL)
	require.NotContains(t, blockerIDs, CompatibilityStreamImportExport)
	require.NotContains(t, blockerIDs, CompatibilitySubscriptions)
	require.NotContains(t, blockerIDs, CompatibilityEncryptionKeyRegistry)
	require.NotContains(t, blockerIDs, CompatibilityMetricsCache)

	err := CheckPostingBackendReady()
	require.ErrorIs(t, err, ErrUnsupportedFeature)
	require.Contains(t, err.Error(), "TreeDB posting-store backend is not ready")
	require.Contains(t, err.Error(), string(FeatureBadgerManagedTransactions))
	require.NotContains(t, err.Error(), string(FeatureBadgerStreamImportExport))
	require.NotContains(t, err.Error(), string(FeatureEncryptionKeyRegistry))

	var readinessErr *FeatureReadinessError
	require.True(t, errors.As(err, &readinessErr))
	require.NotEmpty(t, readinessErr.Blockers)
}

func TestPostingBackendBlockersForTier(t *testing.T) {
	benchmark, err := PostingBackendBlockersForTier(TierBenchmarkMinimal)
	require.NoError(t, err)
	require.Len(t, benchmark, 3)

	operational, err := PostingBackendBlockersForTier(TierOperational)
	require.NoError(t, err)
	require.Greater(t, len(operational), len(benchmark))
	require.Contains(t, compatibilityIDs(operational), CompatibilityStreamImportExport)
	require.Contains(t, compatibilityIDs(operational), CompatibilityEntryTTL)

	production, err := PostingBackendBlockersForTier(TierProduction)
	require.NoError(t, err)
	require.Contains(t, compatibilityIDs(production), CompatibilityEncryptionKeyRegistry)
}

func compatibilityIDs(records []CompatibilityRecord) []CompatibilityFamilyID {
	ids := make([]CompatibilityFamilyID, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func TestPostingCompatibilityMatrixIsImmutable(t *testing.T) {
	matrix := PostingCompatibilityMatrix()
	require.NotEmpty(t, matrix)

	matrix[0].RequiredAPIs[0] = "mutated"
	matrix[0].DgraphCallSites[0] = "mutated"
	matrix[0].Evidence[0] = "mutated"

	fresh := PostingCompatibilityMatrix()
	require.NotEqual(t, "mutated", fresh[0].RequiredAPIs[0])
	require.NotEqual(t, "mutated", fresh[0].DgraphCallSites[0])
	require.NotEqual(t, "mutated", fresh[0].Evidence[0])
}
