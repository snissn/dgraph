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

	require.Equal(t, StatusDisabledNeedBlocker, byID[CompatibilityEntryMetadataTTL].Status)
	require.Contains(t, byID[CompatibilityEntryMetadataTTL].RequiredAPIs, "badger.Entry.UserMeta")
	require.Contains(t, byID[CompatibilityEntryMetadataTTL].RequiredAPIs, "(*badger.Entry).WithDiscard")

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
	require.Contains(t, required, FeatureBadgerEntryMetadataTTL)
	require.Contains(t, required, FeatureBadgerAllVersionIterators)
	require.Contains(t, required, FeatureBadgerStreamImportExport)
	require.Contains(t, required, FeatureBadgerSubscriptions)
	require.Contains(t, required, FeatureEncryptionKeyRegistry)
	require.NotContains(t, required, FeatureMetricsCacheAPIs, "disabled-want metrics must be surfaced but should not alone block selector work")

	blockers := PostingBackendBlockers()
	require.NotEmpty(t, blockers)
	blockerIDs := make([]CompatibilityFamilyID, 0, len(blockers))
	for _, blocker := range blockers {
		blockerIDs = append(blockerIDs, blocker.ID)
		require.NotEqual(t, StatusSupported, blocker.Status)
		require.NotEmpty(t, blocker.OperatorMessage)
	}
	require.Contains(t, blockerIDs, CompatibilityManagedTimestampTransactions)
	require.Contains(t, blockerIDs, CompatibilityEntryMetadataTTL)
	require.Contains(t, blockerIDs, CompatibilityAllVersionIteration)
	require.Contains(t, blockerIDs, CompatibilityStreamImportExport)
	require.Contains(t, blockerIDs, CompatibilitySubscriptions)
	require.Contains(t, blockerIDs, CompatibilityEncryptionKeyRegistry)
	require.NotContains(t, blockerIDs, CompatibilityMetricsCache)

	err := CheckPostingBackendReady()
	require.ErrorIs(t, err, ErrUnsupportedFeature)
	require.Contains(t, err.Error(), "TreeDB posting-store backend is not ready")
	require.Contains(t, err.Error(), string(FeatureBadgerManagedTransactions))
	require.Contains(t, err.Error(), string(FeatureBadgerStreamImportExport))
	require.Contains(t, err.Error(), string(FeatureEncryptionKeyRegistry))

	var readinessErr *FeatureReadinessError
	require.True(t, errors.As(err, &readinessErr))
	require.NotEmpty(t, readinessErr.Blockers)
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
