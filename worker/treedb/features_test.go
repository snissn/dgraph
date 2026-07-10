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

func TestFeatureRegistryStatusTaxonomy(t *testing.T) {
	registry := FeatureRegistry()
	require.NotEmpty(t, registry)

	ids := make(map[FeatureID]bool)
	statuses := make(map[FeatureStatus]bool)
	for _, feature := range registry {
		require.NotEmpty(t, feature.ID)
		require.NotEmpty(t, feature.Status)
		require.NotEmpty(t, feature.Reason)
		require.NotEmpty(t, feature.Evidence)
		require.False(t, ids[feature.ID], "duplicate feature ID %s", feature.ID)
		if feature.RequiredTier != "" {
			_, ok := capabilityTierRank(feature.RequiredTier)
			require.True(t, ok, "unknown required tier %q for feature %s", feature.RequiredTier, feature.ID)
		}

		ids[feature.ID] = true
		switch feature.Status {
		case StatusSupported, StatusDisabledWant, StatusDisabledNeedBlocker, StatusUnsupported:
			statuses[feature.Status] = true
		default:
			t.Fatalf("unknown status %q for feature %s", feature.Status, feature.ID)
		}
	}

	require.Equal(t, map[FeatureStatus]bool{
		StatusSupported:           true,
		StatusDisabledWant:        true,
		StatusDisabledNeedBlocker: true,
		StatusUnsupported:         true,
	}, statuses)

	feature, ok := FeatureForID(FeatureTreeDBOpen)
	require.True(t, ok)
	require.Equal(t, StatusSupported, feature.Status)
	require.Equal(t, []string{
		"TestResolveOptionsUsesDgraphDefaults",
		"TestOpenSmoke",
	}, feature.Evidence)

	registry[0].Evidence[0] = "mutated"
	feature, ok = FeatureForID(FeatureTreeDBOpen)
	require.True(t, ok)
	require.Equal(t, "TestResolveOptionsUsesDgraphDefaults", feature.Evidence[0])
}

func TestCapabilityTierRequirementsAndBlockers(t *testing.T) {
	require.Equal(t, []CapabilityTier{
		TierBenchmarkMinimal,
		TierOperational,
		TierProduction,
	}, CapabilityTiers())

	tests := []struct {
		name                string
		tier                CapabilityTier
		wantRequired        []FeatureID
		wantBlockers        []FeatureID
		wantExcludedFeature []FeatureID
	}{
		{
			name: "benchmark minimal",
			tier: TierBenchmarkMinimal,
			wantRequired: []FeatureID{
				FeatureTreeDBOpen,
				FeaturePostingStoreAdapterContract,
				FeatureTreeDBStoreImplementation,
				FeatureBadgerManagedTransactions,
				FeatureBadgerEntryMetadata,
				FeatureBadgerAllVersionIterators,
				FeatureLifecycleGCStats,
			},
			wantBlockers: []FeatureID{
				FeatureTreeDBStoreImplementation,
				FeatureBadgerManagedTransactions,
				FeatureBadgerEntryMetadata,
				FeatureBadgerAllVersionIterators,
				FeatureLifecycleGCStats,
			},
			wantExcludedFeature: []FeatureID{
				FeatureBadgerEntryTTL,
				FeatureBadgerStreamImportExport,
				FeatureBadgerSubscriptions,
				FeatureEncryptionKeyRegistry,
				FeatureCommandWALConditionalTransactions,
				FeatureInMemoryPostingStore,
			},
		},
		{
			name: "operational is cumulative",
			tier: TierOperational,
			wantRequired: []FeatureID{
				FeatureBadgerManagedTransactions,
				FeatureBadgerEntryTTL,
				FeatureBadgerStreamImportExport,
				FeatureBadgerSubscriptions,
				FeatureBadgerProtobufCompatibility,
				FeatureMetricsCacheAPIs,
			},
			wantBlockers: []FeatureID{
				FeatureTreeDBStoreImplementation,
				FeatureBadgerManagedTransactions,
				FeatureBadgerEntryMetadata,
				FeatureBadgerEntryTTL,
				FeatureBadgerAllVersionIterators,
				FeatureBadgerStreamImportExport,
				FeatureBadgerSubscriptions,
				FeatureBadgerProtobufCompatibility,
				FeatureMetricsCacheAPIs,
				FeatureLifecycleGCStats,
			},
			wantExcludedFeature: []FeatureID{
				FeatureEncryptionKeyRegistry,
				FeatureCommandWALConditionalTransactions,
				FeatureInMemoryPostingStore,
			},
		},
		{
			name: "production is cumulative",
			tier: TierProduction,
			wantRequired: []FeatureID{
				FeatureBadgerManagedTransactions,
				FeatureBadgerStreamImportExport,
				FeatureEncryptionKeyRegistry,
			},
			wantBlockers: []FeatureID{
				FeatureTreeDBStoreImplementation,
				FeatureBadgerManagedTransactions,
				FeatureBadgerEntryMetadata,
				FeatureBadgerEntryTTL,
				FeatureBadgerAllVersionIterators,
				FeatureBadgerStreamImportExport,
				FeatureBadgerSubscriptions,
				FeatureEncryptionKeyRegistry,
				FeatureBadgerProtobufCompatibility,
				FeatureMetricsCacheAPIs,
				FeatureLifecycleGCStats,
			},
			wantExcludedFeature: []FeatureID{
				FeatureCommandWALConditionalTransactions,
				FeatureInMemoryPostingStore,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			required, err := RequiredFeaturesForTier(tt.tier)
			require.NoError(t, err)
			for _, id := range tt.wantRequired {
				require.Contains(t, required, id)
			}
			for _, id := range tt.wantExcludedFeature {
				require.NotContains(t, required, id)
			}

			blockers, err := CapabilityTierBlockers(tt.tier)
			require.NoError(t, err)
			blockerIDs := make([]FeatureID, 0, len(blockers))
			for _, blocker := range blockers {
				blockerIDs = append(blockerIDs, blocker.ID)
			}
			require.ElementsMatch(t, tt.wantBlockers, blockerIDs)

			err = CheckCapabilityTier(tt.tier)
			require.ErrorIs(t, err, ErrUnsupportedFeature)
			require.Contains(t, err.Error(), "capability tier "+string(tt.tier)+" is not ready")
		})
	}

	_, err := RequiredFeaturesForTier("future")
	require.ErrorIs(t, err, ErrUnsupportedFeature)
	require.EqualError(t, err, `dgraph treedb integration: unsupported feature: unknown capability tier "future"`)
}

func TestOptionalCapabilityInvocationFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		feature FeatureID
		status  FeatureStatus
	}{
		{name: "ttl", feature: FeatureBadgerEntryTTL, status: StatusUnsupported},
		{name: "stream", feature: FeatureBadgerStreamImportExport, status: StatusDisabledNeedBlocker},
		{name: "subscriptions", feature: FeatureBadgerSubscriptions, status: StatusDisabledNeedBlocker},
		{name: "encryption", feature: FeatureEncryptionKeyRegistry, status: StatusUnsupported},
		{name: "in memory", feature: FeatureInMemoryPostingStore, status: StatusUnsupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckFeatureAvailable(tt.feature)
			require.ErrorIs(t, err, ErrUnsupportedFeature)
			require.Contains(t, err.Error(), string(tt.feature)+"="+string(tt.status))
		})
	}
}

func TestCheckRequiredFeaturesFailClosed(t *testing.T) {
	require.NoError(t, CheckRequiredFeatures(
		FeatureTreeDBOpen,
		FeatureDurableCommandWALProfile,
		FeaturePointReadWrite,
	))

	required := []FeatureID{
		FeatureMetricsCacheAPIs,
		FeatureBadgerManagedTransactions,
		FeatureEncryptionKeyRegistry,
		FeatureID("future_feature"),
	}
	blockers := RequiredFeatureBlockers(required...)
	require.Len(t, blockers, 4)
	require.Equal(t, FeatureMetricsCacheAPIs, blockers[0].ID)
	require.Equal(t, StatusDisabledWant, blockers[0].Status)
	require.Equal(t, FeatureBadgerManagedTransactions, blockers[1].ID)
	require.Equal(t, StatusDisabledNeedBlocker, blockers[1].Status)
	require.Equal(t, FeatureEncryptionKeyRegistry, blockers[2].ID)
	require.Equal(t, StatusUnsupported, blockers[2].Status)
	require.Equal(t, FeatureID("future_feature"), blockers[3].ID)
	require.Equal(t, StatusUnsupported, blockers[3].Status)

	err := CheckRequiredFeatures(required...)
	require.ErrorIs(t, err, ErrUnsupportedFeature)
	require.EqualError(t, err, "dgraph treedb integration: unsupported feature: required feature set is not ready: "+
		"metrics_cache_apis=disabled_want (Badger cache sizing and metrics are desirable "+
		"for Dgraph monitoring but are not wired for TreeDB yet); "+
		"badger_managed_transactions=disabled_need_blocker (Dgraph posting store relies on externally "+
		"managed Badger timestamps that are not mapped to TreeDB yet); "+
		"encryption_key_registry=unsupported (TreeDB does not expose a Dgraph-compatible encrypted-at-rest/"+
		"key-registry contract in this integration lane); "+
		"future_feature=unsupported (unknown TreeDB feature id)")

	var readinessErr *FeatureReadinessError
	require.True(t, errors.As(err, &readinessErr))
	require.Len(t, readinessErr.Blockers, 4)
}

func TestUnsupportedFeaturesCompatibilityViewUsesRegistry(t *testing.T) {
	registry := FeatureRegistry()
	wantUnsupported := 0
	for _, feature := range registry {
		if feature.Status != StatusSupported {
			wantUnsupported++
		}
	}

	unsupported := UnsupportedFeatures()
	require.Len(t, unsupported, wantUnsupported)

	joined := strings.Join(unsupported, "\n")
	require.Contains(t, joined, "badger_managed_transactions: Dgraph posting store relies on externally "+
		"managed Badger timestamps")
	require.Contains(t, joined, "status: disabled_need_blocker")
	require.Contains(t, joined, "encryption_key_registry: TreeDB does not expose a Dgraph-compatible")
	require.Contains(t, joined, "status: unsupported")
	require.NotContains(t, joined, string(FeatureTreeDBOpen))
}
