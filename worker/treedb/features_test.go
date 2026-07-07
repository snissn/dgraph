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
