/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperatorGateReportDocumentsFinalTreeDBState(t *testing.T) {
	report := OperatorGateReport()
	require.NotEmpty(t, report)

	byID := make(map[OperatorGateID]OperatorGate)
	for _, gate := range report {
		require.NotEmpty(t, gate.ID)
		require.NotEmpty(t, gate.Status)
		require.NotEmpty(t, gate.Decision)
		require.NotEmpty(t, gate.Summary)
		require.NotEmpty(t, gate.Evidence)
		require.NotContains(t, byID, gate.ID)
		byID[gate.ID] = gate
	}

	require.Equal(t, GateStatusPass, byID[GateBadgerDefault].Status)
	require.Contains(t, byID[GateBadgerDefault].Decision, "Badger remains the default")
	require.Equal(t, GateStatusFailClosed, byID[GateBenchmarkMinimalTier].Status)
	require.Contains(t, byID[GateBenchmarkMinimalTier].Summary, "all-version iteration")
	require.Equal(t, GateStatusFailClosed, byID[GateOperationalTier].Status)
	require.Contains(t, byID[GateOperationalTier].Summary, "do not block benchmark-minimal startup")
	require.Equal(t, GateStatusUnsupported, byID[GateProductionTier].Status)
	require.Contains(t, byID[GateProductionTier].Summary, "future tier")

	require.Equal(t, GateStatusEvidence, byID[GateTreeDBPrimitiveDurability].Status)
	require.Contains(t, byID[GateTreeDBPrimitiveDurability].Summary, "not satisfy Badger posting semantics")

	require.Equal(t, GateStatusFailClosed, byID[GateTreeDBSelector].Status)
	require.Contains(t, byID[GateTreeDBSelector].Summary, "no silent fallback")

	require.Equal(t, GateStatusFailClosed, byID[GatePostingSchemaWorkflows].Status)
	require.Equal(t, GateStatusFailClosed, byID[GateBackupRestoreExport].Status)
	require.Equal(t, GateStatusFailClosed, byID[GateSubscriptions].Status)
	require.Equal(t, GateStatusUnsupported, byID[GateEncryptionKeyRegistry].Status)
	require.Equal(t, GateStatusPass, byID[GateBenchmarkMatrix].Status)
	require.Equal(t, DefaultDecisionKeepBadger, byID[GateDefaultDecision].Decision)
	require.Equal(t, DefaultDecisionKeepBadger, TreeDBDefaultDecision())
}

func TestOperatorGateReportIsImmutable(t *testing.T) {
	report := OperatorGateReport()
	require.NotEmpty(t, report)

	report[0].Evidence[0] = "mutated"
	if len(report[0].FollowUps) > 0 {
		report[0].FollowUps[0] = "mutated"
	}

	fresh := OperatorGateReport()
	require.NotEqual(t, "mutated", fresh[0].Evidence[0])
	if len(fresh[0].FollowUps) > 0 {
		require.NotEqual(t, "mutated", fresh[0].FollowUps[0])
	}
}
