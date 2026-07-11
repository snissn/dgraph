/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

// OperatorGateID names a final operator-facing TreeDB readiness gate.
type OperatorGateID string

const (
	GateBadgerDefault             OperatorGateID = "badger_default"
	GateBenchmarkMinimalTier      OperatorGateID = "benchmark_minimal_tier"
	GateOperationalTier           OperatorGateID = "operational_tier"
	GateProductionTier            OperatorGateID = "production_tier"
	GateTreeDBPrimitiveDurability OperatorGateID = "treedb_primitive_durability"
	GateTreeDBSelector            OperatorGateID = "treedb_selector"
	GatePostingSchemaWorkflows    OperatorGateID = "posting_schema_workflows"
	GateBackupRestoreExport       OperatorGateID = "backup_restore_export"
	GateSubscriptions             OperatorGateID = "subscriptions"
	GateEncryptionKeyRegistry     OperatorGateID = "encryption_key_registry"
	GateBenchmarkMatrix           OperatorGateID = "benchmark_matrix"
	GateDefaultDecision           OperatorGateID = "default_decision"
)

// OperatorGateStatus is the current final-readiness decision for a gate.
type OperatorGateStatus string

const (
	GateStatusPass        OperatorGateStatus = "pass"
	GateStatusFailClosed  OperatorGateStatus = "fail_closed"
	GateStatusUnsupported OperatorGateStatus = "unsupported"
	GateStatusEvidence    OperatorGateStatus = "evidence_only"
)

const (
	// DefaultDecisionKeepBadger is the only accepted final decision for this graph.
	DefaultDecisionKeepBadger = "keep_badger_default"
)

// OperatorGate describes what an operator can rely on today.
type OperatorGate struct {
	ID        OperatorGateID
	Status    OperatorGateStatus
	Decision  string
	Summary   string
	Evidence  []string
	FollowUps []string
}

var operatorGateReport = []OperatorGate{
	{
		ID:       GateBadgerDefault,
		Status:   GateStatusPass,
		Decision: "Badger remains the default Alpha posting-store backend.",
		Summary:  "Blank/default posting-store configuration normalizes to badger and uses the existing managed Badger open path.",
		Evidence: []string{
			"worker.TestNormalizePostingStoreBackend",
			"worker.TestSetConfigurationNormalizesPostingStoreBackend",
			"worker.TestPostingStoreBackendStatus",
		},
	},
	{
		ID:       GateBenchmarkMinimalTier,
		Status:   GateStatusPass,
		Decision: "TreeDB may run the explicit restricted Alpha benchmark-minimal tier.",
		Summary:  "TreeDBStore lifecycle, managed timestamps, posting metadata/discard markers, all-version iteration, GC, and stats pass the benchmark-minimal tier.",
		Evidence: []string{
			"CheckCapabilityTier(benchmark_minimal)",
			"TestCapabilityTierRequirementsAndBlockers/benchmark_minimal",
			"worker.TestServerStateTreeDBLifecycle",
			"worker.TestTreeDBRestrictedRuntimeMutationQuerySchemaRestart",
		},
	},
	{
		ID:       GateOperationalTier,
		Status:   GateStatusFailClosed,
		Decision: "Backup/import/export, subscriptions, TTL, Badger protobuf translation, and monitoring remain outside the Alpha benchmark.",
		Summary:  "Operational capabilities are cumulative but do not block benchmark-minimal startup; each unsupported invocation must fail explicitly.",
		Evidence: []string{
			"CheckCapabilityTier(operational)",
			"TestOptionalCapabilityInvocationFailsClosed",
		},
		FollowUps: []string{
			"Ticket operational parity only after the Alpha benchmark decision gate justifies continuing.",
		},
	},
	{
		ID:       GateProductionTier,
		Status:   GateStatusUnsupported,
		Decision: "TreeDB is not a production backend and encryption/key-registry support is not implemented in this lane.",
		Summary:  "Production is a cumulative future tier, not a claim or deliverable of the Alpha benchmark graph.",
		Evidence: []string{
			"CheckCapabilityTier(production)",
			"TestResolveOptionsRejectsUnsupportedFeatures",
		},
		FollowUps: []string{
			"Create a production-readiness graph only after operational parity and durability evidence exist.",
		},
	},
	{
		ID:       GateTreeDBPrimitiveDurability,
		Status:   GateStatusPass,
		Decision: "TreeDB primitives and the posting adapter can open, write, close, reopen, and serve the restricted Alpha runtime.",
		Summary:  "TreeDBStore satisfies benchmark-minimal posting semantics and the restricted Alpha lifecycle owns exactly one backend handle.",
		Evidence: []string{
			"TestOpenSmoke",
			"TestOpenReopenDurability",
			"posting.TestTreeDBStoreDiscardFloorPruneAndReopen",
		},
	},
	{
		ID:       GateTreeDBSelector,
		Status:   GateStatusPass,
		Decision: "An explicit TreeDB selector opens only the restricted benchmark-minimal tier.",
		Summary:  "The selector owns exactly the requested backend and never silently falls back from TreeDB to Badger.",
		Evidence: []string{
			"worker.TestCheckPostingStoreBackendReadyAllowsBenchmarkMinimalTreeDB",
			"worker.TestServerStateTreeDBLifecycle",
			"CheckPostingBackendReady",
		},
	},
	{
		ID:       GatePostingSchemaWorkflows,
		Status:   GateStatusPass,
		Decision: "Basic point posting mutations, reads, and schema persistence are supported in the restricted TreeDB Alpha runtime.",
		Summary:  "The benchmark-minimal runtime exercises posting writes/reads and schema update/load across close and reopen; later-tier indexed and transfer workflows remain gated.",
		Evidence: []string{
			"posting.TestTreeDBStoreMatchesBadgerGoldenIteratorTrace",
			"worker.TestTreeDBRestrictedRuntimeMutationQuerySchemaRestart",
		},
	},
	{
		ID:       GateBackupRestoreExport,
		Status:   GateStatusFailClosed,
		Decision: "Backup, restore, export, import, and snapshot workflows remain Badger-only for TreeDB requests.",
		Summary:  "TreeDB does not provide Dgraph-compatible Badger stream import/export contracts in this scaffold.",
		Evidence: []string{
			"PostingCompatibilityMatrix stream_import_export row",
			"BenchmarkDgraphTreeDBMatrix/Blocked/StreamBackupExport",
			"BenchmarkDgraphTreeDBMatrix/Blocked/StreamWriterImport",
		},
		FollowUps: []string{
			"Add TreeDB stream/export/import contract or explicit replacement workflow tests before enabling TreeDB runtime.",
		},
	},
	{
		ID:       GateSubscriptions,
		Status:   GateStatusFailClosed,
		Decision: "TreeDB Alpha requires restricted internal worker event delivery; full Badger DB.Subscribe compatibility remains unavailable.",
		Summary:  "The Dgraph-owned bridge filters future ordered successful commits into the pb.KVList fields used by ACL and GraphQL schema watchers; startup fails closed when delivery is disabled.",
		Evidence: []string{
			"PostingCompatibilityMatrix subscriptions row",
			"posting.TestCommitEventBusOrdersOutOfOrderCompletionsAndSkipsFailures",
			"posting.TestCommitEventBusBackpressureCancellationAndShutdown",
			"worker.TestTreeDBCommitEventSubscriptionStreamsFutureCommitsAndCancels",
			"worker.TestServerStateTreeDBRejectsDisabledCommitEvents",
			"worker.TestTreeDBDisabledSubscriptionsExitWithoutRetryOrStateAccess",
		},
		FollowUps: []string{
			"Require a separate operational-tier contract before claiming full Badger subscription parity.",
		},
	},
	{
		ID:       GateEncryptionKeyRegistry,
		Status:   GateStatusUnsupported,
		Decision: "TreeDB encryption/key-registry integration is unsupported in this integration lane.",
		Summary:  "Encryption requests fail closed instead of opening TreeDB without Dgraph-compatible key handling.",
		Evidence: []string{
			"TestResolveOptionsRejectsUnsupportedFeatures",
			"PostingCompatibilityMatrix encryption_key_registry row",
		},
		FollowUps: []string{
			"Link upstream TreeDB encryption/key-registry support before revisiting this gate.",
		},
	},
	{
		ID:       GateBenchmarkMatrix,
		Status:   GateStatusPass,
		Decision: "The Dgraph Badger-vs-TreeDB benchmark matrix is available for final and future before/after evidence.",
		Summary:  "Current TreeDB rows are primitive evidence; posting adapter benchmarks live in posting, and blocked rows document later-tier contracts.",
		Evidence: []string{
			"BenchmarkDgraphTreeDBMatrix",
			"worker/treedb/run_benchmark_matrix.sh",
		},
	},
	{
		ID:       GateDefaultDecision,
		Status:   GateStatusPass,
		Decision: DefaultDecisionKeepBadger,
		Summary:  "Final decision for this graph: keep Badger as the default backend; TreeDB is explicit, experimental, and limited to the restricted benchmark-minimal tier, with unsupported workflows failing closed.",
		Evidence: []string{
			"worker.PostingStoreDefaults backend=badger",
			"worker.TestNormalizePostingStoreBackend",
		},
	},
}

// OperatorGateReport returns a copy of the final TreeDB operator gate report.
func OperatorGateReport() []OperatorGate {
	out := make([]OperatorGate, 0, len(operatorGateReport))
	for _, gate := range operatorGateReport {
		out = append(out, cloneOperatorGate(gate))
	}
	return out
}

// TreeDBDefaultDecision returns the final backend default decision for this graph.
func TreeDBDefaultDecision() string {
	return DefaultDecisionKeepBadger
}

func cloneOperatorGate(gate OperatorGate) OperatorGate {
	out := gate
	out.Evidence = append([]string(nil), gate.Evidence...)
	out.FollowUps = append([]string(nil), gate.FollowUps...)
	return out
}
