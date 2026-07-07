/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

// OperatorGateID names a final operator-facing TreeDB readiness gate.
type OperatorGateID string

const (
	GateBadgerDefault             OperatorGateID = "badger_default"
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
		ID:       GateTreeDBPrimitiveDurability,
		Status:   GateStatusEvidence,
		Decision: "TreeDB primitives can open, write, close, and reopen in the scaffold, but this is not a Dgraph posting-store backend.",
		Summary:  "The TreeDB scaffold validates primitive durability only; it does not satisfy Badger posting semantics.",
		Evidence: []string{
			"TestOpenSmoke",
			"TestOpenReopenDurability",
		},
	},
	{
		ID:       GateTreeDBSelector,
		Status:   GateStatusFailClosed,
		Decision: "An explicit TreeDB selector value is accepted but startup refuses to open TreeDB while blockers remain.",
		Summary:  "There is no silent fallback from requested TreeDB to Badger.",
		Evidence: []string{
			"worker.TestCheckPostingStoreBackendReadyFailsClosedForTreeDB",
			"CheckPostingBackendReady",
		},
		FollowUps: []string{
			"Resolve managed timestamp transactions before opening TreeDB as a posting store.",
			"Resolve metadata, all-version iteration, stream, subscription, protobuf, and encryption gates before enabling runtime TreeDB.",
		},
	},
	{
		ID:       GatePostingSchemaWorkflows,
		Status:   GateStatusFailClosed,
		Decision: "Dgraph posting/schema workflows remain Badger-only until the adapter satisfies metadata and all-version semantics.",
		Summary:  "Posting writes/reads and schema scans require Badger UserMeta, discard markers, and all-version iterators.",
		Evidence: []string{
			"posting.TestBadgerStorePreservesManagedTimestampsMetadataAndIteration",
			"posting.TestTxnWriterForStorePreservesBadgerWriteBehavior",
			"PostingCompatibilityMatrix entry_metadata_ttl and all_version_iteration rows",
		},
		FollowUps: []string{
			"Implement TreeDB adapter tests for posting list write/read and schema load/delete before changing this gate.",
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
		Decision: "Subscriptions remain Badger-only for TreeDB requests.",
		Summary:  "worker.SubscribeForUpdates requires Badger subscription filtering, ordering, cancellation, and pb.KVList payloads.",
		Evidence: []string{
			"PostingCompatibilityMatrix subscriptions row",
			"BenchmarkDgraphTreeDBMatrix/Blocked/Subscriptions",
		},
		FollowUps: []string{
			"Add TreeDB-backed subscription semantics and tests before enabling runtime TreeDB.",
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
		Summary:  "Current TreeDB rows are primitive evidence; blocked rows explicitly document Dgraph-required contracts that cannot run yet.",
		Evidence: []string{
			"BenchmarkDgraphTreeDBMatrix",
			"worker/treedb/run_benchmark_matrix.sh",
		},
	},
	{
		ID:       GateDefaultDecision,
		Status:   GateStatusPass,
		Decision: DefaultDecisionKeepBadger,
		Summary:  "Final decision for this graph: keep Badger as the default backend; TreeDB remains explicit, experimental, and fail-closed.",
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
