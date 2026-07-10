/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

import "fmt"

// CompatibilityFamilyID names a Dgraph-required Badger compatibility family
// that must be resolved before TreeDB can be exposed as a posting-store backend.
type CompatibilityFamilyID string

const (
	CompatibilityManagedTimestampTransactions CompatibilityFamilyID = "managed_timestamp_transactions"
	CompatibilityCommandWALTransactions       CompatibilityFamilyID = "command_wal_transactions"
	CompatibilityEntryMetadata                CompatibilityFamilyID = "entry_metadata"
	CompatibilityEntryTTL                     CompatibilityFamilyID = "entry_ttl"
	// CompatibilityEntryMetadataTTL retains the legacy operator-facing family
	// ID while metadata and TTL are modeled by separate tier rows.
	// Deprecated: use CompatibilityEntryMetadata and CompatibilityEntryTTL.
	CompatibilityEntryMetadataTTL      CompatibilityFamilyID = "entry_metadata_ttl"
	CompatibilityAllVersionIteration   CompatibilityFamilyID = "all_version_iteration"
	CompatibilityStreamImportExport    CompatibilityFamilyID = "stream_import_export"
	CompatibilitySubscriptions         CompatibilityFamilyID = "subscriptions"
	CompatibilityEncryptionKeyRegistry CompatibilityFamilyID = "encryption_key_registry"
	CompatibilityBadgerProtobuf        CompatibilityFamilyID = "badger_protobuf"
	CompatibilityMetricsCache          CompatibilityFamilyID = "metrics_cache"
)

// CompatibilityRecord is the explicit Dgraph posting-store compatibility
// decision for one Badger feature family.
type CompatibilityRecord struct {
	ID              CompatibilityFamilyID
	Feature         FeatureID
	Status          FeatureStatus
	RequiredTier    CapabilityTier
	Decision        string
	OperatorMessage string
	RequiredAPIs    []string
	DgraphCallSites []string
	Evidence        []string
}

var postingCompatibilityMatrix = []CompatibilityRecord{
	{
		ID:              CompatibilityManagedTimestampTransactions,
		Feature:         FeatureBadgerManagedTransactions,
		Status:          StatusDisabledNeedBlocker,
		RequiredTier:    TierBenchmarkMinimal,
		Decision:        "fail closed until a TreeDB adapter can preserve externally managed read/write timestamps and commit-at semantics",
		OperatorMessage: "TreeDB posting-store backend is disabled because Dgraph requires Badger managed transactions: OpenManaged, NewTransactionAt, CommitAt, NewManagedWriteBatch, and SetEntryAt.",
		RequiredAPIs: []string{
			"badger.OpenManaged",
			"(*badger.DB).NewTransactionAt",
			"(*badger.Txn).CommitAt",
			"(*badger.DB).NewManagedWriteBatch",
			"(*badger.WriteBatch).SetEntryAt",
		},
		DgraphCallSites: []string{
			"worker.ServerState.InitStorage",
			"posting.TxnWriter.SetAt",
			"posting.Txn.CommitToDisk",
			"posting.MemoryLayer.readFromDisk",
			"worker/sort.go rollup paths",
		},
		Evidence: []string{
			"posting.Store adapter contract requires managed read and write transactions",
			"posting.TestBadgerStorePreservesManagedTimestampsMetadataAndIteration",
		},
	},
	{
		ID:              CompatibilityCommandWALTransactions,
		Feature:         FeatureCommandWALConditionalTransactions,
		Status:          StatusUnsupported,
		Decision:        "not required: Dgraph owns posting conflict detection instead of delegating it to TreeDB-native conditional transactions",
		OperatorMessage: "TreeDB-native conditional transactions are unavailable; Dgraph's posting adapter must use Dgraph-owned conflict detection.",
		RequiredAPIs: []string{
			"TreeDB durable command-WAL profile",
			"TreeDB conditional transaction begin/commit/abort semantics",
		},
		DgraphCallSites: []string{
			"worker/treedb.ResolveOptions",
			"posting.Txn.CommitToDisk",
		},
		Evidence: []string{
			"TestOpenSmoke verifies NewConditionalTxn fails closed under DefaultProfile",
		},
	},
	{
		ID:              CompatibilityEntryMetadata,
		Feature:         FeatureBadgerEntryMetadata,
		Status:          StatusDisabledNeedBlocker,
		RequiredTier:    TierBenchmarkMinimal,
		Decision:        "fail closed until UserMeta and discard markers survive TreeDB writes, reads, and iteration",
		OperatorMessage: "TreeDB benchmark-minimal posting storage is disabled because Dgraph posting lists require Badger UserMeta and discard markers.",
		RequiredAPIs: []string{
			"badger.Entry.UserMeta",
			"(*badger.Item).UserMeta",
			"(*badger.Entry).WithDiscard",
		},
		DgraphCallSites: []string{
			"posting.ReadPostingList",
			"posting.TxnWriter.SetAt",
			"worker.restore_map.go",
			"worker.export.go",
		},
		Evidence: []string{
			"posting.Store Entry and Item interfaces include UserMeta, ExpiresAt, and DiscardEarlierVersions",
			"posting.TestBadgerStorePreservesManagedTimestampsMetadataAndIteration",
		},
	},
	{
		ID:              CompatibilityEntryTTL,
		Feature:         FeatureBadgerEntryTTL,
		Status:          StatusUnsupported,
		RequiredTier:    TierOperational,
		Decision:        "reject nonzero expiry at invocation until TreeDB has a tested TTL contract",
		OperatorMessage: "TreeDB entry TTL is unavailable; nonzero ExpiresAt values are rejected instead of being dropped.",
		RequiredAPIs: []string{
			"badger.Entry.ExpiresAt",
			"(*badger.Item).ExpiresAt",
		},
		DgraphCallSites: []string{
			"posting.TxnWriter.SetAt",
			"worker.restore_map.go",
		},
		Evidence: []string{
			"FeatureRegistry badger_entry_ttl invocation gate",
		},
	},
	{
		ID:              CompatibilityEntryMetadataTTL,
		Feature:         FeatureBadgerEntryMetadataTTL,
		Status:          StatusUnsupported,
		Decision:        "retain the legacy combined family as a fail-closed compatibility gate while callers migrate to the split metadata and TTL families",
		OperatorMessage: "TreeDB entry metadata/TTL compatibility remains unavailable; use the split entry_metadata and entry_ttl readiness rows for tier decisions.",
		RequiredAPIs: []string{
			"badger.Entry.UserMeta",
			"(*badger.Item).UserMeta",
			"badger.Entry.ExpiresAt",
			"(*badger.Item).ExpiresAt",
		},
		DgraphCallSites: []string{
			"posting.TxnWriter.SetAt",
			"posting.ReadPostingList",
			"worker.restore_map.go",
		},
		Evidence: []string{
			"legacy entry_metadata_ttl compatibility family",
			"FeatureRegistry split metadata and TTL invocation gates",
		},
	},
	{
		ID:              CompatibilityAllVersionIteration,
		Feature:         FeatureBadgerAllVersionIterators,
		Status:          StatusDisabledNeedBlocker,
		RequiredTier:    TierBenchmarkMinimal,
		Decision:        "fail closed until prefix, reverse, all-version, prefetch, and key-iterator behavior matches Dgraph posting-list expectations",
		OperatorMessage: "TreeDB posting-store backend is disabled because Dgraph reconstructs posting lists with Badger all-version iterators.",
		RequiredAPIs: []string{
			"badger.IteratorOptions.Prefix",
			"badger.IteratorOptions.Reverse",
			"badger.IteratorOptions.AllVersions",
			"badger.IteratorOptions.PrefetchValues",
			"(*badger.Txn).NewKeyIterator",
		},
		DgraphCallSites: []string{
			"posting.ReadPostingList",
			"posting.List.getMutation",
			"worker/tokens.go",
			"worker/sort.go",
			"worker/export.go",
		},
		Evidence: []string{
			"posting.Store IteratorOptions includes Prefix, Reverse, AllVersions, and PrefetchValues",
			"posting.TestBadgerStorePreservesManagedTimestampsMetadataAndIteration",
		},
	},
	{
		ID:              CompatibilityStreamImportExport,
		Feature:         FeatureBadgerStreamImportExport,
		Status:          StatusDisabledNeedBlocker,
		RequiredTier:    TierOperational,
		Decision:        "fail closed until TreeDB can provide Dgraph-compatible snapshot/export/import stream contracts or explicit replacement workflows",
		OperatorMessage: "TreeDB posting-store backend is disabled because backup, export, restore, and snapshot paths require Badger stream APIs.",
		RequiredAPIs: []string{
			"(*badger.DB).NewStreamAt",
			"(*badger.Stream).Orchestrate",
			"(*badger.DB).NewStreamWriter",
			"badger.Stream.KeyToList",
			"badger.Stream.ChooseKey",
		},
		DgraphCallSites: []string{
			"worker/export.go",
			"worker/snapshot.go",
			"worker/online_restore.go",
			"worker/restore_map.go",
		},
		Evidence: []string{
			"BenchmarkDgraphTreeDBMatrix/Blocked/StreamBackupExport",
			"BenchmarkDgraphTreeDBMatrix/Blocked/StreamWriterImport",
		},
	},
	{
		ID:              CompatibilitySubscriptions,
		Feature:         FeatureBadgerSubscriptions,
		Status:          StatusDisabledNeedBlocker,
		RequiredTier:    TierOperational,
		Decision:        "fail closed until worker update subscriptions have TreeDB-backed ordering, filtering, and cancellation semantics",
		OperatorMessage: "TreeDB posting-store backend is disabled because worker.SubscribeForUpdates requires Badger subscription behavior.",
		RequiredAPIs: []string{
			"badger.DB.Subscribe",
			"badger pb.Match prefix filters",
			"badger pb.KVList callback payloads",
		},
		DgraphCallSites: []string{
			"worker.SubscribeForUpdates",
			"worker/groups.go",
		},
		Evidence: []string{
			"BenchmarkDgraphTreeDBMatrix/Blocked/Subscriptions",
		},
	},
	{
		ID:              CompatibilityEncryptionKeyRegistry,
		Feature:         FeatureEncryptionKeyRegistry,
		Status:          StatusUnsupported,
		RequiredTier:    TierProduction,
		Decision:        "unsupported in this integration lane until TreeDB exposes Dgraph-compatible encryption/key-registry semantics",
		OperatorMessage: "TreeDB posting-store backend is disabled when encryption or key-registry support is required.",
		RequiredAPIs: []string{
			"Badger encryption-at-rest options",
			"Dgraph key registry integration",
		},
		DgraphCallSites: []string{
			"worker.ServerState.InitStorage",
			"worker/treedb.ResolveOptions",
		},
		Evidence: []string{
			"ResolveOptions RequireEncryption fail-closed check",
			"TestResolveOptionsRejectsUnsupportedFeatures",
		},
	},
	{
		ID:              CompatibilityBadgerProtobuf,
		Feature:         FeatureBadgerProtobufCompatibility,
		Status:          StatusDisabledNeedBlocker,
		RequiredTier:    TierOperational,
		Decision:        "fail closed until TreeDB import/export/subscription code can produce or replace Badger pb.KV, pb.KVList, and pb.Match shapes without data loss",
		OperatorMessage: "TreeDB posting-store backend is disabled because Dgraph backup and subscription paths exchange Badger protobuf payloads.",
		RequiredAPIs: []string{
			"github.com/dgraph-io/badger/v4/pb.KV",
			"github.com/dgraph-io/badger/v4/pb.KVList",
			"github.com/dgraph-io/badger/v4/pb.Match",
		},
		DgraphCallSites: []string{
			"posting.TxnWriter.Write",
			"worker.restore_map.go",
			"worker.export.go",
			"worker.SubscribeForUpdates",
		},
		Evidence: []string{
			"posting.TxnWriter.Write continues to accept badger pb.KVList at the current boundary",
		},
	},
	{
		ID:              CompatibilityMetricsCache,
		Feature:         FeatureMetricsCacheAPIs,
		Status:          StatusDisabledWant,
		RequiredTier:    TierOperational,
		Decision:        "not a selector blocker by itself, but must be surfaced as disabled-want until TreeDB-native metrics/cache fields exist",
		OperatorMessage: "TreeDB metrics and cache counters are not wired; monitoring must treat them as unavailable, not zero.",
		RequiredAPIs: []string{
			"Badger cache sizing options",
			"Badger LSM/value-log metrics",
		},
		DgraphCallSites: []string{
			"worker.ServerState.InitStorage",
			"Dgraph metrics/monitoring paths",
		},
		Evidence: []string{
			"FeatureRegistry metrics_cache_apis disabled_want status",
		},
	},
}

// PostingBackendRequiredFeatures returns the feature set that must be supported
// before the experimental TreeDB posting-store backend can be enabled.
func PostingBackendRequiredFeatures() []FeatureID {
	required, err := RequiredFeaturesForTier(TierBenchmarkMinimal)
	if err != nil {
		panic(err)
	}
	return required
}

// PostingCompatibilityMatrix returns a copy of the current compatibility
// decisions for Dgraph-required Badger feature families.
func PostingCompatibilityMatrix() []CompatibilityRecord {
	out := make([]CompatibilityRecord, 0, len(postingCompatibilityMatrix))
	for _, record := range postingCompatibilityMatrix {
		out = append(out, cloneCompatibilityRecord(record))
	}
	return out
}

// PostingBackendBlockers returns compatibility rows that still block enabling
// TreeDB as a posting-store backend.
func PostingBackendBlockers() []CompatibilityRecord {
	blockers := make([]CompatibilityRecord, 0)
	required := make(map[FeatureID]struct{})
	for _, feature := range PostingBackendRequiredFeatures() {
		required[feature] = struct{}{}
	}
	for _, record := range postingCompatibilityMatrix {
		if _, ok := required[record.Feature]; !ok {
			continue
		}
		if record.Status != StatusSupported {
			blockers = append(blockers, cloneCompatibilityRecord(record))
		}
	}
	return blockers
}

// PostingBackendBlockersForTier returns compatibility rows that block a
// cumulative capability tier. Primitive registry-only rows are enforced by
// CheckCapabilityTier but do not appear in this compatibility-specific view.
func PostingBackendBlockersForTier(tier CapabilityTier) ([]CompatibilityRecord, error) {
	requiredFeatures, err := RequiredFeaturesForTier(tier)
	if err != nil {
		return nil, err
	}
	required := make(map[FeatureID]struct{}, len(requiredFeatures))
	for _, feature := range requiredFeatures {
		required[feature] = struct{}{}
	}
	blockers := make([]CompatibilityRecord, 0)
	for _, record := range postingCompatibilityMatrix {
		if _, ok := required[record.Feature]; ok && record.Status != StatusSupported {
			blockers = append(blockers, cloneCompatibilityRecord(record))
		}
	}
	return blockers, nil
}

// CheckPostingBackendReady fails closed for every unsupported Dgraph-required
// Badger feature family. Runtime selector code should call this before opening
// a TreeDB-backed posting store.
func CheckPostingBackendReady() error {
	if err := CheckCapabilityTier(TierBenchmarkMinimal); err != nil {
		return fmt.Errorf("TreeDB posting-store backend is not ready for %s; refusing to enable experimental TreeDB: %w",
			TierBenchmarkMinimal, err)
	}
	return nil
}

func cloneCompatibilityRecord(record CompatibilityRecord) CompatibilityRecord {
	out := record
	out.RequiredAPIs = append([]string(nil), record.RequiredAPIs...)
	out.DgraphCallSites = append([]string(nil), record.DgraphCallSites...)
	out.Evidence = append([]string(nil), record.Evidence...)
	return out
}
