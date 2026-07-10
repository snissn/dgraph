/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package treedb

import (
	"fmt"
	"strings"
)

// FeatureID identifies a Dgraph/Badger capability relevant to the TreeDB
// integration readiness gate.
type FeatureID string

const (
	FeatureTreeDBOpen                        FeatureID = "treedb_open"
	FeatureDurableCommandWALProfile          FeatureID = "durable_command_wal_profile"
	FeaturePointReadWrite                    FeatureID = "point_read_write"
	FeatureVersionedPointRead                FeatureID = "versioned_point_read"
	FeatureBatchWrite                        FeatureID = "batch_write"
	FeatureSnapshotRead                      FeatureID = "snapshot_read"
	FeatureForwardIterator                   FeatureID = "forward_iterator"
	FeaturePostingStoreAdapterContract       FeatureID = "posting_store_adapter_contract"
	FeatureTreeDBStoreImplementation         FeatureID = "treedb_store_implementation"
	FeatureBadgerManagedTransactions         FeatureID = "badger_managed_transactions"
	FeatureCommandWALConditionalTransactions FeatureID = "command_wal_conditional_transactions"
	FeatureBadgerEntryMetadata               FeatureID = "badger_entry_metadata"
	FeatureBadgerEntryTTL                    FeatureID = "badger_entry_ttl"
	// FeatureBadgerEntryMetadataTTL retains the legacy operator-facing ID and
	// continues to fail closed for the combined metadata-and-TTL contract.
	// Deprecated: use FeatureBadgerEntryMetadata and FeatureBadgerEntryTTL.
	FeatureBadgerEntryMetadataTTL      FeatureID = "badger_entry_metadata_ttl"
	FeatureBadgerAllVersionIterators   FeatureID = "badger_all_version_iterators"
	FeatureBadgerStreamImportExport    FeatureID = "badger_stream_import_export"
	FeatureBadgerSubscriptions         FeatureID = "badger_subscriptions"
	FeatureEncryptionKeyRegistry       FeatureID = "encryption_key_registry"
	FeatureBadgerProtobufCompatibility FeatureID = "badger_protobuf_compatibility"
	FeatureMetricsCacheAPIs            FeatureID = "metrics_cache_apis"
	FeatureLifecycleGCStats            FeatureID = "lifecycle_gc_stats"
	FeatureInMemoryPostingStore        FeatureID = "in_memory_posting_store"
)

// CapabilityTier names a cumulative TreeDB integration gate. A later tier
// includes every requirement from the tiers before it.
type CapabilityTier string

const (
	TierBenchmarkMinimal CapabilityTier = "benchmark_minimal"
	TierOperational      CapabilityTier = "operational"
	TierProduction       CapabilityTier = "production"
)

// FeatureStatus classifies whether a TreeDB capability can be required by a
// caller today.
type FeatureStatus string

const (
	StatusSupported           FeatureStatus = "supported"
	StatusDisabledWant        FeatureStatus = "disabled_want"
	StatusDisabledNeedBlocker FeatureStatus = "disabled_need_blocker"
	StatusUnsupported         FeatureStatus = "unsupported"
)

// FeatureRecord is one registry entry in the TreeDB readiness taxonomy.
type FeatureRecord struct {
	ID           FeatureID
	Status       FeatureStatus
	RequiredTier CapabilityTier
	Reason       string
	Evidence     []string
}

var featureRegistry = []FeatureRecord{
	{
		ID:           FeatureTreeDBOpen,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB opens through ResolveOptions/Open with Dgraph-shaped durable defaults",
		Evidence: []string{
			"TestResolveOptionsUsesDgraphDefaults",
			"TestOpenSmoke",
		},
	},
	{
		ID:           FeatureDurableCommandWALProfile,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "DefaultProfile selects TreeDB's public durable command-WAL profile",
		Evidence: []string{
			"TestResolveOptionsUsesDgraphDefaults",
		},
	},
	{
		ID:           FeaturePointReadWrite,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB point reads and writes are exercised through the scaffold handle",
		Evidence: []string{
			"TestOpenSmoke",
		},
	},
	{
		ID:           FeatureVersionedPointRead,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB GetVersioned compiles and is exercised for current-value reads",
		Evidence: []string{
			"TestOpenSmoke",
			"dgraphTreeDBAPI compile assertion",
		},
	},
	{
		ID:           FeatureBatchWrite,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB batch Set/Write/Close compiles and is exercised by the smoke test",
		Evidence: []string{
			"TestOpenSmoke",
			"dgraphTreeDBBatchAPI compile assertion",
		},
	},
	{
		ID:           FeatureSnapshotRead,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB snapshots compile and are exercised for point reads",
		Evidence: []string{
			"TestOpenSmoke",
			"dgraphTreeDBSnapshotAPI compile assertion",
		},
	},
	{
		ID:           FeatureForwardIterator,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB forward iterators compile and are exercised by the smoke test",
		Evidence: []string{
			"TestOpenSmoke",
			"dgraphTreeDBAPI compile assertion",
		},
	},
	{
		ID:           FeaturePostingStoreAdapterContract,
		Status:       StatusSupported,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "Dgraph has a narrow posting.Store adapter boundary with a Badger implementation for managed reads, managed writes, metadata, expiry, and all-version iteration",
		Evidence: []string{
			"posting.TestBadgerStorePreservesManagedTimestampsMetadataAndIteration",
			"posting.TestTxnWriterForStorePreservesBadgerWriteBehavior",
		},
	},
	{
		ID:           FeatureTreeDBStoreImplementation,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "the posting.Store seam exists, but no TreeDBStore implementation satisfies it yet",
		Evidence: []string{
			"posting.NewBadgerStore is the only posting.Store implementation",
			"Dgraph issue #21",
		},
	},
	{
		ID:           FeatureBadgerManagedTransactions,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "Dgraph posting store relies on externally managed Badger timestamps that are not mapped to TreeDB yet",
		Evidence: []string{
			"worker/treedb README compatibility inventory: OpenManaged, NewTransactionAt, CommitAt",
			"worker/treedb README compatibility inventory: NewManagedWriteBatch, SetEntryAt",
		},
	},
	{
		ID:     FeatureCommandWALConditionalTransactions,
		Status: StatusUnsupported,
		Reason: "Dgraph owns posting conflict detection, so TreeDB-native conditional transactions are not required by any integration tier",
		Evidence: []string{
			"TestOpenSmoke verifies NewConditionalTxn fails closed under DefaultProfile",
		},
	},
	{
		ID:           FeatureBadgerEntryMetadata,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "Dgraph posting values require UserMeta and discard-earlier-version markers that do not have a TreeDB compatibility contract yet",
		Evidence: []string{
			"worker/treedb README compatibility inventory: Entry.UserMeta, Item.UserMeta, Entry.WithDiscard",
		},
	},
	{
		ID:           FeatureBadgerEntryTTL,
		Status:       StatusUnsupported,
		RequiredTier: TierOperational,
		Reason:       "nonzero Badger ExpiresAt values are outside the benchmark-minimal contract and must fail closed until TreeDB has an expiry contract",
		Evidence: []string{
			"worker/treedb README compatibility inventory: Entry.ExpiresAt, Item.ExpiresAt",
		},
	},
	{
		ID:     FeatureBadgerEntryMetadataTTL,
		Status: StatusUnsupported,
		Reason: "legacy combined metadata/TTL requests remain blocked unless both split compatibility contracts are supported",
		Evidence: []string{
			"FeatureRegistry badger_entry_metadata and badger_entry_ttl split rows",
			"legacy badger_entry_metadata_ttl invocation gate",
		},
	},
	{
		ID:           FeatureBadgerAllVersionIterators,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "Dgraph relies on Badger key/all-version iterator options that are not mapped to TreeDB iterators yet",
		Evidence: []string{
			"worker/treedb README compatibility inventory: NewKeyIterator, IteratorOptions.AllVersions, Prefix, PrefetchValues",
		},
	},
	{
		ID:           FeatureBadgerStreamImportExport,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierOperational,
		Reason:       "Dgraph backup/import/export flows use Badger stream APIs without a TreeDB equivalent in this scaffold",
		Evidence: []string{
			"worker/treedb README compatibility inventory: NewStreamAt, Stream.Orchestrate, NewStreamWriter",
		},
	},
	{
		ID:           FeatureBadgerSubscriptions,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierOperational,
		Reason:       "worker.SubscribeForUpdates depends on Badger subscription semantics not exposed by TreeDB here",
		Evidence: []string{
			"worker/treedb README compatibility inventory: worker.SubscribeForUpdates",
		},
	},
	{
		ID:           FeatureEncryptionKeyRegistry,
		Status:       StatusUnsupported,
		RequiredTier: TierProduction,
		Reason:       "TreeDB does not expose a Dgraph-compatible encrypted-at-rest/key-registry contract in this integration lane",
		Evidence: []string{
			"ResolveOptions RequireEncryption fail-closed check",
			"TestResolveOptionsRejectsUnsupportedFeatures",
		},
	},
	{
		ID:           FeatureBadgerProtobufCompatibility,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierOperational,
		Reason:       "Dgraph backup and stream paths consume Badger protobuf KV/KVList/Match shapes that TreeDB does not emit",
		Evidence: []string{
			"worker/treedb README compatibility inventory: github.com/dgraph-io/badger/v4/pb KV/KVList/Match",
		},
	},
	{
		ID:           FeatureMetricsCacheAPIs,
		Status:       StatusDisabledWant,
		RequiredTier: TierOperational,
		Reason:       "Badger cache sizing and metrics are desirable for Dgraph monitoring but are not wired for TreeDB yet",
		Evidence: []string{
			"worker/treedb README compatibility inventory: cache metrics and cache sizing APIs",
		},
	},
	{
		ID:           FeatureLifecycleGCStats,
		Status:       StatusDisabledNeedBlocker,
		RequiredTier: TierBenchmarkMinimal,
		Reason:       "TreeDB close, value-log GC, full compaction, and stats APIs compile, but the Alpha lifecycle does not invoke them through a backend-neutral contract yet",
		Evidence: []string{
			"dgraphTreeDBAPI compile assertion",
			"TestOpenSmoke",
		},
	},
	{
		ID:     FeatureInMemoryPostingStore,
		Status: StatusUnsupported,
		Reason: "Dgraph's posting store is persistent; in-memory TreeDB mode is intentionally not supported here",
		Evidence: []string{
			"ResolveOptions RequireInMemory fail-closed check",
			"TestResolveOptionsRejectsUnsupportedFeatures",
		},
	},
}

var capabilityTierOrder = []CapabilityTier{
	TierBenchmarkMinimal,
	TierOperational,
	TierProduction,
}

// CapabilityTiers returns the ordered cumulative integration tiers.
func CapabilityTiers() []CapabilityTier {
	return append([]CapabilityTier(nil), capabilityTierOrder...)
}

// RequiredFeaturesForTier returns the cumulative feature requirements for tier.
func RequiredFeaturesForTier(tier CapabilityTier) ([]FeatureID, error) {
	maxRank, ok := capabilityTierRank(tier)
	if !ok {
		return nil, fmt.Errorf("%w: unknown capability tier %q", ErrUnsupportedFeature, tier)
	}
	required := make([]FeatureID, 0)
	for _, feature := range featureRegistry {
		rank, requiredAtTier := capabilityTierRank(feature.RequiredTier)
		if requiredAtTier && rank <= maxRank {
			required = append(required, feature.ID)
		}
	}
	return required, nil
}

// CapabilityTierBlockers returns the unsupported cumulative requirements for tier.
func CapabilityTierBlockers(tier CapabilityTier) ([]FeatureRecord, error) {
	required, err := RequiredFeaturesForTier(tier)
	if err != nil {
		return nil, err
	}
	return RequiredFeatureBlockers(required...), nil
}

// CheckCapabilityTier fails closed unless every cumulative tier requirement is supported.
func CheckCapabilityTier(tier CapabilityTier) error {
	blockers, err := CapabilityTierBlockers(tier)
	if err != nil {
		return err
	}
	if len(blockers) == 0 {
		return nil
	}
	return &FeatureReadinessError{Tier: tier, Blockers: blockers}
}

// CheckFeatureAvailable is the invocation-time gate for an optional TreeDB capability.
func CheckFeatureAvailable(id FeatureID) error {
	return CheckRequiredFeatures(id)
}

func capabilityTierRank(tier CapabilityTier) (int, bool) {
	for rank, candidate := range capabilityTierOrder {
		if tier == candidate {
			return rank, true
		}
	}
	return 0, false
}

// FeatureRegistry returns a copy of the ordered TreeDB readiness registry.
func FeatureRegistry() []FeatureRecord {
	out := make([]FeatureRecord, 0, len(featureRegistry))
	for _, feature := range featureRegistry {
		out = append(out, cloneFeatureRecord(feature))
	}
	return out
}

// FeatureForID returns the registry record for id.
func FeatureForID(id FeatureID) (FeatureRecord, bool) {
	for _, feature := range featureRegistry {
		if feature.ID == id {
			return cloneFeatureRecord(feature), true
		}
	}
	return FeatureRecord{}, false
}

// RequiredFeatureBlockers returns every requested feature that is not currently
// supported. Unknown IDs are reported as unsupported to fail closed.
func RequiredFeatureBlockers(required ...FeatureID) []FeatureRecord {
	blockers := make([]FeatureRecord, 0)
	for _, id := range required {
		feature, ok := FeatureForID(id)
		if !ok {
			blockers = append(blockers, FeatureRecord{
				ID:     id,
				Status: StatusUnsupported,
				Reason: "unknown TreeDB feature id",
				Evidence: []string{
					"fail-closed readiness check",
				},
			})
			continue
		}
		if feature.Status != StatusSupported {
			blockers = append(blockers, feature)
		}
	}
	return blockers
}

// CheckRequiredFeatures fails closed unless every requested feature is supported.
func CheckRequiredFeatures(required ...FeatureID) error {
	blockers := RequiredFeatureBlockers(required...)
	if len(blockers) == 0 {
		return nil
	}
	return &FeatureReadinessError{Blockers: blockers}
}

// FeatureReadinessError reports a required feature set that is not yet safe for
// the TreeDB integration lane.
type FeatureReadinessError struct {
	Tier     CapabilityTier
	Blockers []FeatureRecord
}

func (e *FeatureReadinessError) Error() string {
	if e == nil || len(e.Blockers) == 0 {
		return ErrUnsupportedFeature.Error()
	}
	parts := make([]string, 0, len(e.Blockers))
	for _, blocker := range e.Blockers {
		parts = append(parts, fmt.Sprintf("%s=%s (%s)", blocker.ID, blocker.Status, blocker.Reason))
	}
	if e.Tier != "" {
		return fmt.Sprintf("%s: capability tier %s is not ready: %s", ErrUnsupportedFeature, e.Tier,
			strings.Join(parts, "; "))
	}
	return fmt.Sprintf("%s: required feature set is not ready: %s", ErrUnsupportedFeature,
		strings.Join(parts, "; "))
}

// Unwrap lets callers use errors.Is(err, ErrUnsupportedFeature).
func (e *FeatureReadinessError) Unwrap() error {
	return ErrUnsupportedFeature
}

// UnsupportedFeatures returns a compatibility string view of all TreeDB
// features that are not supported yet.
func UnsupportedFeatures() []string {
	unsupported := make([]string, 0)
	for _, feature := range featureRegistry {
		if feature.Status == StatusSupported {
			continue
		}
		unsupported = append(unsupported, formatUnsupportedFeature(feature))
	}
	return unsupported
}

func cloneFeatureRecord(feature FeatureRecord) FeatureRecord {
	feature.Evidence = append([]string(nil), feature.Evidence...)
	return feature
}

func formatUnsupportedFeature(feature FeatureRecord) string {
	return fmt.Sprintf(
		"%s: %s (status: %s; evidence: %s)",
		feature.ID,
		feature.Reason,
		feature.Status,
		strings.Join(feature.Evidence, "; "),
	)
}
