/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/dgraph/v25/worker/treedb"
	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	// PostingStoreDefaults controls the experimental Alpha posting-store selector.
	PostingStoreDefaults = `backend=badger; tier=; durability=durable; events=;`

	// PostingStoreBackendBadger is the production/default posting-store backend.
	PostingStoreBackendBadger = "badger"
	// PostingStoreBackendTreeDB is the experimental TreeDB selector value. It is
	// restricted to the benchmark-minimal tier and fails closed unless
	// treedb.CheckPostingBackendReady succeeds.
	PostingStoreBackendTreeDB = "treedb"

	// PostingStoreTierProduction is the only tier accepted by the Badger backend.
	PostingStoreTierProduction = string(treedb.TierProduction)
	// PostingStoreTierBenchmarkMinimal is the only tier accepted by the restricted TreeDB Alpha.
	PostingStoreTierBenchmarkMinimal = string(treedb.TierBenchmarkMinimal)
)

// PostingStoreSuperFlag is the parsed Alpha posting-store selector. Blank tier
// and events values are resolved by Options.validate after the backend is known.
type PostingStoreSuperFlag struct {
	Backend          string
	Tier             string
	Durability       string
	Events           bool
	EventsConfigured bool
}

// ParsePostingStoreSuperFlag parses the CLI selector while preserving whether
// events=false was explicit. SuperFlag.GetBool alone cannot distinguish false
// from an omitted backend-specific default.
func ParsePostingStoreSuperFlag(value string) PostingStoreSuperFlag {
	parsed := z.NewSuperFlag(value).MergeAndCheckDefault(PostingStoreDefaults)
	return PostingStoreSuperFlag{
		Backend:          parsed.GetString("backend"),
		Tier:             parsed.GetString("tier"),
		Durability:       parsed.GetString("durability"),
		Events:           parsed.GetBool("events"),
		EventsConfigured: parsed.Has("events"),
	}
}

const (
	PostingStoreDurabilityDurable = "durable"
	PostingStoreDurabilityRelaxed = "relaxed"
)

// ValidatePostingStoreSelection enforces the deliberately narrow TreeDB Alpha
// runtime contract before any posting-store directory is opened.
func ValidatePostingStoreSelection(backend, tier, durability string, encrypted, events bool) error {
	normalized, err := NormalizePostingStoreBackend(backend)
	if err != nil {
		return err
	}
	tier = strings.ToLower(strings.TrimSpace(tier))
	durability = normalizePostingStoreDurability(durability)
	if normalized == PostingStoreBackendBadger {
		if tier != "" && tier != PostingStoreTierProduction {
			return fmt.Errorf("posting-store backend %q requires tier %q, got %q",
				PostingStoreBackendBadger, PostingStoreTierProduction, tier)
		}
		if durability != "" && durability != PostingStoreDurabilityDurable {
			return fmt.Errorf("posting-store backend %q does not expose durability %q through the experimental selector",
				PostingStoreBackendBadger, durability)
		}
		return nil
	}
	if tier != PostingStoreTierBenchmarkMinimal {
		return fmt.Errorf("posting-store backend %q is restricted to tier %q, got %q",
			PostingStoreBackendTreeDB, PostingStoreTierBenchmarkMinimal, tier)
	}
	if durability != PostingStoreDurabilityDurable && durability != PostingStoreDurabilityRelaxed {
		return fmt.Errorf("posting-store backend %q durability must be %q or %q, got %q",
			PostingStoreBackendTreeDB, PostingStoreDurabilityDurable,
			PostingStoreDurabilityRelaxed, durability)
	}
	if !events {
		return fmt.Errorf("posting-store backend %q requires events=true so internal ACL and GraphQL schema watchers cannot become stale",
			PostingStoreBackendTreeDB)
	}
	return CheckPostingStoreBackendReadyForConfig(normalized, encrypted)
}

func normalizePostingStoreDurability(durability string) string {
	return strings.ToLower(strings.TrimSpace(durability))
}

// NormalizePostingStoreBackend validates and normalizes an Alpha posting-store
// backend selector value.
func NormalizePostingStoreBackend(backend string) (string, error) {
	switch normalized := strings.ToLower(strings.TrimSpace(backend)); normalized {
	case "", PostingStoreBackendBadger:
		return PostingStoreBackendBadger, nil
	case PostingStoreBackendTreeDB:
		return PostingStoreBackendTreeDB, nil
	default:
		return "", fmt.Errorf("posting-store backend %q is not supported: expected %q or %q",
			backend, PostingStoreBackendBadger, PostingStoreBackendTreeDB)
	}
}

// CheckPostingStoreBackendReady returns nil for the default Badger backend and
// fails closed for TreeDB until every required Dgraph posting-store capability
// is supported.
func CheckPostingStoreBackendReady(backend string) error {
	return CheckPostingStoreBackendReadyForConfig(backend, false)
}

// CheckPostingStoreBackendReadyForConfig applies startup requirements that
// are outside the benchmark-minimal capability tier before selecting TreeDB.
// Badger remains valid for configurations, such as encryption, that TreeDB
// explicitly does not support yet.
func CheckPostingStoreBackendReadyForConfig(backend string, requireEncryption bool) error {
	normalized, err := NormalizePostingStoreBackend(backend)
	if err != nil {
		return err
	}

	switch normalized {
	case PostingStoreBackendBadger:
		return nil
	case PostingStoreBackendTreeDB:
		if requireEncryption {
			if err := treedb.CheckFeatureAvailable(treedb.FeatureEncryptionKeyRegistry); err != nil {
				return fmt.Errorf("posting-store backend %q cannot satisfy the configured encryption requirement: %w",
					PostingStoreBackendTreeDB, err)
			}
		}
		if err := treedb.CheckPostingBackendReady(); err != nil {
			return fmt.Errorf("posting-store backend %q is experimental and not ready: %w",
				PostingStoreBackendTreeDB, err)
		}
		return nil
	default:
		return fmt.Errorf("posting-store backend %q is not supported", backend)
	}
}

// PostingStoreBackendStatus returns a compact operator-facing status string.
func PostingStoreBackendStatus(backend string) string {
	normalized, err := NormalizePostingStoreBackend(backend)
	if err != nil {
		return err.Error()
	}
	if normalized == PostingStoreBackendBadger {
		return "posting-store backend badger: default production backend"
	}
	blockers, err := treedb.CapabilityTierBlockers(treedb.TierBenchmarkMinimal)
	if err != nil {
		return fmt.Sprintf("posting-store backend treedb: invalid capability registry: %v", err)
	}
	if len(blockers) == 0 {
		return fmt.Sprintf("posting-store backend treedb: experimental, tier=%s", treedb.TierBenchmarkMinimal)
	}
	return fmt.Sprintf("posting-store backend treedb: experimental, %s disabled until %d capability blockers are resolved",
		treedb.TierBenchmarkMinimal, len(blockers))
}
