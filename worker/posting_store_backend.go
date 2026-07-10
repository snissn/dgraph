/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/dgraph/v25/worker/treedb"
)

const (
	// PostingStoreDefaults controls the experimental Alpha posting-store selector.
	PostingStoreDefaults = `backend=badger;`

	// PostingStoreBackendBadger is the production/default posting-store backend.
	PostingStoreBackendBadger = "badger"
	// PostingStoreBackendTreeDB is the experimental TreeDB selector value. It is
	// fail-closed until treedb.CheckPostingBackendReady succeeds and a runtime
	// opener is implemented.
	PostingStoreBackendTreeDB = "treedb"
)

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
		return fmt.Errorf("posting-store backend %q passed readiness checks, but the runtime TreeDB opener is not implemented",
			PostingStoreBackendTreeDB)
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
	return fmt.Sprintf("posting-store backend treedb: experimental, %s disabled until %d capability blockers are resolved",
		treedb.TierBenchmarkMinimal, len(blockers))
}
