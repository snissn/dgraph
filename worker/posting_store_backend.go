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
	normalized, err := NormalizePostingStoreBackend(backend)
	if err != nil {
		return err
	}

	switch normalized {
	case PostingStoreBackendBadger:
		return nil
	case PostingStoreBackendTreeDB:
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
	return fmt.Sprintf("posting-store backend treedb: experimental, disabled until %d compatibility blockers are resolved",
		len(treedb.PostingBackendBlockers()))
}
