/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
package posting

import (
	"bytes"
	"strings"
)

// DeltaSnapshot owns a bounded immutable copy of already-extracted transaction
// deltas. Live transaction readers may keep populating the original cache. The
// snapshot never calls Update or reads that live cache after admission.
type DeltaSnapshot struct{ txn Txn }

// SnapshotDelta checks and copies under one cache read lock. Its payload/key/map
// budgets cover new snapshot custody, not the concurrently growable live cache.
func (txn *Txn) SnapshotDelta(maxKeys, maxBytes, maxMapEntries int) (*DeltaSnapshot, int, bool) {
	if txn == nil || txn.cache == nil || txn.ShouldAbort() {
		return nil, 0, false
	}
	c := txn.cache
	c.RLock()
	defer c.RUnlock()
	if len(c.plists) != 0 || len(c.deltas) == 0 || len(c.deltas) > maxKeys || len(c.maxVersions) > maxKeys || len(c.deltas)+len(c.maxVersions) > maxMapEntries {
		return nil, 0, false
	}
	size := 0
	for key, value := range c.deltas {
		if len(key) > maxBytes-size {
			return nil, 0, false
		}
		size += len(key)
		if len(value) > maxBytes-size {
			return nil, 0, false
		}
		size += len(value)
	}
	for key := range c.maxVersions {
		if len(key)+8 > maxBytes-size {
			return nil, 0, false
		}
		size += len(key) + 8
	}
	owned := &LocalCache{startTs: c.startTs, deltas: make(map[string][]byte, len(c.deltas)), maxVersions: make(map[string]uint64, len(c.maxVersions))}
	for key, value := range c.deltas {
		owned.deltas[strings.Clone(key)] = bytes.Clone(value)
	}
	for key, version := range c.maxVersions {
		owned.maxVersions[strings.Clone(key)] = version
	}
	return &DeltaSnapshot{txn: Txn{StartTs: txn.StartTs, cache: owned}}, size, true
}

// Disjoint tests only immutable snapshot keys; no mutable cache is revisited.
func (s *DeltaSnapshot) Disjoint(other *DeltaSnapshot) bool {
	for key := range s.txn.cache.deltas {
		if _, ok := other.txn.cache.deltas[key]; ok {
			return false
		}
	}
	return true
}

// CommitToDisk reuses original chunk/skipping/early-rollup semantics on owned
// bytes. The caller must submit once and retain the snapshot through callbacks.
func (s *DeltaSnapshot) CommitToDisk(writer *TxnWriter, commitTs uint64) error {
	return s.txn.CommitToDisk(writer, commitTs)
}

// UpdateCachedKeys publishes the exact submitted snapshot, after its callbacks.
func (s *DeltaSnapshot) UpdateCachedKeys(commitTs uint64) { s.txn.UpdateCachedKeys(commitTs) }

// MapEntries counts copied payload and version-map entries for a whole-window budget.
func (s *DeltaSnapshot) MapEntries() int {
	return len(s.txn.cache.deltas) + len(s.txn.cache.maxVersions)
}
