/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftPendingOverlayExactRootAndStrictAccounting(t *testing.T) {
	Oracle().ResetTxns()
	t.Cleanup(func() { Oracle().ResetTxns() })
	s := openRaftTransferStore(t, "pending-accounting")
	a, err := MarshalRaftPendingTxn(pendingTestTxn(t, 50))
	require.NoError(t, err)
	require.NoError(t, s.ReplaceRaftPendingTxn(50, a))
	manifest, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.Count)
	charge, err := RaftPendingTxnCharge(a)
	require.NoError(t, err)
	require.Equal(t, charge, manifest.Charge)
	require.NoError(t, s.CheckpointRaft(context.Background(), []byte("pending-root")))
	require.NoError(t, s.RestoreRaftPendingOverlay(manifest))
	require.NotNil(t, Oracle().GetTxn(50))
	// A bad metadata reference cannot replace the current Oracle transaction set.
	bad := manifest
	bad.Charge++
	require.ErrorContains(t, s.RestoreRaftPendingOverlay(bad), "manifest mismatch")
	require.NotNil(t, Oracle().GetTxn(50))
	// The record checksum and full accounting are checked at recovery, while
	// checkpointing itself needs only the incrementally maintained manifest.
	corrupt := append([]byte(nil), a...)
	corrupt[len(corrupt)-1] ^= 1
	require.NoError(t, s.external.Apply(func() error { return s.db.Set(raftPendingKey(50), corrupt) }))
	require.Error(t, s.RestoreRaftPendingOverlay(manifest))
	require.NotNil(t, Oracle().GetTxn(50), "failed full validation is atomic for Oracle")
	require.NoError(t, s.external.Apply(func() error { return s.db.Delete(raftPendingKey(50)) }))
	require.ErrorContains(t, s.RestoreRaftPendingOverlay(manifest), "do not match")
}

func TestRaftPendingOverlayReplacementAndCapacityPreflight(t *testing.T) {
	s := openRaftTransferStore(t, "pending-capacity")
	data, err := MarshalRaftPendingTxn(pendingTestTxn(t, 50))
	require.NoError(t, err)
	require.NoError(t, s.ReplaceRaftPendingTxn(50, data))
	before, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.NoError(t, s.ReplaceRaftPendingTxn(50, data))
	after, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, before, after, "replacement is not double charged")
	require.Error(t, s.CheckRaftPendingReplacement(51, data), "key/start mismatch is not capacity")
	// Fill the real bounded record namespace, rather than injecting an inflated
	// in-memory accounting value. Empty canonical transactions still own rows.
	for ts := uint64(100); ts < 100+RaftPendingMaxCount-1; ts++ {
		row, err := MarshalRaftPendingTxn(NewTxn(ts))
		require.NoError(t, err)
		require.NoError(t, s.ReplaceRaftPendingTxn(ts, row))
	}
	full, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(RaftPendingMaxCount), full.Count)
	allocs := testing.AllocsPerRun(100, func() {
		_, manifestErr := s.RaftPendingManifest()
		if manifestErr != nil {
			panic(manifestErr)
		}
	})
	require.Zero(t, allocs, "checkpoint overlay reference remains allocation-free at the full record bound")
	row, err := MarshalRaftPendingTxn(NewTxn(10000))
	require.NoError(t, err)
	require.ErrorIs(t, s.CheckRaftPendingReplacement(10000, row), ErrRaftPendingCapacity)
	require.ErrorIs(t, s.ReplaceRaftPendingTxn(10000, row), ErrRaftPendingCapacity)
	unchanged, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, full, unchanged)
	raw, err := s.db.Get(raftPendingKey(10000))
	require.NoError(t, err)
	require.Nil(t, raw, "capacity refusal publishes no row")
	// Existing decisions always free space, even at the exact capacity limit.
	require.NoError(t, s.RemoveRaftPendingTxn(50))
	require.NoError(t, s.ReplaceRaftPendingTxn(10000, row))
	final, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, full.Count, final.Count)
	require.NoError(t, s.RestoreRaftPendingOverlay(final))
	Oracle().ResetTxns()
}
