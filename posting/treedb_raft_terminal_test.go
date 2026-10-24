/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"errors"
	"testing"

	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestRaftTerminalNativeRootReopenAndRetirementFailure(t *testing.T) {
	Oracle().ResetTxns()
	t.Cleanup(func() { Oracle().ResetTxns() })
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	source, err := OpenRaftTreeDBStore(opts, "terminal-source")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	pending, err := MarshalRaftPendingTxn(pendingTestTxn(t, 50))
	require.NoError(t, err)
	require.NoError(t, source.ReplaceRaftPendingTxn(50, pending))
	require.NoError(t, source.ResolveRaftPendingTxns([]RaftTxnDisposition{{50, 51}, {60, 0}, {50, 51}}))
	pending, err = MarshalRaftPendingTxn(pendingTestTxn(t, 70))
	require.NoError(t, err)
	require.NoError(t, source.ReplaceRaftPendingTxn(70, pending))
	manifest, err := source.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(2), manifest.TerminalCount)
	allocs := testing.AllocsPerRun(100, func() {
		_, err := source.RaftPendingManifest()
		if err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs, "fixed pending/terminal reference does not scan or allocate with retained history")
	require.Equal(t, uint64(1), manifest.Count)
	require.ErrorIs(t, source.ResolveRaftPendingTxns([]RaftTxnDisposition{{70, 71}, {50, 0}}), ErrRaftTerminalConflict)
	unchanged, err := source.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, manifest, unchanged)
	require.NoError(t, source.CheckpointRaft(context.Background(), []byte("terminal-cut")))
	cert, ok := source.RaftCheckpointCertificate()
	require.True(t, ok)
	var archive bytes.Buffer
	require.NoError(t, source.ExportRaftCheckpoint(context.Background(), &archive, cert.Generation, cert.Digest))
	recvOpts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	recvOpts.MemtableMode = "cow_ordered"
	receiver, err := OpenRaftTreeDBStore(recvOpts, "terminal-receiver")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	_, err = receiver.InstallRaftCheckpoint(context.Background(), &archive, func(td.ExternalRecoveryCheckpoint) ([]byte, error) { return []byte("accepted-terminal-cut"), nil })
	require.NoError(t, err)
	require.NoError(t, receiver.RestoreRaftPendingOverlay(manifest))
	require.NotNil(t, Oracle().GetTxn(70))
	require.NoError(t, receiver.Close())
	receiver, err = OpenRaftTreeDBStore(recvOpts, "terminal-receiver")
	require.NoError(t, err)
	require.NoError(t, receiver.RestoreRaftPendingOverlay(manifest))
	for _, d := range []RaftTxnDisposition{{50, 51}, {60, 0}} {
		commit, known, err := receiver.RaftTxnDisposition(d.StartTs)
		require.NoError(t, err)
		require.True(t, known)
		require.Equal(t, d.CommitTs, commit)
	}
	// Multiple bounded deletion batches really execute before a corrupt later
	// record fails. Admission seals and reopen selects the old certified root.
	decisions := make([]RaftTxnDisposition, 600)
	for i := range decisions {
		decisions[i] = RaftTxnDisposition{uint64(i) + 100, 0}
	}
	require.NoError(t, receiver.ResolveRaftPendingTxns(decisions))
	full, err := receiver.RaftPendingManifest()
	require.NoError(t, err)
	require.NoError(t, receiver.CheckpointRaft(context.Background(), []byte("before-retirement")))
	require.NoError(t, receiver.external.Apply(func() error { return receiver.db.Set(raftTerminalKey(699), []byte("corrupt")) }))
	require.ErrorContains(t, receiver.RetireRaftTxnDispositions(1000), "malformed terminal")
	require.True(t, receiver.IsClosed())
	require.Less(t, receiver.raftPending.TerminalCount, full.TerminalCount, "earlier batches executed")
	require.NoError(t, receiver.Close())
	receiver, err = OpenRaftTreeDBStore(recvOpts, "terminal-receiver")
	require.NoError(t, err)
	require.NoError(t, receiver.RestoreRaftPendingOverlay(full))
	require.NoError(t, receiver.RetireRaftTxnDispositions(600))
	retired, err := receiver.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(99), retired.TerminalCount)
	require.NoError(t, receiver.RestoreRaftPendingOverlay(retired))
}

func TestRaftTerminalDecisionsProgressAtPendingCapacity(t *testing.T) {
	s := openRaftTransferStore(t, "terminal-full-pending")
	decisions := make([]RaftTxnDisposition, RaftPendingMaxCount)
	for i := range decisions {
		ts := uint64(i) + 1
		row, err := MarshalRaftPendingTxn(NewTxn(ts))
		require.NoError(t, err)
		require.NoError(t, s.ReplaceRaftPendingTxn(ts, row))
		decisions[i] = RaftTxnDisposition{ts, 0}
	}
	require.NoError(t, s.ResolveRaftPendingTxns(decisions))
	manifest, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Zero(t, manifest.Count)
	require.Zero(t, manifest.Charge)
	require.Equal(t, uint64(RaftPendingMaxCount), manifest.TerminalCount)
	require.NoError(t, s.ResolveRaftPendingTxns(decisions))
	same, err := s.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, manifest, same)
}

func TestRaftTerminalPartialPublicationSealsAndRestoresPendingRoot(t *testing.T) {
	Oracle().ResetTxns()
	t.Cleanup(func() { Oracle().ResetTxns() })
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	store, err := OpenRaftTreeDBStore(opts, "terminal-partial")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	row, err := MarshalRaftPendingTxn(pendingTestTxn(t, 50))
	require.NoError(t, err)
	require.NoError(t, store.ReplaceRaftPendingTxn(50, row))
	before, err := store.RaftPendingManifest()
	require.NoError(t, err)
	require.NoError(t, store.CheckpointRaft(context.Background(), []byte("before-decisions")))
	decisions := make([]RaftTxnDisposition, 600)
	for i := range decisions {
		decisions[i] = RaftTxnDisposition{uint64(i) + 1, 0}
	}
	failure := errors.New("injected second terminal publication failure")
	require.ErrorIs(t, store.resolveRaftPendingTxns(decisions, func(offset int) error {
		if offset == 256 {
			return failure
		}
		return nil
	}), failure)
	require.True(t, store.IsClosed())
	require.Equal(t, uint64(256), store.raftPending.TerminalCount)
	require.Error(t, store.CheckpointRaft(context.Background(), []byte("must not certify")))
	require.NoError(t, store.Close())
	store, err = OpenRaftTreeDBStore(opts, "terminal-partial")
	require.NoError(t, err)
	require.NoError(t, store.RestoreRaftPendingOverlay(before))
	require.NotNil(t, Oracle().GetTxn(50))
	_, known, err := store.RaftTxnDisposition(50)
	require.NoError(t, err)
	require.False(t, known)
	require.NoError(t, store.ResolveRaftPendingTxns(decisions))
	after, err := store.RaftPendingManifest()
	require.NoError(t, err)
	require.Equal(t, uint64(600), after.TerminalCount)
	require.Zero(t, after.Count)
	require.ErrorIs(t, store.ReplaceRaftPendingTxn(50, row), ErrRaftTxnResolved)
}
