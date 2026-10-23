package raftwal

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestCheckpointCertificateSnapshotSyncOrdersReadyDependencies(t *testing.T) {
	ds := Init(t.TempDir())
	defer ds.Close()
	ds.RequireCheckpointCertificate()
	require.NoError(t, ds.Save(&raftpb.HardState{Term: 1, Commit: 2}, []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, &raftpb.Snapshot{}))
	require.NoError(t, ds.AuthorizeCheckpoint(1))
	var stages []string
	ds.syncStageForTest = func(stage string) { stages = append(stages, stage) }
	// A newer Ready is saved but not synced when the apply goroutine certifies
	// an older prefix. Snapshot publication must seal Ready's dependencies too.
	require.NoError(t, ds.CreateSnapshot(1, &raftpb.ConfState{}, nil))
	require.Equal(t, []string{"wal", "directory", "meta"}, stages)
	require.Empty(t, ds.wal.dirty)
	ds.syncStageForTest = nil
}

func TestCheckpointCertificateConcurrentHardState(t *testing.T) {
	ds := Init(t.TempDir())
	defer ds.Close()
	ds.RequireCheckpointCertificate()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_, err := ds.HardState()
			if err != nil {
				t.Error(err)
			}
		}
	}()
	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, ds.Save(&raftpb.HardState{Term: 1, Commit: i}, []raftpb.Entry{{Index: i, Term: 1}}, &raftpb.Snapshot{}))
	}
	wg.Wait()
}

func TestCheckpointCertificateGuardsSnapshotRetention(t *testing.T) {
	ds := Init(t.TempDir())
	defer ds.Close()
	entries := []raftpb.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 2}, {Index: 3, Term: 2}}
	require.NoError(t, ds.Save(&raftpb.HardState{Term: 2, Commit: 3}, entries, &raftpb.Snapshot{}))
	require.NoError(t, ds.Sync())
	ds.RequireCheckpointCertificate()
	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.Error(t, ds.CreateSnapshot(2, cs, nil))
	first, err := ds.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Error(t, ds.AuthorizeCheckpoint(4))
	require.NoError(t, ds.AuthorizeCheckpoint(2))
	require.NoError(t, ds.CreateSnapshot(2, cs, nil))
	require.Error(t, ds.CreateSnapshot(3, cs, nil))
	require.Error(t, ds.AuthorizeCheckpoint(1))
	first, err = ds.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), first)
	// A leader's snapshot cannot bypass the missing TreeDB install protocol.
	require.Error(t, ds.Save(&raftpb.HardState{}, nil, &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 2}}))
}

func TestCheckpointCertificateCrashBeforeSnapshotKeepsReplayHistory(t *testing.T) {
	dir := t.TempDir()
	ds := Init(dir)
	require.NoError(t, ds.Save(&raftpb.HardState{Term: 1, Commit: 2}, []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, &raftpb.Snapshot{}))
	ds.RequireCheckpointCertificate()
	require.NoError(t, ds.AuthorizeCheckpoint(1))
	require.NoError(t, ds.Close())
	// Authorization is not a persisted snapshot: restart retains the full log,
	// and requires the materialized store to revalidate its certificate.
	reopened := Init(dir)
	defer reopened.Close()
	reopened.RequireCheckpointCertificate()
	first, err := reopened.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Error(t, reopened.CreateSnapshot(1, &raftpb.ConfState{}, nil))
	require.NoError(t, reopened.AuthorizeCheckpoint(1))
	require.NoError(t, reopened.CreateSnapshot(1, &raftpb.ConfState{}, nil))
}

func TestCheckpointCertificateDirtySyncTracksRotation(t *testing.T) {
	ds := Init(t.TempDir())
	defer ds.Close()
	ds.RequireCheckpointCertificate()
	require.NoError(t, ds.Sync())
	require.Empty(t, ds.wal.dirty)
	entries := make([]raftpb.Entry, maxNumEntries+1)
	for i := range entries {
		entries[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1, Data: []byte("command")}
	}
	require.NoError(t, ds.Save(&raftpb.HardState{Term: 1, Commit: uint64(len(entries))}, entries, &raftpb.Snapshot{}))
	require.Len(t, ds.wal.dirty, 2, "rotation retains both unsynced file dependencies")
	require.True(t, ds.wal.namespaceDirty)
	require.NoError(t, ds.Sync())
	require.Empty(t, ds.wal.dirty)
	require.False(t, ds.wal.namespaceDirty)
	require.NoError(t, ds.Save(&raftpb.HardState{Term: 1, Commit: uint64(len(entries)) + 1}, []raftpb.Entry{{Index: uint64(len(entries)) + 1, Term: 1, Data: []byte("next")}}, &raftpb.Snapshot{}))
	require.Len(t, ds.wal.dirty, 1, "new Ready does not rescan or resync retained old files")
	require.NoError(t, ds.Sync())
	require.Empty(t, ds.wal.dirty)
}
