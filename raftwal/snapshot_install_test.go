/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func installIntentFixture(t *testing.T, dir string) (*DiskStorage, AcceptedSnapshotInstall) {
	t.Helper()
	return installIntentFixtureWithFormat(t, dir, false)
}
func installIntentFixtureWithFormat(t *testing.T, dir string, ready bool) (*DiskStorage, AcceptedSnapshotInstall) {
	t.Helper()
	var w *DiskStorage
	if ready {
		var err error
		w, err = InitCheckpointEncrypted(dir, nil)
		require.NoError(t, err)
	} else {
		w = Init(dir)
	}
	w.SetUint(RaftId, 1)
	w.SetUint(GroupId, 1)
	w.RequireCheckpointCertificate()
	var entries []raftpb.Entry
	for index := uint64(1); index <= 12; index++ {
		entries = append(entries, raftpb.Entry{Index: index, Term: 4, Data: []byte("old-prefix-or-divergent-tail")})
	}
	require.NoError(t, w.Save(&raftpb.HardState{Term: 4, Vote: 1, Commit: 1}, entries, &raftpb.Snapshot{}))
	require.NoError(t, w.Sync())
	archive := []byte("complete archive authority is opaque to raftwal")
	name := "snapshot-install-fixture.tar"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), archive, 0600))
	hash := sha256.Sum256(archive)
	intent := AcceptedSnapshotInstall{
		Version: 1, ReceiverIdentity: "receiver", Group: 1, RaftID: 1,
		Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 3,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}, Data: []byte("accepted-peer-snapshot")},
		HardState:   raftpb.HardState{Term: 4, Vote: 1, Commit: 10},
		ArchiveName: name, ArchiveSHA256: hex.EncodeToString(hash[:]), ArchiveBytes: int64(len(archive)),
		SourceCertificate: []byte("source-certified-root"), ReceiverMetadata: []byte("receiver-accepted-cut"),
	}
	return w, intent
}

func TestAcceptedSnapshotInstallCrashHelper(t *testing.T) {
	stage := os.Getenv("DGRAPH_SNAPSHOT_INSTALL_CUT")
	if stage == "" {
		return
	}
	w, intent := installIntentFixture(t, os.Getenv("DGRAPH_SNAPSHOT_INSTALL_DIR"))
	w.installStageForTest = func(at string) {
		if at == stage {
			os.Exit(0) // no deferred Close: abrupt process cut at the sync boundary
		}
	}
	require.NoError(t, w.AcceptSnapshotInstall(intent))
	require.NoError(t, w.CompleteSnapshotInstall(intent.ReceiverMetadata))
	t.Fatalf("did not reach selected crash cut %q", stage)
}

func TestAcceptedSnapshotInstallCrashCuts(t *testing.T) {
	for _, stage := range []string{"accepted", "metadata", "wal"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestAcceptedSnapshotInstallCrashHelper$", "-test.count=1")
			command.Env = append(os.Environ(), "DGRAPH_SNAPSHOT_INSTALL_CUT="+stage, "DGRAPH_SNAPSHOT_INSTALL_DIR="+dir)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
			intent, err := ReadAcceptedSnapshotInstall(dir)
			require.NoError(t, err)
			require.NotNil(t, intent)
			for restart := 0; restart < 2; restart++ {
				w, err := InitEncrypted(dir, nil)
				require.NoError(t, err, "pending intent must defer normal destructive alignment")
				w.RequireCheckpointCertificate()
				require.Error(t, w.CompleteSnapshotInstall([]byte("unrelated-root")))
				require.NoError(t, w.CompleteSnapshotInstall(intent.ReceiverMetadata))
				hs, err := w.HardState()
				require.NoError(t, err)
				require.Equal(t, raftpb.HardState{Term: 4, Vote: 1, Commit: 10}, hs)
				last, err := w.LastIndex()
				require.NoError(t, err)
				require.Equal(t, uint64(10), last, "old divergent suffix must not survive install")
				require.FileExists(t, filepath.Join(dir, intent.ArchiveName), "archive survives repeated startup/completion")
				require.NoError(t, w.Close())
			}
			w := Init(dir)
			w.RequireCheckpointCertificate()
			require.NoError(t, w.CompleteSnapshotInstall(intent.ReceiverMetadata))
			require.NoError(t, w.FinishSnapshotInstall(intent.Digest))
			require.NoFileExists(t, filepath.Join(dir, intent.ArchiveName))
			require.NoError(t, w.Close())
			w, err = InitEncrypted(dir, nil)
			require.NoError(t, err, "ordinary startup resumes after durable intent cleanup")
			require.NoError(t, w.Close())
		})
	}
}

func TestAcceptedSnapshotInstallRejectsCorruptionAndIdentity(t *testing.T) {
	for _, corruption := range []string{"archive", "missing-archive", "intent", "identity"} {
		t.Run(corruption, func(t *testing.T) {
			dir := t.TempDir()
			w, intent := installIntentFixture(t, dir)
			defer w.Close()
			require.NoError(t, w.AcceptSnapshotInstall(intent))
			switch corruption {
			case "archive":
				require.NoError(t, os.WriteFile(filepath.Join(dir, intent.ArchiveName), []byte("corrupt"), 0600))
			case "missing-archive":
				require.NoError(t, os.Remove(filepath.Join(dir, intent.ArchiveName)))
			case "intent":
				require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotInstallFile), []byte("corrupt"), 0600))
			case "identity":
				w.SetUint(RaftId, 2)
				require.NoError(t, w.Sync())
			}
			_, err := InitEncrypted(dir, nil)
			require.Error(t, err, "must refuse before pruning or rewriting log state")
			last, err := w.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(12), last, "rejection preserves prior history")
		})
	}
}
