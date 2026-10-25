/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func readyStore(t *testing.T, dir string) *DiskStorage {
	t.Helper()
	s, err := InitCheckpointEncrypted(dir, nil)
	require.NoError(t, err)
	require.NotNil(t, s.ready)
	return s
}
func saveReady(t *testing.T, s *DiskStorage, index uint64, data []byte) {
	t.Helper()
	require.NoError(t, s.Save(&raftpb.HardState{Term: 1, Vote: 1, Commit: index}, []raftpb.Entry{{Index: index, Term: 1, Data: data}}, nil))
}
func TestReadyCutSaveSyncReopenOneBarrier(t *testing.T) {
	dir := t.TempDir()
	s := readyStore(t, dir)
	s.SetUint(RaftId, 11)
	s.SetUint(GroupId, 1)
	s.RequireCheckpointCertificate()
	calls := 0
	s.ready.testSync = func() error { calls++; return s.ready.f.Sync() }
	input := []byte("owned payload")
	saveReady(t, s, 1, input)
	input[0] = 'X'
	require.NoError(t, s.Sync())
	require.Equal(t, 1, calls, "ordinary Ready is one actual file barrier")
	require.NoError(t, s.Close())
	require.Equal(t, 1, calls)
	_, err := os.Stat(filepath.Join(dir, metaName))
	require.True(t, os.IsNotExist(err), "no competing legacy metadata")
	reopened, err := InitEncrypted(dir, nil)
	require.NoError(t, err)
	defer reopened.Close()
	require.NotNil(t, reopened.ready)
	hs, err := reopened.HardState()
	require.NoError(t, err)
	require.Equal(t, raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, hs)
	es, err := reopened.Entries(1, 2, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("owned payload"), es[0].Data)
	require.Equal(t, uint64(11), reopened.Uint(RaftId))
	id, group, err := readRaftIdentity(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(11), id)
	require.Equal(t, uint64(1), group)
}
func TestReadyCutActualFormatAndLegacyCharacterization(t *testing.T) {
	dir := t.TempDir()
	legacy := Init(dir)
	legacy.RequireCheckpointCertificate()
	var stages []string
	legacy.syncStageForTest = func(s string) { stages = append(stages, s) }
	saveReady(t, legacy, 1, []byte("legacy"))
	require.NoError(t, legacy.Sync())
	require.Equal(t, []string{"wal", "directory", "meta"}, stages)
	require.NoError(t, legacy.Close())
	same, err := InitCheckpointEncrypted(dir, nil)
	require.NoError(t, err)
	require.Nil(t, same.ready)
	require.NoError(t, same.Close())
	fresh := t.TempDir()
	s := readyStore(t, fresh)
	require.NoError(t, s.Close())
	require.NoError(t, os.WriteFile(filepath.Join(fresh, metaName), []byte("competing"), 0600))
	_, err = InitEncrypted(fresh, nil)
	require.Error(t, err)
	orphan := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(orphan, "00000000000000000000000000000000.rdc"), nil, 0600))
	_, err = InitEncrypted(orphan, nil)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(orphan, metaName))
	require.True(t, os.IsNotExist(err))
}
func TestReadyCutPartialFrameAtomicityAndCorruption(t *testing.T) {
	for _, cut := range []int{1, readyHeaderSize, readyHeaderSize + readyStateSize + 1} {
		t.Run(string(rune('a'+cut%26)), func(t *testing.T) {
			dir := t.TempDir()
			s := readyStore(t, dir)
			saveReady(t, s, 1, []byte("A"))
			require.NoError(t, s.Sync())
			baseline := s.ready.size
			fault := errors.New("partial cut")
			left := cut
			s.ready.testWrite = func(p []byte, off int64) (int, error) {
				if left <= 0 {
					return 0, fault
				}
				n := min(left, len(p))
				left -= n
				written, err := s.ready.f.WriteAt(p[:n], off)
				if err == nil && n < len(p) {
					err = fault
				}
				return written, err
			}
			err := s.Save(&raftpb.HardState{Term: 2, Vote: 2, Commit: 2}, []raftpb.Entry{{Index: 2, Term: 2, Data: bytes.Repeat([]byte("B"), 512)}}, nil)
			require.True(t, RecoveryRequired(err))
			require.ErrorIs(t, s.Sync(), fault)
			require.ErrorIs(t, s.Close(), fault)
			reopened := readyStore(t, dir)
			defer reopened.Close()
			files, err := filepath.Glob(filepath.Join(dir, "*.rdc"))
			require.NoError(t, err)
			require.Len(t, files, 1, "validated selected root retires abandoned generations")
			hs, err := reopened.HardState()
			require.NoError(t, err)
			require.Equal(t, uint64(1), hs.Commit)
			require.Equal(t, uint64(1), hs.Term)
			last, err := reopened.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(1), last)
			require.Equal(t, baseline, reopened.ready.size)
		})
	}
	dir := t.TempDir()
	s := readyStore(t, dir)
	saveReady(t, s, 1, []byte("A"))
	require.NoError(t, s.Sync())
	name := s.ready.name
	require.NoError(t, s.Close())
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{255}, readyHeaderSize+1)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = InitCheckpointEncrypted(dir, nil)
	require.ErrorContains(t, err, "corrupt Ready footer", "integrity precedes invalid state interpretation")
}
func TestReadyCutSyncFailureLatchesAndNoEarlyCompletion(t *testing.T) {
	s := readyStore(t, t.TempDir())
	saveReady(t, s, 1, []byte("A"))
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var after atomic.Bool
	fault := errors.New("writeback EIO")
	s.ready.testSync = func() error { close(entered); <-release; return fault }
	go func() {
		err := s.Sync()
		if err == nil {
			after.Store(true)
		}
		done <- err
	}()
	<-entered
	require.False(t, after.Load())
	close(release)
	err := <-done
	require.ErrorIs(t, err, fault)
	require.True(t, RecoveryRequired(err))
	s.ready.testSync = func() error { t.Error("poison must prevent a later same-fd success"); return nil }
	require.ErrorIs(t, s.Sync(), fault)
	require.ErrorIs(t, s.Save(nil, nil, nil), fault)
	require.ErrorIs(t, s.Close(), fault)
	require.False(t, after.Load())
}
func TestReadyCutValidationElectionConflictAndLarge(t *testing.T) {
	s := readyStore(t, t.TempDir())
	defer s.Close()
	saveReady(t, s, 1, []byte("A"))
	require.NoError(t, s.Sync())
	before := s.ready.size
	for _, v := range []struct {
		h raftpb.HardState
		e []raftpb.Entry
	}{{raftpb.HardState{Term: 1, Commit: 2}, nil}, {raftpb.HardState{Term: 1, Vote: 2, Commit: 1}, nil}, {raftpb.HardState{Term: 1}, nil}, {raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, []raftpb.Entry{{Index: 1, Term: 2, Data: []byte("bad")}}}} {
		err := s.Save(&v.h, v.e, nil)
		require.True(t, PermanentStorageError(err))
		require.False(t, RecoveryRequired(err))
		require.Equal(t, before, s.ready.size)
	}
	require.NoError(t, s.Save(&raftpb.HardState{Term: 2, Vote: 2, Commit: 1}, nil, nil))
	require.NoError(t, s.Sync())
	big := bytes.Repeat([]byte("large"), 1<<20)
	require.NoError(t, s.Save(nil, []raftpb.Entry{{Index: 2, Term: 2, Data: []byte("old")}, {Index: 3, Term: 2, Data: []byte("suffix")}}, nil))
	require.NoError(t, s.Save(&raftpb.HardState{Term: 3, Vote: 3, Commit: 2}, []raftpb.Entry{{Index: 2, Term: 3, Data: big}}, nil))
	require.NoError(t, s.Sync())
	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
	es, err := s.Entries(2, 3, 1)
	require.NoError(t, err)
	require.Equal(t, big, es[0].Data)
	es[0].Data[0] = 'X'
	es, err = s.Entries(2, 3, 1)
	require.NoError(t, err)
	require.Equal(t, big, es[0].Data)
}
func TestReadyCutCertifiedGenerationAndFaults(t *testing.T) {
	for _, stage := range []string{"success", "base_written", "base_synced", "before_root", "after_root", "before_retire"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			s := readyStore(t, dir)
			s.RequireCheckpointCertificate()
			for i := uint64(1); i <= 3; i++ {
				saveReady(t, s, i, []byte{byte(i)})
			}
			require.NoError(t, s.Sync())
			// Replacement must carry the newer saved suffix even though the
			// next required Ready barrier has not synced it yet.
			saveReady(t, s, 4, []byte{4})
			require.True(t, s.ready.dirty)
			old := s.ready.name
			require.Error(t, s.CreateSnapshot(2, &raftpb.ConfState{Voters: []uint64{1}}, []byte("cert")))
			require.NoError(t, s.AuthorizeCheckpoint(2))
			fault := errors.New(stage)
			s.ready.testStage = func(got string) error {
				if got == stage {
					return fault
				}
				return nil
			}
			err := s.CreateSnapshot(2, &raftpb.ConfState{Voters: []uint64{1}}, []byte("cert"))
			if stage == "success" {
				require.NoError(t, err)
				require.NotEqual(t, old, s.ready.name)
			} else {
				require.ErrorIs(t, err, fault)
				require.True(t, RecoveryRequired(err))
			}
			s.Close()
			reopened := readyStore(t, dir)
			defer reopened.Close()
			files, err := filepath.Glob(filepath.Join(dir, "*.rdc"))
			require.NoError(t, err)
			require.Len(t, files, 1, "validated selected root retires abandoned generations")
			hs, err := reopened.HardState()
			require.NoError(t, err)
			require.Equal(t, uint64(4), hs.Commit)
			es, err := reopened.Entries(3, 5, 1<<20)
			require.NoError(t, err)
			require.Len(t, es, 2)
			snap, err := reopened.Snapshot()
			require.NoError(t, err)
			require.Equal(t, uint64(2), snap.Metadata.Index)
			require.Equal(t, []byte("cert"), snap.Data)
			_, err = reopened.Term(1)
			require.ErrorIs(t, err, raft.ErrCompacted)
		})
	}
}
func TestReadyCutEncryptionFreshNonceAfterTornTail(t *testing.T) {
	dir := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	s, err := InitCheckpointEncrypted(dir, key)
	require.NoError(t, err)
	saveReady(t, s, 1, []byte("secret A"))
	require.NoError(t, s.Sync())
	off := s.ready.size
	fault := errors.New("tail")
	left := readyHeaderSize + 12
	s.ready.testWrite = func(p []byte, off int64) (int, error) {
		n := min(left, len(p))
		left -= n
		if n == 0 {
			return 0, fault
		}
		got, err := s.ready.f.WriteAt(p[:n], off)
		if err == nil && n < len(p) {
			err = fault
		}
		return got, err
	}
	require.Error(t, s.Save(&raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{{Index: 2, Term: 1, Data: []byte("secret B")}}, nil))
	var first [readyHeaderSize]byte
	_, err = s.ready.f.ReadAt(first[:], off)
	require.NoError(t, err)
	require.Error(t, s.Close())
	reopened, err := InitCheckpointEncrypted(dir, key)
	require.NoError(t, err)
	saveReady(t, reopened, 2, []byte("secret C"))
	require.NoError(t, reopened.Sync())
	var second [readyHeaderSize]byte
	_, err = reopened.ready.f.ReadAt(second[:], off)
	require.NoError(t, err)
	require.Equal(t, binary.BigEndian.Uint64(first[16:24]), binary.BigEndian.Uint64(second[16:24]))
	require.NotEqual(t, first[32:48], second[32:48], "same sequence/offset cannot reuse CTR nonce")
	name := reopened.ready.name
	require.NoError(t, reopened.Close())
	raw, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "secret C")
	_, err = InitEncrypted(dir, nil)
	require.Error(t, err)
	good, err := InitEncrypted(dir, key)
	require.NoError(t, err)
	defer good.Close()
	es, err := good.Entries(1, 3, mathMax())
	require.NoError(t, err)
	require.Equal(t, []byte("secret C"), es[1].Data)
}
func mathMax() uint64 { return ^uint64(0) }

func TestReadyCutInstallCrashHelper(t *testing.T) {
	stage := os.Getenv("DGRAPH_READY_INSTALL_CUT")
	if stage == "" {
		return
	}
	w, intent := installIntentFixtureWithFormat(t, os.Getenv("DGRAPH_READY_INSTALL_DIR"), true)
	w.installStageForTest = func(at string) {
		if at == stage {
			os.Exit(0)
		}
	}
	w.ready.testStage = func(at string) error {
		if at == stage {
			os.Exit(0)
		}
		return nil
	}
	require.NoError(t, w.AcceptSnapshotInstall(intent))
	require.NoError(t, w.CompleteSnapshotInstall(intent.ReceiverMetadata))
	t.Fatalf("missing stage %s", stage)
}
func TestReadyCutInstallCrashAndActualIdentity(t *testing.T) {
	for _, stage := range []string{"accepted", "install_cut_written", "install_cut_synced", "base_written", "base_synced", "before_root", "after_root", "before_retire"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestReadyCutInstallCrashHelper$", "-test.count=1")
			cmd.Env = append(os.Environ(), "DGRAPH_READY_INSTALL_CUT="+stage, "DGRAPH_READY_INSTALL_DIR="+dir)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			intent, err := ReadAcceptedSnapshotInstall(dir)
			require.NoError(t, err)
			require.NotNil(t, intent)
			for i := 0; i < 2; i++ {
				s := readyStore(t, dir)
				s.RequireCheckpointCertificate()
				require.Error(t, s.CompleteSnapshotInstall([]byte("wrong receiver")))
				require.NoError(t, s.CompleteSnapshotInstall(intent.ReceiverMetadata))
				hs, err := s.HardState()
				require.NoError(t, err)
				require.Equal(t, raftpb.HardState{Term: 4, Vote: 1, Commit: 10}, hs)
				last, err := s.LastIndex()
				require.NoError(t, err)
				require.Equal(t, uint64(10), last)
				require.NoError(t, s.Close())
			}
			s := readyStore(t, dir)
			s.RequireCheckpointCertificate()
			require.NoError(t, s.CompleteSnapshotInstall(intent.ReceiverMetadata))
			require.NoError(t, s.FinishSnapshotInstall(intent.Digest))
			require.NoError(t, s.Close())
			require.NoFileExists(t, filepath.Join(dir, metaName))
			require.NoFileExists(t, filepath.Join(dir, snapshotInstallFile))
			require.NoFileExists(t, filepath.Join(dir, intent.ArchiveName))
		})
	}
}

// These are complete checksummed records with invalid transitions, not torn
// tails: recovery must enforce the same invariants as the public Save boundary.
func TestReadyCutRecoveryTransitionAndPrewriteValidation(t *testing.T) {
	for _, kind := range []string{"vote_clear", "commit_regress", "gap", "unknown_type"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			s := readyStore(t, dir)
			saveReady(t, s, 1, []byte("A"))
			require.NoError(t, s.Sync())
			off := s.ready.size
			h := raftpb.HardState{Term: 1, Vote: 1, Commit: 2}
			es := []raftpb.Entry{{Index: 2, Term: 1, Data: []byte("B")}}
			switch kind {
			case "vote_clear":
				h.Vote = 0
			case "commit_regress":
				h.Commit = 0
			case "gap":
				es[0].Index, h.Commit = 3, 3
			case "unknown_type":
				es[0].Type = raftpb.EntryType(255)
			}
			err := s.Save(&h, es, nil)
			require.True(t, PermanentStorageError(err))
			require.False(t, RecoveryRequired(err))
			require.Equal(t, off, s.ready.size)
			saveReady(t, s, 2, []byte("B"))
			require.NoError(t, s.Sync())
			name := s.ready.name
			require.NoError(t, s.Close())
			raw, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			body := off + readyHeaderSize
			switch kind {
			case "vote_clear":
				binary.BigEndian.PutUint64(raw[body+40:], 0)
			case "commit_regress":
				binary.BigEndian.PutUint64(raw[body+48:], 0)
			case "gap":
				binary.BigEndian.PutUint64(raw[body+48:], 3)
				binary.BigEndian.PutUint64(raw[body+readyStateSize:], 3)
			case "unknown_type":
				binary.BigEndian.PutUint64(raw[body+readyStateSize+16:], 255)
			}
			n := int64(binary.BigEndian.Uint64(raw[off+8:]))
			binary.BigEndian.PutUint32(raw[body+n:], crc32.Checksum(raw[body:body+n], readyCRC))
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), raw, 0600))
			_, err = InitEncrypted(dir, nil)
			require.Error(t, err, "valid checksum does not authorize an invalid transition")
		})
	}
}

func TestReadyCutInvalidIntentPreservesTailAndAbandonedGeneration(t *testing.T) {
	dir := t.TempDir()
	s := readyStore(t, dir)
	saveReady(t, s, 1, []byte("A"))
	require.NoError(t, s.Close())
	path := filepath.Join(dir, s.ready.name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte("incomplete"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	abandoned := filepath.Join(dir, "00000000000000000000000000000000.rdc")
	require.NoError(t, os.WriteFile(abandoned, []byte("unselected"), 0600))
	rootTemp := filepath.Join(dir, ".raft-ready-root-abandoned")
	unrelated := filepath.Join(dir, "unrelated.keep")
	require.NoError(t, os.WriteFile(rootTemp, []byte("unpublished root"), 0600))
	require.NoError(t, os.WriteFile(unrelated, []byte("preserve"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotInstallFile), []byte("corrupt accepted intent"), 0600))
	_, err = InitEncrypted(dir, nil)
	require.Error(t, err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = os.Stat(abandoned)
	require.NoError(t, err)
	_, err = os.Stat(rootTemp)
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, snapshotInstallFile)))
	reopened := readyStore(t, dir)
	defer reopened.Close()
	require.Equal(t, int64(len(before)-len("incomplete")), reopened.ready.size)
	_, err = os.Stat(abandoned)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(rootTemp)
	require.True(t, os.IsNotExist(err))
	kept, err := os.ReadFile(unrelated)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(kept))
}
