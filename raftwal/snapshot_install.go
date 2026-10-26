/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	snapshotInstallFile           = "accepted-snapshot-install.json"
	maxSnapshotInstallIntentBytes = 4 << 20
)

// AcceptedSnapshotInstall is durable authority for finishing one peer install.
// The complete archive remains outside the posting owner's working namespaces.
// It is synced before this checksummed record is atomically published. Only the
// external owner's verified authorization callback may accept this intent.
type AcceptedSnapshotInstall struct {
	Version           uint32
	ReceiverIdentity  string
	Group             uint32
	RaftID            uint64
	Snapshot          raftpb.Snapshot
	HardState         raftpb.HardState
	ArchiveName       string
	ArchiveSHA256     string
	ArchiveBytes      int64
	SourceCertificate []byte
	ReceiverMetadata  []byte
	Digest            string `json:"-"`
}

type snapshotInstallEnvelope struct {
	Payload json.RawMessage
	SHA256  string
}

type snapshotInstallPublicationError struct{ error }

func (e *snapshotInstallPublicationError) Unwrap() error { return e.error }

// SnapshotInstallPublicationUncertain means the archive must be retained and
// the process must reopen; an intent namespace operation may have succeeded.
func SnapshotInstallPublicationUncertain(err error) bool {
	var publication *snapshotInstallPublicationError
	return errors.As(err, &publication)
}

func sameInstalledSnapshot(a, b raftpb.Snapshot) bool {
	x, err := a.Marshal()
	if err != nil {
		return false
	}
	y, err := b.Marshal()
	return err == nil && bytes.Equal(x, y)
}

func syncInstallDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// ReadAcceptedSnapshotInstall must run before posting-owner open/cleanup and
// before WAL alignment/pruning. Missing or corrupted accepted authority refuses
// startup; it never silently falls back to an independently persisted hint.
func ReadAcceptedSnapshotInstall(dir string) (*AcceptedSnapshotInstall, error) {
	f, err := os.Open(filepath.Join(dir, snapshotInstallFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, maxSnapshotInstallIntentBytes+1))
	closeErr := f.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	intent, err := decodeAcceptedSnapshotInstall(dir, data)
	if err != nil {
		return nil, err
	}
	id, group, err := readRaftIdentity(dir)
	if err != nil {
		return nil, err
	}
	if id != intent.RaftID || group != uint64(intent.Group) {
		return nil, fmt.Errorf("snapshot install WAL identity mismatch")
	}
	return intent, nil
}

func decodeAcceptedSnapshotInstall(dir string, data []byte) (*AcceptedSnapshotInstall, error) {
	if len(data) > maxSnapshotInstallIntentBytes {
		return nil, fmt.Errorf("snapshot install intent exceeds 4 MiB")
	}
	var envelope snapshotInstallEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.SHA256 != hex.EncodeToString(digest[:]) {
		return nil, fmt.Errorf("snapshot install intent checksum mismatch")
	}
	var intent AcceptedSnapshotInstall
	if err := json.Unmarshal(envelope.Payload, &intent); err != nil {
		return nil, err
	}
	if intent.Version != 1 || intent.Group == 0 || intent.RaftID == 0 ||
		intent.ReceiverIdentity == "" || raft.IsEmptySnap(intent.Snapshot) ||
		intent.Snapshot.Metadata.Term == 0 || intent.HardState.Term < intent.Snapshot.Metadata.Term ||
		intent.HardState.Commit != intent.Snapshot.Metadata.Index || len(intent.ReceiverMetadata) == 0 ||
		len(intent.ReceiverMetadata) > 1<<20 || len(intent.SourceCertificate) == 0 || len(intent.SourceCertificate) > 2<<20 {
		return nil, fmt.Errorf("invalid snapshot install authority")
	}
	if intent.ArchiveName != filepath.Base(intent.ArchiveName) ||
		!strings.HasPrefix(intent.ArchiveName, "snapshot-install-") ||
		!strings.HasSuffix(intent.ArchiveName, ".tar") || intent.ArchiveBytes <= 0 ||
		intent.ArchiveBytes > 8<<30 || len(intent.ArchiveSHA256) != 64 {
		return nil, fmt.Errorf("invalid snapshot install archive identity")
	}
	archive := filepath.Join(dir, intent.ArchiveName)
	info, err := os.Lstat(archive)
	if err != nil {
		return nil, fmt.Errorf("snapshot install archive unavailable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() != intent.ArchiveBytes {
		return nil, fmt.Errorf("snapshot install archive size/type mismatch")
	}
	f, err := os.Open(archive)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, io.LimitReader(f, intent.ArchiveBytes+1))
	closeErr := f.Close()
	if copyErr != nil {
		return nil, copyErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if hex.EncodeToString(hash.Sum(nil)) != intent.ArchiveSHA256 {
		return nil, fmt.Errorf("snapshot install archive checksum mismatch")
	}
	intent.Digest = envelope.SHA256
	return &intent, nil
}

// AcceptSnapshotInstall is called only after archive validation and sync, from
// the posting owner's authorization callback, before CURRENT can be changed.
func (w *DiskStorage) AcceptSnapshotInstall(intent AcceptedSnapshotInstall) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if !w.checkpointGuard || intent.RaftID != w.uintLocked(RaftId) || uint64(intent.Group) != w.uintLocked(GroupId) {
		return fmt.Errorf("snapshot install receiver identity mismatch")
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	current, err := w.hardStateLocked()
	if err != nil {
		return err
	}
	previous, err := w.snapshotLocked()
	if err != nil {
		return err
	}
	if current.Commit > intent.Snapshot.Metadata.Index || current.Term > intent.HardState.Term ||
		(current.Term == intent.HardState.Term && current.Vote != 0 && current.Vote != intent.HardState.Vote) ||
		(previous.Metadata.Index >= intent.Snapshot.Metadata.Index && !sameInstalledSnapshot(previous, intent.Snapshot)) {
		return fmt.Errorf("snapshot acceptance would replace newer committed or election authority")
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(payload)
	data, err := json.Marshal(snapshotInstallEnvelope{Payload: payload, SHA256: hex.EncodeToString(hash[:])})
	if err != nil {
		return err
	}
	if _, err := decodeAcceptedSnapshotInstall(w.dir, data); err != nil {
		return err
	}
	archive, err := os.Open(filepath.Join(w.dir, intent.ArchiveName))
	if err != nil {
		return err
	}
	syncErr := archive.Sync()
	closeErr := archive.Close()
	if syncErr != nil {
		return w.installStorageFailure(syncErr)
	}
	if closeErr != nil {
		return w.installStorageFailure(closeErr)
	}
	if err := syncInstallDir(w.dir); err != nil {
		if w.ready != nil {
			return w.ready.fail(err)
		}
		return err
	}
	old, err := ReadAcceptedSnapshotInstall(w.dir)
	if err != nil {
		return err
	}
	if old != nil {
		if old.Digest == hex.EncodeToString(hash[:]) {
			return nil
		}
		return fmt.Errorf("another accepted snapshot install is unfinished")
	}
	file, err := os.CreateTemp(w.dir, ".snapshot-install-intent-")
	if err != nil {
		return w.installStorageFailure(err)
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := file.Write(data); err != nil {
		file.Close()
		return w.installStorageFailure(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return w.installStorageFailure(err)
	}
	if err := file.Close(); err != nil {
		return w.installStorageFailure(err)
	}
	// The archive and checksummed candidate have both been validated and synced
	// before this atomic namespace edge can authorize a posting CURRENT switch.
	if err := os.Rename(name, filepath.Join(w.dir, snapshotInstallFile)); err != nil {
		if w.ready != nil {
			return w.ready.fail(&snapshotInstallPublicationError{err})
		}
		return &snapshotInstallPublicationError{err}
	}
	if err := syncInstallDir(w.dir); err != nil {
		if w.ready != nil {
			return w.ready.fail(&snapshotInstallPublicationError{err})
		}
		return &snapshotInstallPublicationError{err}
	}
	if w.installStageForTest != nil {
		w.installStageForTest("accepted")
	}
	return nil
}

// CompleteSnapshotInstall may run only after the posting owner confirms the
// exact receiver certificate/root. The accepted archive/intent survives every
// intermediate WAL state and makes this replacement idempotently recoverable.
func (w *DiskStorage) CompleteSnapshotInstall(receiverMetadata []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	intent, err := ReadAcceptedSnapshotInstall(w.dir)
	if err != nil {
		return err
	}
	if !w.checkpointGuard || intent == nil || !bytes.Equal(intent.ReceiverMetadata, receiverMetadata) ||
		intent.RaftID != w.uintLocked(RaftId) || uint64(intent.Group) != w.uintLocked(GroupId) {
		return fmt.Errorf("snapshot install lacks matching receiver certificate")
	}
	current, err := w.hardStateLocked()
	if err != nil {
		return err
	}
	snapshot, err := w.snapshotLocked()
	if err != nil {
		return err
	}
	index := intent.Snapshot.Metadata.Index
	if snapshot.Metadata.Index >= index {
		if !sameInstalledSnapshot(snapshot, intent.Snapshot) {
			return fmt.Errorf("snapshot install conflicts with existing snapshot")
		}
		if current.Commit > index {
			w.certifiedIndex = index
			return w.syncLocked()
		}
		// A crash may have persisted snapshot metadata but not cleared the old
		// divergent suffix. Repeat the replacement while commit is at the cut.
	}
	if current.Commit > index {
		return fmt.Errorf("snapshot install would move committed progress backward")
	}
	hardState := intent.HardState
	if current.Term > hardState.Term {
		hardState.Term, hardState.Vote = current.Term, current.Vote
	} else if current.Term == hardState.Term && current.Vote != 0 {
		if hardState.Vote != 0 && hardState.Vote != current.Vote {
			return fmt.Errorf("snapshot install would change an existing vote")
		}
		hardState.Vote = current.Vote
	}
	if w.ready != nil {
		if err := w.ready.append(&hardState, nil, &intent.Snapshot, true); err != nil {
			return err
		}
		if err := w.ready.stage("install_cut_written"); err != nil {
			return w.ready.fail(err)
		}
		if err := w.ready.sync(); err != nil {
			return err
		}
		if err := w.ready.stage("install_cut_synced"); err != nil {
			return w.ready.fail(err)
		}
		w.certifiedIndex = index
		// Atomic cut has already replaced both election state and divergent suffix.
		return w.ready.replace()
	}
	if err := w.meta.StoreSnapshot(&intent.Snapshot); err != nil {
		return err
	}
	if err := w.meta.StoreHardState(&hardState); err != nil {
		return err
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	if w.installStageForTest != nil {
		w.installStageForTest("metadata")
	}
	w.certifiedIndex = index
	// The old uncommitted suffix may diverge from the imported source. Its
	// replacement is safe only because exact posting state and install input
	// are now durable. Init defers ordinary alignment while intent is present.
	if err := w.wal.reset(); err != nil {
		return err
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	if w.installStageForTest != nil {
		w.installStageForTest("wal")
	}
	return nil
}

func (w *DiskStorage) FinishSnapshotInstall(expectedDigest string) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	intent, err := ReadAcceptedSnapshotInstall(w.dir)
	if err != nil {
		return err
	}
	if intent == nil || intent.Digest != expectedDigest || w.certifiedIndex != intent.Snapshot.Metadata.Index {
		return fmt.Errorf("snapshot install completion identity mismatch")
	}
	snapshot, err := w.snapshotLocked()
	if err != nil {
		return err
	}
	hs, err := w.hardStateLocked()
	if err != nil {
		return err
	}
	last := w.lastIndexLocked()
	if last < snapshot.Metadata.Index {
		last = snapshot.Metadata.Index
	}
	if !sameInstalledSnapshot(snapshot, intent.Snapshot) || hs.Commit < snapshot.Metadata.Index || last < hs.Commit {
		return fmt.Errorf("snapshot install WAL completion is not durable and contiguous")
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(w.dir, snapshotInstallFile)); err != nil {
		if w.ready != nil {
			return w.ready.fail(err)
		}
		return err
	}
	if err := syncInstallDir(w.dir); err != nil {
		if w.ready != nil {
			return w.ready.fail(err)
		}
		return err
	}
	if err := os.Remove(filepath.Join(w.dir, intent.ArchiveName)); err != nil {
		return w.installStorageFailure(err)
	}
	return w.installStorageFailure(syncInstallDir(w.dir))
}

// Called only at storage I/O boundaries, never for semantic intent rejection.
func (w *DiskStorage) installStorageFailure(err error) error {
	if err != nil && w.ready != nil {
		return w.ready.fail(err)
	}
	return err
}

// readRaftIdentity is used while an existing DiskStorage lock may already be
// held. It performs cold actual-format reads and never calls locking accessors.
func readRaftIdentity(dir string) (uint64, uint64, error) {
	if _, err := os.Stat(filepath.Join(dir, readyRootName)); err == nil {
		if _, err = os.Stat(filepath.Join(dir, metaName)); err == nil {
			return 0, 0, fmt.Errorf("competing Raft identities")
		} else if !os.IsNotExist(err) {
			return 0, 0, err
		}
		return readyIdentity(dir)
	} else if !os.IsNotExist(err) {
		return 0, 0, err
	}
	f, err := os.Open(filepath.Join(dir, metaName))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	var b [16]byte
	if _, err = io.ReadFull(f, b[:]); err != nil {
		return 0, 0, err
	}
	return binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint64(b[8:]), nil
}
