/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	stdErrors "errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/net/trace"

	"github.com/dgraph-io/dgraph/v25/x"
)

// DiskStorage handles disk access and writing for the RAFT write-ahead log.
// Fresh explicit checkpoint directories use the atomic Ready-cut journal.
// Existing actual format is authoritative. The layout below describes only the
// legacy wal.meta and <start idx zero padded>.wal format.
//
// === wal.meta file ===
// This file is generally around 4KB, so it can fit nicely in one Linux page.
//
//	Layout:
//
// 00-08 Bytes: Raft ID
// 08-16 Bytes: Group ID
// 16-24 Bytes: Checkpoint Index
// 512 Bytes: Hard State (Marshalled)
// 1024-1032 Bytes: Snapshot Index
// 1032-1040 Bytes: Snapshot Term
// 1040 Bytes: Snapshot (Marshalled)
//
// --- <0000i>.wal files ---
// These files contain raftpb.Entry protos. Each entry is composed of term, index, type and data.
//
// Term takes 8 bytes. Index takes 8 bytes. Type takes 8 bytes. And for data, we store an offset to
// the actual slice, which is 8 bytes. Size of entry = 32 bytes.
// First 30K entries would consume 960KB, hence fitting on the first MB of the file (logFileOffset).
//
// Pre-allocate 1MB in each file just for these entries, and zero them out explicitly. Zeroing them
// out ensures that we know when these entries end, in case of a restart.
//
// And the data for these entries are laid out starting logFileOffset. Those are the offsets you
// store in the Entry for Data field.
// After 30K entries, we rotate the file.
//
// --- clean up ---
// If snapshot idx = Idx_s. We find the first log file whose first entry is
// less than Idx_s. This file and anything above MUST be kept. All the log
// files lower than this file can be deleted.
//
// --- sync ---
// mmap fares well with process crashes without doing anything. In case
// HardSync is set, msync is called after every write, which flushes those
// writes to disk.
type DiskStorage struct {
	dir  string
	elog trace.EventLog

	meta  *metaFile
	wal   *wal
	ready *readyJournal
	lock  sync.Mutex

	// External materialized stores may compact only a certified durable prefix.
	checkpointGuard     bool
	certifiedIndex      uint64
	syncStageForTest    func(string)
	installStageForTest func(string)
}

// Init initializes an instance of DiskStorage without encryption.
func Init(dir string) *DiskStorage {
	ds, err := InitEncrypted(dir, nil)
	x.Check(err)
	return ds
}

// InitEncrypted initializes returns a properly initialized instance of DiskStorage.
// To gracefully shutdown DiskStorage, store.Closer.SignalAndWait() should be called.
func InitEncrypted(dir string, encKey x.Sensitive) (*DiskStorage, error) {
	return initStorage(dir, encKey, false)
}

// InitCheckpointEncrypted chooses the Ready-cut format only for a fresh
// explicit external-checkpoint directory. Existing actual formats remain
// authoritative, regardless of the constructor used to reopen them.
func InitCheckpointEncrypted(dir string, encKey x.Sensitive) (*DiskStorage, error) {
	return initStorage(dir, encKey, true)
}

func initStorage(dir string, encKey x.Sensitive, checkpoint bool) (*DiskStorage, error) {
	_, rootErr := os.Stat(filepath.Join(dir, readyRootName))
	_, metaErr := os.Stat(filepath.Join(dir, metaName))
	if rootErr != nil && !os.IsNotExist(rootErr) {
		return nil, rootErr
	}
	if metaErr != nil && !os.IsNotExist(metaErr) {
		return nil, metaErr
	}
	if rootErr == nil && metaErr == nil {
		return nil, errors.New("competing Raft storage formats")
	}
	// Validate accepted authority before creation, tail truncation or orphan cleanup.
	if _, err := ReadAcceptedSnapshotInstall(dir); err != nil {
		return nil, err
	}
	if rootErr == nil || (checkpoint && os.IsNotExist(metaErr)) {
		if rootErr != nil {
			if err := os.MkdirAll(dir, 0700); err != nil {
				return nil, err
			}
			files, err := os.ReadDir(dir)
			if err != nil {
				return nil, err
			}
			// Do not initialize over legacy fragments or a failed new creation.
			for _, f := range files {
				if strings.HasSuffix(f.Name(), logSuffix) || strings.HasSuffix(f.Name(), ".rdc") {
					return nil, errors.New("Raft directory lacks authoritative format root")
				}
			}
		}
		journal, err := openReadyJournal(dir, encKey, rootErr != nil)
		if err != nil {
			return nil, err
		}
		w := &DiskStorage{dir: dir, ready: journal, elog: trace.NewEventLog("ReadyCut", "RaftStorage")}
		glog.Infof("Raft storage opened: format=ready_cut directory=%s", dir)
		return w, nil
	}
	if os.IsNotExist(metaErr) {
		files, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".rdc") {
				return nil, errors.New("ready fragments lack authoritative root")
			}
		}
	}
	return initLegacyStorage(dir, encKey)
}

func initLegacyStorage(dir string, encKey x.Sensitive) (*DiskStorage, error) {
	intent, err := ReadAcceptedSnapshotInstall(dir)
	if err != nil {
		return nil, err
	}
	w := &DiskStorage{
		dir: dir,
	}

	if w.meta, err = newMetaFile(dir); err != nil {
		return nil, err
	}
	// fmt.Printf("meta: %s\n", hex.Dump(w.meta.data[1024:2048]))
	// fmt.Printf("found snapshot of size: %d\n", sliceSize(w.meta.data, snapshotOffset))

	encryptionKey = encKey
	if w.wal, err = openWal(dir); err != nil {
		return nil, err
	}

	w.elog = trace.NewEventLog("Badger", "RaftStorage")
	glog.Infof("Raft storage opened: format=legacy directory=%s", dir)
	if intent != nil {
		// The durable archive plus accepted intent owns this transition. Normal
		// snapshot/log alignment and pruning must wait until the posting owner
		// confirms its exact receiver root and CompleteSnapshotInstall runs.
		return w, nil
	}

	snap, err := w.meta.snapshot()
	if err != nil {
		return nil, err
	}

	first, _ := w.FirstIndex()
	if !raft.IsEmptySnap(snap) {
		x.AssertTruef(snap.Metadata.Index+1 == first,
			"snap index: %d + 1 should be equal to first: %d\n", snap.Metadata.Index, first)
	}

	// If db is not closed properly, there might be index ranges for which delete entries are not
	// inserted. So insert delete entries for those ranges starting from 0 to (first-1).
	w.wal.deleteBefore(first - 1)
	last := w.wal.LastIndex()

	glog.Infof("Init Raft Storage with snap: %d, first: %d, last: %d\n",
		snap.Metadata.Index, first, last)
	return w, nil
}

func (w *DiskStorage) SetUint(info MetaInfo, id uint64) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.ready != nil {
		if info < RaftId || info > CheckpointIndex {
			panic("invalid writable Ready metadata")
		}
		if w.ready.check() == nil {
			w.ready.ids[int(info)] = id
			w.ready.dirty = true
		}
		return
	}
	w.meta.SetUint(info, id)
}
func (w *DiskStorage) Uint(info MetaInfo) uint64 {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.uintLocked(info)
}
func (w *DiskStorage) uintLocked(info MetaInfo) uint64 {
	if w.ready != nil {
		switch info {
		case SnapshotIndex:
			return w.ready.snapshot.Index
		case SnapshotTerm:
			return w.ready.snapshot.Term
		default:
			return w.ready.ids[int(info)]
		}
	}
	return w.meta.Uint(info)
}
func (w *DiskStorage) hardStateLocked() (raftpb.HardState, error) {
	if w.ready != nil {
		return w.ready.hs, nil
	}
	return w.meta.HardState()
}
func (w *DiskStorage) snapshotLocked() (raftpb.Snapshot, error) {
	if w.ready != nil {
		return w.ready.getSnapshot()
	}
	return w.meta.snapshot()
}
func (w *DiskStorage) lastIndexLocked() uint64 {
	if w.ready != nil {
		return w.ready.lastIndex()
	}
	return max(w.wal.LastIndex(), w.meta.Uint(SnapshotIndex))
}

// reset resets the entries. Used for testing.
func (w *DiskStorage) reset(es []raftpb.Entry) error {
	// Clean out the state.
	if err := w.wal.reset(); err != nil {
		return err
	}
	return w.addEntries(es)
}

func (w *DiskStorage) HardState() (raftpb.HardState, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.ready != nil {
		return w.ready.hs, nil
	}
	if w.meta == nil {
		return raftpb.HardState{}, errors.Errorf("uninitialized meta file")
	}
	return w.meta.HardState()
}

// Checkpoint returns the Raft index corresponding to the checkpoint.
func (w *DiskStorage) Checkpoint() (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.ready != nil {
		return w.ready.ids[2], nil
	}
	if w.meta == nil {
		return 0, errors.Errorf("uninitialized meta file")
	}
	return w.meta.Uint(CheckpointIndex), nil
}

// Implement the Raft.Storage interface.
// -------------------------------------

// InitialState returns the saved HardState and ConfState information.
func (w *DiskStorage) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.elog.Printf("InitialState")
	defer w.elog.Printf("Done")
	hs, err = w.hardStateLocked()
	if err != nil {
		return
	}
	var snap raftpb.Snapshot
	snap, err = w.snapshotLocked()
	if err != nil {
		return
	}
	return hs, snap.Metadata.ConfState, nil
}

func (w *DiskStorage) NumEntries() int {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.ready != nil {
		return len(w.ready.entries)
	}
	start := w.wal.firstIndex()

	var count int
	for {
		ents := w.wal.allEntries(start, math.MaxUint64, 64<<20)
		if len(ents) == 0 {
			return count
		}
		count += len(ents)
		start = ents[len(ents)-1].Index + 1
	}
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (w *DiskStorage) Entries(lo, hi, maxSize uint64) (es []raftpb.Entry, rerr error) {
	// glog.Infof("Entries: [%d, %d) maxSize:%d", lo, hi, maxSize)
	w.lock.Lock()
	defer w.lock.Unlock()

	// glog.Infof("Entries after lock: [%d, %d) maxSize:%d", lo, hi, maxSize)

	if w.ready != nil {
		return w.ready.getEntries(lo, hi, maxSize)
	}
	first := w.wal.firstIndex()
	if lo < first {
		glog.Errorf("lo: %d <first: %d\n", lo, first)
		return nil, raft.ErrCompacted
	}

	last := w.wal.LastIndex()
	if hi > last+1 {
		glog.Errorf("hi: %d > last+1: %d\n", hi, last+1)
		return nil, raft.ErrUnavailable
	}

	ents := w.wal.allEntries(lo, hi, maxSize)
	// glog.Infof("got entries [%d, %d): %+v\n", lo, hi, ents)
	return ents, nil
}

func (w *DiskStorage) Term(idx uint64) (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.ready != nil {
		return w.ready.term(idx)
	}
	si := w.meta.Uint(SnapshotIndex)
	if idx < si {
		glog.Errorf("TERM for %d = %v\n", idx, raft.ErrCompacted)
		return 0, raft.ErrCompacted
	}
	if idx == si {
		return w.meta.Uint(SnapshotTerm), nil
	}

	term, err := w.wal.Term(idx)
	if err != nil {
		glog.Errorf("TERM for %d = %v\n", idx, err)
	}
	// glog.Errorf("Got term: %d for index: %d\n", term, idx)
	return term, err
}

func (w *DiskStorage) LastIndex() (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.ready != nil {
		return w.ready.lastIndex(), nil
	}
	li := w.wal.LastIndex()
	si := w.meta.Uint(SnapshotIndex)
	if li < si {
		return si, nil
	}
	return li, nil
}

func (w *DiskStorage) firstIndex() uint64 {
	if w.ready != nil {
		return w.ready.firstIndex()
	}
	if si := w.uintLocked(SnapshotIndex); si > 0 {
		return si + 1
	}
	return w.wal.firstIndex()
}

// FirstIndex returns the first index. It is typically SnapshotIndex+1.
func (w *DiskStorage) FirstIndex() (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	return w.firstIndex(), nil
}

// Snapshot returns the most recent snapshot.  If snapshot is temporarily
// unavailable, it should return ErrSnapshotTemporarilyUnavailable, so raft
// state machine could know that Storage needs some time to prepare snapshot
// and call Snapshot later.
func (w *DiskStorage) Snapshot() (raftpb.Snapshot, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	return w.snapshotLocked()
}

// ---------------- Raft.Storage interface complete.

// RequireCheckpointCertificate enables fail-closed retention for a materialized
// state machine. The caller must first validate its durable checkpoint against
// this WAL. The guard starts at zero; authorization is deliberately not inferred
// from CheckpointIndex, which historically was only an independently saved hint.
func (w *DiskStorage) RequireCheckpointCertificate() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.checkpointGuard = true
	w.certifiedIndex = 0
	if w.ready != nil {
		return
	}
	w.wal.dirty = make(map[*logFile]struct{})
	// One open-time pass also covers Ready writes made before the owner opted in.
	for _, file := range w.wal.files {
		w.wal.markDirty(file)
	}
	w.wal.markDirty(w.wal.current)
	w.wal.namespaceDirty = true
}

// AuthorizeCheckpoint permits pruning through a previously published durable
// state-machine certificate. Callers must not invoke it before certificate sync.
func (w *DiskStorage) AuthorizeCheckpoint(index uint64) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	hs, err := w.hardStateLocked()
	if err != nil {
		return err
	}
	if !w.checkpointGuard || index < w.certifiedIndex || index > hs.Commit {
		return errors.Errorf("invalid checkpoint retention authorization: index=%d certified=%d commit=%d", index, w.certifiedIndex, hs.Commit)
	}
	w.certifiedIndex = index
	return nil
}

// CreateSnapshot generates a snapshot with the given ConfState and data and writes it to disk.
func (w *DiskStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) error {
	glog.V(2).Infof("CreateSnapshot i=%d, cs=%+v", i, cs)

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.checkpointGuard && i > w.certifiedIndex {
		return errors.Errorf("snapshot index %d exceeds certified checkpoint %d", i, w.certifiedIndex)
	}
	first := w.firstIndex()
	if i < first {
		glog.Errorf("i=%d<first=%d, ErrSnapOutOfDate", i, first)
		return raft.ErrSnapOutOfDate
	}

	if w.ready != nil {
		term, err := w.ready.term(i)
		if err != nil {
			return err
		}
		if cs == nil {
			return readyInvalid("nil snapshot configuration")
		}
		snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: i, Term: term, ConfState: *cs}, Data: data}
		if err = w.ready.append(nil, nil, &snap, false); err != nil {
			return err
		}
		// The durable base contains every saved suffix, not only the older certificate.
		if err = w.ready.replace(); err != nil {
			return err
		}
		return nil
	}
	e, err := w.wal.seekEntry(i)
	if err != nil {
		return err
	}

	var snap raftpb.Snapshot
	snap.Metadata.Index = i
	snap.Metadata.Term = e.Term()
	x.AssertTrue(cs != nil)
	snap.Metadata.ConfState = *cs
	snap.Data = data

	if err := w.meta.StoreSnapshot(&snap); err != nil {
		return err
	}
	// Persist retention authorization before deleting any recovery input. Without
	// this ordering a power loss could retain an old snapshot with missing logs.
	if w.checkpointGuard {
		if err := w.syncLocked(); err != nil {
			return err
		}
	}
	// Now we delete all the files which are below the snapshot index.
	w.wal.deleteBefore(snap.Metadata.Index)
	return nil
}

// TODO(Aman): In the raft example here, we store the snapshot first, followed by entries
// and then, hard state. https://github.com/etcd-io/etcd/blob/main/contrib/raftexample/raft.go
// We should do the same here.
// Save would write Entries, HardState and Snapshot to persistent storage in order, i.e. Entries
// first, then HardState and Snapshot if they are not empty. If persistent storage supports atomic
// writes then all of them can be written together. Note that when writing an Entry with Index i,
// any previously-persisted entries with Index >= i must be discarded.
func (w *DiskStorage) Save(h *raftpb.HardState, es []raftpb.Entry, snap *raftpb.Snapshot) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.checkpointGuard && snap != nil && !raft.IsEmptySnap(*snap) && snap.Metadata.Index > w.certifiedIndex {
		if w.ready != nil {
			return readyInvalid("snapshot installation requires a certified TreeDB install protocol")
		}
		return errors.Errorf("snapshot installation requires a certified TreeDB install protocol")
	}
	if w.ready != nil {
		return w.ready.append(h, es, snap, false)
	}
	if err := w.wal.AddEntries(es); err != nil {
		return err
	}
	if err := w.meta.StoreHardState(h); err != nil {
		return err
	}
	if err := w.meta.StoreSnapshot(snap); err != nil {
		return err
	}
	return nil
}

// Append the new entries to storage.
func (w *DiskStorage) addEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	first, err := w.FirstIndex()
	if err != nil {
		return err
	}
	firste := entries[0].Index
	if firste+uint64(len(entries))-1 < first {
		// All of these entries have already been compacted.
		return nil
	}
	if first > firste {
		// Truncate compacted entries
		entries = entries[first-firste:]
	}

	// AddEntries would zero out all the entries starting entries[0].Index before writing.
	if err := w.wal.AddEntries(entries); err != nil {
		return errors.Wrapf(err, "while adding entries")
	}
	return nil
}

// truncateEntriesUntil deletes the data field of every raft entry
// of type EntryNormal and index ∈ [0, lastIdx).
func (w *DiskStorage) TruncateEntriesUntil(lastIdx uint64) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.checkpointGuard {
		return
	} // Only certificate-owned snapshot pruning is authorized.
	if w.ready != nil {
		// Destructive debug/legacy operation: represent it as a complete generation,
		// retaining entry identities while replacing only normal payloads.
		for i := range w.ready.entries {
			e := &w.ready.entries[i]
			if e.index >= lastIdx {
				break
			}
			if e.typ == raftpb.EntryNormal {
				e.value.size = 0
			}
		}
		if err := w.ready.replace(); err != nil {
			_ = w.ready.fail(err)
		}
		return
	}
	w.wal.truncateEntriesUntil(lastIdx)
}

func (w *DiskStorage) NumLogFiles() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.ready != nil {
		return 0
	} // One active generation; no sealed legacy files.
	return len(w.wal.files)
}

// Sync establishes the selected format's required durable boundary.
func (w *DiskStorage) Sync() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.syncLocked()
}

// syncLocked seals every dirty dependency before explicitly syncing metadata
// with its new commit frontier. Ready and certificate publication share this order.
func (w *DiskStorage) syncLocked() error {
	if w.ready != nil {
		if err := w.ready.check(); err != nil {
			return err
		}
		// SetUint has no error return. Serialize its bounded staged fields before
		// the barrier, including identity set before the first Ready.
		if w.ready.ids != w.ready.persistedIDs {
			if err := w.ready.append(nil, nil, nil, false); err != nil {
				return err
			}
		}
		if err := w.ready.sync(); err != nil {
			return err
		}
		if w.syncStageForTest != nil {
			w.syncStageForTest("ready")
		}
		return nil
	}
	if w.checkpointGuard {
		for file := range w.wal.dirty {
			if err := file.Sync(); err != nil {
				return err
			}
			if w.syncStageForTest != nil {
				w.syncStageForTest("wal")
			}
		}
		if w.wal.namespaceDirty {
			dir, err := os.Open(w.dir)
			if err != nil {
				return err
			}
			err = dir.Sync()
			closeErr := dir.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			if w.syncStageForTest != nil {
				w.syncStageForTest("directory")
			}
		}
		if err := w.meta.Sync(); err != nil {
			return err
		}
		if w.syncStageForTest != nil {
			w.syncStageForTest("meta")
		}
		clear(w.wal.dirty)
		w.wal.namespaceDirty = false
		return nil
	}
	if err := w.meta.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing meta")
	}
	if err := w.wal.current.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing current file")
	}
	return nil
}

// Close closes the DiskStorage.
func (w *DiskStorage) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.ready != nil {
		err := w.syncLocked()
		return stdErrors.Join(err, w.ready.close())
	}
	return w.syncLocked()
}
