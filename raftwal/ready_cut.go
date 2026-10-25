/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

// The Ready journal is an alternative authoritative format, not a write-ahead
// copy of wal.meta. All methods below run under DiskStorage.lock.
import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/x"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	readyRootName    = "raft-ready.current"
	readyHeaderSize  = 80
	readyFooterSize  = 16
	readyStateSize   = 80
	readyScratchSize = 64 << 10
	readyHasSnapshot = 1
	readyReset       = 2
)

var readyCRC = crc32.MakeTable(crc32.Castagnoli)
var readyMagic = [8]byte{'D', 'G', 'R', 'C', 'U', 'T', '0', '1'}
var readyEnd = [8]byte{'D', 'G', 'R', 'E', 'N', 'D', '0', '1'}

// RecoveryRequired reports an uncertain new-format storage failure. No later
// same-descriptor Sync can establish which earlier bytes survived writeback EIO.
func RecoveryRequired(err error) bool { var e *readyRecoveryError; return errors.As(err, &e) }

type readyRecoveryError struct{ err error }

func (e *readyRecoveryError) Error() string {
	return "Raft Ready storage requires reopen: " + e.err.Error()
}
func (e *readyRecoveryError) Unwrap() error { return e.err }

// PermanentStorageError also includes invalid pre-write updates. Those do not
// poison storage, but retrying unchanged input cannot make them valid.
func PermanentStorageError(err error) bool {
	var e *readyInputError
	return RecoveryRequired(err) || errors.As(err, &e)
}

type readyInputError struct{ err error }

func (e *readyInputError) Error() string { return "invalid Raft Ready update: " + e.err.Error() }
func (e *readyInputError) Unwrap() error { return e.err }
func readyInvalid(s string) error        { return &readyInputError{errors.New(s)} }

type readyRoot struct {
	Version uint32
	File    string
	CRC     uint32
}

func (r readyRoot) checksum() uint32 {
	return crc32.Checksum([]byte(fmt.Sprintf("%d:%s", r.Version, r.File)), readyCRC)
}
func readReadyRoot(dir string) (readyRoot, error) {
	var r readyRoot
	f, err := os.Open(filepath.Join(dir, readyRootName))
	if err != nil {
		return r, err
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	err = errors.Join(err, f.Close())
	if err != nil {
		return r, err
	}
	if len(data) > 4096 {
		return r, fmt.Errorf("Ready root too large")
	}
	if err = json.Unmarshal(data, &r); err != nil {
		return r, err
	}
	if r.Version != 2 || len(r.File) != 36 || filepath.Base(r.File) != r.File || filepath.Ext(r.File) != ".rdc" || r.CRC != r.checksum() {
		return r, fmt.Errorf("invalid Ready root")
	}
	if _, err = hex.DecodeString(r.File[:32]); err != nil {
		return r, err
	}
	return r, nil
}

type readyFrame struct {
	body  int64
	key   []byte
	nonce [16]byte
}
type readyValue struct {
	frame *readyFrame
	off   int64
	size  uint64
}
type readyEntry struct {
	index, term uint64
	typ         raftpb.EntryType
	value       readyValue
}
type readyDestination struct {
	f   *os.File
	off int64
}

func (d *readyDestination) Write(p []byte) (int, error) {
	n, e := d.f.WriteAt(p, d.off)
	d.off += int64(n)
	if e == nil && n != len(p) {
		e = io.ErrShortWrite
	}
	return n, e
}

type readyJournal struct {
	dir         string
	f           *os.File
	name        string
	size        int64
	capacity    int64
	identity    [16]byte
	initialized bool
	tailSum     uint32
	wire        struct {
		header  [readyHeaderSize]byte
		state   [readyStateSize]byte
		entry   [32]byte
		footer  [readyFooterSize]byte
		witness [readyWitnessSize]byte
	}
	seq           uint64
	ids           [3]uint64
	persistedIDs  [3]uint64
	hs            raftpb.HardState
	snapshot      raftpb.SnapshotMetadata
	snapshotValue readyValue
	entries       []readyEntry
	registry      *badger.KeyRegistry
	dirty         bool
	poison        error
	closed        bool
	dest          readyDestination
	bw            *bufio.Writer
	scratch       [readyScratchSize]byte
	// Fault hooks belong to this exact store, never a process-global switch.
	testWrite func([]byte, int64) (int, error)
	testSync  func() error
	testStage func(string) error
}

func (j *readyJournal) fail(err error) error {
	if err == nil {
		return nil
	}
	if j.poison == nil {
		j.poison = &readyRecoveryError{err}
	}
	return j.poison
}
func (j *readyJournal) stage(s string) error {
	if j.testStage != nil {
		return j.testStage(s)
	}
	return nil
}
func (j *readyJournal) check() error {
	if j.poison != nil {
		return j.poison
	}
	if j.closed {
		return &readyInputError{os.ErrClosed}
	}
	return nil
}
func openReadyJournal(dir string, key x.Sensitive, create bool) (j *readyJournal, err error) {
	j = &readyJournal{dir: dir}
	if len(key) > 0 {
		j.registry, err = badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: dir, EncryptionKey: key, EncryptionKeyRotationDuration: 10 * 24 * time.Hour})
		if err != nil {
			return nil, err
		}
	}
	owner := j
	defer func() {
		if err != nil {
			if owner.f != nil {
				owner.f.Close()
			}
			if owner.registry != nil {
				owner.registry.Close()
			}
		}
	}()
	root, e := readReadyRoot(dir)
	if os.IsNotExist(e) && create {
		if e = j.newFile(readyCapacityFloor); e != nil {
			return nil, e
		}
		if e = j.append(nil, nil, nil, true); e != nil {
			return nil, e
		}
		if e = j.sync(); e != nil {
			return nil, e
		}
		if e = j.publishRoot(); e != nil {
			return nil, e
		}
	} else {
		if e != nil {
			return nil, e
		}
		j.name = root.File
		j.f, e = os.OpenFile(filepath.Join(dir, j.name), os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		info, e := j.f.Stat()
		if e != nil {
			return nil, e
		}
		j.capacity = info.Size()
		if e = j.recover(); e != nil {
			return nil, e
		}
	}
	if e = j.retireUnselected(); e != nil {
		return nil, e
	}
	j.dest = readyDestination{f: j.f, off: j.size}
	j.bw = bufio.NewWriterSize(&j.dest, readyScratchSize)
	return j, nil
}

// Only a fully validated selected generation authorizes retiring abandoned
// generations. Accepted-install validation precedes opening this owner.
func (j *readyJournal) retireUnselected() error {
	files, err := os.ReadDir(j.dir)
	if err != nil {
		return err
	}
	changed := false
	for _, f := range files {
		name := f.Name()
		generation := name != j.name && len(name) == 36 && filepath.Ext(name) == ".rdc"
		if generation {
			_, err := hex.DecodeString(name[:32])
			generation = err == nil
		}
		rootTemp := strings.HasPrefix(name, ".raft-ready-root-")
		if !generation && !rootTemp {
			continue
		}
		info, err := f.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular abandoned Ready generation")
		}
		if err := os.Remove(filepath.Join(j.dir, name)); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return syncInstallDir(j.dir)
	}
	return nil
}

// validateTransition is shared by emission and replay. Reset is a complete
// replacement root, not permission to forget an election or committed frontier.
func (j *readyJournal) validateTransition(hs raftpb.HardState, flags, snapIndex, first, last uint64, n int) error {
	if snapIndex < j.snapshot.Index {
		return readyInvalid("snapshot regresses")
	}
	effectiveLast := j.lastIndex()
	if flags&readyReset != 0 {
		effectiveLast = snapIndex
	}
	if n > 0 {
		effectiveLast = max(snapIndex, last)
		base := j.lastIndex()
		if flags&readyReset != 0 {
			base = snapIndex
		}
		if first > base && first-base > 1 && first > snapIndex && first-snapIndex > 1 {
			return readyInvalid("entry gap")
		}
	}
	if hs.Commit < j.hs.Commit || hs.Commit > effectiveLast || hs.Term < j.hs.Term ||
		(hs.Term == j.hs.Term && j.hs.Vote != 0 && hs.Vote != j.hs.Vote) {
		return readyInvalid("HardState contradicts retained entries or election state")
	}
	return nil
}

func (j *readyJournal) newFile(capacity int64) error {
	checked, err := readyCapacitySize(capacity, 0, false)
	if err != nil || checked != capacity {
		return readyInvalid("invalid physical capacity")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	j.name = hex.EncodeToString(id[:]) + ".rdc"
	j.identity = id
	j.f, err = os.OpenFile(filepath.Join(j.dir, j.name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := j.initializeCapacity(capacity); err != nil {
		err = errors.Join(err, j.f.Close())
		j.f = nil
		return err
	}
	j.dest = readyDestination{f: j.f, off: j.size}
	j.bw = bufio.NewWriterSize(&j.dest, readyScratchSize)
	return nil
}
func (j *readyJournal) publishRoot() error {
	r := readyRoot{Version: 2, File: j.name}
	r.CRC = r.checksum()
	data, err := json.Marshal(r)
	if err != nil {
		return j.fail(err)
	}
	f, err := os.CreateTemp(j.dir, ".raft-ready-root-")
	if err != nil {
		return j.fail(err)
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return j.fail(err)
	}
	if err = j.stage("before_root"); err != nil {
		return j.fail(err)
	}
	if err = os.Rename(name, filepath.Join(j.dir, readyRootName)); err != nil {
		return j.fail(err)
	}
	if err = j.stage("after_root"); err != nil {
		return j.fail(err)
	}
	if err = syncInstallDir(j.dir); err != nil {
		return j.fail(err)
	}
	return nil
}
func addReadySize(total *uint64, n uint64) error {
	if n > math.MaxInt64-*total {
		return readyInvalid("record size overflow")
	}
	*total += n
	return nil
}

// append streams one complete logical cut before publishing its derived index.
// refs is used only by cold generation replacement; payloads are copied through
// bounded scratch directly from their still-owned exact old file.
func (j *readyJournal) append(h *raftpb.HardState, es []raftpb.Entry, snap *raftpb.Snapshot, reset bool) error {
	return j.appendSource(h, es, nil, nil, snap, reset)
}
func (j *readyJournal) appendSource(h *raftpb.HardState, es []raftpb.Entry, old *readyJournal, refs []readyEntry, snap *raftpb.Snapshot, reset bool) error {
	if err := j.check(); err != nil {
		return err
	}
	hs := j.hs
	if h != nil && !raft.IsEmptyHardState(*h) {
		hs = *h
	}
	n := len(es)
	if old != nil {
		n = len(refs)
	}
	var flags uint64
	var meta []byte
	var snapValue readyValue
	var snapData []byte
	if snap != nil && !raft.IsEmptySnap(*snap) {
		flags |= readyHasSnapshot
		var err error
		meta, err = snap.Metadata.Marshal()
		if err != nil {
			return &readyInputError{err}
		}
		snapData = snap.Data
	}
	if old != nil && !raft.IsEmptySnap(raftpb.Snapshot{Metadata: old.snapshot}) {
		flags |= readyHasSnapshot
		var err error
		meta, err = old.snapshot.Marshal()
		if err != nil {
			return &readyInputError{err}
		}
		snapValue = old.snapshotValue
	}
	if reset {
		flags |= readyReset
	}
	first, last := uint64(0), uint64(0)
	total := uint64(readyStateSize)
	if err := addReadySize(&total, uint64(len(meta))); err != nil {
		return err
	}
	sdlen := uint64(len(snapData))
	if old != nil {
		sdlen = snapValue.size
	}
	if err := addReadySize(&total, sdlen); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		var index, sz uint64
		if old != nil {
			index, sz = refs[i].index, refs[i].value.size
		} else {
			index, sz = es[i].Index, uint64(len(es[i].Data))
			if es[i].Type < raftpb.EntryNormal || es[i].Type > raftpb.EntryConfChangeV2 {
				return readyInvalid("unknown entry type")
			}
		}
		if index == 0 || (i > 0 && (last == math.MaxUint64 || index != last+1)) {
			return readyInvalid("entries are not a positive contiguous sequence")
		}
		if i == 0 {
			first = index
		}
		last = index
		if err := addReadySize(&total, 32); err != nil {
			return err
		}
		if err := addReadySize(&total, sz); err != nil {
			return err
		}
	}
	snapIndex := j.snapshot.Index
	if flags&readyHasSnapshot != 0 {
		if old != nil {
			snapIndex = old.snapshot.Index
		} else {
			snapIndex = snap.Metadata.Index
		}
	}
	if err := j.validateTransition(hs, flags, snapIndex, first, last, n); err != nil {
		return err
	}
	if old == nil && n > 0 && first <= j.hs.Commit {
		for _, e := range es {
			if e.Index > j.hs.Commit {
				break
			}
			i := sort.Search(len(j.entries), func(i int) bool { return j.entries[i].index >= e.Index })
			if i == len(j.entries) || j.entries[i].index != e.Index {
				return readyInvalid("cannot overwrite compacted committed entry")
			}
			prev := j.entries[i]
			if prev.term != e.Term || prev.typ != e.Type || prev.value.size != uint64(len(e.Data)) {
				return readyInvalid("cannot overwrite committed entry")
			}
			data, err := j.readValue(prev.value)
			if err != nil {
				return j.fail(err)
			}
			if !bytes.Equal(data, e.Data) {
				return readyInvalid("cannot replace committed payload")
			}
		}
	}
	if total > math.MaxInt64-readyHeaderSize-readyFooterSize || j.size > math.MaxInt64-int64(total)-readyHeaderSize-readyFooterSize {
		return readyInvalid("file size overflow")
	}
	if j.seq == math.MaxUint64 {
		return readyInvalid("cut sequence exhausted")
	}
	frameSize := int64(total) + readyHeaderSize + readyFooterSize
	if frameSize > j.capacity-j.size {
		if old != nil {
			return j.fail(readyInvalid("base exceeds initialized capacity"))
		}
		base, err := j.baseFrameSize()
		if err != nil {
			return err
		}
		if base > math.MaxInt64-readyCapacityHeader-frameSize {
			return readyInvalid("capacity overflow")
		}
		capacity, err := readyCapacitySize(readyCapacityHeader+base+frameSize, j.capacity, true)
		if err != nil {
			return err
		}
		if err := j.replaceCapacity(capacity); err != nil {
			return err
		}
	}
	header := &j.wire.header
	clear(header[:])
	copy(header[:8], readyMagic[:])
	binary.BigEndian.PutUint64(header[8:16], total)
	binary.BigEndian.PutUint64(header[16:24], j.seq+1)
	var key []byte
	if j.registry != nil {
		dk, err := j.registry.LatestDataKey()
		if err != nil {
			return j.fail(err)
		}
		key = dk.Data
		binary.BigEndian.PutUint64(header[24:32], dk.KeyId)
		// Fresh randomness is independent of sequence/offset, including after a torn
		// tail is discarded. Reopening cannot reuse a former cut's CTR keystream.
		if _, err = rand.Read(header[32:48]); err != nil {
			return j.fail(err)
		}
	}
	binary.BigEndian.PutUint64(header[48:56], j.ids[0])
	binary.BigEndian.PutUint64(header[56:64], j.ids[1])
	binary.BigEndian.PutUint32(header[76:], crc32.Checksum(header[:76], readyCRC))
	frame := &readyFrame{body: j.size + readyHeaderSize, key: key}
	copy(frame.nonce[:], header[32:48])
	j.dest = readyDestination{f: j.f, off: j.size}
	j.bw.Reset(&j.dest)
	write := func(p []byte) error {
		if j.testWrite != nil {
			if err := j.bw.Flush(); err != nil {
				return err
			}
			n, e := j.testWrite(p, j.dest.off)
			j.dest.off += int64(n)
			if e == nil && n != len(p) {
				e = io.ErrShortWrite
			}
			return e
		}
		_, e := j.bw.Write(p)
		return e
	}
	if err := write(header[:]); err != nil {
		return j.fail(err)
	}
	sum := uint32(0)
	var stream cipher.Stream
	if key != nil {
		block, err := aes.NewCipher(key)
		if err != nil {
			return j.fail(err)
		}
		stream = cipher.NewCTR(block, frame.nonce[:])
	}
	put := func(p []byte) error {
		for len(p) > 0 {
			take := min(len(p), len(j.scratch))
			out := p[:take]
			if stream != nil {
				stream.XORKeyStream(j.scratch[:take], out)
				out = j.scratch[:take]
			}
			sum = crc32.Update(sum, readyCRC, out)
			if err := write(out); err != nil {
				return err
			}
			p = p[take:]
		}
		return nil
	}
	state := &j.wire.state
	for i, v := range []uint64{flags, j.ids[0], j.ids[1], j.ids[2], hs.Term, hs.Vote, hs.Commit, uint64(len(meta)), sdlen, uint64(n)} {
		binary.BigEndian.PutUint64(state[i*8:], v)
	}
	if err := put(state[:]); err != nil {
		return j.fail(err)
	}
	if err := put(meta); err != nil {
		return j.fail(err)
	}
	pos := frame.body + readyStateSize + int64(len(meta))
	newSnap := readyValue{frame: frame, off: pos, size: sdlen}
	copyValue := func(v readyValue) error {
		for off := uint64(0); off < v.size; {
			take := min(uint64(len(old.scratch)), v.size-off)
			if err := old.readValuePart(v, off, old.scratch[:take]); err != nil {
				return err
			}
			if err := put(old.scratch[:take]); err != nil {
				return err
			}
			off += take
		}
		return nil
	}
	if old != nil {
		if err := copyValue(snapValue); err != nil {
			return j.fail(err)
		}
	} else if err := put(snapData); err != nil {
		return j.fail(err)
	}
	pos += int64(sdlen)
	added := make([]readyEntry, 0, n)
	for i := 0; i < n; i++ {
		var e readyEntry
		var data []byte
		if old != nil {
			e = refs[i]
		} else {
			src := es[i]
			e = readyEntry{index: src.Index, term: src.Term, typ: src.Type}
			e.value.size = uint64(len(src.Data))
			data = src.Data
		}
		eh := &j.wire.entry
		for k, v := range []uint64{e.index, e.term, uint64(e.typ), e.value.size} {
			binary.BigEndian.PutUint64(eh[k*8:], v)
		}
		if err := put(eh[:]); err != nil {
			return j.fail(err)
		}
		pos += 32
		value := readyValue{frame: frame, off: pos, size: e.value.size}
		if old != nil {
			if err := copyValue(e.value); err != nil {
				return j.fail(err)
			}
		} else if err := put(data); err != nil {
			return j.fail(err)
		}
		e.value = value
		added = append(added, e)
		pos += int64(value.size)
	}
	footer := &j.wire.footer
	clear(footer[:])
	binary.BigEndian.PutUint32(footer[:4], sum)
	copy(footer[8:], readyEnd[:])
	if err := write(footer[:]); err != nil {
		return j.fail(err)
	}
	if err := j.bw.Flush(); err != nil {
		return j.fail(err)
	}
	j.size = j.dest.off
	j.seq++
	j.tailSum = sum
	j.dirty = true
	j.persistedIDs = j.ids
	var owned raftpb.Snapshot
	if len(meta) > 0 {
		if err := owned.Metadata.Unmarshal(meta); err != nil {
			return j.fail(err)
		}
	}
	j.apply(hs, flags, added, &owned, newSnap)
	return nil
}
func (j *readyJournal) apply(hs raftpb.HardState, flags uint64, added []readyEntry, snap *raftpb.Snapshot, value readyValue) {
	if flags&readyReset != 0 {
		clear(j.entries)
		j.entries = j.entries[:0]
	}
	if flags&readyHasSnapshot != 0 {
		j.snapshot = snap.Metadata
		j.snapshotValue = value
	}
	if len(added) > 0 {
		i := sort.Search(len(j.entries), func(i int) bool { return j.entries[i].index >= added[0].index })
		clear(j.entries[i:])
		j.entries = append(j.entries[:i], added...)
	}
	if j.snapshot.Index > 0 {
		i := sort.Search(len(j.entries), func(i int) bool { return j.entries[i].index > j.snapshot.Index })
		copy(j.entries, j.entries[i:])
		clear(j.entries[len(j.entries)-i:])
		j.entries = j.entries[:len(j.entries)-i]
	}
	j.hs = hs
}
func (j *readyJournal) sync() error {
	if err := j.check(); err != nil {
		return err
	}
	if !j.dirty {
		return nil
	}
	if err := j.writePrefixWitness(); err != nil {
		return j.fail(err)
	}
	var err error
	if j.testSync != nil {
		err = j.testSync()
	} else if !j.initialized {
		err = j.stage("before_initialize_sync")
		if err == nil {
			err = j.f.Sync()
		}
	} else {
		err = syncReadyData(j.f)
	}
	if err != nil {
		return j.fail(err)
	}
	j.initialized = true
	j.dirty = false
	return nil
}
func (j *readyJournal) lastIndex() uint64 {
	if len(j.entries) > 0 {
		return max(j.snapshot.Index, j.entries[len(j.entries)-1].index)
	}
	return j.snapshot.Index
}
func (j *readyJournal) firstIndex() uint64 {
	if j.snapshot.Index > 0 {
		return j.snapshot.Index + 1
	}
	if len(j.entries) > 0 {
		return j.entries[0].index
	}
	return 1
}
func (j *readyJournal) term(index uint64) (uint64, error) {
	if index < j.snapshot.Index {
		return 0, raft.ErrCompacted
	}
	if index == j.snapshot.Index {
		return j.snapshot.Term, nil
	}
	i := sort.Search(len(j.entries), func(i int) bool { return j.entries[i].index >= index })
	if i == len(j.entries) || j.entries[i].index != index {
		return 0, raft.ErrUnavailable
	}
	return j.entries[i].term, nil
}
func (j *readyJournal) readValuePart(v readyValue, off uint64, dst []byte) error {
	if off > v.size || uint64(len(dst)) > v.size-off {
		return io.ErrUnexpectedEOF
	}
	if len(dst) == 0 {
		return nil
	}
	if _, err := j.f.ReadAt(dst, v.off+int64(off)); err != nil {
		return err
	}
	if len(v.frame.key) > 0 {
		block, err := aes.NewCipher(v.frame.key)
		if err != nil {
			return err
		}
		nonce := v.frame.nonce
		skip := uint64(v.off-v.frame.body) + off
		blocks := skip / aes.BlockSize
		for i := len(nonce) - 1; i >= 0 && blocks > 0; i-- {
			n := uint64(nonce[i]) + (blocks & 255)
			nonce[i] = byte(n)
			blocks = (blocks >> 8) + (n >> 8)
		}
		stream := cipher.NewCTR(block, nonce[:])
		var pad [aes.BlockSize]byte
		if n := skip % aes.BlockSize; n > 0 {
			stream.XORKeyStream(pad[:n], pad[:n])
		}
		stream.XORKeyStream(dst, dst)
	}
	return nil
}
func (j *readyJournal) readValue(v readyValue) ([]byte, error) {
	if v.size > uint64(math.MaxInt) {
		return nil, fmt.Errorf("Ready value too large")
	}
	b := make([]byte, int(v.size))
	if err := j.readValuePart(v, 0, b); err != nil {
		return nil, err
	}
	return b, nil
}
func (j *readyJournal) getEntries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if lo < j.firstIndex() {
		return nil, raft.ErrCompacted
	}
	if hi > j.lastIndex()+1 {
		return nil, raft.ErrUnavailable
	}
	var out []raftpb.Entry
	var used uint64
	i := sort.Search(len(j.entries), func(i int) bool { return j.entries[i].index >= lo })
	for ; i < len(j.entries) && j.entries[i].index < hi; i++ {
		e := j.entries[i]
		size := uint64(32) + e.value.size
		if len(out) > 0 && (used > maxSize || size > maxSize-used) {
			break
		}
		data, err := j.readValue(e.value)
		if err != nil {
			return nil, err
		}
		out = append(out, raftpb.Entry{Index: e.index, Term: e.term, Type: e.typ, Data: data})
		used += size
	}
	return out, nil
}
func (j *readyJournal) getSnapshot() (raftpb.Snapshot, error) {
	data, err := j.readValue(j.snapshotValue)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	meta, err := j.snapshot.Marshal()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	var snapshot raftpb.Snapshot
	snapshot.Data = data
	err = snapshot.Metadata.Unmarshal(meta)
	return snapshot, err
}

// replace publishes a complete new base generation before retiring the old one.
// It holds the original storage lock throughout; saved concurrent suffixes are
// therefore sampled once, not lost behind a certificate's older index.
func (j *readyJournal) replace() error {
	base, err := j.baseFrameSize()
	if err != nil {
		return err
	}
	if base > math.MaxInt64-readyCapacityHeader {
		return readyInvalid("capacity overflow")
	}
	capacity, err := readyCapacitySize(readyCapacityHeader+base, j.capacity, false)
	if err != nil {
		return err
	}
	return j.replaceCapacity(capacity)
}
func (j *readyJournal) replaceCapacity(capacity int64) error {
	if err := j.check(); err != nil {
		return err
	}
	next := &readyJournal{dir: j.dir, registry: j.registry, ids: j.ids, hs: j.hs, testStage: j.testStage}
	if err := next.newFile(capacity); err != nil {
		return j.fail(err)
	}
	defer func() {
		if next.f != nil {
			next.f.Close()
		}
	}()
	if err := next.appendSource(&j.hs, nil, j, j.entries, nil, true); err != nil {
		return j.fail(err)
	}
	if err := next.stage("base_written"); err != nil {
		return j.fail(err)
	}
	if err := next.sync(); err != nil {
		return j.fail(err)
	}
	if err := next.stage("base_synced"); err != nil {
		return j.fail(err)
	}
	if err := next.publishRoot(); err != nil {
		return j.fail(err)
	}
	oldFile, oldName := j.f, j.name
	j.f, j.name, j.size, j.seq = next.f, next.name, next.size, next.seq
	j.capacity, j.identity, j.initialized, j.tailSum = next.capacity, next.identity, next.initialized, next.tailSum
	j.entries, j.snapshot, j.snapshotValue = next.entries, next.snapshot, next.snapshotValue
	next.f = nil
	j.persistedIDs = next.persistedIDs
	j.dest = readyDestination{f: j.f, off: j.size}
	j.bw.Reset(&j.dest)
	j.dirty = false
	if err := oldFile.Close(); err != nil {
		return j.fail(err)
	}
	if err := j.stage("before_retire"); err != nil {
		return j.fail(err)
	}
	if err := os.Remove(filepath.Join(j.dir, oldName)); err != nil {
		return j.fail(err)
	}
	if err := syncInstallDir(j.dir); err != nil {
		return j.fail(err)
	}
	return nil
}
func (j *readyJournal) close() error {
	if j.closed {
		return j.poison
	}
	err := j.sync()
	j.closed = true
	if e := j.f.Close(); e != nil {
		err = errors.Join(err, j.fail(e))
	}
	if j.registry != nil {
		if e := j.registry.Close(); e != nil {
			err = errors.Join(err, j.fail(e))
		}
	}
	return err
}
