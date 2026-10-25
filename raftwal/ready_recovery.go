/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func readyHeader(f *os.File, off, size int64) ([readyHeaderSize]byte, bool, error) {
	var h [readyHeaderSize]byte
	if size-off < readyHeaderSize {
		return h, true, nil
	}
	if _, err := f.ReadAt(h[:], off); err != nil {
		return h, false, err
	}
	if string(h[:8]) != string(readyMagic[:]) || binary.BigEndian.Uint32(h[76:]) != crc32.Checksum(h[:76], readyCRC) {
		return h, false, fmt.Errorf("corrupt Ready header at %d", off)
	}
	for _, b := range h[64:76] {
		if b != 0 {
			return h, false, fmt.Errorf("unknown Ready header flags")
		}
	}
	n := binary.BigEndian.Uint64(h[8:16])
	if n < readyStateSize || n > math.MaxInt64-readyHeaderSize-readyFooterSize {
		return h, false, fmt.Errorf("invalid Ready length")
	}
	return h, int64(n) > size-off-readyHeaderSize-readyFooterSize, nil
}

// readyIdentity is cold and lock-independent. It reads the actual selected root
// and complete checksummed records, including identity fields covered by each
// header CRC. Accepted-install callers already hold DiskStorage.lock; this must
// never call a locking DiskStorage accessor or create a legacy wal.meta file.
func readyIdentity(dir string) (uint64, uint64, error) {
	root, err := readReadyRoot(dir)
	if err != nil {
		return 0, 0, err
	}
	f, err := os.Open(filepath.Join(dir, root.File))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	var id, group uint64
	_, _, seq, err := scanReadyPhysical(f, func(_ int64, h [readyHeaderSize]byte) error {
		id, group = binary.BigEndian.Uint64(h[48:56]), binary.BigEndian.Uint64(h[56:64])
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if seq == 0 {
		return 0, 0, fmt.Errorf("ready root lacks complete base")
	}
	return id, group, nil
}
func (j *readyJournal) recover() error {
	p, end, seq, err := scanReadyPhysical(j.f, func(off int64, h [readyHeaderSize]byte) error {
		var err error
		n := int64(binary.BigEndian.Uint64(h[8:16]))
		frame := &readyFrame{body: off + readyHeaderSize}
		copy(frame.nonce[:], h[32:48])
		keyID := binary.BigEndian.Uint64(h[24:32])
		if keyID != 0 {
			if j.registry == nil {
				return fmt.Errorf("encrypted Ready journal requires key")
			}
			key, err := j.registry.DataKey(keyID)
			if err != nil {
				return err
			}
			frame.key = key.Data
		} else if j.registry != nil {
			return fmt.Errorf("unencrypted Ready journal cannot change encryption policy on reopen")
		}
		// The shared physical scanner authenticated the envelope before allocation.
		var r io.Reader = io.NewSectionReader(j.f, frame.body, n)
		if frame.key != nil {
			block, err := aes.NewCipher(frame.key)
			if err != nil {
				return err
			}
			r = cipher.StreamReader{S: cipher.NewCTR(block, frame.nonce[:]), R: r}
		}
		var state [readyStateSize]byte
		if _, err = io.ReadFull(r, state[:]); err != nil {
			return err
		}
		var v [10]uint64
		for i := range v {
			v[i] = binary.BigEndian.Uint64(state[i*8:])
		}
		flags := v[0]
		if flags & ^uint64(readyHasSnapshot|readyReset) != 0 || (j.seq == 0 && flags&readyReset == 0) {
			return fmt.Errorf("invalid Ready cut flags")
		}
		if v[1] != binary.BigEndian.Uint64(h[48:56]) || v[2] != binary.BigEndian.Uint64(h[56:64]) {
			return fmt.Errorf("ready identity mismatch")
		}
		remaining := uint64(n - readyStateSize)
		if v[7] > remaining || v[7] > uint64(math.MaxInt) {
			return fmt.Errorf("invalid Ready snapshot metadata size")
		}
		meta := make([]byte, int(v[7]))
		if _, err = io.ReadFull(r, meta); err != nil {
			return err
		}
		remaining -= v[7]
		if v[8] > remaining {
			return fmt.Errorf("invalid Ready snapshot size")
		}
		snapshotValue := readyValue{frame: frame, off: frame.body + readyStateSize + int64(v[7]), size: v[8]}
		if _, err = io.CopyBuffer(io.Discard, io.LimitReader(r, int64(v[8])), j.scratch[:]); err != nil {
			return err
		}
		remaining -= v[8]
		var snap raftpb.Snapshot
		if flags&readyHasSnapshot != 0 {
			if err = snap.Metadata.Unmarshal(meta); err != nil {
				return err
			}
			if raft.IsEmptySnap(snap) {
				return fmt.Errorf("empty Ready snapshot flag")
			}
		} else if v[7] != 0 || v[8] != 0 {
			return fmt.Errorf("ready snapshot bytes without flag")
		}
		if v[9] > remaining/32 || v[9] > uint64(math.MaxInt) {
			return fmt.Errorf("invalid Ready entry count")
		}
		added := make([]readyEntry, 0, int(v[9]))
		pos := snapshotValue.off + int64(v[8])
		for i := uint64(0); i < v[9]; i++ {
			if remaining < 32 {
				return fmt.Errorf("short Ready entry")
			}
			var eh [32]byte
			if _, err = io.ReadFull(r, eh[:]); err != nil {
				return err
			}
			remaining -= 32
			pos += 32
			index, term, typ, sz := binary.BigEndian.Uint64(eh[:8]), binary.BigEndian.Uint64(eh[8:16]), binary.BigEndian.Uint64(eh[16:24]), binary.BigEndian.Uint64(eh[24:])
			if index == 0 || (i > 0 && index != added[len(added)-1].index+1) || sz > remaining || typ > uint64(raftpb.EntryConfChangeV2) {
				return fmt.Errorf("invalid Ready entry geometry")
			}
			added = append(added, readyEntry{index: index, term: term, typ: raftpb.EntryType(typ), value: readyValue{frame: frame, off: pos, size: sz}})
			if _, err = io.CopyBuffer(io.Discard, io.LimitReader(r, int64(sz)), j.scratch[:]); err != nil {
				return err
			}
			remaining -= sz
			pos += int64(sz)
		}
		if remaining != 0 {
			return fmt.Errorf("extra Ready body bytes")
		}
		hs := raftpb.HardState{Term: v[4], Vote: v[5], Commit: v[6]}
		snapIndex := j.snapshot.Index
		if flags&readyHasSnapshot != 0 {
			snapIndex = snap.Metadata.Index
		}
		var first, last uint64
		if len(added) > 0 {
			first, last = added[0].index, added[len(added)-1].index
		}
		if err := j.validateTransition(hs, flags, snapIndex, first, last, len(added)); err != nil {
			return err
		}
		// Replay must enforce the same committed-prefix immutability as Save.
		if flags&readyReset == 0 {
			for _, e := range added {
				if e.index > j.hs.Commit {
					break
				}
				i := sort.Search(len(j.entries), func(i int) bool { return j.entries[i].index >= e.index })
				if i == len(j.entries) || j.entries[i].index != e.index {
					return fmt.Errorf("ready replaces compacted committed entry")
				}
				prev := j.entries[i]
				if prev.term != e.term || prev.typ != e.typ || prev.value.size != e.value.size {
					return fmt.Errorf("ready replaces committed entry")
				}
				before, err := j.readValue(prev.value)
				if err != nil {
					return err
				}
				after, err := j.readValue(e.value)
				if err != nil {
					return err
				}
				if !bytes.Equal(before, after) {
					return fmt.Errorf("ready replaces committed payload")
				}
			}
		}
		j.ids = [3]uint64{v[1], v[2], v[3]}
		j.persistedIDs = j.ids
		j.apply(hs, flags, added, &snap, snapshotValue)
		if j.hs.Commit > j.lastIndex() {
			return fmt.Errorf("ready commit exceeds retained log")
		}
		j.seq++
		return nil
	})
	if err != nil {
		return err
	}
	j.size, j.seq, j.capacity, j.identity, j.initialized = end, seq, p.capacity, p.identity, true
	var footer [readyFooterSize]byte
	if _, err = j.f.ReadAt(footer[:], end-readyFooterSize); err != nil {
		return err
	}
	j.tailSum = binary.BigEndian.Uint32(footer[:4])
	zero, err := readyRangeZero(j.f, end, p.capacity, j.scratch[:])
	if err != nil {
		return err
	}
	if !zero {
		// Only the shared verifier can authorize this positively incomplete tail.
		clear(j.scratch[:])
		for off := end; off < p.capacity; {
			n := min(int64(len(j.scratch)), p.capacity-off)
			if err := readyWriteAt(j.f, j.scratch[:n], off); err != nil {
				return err
			}
			off += n
		}
		if err := j.f.Sync(); err != nil {
			return err
		}
	}
	j.dirty = seq > p.seq
	if j.hs.Commit > j.snapshot.Index {
		if len(j.entries) == 0 || j.entries[0].index > j.snapshot.Index+1 {
			return fmt.Errorf("missing Ready replay prefix")
		}
	}
	return nil
}
