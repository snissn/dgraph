/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	readyCapacityHeader = 4096
	readyWitnessSize    = 96
	readyCapacityFloor  = 64 << 10
	readyCapacityBudget = 16 << 20
)

var readyCapacityMagic = [8]byte{'D', 'G', 'R', 'C', 'A', 'P', '0', '2'}
var readyWitnessMagic = [8]byte{'D', 'G', 'R', 'B', 'N', 'D', '0', '2'}
var readyWitnessOffsets = [2]int64{128, 256}

type readyBound struct {
	seq uint64
	end int64
	sum uint32
}

type readyPhysical struct {
	capacity int64
	identity [16]byte
	end      int64
	seq      uint64
	sum      uint32
	bounds   [2]readyBound
}

func readyCapacitySize(required, previous int64, pressure bool) (int64, error) {
	if required < readyCapacityHeader || required > math.MaxInt64-63 {
		return 0, readyInvalid("capacity overflow")
	}
	// Certificate replacement may shrink an oversized generation back to the
	// normal reserve. Capacity pressure must instead be amortized even when the
	// retained live prefix is larger than that reserve budget. Cap doubling by
	// required before saturating, so compaction does not retain an enormous old
	// file solely for headroom. A giant single incoming Ready remains legal.
	normal := max(int64(readyCapacityFloor), min(previous, int64(readyCapacityBudget)))
	if pressure {
		const largest = int64(math.MaxInt64 &^ 63)
		base := min(previous, required)
		if base > largest/2 {
			normal = largest
		} else {
			normal = max(int64(readyCapacityFloor), base*2)
		}
	}
	return (max(required, normal) + 63) &^ 63, nil
}
func readyWriteAt(f *os.File, p []byte, off int64) error {
	n, err := f.WriteAt(p, off)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return err
}
func putReadyWitness(dst []byte, p readyPhysical) {
	clear(dst)
	copy(dst, readyWitnessMagic[:])
	copy(dst[8:24], p.identity[:])
	binary.BigEndian.PutUint64(dst[24:32], uint64(p.capacity))
	binary.BigEndian.PutUint64(dst[32:40], p.seq)
	binary.BigEndian.PutUint64(dst[40:48], uint64(p.end))
	binary.BigEndian.PutUint32(dst[48:52], p.sum)
	binary.BigEndian.PutUint32(dst[92:96], crc32.Checksum(dst[:92], readyCRC))
}
func readyZero(p []byte) bool { return bytes.Count(p, []byte{0}) == len(p) }

// initializeCapacity writes only the unpublished generation. First sync uses
// full file persistence, covering every initialized byte before root publication.
func (j *readyJournal) initializeCapacity(capacity int64) error {
	checked, err := readyCapacitySize(capacity, 0, false)
	if err != nil || checked != capacity {
		return readyInvalid("invalid physical capacity")
	}
	j.capacity = capacity
	var header [readyCapacityHeader]byte
	copy(header[:8], readyCapacityMagic[:])
	copy(header[8:24], j.identity[:])
	binary.BigEndian.PutUint64(header[24:32], uint64(capacity))
	binary.BigEndian.PutUint32(header[32:36], crc32.Checksum(header[:32], readyCRC))
	p := readyPhysical{capacity: capacity, identity: j.identity, end: readyCapacityHeader}
	for _, off := range readyWitnessOffsets {
		putReadyWitness(header[off:off+readyWitnessSize], p)
	}
	if err := readyWriteAt(j.f, header[:], 0); err != nil {
		return err
	}
	clear(j.scratch[:])
	for off := int64(readyCapacityHeader); off < capacity; {
		n := min(int64(len(j.scratch)), capacity-off)
		if err := readyWriteAt(j.f, j.scratch[:n], off); err != nil {
			return err
		}
		off += n
	}
	j.size = readyCapacityHeader
	j.dest = readyDestination{f: j.f, off: j.size}
	return nil
}
func readReadyPhysical(f *os.File) (readyPhysical, error) {
	var p readyPhysical
	info, err := f.Stat()
	if err != nil {
		return p, err
	}
	var h [readyCapacityHeader]byte
	if _, err = f.ReadAt(h[:], 0); err != nil {
		return p, err
	}
	if !bytes.Equal(h[:8], readyCapacityMagic[:]) || crc32.Checksum(h[:32], readyCRC) != binary.BigEndian.Uint32(h[32:36]) {
		return p, fmt.Errorf("corrupt Ready capacity header")
	}
	p.capacity = int64(binary.BigEndian.Uint64(h[24:32]))
	copy(p.identity[:], h[8:24])
	if filepath.Base(f.Name()) != hex.EncodeToString(p.identity[:])+".rdc" {
		return p, fmt.Errorf("ready generation identity mismatch")
	}
	if p.capacity != info.Size() || p.capacity < readyCapacityFloor || p.capacity&63 != 0 {
		return p, fmt.Errorf("invalid Ready physical capacity")
	}
	// All non-witness reserved header bytes remain canonical.
	if !readyZero(h[36:128]) || !readyZero(h[224:256]) || !readyZero(h[352:]) {
		return p, fmt.Errorf("corrupt Ready reserved header")
	}
	var previous readyPhysical
	for slot, off := range readyWitnessOffsets {
		w := h[off : off+readyWitnessSize]
		seq, end := binary.BigEndian.Uint64(w[32:40]), int64(binary.BigEndian.Uint64(w[40:48]))
		sum := binary.BigEndian.Uint32(w[48:52])
		p.bounds[slot] = readyBound{seq: seq, end: end, sum: sum}
		if !bytes.Equal(w[:8], readyWitnessMagic[:]) || !bytes.Equal(w[8:24], p.identity[:]) || binary.BigEndian.Uint64(w[24:32]) != uint64(p.capacity) || !readyZero(w[52:92]) || crc32.Checksum(w[:92], readyCRC) != binary.BigEndian.Uint32(w[92:]) || end < readyCapacityHeader || end > p.capacity || (seq == 0 && (end != readyCapacityHeader || sum != 0)) || (seq != 0 && end == readyCapacityHeader) {
			return p, fmt.Errorf("corrupt Ready prefix witness")
		}
		if previous.end != 0 && ((seq > previous.seq && end <= previous.end) || (seq < previous.seq && end >= previous.end)) {
			return p, fmt.Errorf("conflicting Ready prefix order")
		}
		previous = readyPhysical{seq: seq, end: end}
		if seq == p.seq && p.end != 0 && (end != p.end || sum != p.sum) {
			return p, fmt.Errorf("conflicting Ready prefix witnesses")
		}
		if seq > p.seq || p.end == 0 {
			p.seq, p.end, p.sum = seq, end, sum
		}
	}
	if p.seq == 0 {
		return p, fmt.Errorf("ready generation lacks witnessed base")
	}
	return p, nil
}

// scanReadyPhysical is the sole fixed-prefix/reserve interpretation used by
// recovery and cold accepted-install identity reads. visit sees only complete,
// checksummed frames; mandatory prefix failures never fall back to an old slot.
func scanReadyPhysical(f *os.File, visit func(int64, [readyHeaderSize]byte) error) (readyPhysical, int64, uint64, error) {
	p, err := readReadyPhysical(f)
	if err != nil {
		return p, 0, 0, err
	}
	var scratch [readyScratchSize]byte
	off, seq := int64(readyCapacityHeader), uint64(0)
	mandatorySeen := false
	for off < p.capacity {
		var h [readyHeaderSize]byte
		take := min(int64(len(h)), p.capacity-off)
		if _, err = f.ReadAt(h[:take], off); err != nil {
			return p, 0, 0, err
		}
		if readyZero(h[:take]) {
			if off < p.end {
				return p, 0, 0, fmt.Errorf("zero frame inside Ready witnessed prefix")
			}
			if err = readyCheckZeroRange(f, off, p.capacity, scratch[:]); err != nil {
				return p, 0, 0, err
			}
			break
		}
		if p.capacity-off < readyHeaderSize+readyStateSize+readyFooterSize {
			return p, 0, 0, fmt.Errorf("nonzero unexplained Ready reserve at %d", off)
		}
		// A short prefix of the magic followed entirely by reserve is a positively
		// incomplete first header write, never an excuse inside the mandatory cut.
		prefix := 0
		for prefix < 8 && h[prefix] == readyMagic[prefix] {
			prefix++
		}
		if off >= p.end && prefix > 0 && prefix < 8 && readyZero(h[prefix:take]) {
			if err = readyCheckZeroRange(f, off+int64(prefix), p.capacity, scratch[:]); err != nil {
				return p, 0, 0, err
			}
			break
		}
		header, partial, e := readyHeader(f, off, p.capacity)
		if e != nil {
			return p, 0, 0, e
		}
		if partial {
			return p, 0, 0, fmt.Errorf("ready frame exceeds initialized capacity")
		}
		n := int64(binary.BigEndian.Uint64(header[8:16]))
		next := binary.BigEndian.Uint64(header[16:24])
		if seq == math.MaxUint64 || next != seq+1 {
			return p, 0, 0, fmt.Errorf("Ready sequence discontinuity")
		}
		end := off + readyHeaderSize + n + readyFooterSize
		hash := crc32.New(readyCRC)
		if _, err = io.CopyBuffer(hash, io.NewSectionReader(f, off+readyHeaderSize, n), scratch[:]); err != nil {
			return p, 0, 0, err
		}
		var footer [readyFooterSize]byte
		if _, err = f.ReadAt(footer[:], end-readyFooterSize); err != nil {
			return p, 0, 0, err
		}
		sum := hash.Sum32()
		if !bytes.Equal(footer[8:], readyEnd[:]) || binary.BigEndian.Uint32(footer[:4]) != sum || binary.BigEndian.Uint32(footer[4:8]) != 0 {
			if off >= p.end && readyZero(footer[:]) {
				if err = readyCheckZeroRange(f, end, p.capacity, scratch[:]); err != nil {
					return p, 0, 0, err
				}
				break
			}
			return p, 0, 0, fmt.Errorf("corrupt Ready footer at %d", end-readyFooterSize)
		}
		for _, bound := range p.bounds {
			if end > bound.end && off < bound.end {
				return p, 0, 0, fmt.Errorf("Ready witness is not a frame boundary")
			}
			if end == bound.end && (next != bound.seq || sum != bound.sum) {
				return p, 0, 0, fmt.Errorf("Ready witness contradicts frame")
			}
		}
		if end == p.end {
			mandatorySeen = true
		}
		if visit != nil {
			if err = visit(off, header); err != nil {
				return p, 0, 0, err
			}
		}
		off, seq = end, next
	}
	if !mandatorySeen || seq == 0 {
		return p, 0, 0, fmt.Errorf("Ready witnessed prefix is missing")
	}
	return p, off, seq, nil
}
func readyRangeZero(f *os.File, start, end int64, scratch []byte) (bool, error) {
	for start < end {
		n := min(int64(len(scratch)), end-start)
		if _, err := f.ReadAt(scratch[:n], start); err != nil {
			return false, err
		}
		if !readyZero(scratch[:n]) {
			return false, nil
		}
		start += n
	}
	return true, nil
}
func readyCheckZeroRange(f *os.File, start, end int64, scratch []byte) error {
	zero, err := readyRangeZero(f, start, end, scratch)
	if err != nil {
		return err
	}
	if !zero {
		return fmt.Errorf("nonzero unexplained Ready reserve at %d", start)
	}
	return nil
}

func (j *readyJournal) writePrefixWitness() error {
	putReadyWitness(j.wire.witness[:], readyPhysical{capacity: j.capacity, identity: j.identity, end: j.size, seq: j.seq, sum: j.tailSum})
	return readyWriteAt(j.f, j.wire.witness[:], readyWitnessOffsets[j.seq&1])
}
func (j *readyJournal) baseFrameSize() (int64, error) {
	total := uint64(readyHeaderSize + readyStateSize + readyFooterSize)
	if j.snapshot.Index != 0 {
		if err := addReadySize(&total, uint64(j.snapshot.Size())); err != nil {
			return 0, err
		}
	}
	if err := addReadySize(&total, j.snapshotValue.size); err != nil {
		return 0, err
	}
	for _, e := range j.entries {
		if err := addReadySize(&total, 32); err != nil {
			return 0, err
		}
		if err := addReadySize(&total, e.value.size); err != nil {
			return 0, err
		}
	}
	return int64(total), nil
}
