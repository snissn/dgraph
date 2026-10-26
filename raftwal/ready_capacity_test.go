/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A fresh real Storage must reserve stable physical space before ordinary
// Save+required Sync. This is a physical-policy activation, not a fake loss RED.
func TestReadyCapacityStableFileSize(t *testing.T) {
	s := readyStore(t, t.TempDir())
	defer s.Close()
	before, err := s.ready.f.Stat()
	require.NoError(t, err)
	for i := uint64(1); i <= 3; i++ {
		saveReady(t, s, i, []byte("owned"))
		require.NoError(t, s.Sync())
		after, err := s.ready.f.Stat()
		require.NoError(t, err)
		require.Equal(t, before.Size(), after.Size(), "ordinary accepted Ready must not extend physical EOF")
	}
}

func TestReadyCapacityGrowthReopenAndOversize(t *testing.T) {
	dir := t.TempDir()
	s := readyStore(t, dir)
	saveReady(t, s, 1, []byte("A"))
	require.NoError(t, s.Sync())
	initial, name := s.ready.capacity, s.ready.name
	// Exact remaining capacity admits a complete frame without a namespace change.
	n := int(s.ready.capacity - s.ready.size - readyHeaderSize - readyStateSize - 32 - readyFooterSize)
	saveReady(t, s, 2, bytes.Repeat([]byte{'B'}, n))
	require.NoError(t, s.Sync())
	require.Equal(t, name, s.ready.name)
	require.Equal(t, s.ready.capacity, s.ready.size)
	saveReady(t, s, 3, []byte("C"))
	require.NoError(t, s.Sync())
	require.NotEqual(t, name, s.ready.name)
	require.Greater(t, s.ready.capacity, initial)
	// No legal input cap is inferred from the normal physical reserve budget.
	big := bytes.Repeat([]byte{'X'}, readyCapacityBudget+1)
	saveReady(t, s, 4, big)
	require.NoError(t, s.Sync())
	require.Greater(t, s.ready.capacity, int64(readyCapacityBudget))
	capacity := s.ready.capacity
	require.NoError(t, s.Close())
	s = readyStore(t, dir)
	defer s.Close()
	require.Equal(t, capacity, s.ready.capacity)
	info, err := s.ready.f.Stat()
	require.NoError(t, err)
	require.Equal(t, capacity, info.Size())
	es, err := s.Entries(1, 5, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, es, 4)
	require.Equal(t, big, es[3].Data)
}

// Large retained prefixes need amortized headroom too: a single legal large
// Ready must not make every following small Ready copy the entire generation.
func TestReadyCapacityLargeRetainedPrefixAmortizedGrowth(t *testing.T) {
	dir := t.TempDir()
	s := readyStore(t, dir)
	big := bytes.Repeat([]byte{'L'}, readyCapacityBudget+1)
	saveReady(t, s, 1, big)
	require.NoError(t, s.Sync())
	saveReady(t, s, 2, []byte("small"))
	require.NoError(t, s.Sync())
	name, capacity := s.ready.name, s.ready.capacity
	for i := uint64(3); i <= 10; i++ {
		saveReady(t, s, i, []byte("small"))
		require.NoError(t, s.Sync())
		require.Equal(t, name, s.ready.name, "retained-prefix pressure must leave geometric headroom")
		require.Equal(t, capacity, s.ready.capacity)
	}
	require.NoError(t, s.Close())
	s = readyStore(t, dir)
	defer s.Close()
	es, err := s.Entries(1, 11, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, es, 10)
	require.Equal(t, big, es[0].Data)
	for _, e := range es[1:] {
		require.Equal(t, []byte("small"), e.Data)
	}
}

func TestReadyCapacityGrowthFailurePreservesSelectedPrefix(t *testing.T) {
	dir := t.TempDir()
	s := readyStore(t, dir)
	saveReady(t, s, 1, []byte("A"))
	require.NoError(t, s.Sync())
	old := s.ready.name
	before, err := os.ReadFile(filepath.Join(dir, old))
	require.NoError(t, err)
	fault := errors.New("initialization ENOSPC")
	s.ready.testStage = func(stage string) error {
		if stage == "before_initialize_sync" {
			return fault
		}
		return nil
	}
	err = s.Save(&raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{{Index: 2, Term: 1, Data: bytes.Repeat([]byte{'B'}, readyCapacityFloor)}}, nil)
	require.ErrorIs(t, err, fault)
	require.True(t, RecoveryRequired(err))
	require.ErrorIs(t, s.Close(), fault)
	after, err := os.ReadFile(filepath.Join(dir, old))
	require.NoError(t, err)
	require.Equal(t, before, after)
	s = readyStore(t, dir)
	defer s.Close()
	hs, err := s.HardState()
	require.NoError(t, err)
	require.Equal(t, uint64(1), hs.Commit)
	files, err := filepath.Glob(filepath.Join(dir, "*.rdc"))
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestReadyCapacityWitnessReserveAndCorruption(t *testing.T) {
	for _, kind := range []string{"erased_acknowledged_frame", "newer_witness_torn_frame", "malformed_witness", "zero_witness", "contradicting_witness", "nonzero_reserve", "capacity_mismatch", "lower_witness_contradiction", "generation_identity"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			s := readyStore(t, dir)
			saveReady(t, s, 1, []byte("A"))
			require.NoError(t, s.Sync())
			off := s.ready.size
			saveReady(t, s, 2, []byte("B"))
			require.NoError(t, s.Sync())
			end := s.ready.size
			seq := s.ready.seq
			name := s.ready.name
			require.NoError(t, s.Close())
			path := filepath.Join(dir, name)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			switch kind {
			case "erased_acknowledged_frame":
				clear(raw[off:end])
			case "newer_witness_torn_frame":
				// Identical to a valid witness surviving before unACKed data. Either
				// history is refused; the older slot cannot justify rollback.
				clear(raw[end-readyFooterSize : end])
			case "malformed_witness":
				raw[readyWitnessOffsets[seq&1]+20] ^= 1
			case "zero_witness":
				clear(raw[readyWitnessOffsets[seq&1] : readyWitnessOffsets[seq&1]+readyWitnessSize])
			case "contradicting_witness":
				slot := readyWitnessOffsets[seq&1]
				w := raw[slot : slot+readyWitnessSize]
				binary.BigEndian.PutUint64(w[40:48], uint64(end-1))
				binary.BigEndian.PutUint32(w[92:], crc32.Checksum(w[:92], readyCRC))
			case "nonzero_reserve":
				raw[len(raw)-1] = 1
			case "lower_witness_contradiction":
				slot := readyWitnessOffsets[(seq&1)^1]
				w := raw[slot : slot+readyWitnessSize]
				binary.BigEndian.PutUint32(w[48:52], binary.BigEndian.Uint32(w[48:52])^1)
				binary.BigEndian.PutUint32(w[92:], crc32.Checksum(w[:92], readyCRC))
			case "generation_identity":
				// Even a self-consistent copied header belongs to its named
				// generation, never the selected path of another generation.
				raw[8] ^= 1
				binary.BigEndian.PutUint32(raw[32:36], crc32.Checksum(raw[:32], readyCRC))
				for _, slot := range readyWitnessOffsets {
					w := raw[slot : slot+readyWitnessSize]
					copy(w[8:24], raw[8:24])
					binary.BigEndian.PutUint32(w[92:], crc32.Checksum(w[:92], readyCRC))
				}
			case "capacity_mismatch":
				raw = raw[:len(raw)-1]
			}
			require.NoError(t, os.WriteFile(path, raw, 0600))
			_, _, coldErr := readyIdentity(dir)
			require.Error(t, coldErr, "cold identity must share strict prefix interpretation")
			_, err = InitCheckpointEncrypted(dir, nil)
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, after, "refused corruption must not repair")
		})
	}
}

func TestReadyCapacityBarrierAndGeometry(t *testing.T) {
	s := readyStore(t, t.TempDir())
	defer s.Close()
	require.True(t, s.ready.initialized)
	info, err := s.ready.f.Stat()
	require.NoError(t, err)
	require.Equal(t, s.ready.capacity, info.Size())
	require.NoError(t, readyCheckZeroRange(s.ready.f, s.ready.size, s.ready.capacity, make([]byte, 4096)))
	initCalls := 0
	s.ready.testStage = func(stage string) error {
		if stage == "before_initialize_sync" {
			initCalls++
		}
		return nil
	}
	syncCalls := 0
	s.ready.testSync = func() error { syncCalls++; return syncReadyData(s.ready.f) }
	saveReady(t, s, 1, []byte("A"))
	require.NoError(t, s.Sync())
	require.Equal(t, 1, syncCalls)
	require.Zero(t, initCalls)
	require.NoError(t, s.Sync())
	require.Equal(t, 1, syncCalls, "clean prefix adds no barrier")
	before := s.ready.size
	_, err = readyCapacitySize(math.MaxInt64, 0, false)
	require.True(t, PermanentStorageError(err))
	require.Equal(t, before, s.ready.size)
	// Refuse the previous experimental Ready physical format explicitly.
	dir := t.TempDir()
	r := readyRoot{Version: 1, File: strings.Repeat("0", 32) + ".rdc"}
	r.CRC = r.checksum()
	data, err := json.Marshal(r)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, readyRootName), data, 0600))
	_, err = InitCheckpointEncrypted(dir, nil)
	require.Error(t, err)
}

func TestReadyCapacityInvalidGeometryBeforeTouch(t *testing.T) {
	dir := t.TempDir()
	for _, capacity := range []int64{0, readyCapacityFloor - 1, math.MaxInt64} {
		j := &readyJournal{dir: dir}
		err := j.newFile(capacity)
		require.True(t, PermanentStorageError(err))
		require.Nil(t, j.f)
		files, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, files)
	}
}
