/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	td "github.com/snissn/gomap/TreeDB"
)

const (
	RaftPendingMaxCount  = 4096
	RaftPendingMaxCharge = 64 << 20
)

// This raw keyspace is disjoint from TreeDB's NUL+TDBMVCC external timestamp
// namespace and floor metadata. Only this owner can mutate it, inside Apply.
var raftPendingPrefix = []byte{0, 'D', 'G', 'R', 'A', 'F', 'T', 'P', 'E', 'N', 'D', 1}

type RaftPendingManifest struct {
	Version uint32
	Count   uint64
	Charge  uint64
	Binding string
}

func EmptyRaftPendingManifest() RaftPendingManifest {
	return RaftPendingManifest{Version: 1, Binding: "same-treedb-root:" + RaftPendingStateFormat}
}

// ValidateRaftPendingManifest validates the fixed-size application reference.
// Content integrity comes from the enclosing exact TreeDB root; this is not a
// second digest protocol and checkpointing never scans the pending records.
func ValidateRaftPendingManifest(state RaftPendingManifest) error {
	if state.Version != 1 || state.Binding != EmptyRaftPendingManifest().Binding ||
		state.Count > RaftPendingMaxCount || state.Charge > RaftPendingMaxCharge ||
		(state.Count == 0 && state.Charge != 0) || state.Charge < state.Count*RaftPendingRowCharge {
		return fmt.Errorf("invalid persisted pending manifest")
	}
	return nil
}
func raftPendingManifestKey() []byte { return append(append([]byte(nil), raftPendingPrefix...), 'M') }
func raftPendingKey(ts uint64) []byte {
	key := append(append([]byte(nil), raftPendingPrefix...), 'T')
	return binary.BigEndian.AppendUint64(key, ts)
}
func (s *TreeDBStore) loadRaftPendingManifest() error {
	raw, err := s.db.Get(raftPendingManifestKey())
	if err != nil {
		return err
	}
	state := EmptyRaftPendingManifest()
	if raw != nil {
		if err := json.Unmarshal(raw, &state); err != nil {
			return err
		}
	}
	if err := ValidateRaftPendingManifest(state); err != nil {
		return err
	}
	s.raftPending = state
	return nil
}

func (s *TreeDBStore) RaftPendingManifest() (RaftPendingManifest, error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return RaftPendingManifest{}, fmt.Errorf("external pending owner unavailable")
	}
	return s.raftPending, nil
}

// ErrRaftPendingCapacity is returned only by checks BEFORE raw publication. A
// caller that staged isolated canonical state can safely reject that proposal.
var ErrRaftPendingCapacity = errors.New("canonical pending state capacity reached")

func (s *TreeDBStore) CheckRaftPendingReplacement(ts uint64, data []byte) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	_, err := s.planRaftPendingReplacement(ts, data)
	return err
}
func (s *TreeDBStore) planRaftPendingReplacement(ts uint64, data []byte) (RaftPendingManifest, error) {
	if s.IsClosed() || s.external == nil {
		return RaftPendingManifest{}, fmt.Errorf("external pending owner unavailable")
	}
	actual, err := ValidateRaftPendingTxn(data)
	if err != nil {
		return RaftPendingManifest{}, err
	}
	if actual != ts {
		return RaftPendingManifest{}, fmt.Errorf("canonical pending key/start mismatch")
	}
	charge, err := RaftPendingTxnCharge(data)
	if err != nil {
		return RaftPendingManifest{}, err
	}
	key := raftPendingKey(ts)
	old, err := s.db.Get(key)
	if err != nil {
		return RaftPendingManifest{}, err
	}
	next := s.raftPending
	if old == nil {
		next.Count++
	} else {
		previous, err := RaftPendingTxnCharge(old)
		if err != nil {
			return RaftPendingManifest{}, err
		}
		if previous > next.Charge {
			return RaftPendingManifest{}, fmt.Errorf("pending manifest charge underflow")
		}
		next.Charge -= previous
	}
	if next.Count > RaftPendingMaxCount || charge > RaftPendingMaxCharge || next.Charge > RaftPendingMaxCharge-charge {
		return RaftPendingManifest{}, ErrRaftPendingCapacity
	}
	next.Charge += charge
	return next, nil
}

// ReplaceRaftPendingTxn atomically replaces one canonical record and fixed-size
// accounting in the materialization root. It never checkpoints or prunes WAL.
func (s *TreeDBStore) ReplaceRaftPendingTxn(ts uint64, data []byte) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	next, err := s.planRaftPendingReplacement(ts, data)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	err = s.external.Apply(func() error {
		batch := s.db.NewBatch()
		if batch == nil {
			return td.ErrClosed
		}
		err := batch.Set(raftPendingKey(ts), data)
		if err == nil {
			err = batch.Set(raftPendingManifestKey(), raw)
		}
		if err == nil {
			err = batch.Write()
		}
		return errors.Join(err, batch.Close())
	})
	if err == nil {
		s.raftPending = next
	}
	return err
}

// RemoveRaftPendingTxn follows the drained posting commit/abort writer. Keeping
// an obsolete record after an uncertain failure never authorizes a certificate.
func (s *TreeDBStore) RemoveRaftPendingTxn(ts uint64) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return fmt.Errorf("external pending owner unavailable")
	}
	key := raftPendingKey(ts)
	old, err := s.db.Get(key)
	if err != nil || old == nil {
		return err
	}
	charge, err := RaftPendingTxnCharge(old)
	if err != nil {
		return err
	}
	next := s.raftPending
	if next.Count == 0 || next.Charge < charge {
		return fmt.Errorf("pending manifest removal underflow")
	}
	next.Count--
	next.Charge -= charge

	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	err = s.external.Apply(func() error {
		batch := s.db.NewBatch()
		if batch == nil {
			return td.ErrClosed
		}
		err := batch.Delete(key)
		if err == nil {
			err = batch.Set(raftPendingManifestKey(), raw)
		}
		if err == nil {
			err = batch.Write()
		}
		return errors.Join(err, batch.Close())
	})
	if err == nil {
		s.raftPending = next
	}
	return err
}

// RestoreRaftPendingOverlay validates the bounded full set once during startup
// or root installation. Checkpoints read only the incremental manifest.
func (s *TreeDBStore) RestoreRaftPendingOverlay(expected RaftPendingManifest) (err error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return fmt.Errorf("external pending owner unavailable")
	}
	if err := s.loadRaftPendingManifest(); err != nil {
		return err
	}
	if expected.Version == 0 && expected.Count == 0 && expected.Charge == 0 && expected.Binding == "" {
		expected = EmptyRaftPendingManifest()
	}
	if expected != s.raftPending {
		return fmt.Errorf("certified pending manifest mismatch")
	}
	prefix := append(append([]byte(nil), raftPendingPrefix...), 'T')
	upper := append(append([]byte(nil), raftPendingPrefix...), 'U')
	it, err := s.db.Iterator(prefix, upper)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	records := make([][]byte, 0, expected.Count)
	actual := EmptyRaftPendingManifest()
	for it.Seek(prefix); it.Valid(); it.Next() {
		key, data := it.KeyCopy(nil), it.ValueCopy(nil)
		if len(key) != len(prefix)+8 {
			return fmt.Errorf("malformed pending record key")
		}
		ts, err := ValidateRaftPendingTxn(data)
		if err != nil {
			return err
		}
		if ts != binary.BigEndian.Uint64(key[len(prefix):]) {
			return fmt.Errorf("pending record key/start mismatch")
		}
		charge, err := RaftPendingTxnCharge(data)
		if err != nil {
			return err
		}
		if actual.Count >= RaftPendingMaxCount || charge > RaftPendingMaxCharge || actual.Charge > RaftPendingMaxCharge-charge {
			return fmt.Errorf("pending overlay exceeds retained-state bounds")
		}
		actual.Count++
		actual.Charge += charge
		records = append(records, data)
	}
	if err := it.Error(); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("pending records do not match certified manifest")
	}
	return RestoreRaftPendingTxns(records)
}
