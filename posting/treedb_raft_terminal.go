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

// RaftTerminalAdmissionLimit pauses NEW prewrites, not decisions. Pending rows
// may always become terminal, and unseen global decisions may exceed this soft
// threshold. Explicit ordered S is required to retire that semantic history.
const RaftTerminalAdmissionLimit = 65536

type RaftTxnDisposition struct{ StartTs, CommitTs uint64 }

var ErrRaftTxnResolved = errors.New("transaction already resolved")

var ErrRaftTerminalConflict = errors.New("contradictory terminal transaction disposition")

func raftTerminalPrefix() []byte { return append(append([]byte(nil), raftPendingPrefix...), 'R') }
func raftTerminalKey(ts uint64) []byte {
	return binary.BigEndian.AppendUint64(raftTerminalPrefix(), ts)
}
func decodeRaftTerminal(key, data []byte) (RaftTxnDisposition, error) {
	p := raftTerminalPrefix()
	if len(key) != len(p)+8 || len(data) != 8 {
		return RaftTxnDisposition{}, fmt.Errorf("malformed terminal transaction record")
	}
	d := RaftTxnDisposition{binary.BigEndian.Uint64(key[len(p):]), binary.BigEndian.Uint64(data)}
	if d.StartTs == 0 || (d.CommitTs != 0 && d.CommitTs <= d.StartTs) {
		return d, fmt.Errorf("invalid terminal transaction timestamp")
	}
	return d, nil
}
func (s *TreeDBStore) raftTxnDisposition(ts uint64) (uint64, bool, error) {
	data, err := s.db.Get(raftTerminalKey(ts))
	if err != nil || data == nil {
		return 0, false, err
	}
	d, err := decodeRaftTerminal(raftTerminalKey(ts), data)
	return d.CommitTs, err == nil, err
}

// RaftTxnDisposition reads one exact-root record without a process-wide map.
func (s *TreeDBStore) RaftTxnDisposition(ts uint64) (uint64, bool, error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return 0, false, td.ErrClosed
	}
	return s.raftTxnDisposition(ts)
}
func (s *TreeDBStore) validateRaftTerminalRows(expected uint64) (err error) {
	prefix := raftTerminalPrefix()
	upper := append(append([]byte(nil), raftPendingPrefix...), 'S')
	it, err := s.db.Iterator(prefix, upper)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	var count uint64
	for it.Seek(prefix); it.Valid(); it.Next() {
		if _, err := decodeRaftTerminal(it.KeyCopy(nil), it.ValueCopy(nil)); err != nil {
			return err
		}
		if count == ^uint64(0) {
			return fmt.Errorf("terminal count overflow")
		}
		count++
	}
	if err := it.Error(); err != nil {
		return err
	}
	if count != expected {
		return fmt.Errorf("certified terminal count mismatch: got %d want %d", count, expected)
	}
	return nil
}

// ResolveRaftPendingTxns follows the drained posting writer. All rows and the
// fixed manifest change in bounded owner batches before the application can certify.
// Input is bounded by one ordered Delta; no retained-history map is constructed.
func (s *TreeDBStore) ResolveRaftPendingTxns(decisions []RaftTxnDisposition) error {
	return s.resolveRaftPendingTxns(decisions, nil)
}

// beforeBatch is a private deterministic failure seam; production passes nil.
func (s *TreeDBStore) resolveRaftPendingTxns(decisions []RaftTxnDisposition, beforeBatch func(int) error) (err error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return td.ErrClosed
	}
	next := s.raftPending
	unique := make(map[uint64]uint64, len(decisions))
	pending := make(map[uint64]uint64)
	fresh := make([]RaftTxnDisposition, 0, len(decisions))
	for _, d := range decisions {
		if d.StartTs == 0 || (d.CommitTs != 0 && d.CommitTs <= d.StartTs) {
			return fmt.Errorf("invalid transaction decision")
		}
		if old, ok := unique[d.StartTs]; ok {
			if old != d.CommitTs {
				return ErrRaftTerminalConflict
			}
			continue
		}
		unique[d.StartTs] = d.CommitTs
		commit, known, err := s.raftTxnDisposition(d.StartTs)
		if err != nil {
			return err
		}
		if known {
			if commit != d.CommitTs {
				return ErrRaftTerminalConflict
			}
			continue
		}
		if next.TerminalCount == ^uint64(0) {
			return fmt.Errorf("terminal count overflow")
		}
		next.TerminalCount++
		fresh = append(fresh, d)
		old, err := s.db.Get(raftPendingKey(d.StartTs))
		if err != nil {
			return err
		}
		if old == nil {
			continue
		}
		charge, err := RaftPendingTxnCharge(old)
		if err != nil {
			return err
		}
		if next.Count == 0 || next.Charge < charge {
			return fmt.Errorf("pending decision accounting underflow")
		}
		next.Count--
		next.Charge -= charge
		pending[d.StartTs] = charge
	}
	if len(fresh) == 0 {
		return nil
	}
	started := false
	defer func() {
		if err != nil && started {
			s.closed.Store(true)
		}
	}()
	err = s.external.Apply(func() error {
		started = true
		for offset := 0; offset < len(fresh); offset += 256 {
			if beforeBatch != nil {
				if err := beforeBatch(offset); err != nil {
					return err
				}
			}
			end := offset + 256
			if end > len(fresh) {
				end = len(fresh)
			}
			working := s.raftPending
			for _, d := range fresh[offset:end] {
				working.TerminalCount++
				if charge, ok := pending[d.StartTs]; ok {
					working.Count--
					working.Charge -= charge
				}
			}
			raw, e := json.Marshal(working)
			if e != nil {
				return e
			}
			b := s.db.NewBatch()
			if b == nil {
				return td.ErrClosed
			}
			for _, d := range fresh[offset:end] {
				if e = b.Set(raftTerminalKey(d.StartTs), binary.BigEndian.AppendUint64(nil, d.CommitTs)); e != nil {
					break
				}
				if _, ok := pending[d.StartTs]; ok {
					if e = b.Delete(raftPendingKey(d.StartTs)); e != nil {
						break
					}
				}
			}
			if e == nil {
				e = b.Set(raftPendingManifestKey(), raw)
			}
			if e == nil {
				e = b.Write()
			}
			e = errors.Join(e, b.Close())
			if e != nil {
				return e
			}
			s.raftPending = working
		}
		return nil
	})
	if err == nil && s.raftPending != next {
		err = fmt.Errorf("terminal publication accounting mismatch")
	}

	return err
}

// RetireRaftTxnDispositions removes at most 256 records per batch. On ANY error
// seal admission: a partially retired working root must never certify old S.
// Recovery restores the preceding exact certificate and replays the S request.
func (s *TreeDBStore) RetireRaftTxnDispositions(floor uint64) (err error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return td.ErrClosed
	}
	defer func() {
		if err != nil {
			s.closed.Store(true)
		}
	}()
	for {
		prefix := raftTerminalPrefix()
		upper := append(append([]byte(nil), raftPendingPrefix...), 'S')
		it, e := s.db.Iterator(prefix, upper)
		if e != nil {
			return e
		}
		keys := make([][]byte, 0, 256)
		for it.Seek(prefix); it.Valid() && len(keys) < 256; it.Next() {
			key := it.KeyCopy(nil)
			d, e := decodeRaftTerminal(key, it.ValueCopy(nil))
			if e != nil {
				err = e
				break
			}
			if d.StartTs > floor {
				break
			}
			keys = append(keys, key)
		}
		err = errors.Join(err, it.Error(), it.Close())
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		next := s.raftPending
		if next.TerminalCount < uint64(len(keys)) {
			return fmt.Errorf("terminal retirement count underflow")
		}
		next.TerminalCount -= uint64(len(keys))
		raw, e := json.Marshal(next)
		if e != nil {
			return e
		}
		err = s.external.Apply(func() error {
			b := s.db.NewBatch()
			if b == nil {
				return td.ErrClosed
			}
			var e error
			for _, key := range keys {
				if e = b.Delete(key); e != nil {
					break
				}
			}
			if e == nil {
				e = b.Set(raftPendingManifestKey(), raw)
			}
			if e == nil {
				e = b.Write()
			}
			return errors.Join(e, b.Close())
		})
		if err != nil {
			return err
		}
		s.raftPending = next
	}
}
