/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	td "github.com/snissn/gomap/TreeDB"
)

// Bans are private raw keys in the same exact certified DATA root. Logical
// drops, MVCC pruning and namespace reuse never remove this authority.
var raftNamespaceBanPrefix = []byte{0, 'D', 'G', 'R', 'A', 'F', 'T', 'B', 'A', 'N', 1}
var raftNamespaceBanValue = []byte("dgraph-namespace-ban-v1")

func raftNamespaceBanKey(ns uint64) []byte {
	key := make([]byte, len(raftNamespaceBanPrefix)+8)
	copy(key, raftNamespaceBanPrefix)
	binary.BigEndian.PutUint64(key[len(raftNamespaceBanPrefix):], ns)
	return key
}

// CheckRaftNamespace returns the existing Badger banned-key sentinel. Root can
// never be banned and retains its allocation-free ordinary request fast path.
func (s *TreeDBStore) CheckRaftNamespace(ns uint64) error {
	if ns == 0 {
		return nil
	}
	if s == nil || s.IsClosed() {
		return fmt.Errorf("namespace owner unavailable")
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return fmt.Errorf("namespace owner unavailable")
	}
	value, err := s.db.Get(raftNamespaceBanKey(ns))
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	if !bytes.Equal(value, raftNamespaceBanValue) {
		return fmt.Errorf("invalid persisted namespace ban")
	}
	return fmt.Errorf("%w: namespace %d", badger.ErrBannedKey, ns)
}

// BanRaftNamespace is called only inside a logged semantic/reader barrier after
// all affected posting and schema tombstones have drained. It never checkpoints.
func (s *TreeDBStore) BanRaftNamespace(ns uint64) error {
	if ns == 0 {
		return fmt.Errorf("cannot ban root namespace")
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return fmt.Errorf("namespace owner unavailable")
	}
	return s.external.Apply(func() error {
		batch := s.db.NewBatch()
		if batch == nil {
			return td.ErrClosed
		}
		err := batch.Set(raftNamespaceBanKey(ns), raftNamespaceBanValue)
		if err == nil {
			err = batch.Write()
		}
		return errors.Join(err, batch.Close())
	})
}

// ValidateRaftNamespaceBans runs on recovered/installed roots before health.
// Checkpoints bind them through the exact root without rescanning this keyset.
func (s *TreeDBStore) ValidateRaftNamespaceBans() (err error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.external == nil {
		return fmt.Errorf("namespace owner unavailable")
	}
	upper := append([]byte(nil), raftNamespaceBanPrefix...)
	upper[len(upper)-1]++
	it, err := s.db.Iterator(raftNamespaceBanPrefix, upper)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	for it.Seek(raftNamespaceBanPrefix); it.Valid(); it.Next() {
		key := it.KeyCopy(nil)
		if len(key) != len(raftNamespaceBanPrefix)+8 || binary.BigEndian.Uint64(key[len(raftNamespaceBanPrefix):]) == 0 || !bytes.Equal(it.ValueCopy(nil), raftNamespaceBanValue) {
			return fmt.Errorf("invalid persisted namespace ban")
		}
	}
	return it.Error()
}
