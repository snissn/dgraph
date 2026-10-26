/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
package conn

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type heldReadySync struct {
	store            *raftwal.DiskStorage
	entered, release chan struct{}
	calls            atomic.Int32
}

func (h *heldReadySync) Sync() error {
	h.calls.Add(1)
	close(h.entered)
	<-h.release
	return h.store.Sync()
}

// This exercises the shared synchronous production barrier called by Alpha Run
// and Zero before apply/Advance. It is not a full Run/network/Oracle test. The
// raftwal exact-file fault control separately establishes EIO poisoning.
func TestReadyCutRequiredSyncCallerBoundary(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "terminal"}[terminal], func(t *testing.T) {
			store, err := raftwal.InitCheckpointEncrypted(t.TempDir(), nil)
			require.NoError(t, err)
			require.NoError(t, store.Save(&raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("committed")}}, nil))
			gate := &heldReadySync{store: store, entered: make(chan struct{}), release: make(chan struct{})}
			var downstream atomic.Bool
			done := make(chan error, 1)
			go func() {
				err := SyncToStorage(gate)
				if err == nil {
					downstream.Store(true)
				}
				done <- err
			}()
			<-gate.entered
			require.False(t, downstream.Load())
			select {
			case <-done:
				t.Fatal("required barrier returned while held")
			default:
			}
			if terminal {
				require.NoError(t, store.Close())
			}
			close(gate.release)
			err = <-done
			require.Equal(t, int32(1), gate.calls.Load())
			if terminal {
				require.True(t, raftwal.PermanentStorageError(err))
				require.ErrorIs(t, err, os.ErrClosed)
				require.False(t, downstream.Load())
			} else {
				require.NoError(t, err)
				require.True(t, downstream.Load())
				require.NoError(t, store.Close())
			}
		})
	}
}

type legacySyncFailures struct {
	calls int
	err   error
}

func (s *legacySyncFailures) Sync() error {
	s.calls++
	if s.calls < 3 {
		return s.err
	}
	return nil
}
func TestReadyCutRequiredSyncRetainsLegacyRetry(t *testing.T) {
	s := &legacySyncFailures{err: errors.New("legacy transient")}
	require.NoError(t, SyncToStorage(s))
	require.Equal(t, 3, s.calls)
}
