/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/x"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestRaftSchemaTransitionDrainsRequestsAndFencesPrivateHandles(t *testing.T) {
	s := openRaftTransferStore(t, "schema-drain")
	release, err := s.PinRaftReadTimestamp(10)
	require.NoError(t, err)
	t.Cleanup(release)
	done := make(chan *RaftSchemaTransition, 1)
	go func() {
		cap, err := s.BeginRaftSchemaTransition(context.Background())
		if err != nil {
			t.Error(err)
		}
		done <- cap
	}()
	require.Eventually(t, func() bool { return s.CheckRaftReadTimestamp(10) != nil }, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("schema drain returned before the admitted request exited")
	default:
	}
	release()
	var cap *RaftSchemaTransition
	select {
	case cap = <-done:
	case <-time.After(time.Second):
		t.Fatal("schema drain did not progress after request release")
	}
	require.NotNil(t, cap)
	read := cap.Store().NewReadTxn(10)
	stale := cap.Store().NewWriteTxn()
	require.NoError(t, stale.SetEntry(Entry{Key: x.DataKey(x.AttrInRootNamespace("stale-schema"), 1), Value: []byte("old")}))
	require.NoError(t, cap.Complete())
	cap.Close()
	cap.Close()
	_, err = read.Get([]byte("missing"))
	require.ErrorIs(t, err, td.ErrClosed)
	require.Error(t, stale.CommitAt(11, nil))
	stale.Discard()
	require.NoError(t, s.CheckRaftReadTimestamp(10))
	require.False(t, s.IsClosed())
	physical, err := s.DiscardFloor()
	require.NoError(t, err)
	require.Zero(t, physical, "schema drainage grants no physical history pruning")
}

func TestRaftSchemaTransitionIncompletePassSealsAdmission(t *testing.T) {
	s := openRaftTransferStore(t, "schema-incomplete")
	cap, err := s.BeginRaftSchemaTransition(context.Background())
	require.NoError(t, err)
	cap.Close()
	require.True(t, s.IsClosed())
	require.Error(t, s.CheckRaftReadTimestamp(10))
}
