/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestRaftTaskRejectsExpiredTimestampBeforeOracleAndCaches(t *testing.T) {
	oldConfig, oldState, oldNode := Config, State, gr.Node
	t.Cleanup(func() { Config, State, gr.Node = oldConfig, oldState, oldNode })
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	store, err := posting.OpenRaftTreeDBStore(opts, "task-admission-test")
	require.NoError(t, err)
	defer store.Close()
	State = ServerState{TreeDBStore: store, PostingStore: store}
	Config = Options{PostingStoreBackend: PostingStoreBackendTreeDB, PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint}
	require.NoError(t, store.AdvanceRaftSemanticFloor(10))
	// A stale task must fail before even the node/span lookup, Oracle wait or
	// transaction cache admission. No initialized Raft node is needed to reject.
	gr.Node = nil
	for _, ts := range []uint64{0, 9, 10} {
		_, err := processTask(context.Background(), &pb.Query{ReadTs: ts, Cache: UseTxnCache}, 1)
		require.ErrorContains(t, err, "replicated posting floor")
	}
}

func TestRaftFutureOracleWaitDoesNotBlockMaintenanceAdmission(t *testing.T) {
	oldConfig, oldState, oldNode := Config, State, gr.Node
	t.Cleanup(func() { Config, State, gr.Node = oldConfig, oldState, oldNode })
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	store, err := posting.OpenRaftTreeDBStore(opts, "future-task-admission-test")
	require.NoError(t, err)
	defer store.Close()
	State = ServerState{TreeDBStore: store, PostingStore: store}
	Config = Options{PostingStoreBackend: PostingStoreBackendTreeDB, PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint}
	gr.Node = &node{Node: &conn.Node{Id: 1}}
	require.NoError(t, store.CheckpointRaft(context.Background(), []byte("before-waiter")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := &oracleWaitContext{Context: ctx, entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := processTask(waiting, &pb.Query{ReadTs: posting.Oracle().MaxAssigned() + 100, Cache: UseTxnCache}, 1)
		done <- err
	}()
	select {
	case <-waiting.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not reach future Oracle wait")
	}
	maintenanceCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	cap, err := store.BeginRaftPostingMaintenance(maintenanceCtx, 0)
	require.NoError(t, err, "an Oracle waiter must not retain a posting dependency pin")
	cap.Close()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Oracle waiter did not exit")
	}
}

type oracleWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *oracleWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}
