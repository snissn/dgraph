/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
)

func TestServerStateTreeDBLifecycle(t *testing.T) {
	oldConfig := Config
	t.Cleanup(func() { Config = oldConfig })
	dir := t.TempDir()
	Config = Options{
		PostingDir: dir, PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier: "benchmark_minimal", PostingStoreDurability: "durable",
		PostingStoreEvents: true,
	}

	first := &ServerState{}
	require.NoError(t, first.openPostingStore())
	require.Nil(t, first.Pstore)
	require.NotNil(t, first.TreeDBStore)
	require.NotNil(t, first.CommitEvents)
	status := first.PostingStoreRuntimeStatus()
	require.Equal(t, "treedb", status["backend"])
	require.Equal(t, "benchmark_minimal", status["tier"])
	require.Equal(t, "durable", status["durability"])
	require.Equal(t, "true", status["post_commit_events"])
	require.Contains(t, status["unsupported"], "backup")
	stats, err := first.PostingStoreStats()
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.NoError(t, first.RunPostingStoreGC(context.Background()))

	txn := first.PostingStore.NewWriteTxn()
	require.NoError(t, txn.SetEntry(posting.Entry{Key: []byte("restart"), Value: []byte("value")}))
	require.NoError(t, txn.CommitAt(7, nil))
	first.closePostingStore()
	require.True(t, first.TreeDBStore.IsClosed())

	second := &ServerState{}
	require.NoError(t, second.openPostingStore())
	defer second.closePostingStore()
	read := second.PostingStore.NewReadTxn(7)
	defer read.Discard()
	item, err := read.Get([]byte("restart"))
	require.NoError(t, err)
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestServerStateTreeDBRejectsDisabledCommitEvents(t *testing.T) {
	oldConfig := Config
	t.Cleanup(func() { Config = oldConfig })
	Config = Options{
		PostingDir: t.TempDir(), PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier:       PostingStoreTierBenchmarkMinimal,
		PostingStoreDurability: PostingStoreDurabilityDurable,
		PostingStoreEventsSet:  true,
	}

	state := &ServerState{}
	err := state.openPostingStore()
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires events=true")
	require.Nil(t, state.TreeDBStore)
	require.Nil(t, state.PostingStore)
}

func TestServerStateTreeDBNormalizesRelaxedDurabilitySelection(t *testing.T) {
	oldConfig := Config
	t.Cleanup(func() { Config = oldConfig })
	Config = Options{
		PostingDir: t.TempDir(), PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier: "benchmark_minimal", PostingStoreDurability: " ReLaXeD ",
		PostingStoreEvents: true,
	}

	state := &ServerState{}
	require.NoError(t, state.openPostingStore())
	defer state.closePostingStore()
	require.Equal(t, PostingStoreDurabilityRelaxed, Config.PostingStoreDurability)
	status := state.PostingStoreRuntimeStatus()
	require.Equal(t, PostingStoreDurabilityRelaxed, status["durability"])
	require.Contains(t, status["profile"], "relaxed_sync")
	require.Equal(t, "false", status["durable_commits"])
}

func TestServerStateTreeDBRejectsInMemoryStartup(t *testing.T) {
	oldConfig, oldBadger := Config, x.WorkerConfig.Badger
	t.Cleanup(func() {
		Config, x.WorkerConfig.Badger = oldConfig, oldBadger
	})
	Config = Options{
		PostingDir: t.TempDir(), PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier:       PostingStoreTierBenchmarkMinimal,
		PostingStoreDurability: PostingStoreDurabilityDurable,
		PostingStoreEvents:     true,
	}
	x.WorkerConfig.Badger.InMemory = true

	state := &ServerState{}
	err := state.openPostingStore()
	require.Error(t, err)
	require.Contains(t, err.Error(), "in_memory_posting_store=unsupported")
	require.Nil(t, state.TreeDBStore)
	require.Nil(t, state.PostingStore)
}

func TestServerStateBadgerPreservesConfiguredSyncWrites(t *testing.T) {
	oldConfig, oldBadger := Config, x.WorkerConfig.Badger
	t.Cleanup(func() { Config, x.WorkerConfig.Badger = oldConfig, oldBadger })
	Config = Options{
		PostingDir: t.TempDir(), PostingStoreBackend: PostingStoreBackendBadger,
		PostingStoreTier: PostingStoreTierProduction, PostingStoreDurability: PostingStoreDurabilityDurable,
	}
	x.WorkerConfig.Badger = badger.DefaultOptions("").WithSyncWrites(true)

	state := &ServerState{}
	require.NoError(t, state.openPostingStore())
	defer state.closePostingStore()
	require.True(t, state.Pstore.Opts().SyncWrites)
	status := state.PostingStoreRuntimeStatus()
	require.Equal(t, "badger", status["backend"])
	require.Equal(t, "syncwrites=true", status["profile"])
	require.Equal(t, "true", status["durable_commits"])
}

func TestTreeDBRestrictedRuntimeMutationQuerySchemaRestart(t *testing.T) {
	oldConfig, oldState := Config, State
	oldPstore, oldPostingStore := pstore, postingStore
	posting.Cleanup()
	var first, second *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		if second != nil {
			second.closePostingStore()
		}
		if first != nil && !first.TreeDBStore.IsClosed() {
			first.closePostingStore()
		}
		Config, State = oldConfig, oldState
		pstore, postingStore = oldPstore, oldPostingStore
		posting.Init(oldPstore, 0, false)
		schema.Init(oldPstore)
	})

	dir := t.TempDir()
	Config = Options{
		PostingDir: dir, PostingStoreBackend: PostingStoreBackendTreeDB,
		PostingStoreTier: "benchmark_minimal", PostingStoreDurability: "durable",
		PostingStoreEvents: true,
	}
	first = &ServerState{}
	require.NoError(t, first.openPostingStore())
	State, pstore, postingStore = *first, nil, first.PostingStore
	posting.InitForStore(first.PostingStore, 0, false)
	schema.InitForStore(schemaPostingStore{store: first.PostingStore})

	predicate := x.AttrInRootNamespace("treedb-smoke")
	require.NoError(t, updateSchema(&pb.SchemaUpdate{
		Predicate: predicate, ValueType: pb.Posting_STRING,
	}, 1))
	typeName := x.AttrInRootNamespace("TreeDBSmokeType")
	wantType := &pb.TypeUpdate{
		TypeName: typeName,
		Fields:   []*pb.SchemaUpdate{{Predicate: predicate, ValueType: pb.Posting_STRING}},
	}
	require.NoError(t, updateType(typeName, wantType, 2))
	key := x.DataKey(predicate, 1)
	writer := posting.NewTxnWriterForStore(first.PostingStore)
	require.NoError(t, writer.SetAt(key, []byte("mutation"), posting.BitCompletePosting, 7))
	require.NoError(t, writer.Flush())
	read := first.PostingStore.NewReadTxn(math.MaxUint64)
	item, err := read.Get(key)
	require.NoError(t, err)
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("mutation"), value)
	read.Discard()
	posting.Cleanup()
	first.closePostingStore()

	second = &ServerState{}
	require.NoError(t, second.openPostingStore())
	State, postingStore = *second, second.PostingStore
	posting.InitForStore(second.PostingStore, 0, false)
	schema.InitForStore(schemaPostingStore{store: second.PostingStore})
	require.NoError(t, schema.LoadFromDb(context.Background()))
	gotSchema, ok := schema.State().Get(context.Background(), predicate)
	require.True(t, ok)
	require.Equal(t, pb.Posting_STRING, gotSchema.ValueType)
	gotType, ok := schema.State().GetType(typeName)
	require.True(t, ok)
	require.True(t, proto.Equal(wantType, &gotType))
	read = second.PostingStore.NewReadTxn(math.MaxUint64)
	defer read.Discard()
	item, err = read.Get(key)
	require.NoError(t, err)
	value, err = item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("mutation"), value)
}

func TestTreeDBOperationalPathsFailBeforeBadgerDereference(t *testing.T) {
	oldPstore := pstore
	pstore = nil
	t.Cleanup(func() { pstore = oldPstore })
	w := &grpcWorker{}

	_, err := w.Backup(context.Background(), &pb.BackupRequest{})
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "backup")
	_, err = w.Export(context.Background(), &pb.ExportRequest{})
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "export")
	_, err = w.Restore(context.Background(), &pb.RestoreRequest{})
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "restore")
	err = UpdateCacheMb(1)
	require.True(t, errors.Is(err, ErrPostingStoreOperationalPath))
	err = w.Subscribe(nil, nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "subscription")
	indexed := sortWithIndex(context.Background(), &pb.SortMessage{})
	require.ErrorIs(t, indexed.err, ErrPostingStoreOperationalPath)
	require.Contains(t, indexed.err.Error(), "indexed sort")
	err = w.StreamSnapshot(nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "snapshot")
	err = w.StreamExtSnapshot(nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "snapshot import")
	err = InStream(nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "snapshot import")
	err = w.ReceivePredicate(nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "predicate move receive")
	_, err = w.MovePredicate(context.Background(), nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "predicate move send")
	err = (&node{}).populateSnapshot(nil, nil)
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "Raft snapshot receive")
}

func TestTreeDBLocalBackupAndExportBypassesFailBeforeBadgerDereference(t *testing.T) {
	oldConfig, oldPstore := Config, pstore
	Config.PostingStoreBackend = PostingStoreBackendTreeDB
	pstore = nil
	t.Cleanup(func() {
		Config, pstore = oldConfig, oldPstore
	})

	_, err := BackupGroup(context.Background(), &pb.BackupRequest{
		GroupId: groups().groupId(),
	})
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "backup")

	_, err = handleExportOverNetwork(context.Background(), &pb.ExportRequest{
		GroupId: groups().groupId(),
	})
	require.ErrorIs(t, err, ErrPostingStoreOperationalPath)
	require.Contains(t, err.Error(), "export")
}
