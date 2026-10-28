/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestOrderedDeltaCrashReopensUnpublishedSuccessor(t *testing.T) {
	dir := t.TempDir()
	run := func(stage string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOrderedDeltaCrashHelper$", "-test.timeout=40s")
		cmd.Env = append(os.Environ(), "DGRAPH_ORDERED_DELTA_CRASH_DIR="+dir, "DGRAPH_ORDERED_DELTA_CRASH_STAGE="+stage)
		return cmd.CombinedOutput()
	}
	out, err := run("crash")
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "%s", out)
	require.Equal(t, 86, exit.ExitCode(), "%s", out)
	raw, err := os.ReadFile(filepath.Join(dir, "ordered-delta-crash-witness.json"))
	require.NoError(t, err)
	var witness map[string]uint64
	require.NoError(t, json.Unmarshal(raw, &witness))
	require.Equal(t, uint64(5), witness["applied"])
	require.Equal(t, uint64(104), witness["oracle"])
	require.Equal(t, uint64(103), witness["pending"])
	require.Equal(t, uint64(105), witness["physical_successor"])
	require.Less(t, witness["snapshot"], uint64(4))
	require.Positive(t, witness["staged"])
	out, err = run("replay")
	require.NoError(t, err, "%s", out)
	require.Contains(t, string(out), "ORDERED_DELTA_REPLAY_COMPLETE physical=105 applied=7 oracle=120")
	out, err = run("reopen")
	require.NoError(t, err, "%s", out)
	require.Contains(t, string(out), "ORDERED_DELTA_REPLAY_COMPLETE physical=105 applied=7 oracle=120")
}

func TestOrderedDeltaCrashHelper(t *testing.T) {
	dir, stage := os.Getenv("DGRAPH_ORDERED_DELTA_CRASH_DIR"), os.Getenv("DGRAPH_ORDERED_DELTA_CRASH_STAGE")
	if stage == "" {
		return
	}
	require.NotEmpty(t, dir)
	Config = snapshotInstallOptions(dir)
	Config.PostingStoreDurability = PostingStoreDurabilityDurable
	x.WorkerConfig.HardSync = false
	attr := x.AttrInRootNamespace("ordered-delta-crash")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}}, blockDeletes: new(sync.Mutex), closer: gr.closer}
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	wal := raftwal.Init(Config.WALDir)
	defer func() { require.NoError(t, wal.Close()) }()
	state := &ServerState{}
	require.NoError(t, state.openPostingStore())
	defer state.closePostingStore()
	State, pstore, postingStore = *state, nil, state.PostingStore
	posting.InitForStore(postingStore, 1<<20, false)
	schema.InitForStore(schemaPostingStore{store: postingStore})
	require.NoError(t, schema.LoadFromDb(context.Background()))
	n := newNode(wal, 1, 1, "")
	n.orderedDelta = true
	gr.Node = n
	n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
	mutation := func(start, uid uint64) *pb.Proposal {
		return &pb.Proposal{Mutations: &pb.Mutations{StartTs: start, Edges: []*pb.DirectedEdge{{Entity: uid, Attr: attr, Value: []byte(fmt.Sprint(uid)), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}}
	}
	decision := func(start, commit uint64) *pb.Proposal {
		return &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: commit, Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}, GroupChecksums: map[uint32]uint64{1: 111}, ChecksumSequence: commit - 97}}
	}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: 100}}),
		checkpointEntry(t, 2, &pb.Proposal{Mutations: &pb.Mutations{StartTs: 101, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}}),
		checkpointEntry(t, 3, mutation(102, 2)), checkpointEntry(t, 4, mutation(103, 3)),
		checkpointEntry(t, 5, decision(102, 104)), checkpointEntry(t, 6, decision(103, 105)), checkpointEntry(t, 7, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: 120}}),
	}
	if stage == "crash" {
		require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 7}, entries, &raftpb.Snapshot{}))
		require.NoError(t, wal.Sync())
	} else {
		raw := postingStore.NewReadTxn(105)
		item, err := raw.Get(x.DataKey(attr, 3))
		require.NoError(t, err, "unpublished physical successor must survive actual process crash")
		require.Equal(t, uint64(105), item.Version())
		raw.Discard()
		entries, err = wal.Entries(1, 8, ^uint64(0))
		require.NoError(t, err)
		require.Len(t, entries, 7, "original mutation and decision records must survive crash")
	}
	for _, e := range entries[:4] {
		var p pb.Proposal
		require.NoError(t, proto.Unmarshal(e.Data[8:], &p))
		p.Index = e.Index
		updateStartTs(&p)
		require.NoError(t, n.applyTrackedProposal(&p, binary.BigEndian.Uint64(e.Data[:8]), e.Size()))
	}
	n.Applied.SetDoneUntil(4)
	var release func()
	if stage == "crash" {
		entered, unblock, staged := make(chan struct{}), make(chan struct{}), make(chan struct{}, 8)
		var once sync.Once
		release = func() { once.Do(func() { close(unblock) }) }
		defer release()
		var syncs, stages atomic.Uint64
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if !strings.HasPrefix(e.Root+string(filepath.Separator), Config.PostingDir+string(filepath.Separator)) {
				return nil
			}
			if e.Point == durabilitycut.AfterLogicalMVCCStaging {
				stages.Add(1)
				staged <- struct{}{}
			}
			if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && syncs.Add(1) == 1 {
				close(entered)
				<-unblock
			}
			return nil
		})
		defer restore()
		base := postingStore
		held := &orderedDeltaCrashCallbackStore{Store: &orderedDeltaSyncAdmissionStore{Store: base, entered: entered}, held: make(chan struct{}), release: make(chan struct{})}
		postingStore = held
		defer close(held.release)
		published := make(chan struct{})
		n.deltaInquiry = &deltaInquiry{emit: func(e inquiryEvent) {
			if e.Index == 5 && e.Phase == "oracle_end" {
				close(published)
			}
		}}
		startOrderedDeltaCrashApply(n, entries[4:])
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("A sync missing")
		}
		select {
		case <-staged:
		case <-time.After(5 * time.Second):
			t.Fatal("B did not actually stage during A sync")
		}
		release()
		select {
		case <-held.held:
		case <-time.After(5 * time.Second):
			t.Fatal("B native completion missing")
		}
		select {
		case <-published:
		case <-time.After(5 * time.Second):
			t.Fatal("A ordered publication missing")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Applied.WaitForMark(ctx, 5))
		require.Equal(t, uint64(104), posting.Oracle().MaxAssigned())
		require.Equal(t, uint64(103), posting.Oracle().MinPendingStartTs())
		cancelled, stop := context.WithCancel(context.Background())
		stop()
		require.ErrorIs(t, posting.Oracle().WaitForTs(cancelled, 105), context.Canceled)
		raw := base.NewReadTxn(105)
		item, err := raw.Get(x.DataKey(attr, 3))
		require.NoError(t, err)
		require.Equal(t, uint64(105), item.Version())
		raw.Discard()
		snap, err := n.calculateSnapshot(0, n.Applied.DoneUntil(), posting.Oracle().MinPendingStartTs())
		require.NoError(t, err)
		require.NotNil(t, snap)
		require.Less(t, snap.Index, uint64(4))
		witness, err := json.Marshal(map[string]uint64{"applied": n.Applied.DoneUntil(), "oracle": posting.Oracle().MaxAssigned(), "pending": posting.Oracle().MinPendingStartTs(), "physical_successor": 105, "snapshot": snap.Index, "staged": stages.Load()})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "ordered-delta-crash-witness.json"), witness, 0600))
		os.Exit(86) // actual process termination; no callback release, Close or future Oracle.
	}
	done := startOrderedDeltaCrashApply(n, entries[4:])
	close(n.applyCh)
	<-done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, n.Applied.WaitForMark(ctx, 7))
	require.Equal(t, uint64(120), posting.Oracle().MaxAssigned())
	require.Nil(t, posting.Oracle().GetTxn(102))
	require.Nil(t, posting.Oracle().GetTxn(103))
	for _, point := range []struct {
		ts, uid uint64
		present bool
	}{{104, 2, true}, {104, 3, false}, {105, 2, true}, {105, 3, true}} {
		txn := postingStore.NewReadTxn(point.ts)
		_, err := txn.Get(x.DataKey(attr, point.uid))
		if point.present {
			require.NoError(t, err)
			list, err := posting.GetNoStore(x.DataKey(attr, point.uid), point.ts)
			require.NoError(t, err)
			value, err := list.Value(point.ts)
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprint(point.uid)), value.Value)
		} else {
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
		}
		txn.Discard()
	}
	fmt.Fprintln(os.Stderr, "ORDERED_DELTA_REPLAY_COMPLETE physical=105 applied=7 oracle=120")
}
func startOrderedDeltaCrashApply(n *node, entries []raftpb.Entry) <-chan struct{} {
	n.closer = z.NewCloser(1)
	for _, e := range entries {
		n.Applied.Begin(e.Index)
		atomic.AddInt64(&n.pendingSize, int64(e.Size()))
	}
	done := make(chan struct{})
	go func() { n.processApplyCh(); close(done) }()
	n.enqueueApply(entries)
	return done
}

type orderedDeltaCrashCallbackStore struct {
	posting.Store
	commits       atomic.Int32
	held, release chan struct{}
}

func (s *orderedDeltaCrashCallbackStore) NewWriteTxn() posting.WriteTxn {
	return &orderedDeltaCrashCallbackTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *orderedDeltaCrashCallbackStore) SealCommitWindow() {
	if v, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		v.SealCommitWindow()
	}
}

type orderedDeltaCrashCallbackTxn struct {
	posting.WriteTxn
	owner *orderedDeltaCrashCallbackStore
}

func (t *orderedDeltaCrashCallbackTxn) CommitAt(ts uint64, cb func(error)) error {
	which := t.owner.commits.Add(1)
	return t.WriteTxn.CommitAt(ts, func(err error) {
		if which == 2 {
			close(t.owner.held)
			<-t.owner.release
		}
		cb(err)
	})
}
