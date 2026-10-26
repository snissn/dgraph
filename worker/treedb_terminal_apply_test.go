/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The first two-entry native chunk is accepted. Its callback stays owned until
// the test releases it, then reports a second cause. The next native preparation
// rejects an oversized value; no callback, prefix or timestamp is resubmitted.
type terminalApplyStore struct {
	posting.Store
	held, release chan struct{}
	commits       atomic.Int32
	callbacks     atomic.Int32
	failure       error
}

func (s *terminalApplyStore) NewWriteTxn() posting.WriteTxn {
	return &terminalApplyTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}

func (s *terminalApplyStore) SealCommitWindow() {
	if sealer, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		sealer.SealCommitWindow()
	}
}

type terminalApplyTxn struct {
	posting.WriteTxn
	owner *terminalApplyStore
	count int
}

func (t *terminalApplyTxn) SetEntry(entry posting.Entry) error {
	if t.count == 2 {
		return badger.ErrTxnTooBig
	}
	if t.owner.commits.Load() != 0 {
		entry.Value = make([]byte, 8<<20)
	}
	if err := t.WriteTxn.SetEntry(entry); err != nil {
		return err
	}
	t.count++
	return nil
}

func (t *terminalApplyTxn) CommitAt(ts uint64, cb func(error)) error {
	t.owner.commits.Add(1)
	return t.WriteTxn.CommitAt(ts, func(err error) {
		close(t.owner.held)
		<-t.owner.release
		t.owner.callbacks.Add(1)
		cb(errors.Join(err, t.owner.failure))
	})
}

func TestTreeDBTerminalDecisionStopsApply(t *testing.T) {
	for _, mode := range []string{PostingStoreDurabilityDurable, PostingStoreDurabilityRelaxed} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			run := func(stage string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTreeDBTerminalDecisionApplyHelper$", "-test.timeout=40s")
				cmd.Env = append(os.Environ(), "DGRAPH_TERMINAL_APPLY_DIR="+dir,
					"DGRAPH_TERMINAL_APPLY_MODE="+mode, "DGRAPH_TERMINAL_APPLY_STAGE="+stage)
				return cmd.CombinedOutput()
			}
			output, err := run("failure")
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, "%s", output)
			require.Equal(t, 86, exit.ExitCode(), "%s", output)
			require.Contains(t, string(output), "treedb decision materialization failed at Raft index 6")
			witness, err := os.ReadFile(filepath.Join(dir, "terminal-witness.json"))
			require.NoError(t, err)
			var got map[string]uint64
			require.NoError(t, json.Unmarshal(witness, &got))
			require.Equal(t, map[string]uint64{"applied": 5, "commits": 1, "callbacks": 1, "prefix_keys": 2}, got)
			output, err = run("replay")
			require.NoError(t, err, "%s", output)
			require.Contains(t, string(output), "TERMINAL_REPLAY_COMPLETE keys=6 version=106")
		})
	}
}

func TestTreeDBTerminalDecisionApplyHelper(t *testing.T) {
	dir, mode, stage := os.Getenv("DGRAPH_TERMINAL_APPLY_DIR"), os.Getenv("DGRAPH_TERMINAL_APPLY_MODE"), os.Getenv("DGRAPH_TERMINAL_APPLY_STAGE")
	if stage == "" {
		return
	}
	require.NotEmpty(t, dir)
	require.Contains(t, []string{PostingStoreDurabilityDurable, PostingStoreDurabilityRelaxed}, mode)
	Config = snapshotInstallOptions(dir)
	Config.PostingStoreDurability = mode
	x.WorkerConfig.HardSync, x.Config.MaxRetries = false, 3
	require.False(t, raftCheckpointEnabled())
	attr := x.AttrInRootNamespace("terminal-apply")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}},
		blockDeletes: new(sync.Mutex), closer: gr.closer}
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
	gr.Node = n
	n.SetConfState(&raftpb.ConfState{Voters: []uint64{1}})
	const base uint64 = 100
	mutation := func(ts uint64, value string) *pb.Proposal {
		m := &pb.Mutations{StartTs: ts}
		for uid := uint64(1); uid <= 6; uid++ {
			m.Edges = append(m.Edges, &pb.DirectedEdge{Entity: uid, Attr: attr,
				Value: []byte(value), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
		}
		return &pb.Proposal{Mutations: m}
	}
	decision := func(start, commit, max uint64) *pb.Proposal {
		return &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: max,
			Txns: []*pb.TxnStatus{{StartTs: start, CommitTs: commit}}}}
	}
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}}),
		checkpointEntry(t, 2, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1,
			Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}}),
		checkpointEntry(t, 3, mutation(base+2, "before")),
		checkpointEntry(t, 4, decision(base+2, base+3, base+4)),
		checkpointEntry(t, 5, mutation(base+5, "after")),
		checkpointEntry(t, 6, decision(base+5, base+6, base+7)),
		checkpointEntry(t, 7, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 20}}),
	}
	apply := func(entry raftpb.Entry) {
		var p pb.Proposal
		require.NoError(t, proto.Unmarshal(entry.Data[8:], &p))
		p.Index = entry.Index
		updateStartTs(&p)
		require.NoError(t, n.applyTrackedProposal(&p, binary.BigEndian.Uint64(entry.Data[:8]), entry.Size()))
	}
	if stage == "replay" {
		retained, err := wal.Entries(1, 8, ^uint64(0))
		require.NoError(t, err)
		require.Len(t, retained, 7)
		for _, entry := range retained {
			apply(entry)
		}
		require.Equal(t, base+20, posting.Oracle().MaxAssigned())
		read := postingStore.NewReadTxn(base + 6)
		defer read.Discard()
		for uid := uint64(1); uid <= 6; uid++ {
			item, err := read.Get(x.DataKey(attr, uid))
			require.NoError(t, err)
			require.Equal(t, base+6, item.Version())
			list, err := posting.MemLayerInstance.ReadData(x.DataKey(attr, uid), postingStore, base+6, false)
			require.NoError(t, err)
			value, err := list.Value(base + 6)
			require.NoError(t, err)
			require.Equal(t, []byte("after"), value.Value)
		}
		fmt.Fprintln(os.Stderr, "TERMINAL_REPLAY_COMPLETE keys=6 version=106")
		return
	}
	require.Equal(t, "failure", stage)
	require.NoError(t, wal.Save(&raftpb.HardState{Term: 1, Commit: 7}, entries, &raftpb.Snapshot{}))
	require.NoError(t, wal.Sync())
	for _, entry := range entries[:5] {
		apply(entry)
	}
	n.Applied.SetDoneUntil(5)
	failure := errors.New("terminal apply callback cause")
	wrapped := &terminalApplyStore{Store: postingStore, held: make(chan struct{}), release: make(chan struct{}), failure: failure}
	postingStore = wrapped
	n.closer = z.NewCloser(1)
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		n.processApplyCh()
	}()
	for _, entry := range entries[5:] {
		n.Applied.Begin(entry.Index)
		atomic.AddInt64(&n.pendingSize, int64(entry.Size()))
	}
	n.applyCh <- entries[5:]
	close(n.applyCh)
	<-wrapped.held
	select {
	case result := <-done:
		t.Fatalf("apply terminated before its accepted callback drained: %v", result)
	default:
	}
	require.Zero(t, wrapped.callbacks.Load())
	close(wrapped.release)
	result := <-done
	err, ok := result.(error)
	require.True(t, ok, "ordinary apply continued past terminal failure: applied=%d maxAssigned=%d", n.Applied.DoneUntil(), posting.Oracle().MaxAssigned())
	require.ErrorIs(t, err, posting.ErrTreeDBEntryTooLarge)
	require.ErrorIs(t, err, failure)
	require.Equal(t, uint64(5), n.Applied.DoneUntil())
	require.Equal(t, base+4, posting.Oracle().MaxAssigned())
	require.NotNil(t, posting.Oracle().GetTxn(base+5))
	require.NotContains(t, n.appliedProposals, uint64(7))
	require.Equal(t, int32(1), wrapped.commits.Load())
	require.Equal(t, int32(1), wrapped.callbacks.Load())
	read := wrapped.Store.NewReadTxn(base + 6)
	prefix := uint64(0)
	for uid := uint64(1); uid <= 6; uid++ {
		item, err := read.Get(x.DataKey(attr, uid))
		require.NoError(t, err)
		if item.Version() == base+6 {
			prefix++
		}
	}
	read.Discard()
	require.Equal(t, uint64(2), prefix)
	witness, err := json.Marshal(map[string]uint64{"applied": n.Applied.DoneUntil(),
		"commits": uint64(wrapped.commits.Load()), "callbacks": uint64(wrapped.callbacks.Load()), "prefix_keys": prefix})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "terminal-witness.json"), witness, 0600))
	fmt.Fprintln(os.Stderr, result)
	os.Exit(86) // Test-only fatal witness; no Close or subsequent Oracle entry.
}
