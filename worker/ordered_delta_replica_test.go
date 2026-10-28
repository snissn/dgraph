/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Three real Raft replicas persist before routing messages. Replica 2 runs the
// production posting apply path live: it admits while a follower and finalizes
// after becoming leader. Other replicas' complete durable logs are replayed into
// separate stores afterwards. Process-global Oracle/State/gr prevent concurrent
// live posting replicas here; this is not a full Alpha network/server fixture.
func TestOrderedDeltaAdvancingChecksumSequenceFollowerLeadershipTransferReplay(t *testing.T) {
	dir := t.TempDir()
	oldConfig, oldState, oldBadger, oldStore, oldGroup := Config, State, pstore, postingStore, gr
	oldHard, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var current *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if current != nil {
			current.closePostingStore()
		}
		Config, State, pstore, postingStore, gr = oldConfig, oldState, oldBadger, oldStore, oldGroup
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHard, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	x.WorkerConfig.HardSync, x.Config.MaxRetries = false, 3
	attr := x.AttrInRootNamespace("ordered-delta-replicas")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}}, blockDeletes: new(sync.Mutex), closer: oldGroup.closer}
	openStore := func(root string) {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if current != nil {
			current.closePostingStore()
		}
		Config = snapshotInstallOptions(root)
		Config.PostingStoreDurability = PostingStoreDurabilityDurable
		current = &ServerState{}
		require.NoError(t, current.openPostingStore())
		State, pstore, postingStore = *current, nil, current.PostingStore
		posting.InitForStore(postingStore, 1<<20, false)
		schema.InitForStore(schemaPostingStore{store: postingStore})
		require.NoError(t, schema.LoadFromDb(context.Background()))
	}
	openStore(filepath.Join(dir, "live"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wals := make(map[uint64]*raftwal.DiskStorage)
	for id := uint64(1); id <= 3; id++ {
		path := filepath.Join(dir, fmt.Sprintf("raft-%d", id))
		require.NoError(t, os.MkdirAll(path, 0700))
		wals[id] = raftwal.Init(path)
		defer func(w *raftwal.DiskStorage) { require.NoError(t, w.Close()) }(wals[id])
	}
	n := newNode(wals[2], 1, 2, "")
	n.orderedDelta = true
	gr.Node = n
	require.True(t, n.orderedDeltaEnabled())
	peers := make(map[uint64]raft.Node)
	voters := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	for id := uint64(1); id <= 3; id++ {
		cfg := *n.Cfg
		cfg.ID, cfg.Storage = id, wals[id]
		peers[id] = raft.StartNode(&cfg, voters)
	}
	n.SetRaft(peers[2])
	type receipt struct {
		event inquiryEvent
		role  raft.StateType
	}
	var mu sync.Mutex
	var receipts []receipt
	var observe atomic.Bool
	n.deltaInquiry = &deltaInquiry{emit: func(e inquiryEvent) {
		if observe.Load() && (e.Phase == "delta_begin" || e.Phase == "oracle_end") {
			mu.Lock()
			defer mu.Unlock()
			receipts = append(receipts, receipt{e, n.Raft().Status().RaftState})
		}
	}}
	n.closer = z.NewCloser(1)
	applyDone := make(chan struct{})
	go func() { defer close(applyDone); n.processApplyCh() }()
	stopReady := make(chan struct{})
	var drivers sync.WaitGroup
	for id := uint64(1); id <= 3; id++ {
		drivers.Add(1)
		go func(id uint64) {
			defer drivers.Done()
			for {
				select {
				case rd := <-peers[id].Ready():
					if err := wals[id].Save(&rd.HardState, rd.Entries, &rd.Snapshot); err != nil {
						panic(err)
					}
					if rd.MustSync {
						if err := wals[id].Sync(); err != nil {
							panic(err)
						}
					}
					var apply []raftpb.Entry
					for _, e := range rd.CommittedEntries {
						if e.Type == raftpb.EntryConfChange {
							var cc raftpb.ConfChange
							if err := cc.Unmarshal(e.Data); err != nil {
								panic(err)
							}
							peers[id].ApplyConfChange(cc)
						}
						if id != 2 {
							continue
						}
						n.Applied.Begin(e.Index)
						if e.Type == raftpb.EntryConfChange || len(e.Data) == 0 {
							n.Applied.Done(e.Index)
							continue
						}
						n.markProposalCommitted(binary.BigEndian.Uint64(e.Data[:8]), e.Index)
						atomic.AddInt64(&n.pendingSize, int64(e.Size()))
						apply = append(apply, e)
					}
					if len(apply) > 0 {
						n.enqueueApply(apply)
					}
					for _, message := range rd.Messages {
						// Each message is routed only after its sender's durable
						// Ready persistence, exactly as required by Raft.
						_ = peers[message.To].Step(ctx, message)
					}
					peers[id].Advance()
				case <-stopReady:
					return
				}
			}
		}(id)
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(stopReady)
			for _, peer := range peers {
				peer.Stop()
			}
			drivers.Wait()
			close(n.applyCh)
			<-applyDone
		})
	}
	defer stop()
	require.Eventually(t, func() bool {
		if peers[1].Status().Lead == 1 && peers[2].Status().Lead == 1 && peers[3].Status().Lead == 1 {
			return true
		}
		_ = peers[1].Campaign(ctx)
		return false
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, raft.StateFollower, n.Raft().Status().RaftState)
	type pendingProposal struct {
		index <-chan uint64
		ack   <-chan error
	}
	key := uint64(1000)
	submit := func(p *pb.Proposal) pendingProposal {
		key++
		ack, committed := make(chan error, 1), make(chan uint64, 1)
		require.True(t, n.Proposals.Store(key, &conn.ProposalCtx{Ctx: ctx, ErrCh: ack, Committed: committed}))
		require.NoError(t, peers[1].Propose(ctx, checkpointEntry(t, key, p).Data))
		return pendingProposal{committed, ack}
	}
	awaitIndex := func(p pendingProposal) uint64 {
		select {
		case index := <-p.index:
			return index
		case <-ctx.Done():
			t.Fatal("replica 2 did not observe original committed proposal")
			return 0
		}
	}
	awaitAck := func(p pendingProposal) {
		select {
		case err := <-p.ack:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("original proposal ACK missing")
		}
	}
	propose := func(p *pb.Proposal) { pending := submit(p); awaitIndex(pending); awaitAck(pending) }
	base := posting.Oracle().MaxAssigned() + 100
	propose(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}})
	propose(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}})
	for uid := uint64(2); uid <= 4; uid++ {
		propose(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + uid, Edges: []*pb.DirectedEdge{{Entity: uid, Attr: attr, Value: []byte(fmt.Sprint(uid)), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}})
	}
	statsBefore, err := State.TreeDBStore.Stats()
	require.NoError(t, err)
	observe.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var syncs, staged atomic.Int32
	postingRoot := Config.PostingDir
	restoreHook := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !strings.HasPrefix(e.Root+string(filepath.Separator), postingRoot+string(filepath.Separator)) {
			return nil
		}
		if e.Point == durabilitycut.AfterLogicalMVCCStaging {
			staged.Add(1)
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	var restoreOnce sync.Once
	restore := func() { restoreOnce.Do(restoreHook) }
	defer restore()
	var pending []pendingProposal
	var indices []uint64
	for uid := uint64(2); uid <= 4; uid++ {
		p := submit(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + uid + 4, Txns: []*pb.TxnStatus{{StartTs: base + uid, CommitTs: base + uid + 4}}, GroupChecksums: map[uint32]uint64{1: 111, 2: 222}, ChecksumSequence: uid + 6}})
		pending = append(pending, p)
		indices = append(indices, awaitIndex(p))
		if uid == 2 {
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("follower's predecessor sync missing")
			}
		}
	}
	require.Eventually(t, func() bool { return staged.Load() == 2 }, 3*time.Second, time.Millisecond, "follower must stage B and C under held A")
	for _, p := range pending {
		select {
		case err := <-p.ack:
			t.Fatalf("ACK before durability or leadership transfer: %v", err)
		default:
		}
	}
	require.Equal(t, base, posting.Oracle().MaxAssigned())
	require.Less(t, n.Applied.DoneUntil(), indices[0])
	peers[1].TransferLeadership(ctx, 1, 2)
	require.Eventually(t, func() bool {
		return peers[1].Status().Lead == 2 && peers[2].Status().Lead == 2 && peers[3].Status().Lead == 2
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, raft.StateLeader, n.Raft().Status().RaftState)
	unblock()
	for _, p := range pending {
		awaitAck(p)
	}
	require.NoError(t, n.Applied.WaitForMark(ctx, indices[2]))
	require.Equal(t, base+8, posting.Oracle().MaxAssigned())
	require.Equal(t, int32(2), syncs.Load(), "three original follower decisions must use two native durable syncs")
	statsAfter, err := State.TreeDBStore.Stats()
	require.NoError(t, err)
	counter := func(stats map[string]string, name string) uint64 {
		value, err := strconv.ParseUint(stats[name], 10, 64)
		require.NoError(t, err)
		return value
	}
	require.Equal(t, uint64(2), counter(statsAfter, "dgraph.treedb.durable_cohorts_total")-counter(statsBefore, "dgraph.treedb.durable_cohorts_total"))
	mu.Lock()
	require.Len(t, receipts, 6)
	for i := range 3 {
		prepare, finish := receipts[i], receipts[i+3]
		require.Equal(t, "delta_begin", prepare.event.Phase)
		require.Equal(t, raft.StateFollower, prepare.role)
		require.Equal(t, indices[i], prepare.event.Index)
		require.Equal(t, "oracle_end", finish.event.Phase)
		require.Equal(t, raft.StateLeader, finish.role)
		require.Equal(t, indices[i], finish.event.Index)
		require.Equal(t, base+uint64(i)+6, finish.event.MaxAssigned)
	}
	mu.Unlock()
	require.Eventually(t, func() bool {
		for _, wal := range wals {
			hs, _, err := wal.InitialState()
			if err != nil || hs.Commit < indices[2] {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
	stop()
	restore()
	var reference []raftpb.Entry
	for id := uint64(1); id <= 3; id++ {
		entries, err := wals[id].Entries(1, indices[2]+1, ^uint64(0))
		require.NoError(t, err)
		if reference == nil {
			reference = entries
		} else {
			require.Equal(t, reference, entries, "all replicas must retain identical original committed metadata")
		}
		for _, reopening := range []bool{false, true} {
			openStore(filepath.Join(dir, fmt.Sprintf("replay-%d", id)))
			replayed := newNode(wals[id], 1, id, "")
			gr.Node = replayed
			for _, entry := range entries {
				if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
					continue
				}
				var proposal pb.Proposal
				require.NoError(t, proto.Unmarshal(entry.Data[8:], &proposal))
				proposal.Index = entry.Index
				updateStartTs(&proposal)
				require.NoError(t, replayed.applyTrackedProposal(&proposal, binary.BigEndian.Uint64(entry.Data[:8]), entry.Size()))
			}
			require.Equal(t, base+8, posting.Oracle().MaxAssigned(), "reopen=%t replica=%d", reopening, id)
			for ts := base + 6; ts <= base+8; ts++ {
				for uid := uint64(2); uid <= 4; uid++ {
					read := postingStore.NewReadTxn(ts)
					_, err := read.Get(x.DataKey(attr, uid))
					if base+uid+4 > ts {
						require.ErrorIs(t, err, badger.ErrKeyNotFound)
					} else {
						require.NoError(t, err)
						list, err := posting.GetNoStore(x.DataKey(attr, uid), ts)
						require.NoError(t, err)
						value, err := list.Value(ts)
						require.NoError(t, err)
						require.Equal(t, []byte(fmt.Sprint(uid)), value.Value)
					}
					read.Discard()
				}
			}
		}
	}
}
