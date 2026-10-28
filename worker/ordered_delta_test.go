/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type orderedDeltaZero struct {
	pb.UnimplementedZeroServer
	deltas    chan *pb.OracleDelta
	connected chan struct{}
}

func (s *orderedDeltaZero) Oracle(_ *api.Payload, stream pb.Zero_OracleServer) error {
	close(s.connected)
	for {
		select {
		case delta := <-s.deltas:
			if err := stream.Send(delta); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// The fixture uses a real Zero gRPC stream, production proposal registration and
// waits, a real single-node Raft log/Ready, the ordinary apply owner and native
// durable TreeDB. Its Ready driver owns persistence and committed-entry transfer;
// it does not model a full Alpha network/health/recovery server.
func TestOrderedDeltaRealStreamSharesNativeSuccessor(t *testing.T) {
	testOrderedDeltaRealStreamSharesNativeSuccessor(t, false, false)
}
func TestOrderedDeltaRealStreamPreservesMultiTransactionAggregate(t *testing.T) {
	testOrderedDeltaRealStreamSharesNativeSuccessor(t, true, false)
}

// Three original proposals retain independent ACKs while B and C share a real
// successor cohort, even though every Zero source sequence advances.
func TestOrderedDeltaRealStreamAdvancingChecksumSequenceSharesNativeSuccessor(t *testing.T) {
	testOrderedDeltaRealStreamSharesNativeSuccessor(t, false, true)
}
func TestOrderedDeltaRealStreamAdvancingChecksumSequencePreservesAggregate(t *testing.T) {
	testOrderedDeltaRealStreamSharesNativeSuccessor(t, true, true)
}

func testOrderedDeltaRealStreamSharesNativeSuccessor(t *testing.T, multi, advancingSequence bool) {
	t.Setenv("DGRAPH_ORDERED_DELTA_PROVISIONAL", "1")
	oldLimit := 4
	if limiter.c != nil {
		limiter.c.L.Lock()
		oldLimit = limiter.max
		limiter.c.L.Unlock()
	}
	configureRateLimiter(64)
	t.Cleanup(func() { configureRateLimiter(oldLimit) })
	dir := t.TempDir()
	oldConfig, oldState, oldBadger, oldStore, oldGroup := Config, State, pstore, postingStore, gr
	oldHard, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	attr := x.AttrInRootNamespace("ordered-delta")
	var state ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		state.closePostingStore()
		Config, State, pstore, postingStore, gr = oldConfig, oldState, oldBadger, oldStore, oldGroup
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHard, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	Config = snapshotInstallOptions(dir)
	Config.PostingStoreDurability = PostingStoreDurabilityDurable

	x.WorkerConfig.HardSync, x.Config.MaxRetries = false, 3
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	wal := raftwal.Init(Config.WALDir)
	defer func() { require.NoError(t, wal.Close()) }()
	require.NoError(t, state.openPostingStore())

	State, pstore, postingStore = state, nil, state.PostingStore
	posting.InitForStore(postingStore, 1<<20, false)
	schema.InitForStore(schemaPostingStore{store: postingStore})
	require.NoError(t, schema.LoadFromDb(context.Background()))
	n := newNode(wal, 1, 1, "")
	var observe atomic.Bool
	var phasesMu sync.Mutex
	phases := make(map[uint64][]string)
	capture := func(e inquiryEvent) {
		if observe.Load() {
			phasesMu.Lock()
			if e.Phase == "oracle_end" || e.Phase == "proposal_done_begin" || e.Phase == "applied_done" {
				phases[e.Index] = append(phases[e.Index], e.Phase)
			}
			phasesMu.Unlock()
		}
	}
	n.deltaInquiry = &deltaInquiry{emit: capture}
	gr = &groupi{gid: 1, Node: n, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}}, blockDeletes: new(sync.Mutex), closer: z.NewCloser(0), oracleWakeCh: make(chan struct{}, 1)}
	require.NoError(t, initProposalKey(1))
	n.SetRaft(raft.StartNode(n.Cfg, []raft.Peer{{ID: 1}}))
	defer n.Raft().Stop()
	n.closer = z.NewCloser(1)
	applyDone := make(chan struct{})
	go func() { defer close(applyDone); n.processApplyCh() }()
	committed := make(chan uint64, 16)
	readyStop, readyDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readyDone)
		for {
			select {
			case rd := <-n.Raft().Ready():
				if err := wal.Save(&rd.HardState, rd.Entries, &rd.Snapshot); err != nil {
					panic(err)
				}
				if rd.MustSync {
					if err := wal.Sync(); err != nil {
						panic(err)
					}
				}
				var entries []raftpb.Entry
				for _, e := range rd.CommittedEntries {
					n.Applied.Begin(e.Index)
					if e.Type == raftpb.EntryConfChange {
						var change raftpb.ConfChange
						if err := change.Unmarshal(e.Data); err != nil {
							panic(err)
						}
						n.Raft().ApplyConfChange(change)
						n.Applied.Done(e.Index)
						continue
					}
					if len(e.Data) == 0 {
						n.Applied.Done(e.Index)
						continue
					}
					key := binary.BigEndian.Uint64(e.Data[:8])
					n.markProposalCommitted(key, e.Index)
					var p pb.Proposal
					if err := proto.Unmarshal(e.Data[8:], &p); err != nil {
						panic(err)
					}
					if p.Delta != nil {
						for _, tx := range p.Delta.Txns {
							committed <- tx.StartTs
						}
					}
					atomic.AddInt64(&n.pendingSize, int64(e.Size()))
					entries = append(entries, e)
				}
				if len(entries) > 0 {
					n.enqueueApply(entries)
				}
				n.Raft().Advance()
			case <-readyStop:
				return
			}
		}
	}()
	defer func() { close(readyStop); <-readyDone; close(n.applyCh); <-applyDone }()
	require.Eventually(t, func() bool { _, ok := n.Raft().Status().Config.Voters.IDs()[1]; return ok }, 3*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		if n.Raft().Status().Lead == 1 {
			return true
		}
		_ = n.Raft().Campaign(context.Background())
		return false
	}, 3*time.Second, time.Millisecond)
	base := posting.Oracle().MaxAssigned() + 100
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	propose := func(p *pb.Proposal) { require.NoError(t, n.proposeAndWait(ctx, p)) }
	propose(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}})
	propose(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}})
	lastUID := uint64(4)
	if multi {
		lastUID = 5
	}
	for uid := uint64(2); uid <= lastUID; uid++ {
		propose(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + uid, Edges: []*pb.DirectedEdge{{Entity: uid, Attr: attr, Value: []byte("value"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}})
	}
	statsBefore, err := State.TreeDBStore.Stats()
	require.NoError(t, err)
	counter := func(stats map[string]string, key string) uint64 {
		value, err := strconv.ParseUint(stats[key], 10, 64)
		require.NoError(t, err)
		return value
	}
	observe.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var syncs, staged atomic.Int32
	restore := func() {}
	restore = durabilitycut.Install(func(e durabilitycut.Event) error {
		if !strings.HasPrefix(e.Root+string(filepath.Separator), Config.PostingDir+string(filepath.Separator)) {
			return nil
		}
		if e.Point == durabilitycut.AfterLogicalMVCCStaging {
			staged.Add(1)
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			if syncs.Add(1) == 1 {
				close(entered)
				<-release
			}
		}
		return nil
	})
	defer restore()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	zero := &orderedDeltaZero{deltas: make(chan *pb.OracleDelta), connected: make(chan struct{})}
	pb.RegisterZeroServer(server, zero)
	pb.RegisterRaftServer(server, oracleWakeHeartbeat{})
	go func() { _ = server.Serve(listener) }()
	addr := listener.Addr().String()
	require.NotNil(t, conn.GetPools().Connect(addr, nil))
	defer func() { conn.GetPools().Remove(addr); server.Stop() }()
	gr.state = &pb.MembershipState{Zeros: map[uint64]*pb.Member{1: {Id: 1, Addr: addr, Leader: true}}}
	gr.closer = z.NewCloser(1)
	go gr.processOracleDeltaStream()
	defer gr.closer.SignalAndWait()
	gr.triggerOracleSync()
	select {
	case <-zero.connected:
	case <-ctx.Done():
		t.Fatal("Zero stream missing")
	}
	sequence := uint64(7)
	send := func(start, commit uint64, more ...*pb.TxnStatus) {
		statuses := append([]*pb.TxnStatus{{StartTs: start, CommitTs: commit}}, more...)
		maxAssigned := statuses[len(statuses)-1].CommitTs
		if advancingSequence {
			sequence++
		}
		select {
		case zero.deltas <- &pb.OracleDelta{MaxAssigned: maxAssigned, Txns: statuses, GroupChecksums: map[uint32]uint64{1: 111, 2: 222}, ChecksumSequence: sequence}:
		case <-ctx.Done():
			t.Fatal("Delta send timed out")
		}
	}
	awaitCommit := func(start uint64) {
		select {
		case got := <-committed:
			require.Equal(t, start, got)
		case <-time.After(2 * time.Second):
			t.Fatalf("transaction %d never committed while predecessor native sync was held", start)
		}
	}
	send(base+2, base+6)
	awaitCommit(base + 2)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("A never entered native sync")
	}
	if multi {
		send(base+3, base+7, &pb.TxnStatus{StartTs: base + 4, CommitTs: base + 8})
	} else {
		send(base+3, base+7)
	}
	awaitCommit(base + 3)
	if multi {
		awaitCommit(base + 4)
	}
	send(base+lastUID, base+lastUID+4)
	awaitCommit(base + lastUID)
	require.Eventually(t, func() bool { return staged.Load() >= 2 }, 2*time.Second, time.Millisecond, "B and C must actually stage in the native successor while A is held")
	require.Equal(t, base, posting.Oracle().MaxAssigned())
	unblock()
	require.NoError(t, posting.Oracle().WaitForTs(ctx, base+lastUID+4))
	require.Equal(t, int32(2), syncs.Load(), "three singleton decisions must share one real native successor sync")
	statsAfter, err := State.TreeDBStore.Stats()
	require.NoError(t, err)
	require.Equal(t, uint64(2), counter(statsAfter, "dgraph.treedb.durable_cohorts_total")-counter(statsBefore, "dgraph.treedb.durable_cohorts_total"))
	require.Equal(t, lastUID-1, counter(statsAfter, "dgraph.treedb.durable_requests_total")-counter(statsBefore, "dgraph.treedb.durable_requests_total"))
	require.GreaterOrEqual(t, counter(statsAfter, "dgraph.treedb.durable_cohort_size_max"), uint64(2))
	require.NoError(t, n.Applied.WaitForMark(ctx, n.Raft().Status().Commit))
	phasesMu.Lock()
	require.Len(t, phases, 3)
	for _, got := range phases {
		require.Equal(t, []string{"oracle_end", "proposal_done_begin", "applied_done"}, got)
	}
	phasesMu.Unlock()
	for ts := base + 6; ts <= base+lastUID+4; ts++ {
		for uid := uint64(2); uid <= lastUID; uid++ {
			read := postingStore.NewReadTxn(ts)
			_, err := read.Get(x.DataKey(attr, uid))
			if uid+base+4 <= ts {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
			}
			read.Discard()
		}
	}
}
