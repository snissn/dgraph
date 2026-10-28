/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/testutil/durabilitycut"
	"os"
	"path/filepath"
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
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestOrderedDeltaProductionPhysicalBoundary(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "")
}
func TestOrderedDeltaDisabledRetainsSerialBoundary(t *testing.T) {
	testOrderedDeltaPhysical(t, false, false, false, "")
}
func TestOrderedDeltaWatermarkBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "")
}
func TestOrderedDeltaSyncFailureRetainsFrontiers(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, true, "")
}

func TestOrderedDeltaSuccessorSyncFailureRetainsFrontier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "successor-fail")
}

func TestOrderedDeltaChecksumTransitionIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "checksum")
}
func TestOrderedDeltaUnknownPayloadIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "unknown")
}
func TestOrderedDeltaSharedPostingIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "shared")
}
func TestOrderedDeltaRepeatedStartIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "repeated-start")
}

func TestOrderedDeltaProducerPanicDrainsAccepted(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "producer")
}
func TestOrderedDeltaFinalizerPanicDrainsAccepted(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "finalize")
}
func TestOrderedDeltaCapacityOneProgress(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "capacity-one")
}
func TestOrderedDeltaNativeOwnershipSaturation(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "saturation")
}
func TestOrderedDeltaCloseDrainsWindow(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "close")
}

func TestOrderedDeltaSaturatedCloseDrainsWindow(t *testing.T) {
	testOrderedDeltaPhysical(t, true, false, false, "saturation-close")
}
func TestOrderedDeltaChecksumRegressionIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "checksum-regression")
}
func TestOrderedDeltaSameSequenceContradictoryMapIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "checksum-contradiction")
}
func TestOrderedDeltaForeignGroupMapChangeIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "checksum-foreign")
}
func TestOrderedDeltaAbortIsBarrier(t *testing.T) {
	testOrderedDeltaPhysical(t, true, true, false, "abort")
}

func TestOrderedDeltaAdvancingChecksumSequenceLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fail  bool
		stage string
	}{
		{name: "physical"},
		{name: "predecessor-error", fail: true},
		{name: "successor-error", stage: "successor-fail"},
		{name: "close", stage: "close"},
		{name: "saturated-close", stage: "saturation-close"},
		{name: "capacity-one", stage: "capacity-one"},
		{name: "producer-panic", stage: "producer"},
		{name: "finalizer-panic", stage: "finalize"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testOrderedDeltaPhysicalSequence(t, true, false, tc.fail, tc.stage, true)
		})
	}
}

func testOrderedDeltaPhysical(t *testing.T, enabled, barrier, fail bool, panicStage string) {
	testOrderedDeltaPhysicalSequence(t, enabled, barrier, fail, panicStage, false)
}

func testOrderedDeltaPhysicalSequence(t *testing.T, enabled, barrier, fail bool, panicStage string, advancingSequence bool) {
	barrierKind := ""
	if barrier {
		barrierKind = panicStage
		panicStage = ""
	}
	successorFailure := panicStage == "successor-fail"
	capacityOne, saturation, closeMode := panicStage == "capacity-one", strings.Contains(panicStage, "saturation"), strings.Contains(panicStage, "close")
	if capacityOne || saturation || closeMode || successorFailure {
		panicStage = ""
	}
	dir := t.TempDir()
	oldConfig, oldState, oldBadger, oldStore, oldGroup := Config, State, pstore, postingStore, gr
	oldHardSync, oldRetries := x.WorkerConfig.HardSync, x.Config.MaxRetries
	attr := x.AttrInRootNamespace("delta-inquiry")
	gr = &groupi{gid: 1, tablets: map[string]*pb.Tablet{attr: {Predicate: attr, GroupId: 1}}, blockDeletes: new(sync.Mutex), closer: oldGroup.closer}
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var state ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		state.closePostingStore()
		Config, State, pstore, postingStore, gr = oldConfig, oldState, oldBadger, oldStore, oldGroup
		x.WorkerConfig.HardSync, x.Config.MaxRetries = oldHardSync, oldRetries
		posting.Init(oldBadger, 0, false)
		schema.Init(oldBadger)
	})
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
	gr.Node = n
	base := posting.Oracle().MaxAssigned() + 100
	apply := func(p *pb.Proposal) { require.NoError(t, n.applyCommitted(p, 0)) }
	apply(&pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base}})
	apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 1, Schema: []*pb.SchemaUpdate{{Predicate: attr, ValueType: pb.Posting_STRING}}}})
	for i := uint64(2); i <= 3; i++ {
		apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + i, Edges: []*pb.DirectedEdge{{Entity: i, Attr: attr, Value: []byte("value"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}})
	}
	if barrierKind == "shared" {
		apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 3, Edges: []*pb.DirectedEdge{{Entity: 2, Attr: attr, Value: []byte("successor"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}})
	}
	if saturation {
		edges := make([]*pb.DirectedEdge, 0, 63)
		for uid := uint64(4); uid <= 66; uid++ {
			edges = append(edges, &pb.DirectedEdge{Entity: uid, Attr: attr, Value: []byte("value"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET})
		}
		apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 3, Edges: edges}})
	}
	if panicStage == "producer" {
		apply(&pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 3, Edges: []*pb.DirectedEdge{{Entity: 4, Attr: attr, Value: []byte("value"), ValueType: pb.Posting_STRING, Op: pb.DirectedEdge_SET}}}})
	}
	panicPayload := errors.New("injected cross-Delta " + panicStage + " panic")
	panicked := make(chan struct{})
	panicDrain := make(chan struct{})
	var drainStarted sync.Once
	n.orderedDelta = enabled
	if advancingSequence {
		require.True(t, n.orderedDeltaEnabled())
		// Certified authority and terminal replay require the original serial
		// checkpoint path; the experiment must never become its selector.
		Config.PostingStoreDurability = PostingStoreDurabilityRaftCheckpoint
		require.False(t, n.orderedDeltaEnabled())
		Config.PostingStoreDurability = PostingStoreDurabilityDurable
	}
	flushed := make(chan struct{}, 2)
	published := make(chan struct{})
	var callbacks atomic.Int32
	var flushes [2]atomic.Int32
	n.deltaInquiry = &deltaInquiry{emit: func(e inquiryEvent) {
		if panicStage == "finalize" && e.Index == 1 && e.Phase == "cache_invalidation_begin" {
			close(panicked)
			panic(panicPayload)
		}
		if panicStage != "" && e.Phase == "flush_begin" && e.Index == 2 {
			drainStarted.Do(func() { close(panicDrain) })
		}
		if e.Phase == "flush_begin" && e.Index >= 1 && e.Index <= 2 {
			flushes[e.Index-1].Add(1)
		}
		if e.Phase == "callback" {
			callbacks.Add(1)
		}
		if e.Index == 1 && e.Phase == "oracle_end" {
			close(published)
		}
		if e.Index == 1 && e.Phase == "flush_begin" {
			flushed <- struct{}{}
		}
	}}
	entered, release, staged := make(chan struct{}), make(chan struct{}), make(chan struct{}, 128)
	secondEntered, secondRelease := make(chan struct{}), make(chan struct{})
	var secondOnce sync.Once
	unblockSecond := func() { secondOnce.Do(func() { close(secondRelease) }) }
	defer unblockSecond()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var syncs atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if !strings.HasPrefix(e.Root+string(filepath.Separator), Config.PostingDir+string(filepath.Separator)) {
			return nil
		}
		if e.Point == durabilitycut.AfterLogicalMVCCStaging {
			staged <- struct{}{}
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			which := syncs.Add(1)
			if which == 2 && enabled && !barrier && !fail {
				close(secondEntered)
				<-secondRelease
				if successorFailure {
					return errors.New("injected successor physical sync failure")
				}
			}
			if which == 1 {
				close(entered)
				<-release
				if fail {
					return errors.New("injected predecessor physical sync failure")
				}
			}
		}
		return nil
	})
	defer restore()
	postingStore = &orderedDeltaSyncAdmissionStore{Store: postingStore, entered: entered}
	capacityAttempt := make(chan struct{})
	allSubmitted := make(chan struct{})
	if capacityOne {
		postingStore = &orderedDeltaCapacityStore{Store: postingStore, slots: make(chan struct{}, 1), attempt: capacityAttempt}
	}
	if saturation {
		postingStore = &orderedDeltaOneEntryStore{Store: postingStore, all: allSubmitted}
	}
	if panicStage == "producer" {
		postingStore = &orderedDeltaPanicStore{Store: postingStore, signal: panicked, payload: panicPayload}
	}
	a := checkpointEntry(t, 1, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 4, Txns: []*pb.TxnStatus{{StartTs: base + 2, CommitTs: base + 4}}}})
	b := checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5, Txns: []*pb.TxnStatus{{StartTs: base + 3, CommitTs: base + 5}}}})
	if barrier && barrierKind == "" {
		b = checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5}})
	}
	// Real Zero streams attach checksum metadata to every outgoing Delta.
	for _, entry := range []*raftpb.Entry{&a, &b} {
		var proposal pb.Proposal
		require.NoError(t, proto.Unmarshal(entry.Data[8:], &proposal))
		proposal.Delta.GroupChecksums = map[uint32]uint64{1: 111, 2: 222}
		proposal.Delta.ChecksumSequence = 7
		if entry == &b {
			if advancingSequence {
				proposal.Delta.ChecksumSequence = 8
			}
			switch barrierKind {
			case "checksum":
				proposal.Delta.ChecksumSequence = 8
				proposal.Delta.GroupChecksums = map[uint32]uint64{1: 222}
			case "checksum-regression":
				proposal.Delta.ChecksumSequence = 6
			case "checksum-contradiction":
				proposal.Delta.GroupChecksums[1] = 333
			case "checksum-foreign":
				proposal.Delta.ChecksumSequence = 8
				proposal.Delta.GroupChecksums[2] = 333
			case "abort":
				proposal.Delta.Txns[0].CommitTs = 0
			case "unknown":
				proposal.Delta.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "repeated-start":
				proposal.Delta.Txns[0].StartTs = base + 2
			}
		}
		*entry = checkpointEntry(t, entry.Index, &proposal)
	}
	acks := []chan error{make(chan error, 1), make(chan error, 1)}
	for i, entry := range []raftpb.Entry{a, b} {
		require.True(t, n.Proposals.Store(binary.BigEndian.Uint64(entry.Data[:8]), &conn.ProposalCtx{Ctx: context.Background(), ErrCh: acks[i]}))
	}
	noAck := func(i int) {
		t.Helper()
		select {
		case err := <-acks[i]:
			t.Fatalf("proposal %d ACK escaped its durability/finalization boundary: %v", i+1, err)
		default:
		}
	}
	n.closer = z.NewCloser(1)
	n.Applied.Begin(1)
	n.Applied.Begin(2)
	atomic.AddInt64(&n.pendingSize, int64(a.Size()+b.Size()))
	done := make(chan struct{})
	var recovered any
	go func() { defer func() { recovered = recover(); close(done) }(); n.processApplyCh() }()
	var stopped sync.Once
	var applyClosed sync.Once
	closeApply := func() { applyClosed.Do(func() { close(n.applyCh) }) }
	stop := func() { stopped.Do(func() { unblock(); unblockSecond(); closeApply(); <-done }) }
	defer stop()
	n.enqueueApply([]raftpb.Entry{a, b})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("predecessor never entered physical sync")
	}

	if capacityOne {
		select {
		case <-capacityAttempt:
		case <-time.After(5 * time.Second):
			t.Fatal("capacity-one successor admission was never attempted")
		}
	} else if saturation {
		select {
		case <-allSubmitted:
		case <-time.After(5 * time.Second):
			t.Fatal("native ownership window did not accept all 65 bounded requests")
		}
		require.Zero(t, callbacks.Load(), "all 65 accepted requests remain owned while A sync is held")
		// Slots precede queue handoff, so a full ownership cut may seal a prefix.
		// Require actual successor staging here and all 65 callbacks after release.
		select {
		case <-staged:
		case <-time.After(5 * time.Second):
			t.Fatal("native saturated successor never staged")
		}
	} else if enabled && !barrier {
		select {
		case <-staged:
		case <-time.After(5 * time.Second):
			t.Fatal("successor did not stage during held predecessor sync")
		}
	} else {
		select {
		case <-flushed:
		case <-time.After(5 * time.Second):
			t.Fatal("serial predecessor never entered Flush")
		}
		select {
		case <-staged:
			t.Fatal("barrier/default path staged successor before predecessor completion")
		default:
		}
	}

	require.Equal(t, base, posting.Oracle().MaxAssigned())
	require.Zero(t, n.Applied.DoneUntil())
	noAck(0)
	noAck(1)
	var storeClosed chan error
	if closeMode {
		closeApply()
		storeClosed = make(chan error, 1)
		go func() { storeClosed <- State.TreeDBStore.Close() }()
		require.Eventually(t, State.TreeDBStore.IsClosed, 2*time.Second, time.Millisecond, "Close must actually publish its admission fence before checking held work")
		select {
		case err := <-storeClosed:
			t.Fatalf("store close escaped held native window: %v", err)
		default:
		}
	}
	if enabled && !barrier && !fail && panicStage == "" {
		unblock()
		select {
		case <-secondEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("successor never entered physical sync")
		}
		select {
		case <-published:
		case <-time.After(5 * time.Second):
			t.Fatal("predecessor not published while successor sync held")
		}
		require.Equal(t, base+4, posting.Oracle().MaxAssigned())
		noAck(1)
		require.NotNil(t, posting.Oracle().GetTxn(base+3))
		if !closeMode {
			read := postingStore.NewReadTxn(base + 4)
			_, err := read.Get(x.DataKey(attr, 2))
			require.NoError(t, err)
			_, err = read.Get(x.DataKey(attr, 3))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			read.Discard()
		}
	}
	if panicStage != "" {
		if panicStage == "finalize" {
			unblock()
		}
		select {
		case <-panicked:
		case <-time.After(5 * time.Second):
			t.Fatal("injected panic boundary not reached")
		}
		select {
		case <-panicDrain:
		case <-done:
			t.Fatal("panic escaped without entering accepted successor callback drain")
		case <-time.After(5 * time.Second):
			t.Fatal("panic guard never entered successor drain")
		}
		select {
		case <-done:
			t.Fatal("panic escaped before accepted held callbacks drained")
		default:
		}
		require.Equal(t, base, posting.Oracle().MaxAssigned())
	}
	stop()
	// Ordinary durable mode cannot publish or replace certified checksum authority.
	require.Zero(t, n.raftChecksumSequence)
	require.Zero(t, n.raftOrderedChecksum)
	require.Empty(t, n.raftChecksumDigest)
	if storeClosed != nil {
		require.NoError(t, <-storeClosed)
	}
	if panicStage != "" {
		noAck(0)
		noAck(1)
		require.Same(t, panicPayload, recovered, "preserve original panic payload")
		require.Equal(t, int32(2), callbacks.Load())
		require.Equal(t, int32(1), flushes[0].Load())
		require.Equal(t, int32(1), flushes[1].Load())
		require.Equal(t, base, posting.Oracle().MaxAssigned())
		require.Zero(t, n.Applied.DoneUntil())
		require.NotNil(t, posting.Oracle().GetTxn(base+2))
		require.NotNil(t, posting.Oracle().GetTxn(base+3))
	} else if successorFailure {
		require.NoError(t, <-acks[0])
		noAck(1)
		require.Equal(t, int32(2), callbacks.Load())
		require.NotNil(t, recovered)
		require.Equal(t, base+4, posting.Oracle().MaxAssigned())
		require.Nil(t, posting.Oracle().GetTxn(base+2))
		require.NotNil(t, posting.Oracle().GetTxn(base+3))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Applied.WaitForMark(ctx, 1))
		require.Equal(t, uint64(1), n.Applied.DoneUntil())
	} else if fail {
		noAck(0)
		noAck(1)
		require.Equal(t, int32(2), callbacks.Load(), "both accepted native callbacks must drain")
		require.NotNil(t, recovered)
		require.Equal(t, base, posting.Oracle().MaxAssigned())
		require.Zero(t, n.Applied.DoneUntil())
		require.NotNil(t, posting.Oracle().GetTxn(base+2))
		require.NotNil(t, posting.Oracle().GetTxn(base+3))
	} else {
		require.NoError(t, <-acks[0])
		require.NoError(t, <-acks[1])
		require.Nil(t, recovered)
		if saturation {
			require.Equal(t, int32(65), callbacks.Load(), "all native ownership slots must complete exactly once")
		}
		require.Equal(t, base+5, posting.Oracle().MaxAssigned())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Applied.WaitForMark(ctx, 2))
		require.Equal(t, uint64(2), n.Applied.DoneUntil())
	}
}

// Force the first real admission to return only after its native sync starts;
// no callback is delayed and no scheduler timing is assumed by this fixture.
type orderedDeltaSyncAdmissionStore struct {
	posting.Store
	entered <-chan struct{}
	first   sync.Once
}

func (s *orderedDeltaSyncAdmissionStore) NewWriteTxn() posting.WriteTxn {
	return &orderedDeltaSyncAdmissionTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *orderedDeltaSyncAdmissionStore) SealCommitWindow() {
	if v, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		v.SealCommitWindow()
	}
}

type orderedDeltaSyncAdmissionTxn struct {
	posting.WriteTxn
	owner *orderedDeltaSyncAdmissionStore
}

func (t *orderedDeltaSyncAdmissionTxn) CommitAt(ts uint64, cb func(error)) error {
	err := t.WriteTxn.CommitAt(ts, cb)
	if err == nil {
		t.owner.first.Do(func() { <-t.owner.entered })
	}
	return err
}

// B accepts its first real native chunk, then its next producer chunk panics.
// This exercises the writer that has not yet returned from prepareDeltaMode.
type orderedDeltaPanicStore struct {
	posting.Store
	commits int
	signal  chan struct{}
	payload any
}

func (s *orderedDeltaPanicStore) NewWriteTxn() posting.WriteTxn {
	return &orderedDeltaPanicTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *orderedDeltaPanicStore) SealCommitWindow() {
	if v, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		v.SealCommitWindow()
	}
}

type orderedDeltaPanicTxn struct {
	posting.WriteTxn
	owner   *orderedDeltaPanicStore
	entries int
}

func (t *orderedDeltaPanicTxn) SetEntry(e posting.Entry) error {
	if t.owner.commits >= 2 {
		close(t.owner.signal)
		panic(t.owner.payload)
	}
	if t.owner.commits == 1 && t.entries == 1 {
		return badger.ErrTxnTooBig
	}
	if err := t.WriteTxn.SetEntry(e); err != nil {
		return err
	}
	t.entries++
	return nil
}
func (t *orderedDeltaPanicTxn) CommitAt(ts uint64, cb func(error)) error {
	t.owner.commits++
	return t.WriteTxn.CommitAt(ts, cb)
}

// Capacity one is an admission control fixture, not a new production mode.
// Native callbacks release its sole permit without needing the apply owner.
type orderedDeltaCapacityStore struct {
	posting.Store
	slots       chan struct{}
	attempt     chan struct{}
	submissions int
}

func (s *orderedDeltaCapacityStore) NewWriteTxn() posting.WriteTxn {
	return &orderedDeltaCapacityTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *orderedDeltaCapacityStore) SealCommitWindow() {
	if v, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		v.SealCommitWindow()
	}
}

type orderedDeltaCapacityTxn struct {
	posting.WriteTxn
	owner *orderedDeltaCapacityStore
}

func (t *orderedDeltaCapacityTxn) CommitAt(ts uint64, cb func(error)) error {
	t.owner.submissions++
	if t.owner.submissions == 2 {
		close(t.owner.attempt)
	}
	t.owner.slots <- struct{}{}
	return t.WriteTxn.CommitAt(ts, func(err error) { <-t.owner.slots; cb(err) })
}

// One original key per chunk fills the real 65-request native ownership bound:
// one held predecessor plus all 64 bounded successor keys. No extra work is made.
type orderedDeltaOneEntryStore struct {
	posting.Store
	commits int
	all     chan struct{}
}

func (s *orderedDeltaOneEntryStore) NewWriteTxn() posting.WriteTxn {
	return &orderedDeltaOneEntryTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s}
}
func (s *orderedDeltaOneEntryStore) SealCommitWindow() {
	if v, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		v.SealCommitWindow()
	}
}

type orderedDeltaOneEntryTxn struct {
	posting.WriteTxn
	owner   *orderedDeltaOneEntryStore
	entries int
}

func (t *orderedDeltaOneEntryTxn) SetEntry(e posting.Entry) error {
	if t.entries == 1 {
		return badger.ErrTxnTooBig
	}
	if err := t.WriteTxn.SetEntry(e); err != nil {
		return err
	}
	t.entries++
	return nil
}
func (t *orderedDeltaOneEntryTxn) CommitAt(ts uint64, cb func(error)) error {
	err := t.WriteTxn.CommitAt(ts, cb)
	if err == nil {
		t.owner.commits++
		if t.owner.commits == 65 {
			close(t.owner.all)
		}
	}
	return err
}
