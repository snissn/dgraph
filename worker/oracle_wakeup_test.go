/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only Status is used by the production connection gate. Atomic leadership
// permits transitions concurrently with the gate, including under the race test.
type oracleWakeRaft struct {
	raft.Node
	lead     atomic.Uint64
	observed chan uint64
}

func (r *oracleWakeRaft) Status() raft.Status {
	lead := r.lead.Load()
	select {
	case r.observed <- lead:
	default:
	}
	return raft.Status{BasicStatus: raft.BasicStatus{ID: 1,
		SoftState: raft.SoftState{Lead: lead}}}
}

func oracleWakeGroup(t *testing.T) (*groupi, *oracleWakeRaft) {
	t.Helper()
	r := new(oracleWakeRaft)
	n := &node{Node: &conn.Node{Id: 1}}
	n.SetRaft(r)
	g := &groupi{Node: n, closer: z.NewCloser(0), oracleWakeCh: make(chan struct{}, 1)}
	t.Cleanup(g.closer.Signal)
	return g, r
}

func oracleWakeWait(g *groupi, ticks <-chan time.Time) <-chan bool {
	done := make(chan bool, 1)
	go func() { done <- g.waitForOracleLeadership(ticks) }()
	return done
}

func oracleWakeResult(t *testing.T, done <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-done:
		require.Equal(t, want, got)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Oracle connection gate did not react without a retry tick")
	}
}

func TestOracleWakeBeforeConnectorAndLeadershipRegain(t *testing.T) {
	g, r := oracleWakeGroup(t)
	ticks := make(chan time.Time) // Deliberately held: no polling can pass.
	// An election can finish before processOracleDeltaStream is scheduled.
	r.lead.Store(1)
	g.triggerOracleSync()
	for i := 0; i < 100; i++ {
		g.triggerOracleSync()
	}
	require.Len(t, g.oracleWakeCh, 1)
	oracleWakeResult(t, oracleWakeWait(g, ticks), true)
	require.Empty(t, g.oracleWakeCh)

	// A queued notification confers no authority after leadership is lost.
	r.lead.Store(2)
	r.observed = make(chan uint64, 4)
	g.triggerOracleSync()
	done := oracleWakeWait(g, ticks)
	select {
	case lead := <-r.observed:
		require.EqualValues(t, 2, lead, "stale wake must check actual follower state")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("gate did not consume the stale notification")
	}
	require.Empty(t, g.oracleWakeCh)
	select {
	case <-done:
		t.Fatal("follower accepted a stale leadership notification")
	default:
	}
	r.lead.Store(1)
	g.triggerOracleSync()
	oracleWakeResult(t, done, true)
}

func TestOracleWakePeriodicReconnectAndShutdown(t *testing.T) {
	g, r := oracleWakeGroup(t)
	ticks := make(chan time.Time, 1)
	r.lead.Store(1)
	// The same gate is reentered when an Oracle stream returns, including EOF,
	// Zero-leader change or an open failure. Periodic retry remains sufficient.
	for i := 0; i < 3; i++ {
		done := oracleWakeWait(g, ticks)
		ticks <- time.Now()
		oracleWakeResult(t, done, true)
	}
	r.lead.Store(2)
	done := oracleWakeWait(g, ticks)
	ticks <- time.Now()
	g.closer.Signal()
	oracleWakeResult(t, done, false)
	// A pending wake/tick cannot reopen a stream after shutdown.
	r.lead.Store(1)
	g.triggerOracleSync()
	select {
	case ticks <- time.Now():
	default:
	}
	require.False(t, g.waitForOracleLeadership(ticks))
}

func TestOracleWakeConcurrentDuplicates(t *testing.T) {
	g, r := oracleWakeGroup(t)
	r.lead.Store(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			g.triggerOracleSync()
		}
	}()
	oracleWakeResult(t, oracleWakeWait(g, nil), true)
	<-done
	require.LessOrEqual(t, len(g.oracleWakeCh), 1)
}

// This exercises the real connector and gRPC receive/error/reconnect/cancel
// paths. No delta is sent, so the test cannot advance any visibility watermark.
type oracleWakeZero struct {
	pb.UnimplementedZeroServer
	calls     chan int
	count     atomic.Int32
	active    atomic.Int32
	maxActive atomic.Int32
	failFirst bool
}

func (s *oracleWakeZero) Oracle(_ *api.Payload, stream pb.Zero_OracleServer) error {
	n := int(s.count.Add(1))
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.maxActive.Load(); active > old; old = s.maxActive.Load() {
		if s.maxActive.CompareAndSwap(old, active) {
			break
		}
	}
	s.calls <- n
	if s.failFirst && n == 1 {
		return status.Error(codes.Unavailable, "controlled Zero stream failure")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func oracleWakeServer(t *testing.T, failFirst bool) (string, *oracleWakeZero) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	zero := &oracleWakeZero{calls: make(chan int, 8), failFirst: failFirst}
	pb.RegisterZeroServer(server, zero)
	pb.RegisterRaftServer(server, oracleWakeHeartbeat{})
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	addr := listener.Addr().String()
	require.NotNil(t, conn.GetPools().Connect(addr, nil))
	t.Cleanup(func() {
		conn.GetPools().Remove(addr)
		server.Stop()
		<-done
	})
	return addr, zero
}

func oracleWakeCall(t *testing.T, s *oracleWakeZero, want int, timeout time.Duration) {
	t.Helper()
	select {
	case n := <-s.calls:
		require.Equal(t, want, n)
	case <-time.After(timeout):
		t.Fatal("Oracle stream was not connected")
	}
}

func TestOracleWakeRealStreamReconnectAndCancel(t *testing.T) {
	firstAddr, first := oracleWakeServer(t, true)
	secondAddr, second := oracleWakeServer(t, false)
	g, r := oracleWakeGroup(t)
	g.closer = z.NewCloser(1)
	g.gid = 1
	g.state = &pb.MembershipState{Zeros: map[uint64]*pb.Member{
		1: {Id: 1, Addr: firstAddr, Leader: true},
	}}
	r.lead.Store(1)
	g.triggerOracleSync() // A genuine startup wake can precede the consumer.
	go g.processOracleDeltaStream()
	t.Cleanup(g.closer.SignalAndWait)
	oracleWakeCall(t, first, 1, 500*time.Millisecond)
	// No new wake: the retained retry ticker must reconnect after the error.
	oracleWakeCall(t, first, 2, 2500*time.Millisecond)
	for i := 0; i < 100; i++ {
		g.triggerOracleSync()
	}
	select {
	case n := <-first.calls:
		t.Fatalf("duplicate wake opened parallel stream %d", n)
	case <-time.After(20 * time.Millisecond):
	}
	// Existing periodic Zero-leader detection cancels the old stream and opens
	// the new one. The pending duplicate remains a coalesced hint, not a second
	// connector or a second reader of the Oracle stream.
	g.Lock()
	g.state = &pb.MembershipState{Zeros: map[uint64]*pb.Member{
		2: {Id: 2, Addr: secondAddr, Leader: true},
	}}
	g.Unlock()
	oracleWakeCall(t, second, 1, 2500*time.Millisecond)
	g.closer.SignalAndWait()
	require.EqualValues(t, 1, first.maxActive.Load())
	require.EqualValues(t, 1, second.maxActive.Load())
	select {
	case n := <-second.calls:
		t.Fatalf("shutdown opened stream %d", n)
	default:
	}
}

// The real connection pool requires the maintained heartbeat service, even for
// a fixture whose Oracle endpoint only holds/cancels a stream.
type oracleWakeHeartbeat struct{ pb.UnimplementedRaftServer }

func (oracleWakeHeartbeat) Heartbeat(_ *api.Payload, stream pb.Raft_HeartbeatServer) error {
	if err := stream.Send(&pb.HealthInfo{Instance: "zero"}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
