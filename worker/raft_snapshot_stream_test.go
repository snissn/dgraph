/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// readCheckpointThroughRPC exercises the actual registered production handler,
// including explicit opt-in and old-cut refusal after the source advances.
func readCheckpointThroughRPC(t *testing.T, stale, fresh *pb.Snapshot) ([]byte, error) {
	t.Helper()
	listener := bufconn.Listen(raftSnapshotChunkBytes * 2)
	server := grpc.NewServer()
	pb.RegisterWorkerServer(server, &grpcWorker{})
	go server.Serve(listener)
	defer server.Stop()
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, "passthrough:///snapshot-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer connection.Close()
	client := pb.NewWorkerClient(connection)
	requestError := func(request *pb.Snapshot) error {
		stream, err := client.StreamSnapshot(ctx)
		require.NoError(t, err)
		// Capability guards may close the RPC before accepting the request.
		// Recv carries the server's terminal status even when Send sees EOF.
		if err := stream.Send(request); err != nil {
			_, statusErr := stream.Recv()
			if statusErr != nil {
				return statusErr
			}
			return err
		}
		_, err = stream.Recv()
		return err
	}
	require.ErrorContains(t, requestError(&pb.Snapshot{Index: fresh.Index, ReadTs: fresh.ReadTs,
		Context: fresh.Context}), "explicit full checkpoint transfer format")
	mode := Config.PostingStoreDurability
	Config.PostingStoreDurability = PostingStoreDurabilityDurable
	require.ErrorContains(t, requestError(fresh), "operation requires the Badger operational posting-store backend: Raft snapshot send")
	Config.PostingStoreDurability = mode
	staleError := requestError(stale)
	require.ErrorContains(t, staleError, "no longer the current certified cut")
	stream, err := client.StreamSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(fresh))
	header, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, raftSnapshotStreamHeader, header.Data)
	var archive bytes.Buffer
	for {
		chunk, err := stream.Recv()
		require.NoError(t, err)
		if chunk.Done {
			break
		}
		require.NotEmpty(t, chunk.Data)
		require.LessOrEqual(t, len(chunk.Data), raftSnapshotChunkBytes)
		archive.Write(chunk.Data)
	}
	require.NoError(t, stream.Send(&pb.Snapshot{Done: true, TransferFormat: raftCheckpointTransferFormat}))
	// Read the final status so no sender can still touch global fixture state.
	_, err = stream.Recv()
	require.Error(t, err)
	checkSnapshotFailureReports(t, stale, fresh)
	return archive.Bytes(), staleError
}

type snapshotFailureReport struct {
	id     uint64
	status raft.SnapshotStatus
}

type snapshotReportRecorder struct {
	raft.Node
	reports chan snapshotFailureReport
}

func (r *snapshotReportRecorder) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	r.reports <- snapshotFailureReport{id, status}
}

type snapshotFailureStream struct {
	grpc.ServerStream
	request    *pb.Snapshot
	ack        *pb.Snapshot
	receiveErr error
	failSend   int
	failDone   bool
	sends      int
}

func (s *snapshotFailureStream) Context() context.Context { return context.Background() }

func (s *snapshotFailureStream) Recv() (*pb.Snapshot, error) {
	if s.request != nil {
		request := s.request
		s.request = nil
		return request, nil
	}
	if s.ack != nil {
		return s.ack, nil
	}
	if s.receiveErr != nil {
		return nil, s.receiveErr
	}
	return nil, io.EOF
}

func (s *snapshotFailureStream) Send(chunk *pb.KVS) error {
	s.sends++
	if s.sends == s.failSend || (s.failDone && chunk.Done) {
		return errors.New("injected snapshot transport failure")
	}
	return nil
}

// Use the certified owner already exercised through the RPC above. The real
// handler must notify the same Raft owner exactly once for every failed native
// transfer; successful transfers and missing initial requests must not do so.
func checkSnapshotFailureReports(t *testing.T, stale, fresh *pb.Snapshot) {
	t.Helper()
	oldNode := gr.Node
	recorder := &snapshotReportRecorder{reports: make(chan snapshotFailureReport, 16)}
	gr.Node = &node{Node: new(conn.Node)}
	gr.Node.SetRaft(recorder)
	defer func() { gr.Node = oldNode }()
	for _, tc := range []struct {
		name       string
		configure  func(*snapshotFailureStream)
		wantReport bool
		wantError  string
	}{
		{"missing_request", func(s *snapshotFailureStream) { s.request = nil }, false, "EOF"},
		{"invalid_format", func(s *snapshotFailureStream) { s.request.TransferFormat = "badger" }, true, "explicit full checkpoint"},
		{"stale_cut", func(s *snapshotFailureStream) { s.request.Index = stale.Index }, true, "no longer the current"},
		{"header_failure", func(s *snapshotFailureStream) { s.failSend = 1 }, true, "injected snapshot transport"},
		{"body_failure", func(s *snapshotFailureStream) { s.failSend = 2 }, true, "injected snapshot transport"},
		{"terminal_failure", func(s *snapshotFailureStream) { s.failDone = true }, true, "injected snapshot transport"},
		{"missing_ack", func(s *snapshotFailureStream) {}, true, "EOF"},
		{"invalid_ack", func(s *snapshotFailureStream) { s.ack = &pb.Snapshot{Done: true} }, true, "did not acknowledge"},
		{"success", func(s *snapshotFailureStream) {
			s.ack = &pb.Snapshot{Done: true, TransferFormat: raftCheckpointTransferFormat}
		}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := proto.Clone(fresh).(*pb.Snapshot)
			request.Context = proto.Clone(fresh.Context).(*pb.RaftContext)
			request.Context.Id = 42
			stream := &snapshotFailureStream{request: request}
			tc.configure(stream)
			err := (&grpcWorker{}).StreamSnapshot(stream)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
			if tc.wantReport {
				require.Len(t, recorder.reports, 1)
				require.Equal(t, snapshotFailureReport{42, raft.SnapshotFailure}, <-recorder.reports)
			} else {
				require.Empty(t, recorder.reports)
			}
		})
	}
}

// Invalid transport input must be rejected before either owner authorization or
// accepted intent publication. Staging is removed on every preparation error.
func TestRaftCheckpointStreamRejectsUnacceptedInput(t *testing.T) {
	oldConfig := Config
	defer func() { Config = oldConfig }()
	Config.WALDir = t.TempDir()
	n := new(node)
	for _, tc := range []struct {
		name   string
		format string
		chunks []*pb.KVS
		want   string
	}{
		{"missing_format", "", nil, "requires native full"},
		{"wrong_header", raftCheckpointTransferFormat, []*pb.KVS{{Data: []byte("badger")}}, "format mismatch"},
		{"oversized_chunk", raftCheckpointTransferFormat, []*pb.KVS{{Data: raftSnapshotStreamHeader}, {Data: make([]byte, raftSnapshotChunkBytes+1)}}, "chunk/archive bounds"},
		{"empty_archive", raftCheckpointTransferFormat, []*pb.KVS{{Data: raftSnapshotStreamHeader}, {Done: true}}, "terminal chunk"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(&pb.Snapshot{TransferFormat: tc.format})
			require.NoError(t, err)
			chunks := tc.chunks
			err = n.receiveRaftCheckpoint(context.Background(), raftpb.Snapshot{Data: data}, raftpb.HardState{}, func() (*pb.KVS, error) {
				require.NotEmpty(t, chunks)
				v := chunks[0]
				chunks = chunks[1:]
				return v, nil
			})
			require.ErrorContains(t, err, tc.want)
			entries, err := os.ReadDir(Config.WALDir)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}
