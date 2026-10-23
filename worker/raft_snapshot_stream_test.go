/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, stream.Send(request))
		_, err = stream.Recv()
		return err
	}
	require.ErrorContains(t, requestError(&pb.Snapshot{Index: fresh.Index, ReadTs: fresh.ReadTs,
		Context: fresh.Context}), "explicit full checkpoint transfer format")
	mode := Config.PostingStoreDurability
	Config.PostingStoreDurability = PostingStoreDurabilityDurable
	require.ErrorContains(t, requestError(fresh), "legacy snapshot sender does not support transfer format")
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
	return archive.Bytes(), staleError
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
