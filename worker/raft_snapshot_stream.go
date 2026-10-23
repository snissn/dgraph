/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/v25/conn"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/golang/glog"
	td "github.com/snissn/gomap/TreeDB"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const raftSnapshotChunkBytes = 1 << 20

var raftSnapshotStreamHeader = []byte("dgraph-treedb-checkpoint-v1\n")

type raftSnapshotChunkWriter struct {
	ctx  context.Context
	send func(*pb.KVS) error
}

func (w raftSnapshotChunkWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) != 0 {
		if err := w.ctx.Err(); err != nil {
			return written, err
		}
		n := len(data)
		if n > raftSnapshotChunkBytes {
			n = raftSnapshotChunkBytes
		}
		if err := w.send(&pb.KVS{Data: data[:n]}); err != nil {
			return written, err
		}
		written += n
		data = data[n:]
	}
	return written, nil
}

func streamRaftCheckpoint(snap *pb.Snapshot, out pb.Worker_StreamSnapshotServer) error {
	if snap.TransferFormat != raftCheckpointTransferFormat || snap.SinceTs != 0 {
		return fmt.Errorf("external snapshot sender requires explicit full checkpoint transfer format")
	}
	store := State.TreeDBStore
	if store == nil {
		return fmt.Errorf("external snapshot posting owner unavailable")
	}
	cert, ok := store.RaftCheckpointCertificate()
	var cut raftCheckpointMetadata
	if !ok {
		return fmt.Errorf("external snapshot has no certified root")
	}
	if err := json.Unmarshal(cert.Metadata, &cut); err != nil {
		return err
	}
	if snap.Index == 0 || snap.Index != cut.Index || snap.ReadTs != cut.OracleMaxAssigned ||
		snap.GetContext().GetGroup() != cut.Group {
		return fmt.Errorf("requested external snapshot is no longer the current certified cut")
	}
	if err := out.Send(&pb.KVS{Data: raftSnapshotStreamHeader}); err != nil {
		return err
	}
	if err := store.ExportRaftCheckpoint(out.Context(), raftSnapshotChunkWriter{out.Context(), out.Send},
		cert.Generation, cert.Digest); err != nil {
		return err
	}
	if err := out.Send(&pb.KVS{Done: true}); err != nil {
		return err
	}
	ack, err := out.Recv()
	if err != nil {
		return err
	}
	if !ack.Done || ack.TransferFormat != raftCheckpointTransferFormat {
		return fmt.Errorf("external snapshot receiver did not acknowledge the native format")
	}
	return nil
}

// receiveRaftCheckpoint stages the complete bounded archive in the WAL
// namespace. Only a validated owner authorization callback can publish intent;
// preparation failures remove staging, accepted input survives every failure.
func (n *node) receiveRaftCheckpoint(ctx context.Context, snapshot raftpb.Snapshot, ready raftpb.HardState,
	recv func() (*pb.KVS, error)) error {
	var snap pb.Snapshot
	if err := proto.Unmarshal(snapshot.Data, &snap); err != nil {
		return err
	}
	if snap.TransferFormat != raftCheckpointTransferFormat || snap.SinceTs != 0 {
		return fmt.Errorf("external snapshot receiver requires native full checkpoint format")
	}
	first, err := recv()
	if err != nil {
		return err
	}
	if first.Done || !bytes.Equal(first.Data, raftSnapshotStreamHeader) {
		return fmt.Errorf("external snapshot stream format mismatch")
	}
	file, err := os.CreateTemp(Config.WALDir, "snapshot-install-*.tar")
	if err != nil {
		return err
	}
	name := file.Name()
	accepted := false
	defer func() {
		file.Close()
		if !accepted {
			os.Remove(name)
		}
	}()
	hash := sha256.New()
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := recv()
		if err != nil {
			return err
		}
		if chunk.Done {
			if len(chunk.Data) != 0 || size == 0 {
				return fmt.Errorf("invalid external snapshot terminal chunk")
			}
			break
		}
		if len(chunk.Data) == 0 || len(chunk.Data) > raftSnapshotChunkBytes || size+int64(len(chunk.Data)) > 8<<30 {
			return fmt.Errorf("external snapshot stream exceeds chunk/archive bounds")
		}
		if _, err := io.MultiWriter(file, hash).Write(chunk.Data); err != nil {
			return err
		}
		size += int64(len(chunk.Data))
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	walPath, err := filepath.Abs(Config.WALDir)
	if err != nil {
		return err
	}
	current, err := n.Store.HardState()
	if err != nil {
		return err
	}
	if ready.Term == 0 {
		ready = current
	}
	ready.Commit = snapshot.Metadata.Index
	_, err = State.TreeDBStore.InstallRaftCheckpoint(ctx, file,
		func(source td.ExternalRecoveryCheckpoint) ([]byte, error) {
			var receiver raftCheckpointMetadata
			if err := json.Unmarshal(source.Metadata, &receiver); err != nil {
				return nil, err
			}
			if receiver.Index < n.raftSemanticIndex || receiver.OracleMaxAssigned < n.raftOrderedAssigned || receiver.SemanticFloor < n.raftPostingFloor {
				return nil, fmt.Errorf("external snapshot would regress semantic or Oracle progress")
			}
			receiver.RaftID = n.Id
			metadata, err := json.Marshal(receiver)
			if err != nil {
				return nil, err
			}
			certificate, err := json.Marshal(source)
			if err != nil {
				return nil, err
			}
			intent := raftwal.AcceptedSnapshotInstall{Version: 1, ReceiverIdentity: "dgraph-raft-v1:" + walPath,
				Group: n.gid, RaftID: n.Id, Snapshot: snapshot, HardState: ready,
				ArchiveName: filepath.Base(name), ArchiveSHA256: hex.EncodeToString(hash.Sum(nil)), ArchiveBytes: size,
				SourceCertificate: certificate, ReceiverMetadata: metadata}
			if err := validateInstallMetadata(source, &intent, receiver); err != nil {
				return nil, err
			}
			if err := n.Store.AcceptSnapshotInstall(intent); err != nil {
				accepted = raftwal.SnapshotInstallPublicationUncertain(err)
				return nil, err
			}
			accepted = true
			return metadata, nil
		})
	if err != nil {
		return err
	}
	return n.reloadRaftCheckpointState(ctx)
}

func (n *node) reloadRaftCheckpointState(ctx context.Context) error {
	posting.Oracle().ResetTxns()
	posting.ResetCache()
	schema.InitForStore(schemaPostingStore{store: postingStore})
	if err := schema.LoadFromDb(ctx); err != nil {
		return err
	}
	atomic.StoreUint64(&n.raftRecoveryBytes, 0)
	n.raftAppliedBytes = 0
	return n.recoverRaftCheckpointState()
}

func (n *node) retrieveRaftCheckpoint(snapshot raftpb.Snapshot, ready raftpb.HardState) error {
	var snap pb.Snapshot
	if err := proto.Unmarshal(snapshot.Data, &snap); err != nil {
		return err
	}
	pool, err := conn.GetPools().Get(snap.GetContext().GetAddr())
	if err != nil || pool == nil {
		pool, err = n.leaderBlocking()
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Minute)
	defer cancel()
	stream, err := pb.NewWorkerClient(pool.Get()).StreamSnapshot(ctx)
	if err != nil {
		return err
	}
	request := proto.Clone(&snap).(*pb.Snapshot)
	request.Context, request.SinceTs = n.RaftContext, 0
	if err := stream.Send(request); err != nil {
		return err
	}
	if err := n.receiveRaftCheckpoint(ctx, snapshot, ready, stream.Recv); err != nil {
		return err
	}
	if err := stream.Send(&pb.Snapshot{Done: true, TransferFormat: raftCheckpointTransferFormat}); err != nil {
		// The receiver certificate and matching WAL cut already own the data.
		// A lost transport ACK can cause another request, never rollback.
		glog.Warningf("Native snapshot installed durably but stream ACK failed: %v", err)
	}
	return nil
}

// persistSnapshotElection retains the Ready election promises without claiming
// its not-yet-installed snapshot or committed suffix. This also makes a process
// crash before a failed-download restart recover from the old authority.
func (n *node) persistSnapshotElection(ready raftpb.HardState) error {
	current, err := n.Store.HardState()
	if err != nil {
		return err
	}
	if ready.Term == 0 {
		return n.Store.Sync()
	}
	if ready.Term < current.Term || (ready.Term == current.Term && current.Vote != 0 && ready.Vote != current.Vote) {
		return fmt.Errorf("snapshot Ready would regress election promises")
	}
	ready.Commit = current.Commit
	if err := n.Store.Save(&ready, nil, &raftpb.Snapshot{}); err != nil {
		return err
	}
	return n.Store.Sync()
}

// restartAfterRaftSnapshotFailure runs with semantic apply paused. No accepted
// intent or root switch exists, so the drained current application state stays
// valid. Restart at its exact semantic index and membership without rolling data
// back underneath previously admitted queries or applying old entries twice.
func (n *node) restartAfterRaftSnapshotFailure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	intent, err := raftwal.ReadAcceptedSnapshotInstall(Config.WALDir)
	if err != nil {
		return err
	}
	if intent != nil || State.TreeDBStore.IsClosed() {
		return fmt.Errorf("accepted snapshot failure requires process recovery")
	}
	if n.Applied.DoneUntil() < n.Applied.LastIndex() {
		return fmt.Errorf("cannot restart snapshot receiver with queued semantic apply")
	}
	confState := n.ConfState()
	if confState == nil {
		confState = &raftpb.ConfState{} // a fresh receiver may have no committed membership yet
	}
	return n.RestartRaftAtSemanticCut(n.raftSemanticIndex, *confState)
}

// Snapshot Context is a transport source address/identity. Rebind only that
// context when forwarding an installed snapshot; the certified index, term,
// read frontier and captured ConfState remain exactly the accepted cut.
func (n *node) bindRaftSnapshotSource(message *raftpb.Message) error {
	if !raftCheckpointEnabled() || message.Type != raftpb.MsgSnap {
		return nil
	}
	var snapshot pb.Snapshot
	if err := proto.Unmarshal(message.Snapshot.Data, &snapshot); err != nil {
		return err
	}
	if snapshot.TransferFormat != raftCheckpointTransferFormat {
		return fmt.Errorf("external outgoing snapshot has incompatible transfer format")
	}
	snapshot.Context = n.RaftContext
	data, err := proto.Marshal(&snapshot)
	if err != nil {
		return err
	}
	message.Snapshot.Data = data
	return nil
}
