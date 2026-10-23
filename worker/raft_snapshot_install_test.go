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
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func snapshotInstallOptions(dir string) Options {
	return Options{PostingDir: filepath.Join(dir, "p"), WALDir: filepath.Join(dir, "w"),
		PostingStoreBackend: PostingStoreBackendTreeDB, PostingStoreTier: PostingStoreTierBenchmarkMinimal,
		PostingStoreDurability: PostingStoreDurabilityRaftCheckpoint, PostingStoreEvents: true}
}

func TestRaftSnapshotStartupCrashHelper(t *testing.T) {
	stage, dir := os.Getenv("DGRAPH_OWNER_INSTALL_CUT"), os.Getenv("DGRAPH_OWNER_INSTALL_DIR")
	if stage == "" {
		return
	}
	Config = snapshotInstallOptions(dir)
	x.WorkerConfig.HardSync = true
	identity := "dgraph-raft-v1:" + Config.WALDir
	if stage == "after_election_sync" {
		wal := raftwal.Init(Config.WALDir)
		n := newNode(wal, 1, 2, "")
		require.NoError(t, n.persistSnapshotElection(raftpb.HardState{Term: 5, Vote: 2, Commit: 999}))
		os.Exit(0) // promises persist, old commit/snapshot/suffix remain authoritative
	}
	if stage == "accepted" {
		input, err := os.ReadFile(filepath.Join(dir, "input.json"))
		require.NoError(t, err)
		var intent raftwal.AcceptedSnapshotInstall
		require.NoError(t, json.Unmarshal(input, &intent))
		state := &ServerState{}
		require.NoError(t, state.openPostingStore())
		wal := raftwal.Init(Config.WALDir)
		wal.RequireCheckpointCertificate()
		archive, err := os.Open(filepath.Join(Config.WALDir, intent.ArchiveName))
		require.NoError(t, err)
		_, err = state.TreeDBStore.InstallRaftCheckpoint(context.Background(), archive,
			func(source td.ExternalRecoveryCheckpoint) ([]byte, error) {
				var receiver raftCheckpointMetadata
				require.NoError(t, json.Unmarshal(intent.ReceiverMetadata, &receiver))
				require.NoError(t, validateInstallMetadata(source, &intent, receiver))
				require.NoError(t, wal.AcceptSnapshotInstall(intent))
				os.Exit(0) // abrupt cut before owner CURRENT; no deferred Close
				return nil, nil
			})
		t.Fatalf("acceptance cut was not reached: %v", err)
	}
	intent, err := preflightRaftSnapshotInstall(identity)
	require.NoError(t, err)
	require.NotNil(t, intent)
	if stage == "before_owner_open" {
		os.Exit(0)
	}
	state := &ServerState{}
	require.NoError(t, state.openPostingStore())
	cert, ok := state.TreeDBStore.RaftCheckpointCertificate()
	require.True(t, ok)
	data, err := json.Marshal(cert)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "observed.json"), data, 0600))
	if stage == "after_owner_open" {
		os.Exit(0)
	}
	wal := raftwal.Init(Config.WALDir)
	wal.RequireCheckpointCertificate()
	require.NoError(t, wal.CompleteSnapshotInstall(cert.Metadata))
	if stage == "after_wal_complete" {
		os.Exit(0)
	}
	t.Fatalf("unknown crash stage %q", stage)
}

func TestRaftSnapshotStartupReconcilesCertifiedOwnerBeforeReplay(t *testing.T) {
	oldConfig, oldState, oldPstore, oldStore := Config, State, pstore, postingStore
	oldHardSync, oldNode, oldRetries := x.WorkerConfig.HardSync, gr.Node, x.Config.MaxRetries
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	var state *ServerState
	t.Cleanup(func() {
		posting.Cleanup()
		posting.Oracle().ResetTxns()
		if state != nil {
			state.closePostingStore()
		}
		Config, State, pstore, postingStore = oldConfig, oldState, oldPstore, oldStore
		x.WorkerConfig.HardSync, gr.Node, x.Config.MaxRetries = oldHardSync, oldNode, oldRetries
		posting.Init(oldPstore, 0, false)
		schema.Init(oldPstore)
	})
	x.WorkerConfig.HardSync, x.Config.MaxRetries = true, 3
	Config = snapshotInstallOptions(filepath.Join(t.TempDir(), "source"))
	require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
	sourceWAL := raftwal.Init(Config.WALDir)
	sourceWAL.SetUint(raftwal.RaftId, 1)
	sourceWAL.SetUint(raftwal.GroupId, 1)
	state = &ServerState{}
	require.NoError(t, state.openPostingStore())
	State, pstore, postingStore = *state, nil, state.PostingStore
	posting.InitForStore(state.PostingStore, 0, false)
	schema.InitForStore(schemaPostingStore{store: state.PostingStore})
	require.NoError(t, schema.LoadFromDb(context.Background()))
	n := newNode(sourceWAL, 1, 1, "")
	gr.Node = n
	require.NoError(t, n.recoverRaftCheckpoint())
	n.SetConfState(&raftpb.ConfState{Voters: []uint64{1, 2}})
	base := posting.Oracle().MaxAssigned() + 10
	predicate := x.AttrInRootNamespace("imported")
	entries := []raftpb.Entry{
		checkpointEntry(t, 1, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base, Edges: []*pb.DirectedEdge{
			{Entity: 1, Attr: predicate, Value: []byte("certified peer value"), ValueType: pb.Posting_STRING,
				Op: pb.DirectedEdge_SET}}}}),
		checkpointEntry(t, 2, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 1,
			Txns: []*pb.TxnStatus{{StartTs: base, CommitTs: base + 1}}}}),
	}
	require.NoError(t, sourceWAL.Save(&raftpb.HardState{Term: 1, Commit: 2}, entries, &raftpb.Snapshot{}))
	require.NoError(t, sourceWAL.Sync())
	for _, entry := range entries {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(entry.Data)))
		applyCheckpointEntry(t, n, entry)
	}
	require.NoError(t, n.checkpointRaftCut(2, true))
	staleSnapshot, err := sourceWAL.Snapshot()
	require.NoError(t, err)
	next := []raftpb.Entry{
		checkpointEntry(t, 3, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 2, Edges: []*pb.DirectedEdge{
			{Entity: 1, Attr: predicate, Value: []byte("fresh peer value"), ValueType: pb.Posting_STRING,
				Op: pb.DirectedEdge_SET}}}}),
		checkpointEntry(t, 4, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 3,
			Txns: []*pb.TxnStatus{{StartTs: base + 2, CommitTs: base + 3}}}}),
		checkpointEntry(t, 5, &pb.Proposal{Mutations: &pb.Mutations{StartTs: base + 4, Edges: []*pb.DirectedEdge{
			{Entity: 1, Attr: predicate, Value: []byte("pending peer value"), ValueType: pb.Posting_STRING,
				Op: pb.DirectedEdge_SET}}}}),
	}
	require.NoError(t, sourceWAL.Save(&raftpb.HardState{Term: 1, Commit: 5}, next, &raftpb.Snapshot{}))
	require.NoError(t, sourceWAL.Sync())
	for _, entry := range next {
		atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(entry.Data)))
		applyCheckpointEntry(t, n, entry)
	}
	require.NoError(t, n.checkpointRaftCut(5, true))
	var sourcePending api.TxnContext
	posting.Oracle().GetTxn(base+4).FillContext(&sourcePending, 1, false)
	require.NotEmpty(t, sourcePending.Keys)
	source, ok := state.TreeDBStore.RaftCheckpointCertificate()
	require.True(t, ok)
	snapshot, err := sourceWAL.Snapshot()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, snapshot.Metadata.ConfState.Voters)
	var staleRequest, freshRequest pb.Snapshot
	require.NoError(t, proto.Unmarshal(staleSnapshot.Data, &staleRequest))
	require.NoError(t, proto.Unmarshal(snapshot.Data, &freshRequest))
	freshBytes, staleError := readCheckpointThroughRPC(t, &staleRequest, &freshRequest)
	archive := bytes.NewBuffer(freshBytes)
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	state.closePostingStore()
	require.NoError(t, sourceWAL.Close())
	state = nil

	for _, stage := range []string{"before_owner_open", "after_owner_open", "after_wal_complete",
		"corrupt_archive", "missing_archive", "corrupt_intent", "wrong_identity", "stale_native_retry", "election_before_restart"} {
		t.Run(stage, func(t *testing.T) {
			nativeRetry := stage == "stale_native_retry" || stage == "election_before_restart"
			dir := t.TempDir()
			Config = snapshotInstallOptions(dir)
			require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
			wal := raftwal.Init(Config.WALDir)
			wal.SetUint(raftwal.RaftId, 2)
			wal.SetUint(raftwal.GroupId, 1)
			wal.RequireCheckpointCertificate()
			var old []raftpb.Entry
			for index := uint64(1); index <= 6; index++ {
				old = append(old, raftpb.Entry{Index: index, Term: 4, Data: []byte("divergent uncommitted tail")})
			}
			bootstrap, err := (&raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2}).Marshal()
			require.NoError(t, err)
			old[0].Type, old[0].Data = raftpb.EntryConfChange, bootstrap
			require.NoError(t, wal.Save(&raftpb.HardState{Term: 4, Vote: 2, Commit: 1}, old, &raftpb.Snapshot{}))
			require.NoError(t, wal.Sync())
			state = &ServerState{}
			require.NoError(t, state.openPostingStore())
			state.closePostingStore()
			state = nil
			require.NoError(t, wal.Close())
			name := "snapshot-install-certified.tar"
			require.NoError(t, os.WriteFile(filepath.Join(Config.WALDir, name), archive.Bytes(), 0600))
			hash := sha256.Sum256(archive.Bytes())
			var receiver raftCheckpointMetadata
			require.NoError(t, json.Unmarshal(source.Metadata, &receiver))
			receiver.RaftID = 2
			receiverMetadata, err := json.Marshal(receiver)
			require.NoError(t, err)
			sourceBytes, err := json.Marshal(source)
			require.NoError(t, err)
			intent := raftwal.AcceptedSnapshotInstall{Version: 1, ReceiverIdentity: "dgraph-raft-v1:" + Config.WALDir,
				Group: 1, RaftID: 2, Snapshot: snapshot, HardState: raftpb.HardState{Term: 4, Vote: 2, Commit: 5},
				ArchiveName: name, ArchiveBytes: int64(archive.Len()), ArchiveSHA256: hex.EncodeToString(hash[:]),
				SourceCertificate: sourceBytes, ReceiverMetadata: receiverMetadata}
			input, err := json.Marshal(intent)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "input.json"), input, 0600))
			runCut := func(cut string) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestRaftSnapshotStartupCrashHelper$", "-test.count=1")
				cmd.Env = append(os.Environ(), "DGRAPH_OWNER_INSTALL_CUT="+cut, "DGRAPH_OWNER_INSTALL_DIR="+dir)
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
			}
			if !nativeRetry {
				runCut("accepted")
			}
			if stage == "election_before_restart" {
				runCut("after_election_sync")
			}
			if stage == "corrupt_archive" || stage == "missing_archive" || stage == "corrupt_intent" || stage == "wrong_identity" {
				before, err := os.ReadFile(filepath.Join(Config.PostingDir, "external-recovery.json"))
				require.NoError(t, err)
				canary := filepath.Join(Config.PostingDir, "materialized-preflight-canary")
				require.NoError(t, os.Mkdir(canary, 0700))
				switch stage {
				case "corrupt_archive":
					require.NoError(t, os.WriteFile(filepath.Join(Config.WALDir, name), []byte("corrupt"), 0600))
				case "missing_archive":
					require.NoError(t, os.Remove(filepath.Join(Config.WALDir, name)))
				case "corrupt_intent":
					require.NoError(t, os.WriteFile(filepath.Join(Config.WALDir, "accepted-snapshot-install.json"), []byte("corrupt"), 0600))
				case "wrong_identity":
					wal = raftwal.Init(Config.WALDir)
					wal.SetUint(raftwal.RaftId, 9)
					require.NoError(t, wal.Sync())
					require.NoError(t, wal.Close())
				}
				state = &ServerState{}
				require.Error(t, state.openPostingStore(), "bad install authority must refuse before owner Open")
				require.Nil(t, state.TreeDBStore, "preflight rejection must precede physical owner cleanup")
				after, err := os.ReadFile(filepath.Join(Config.PostingDir, "external-recovery.json"))
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.DirExists(t, canary, "rejected startup cannot clean an owner namespace")
				require.NoFileExists(t, filepath.Join(Config.PostingDir, "CURRENT"))
				state = nil
				return
			}
			var generation uint64
			for restart := 0; !nativeRetry && restart < 2; restart++ {
				runCut(stage)
				require.FileExists(t, filepath.Join(Config.WALDir, name), "opening owner cannot destroy install input")
				if stage != "before_owner_open" {
					data, err := os.ReadFile(filepath.Join(dir, "observed.json"))
					require.NoError(t, err)
					var cert td.ExternalRecoveryCheckpoint
					require.NoError(t, json.Unmarshal(data, &cert))
					if restart == 0 {
						generation = cert.Generation
					}
					require.Equal(t, generation, cert.Generation, "repeated startup must not reinstall CURRENT")
				}
			}
			state = &ServerState{}
			require.NoError(t, state.openPostingStore())
			State, pstore, postingStore = *state, nil, state.PostingStore
			posting.InitForStore(state.PostingStore, 0, false)
			schema.InitForStore(schemaPostingStore{store: state.PostingStore})
			require.NoError(t, schema.LoadFromDb(context.Background()))
			wal = raftwal.Init(Config.WALDir)
			n = newNode(wal, 1, 2, "")
			gr.Node = n
			var applyDone chan struct{}
			if nativeRetry {
				require.NoError(t, os.Remove(filepath.Join(Config.WALDir, name)))
				require.NoError(t, n.recoverRaftCheckpoint())
				n.SetRaft(raft.RestartNode(n.Cfg))
				n.SetConfState(&raftpb.ConfState{})
				rd := <-n.Raft().Ready()
				require.Len(t, rd.CommittedEntries, 1)
				n.closer = z.NewCloser(1)
				applyDone = make(chan struct{})
				go func() { n.processApplyCh(); close(applyDone) }()
				n.Applied.Begin(1)
				n.raftSemanticMu.Lock()
				atomic.AddInt64(&n.pendingSize, int64(rd.CommittedEntries[0].Size()))
				n.applyCh <- rd.CommittedEntries
				require.Empty(t, n.ConfState().Voters, "queued future membership cannot enter a prior semantic cut")
				n.raftSemanticMu.Unlock()
				n.Raft().Advance()
				require.NoError(t, n.Applied.WaitForMark(context.Background(), 1))
				n.raftSemanticMu.Lock()
				require.NoError(t, n.persistSnapshotElection(raftpb.HardState{Term: 5, Vote: 2, Commit: 999}))
				require.Error(t, n.receiveRaftCheckpoint(context.Background(), staleSnapshot, raftpb.HardState{},
					func() (*pb.KVS, error) { return nil, staleError }))
				require.NoError(t, n.restartAfterRaftSnapshotFailure(context.Background()))
				require.Equal(t, uint64(1), n.Cfg.Applied)
				require.Equal(t, []uint64{2}, n.ConfState().Voters)
				before, err := wal.HardState()
				require.NoError(t, err)
				require.Equal(t, raftpb.HardState{Term: 5, Vote: 2, Commit: 1}, before)
				require.NoError(t, n.Raft().Step(context.Background(), raftpb.Message{Type: raftpb.MsgSnap,
					From: 1, To: 2, Term: 6, Snapshot: snapshot}))
				rd = <-n.Raft().Ready()
				require.Equal(t, snapshot.Metadata, rd.Snapshot.Metadata)
				require.NoError(t, n.persistSnapshotElection(rd.HardState))
				chunks := []*pb.KVS{{Data: raftSnapshotStreamHeader}}
				for remaining := freshBytes; len(remaining) > 0; {
					size := len(remaining)
					if size > raftSnapshotChunkBytes {
						size = raftSnapshotChunkBytes
					}
					chunks = append(chunks, &pb.KVS{Data: remaining[:size]})
					remaining = remaining[size:]
				}
				chunks = append(chunks, &pb.KVS{Done: true})
				require.NoError(t, n.receiveRaftCheckpoint(context.Background(), rd.Snapshot, rd.HardState,
					func() (*pb.KVS, error) { chunk := chunks[0]; chunks = chunks[1:]; return chunk, nil }))
				n.raftSemanticMu.Unlock()
				require.NoError(t, wal.Save(&rd.HardState, rd.Entries, &rd.Snapshot))
				require.NoError(t, wal.Sync())
				n.Raft().Advance()
			} else {
				require.NoError(t, n.recoverRaftCheckpoint())
			}
			require.Equal(t, uint64(5), n.Cfg.Applied)
			require.Equal(t, []uint64{1, 2}, n.ConfState().Voters)
			require.Contains(t, n.appliedProposals, uint64(1))
			require.Equal(t, base+3, n.raftOrderedAssigned)
			require.GreaterOrEqual(t, posting.Oracle().MaxAssigned(), base+3, "process-local Oracle hints remain monotone")
			hs, err := wal.HardState()
			require.NoError(t, err)
			if nativeRetry {
				require.Equal(t, raftpb.HardState{Term: 6, Commit: 5}, hs)
			} else {
				require.Equal(t, raftpb.HardState{Term: 4, Vote: 2, Commit: 5}, hs)
			}
			last, err := wal.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(5), last)
			list, err := posting.GetNoStore(x.DataKey(predicate, 1), math.MaxUint64)
			require.NoError(t, err)
			value, err := list.Value(math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, []byte("fresh peer value"), value.Value)
			require.Equal(t, 1, posting.Oracle().NumPendingTxns())
			var importedPending api.TxnContext
			posting.Oracle().GetTxn(base+4).FillContext(&importedPending, 1, false)
			require.ElementsMatch(t, sourcePending.Keys, importedPending.Keys, "native cut retains exact pending conflicts")
			decision := checkpointEntry(t, 6, &pb.Proposal{Delta: &pb.OracleDelta{MaxAssigned: base + 5,
				Txns: []*pb.TxnStatus{{StartTs: base + 4, CommitTs: base + 5}}}})
			decisionState := hs
			decisionState.Commit = 6
			require.NoError(t, wal.Save(&decisionState, []raftpb.Entry{decision}, &raftpb.Snapshot{}))
			require.NoError(t, wal.Sync())
			atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(decision.Data)))
			applyCheckpointEntry(t, n, decision)
			require.Zero(t, posting.Oracle().NumPendingTxns())
			list, err = posting.GetNoStore(x.DataKey(predicate, 1), math.MaxUint64)
			require.NoError(t, err)
			value, err = list.Value(math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, []byte("pending peer value"), value.Value)
			require.NoFileExists(t, filepath.Join(Config.WALDir, name))
			if applyDone != nil {
				close(n.applyCh)
				<-applyDone
			}
			posting.Cleanup()
			posting.Oracle().ResetTxns()
			state.closePostingStore()
			state = nil
			if n.Raft() != nil {
				n.Raft().Stop()
			}
			require.NoError(t, wal.Close())
		})
	}
}
