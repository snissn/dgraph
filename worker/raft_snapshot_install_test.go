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

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
	source, ok := state.TreeDBStore.RaftCheckpointCertificate()
	require.True(t, ok)
	var archive bytes.Buffer
	require.NoError(t, state.TreeDBStore.ExportRaftCheckpoint(context.Background(), &archive,
		source.Generation, source.Digest))
	snapshot, err := sourceWAL.Snapshot()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, snapshot.Metadata.ConfState.Voters)
	posting.Cleanup()
	posting.Oracle().ResetTxns()
	state.closePostingStore()
	require.NoError(t, sourceWAL.Close())
	state = nil

	for _, stage := range []string{"before_owner_open", "after_owner_open", "after_wal_complete",
		"corrupt_archive", "missing_archive", "corrupt_intent", "wrong_identity"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			Config = snapshotInstallOptions(dir)
			require.NoError(t, os.MkdirAll(Config.WALDir, 0700))
			wal := raftwal.Init(Config.WALDir)
			wal.SetUint(raftwal.RaftId, 2)
			wal.SetUint(raftwal.GroupId, 1)
			wal.RequireCheckpointCertificate()
			var old []raftpb.Entry
			for index := uint64(1); index <= 4; index++ {
				old = append(old, raftpb.Entry{Index: index, Term: 4, Data: []byte("divergent uncommitted tail")})
			}
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
				Group: 1, RaftID: 2, Snapshot: snapshot, HardState: raftpb.HardState{Term: 4, Vote: 2, Commit: 2},
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
			runCut("accepted")
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
			for restart := 0; restart < 2; restart++ {
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
			require.NoError(t, n.recoverRaftCheckpoint())
			require.Equal(t, uint64(2), n.Cfg.Applied)
			require.Equal(t, []uint64{1, 2}, n.ConfState().Voters)
			require.Contains(t, n.appliedProposals, uint64(1))
			require.Equal(t, base+1, posting.Oracle().MaxAssigned())
			hs, err := wal.HardState()
			require.NoError(t, err)
			require.Equal(t, raftpb.HardState{Term: 4, Vote: 2, Commit: 2}, hs)
			last, err := wal.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(2), last)
			list, err := posting.GetNoStore(x.DataKey(predicate, 1), math.MaxUint64)
			require.NoError(t, err)
			value, err := list.Value(math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, []byte("certified peer value"), value.Value)
			require.NoFileExists(t, filepath.Join(Config.WALDir, name))
			posting.Cleanup()
			posting.Oracle().ResetTxns()
			state.closePostingStore()
			state = nil
			require.NoError(t, wal.Close())
		})
	}
}
