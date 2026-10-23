/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	td "github.com/snissn/gomap/TreeDB"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const raftCheckpointTransferFormat = "dgraph-treedb-checkpoint-v1"

// preflightRaftSnapshotInstall precedes posting-owner Open and its physical
// restore/cleanup. The complete accepted archive is rooted in the WAL namespace,
// so opening either the old or new CURRENT cannot remove install authority.
func preflightRaftSnapshotInstall(identity string) (*raftwal.AcceptedSnapshotInstall, error) {
	intent, err := raftwal.ReadAcceptedSnapshotInstall(Config.WALDir)
	if err != nil || intent == nil {
		return intent, err
	}
	if intent.ReceiverIdentity != identity {
		return nil, fmt.Errorf("accepted snapshot owner identity mismatch")
	}
	var source td.ExternalRecoveryCheckpoint
	var metadata raftCheckpointMetadata
	if err := json.Unmarshal(intent.SourceCertificate, &source); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(intent.ReceiverMetadata, &metadata); err != nil {
		return nil, err
	}
	if err := validateInstallMetadata(source, intent, metadata); err != nil {
		return nil, err
	}
	return intent, nil
}

func validateInstallMetadata(source td.ExternalRecoveryCheckpoint, intent *raftwal.AcceptedSnapshotInstall,
	receiver raftCheckpointMetadata) error {
	var sender raftCheckpointMetadata
	if err := json.Unmarshal(source.Metadata, &sender); err != nil {
		return err
	}
	snapshot := intent.Snapshot.Metadata
	if source.Generation == 0 || source.Digest == "" || source.Identity == "" ||
		sender.Version != raftCheckpointVersion || sender.PendingTransactions != 0 ||
		sender.Group != intent.Group || sender.Index != snapshot.Index || sender.Term != snapshot.Term ||
		!protoConfStateEqual(sender.ConfState, snapshot.ConfState) {
		return fmt.Errorf("source certificate does not authorize accepted Raft snapshot")
	}
	var data pb.Snapshot
	if err := proto.Unmarshal(intent.Snapshot.Data, &data); err != nil {
		return err
	}
	if data.Index != sender.Index || data.ReadTs != sender.OracleMaxAssigned ||
		data.GetContext().GetGroup() != sender.Group || data.GetContext().GetId() != sender.RaftID {
		return fmt.Errorf("snapshot payload does not match source application certificate")
	}
	// Receiver progress is an exact rewrite of source application metadata:
	// only local Raft identity changes. No frontier/outcome may be invented.
	sender.RaftID = intent.RaftID
	expected, err := json.Marshal(sender)
	if err != nil {
		return err
	}
	actual, err := json.Marshal(receiver)
	if err != nil {
		return err
	}
	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("receiver certificate changes source semantic state")
	}
	return nil
}

// raftpb ConfState is a gogo protobuf. Compare the generated binary contract
// rather than mixing protobuf implementations or reading current membership.
func protoConfStateEqual(a, b raftpb.ConfState) bool {
	x, err := a.Marshal()
	if err != nil {
		return false
	}
	y, err := b.Marshal()
	return err == nil && bytes.Equal(x, y)
}

func (s *ServerState) reconcileRaftSnapshotOwner(ctx context.Context, intent *raftwal.AcceptedSnapshotInstall) error {
	if intent == nil {
		return nil
	}
	var source td.ExternalRecoveryCheckpoint
	if err := json.Unmarshal(intent.SourceCertificate, &source); err != nil {
		return err
	}
	current, exists := s.TreeDBStore.RaftCheckpointCertificate()
	if exists && current.Identity == intent.ReceiverIdentity && current.Root == source.Root &&
		bytes.Equal(current.Metadata, intent.ReceiverMetadata) {
		return nil // crash after CURRENT: do not manufacture another generation
	}
	archive, err := os.Open(filepath.Join(Config.WALDir, intent.ArchiveName))
	if err != nil {
		return err
	}
	defer archive.Close()
	installed, err := s.TreeDBStore.InstallRaftCheckpoint(ctx, archive,
		func(received td.ExternalRecoveryCheckpoint) ([]byte, error) {
			if received.Generation != source.Generation || received.Digest != source.Digest ||
				received.Identity != source.Identity || received.Root != source.Root ||
				!bytes.Equal(received.Metadata, source.Metadata) {
				return nil, fmt.Errorf("accepted snapshot archive source certificate mismatch")
			}
			return intent.ReceiverMetadata, nil
		})
	if err != nil {
		return err
	}
	if installed.Identity != intent.ReceiverIdentity || installed.Root != source.Root ||
		!bytes.Equal(installed.Metadata, intent.ReceiverMetadata) {
		return fmt.Errorf("installed snapshot receiver certificate mismatch")
	}
	return nil
}
