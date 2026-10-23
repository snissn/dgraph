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
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/golang/glog"
	td "github.com/snissn/gomap/TreeDB"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	raftCheckpointVersion  = "dgraph-normal-delta-v5"
	raftCheckpointEvery    = 1024
	raftCheckpointBytes    = 16 << 20
	raftRecoveryMaxEntries = 4096
	raftRecoveryMaxBytes   = 64 << 20
)

// raftCheckpointMetadata is application state bound atomically to the immutable
// TreeDB checkpoint. Every unresolved prewrite is canonical state in the same
// exact root; the fixed-size reference is incrementally maintained with apply.
type raftCheckpointMetadata struct {
	Version              string
	Group                uint32
	RaftID               uint64
	Index                uint64
	Term                 uint64
	OracleMaxAssigned    uint64
	GroupChecksum        uint64
	PendingTransactions  uint64
	ConfState            raftpb.ConfState
	AppliedProposals     map[uint64]raftProposalSignature
	SemanticFloor        uint64
	FloorTransitionIndex uint64
	PhysicalFloor        uint64
	PendingOverlay       posting.RaftPendingManifest
}

type raftProposalSignature struct {
	Size         int
	StartTs      uint64
	Digest       string
	SeenUnixNano int64
	Rejection    string `json:",omitempty"`
}

// A semantic rejection is produced only by the side-effect-free validation
// pass. Never infer it from a wrapped storage error, runtime conflict, or text.
// Before certification its authority is the ordered proposal plus the schema
// reconstructed by the preceding ordered input. After certification the retry
// disposition is part of that same application cut.
type raftSemanticRejection struct{ reason string }

func (e *raftSemanticRejection) Error() string { return e.reason }

func isRaftSemanticRejection(err error) bool {
	_, ok := err.(*raftSemanticRejection)
	return ok
}

func validateRaftMutationEdges(m *pb.Mutations, inferred map[string]types.TypeID) error {
	ctx := schema.GetWriteContext(context.Background())
	for _, edge := range m.Edges {
		if edge.Attr == "" {
			return &raftSemanticRejection{reason: "Predicate cannot be empty"}
		}
		su, exists := schema.State().Get(ctx, edge.Attr)
		if !exists {
			hint := m.GetMetadata().GetPredHints()[edge.Attr]
			su = inferredSchema(edge.Attr, inferred[edge.Attr], hint)
		}
		if su.ValueType == pb.Posting_PASSWORD && edge.ValueType != pb.Posting_PASSWORD {
			// Normal proposal admission hashes passwords before appending the log.
			// Late conversion consumes randomness and is not a pure replay step.
			return fmt.Errorf("Raft password conversion must complete before proposal persistence")
		}
		// ValidateAndConvert may replace value/type. Validate a temporary edge so
		// a rejected proposal leaves both the posting state and its input intact.
		candidate := &pb.DirectedEdge{Entity: edge.Entity, Attr: edge.Attr, Value: edge.Value,
			ValueId: edge.ValueId, ValueType: edge.ValueType, Lang: edge.Lang, Op: edge.Op}
		if err := ValidateAndConvert(candidate, &su); err != nil {
			return &raftSemanticRejection{reason: err.Error()}
		}
	}
	return nil
}

type appliedProposal struct {
	err     error
	size    int
	startTs uint64
	digest  string
	seen    time.Time
}

// External expiry is exclusively an ordered PostingFloorRequest. A local clock
// must never change replicated retry or prewrite outcomes.
func (n *node) expireRaftProposals(time.Time, time.Duration) error { return nil }

func raftProposalIdentity(p *pb.Proposal) (uint64, string, error) {
	canonical := proto.Clone(p).(*pb.Proposal)
	canonical.Index = 0
	updateStartTs(canonical)
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return 0, "", err
	}
	start := canonical.StartTs
	if canonical.Delta != nil {
		start = canonical.Delta.MaxAssigned
	}
	if canonical.PostingFloor != nil {
		start = canonical.PostingFloor.Floor
	}
	sum := sha256.Sum256(data)
	return start, hex.EncodeToString(sum[:]), nil
}

func (n *node) applyTrackedProposal(p *pb.Proposal, key uint64, size int) error {
	var start uint64
	var digest string
	if raftCheckpointEnabled() {
		if p.Delta != nil {
			for _, decision := range p.Delta.Txns {
				if decision.CommitTs != 0 && decision.CommitTs <= decision.StartTs {
					return &raftSemanticRejection{reason: "Oracle commit timestamp must exceed transaction start"}
				}
			}
		}
		// Expiry precedes key lookup: even a byte-identical expired retry must not
		// resurrect a resolved transaction after its disposition was retired.
		if p.Mutations != nil && p.Mutations.StartTs <= n.raftPostingFloor {
			return &raftSemanticRejection{reason: fmt.Sprintf("mutation StartTs %d expired at replicated posting floor %d", p.Mutations.StartTs, n.raftPostingFloor)}
		}
		var err error
		start, digest, err = raftProposalIdentity(p)
		if err != nil {
			return err
		}
	}
	if old := n.appliedProposals[key]; key != 0 && old != nil {
		if raftCheckpointEnabled() && (old.startTs != start || old.digest != digest) {
			return &raftSemanticRejection{reason: "proposal retry key is bound to different transaction/content"}
		}
		if (old.err == nil || (raftCheckpointEnabled() && isRaftSemanticRejection(old.err))) && (raftCheckpointEnabled() || old.size == size) {
			old.seen = time.Now()
			return old.err
		}
	}
	err := n.applyCommitted(p, key)
	if raftCheckpointEnabled() && err == nil && p.Delta != nil && p.Delta.MaxAssigned > n.raftOrderedAssigned {
		n.raftOrderedAssigned = p.Delta.MaxAssigned
	}
	if key != 0 {
		if n.appliedProposals == nil {
			n.appliedProposals = make(map[uint64]*appliedProposal)
		}
		n.appliedProposals[key] = &appliedProposal{err: err, size: size, startTs: start, digest: digest, seen: time.Now()}
	}
	return err
}

func raftCheckpointEnabled() bool {
	return Config.PostingStoreBackend == PostingStoreBackendTreeDB && Config.PostingStoreDurability == PostingStoreDurabilityRaftCheckpoint
}

// validateRaftCheckpoint is read-only and does not accept the old separately
// persisted CheckpointIndex as evidence. Missing history and identity mismatches
// are startup errors, before read health can be published.
func validateRaftApplicationMetadata(c raftCheckpointMetadata) error {
	if c.Version != raftCheckpointVersion || c.PendingTransactions != c.PendingOverlay.Count {
		return fmt.Errorf("unsupported checkpoint application state")
	}
	if err := posting.ValidateRaftPendingManifest(c.PendingOverlay); err != nil {
		return err
	}
	for key, sig := range c.AppliedProposals {
		if _, err := hex.DecodeString(sig.Digest); err != nil || key == 0 || sig.Size <= 0 || sig.SeenUnixNano <= 0 || len(sig.Digest) != 64 {
			return fmt.Errorf("invalid checkpoint proposal signature")
		}
	}
	if c.PhysicalFloor > c.SemanticFloor || c.FloorTransitionIndex > c.Index || (c.SemanticFloor != 0 && (c.FloorTransitionIndex == 0 || c.SemanticFloor >= c.OracleMaxAssigned)) {
		return fmt.Errorf("invalid certified posting floor frontier")
	}
	return nil
}

func validateRaftCheckpoint(c raftCheckpointMetadata, store *raftwal.DiskStorage, gid uint32, id uint64) error {
	if c.Version != raftCheckpointVersion || c.Group != gid || c.RaftID != id || c.PendingTransactions != c.PendingOverlay.Count {
		return fmt.Errorf("incompatible Raft TreeDB checkpoint identity or pending state")
	}
	if err := validateRaftApplicationMetadata(c); err != nil {
		return err
	}
	hs, err := store.HardState()
	if err != nil {
		return err
	}
	first, err := store.FirstIndex()
	if err != nil {
		return err
	}
	if c.Index > hs.Commit || first > c.Index+1 {
		return fmt.Errorf("checkpoint/replay history gap: checkpoint=%d first=%d commit=%d", c.Index, first, hs.Commit)
	}
	if c.Index > 0 {
		term, err := store.Term(c.Index)
		if err != nil {
			return err
		}
		if term != c.Term {
			return fmt.Errorf("checkpoint Raft term mismatch")
		}
	}
	last, err := store.LastIndex()
	if err != nil {
		return err
	}
	for lo := c.Index + 1; lo <= hs.Commit; {
		entries, err := store.Entries(lo, hs.Commit+1, 8<<20)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return fmt.Errorf("empty committed replay range at %d", lo)
		}
		for _, e := range entries {
			if e.Index != lo {
				return fmt.Errorf("missing replay entry %d", lo)
			}
			lo++
		}
	}
	if last < hs.Commit {
		return fmt.Errorf("missing committed replay suffix: last=%d commit=%d", last, hs.Commit)
	}
	return nil
}

func (n *node) recoverRaftCheckpoint() error {
	if !raftCheckpointEnabled() {
		return nil
	}
	if n.RaftContext.IsLearner {
		return fmt.Errorf("external posting recovery learner activation requires its own lifecycle qualification")
	}
	return n.recoverRaftCheckpointState()
}

func (n *node) recoverRaftCheckpointState() error {
	n.Store.RequireCheckpointCertificate()
	walPath, err := filepath.Abs(Config.WALDir)
	if err != nil {
		return err
	}
	intent, err := preflightRaftSnapshotInstall("dgraph-raft-v1:" + walPath)
	if err != nil {
		return err
	}
	if intent != nil {
		current, ok := State.TreeDBStore.RaftCheckpointCertificate()
		var source td.ExternalRecoveryCheckpoint
		if err := json.Unmarshal(intent.SourceCertificate, &source); err != nil {
			return err
		}
		if !ok || current.Root != source.Root || !bytes.Equal(current.Metadata, intent.ReceiverMetadata) {
			return fmt.Errorf("accepted install posting root is not reconciled")
		}
		if err := n.Store.CompleteSnapshotInstall(current.Metadata); err != nil {
			return err
		}
	}
	data, ok := State.TreeDBStore.RecoveredRaftCheckpoint()
	c := raftCheckpointMetadata{Version: raftCheckpointVersion, Group: n.gid, RaftID: n.Id, PendingOverlay: posting.EmptyRaftPendingManifest()}
	if ok {
		if err := json.Unmarshal(data, &c); err != nil {
			return err
		}
	}
	if err := validateRaftCheckpoint(c, n.Store, n.gid, n.Id); err != nil {
		return err
	}
	if err := State.TreeDBStore.RestoreRaftPendingOverlay(c.PendingOverlay); err != nil {
		return err
	}
	if uint64(posting.Oracle().NumPendingTxns()) != c.PendingTransactions || posting.Oracle().MinPendingStartTs() <= c.SemanticFloor {
		return fmt.Errorf("certified pending set crosses semantic floor")
	}
	if err := State.TreeDBStore.RestoreRaftPostingFloor(c.SemanticFloor, c.PhysicalFloor); err != nil {
		return err
	}
	n.raftPostingFloor = c.SemanticFloor
	n.raftFloorIndex = c.FloorTransitionIndex
	n.raftOrderedAssigned = c.OracleMaxAssigned
	if err := n.Store.AuthorizeCheckpoint(c.Index); err != nil {
		return err
	}
	lastIndex, err := n.Store.LastIndex()
	if err != nil {
		return err
	}
	for lo := c.Index + 1; lo <= lastIndex; {
		entries, err := n.Store.Entries(lo, lastIndex+1, 8<<20)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return fmt.Errorf("missing recovery-debt entry %d", lo)
		}
		for _, e := range entries {
			atomic.AddUint64(&n.raftRecoveryBytes, uint64(len(e.Data)))
			lo = e.Index + 1
		}
	}
	n.Cfg.Applied = c.Index
	n.raftSemanticIndex = c.Index
	hs, err := n.Store.HardState()
	if err != nil {
		return err
	}
	n.raftReplayUntil = hs.Commit
	n.appliedProposals = make(map[uint64]*appliedProposal, len(c.AppliedProposals))
	for key, sig := range c.AppliedProposals {
		n.appliedProposals[key] = &appliedProposal{size: sig.Size, startTs: sig.StartTs, digest: sig.Digest, seen: time.Unix(0, sig.SeenUnixNano)}
		if sig.Rejection != "" {
			n.appliedProposals[key].err = &raftSemanticRejection{reason: sig.Rejection}
		}
	}
	n.Applied.SetDoneUntil(c.Index)
	atomic.StoreUint64(&n.raftCertifiedIndex, c.Index)
	n.Store.SetUint(raftwal.CheckpointIndex, c.Index)
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: c.OracleMaxAssigned})
	atomic.StoreUint64(&groups().deltaChecksum, c.GroupChecksum)
	atomic.StoreUint64(&n.checkpointTs, c.OracleMaxAssigned)
	if c.Index > 0 {
		n.SetConfState(&c.ConfState)
		snap, err := n.Store.Snapshot()
		if err != nil {
			return err
		}
		if snap.Metadata.Index < c.Index {
			data, err := proto.Marshal(&pb.Snapshot{Context: n.RaftContext, Index: c.Index, ReadTs: c.OracleMaxAssigned,
				TransferFormat: raftCheckpointTransferFormat})
			if err != nil {
				return err
			}
			if err := n.Store.CreateSnapshot(c.Index, &c.ConfState, data); err != nil {
				return err
			}
		}
	}
	glog.Infof("TreeDB raft_checkpoint recovery: certified=%d oracle=%d", c.Index, c.OracleMaxAssigned)
	if intent != nil {
		if err := n.Store.FinishSnapshotInstall(intent.Digest); err != nil {
			return err
		}
	}
	return nil
}

// checkpointRaftCut runs on the semantic apply goroutine, after posting Flush
// and Oracle advancement. It therefore never samples independent progress
// counters across applications. Background rollup is disabled for this policy.
func (n *node) checkpointRaftCut(index uint64, force bool) error {
	if !raftCheckpointEnabled() {
		return nil
	}
	last := atomic.LoadUint64(&n.raftCertifiedIndex)
	force = force || n.raftConfStateDirty || n.raftFloorDirty
	if index < last || (!force && index-last < raftCheckpointEvery && atomic.LoadUint64(&n.raftRecoveryBytes) < raftCheckpointBytes) {
		return nil
	}

	term, err := n.Store.Term(index)
	if err != nil {
		return err
	}
	c := raftCheckpointMetadata{Version: raftCheckpointVersion, Group: n.gid, RaftID: n.Id, Index: index, Term: term, OracleMaxAssigned: n.raftOrderedAssigned, GroupChecksum: atomic.LoadUint64(&groups().deltaChecksum), ConfState: *n.ConfState()}
	c.PendingOverlay, err = State.TreeDBStore.RaftPendingManifest()
	if err != nil {
		return err
	}
	c.PendingTransactions = c.PendingOverlay.Count
	if c.PendingTransactions != uint64(posting.Oracle().NumPendingTxns()) {
		return fmt.Errorf("canonical pending overlay/Oracle count mismatch")
	}
	c.SemanticFloor = n.raftPostingFloor
	c.FloorTransitionIndex = n.raftFloorIndex
	c.PhysicalFloor, err = State.TreeDBStore.DiscardFloor()
	if err != nil {
		return err
	}
	c.AppliedProposals = make(map[uint64]raftProposalSignature, len(n.appliedProposals))
	for key, p := range n.appliedProposals {
		if p.err == nil || isRaftSemanticRejection(p.err) {
			sig := raftProposalSignature{Size: p.size, StartTs: p.startTs, Digest: p.digest, SeenUnixNano: p.seen.UnixNano()}
			if p.err != nil {
				sig.Rejection = p.err.Error()
			}
			c.AppliedProposals[key] = sig
		} else {
			return fmt.Errorf("uncertain proposal outcome cannot be certified: %w", p.err)
		}
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		// Keep the old certificate and every required input. Admission debt stops
		// new mutations. Never discard dedup signatures just to fit a certificate.
		glog.Warningf("raft_checkpoint metadata exceeds 1MiB; retaining recovery history")
		if force {
			return fmt.Errorf("forced checkpoint metadata exceeds 1MiB")
		}
		return nil
	}
	if err := State.TreeDBStore.CheckpointRaft(context.Background(), data); err != nil {
		return err
	}
	// The durable TreeDB certificate precedes both retention authorization and
	// the independent applied hint. A crash between any steps selects a safe cut.
	if err := n.Store.AuthorizeCheckpoint(index); err != nil {
		return err
	}
	snapData, err := proto.Marshal(&pb.Snapshot{Context: n.RaftContext, Index: index, ReadTs: c.OracleMaxAssigned,
		TransferFormat: raftCheckpointTransferFormat})
	if err != nil {
		return err
	}
	if index > last {
		if err := n.Store.CreateSnapshot(index, &c.ConfState, snapData); err != nil {
			return err
		}
	}
	n.Store.SetUint(raftwal.CheckpointIndex, index)
	atomic.StoreUint64(&n.raftCertifiedIndex, index)
	atomic.StoreUint64(&n.checkpointTs, c.OracleMaxAssigned)
	// Ready persistence can continue during image copying. Subtract only the
	// application prefix included in this checkpoint, retaining concurrent suffix.
	atomic.AddUint64(&n.raftRecoveryBytes, ^(n.raftAppliedBytes - 1))
	n.raftAppliedBytes = 0
	n.raftConfStateDirty = false
	n.raftFloorDirty = false
	glog.Infof("TreeDB raft_checkpoint certified: index=%d oracle=%d pending=%d semantic_floor=%d physical_floor=%d", index, c.OracleMaxAssigned, c.PendingTransactions, c.SemanticFloor, c.PhysicalFloor)
	return nil
}

func checkRaftPrototypeProposal(p *pb.Proposal) error {
	if p.PostingFloor != nil && (p.Mutations != nil || p.Delta != nil || p.State != nil || p.Snapshot != nil || p.Restore != nil || p.DeleteNs != nil || p.ExtSnapshotState != nil || p.CdcState != nil || p.CleanPredicate != "" || len(p.Kv) != 0) {
		return fmt.Errorf("posting floor requires an isolated ordered proposal")
	}
	return checkRaftSupportedProposal(p)
}

// reserveRaftRecoveryDebt bounds accepted mutation input before proposing it.
// Decisions bypass this budget so a full budget cannot prevent pending txns
// from resolving. Reserved bytes overlap persisted bytes conservatively until
// the caller finishes; concurrent proposals cannot all pass the same budget.
func (n *node) reserveRaftRecoveryDebt(size uint64) (func(), error) {
	n.raftDebtMu.Lock()
	defer n.raftDebtMu.Unlock()
	last, err := n.Store.LastIndex()
	if err != nil {
		return nil, err
	}
	certified := atomic.LoadUint64(&n.raftCertifiedIndex)
	if last-certified+n.raftReservedEntries >= raftRecoveryMaxEntries || size > raftRecoveryMaxBytes || atomic.LoadUint64(&n.raftRecoveryBytes)+n.raftReservedBytes+size > raftRecoveryMaxBytes {
		return nil, fmt.Errorf("raft_checkpoint recovery debt limit: new mutations paused until pending decisions permit checkpoint")
	}
	n.raftReservedEntries++
	n.raftReservedBytes += size
	return func() {
		n.raftDebtMu.Lock()
		n.raftReservedEntries--
		n.raftReservedBytes -= size
		n.raftDebtMu.Unlock()
	}, nil
}

func (n *node) raftRecoveryAtLimit() bool {
	n.raftDebtMu.Lock()
	defer n.raftDebtMu.Unlock()
	last, err := n.Store.LastIndex()
	return err != nil || last-atomic.LoadUint64(&n.raftCertifiedIndex)+n.raftReservedEntries >= raftRecoveryMaxEntries || atomic.LoadUint64(&n.raftRecoveryBytes)+n.raftReservedBytes >= raftRecoveryMaxBytes
}
