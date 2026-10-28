/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Diagnostic-only: synchronous logging perturbs scheduling and excludes a run
// from performance acceptance. No queue is consumed or work submitted early.
type deltaInquiry struct {
	mu      sync.Mutex
	seq     uint64
	streams uint64
	session string
	started time.Time
	emit    func(inquiryEvent)
}
type inquiryEvent struct {
	Session          string            `json:"session"`
	Captured         time.Time         `json:"-"`
	CapturedNS       int64             `json:"captured_ns,omitempty"`
	Stream           uint64            `json:"stream,omitempty"`
	Message          uint64            `json:"message,omitempty"`
	Aggregate        uint64            `json:"aggregate,omitempty"`
	Attempt          uint64            `json:"attempt,omitempty"`
	TxnCount         int               `json:"txn_count,omitempty"`
	ParentSequence   uint64            `json:"parent_sequence,omitempty"`
	Ordinal          int               `json:"ordinal"`
	NilTxn           bool              `json:"nil_txn,omitempty"`
	ChecksumSequence uint64            `json:"checksum_sequence,omitempty"`
	ChecksumDigest   string            `json:"checksum_digest,omitempty"`
	UnknownFields    bool              `json:"unknown_fields,omitempty"`
	Version          int               `json:"version"`
	ElapsedNS        int64             `json:"elapsed_ns"`
	Sequence         uint64            `json:"sequence"`
	At               string            `json:"at"`
	Phase            string            `json:"phase"`
	Index            uint64            `json:"index"`
	Key              uint64            `json:"key"`
	Kind             string            `json:"kind,omitempty"`
	Bytes            int               `json:"bytes,omitempty"`
	Txns             []*pb.TxnStatus   `json:"txns,omitempty"`
	StartTs          uint64            `json:"start_ts,omitempty"`
	MaxAssigned      uint64            `json:"max_assigned,omitempty"`
	CommitTs         uint64            `json:"commit_ts,omitempty"`
	Request          uint64            `json:"request,omitempty"`
	Entries          uint64            `json:"entries,omitempty"`
	PayloadBytes     uint64            `json:"payload_bytes,omitempty"`
	Error            string            `json:"error,omitempty"`
	Build            map[string]string `json:"build,omitempty"`
	Detail           string            `json:"detail,omitempty"`
}

func newDeltaInquiry() *deltaInquiry {
	if os.Getenv("DGRAPH_DELTA_INQUIRY") != "1" {
		return nil
	}
	d := &deltaInquiry{emit: func(e inquiryEvent) {
		b, err := json.Marshal(e)
		if err != nil {
			glog.Fatalf("delta inquiry serialization: %v", err)
		}
		glog.Infof("DELTA_INQUIRY %s", b)
	}}
	build := map[string]string{"source_binding": "external clean source and executable hash required"}
	if info, ok := debug.ReadBuildInfo(); ok {
		build["go_version"] = info.GoVersion
		build["module"] = info.Main.Path
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" || setting.Key == "vcs.modified" {
				build[setting.Key] = setting.Value
			}
		}
		for _, dep := range info.Deps {
			if dep.Path == "github.com/snissn/gomap" {
				build["gomap_version"] = dep.Version
				if dep.Replace != nil {
					build["gomap_replacement"] = dep.Replace.Path + "@" + dep.Replace.Version
				}
			}
		}
	}
	d.event(inquiryEvent{Phase: "enabled", Detail: "diagnostic overhead; excluded from performance acceptance", Build: build})
	return d
}
func (d *deltaInquiry) event(e inquiryEvent) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started.IsZero() {
		d.started = time.Now()
	}
	if d.session == "" {
		d.session = uuid.NewString()
	}
	if !e.Captured.IsZero() {
		e.CapturedNS = e.Captured.Sub(d.started).Nanoseconds()
	}
	// Keep decision lists below glog's record-size limit. Exact child count and
	// ordinal permit fail-closed reconstruction after partial logging.
	if len(e.Txns) > 32 {
		txns := e.Txns
		e.Txns = nil
		e.TxnCount = len(txns)
		d.emitLocked(e)
		parent := d.seq
		for i, txn := range txns {
			child := inquiryEvent{Phase: "decision_part", ParentSequence: parent, Ordinal: i}
			if txn == nil {
				child.NilTxn = true
			} else {
				child.StartTs, child.CommitTs = txn.StartTs, txn.CommitTs
				child.UnknownFields = len(txn.ProtoReflect().GetUnknown()) != 0
			}
			d.emitLocked(child)
		}
		return
	}
	d.emitLocked(e)
}

func (d *deltaInquiry) emitLocked(e inquiryEvent) {
	d.seq++
	e.Session = d.session
	e.ElapsedNS = time.Since(d.started).Nanoseconds()
	e.Version = 1
	e.Sequence = d.seq
	e.At = time.Now().UTC().Format(time.RFC3339Nano)
	d.emit(e)
}
func (d *deltaInquiry) phase(index, key uint64, phase string, err error) {
	if d == nil {
		return
	}
	e := inquiryEvent{Index: index, Key: key, Phase: phase}
	if err != nil {
		e.Error = err.Error()
	}
	d.event(e)
}
func (d *deltaInquiry) entries(entries []raftpb.Entry, phase string) {
	if d == nil {
		return
	}
	for _, entry := range entries {
		e := inquiryEvent{Phase: phase, Index: entry.Index, Bytes: entry.Size(), Kind: "control"}
		if entry.Type == raftpb.EntryNormal && len(entry.Data) >= 8 {
			e.Key = binary.BigEndian.Uint64(entry.Data[:8])
			var p pb.Proposal
			if err := proto.Unmarshal(entry.Data[8:], &p); err != nil {
				e.Kind = "decode_error"
				e.Error = err.Error()
			} else {
				switch {
				case p.Mutations != nil:
					e.Kind = "mutation"
				case p.Delta != nil:
					e.Kind = "delta"
					e.Txns = p.Delta.Txns
					e.MaxAssigned = p.Delta.MaxAssigned
				case len(p.Kv) > 0:
					e.Kind = "populate_kv"
				case p.State != nil:
					e.Kind = "membership"
				case p.CleanPredicate != "":
					e.Kind = "clean_predicate"
				case p.Snapshot != nil:
					e.Kind = "snapshot"
				case p.Restore != nil:
					e.Kind = "restore"
				case p.CdcState != nil:
					e.Kind = "cdc"
				case p.DeleteNs != nil:
					e.Kind = "delete_namespace"
				case p.ExtSnapshotState != nil:
					e.Kind = "external_snapshot"
				case p.PostingFloor != nil:
					e.Kind = "posting_floor"
				default:
					e.Kind = "other"
				}
			}
		}
		d.event(e)
	}
}

// Only commitOrAbort's writer uses this decorator. It records actual posting
// adapter submissions, not physical syncs or native cohort membership.
type inquiryStore struct {
	posting.Store
	trace      *deltaInquiry
	index, key uint64
	requests   uint64
}

func (s *inquiryStore) NewWriteTxn() posting.WriteTxn {
	s.requests++
	return &inquiryTxn{WriteTxn: s.Store.NewWriteTxn(), owner: s, request: s.requests}
}
func (s *inquiryStore) SealCommitWindow() {
	s.trace.phase(s.index, s.key, "window_seal", nil)
	if sealer, ok := s.Store.(interface{ SealCommitWindow() }); ok {
		sealer.SealCommitWindow()
	}
}

type inquiryTxn struct {
	posting.WriteTxn
	owner                   *inquiryStore
	request, entries, bytes uint64
}

func (t *inquiryTxn) SetEntry(e posting.Entry) error {
	err := t.WriteTxn.SetEntry(e)
	if err == nil {
		t.entries++
		t.bytes += uint64(len(e.Key) + len(e.Value))
	}
	return err
}
func (t *inquiryTxn) Delete(key []byte) error {
	err := t.WriteTxn.Delete(key)
	if err == nil {
		t.entries++
		t.bytes += uint64(len(key))
	}
	return err
}
func (t *inquiryTxn) record(phase string, ts uint64, err error) {
	e := inquiryEvent{Phase: phase, Index: t.owner.index, Key: t.owner.key, Request: t.request, CommitTs: ts, Entries: t.entries, PayloadBytes: t.bytes}
	if err != nil {
		e.Error = err.Error()
	}
	t.owner.trace.event(e)
}
func (t *inquiryTxn) CommitAt(ts uint64, cb func(error)) error {
	t.record("submit_begin", ts, nil)
	err := t.WriteTxn.CommitAt(ts, func(err error) { t.record("callback", ts, err); cb(err) })
	t.record("submit_return", ts, err)
	return err
}

func (n *node) enqueueApply(entries []raftpb.Entry) {
	n.deltaInquiry.entries(entries, "enqueue_begin")
	n.applyCh <- entries
	n.deltaInquiry.entries(entries, "enqueue_end")
}

// oracleInquiryMessage transfers exactly the same Delta ownership as the
// original pointer channel. IDs are observation metadata, never admission state.
type oracleInquiryMessage struct {
	delta *pb.OracleDelta
	id    uint64
}

type oracleInquiryContextKey struct{}
type oracleInquiryContext struct{ stream, aggregate, attempt uint64 }

func (d *deltaInquiry) oracleStream() uint64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	d.streams++
	id := d.streams
	d.mu.Unlock()
	d.oracle(inquiryEvent{Phase: "oracle_stream_open", Stream: id})
	return id
}

func (d *deltaInquiry) oracle(e inquiryEvent) {
	if d == nil {
		return
	}
	if e.Captured.IsZero() {
		e.Captured = time.Now()
	}
	d.event(e)
}

// Called before ownership transfer or by the sole aggregate owner. Nothing
// reads a mutable Delta after sending it to the consumer.
func (d *deltaInquiry) oracleDelta(e inquiryEvent, delta *pb.OracleDelta) {
	if d == nil {
		return
	}
	if e.Captured.IsZero() {
		e.Captured = time.Now()
	}
	e.Bytes = proto.Size(delta)
	e.Txns = delta.Txns
	e.MaxAssigned = delta.MaxAssigned
	e.ChecksumSequence = delta.ChecksumSequence
	e.ChecksumDigest = raftChecksumDigest(delta)
	e.UnknownFields = len(delta.ProtoReflect().GetUnknown()) != 0
	for _, txn := range delta.Txns {
		if txn != nil && len(txn.ProtoReflect().GetUnknown()) != 0 {
			e.UnknownFields = true
		}
	}
	d.event(e)
}

func (d *deltaInquiry) oracleContext(ctx context.Context, stream, aggregate, attempt uint64) context.Context {
	if d == nil {
		return ctx
	}
	return context.WithValue(ctx, oracleInquiryContextKey{}, oracleInquiryContext{stream, aggregate, attempt})
}

func (d *deltaInquiry) oracleProposal(ctx context.Context, key uint64, bytes int) {
	if d == nil {
		return
	}
	if o, ok := ctx.Value(oracleInquiryContextKey{}).(oracleInquiryContext); ok {
		d.oracle(inquiryEvent{Phase: "oracle_proposal_encoded", Stream: o.stream, Aggregate: o.aggregate, Attempt: o.attempt, Key: key, Bytes: bytes})
	}
}
