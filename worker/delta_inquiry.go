/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/golang/glog"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Diagnostic-only: synchronous logging perturbs scheduling and excludes a run
// from performance acceptance. No queue is consumed or work submitted early.
type deltaInquiry struct {
	mu      sync.Mutex
	seq     uint64
	started time.Time
	emit    func(inquiryEvent)
}
type inquiryEvent struct {
	Version      int               `json:"version"`
	ElapsedNS    int64             `json:"elapsed_ns"`
	Sequence     uint64            `json:"sequence"`
	At           string            `json:"at"`
	Phase        string            `json:"phase"`
	Index        uint64            `json:"index"`
	Key          uint64            `json:"key"`
	Kind         string            `json:"kind,omitempty"`
	Bytes        int               `json:"bytes,omitempty"`
	Txns         []*pb.TxnStatus   `json:"txns,omitempty"`
	StartTs      uint64            `json:"start_ts,omitempty"`
	MaxAssigned  uint64            `json:"max_assigned,omitempty"`
	CommitTs     uint64            `json:"commit_ts,omitempty"`
	Request      uint64            `json:"request,omitempty"`
	Entries      uint64            `json:"entries,omitempty"`
	PayloadBytes uint64            `json:"payload_bytes,omitempty"`
	Error        string            `json:"error,omitempty"`
	Build        map[string]string `json:"build,omitempty"`
	Detail       string            `json:"detail,omitempty"`
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
	d.seq++
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
