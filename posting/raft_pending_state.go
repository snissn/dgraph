/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ErrRaftPendingStateTooLarge identifies only a structurally valid staged
// transaction which exceeds the record limit. The ordered owner can reject
// that private proposal before publishing schema, Oracle or storage effects.
var ErrRaftPendingStateTooLarge = errors.New("canonical pending transaction exceeds record capacity")

const (
	RaftPendingStateFormat = "dgraph-pending-v1"
	// This is a decoder safety bound, not permission to exceed the adapter's
	// smaller total pending byte/count budget or proposal admission limits.
	RaftPendingMaxRecordBytes = 64 << 20
	RaftPendingRowCharge      = 64
	raftPendingMagic          = "DGRPEND1"
	raftPendingHeaderBytes    = 8 + 8 + 8 + 1 + 4 + 4 + 4
)

// MarshalRaftPendingTxn owns its returned bytes. The caller must serialize
// semantic apply while sealing a transaction: the normal cache transition
// preserves pending deltas and conflict versions, but cached Lists are working
// objects and never part of recovery authority. Local lastUpdate is deliberately
// omitted; it controls requests for Zero decisions, not replicated semantics.
func MarshalRaftPendingTxn(txn *Txn) ([]byte, error) {
	return marshalRaftPendingTxn(txn, RaftPendingMaxRecordBytes)
}

func marshalRaftPendingTxn(txn *Txn, recordLimit uint64) ([]byte, error) {
	if txn == nil || txn.StartTs == 0 || txn.cache == nil || txn.cache.startTs != txn.StartTs {
		return nil, fmt.Errorf("invalid pending transaction")
	}
	txn.Update()
	txn.Lock()
	defer txn.Unlock()
	txn.cache.RLock()
	defer txn.cache.RUnlock()
	// Read-only query caches and empty deltas are local working state. Their
	// keysets must not change replicated capacity admission. Every actual delta
	// retains exactly one fixed-width conflict-version slot, including zero.
	deltas := make([]string, 0, len(txn.cache.deltas))
	for key, value := range txn.cache.deltas {
		if len(value) != 0 {
			deltas = append(deltas, key)
		}
	}
	sort.Strings(deltas)
	versions := deltas
	conflicts := make([]uint64, 0, len(txn.conflicts))
	for key := range txn.conflicts {
		if key == 0 {
			return nil, fmt.Errorf("zero pending conflict key")
		}
		conflicts = append(conflicts, key)
	}
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i] < conflicts[j] })
	if uint64(len(deltas)) > math.MaxUint32 || uint64(len(versions)) > math.MaxUint32 || uint64(len(conflicts)) > math.MaxUint32 {
		return nil, fmt.Errorf("pending transaction section count overflows")
	}
	size := uint64(raftPendingHeaderBytes + sha256.Size)
	for _, key := range deltas {
		if err := validatePendingKey(key); err != nil {
			return nil, err
		}
		value := txn.cache.deltas[key]
		if err := validatePendingDelta(value, txn.StartTs); err != nil {
			return nil, err
		}
		size += 8 + uint64(len(key)) + uint64(len(value))
	}
	for _, key := range versions {
		if err := validatePendingKey(key); err != nil {
			return nil, err
		}
		size += 12 + uint64(len(key))
	}
	size += uint64(len(conflicts)) * 8
	if size > recordLimit {
		return nil, fmt.Errorf("%w: %d bytes exceed %d", ErrRaftPendingStateTooLarge, size, recordLimit)
	}
	out := make([]byte, 0, int(size))
	out = append(out, raftPendingMagic...)
	out = binary.BigEndian.AppendUint64(out, txn.StartTs)
	out = binary.BigEndian.AppendUint64(out, txn.cache.commitTs)
	if atomic.LoadUint32(&txn.shouldAbort) != 0 {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(deltas)))
	out = binary.BigEndian.AppendUint32(out, uint32(len(versions)))
	out = binary.BigEndian.AppendUint32(out, uint32(len(conflicts)))
	for _, key := range deltas {
		out = appendPendingBytes(out, []byte(key))
		out = appendPendingBytes(out, txn.cache.deltas[key])
	}
	for _, key := range versions {
		out = appendPendingBytes(out, []byte(key))
		out = binary.BigEndian.AppendUint64(out, txn.cache.maxVersions[key])
	}
	for _, key := range conflicts {
		out = binary.BigEndian.AppendUint64(out, key)
	}
	sum := sha256.Sum256(out)
	return append(out, sum[:]...), nil
}

func appendPendingBytes(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

// ValidateRaftPendingTxn checks the entire canonical record without retaining a
// transaction or allocating maps from untrusted section counts.
func ValidateRaftPendingTxn(data []byte) (uint64, error) {
	txn, err := decodeRaftPendingTxn(data, false)
	if err != nil {
		return 0, err
	}
	return txn.StartTs, nil
}

// RaftPendingTxnCharge is the replicated logical admission charge, not a heap
// or filesystem size estimate. Timestamp/conflict-version values occupy fixed
// widths and query-only cache keys are omitted. Equivalent replicas therefore
// charge the same delta/key/conflict content despite different local P values.
func RaftPendingTxnCharge(data []byte) (uint64, error) {
	if _, err := ValidateRaftPendingTxn(data); err != nil {
		return 0, err
	}
	return uint64(len(data)) + RaftPendingRowCharge, nil
}

// CloneRaftPendingTxn stages ordered mutation processing without publishing a
// new Oracle transaction. All maps/serialized deltas are owned by the clone;
// materialized Lists are rebuilt through the maintained posting read path.
func CloneRaftPendingTxn(startTs uint64) (*Txn, error) {
	if startTs == 0 {
		return nil, fmt.Errorf("zero pending transaction timestamp")
	}
	txn := Oracle().GetTxn(startTs)
	if txn == nil {
		return NewTxn(startTs), nil
	}
	data, err := MarshalRaftPendingTxn(txn)
	if err != nil {
		return nil, err
	}
	return decodeRaftPendingTxn(data, true)
}

// InstallRaftPendingTxn is used only after the ordered owner has validated and
// stored this staged canonical state. It does not publish storage, a certificate
// or a Raft frontier, and must not be called concurrently with semantic apply.
func InstallRaftPendingTxn(txn *Txn) error {
	if txn == nil || txn.StartTs == 0 || txn.cache == nil || txn.cache.startTs != txn.StartTs {
		return fmt.Errorf("invalid staged pending transaction")
	}
	o := Oracle()
	o.Lock()
	txn.lastUpdate = time.Now()
	o.pendingTxns[txn.StartTs] = txn
	o.Unlock()
	return nil
}

// RaftPendingPredicateInUse consults only the canonical nonempty delta keyset.
// Query-only cache entries cannot choose an ordered schema outcome. The caller
// must exclude semantic apply, as for the other pending-state operations.
func RaftPendingPredicateInUse(attr string) bool {
	o := Oracle()
	o.RLock()
	defer o.RUnlock()
	for _, txn := range o.pendingTxns {
		txn.Lock()
		txn.cache.RLock()
		found := false
		for key, value := range txn.cache.deltas {
			if len(value) == 0 {
				continue
			}
			parsed, err := x.Parse([]byte(key))
			if err == nil && parsed.Attr == attr {
				found = true
				break
			}
		}
		txn.cache.RUnlock()
		txn.Unlock()
		if found {
			return true
		}
	}
	return false
}

// RestoreRaftPendingTxns validates ALL records before replacing Oracle state.
// The caller supplies the complete exact-root namespace under startup/install
// exclusion and enforces its total byte/count budget before calling this.
func RestoreRaftPendingTxns(records [][]byte) error {
	pending := make(map[uint64]*Txn)
	for _, data := range records {
		txn, err := decodeRaftPendingTxn(data, true)
		if err != nil {
			return err
		}
		if _, duplicate := pending[txn.StartTs]; duplicate {
			return fmt.Errorf("duplicate pending transaction %d", txn.StartTs)
		}
		pending[txn.StartTs] = txn
	}
	o := Oracle()
	o.Lock()
	o.pendingTxns = pending
	o.Unlock()
	return nil
}

type pendingStateReader struct {
	data []byte
	err  error
}

func (r *pendingStateReader) take(n uint64) []byte {
	if r.err != nil || n > uint64(len(r.data)) {
		r.err = fmt.Errorf("truncated pending transaction")
		return nil
	}
	out := r.data[:int(n)]
	r.data = r.data[int(n):]
	return out
}
func (r *pendingStateReader) u64() uint64 {
	data := r.take(8)
	if data == nil {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}
func (r *pendingStateReader) u32() uint32 {
	data := r.take(4)
	if data == nil {
		return 0
	}
	return binary.BigEndian.Uint32(data)
}
func (r *pendingStateReader) blob() []byte { return r.take(uint64(r.u32())) }

func decodeRaftPendingTxn(data []byte, materialize bool) (*Txn, error) {
	if len(data) < raftPendingHeaderBytes+sha256.Size || len(data) > RaftPendingMaxRecordBytes {
		return nil, fmt.Errorf("invalid pending transaction record size")
	}
	body := data[:len(data)-sha256.Size]
	sum := sha256.Sum256(body)
	if !bytes.Equal(sum[:], data[len(body):]) {
		return nil, fmt.Errorf("pending transaction checksum mismatch")
	}
	r := pendingStateReader{data: body}
	if string(r.take(8)) != raftPendingMagic {
		return nil, fmt.Errorf("unsupported pending transaction format")
	}
	start, commit := r.u64(), r.u64()
	flag := r.take(1)[0]
	deltaCount, versionCount, conflictCount := r.u32(), r.u32(), r.u32()
	if start == 0 || flag > 1 || versionCount != deltaCount || uint64(deltaCount)*8+uint64(versionCount)*12+uint64(conflictCount)*8 > uint64(len(r.data)) {
		return nil, fmt.Errorf("invalid pending transaction header/counts")
	}
	txn := &Txn{StartTs: start}
	if materialize {
		txn = NewTxn(start)
		txn.cache.commitTs = commit
		atomic.StoreUint32(&txn.shouldAbort, uint32(flag))
	}
	deltaKeys := pendingStateReader{data: r.data}
	for section, count := range []uint32{deltaCount, versionCount} {
		previous := ""
		for i := uint32(0); i < count; i++ {
			key := string(r.blob())
			if r.err != nil {
				return nil, r.err
			}
			if err := validatePendingKey(key); err != nil {
				return nil, err
			}
			if i > 0 && key <= previous {
				return nil, fmt.Errorf("unordered or duplicate pending key")
			}
			previous = key
			if section == 0 {
				value := r.blob()
				if r.err != nil {
					return nil, r.err
				}
				if err := validatePendingDelta(value, start); err != nil {
					return nil, err
				}
				if len(value) == 0 {
					return nil, fmt.Errorf("empty delta in canonical pending state")
				}
				if materialize {
					txn.cache.deltas[key] = bytes.Clone(value)
				}
			} else {
				if key != string(deltaKeys.blob()) {
					return nil, fmt.Errorf("pending conflict-version keyset differs from deltas")
				}
				deltaKeys.blob()
				version := r.u64()
				if materialize {
					txn.cache.maxVersions[key] = version
				}
			}
		}
	}
	var previous uint64
	for i := uint32(0); i < conflictCount; i++ {
		key := r.u64()
		if key == 0 || (i > 0 && key <= previous) {
			return nil, fmt.Errorf("unordered, duplicate or zero pending conflict key")
		}
		previous = key
		if materialize {
			txn.addConflictKey(key)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	if len(r.data) != 0 {
		return nil, fmt.Errorf("trailing pending transaction bytes")
	}
	return txn, nil
}

func validatePendingKey(key string) error {
	if len(key) < 12 || len(key) > math.MaxUint16 || key[0] != x.DefaultPrefix {
		return fmt.Errorf("invalid pending posting key size/prefix")
	}
	// x.Parse assumes an attribute is followed by a byte type. Check that
	// boundary first so malformed recovery input cannot panic the parser.
	attrLen := int(binary.BigEndian.Uint16([]byte(key[9:11])))
	if len(key) < 12+attrLen {
		return fmt.Errorf("truncated pending posting key")
	}
	pk, err := x.Parse([]byte(key))
	if err != nil || !(pk.IsData() || pk.IsIndex() || pk.IsReverse() || pk.IsCountOrCountRev()) {
		return fmt.Errorf("invalid pending posting key %x", key)
	}
	var canonical []byte
	switch {
	case pk.IsData():
		canonical = x.DataKey(pk.Attr, pk.Uid)
	case pk.IsIndex():
		canonical = x.IndexKey(pk.Attr, pk.Term)
	case pk.IsReverse():
		canonical = x.ReverseKey(pk.Attr, pk.Uid)
	default:
		canonical = x.CountKey(pk.Attr, pk.Count, pk.IsCountRev())
	}
	if !bytes.Equal(canonical, []byte(key)) {
		return fmt.Errorf("noncanonical pending posting key")
	}
	return nil
}

// Pending delta lists contain only repeated Posting messages, never complete
// packs, split descriptors or a committed timestamp. Validate one Posting at a
// time so a long delta does not allocate an entire second in-memory List just
// to validate or clone serialized recovery authority.
func validatePendingDelta(data []byte, startTs uint64) error {
	for len(data) != 0 {
		if data[0] != 0x12 { // PostingList.postings, canonical bytes field tag.
			return fmt.Errorf("noncanonical pending posting delta")
		}
		value, n := protowire.ConsumeBytes(data[1:])
		if n < 0 || n != len(value)+protowire.SizeVarint(uint64(len(value))) {
			return fmt.Errorf("malformed pending posting delta")
		}
		var p pb.Posting
		if err := (proto.UnmarshalOptions{RecursionLimit: 32}).Unmarshal(value, &p); err != nil {
			return fmt.Errorf("invalid pending posting: %w", err)
		}
		if p.CommitTs != 0 || (p.StartTs != 0 && p.StartTs != startTs) || len(p.ProtoReflect().GetUnknown()) != 0 {
			return fmt.Errorf("invalid pending posting timestamp/format")
		}
		canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&p)
		if err != nil || !bytes.Equal(canonical, value) {
			return fmt.Errorf("noncanonical pending posting payload")
		}
		data = data[n+1:]
	}
	return nil
}
