/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func pendingTestTxn(t *testing.T, start uint64) *Txn {
	t.Helper()
	txn := NewTxn(start)
	attr := x.AttrInRootNamespace("pending-canonical")
	for _, uid := range []uint64{2, 1} {
		key := x.DataKey(attr, uid)
		data, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Uid: uid + 10, Op: Set, StartTs: start, PostingType: pb.Posting_REF}}})
		require.NoError(t, err)
		list := NewList(key, &pb.PostingList{}, 3)
		list.maxTs = uid + 2
		list.setMutation(start, data)
		txn.Store(list)
		txn.addConflictKey(uid + 100)
	}
	return txn
}

func TestRaftPendingStateCanonicalRoundTripAndIsolatedStaging(t *testing.T) {
	Oracle().ResetTxns()
	t.Cleanup(func() { Oracle().ResetTxns() })
	txn := pendingTestTxn(t, 50)
	atomic.StoreUint32(&txn.shouldAbort, 1)
	encoded, err := MarshalRaftPendingTxn(txn)
	require.NoError(t, err)
	require.Empty(t, txn.cache.plists, "maintained cache seal removes disposable Lists")
	start, err := ValidateRaftPendingTxn(encoded)
	require.NoError(t, err)
	require.Equal(t, uint64(50), start)
	charge, err := RaftPendingTxnCharge(encoded)
	require.NoError(t, err)
	require.Equal(t, uint64(len(encoded)+RaftPendingRowCharge), charge)
	require.NoError(t, RestoreRaftPendingTxns([][]byte{encoded}))
	restored := Oracle().GetTxn(50)
	require.True(t, restored.ShouldAbort())
	require.Empty(t, restored.cache.plists)
	require.Equal(t, txn.cache.deltas, restored.cache.deltas)
	require.Equal(t, txn.cache.maxVersions, restored.cache.maxVersions)
	require.Equal(t, txn.conflicts, restored.conflicts)
	second, err := MarshalRaftPendingTxn(restored)
	require.NoError(t, err)
	require.Equal(t, encoded, second)
	clone, err := CloneRaftPendingTxn(50)
	require.NoError(t, err)
	key := string(x.DataKey(x.AttrInRootNamespace("pending-canonical"), 1))
	clone.cache.deltas[key][0] ^= 1
	clone.cache.maxVersions[key] = 999
	clone.addConflictKey(999)
	require.NotEqual(t, clone.cache.deltas[key], restored.cache.deltas[key])
	require.NotEqual(t, clone.cache.maxVersions[key], restored.cache.maxVersions[key])
	require.NotContains(t, restored.conflicts, uint64(999))
	newTxn, err := CloneRaftPendingTxn(70)
	require.NoError(t, err)
	require.Nil(t, Oracle().GetTxn(70), "staging cannot register a transaction")
	require.NoError(t, InstallRaftPendingTxn(newTxn))
	require.Same(t, newTxn, Oracle().GetTxn(70))
	encoded[0] ^= 1
	third, err := MarshalRaftPendingTxn(restored)
	require.NoError(t, err)
	require.Equal(t, second, third, "restore must not alias input bytes")
}

func TestRaftPendingStateChargeIndependentOfPhysicalVersionsAndReadCaches(t *testing.T) {
	txn := pendingTestTxn(t, 50)
	a, err := MarshalRaftPendingTxn(txn)
	require.NoError(t, err)
	for key := range txn.cache.maxVersions {
		txn.cache.maxVersions[key] = math.MaxUint64
	}
	// Local query history can add read-only version keys or empty deltas. None
	// has a replicated mutation effect or may consume the logical capacity.
	txn.cache.maxVersions["local-only-read-key"] = 123
	txn.cache.deltas["local-empty-delta"] = nil
	b, err := MarshalRaftPendingTxn(txn)
	require.NoError(t, err)
	require.NotEqual(t, a, b, "exact conflict-version state remains preserved")
	chargeA, err := RaftPendingTxnCharge(a)
	require.NoError(t, err)
	chargeB, err := RaftPendingTxnCharge(b)
	require.NoError(t, err)
	require.Equal(t, chargeA, chargeB)
	for _, remaining := range []uint64{chargeA - 1, chargeA, chargeA + 1} {
		require.Equal(t, chargeA <= remaining, chargeB <= remaining, "replicas near the capacity boundary must agree")
	}
	decoded, err := decodeRaftPendingTxn(b, true)
	require.NoError(t, err)
	require.Len(t, decoded.cache.maxVersions, 2)
	require.Len(t, decoded.cache.deltas, 2)
	require.NotContains(t, decoded.cache.maxVersions, "local-only-read-key")
	// Sorting map sections and conflicts makes insertion order irrelevant.
	reversed := NewTxn(50)
	for _, key := range sortedPendingTestKeys(decoded.cache.deltas) {
		reversed.cache.deltas[key] = bytes.Clone(decoded.cache.deltas[key])
		reversed.cache.maxVersions[key] = decoded.cache.maxVersions[key]
	}
	reversed.addConflictKey(102)
	reversed.addConflictKey(101)
	c, err := MarshalRaftPendingTxn(reversed)
	require.NoError(t, err)
	require.Equal(t, b, c)
}

func TestRaftPendingStatePredicateEligibilityIgnoresQueryOnlyKeys(t *testing.T) {
	Oracle().ResetTxns()
	t.Cleanup(Oracle().ResetTxns)
	txn := pendingTestTxn(t, 50)
	_, err := MarshalRaftPendingTxn(txn)
	require.NoError(t, err)
	queryAttr := x.AttrInRootNamespace("query-only")
	txn.cache.deltas[string(x.DataKey(queryAttr, 1))] = nil
	txn.cache.maxVersions[string(x.DataKey(queryAttr, 1))] = 40
	require.NoError(t, InstallRaftPendingTxn(txn))
	require.True(t, RaftPendingPredicateInUse(x.AttrInRootNamespace("pending-canonical")))
	require.False(t, RaftPendingPredicateInUse(queryAttr), "local query history cannot reject ordered schema")
}

func sortedPendingTestKeys(values map[string][]byte) []string {
	var keys []string
	for key := range values {
		keys = append(keys, key)
	}
	if len(keys) == 2 && keys[0] < keys[1] {
		keys[0], keys[1] = keys[1], keys[0]
	}
	return keys
}

func pendingReseal(data []byte) []byte {
	data = bytes.Clone(data)
	sum := sha256.Sum256(data[:len(data)-sha256.Size])
	copy(data[len(data)-sha256.Size:], sum[:])
	return data
}

func TestRaftPendingStateMalformedRestoreIsAtomic(t *testing.T) {
	Oracle().ResetTxns()
	t.Cleanup(func() { Oracle().ResetTxns() })
	existing := Oracle().RegisterStartTs(777)
	data, err := MarshalRaftPendingTxn(pendingTestTxn(t, 50))
	require.NoError(t, err)
	for n := 0; n < len(data); n++ {
		_, err := ValidateRaftPendingTxn(data[:n])
		require.Error(t, err, "truncation at byte %d", n)
	}
	for name, mutate := range map[string]func([]byte){
		"format":                   func(b []byte) { b[0] ^= 1 },
		"zero-start":               func(b []byte) { clear(b[8:16]) },
		"abort-flag":               func(b []byte) { b[24] = 2 },
		"count-overflow":           func(b []byte) { binary.BigEndian.PutUint32(b[25:29], math.MaxUint32) },
		"missing-conflict-version": func(b []byte) { binary.BigEndian.PutUint32(b[29:33], 0) },
		"key-length-overflow":      func(b []byte) { binary.BigEndian.PutUint32(b[37:41], math.MaxUint32) },
		"duplicate-delta-key": func(b []byte) {
			r := pendingStateReader{data: b[raftPendingHeaderBytes : len(b)-sha256.Size]}
			first := r.blob()
			r.blob()
			copy(r.blob(), first)
		},
		"wrong-version-keyset": func(b []byte) {
			r := pendingStateReader{data: b[raftPendingHeaderBytes : len(b)-sha256.Size]}
			for i := 0; i < 4; i++ {
				r.blob()
			}
			key := r.blob()
			key[len(key)-1] = 4
		},
		"wrong-pending-start": func(b []byte) {
			r := pendingStateReader{data: b[raftPendingHeaderBytes : len(b)-sha256.Size]}
			r.blob()
			value := r.blob()
			var list pb.PostingList
			require.NoError(t, proto.Unmarshal(value, &list))
			list.Postings[0].StartTs++
			changed, err := proto.Marshal(&list)
			require.NoError(t, err)
			require.Len(t, changed, len(value))
			copy(value, changed)
		},
		"duplicate-conflict-key": func(b []byte) {
			offset := len(b) - sha256.Size - 16
			copy(b[offset+8:offset+16], b[offset:offset+8])
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := bytes.Clone(data)
			mutate(bad)
			bad = pendingReseal(bad)
			require.Error(t, RestoreRaftPendingTxns([][]byte{data, bad}))
			require.Equal(t, 1, Oracle().NumPendingTxns())
			require.Same(t, existing, Oracle().GetTxn(777))
		})
	}
	require.ErrorContains(t, RestoreRaftPendingTxns([][]byte{data, data}), "duplicate pending")
	require.Same(t, existing, Oracle().GetTxn(777))
	badChecksum := bytes.Clone(data)
	badChecksum[len(badChecksum)-1] ^= 1
	_, err = ValidateRaftPendingTxn(badChecksum)
	require.ErrorContains(t, err, "checksum")
	trailing := append(bytes.Clone(data[:len(data)-sha256.Size]), 0)
	trailing = append(trailing, make([]byte, sha256.Size)...)
	_, err = ValidateRaftPendingTxn(pendingReseal(trailing))
	require.ErrorContains(t, err, "trailing")
	for i := 0; i < len(data)-sha256.Size; i++ {
		mutated := bytes.Clone(data)
		mutated[i] ^= 0x80
		require.NotPanics(t, func() { _, _ = ValidateRaftPendingTxn(pendingReseal(mutated)) }, "byte %d", i)
	}
	for n := 0; n <= 14; n++ {
		require.NotPanics(t, func() { require.Error(t, validatePendingKey(string(make([]byte, n)))) })
	}
	_, err = CloneRaftPendingTxn(0)
	require.Error(t, err)
	require.Error(t, InstallRaftPendingTxn(nil))
}

func TestRaftPendingStateCapacityClassificationRequiresValidStagedState(t *testing.T) {
	txn := pendingTestTxn(t, 50)
	_, err := marshalRaftPendingTxn(txn, 1)
	require.ErrorIs(t, err, ErrRaftPendingStateTooLarge)
	txn.cache.deltas["malformed-key"] = []byte{0xff}
	_, err = marshalRaftPendingTxn(txn, 1)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRaftPendingStateTooLarge, "unknown state must never become a semantic capacity rejection")
}
