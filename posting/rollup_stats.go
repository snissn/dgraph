/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"strconv"
	"sync/atomic"

	bpb "github.com/dgraph-io/badger/v4/pb"
)

// Existing rollup producer/completion boundaries only. Native errors can have a
// committed prefix; unconfirmed counts must not be reported as successful KVs.
var rollupWork struct {
	producedKVs, producedBytes, submittedKVs, submittedBytes       atomic.Uint64
	completedKVs, completedBytes, unconfirmedKVs, unconfirmedBytes atomic.Uint64
	nativeCompletedKVs, eventPressureCuts, readDependencyCuts      atomic.Uint64
	readinessWaits, nativeKVs                                      atomic.Uint64
	nativeChunks, nativeCompleted                                  atomic.Uint64
}

func rollupBytes(kvs []*bpb.KV) uint64 {
	var n uint64
	for _, kv := range kvs {
		n += uint64(len(kv.Key) + len(kv.Value) + treeDBEnvelopeHeader)
	}
	return n
}
func rollupProduced(kvs []*bpb.KV) {
	rollupWork.producedKVs.Add(uint64(len(kvs)))
	rollupWork.producedBytes.Add(rollupBytes(kvs))
}
func rollupSubmitted(kvs []*bpb.KV) {
	rollupWork.submittedKVs.Add(uint64(len(kvs)))
	rollupWork.submittedBytes.Add(rollupBytes(kvs))
}
func rollupCompleted(n, bytes uint64, err error) {
	if err == nil {
		rollupWork.completedKVs.Add(n)
		rollupWork.completedBytes.Add(bytes)
	} else {
		rollupWork.unconfirmedKVs.Add(n)
		rollupWork.unconfirmedBytes.Add(bytes)
	}
}

// RollupStats is a detached diagnostic snapshot shared by both backends. Bytes
// charge key + value + posting envelope, not physical WAL bytes or heap capacity.
// Reads are individually atomic, not a barrier or a coherent end-of-run cut.
func RollupStats() map[string]string {
	m := map[string]string{}
	counters := [...]struct {
		name  string
		value *atomic.Uint64
	}{
		{"produced_kvs", &rollupWork.producedKVs},
		{"produced_bytes", &rollupWork.producedBytes},
		{"submitted_kvs", &rollupWork.submittedKVs},
		{"submitted_bytes", &rollupWork.submittedBytes},
		{"completed_kvs", &rollupWork.completedKVs},
		{"completed_bytes", &rollupWork.completedBytes},
		{"unconfirmed_kvs", &rollupWork.unconfirmedKVs},
		{"unconfirmed_bytes", &rollupWork.unconfirmedBytes},
		{"readiness_waits", &rollupWork.readinessWaits},
		{"native_completed_kvs", &rollupWork.nativeCompletedKVs},
		{"event_pressure_cuts", &rollupWork.eventPressureCuts},
		{"read_dependency_cuts", &rollupWork.readDependencyCuts},
		{"native_kvs", &rollupWork.nativeKVs},
		{"native_chunks", &rollupWork.nativeChunks},
		{"native_completed_chunks", &rollupWork.nativeCompleted},
	}
	for _, counter := range counters {
		m["dgraph.rollup."+counter.name+"_total"] = strconv.FormatUint(counter.value.Load(), 10)
	}
	return m
}
