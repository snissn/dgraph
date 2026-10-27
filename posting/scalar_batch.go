/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"fmt"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"google.golang.org/protobuf/proto"
)

// SinglePostingResult keeps prefetched errors at their original input position.
// A caller must consume results in order, including its own value conversion,
// rather than allow a later read error to replace an earlier conversion error.
type SinglePostingResult struct {
	Posting *pb.PostingList
	Err     error
}

// SupportsPostingBatch leaves the default backend and per-key latency metrics
// on their maintained scalar route. No cut is retained in this shared cache.
func (lc *LocalCache) SupportsPostingBatch() bool {
	_, ok := lc.sourceStore().(PostingBatchReader)
	return ok && !EnableDetailedMetrics
}

// GetSinglePostings is the bounded counterpart of GetSinglePosting, only for
// callers already eligible for that scalar shortcut. Resolve local state first;
// physical work runs without a LocalCache lock and never populates the cache.
// Total producer errors are retained at every storage-miss position, including
// when the producer also returned values. There is no retry after any error.
func (lc *LocalCache) GetSinglePostings(keys [][]byte) ([]SinglePostingResult, error) {
	if len(keys) > MaxPostingBatchKeys {
		return nil, fmt.Errorf("posting scalar batch: %d keys exceeds limit %d", len(keys), MaxPostingBatchKeys)
	}
	out := make([]SinglePostingResult, len(keys))
	reader, ok := lc.sourceStore().(PostingBatchReader)
	if len(keys) < 2 || !ok || EnableDetailedMetrics {
		for i, key := range keys {
			out[i].Posting, out[i].Err = lc.GetSinglePosting(key)
		}
		return out, nil
	}
	var misses [MaxPostingBatchKeys][]byte
	var positions [MaxPostingBatchKeys]int
	n := 0
	for i, key := range keys {
		pl, err := lc.singlePostingFromCache(key)
		if pl != nil || err != nil {
			out[i].Posting, out[i].Err = filterSinglePosting(pl, err)
			continue
		}
		misses[n], positions[n] = key, i
		n++
	}
	if n == 0 {
		return out, nil
	}
	values, err := reader.ReadPostingBatch(misses[:n], lc.startTs)
	if err == nil && len(values) != n {
		err = fmt.Errorf("posting scalar batch: got %d results for %d keys", len(values), n)
	}
	for j, i := range positions[:n] {
		if err != nil {
			out[i].Err = err
			continue
		}
		pl := &pb.PostingList{}
		itemErr := values[j].Err
		if itemErr == nil {
			itemErr = proto.Unmarshal(values[j].Value, pl)
		}
		out[i].Posting, out[i].Err = filterSinglePosting(pl, itemErr)
	}
	return out, nil
}
