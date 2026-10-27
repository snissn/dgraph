/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type postingBatchTestStore struct {
	Store
	values                  map[string]PostingReadResult
	keys                    [][]byte
	batchCalls, scalarCalls int
	total                   error
	short                   bool
	before                  func()
}

func (s *postingBatchTestStore) ReadPostingBatch(keys [][]byte, _ uint64) ([]PostingReadResult, error) {
	s.batchCalls++
	if s.before != nil {
		s.before()
	}
	out := make([]PostingReadResult, len(keys))
	for i, key := range keys {
		s.keys = append(s.keys, append([]byte(nil), key...))
		out[i] = s.values[string(key)]
	}
	if s.short {
		out = out[:0]
	}
	return out, s.total
}
func (s *postingBatchTestStore) NewReadTxn(uint64) ReadTxn { return postingBatchTestRead{s: s} }

type postingBatchTestRead struct {
	ReadTxn
	s *postingBatchTestStore
}

func (r postingBatchTestRead) Get(key []byte) (Item, error) {
	r.s.scalarCalls++
	v := r.s.values[string(key)]
	if v.Err != nil {
		return nil, v.Err
	}
	return &treeDBItem{value: v.Value}, nil
}
func (postingBatchTestRead) Discard() {}

func scalarPostingBytes(t *testing.T, value string, op uint32) []byte {
	t.Helper()
	v, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Value: []byte(value), ValType: pb.Posting_STRING, Op: op}}})
	require.NoError(t, err)
	return v
}

func TestPostingBatchLocalPrecedenceDeletionAndOrderedErrors(t *testing.T) {
	keys := make([][]byte, 8)
	for i := range keys {
		keys[i] = x.DataKey(x.AttrInRootNamespace("batch-local"), uint64(i+1))
	}
	s := &postingBatchTestStore{values: map[string]PostingReadResult{}}
	for _, key := range keys {
		s.values[string(key)] = PostingReadResult{Value: scalarPostingBytes(t, "disk", Set)}
	}
	s.values[string(keys[3])] = PostingReadResult{Value: []byte{0xff}}
	s.values[string(keys[4])] = PostingReadResult{Err: badger.ErrKeyNotFound}
	s.values[string(keys[5])] = PostingReadResult{Value: scalarPostingBytes(t, x.Star, Del)}
	s.values[string(keys[6])] = PostingReadResult{Value: scalarPostingBytes(t, "deleted", Del)}
	lc := NewLocalCache(10)
	lc.privateStore = s
	lc.deltas[string(keys[0])] = scalarPostingBytes(t, "delta", Set)
	lc.plists[string(keys[0])] = &List{plist: &pb.PostingList{Postings: []*pb.Posting{{Value: []byte("shadowed")}}}}
	lc.plists[string(keys[1])] = &List{plist: &pb.PostingList{Postings: []*pb.Posting{{Value: []byte("plist")}}}}
	lc.deltas[string(keys[2])] = []byte{0xff}
	s.before = func() { require.True(t, lc.TryLock(), "physical read must not hold LocalCache lock"); lc.Unlock() }
	out, err := lc.GetSinglePostings(keys)
	require.NoError(t, err)
	require.Equal(t, keys[3:], s.keys, "only unresolved storage misses may be batched")
	require.Equal(t, []byte("delta"), out[0].Posting.Postings[0].Value)
	require.Equal(t, []byte("plist"), out[1].Posting.Postings[0].Value)
	require.Error(t, out[2].Err)
	require.Error(t, out[3].Err)
	require.Nil(t, out[4].Posting)
	require.NoError(t, out[4].Err)
	require.Nil(t, out[5].Posting)
	require.NoError(t, out[5].Err)
	require.Empty(t, out[6].Posting.Postings)
	require.NoError(t, out[6].Err)
	require.Equal(t, []byte("disk"), out[7].Posting.Postings[0].Value)
	require.Zero(t, s.scalarCalls, "an item error must never cause a scalar retry")
}

func TestPostingBatchTotalErrorInvalidatesReturnedValuesWithoutRetry(t *testing.T) {
	want := errors.New("read-cut close failure")
	s := &postingBatchTestStore{values: map[string]PostingReadResult{"disk": {Value: scalarPostingBytes(t, "value", Set)}}, total: want}
	lc := NewLocalCache(10)
	lc.privateStore = s
	lc.deltas["local"] = scalarPostingBytes(t, "local", Set)
	out, err := lc.GetSinglePostings([][]byte{[]byte("local"), []byte("disk"), []byte("disk")})
	require.NoError(t, err)
	require.NoError(t, out[0].Err)
	for _, r := range out[1:] {
		require.ErrorIs(t, r.Err, want)
		require.Nil(t, r.Posting)
	}
	require.Equal(t, 1, s.batchCalls)
	require.Zero(t, s.scalarCalls)
	s.total = nil
	s.short = true
	out, err = lc.GetSinglePostings([][]byte{[]byte("disk"), []byte("disk")})
	require.NoError(t, err)
	require.ErrorContains(t, out[0].Err, "0 results for 2 keys")
	require.Zero(t, s.scalarCalls)
}

func TestPostingBatchBoundsSingletonAndMetricsFallback(t *testing.T) {
	key := x.DataKey(x.AttrInRootNamespace("batch-metrics"), 1)
	s := &postingBatchTestStore{values: map[string]PostingReadResult{string(key): {Value: scalarPostingBytes(t, "value", Set)}}}
	lc := NewLocalCache(10)
	lc.privateStore = s
	require.Equal(t, mvcc.MaxGetManyAtKeys, MaxPostingBatchKeys)
	_, err := lc.GetSinglePostings(make([][]byte, MaxPostingBatchKeys+1))
	require.Error(t, err)
	out, err := lc.GetSinglePostings(nil)
	require.NoError(t, err)
	require.Empty(t, out)
	_, err = lc.GetSinglePostings([][]byte{key})
	require.NoError(t, err)
	require.Equal(t, 1, s.scalarCalls)
	require.Zero(t, s.batchCalls)
	old := EnableDetailedMetrics
	EnableDetailedMetrics = true
	t.Cleanup(func() { EnableDetailedMetrics = old })
	require.False(t, lc.SupportsPostingBatch())
	_, err = lc.GetSinglePostings([][]byte{key, key})
	require.NoError(t, err)
	require.Equal(t, 3, s.scalarCalls)
	require.Zero(t, s.batchCalls)
	_, ok := Store(NewBadgerStore(ps)).(PostingBatchReader)
	require.False(t, ok)
	bus := NewCommitEventBus(2)
	defer bus.Close()
	_, ok = WithCommitEvents(NewBadgerStore(ps), bus).(PostingBatchReader)
	require.False(t, ok)
	_, ok = WithCommitEvents(s, bus).(PostingBatchReader)
	require.True(t, ok)
}

func TestTreeDBPostingBatchVersionsErrorsAndOwnedValues(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	s, openErr := OpenTreeDBStore(opts, TreeDBCommitDurable)
	require.NoError(t, openErr)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	a, b, bad := []byte("a"), []byte("b"), []byte("bad-envelope")
	commitStoreEntry(t, s, 2, Entry{Key: a, Value: []byte("old")})
	commitStoreEntry(t, s, 3, Entry{Key: b, Value: []byte("b")})
	commitStoreEntry(t, s, 7, Entry{Key: a, Value: []byte("new")})
	require.NoError(t, s.mvcc.CommitAt(8, []mvcc.Mutation{{Key: b, Delete: true}, {Key: bad, Value: []byte("corrupt")}}, mvcc.CommitDurable))
	keys := [][]byte{a, b, []byte("missing"), a, bad}
	out, err := s.ReadPostingBatch(keys, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), out[0].Value)
	require.Equal(t, []byte("b"), out[1].Value)
	require.ErrorIs(t, out[2].Err, badger.ErrKeyNotFound)
	require.Equal(t, []byte("old"), out[3].Value)
	out[0].Value[0] = 'X'
	require.Equal(t, []byte("old"), out[3].Value, "duplicates must own independent values")
	out, err = s.ReadPostingBatch(keys, 9)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), out[0].Value)
	require.ErrorIs(t, out[1].Err, badger.ErrKeyNotFound)
	require.ErrorIs(t, out[4].Err, ErrTreeDBEnvelope)
	read := s.NewReadTxn(9).(*treeDBReadTxn)
	read.Discard()
	_, err = read.readPostingBatch(keys)
	require.ErrorIs(t, err, treedb.ErrClosed)
	_, err = s.ReadPostingBatch(keys, 0)
	require.ErrorIs(t, err, mvcc.ErrZeroTimestamp)
	_, err = s.ReadPostingBatch(make([][]byte, 33), 9)
	require.ErrorIs(t, err, mvcc.ErrBatchTooLarge)
	require.NoError(t, s.AdvanceDiscardFloor(4))
	_, err = s.ReadPostingBatch(keys, 4)
	require.ErrorIs(t, err, mvcc.ErrReadBeforeDiscardFloor)
	require.NoError(t, s.Close())
	require.Equal(t, []byte("new"), out[0].Value)
	_, err = s.ReadPostingBatch(keys, 9)
	require.ErrorIs(t, err, treedb.ErrClosed)
}
