/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Embed only the maintained scalar Store interface, so the scalar control cannot
// accidentally inherit the optional capability from its source.
type scalarBatchControl struct {
	posting.Store
	mu      sync.Mutex
	scalar  int
	value   []byte
	values  map[string][]byte
	errors  map[string]error
	history int
}

func (s *scalarBatchControl) NewReadTxn(uint64) posting.ReadTxn {
	return scalarBatchRead{store: s}
}
func (*scalarBatchControl) IsClosed() bool { return false }

type scalarBatchRead struct {
	posting.ReadTxn
	store *scalarBatchControl
}

func (r scalarBatchRead) Get(key []byte) (posting.Item, error) {
	r.store.mu.Lock()
	r.store.scalar++
	r.store.mu.Unlock()
	if err := r.store.errors[string(key)]; err != nil {
		return nil, err
	}
	value := r.store.value
	if r.store.values != nil {
		value = r.store.values[string(key)]
	}
	return scalarBatchItem{value: value}, nil
}
func (r scalarBatchRead) Discard() {}
func (r scalarBatchRead) NewKeyIterator([]byte, posting.IteratorOptions) posting.Iterator {
	r.store.mu.Lock()
	r.store.history++
	r.store.mu.Unlock()
	return scalarBatchEmptyIterator{}
}

type scalarBatchEmptyIterator struct{ posting.Iterator }

func (scalarBatchEmptyIterator) Seek([]byte)                {}
func (scalarBatchEmptyIterator) Valid() bool                { return false }
func (scalarBatchEmptyIterator) ValidForPrefix([]byte) bool { return false }
func (scalarBatchEmptyIterator) Error() error               { return nil }
func (scalarBatchEmptyIterator) Close()                     {}

type scalarBatchItem struct {
	posting.Item
	value []byte
}

func (i scalarBatchItem) Value(fn func([]byte) error) error { return fn(i.value) }

type scalarBatchSource struct {
	*scalarBatchControl
	batches []int
	total   error
	before  func()
}

func (s *scalarBatchSource) ReadPostingBatch(keys [][]byte, ts uint64) ([]posting.PostingReadResult, error) {
	s.mu.Lock()
	s.batches = append(s.batches, len(keys))
	s.mu.Unlock()
	if s.before != nil {
		s.before()
	}
	out := make([]posting.PostingReadResult, len(keys))
	for i, key := range keys {
		value := s.value
		if s.values != nil {
			value = s.values[string(key)]
		}
		out[i].Value = append([]byte(nil), value...)
		out[i].Err = s.errors[string(key)]
	}
	return out, s.total
}

func TestScalarBatchOrderedReadAndConversionErrors(t *testing.T) {
	later := errors.New("later storage error")
	for _, conversion := range []bool{false, true} {
		t.Run(map[bool]string{false: "protobuf", true: "conversion"}[conversion], func(t *testing.T) {
			source := &scalarBatchSource{scalarBatchControl: &scalarBatchControl{values: map[string][]byte{}, errors: map[string]error{}}}
			qs, q := scalarBatchWorkerFixture(t, source)
			args := scalarBatchArgs(q, 2)
			bad := []byte{0xff}
			if conversion {
				var err error
				bad, err = proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{ValType: pb.Posting_STRING, Value: []byte("not-an-integer")}}})
				require.NoError(t, err)
				args.srcFn.atype = types.IntID
			}
			source.values[string(x.DataKey(q.Attr, 1))] = bad
			source.errors[string(x.DataKey(q.Attr, 2))] = later
			err := qs.handleValuePostings(context.Background(), args)
			require.Error(t, err)
			require.NotErrorIs(t, err, later)
			require.Equal(t, []int{2}, source.batches)
			require.Zero(t, source.scalar)
			// Compare the same caller with the capability hidden.
			posting.Cleanup()
			posting.InitForStore(source.scalarBatchControl, 0, false)
			qs.cache = posting.NewLocalCache(q.ReadTs)
			args.out = &pb.Result{}
			scalarErr := qs.handleValuePostings(context.Background(), args)
			require.EqualError(t, scalarErr, err.Error())
		})
	}
}

func TestScalarBatchCancellationOwnsActiveCallAndStopsNextChunk(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	source := &scalarBatchSource{scalarBatchControl: &scalarBatchControl{}, before: func() { close(entered); <-release }}
	qs, q := scalarBatchWorkerFixture(t, source)
	args := scalarBatchArgs(q, 65)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- qs.handleValuePostings(ctx, args) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned before active physical work completed: %v", err)
	default:
	}
	close(release)
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, []int{32}, source.batches)
	require.Empty(t, args.out.ValueMatrix)
}

func TestScalarBatchSingletonUnsupportedAndHistoryStayScalar(t *testing.T) {
	for _, shape := range []string{"singleton", "unsupported", "metrics", "count", "expand", "facets", "list", "language"} {
		t.Run(shape, func(t *testing.T) {
			source := &scalarBatchSource{scalarBatchControl: &scalarBatchControl{}}
			var store posting.Store = source
			if shape == "unsupported" {
				store = source.scalarBatchControl
			}
			qs, q := scalarBatchWorkerFixture(t, store)
			n := 2
			if shape == "singleton" {
				n = 1
			}
			args := scalarBatchArgs(q, n)
			old := posting.EnableDetailedMetrics
			t.Cleanup(func() { posting.SetEnabledDetailedMetrics(old) })
			switch shape {
			case "metrics":
				posting.SetEnabledDetailedMetrics(true)
			case "count":
				q.DoCount = true
			case "expand":
				q.ExpandAll = true
			case "facets":
				q.FacetParam = &pb.FacetParams{}
			case "list":
				schema.State().Set(q.Attr, &pb.SchemaUpdate{Predicate: q.Attr, ValueType: pb.Posting_STRING, List: true})
			case "language":
				schema.State().Set(q.Attr, &pb.SchemaUpdate{Predicate: q.Attr, ValueType: pb.Posting_STRING, Lang: true})
			}
			require.NoError(t, qs.handleValuePostings(context.Background(), args))
			require.Empty(t, source.batches)
			if source.history == 0 {
				require.Equal(t, n, source.scalar)
			} else {
				require.Zero(t, source.scalar)
			}
		})
	}
}

// This wrapper counts real default dispatch while still invoking the public
// TreeDB/GoMap path (not a synthetic batch implementation).
type scalarBatchTreeDB struct {
	posting.Store
	reader posting.PostingBatchReader
	calls  int
}

func (s *scalarBatchTreeDB) ReadPostingBatch(keys [][]byte, ts uint64) ([]posting.PostingReadResult, error) {
	s.calls++
	return s.reader.ReadPostingBatch(keys, ts)
}

func TestScalarBatchRealTreeDBCallerParity(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	store, err := posting.OpenTreeDBStore(opts, posting.TreeDBCommitDurable)
	require.NoError(t, err)
	defer store.Close()
	source := &scalarBatchTreeDB{Store: store, reader: store}
	bus := posting.NewCommitEventBus(2)
	defer bus.Close()
	qs, q := scalarBatchWorkerFixture(t, posting.WithCommitEvents(source, bus))
	for _, ts := range []uint64{2, 20} {
		writer := posting.NewTxnWriterForStore(store)
		for i := 1; i <= 34; i++ {
			if i == 3 {
				continue
			}
			value, e := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{Value: []byte{byte(i), byte(ts)}, ValType: pb.Posting_STRING}}})
			require.NoError(t, e)
			require.NoError(t, writer.SetAt(x.DataKey(q.Attr, uint64(i)), value, posting.BitCompletePosting, ts))
		}
		require.NoError(t, writer.Flush())
	}
	args := scalarBatchArgs(q, 34)
	require.NoError(t, qs.handleValuePostings(context.Background(), args))
	require.Equal(t, 2, source.calls)
	posting.Cleanup()
	posting.InitForStore(struct{ posting.Store }{store}, 0, false)
	qs.cache = posting.NewLocalCache(q.ReadTs)
	scalarArgs := args
	scalarArgs.out = &pb.Result{}
	require.NoError(t, qs.handleValuePostings(context.Background(), scalarArgs))
	require.True(t, proto.Equal(args.out, scalarArgs.out))
	require.Empty(t, args.out.ValueMatrix[2].Values)
	require.Equal(t, []byte{1, 2}, args.out.ValueMatrix[0].Values[0].Val, "historical read must not return version20")
}

func scalarBatchWorkerFixture(t *testing.T, store posting.Store) (*queryState, *pb.Query) {
	t.Helper()
	oldStore, oldBadger := postingStore, pstore
	posting.Cleanup()
	posting.InitForStore(store, 0, false)
	schema.Init(nil)
	t.Cleanup(func() { posting.Cleanup(); posting.InitForStore(oldStore, 0, false); schema.Init(oldBadger) })
	attr := x.AttrInRootNamespace("scalar-batch")
	schema.State().Set(attr, &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_STRING})
	q := &pb.Query{Attr: attr, ReadTs: 10, UidList: &pb.List{}}
	return &queryState{cache: posting.NewLocalCache(q.ReadTs)}, q
}

func scalarBatchArgs(q *pb.Query, n int) funcArgs {
	for i := 0; i < n; i++ {
		q.UidList.Uids = append(q.UidList.Uids, uint64(i+1))
	}
	return funcArgs{q: q, srcFn: &functionContext{n: n, atype: types.StringID, fnType: notAFunction}, out: &pb.Result{}}
}

func TestScalarBatchRealCallerActivation(t *testing.T) {
	value, err := proto.Marshal(&pb.PostingList{Postings: []*pb.Posting{{ValType: pb.Posting_STRING, Value: []byte("value")}}})
	require.NoError(t, err)
	source := &scalarBatchSource{scalarBatchControl: &scalarBatchControl{value: value}}
	qs, q := scalarBatchWorkerFixture(t, source)
	args := scalarBatchArgs(q, 65)
	require.NoError(t, qs.handleValuePostings(context.Background(), args))
	require.Equal(t, []int{32, 32}, source.batches)
	require.Equal(t, 1, source.scalar, "last singleton must retain GetSinglePosting")
	require.Len(t, args.out.ValueMatrix, 65)
	for _, values := range args.out.ValueMatrix {
		require.Equal(t, []byte("value"), values.Values[0].Val)
	}
}
