/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type countSplitFailureStore struct {
	Store
	key     []byte
	failure error
}

func (s countSplitFailureStore) NewReadTxn(ts uint64) ReadTxn {
	return countSplitFailureTxn{ReadTxn: s.Store.NewReadTxn(ts), key: s.key, failure: s.failure}
}

type countSplitFailureTxn struct {
	ReadTxn
	key     []byte
	failure error
}

func (t countSplitFailureTxn) Get(key []byte) (Item, error) {
	if bytes.Equal(t.key, key) {
		return nil, t.failure
	}
	return t.ReadTxn.Get(key)
}

func TestStoreCountRebuildPropagatesSplitReadFailure(t *testing.T) {
	store, _ := openTreeDBPostingStore(t, t.TempDir(), TreeDBCommitRelaxed)
	attr := x.AttrInRootNamespace("count-rebuild-split-error")
	key := x.DataKey(attr, 1)
	part, err := x.SplitKey(key, 1)
	require.NoError(t, err)
	pack := codec.Encode([]uint64{10, 20}, 256)
	data, err := proto.Marshal(&pb.PostingList{Pack: pack})
	codec.FreePack(pack)
	require.NoError(t, err)
	commitStoreEntry(t, store, 10, Entry{Key: part, Value: data, UserMeta: BitCompletePosting})
	data, err = proto.Marshal(&pb.PostingList{Splits: []uint64{1}})
	require.NoError(t, err)
	commitStoreEntry(t, store, 10, Entry{Key: key, Value: data, UserMeta: BitCompletePosting})
	old := &pb.SchemaUpdate{Predicate: attr, ValueType: pb.Posting_UID, List: true}
	next := proto.Clone(old).(*pb.SchemaUpdate)
	next.Count = true
	schema.State().Set(attr, next)
	failure := errors.New("count split read failed")
	rb := IndexRebuild{Attr: attr, StartTs: 20, OldSchema: old, CurrentSchema: next,
		Store: countSplitFailureStore{Store: store, key: part, failure: failure}}
	require.ErrorIs(t, rb.BuildIndexes(schema.GetWriteContext(context.Background())), failure)
}
