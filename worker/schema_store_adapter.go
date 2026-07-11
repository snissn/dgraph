/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/schema"
)

// schemaPostingStore bridges the intentionally separate posting and schema
// contracts without introducing a posting -> schema import cycle.
type schemaPostingStore struct{ store posting.Store }

func (s schemaPostingStore) NewReadTxn(ts uint64) schema.ReadTxn {
	return schemaPostingReadTxn{txn: s.store.NewReadTxn(ts)}
}

func (s schemaPostingStore) NewWriteTxn() schema.WriteTxn {
	return schemaPostingWriteTxn{txn: s.store.NewWriteTxn()}
}

type schemaPostingReadTxn struct{ txn posting.ReadTxn }

func (t schemaPostingReadTxn) Get(key []byte) (schema.Item, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, err
	}
	return schemaPostingItem{item: item}, nil
}

func (t schemaPostingReadTxn) NewIterator(prefix []byte) schema.Iterator {
	return schemaPostingIterator{iterator: t.txn.NewIterator(posting.IteratorOptions{
		Prefix: prefix, PrefetchValues: true,
	})}
}

func (t schemaPostingReadTxn) Discard() { t.txn.Discard() }

type schemaPostingWriteTxn struct{ txn posting.WriteTxn }

func (t schemaPostingWriteTxn) Delete(key []byte) error { return t.txn.Delete(key) }
func (t schemaPostingWriteTxn) CommitAt(ts uint64, cb func(error)) error {
	return t.txn.CommitAt(ts, cb)
}
func (t schemaPostingWriteTxn) Discard() { t.txn.Discard() }

type schemaPostingItem struct{ item posting.Item }

func (i schemaPostingItem) Key() []byte                       { return i.item.Key() }
func (i schemaPostingItem) Value(fn func([]byte) error) error { return i.item.Value(fn) }

type schemaPostingIterator struct{ iterator posting.Iterator }

func (i schemaPostingIterator) Rewind() { i.iterator.Rewind() }
func (i schemaPostingIterator) ValidForPrefix(prefix []byte) bool {
	return i.iterator.ValidForPrefix(prefix)
}
func (i schemaPostingIterator) Item() schema.Item {
	return schemaPostingItem{item: i.iterator.Item()}
}
func (i schemaPostingIterator) Next()        { i.iterator.Next() }
func (i schemaPostingIterator) Error() error { return i.iterator.Error() }
func (i schemaPostingIterator) Close()       { i.iterator.Close() }

var _ schema.Store = schemaPostingStore{}
