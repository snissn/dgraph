/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package schema

import (
	"math"

	"github.com/dgraph-io/badger/v4"
)

// Store is the point-read and atomic-delete contract used by schema's live
// mutation path. Schema keeps this small contract separate from posting.Store
// because posting imports schema; sharing the type would create an import cycle.
// Badger stream-based bootstrap remains an explicitly operational path.
type Store interface {
	NewReadTxn(readTs uint64) ReadTxn
	NewWriteTxn() WriteTxn
}

// ReadTxn is a timestamp-bound schema read snapshot.
type ReadTxn interface {
	Get(key []byte) (Item, error)
	NewIterator(prefix []byte) Iterator
	Discard()
}

// WriteTxn is an atomic externally timestamped schema mutation.
type WriteTxn interface {
	Delete(key []byte) error
	CommitAt(commitTs uint64, cb func(error)) error
	Discard()
}

// Item exposes the value callback needed to decode schema records without an
// additional copy on Badger.
type Item interface {
	Key() []byte
	Value(func([]byte) error) error
}

type Iterator interface {
	Rewind()
	ValidForPrefix(prefix []byte) bool
	Item() Item
	Next()
	Error() error
	Close()
}

type badgerStore struct {
	db *badger.DB
}

func newBadgerStore(db *badger.DB) badgerStore {
	return badgerStore{db: db}
}

func (s badgerStore) NewReadTxn(readTs uint64) ReadTxn {
	return badgerReadTxn{txn: s.db.NewTransactionAt(readTs, false)}
}

func (s badgerStore) NewWriteTxn() WriteTxn {
	return badgerWriteTxn{txn: s.db.NewTransactionAt(math.MaxUint64, true)}
}

type badgerReadTxn struct {
	txn *badger.Txn
}

func (t badgerReadTxn) Get(key []byte) (Item, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, err
	}
	return badgerItem{item: item}, nil
}

func (t badgerReadTxn) NewIterator(prefix []byte) Iterator {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	return badgerIterator{iterator: t.txn.NewIterator(opts)}
}

func (t badgerReadTxn) Discard() {
	t.txn.Discard()
}

type badgerWriteTxn struct {
	txn *badger.Txn
}

func (t badgerWriteTxn) Delete(key []byte) error {
	return t.txn.Delete(key)
}

func (t badgerWriteTxn) CommitAt(commitTs uint64, cb func(error)) error {
	return t.txn.CommitAt(commitTs, cb)
}

func (t badgerWriteTxn) Discard() {
	t.txn.Discard()
}

type badgerItem struct {
	item *badger.Item
}

func (i badgerItem) Key() []byte { return i.item.Key() }

func (i badgerItem) Value(fn func([]byte) error) error {
	return i.item.Value(fn)
}

type badgerIterator struct{ iterator *badger.Iterator }

func (i badgerIterator) Rewind()                           { i.iterator.Rewind() }
func (i badgerIterator) ValidForPrefix(prefix []byte) bool { return i.iterator.ValidForPrefix(prefix) }
func (i badgerIterator) Item() Item                        { return badgerItem{item: i.iterator.Item()} }
func (i badgerIterator) Next()                             { i.iterator.Next() }
func (i badgerIterator) Error() error                      { return nil }
func (i badgerIterator) Close()                            { i.iterator.Close() }

var (
	_ Store    = badgerStore{}
	_ ReadTxn  = badgerReadTxn{}
	_ WriteTxn = badgerWriteTxn{}
	_ Item     = badgerItem{}
	_ Iterator = badgerIterator{}
)
