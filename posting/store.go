/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"errors"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v4"
)

// ErrBadgerOperationalPath identifies a deliberately excluded Badger-only
// stream, destructive-maintenance, or namespace operation.
var ErrBadgerOperationalPath = errors.New("operation requires the Badger operational backend")

func requireBadgerOperationalStore(operation string) (*badger.DB, error) {
	if pstore == nil {
		return nil, fmt.Errorf("%w: %s", ErrBadgerOperationalPath, operation)
	}
	return pstore, nil
}

// Store is the narrow posting-store contract needed before experimenting with
// non-Badger posting stores. It intentionally models the Dgraph call sites that
// carry Badger-only semantics today: externally managed read/write timestamps,
// entry metadata, expiry, discard markers, and all-version iteration.
//
// Badger remains the only production implementation. TreeDB implementations
// must fail closed until they can satisfy this contract without silently
// dropping metadata, versions, expiry, or stream/subscription semantics.
type Store interface {
	NewReadTxn(readTs uint64) ReadTxn
	NewWriteTxn() WriteTxn
	IsClosed() bool
}

// ReadTxn is the read side of the posting-store contract.
type ReadTxn interface {
	Get(key []byte) (Item, error)
	NewIterator(opts IteratorOptions) Iterator
	NewKeyIterator(key []byte, opts IteratorOptions) Iterator
	Discard()
}

// WriteTxn is the managed-write side of the posting-store contract.
type WriteTxn interface {
	SetEntry(entry Entry) error
	Delete(key []byte) error
	CommitAt(commitTs uint64, cb func(error)) error
	Discard()
}

// Entry is the Dgraph posting metadata that must survive an adapter boundary.
type Entry struct {
	Key                    []byte
	Value                  []byte
	UserMeta               byte
	ExpiresAt              uint64
	DiscardEarlierVersions bool
}

// IteratorOptions is the subset of Badger iterator behavior used by posting
// reads and schema scans. AllVersions is part of the required contract because
// Dgraph reconstructs posting lists from versioned deltas.
type IteratorOptions struct {
	Prefix         []byte
	Reverse        bool
	AllVersions    bool
	PrefetchValues bool
}

// Iterator is the iterator side of the posting-store contract.
type Iterator interface {
	Rewind()
	Seek(key []byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Item() Item
	Next()
	Error() error
	Close()
}

// Item is the value/metadata view Dgraph expects from posting-store reads.
type Item interface {
	Key() []byte
	KeyCopy(dst []byte) []byte
	Value(func([]byte) error) error
	ValueCopy(dst []byte) ([]byte, error)
	UserMeta() byte
	Version() uint64
	ExpiresAt() uint64
	IsDeletedOrExpired() bool
	DiscardEarlierVersions() bool
	ValueSize() int64
}

// BadgerStore adapts a Badger database to the posting Store contract.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore returns the default production posting-store adapter.
func NewBadgerStore(db *badger.DB) BadgerStore {
	return BadgerStore{db: db}
}

// NewReadTxn opens a Badger read transaction at the caller-provided timestamp.
func (s BadgerStore) NewReadTxn(readTs uint64) ReadTxn {
	return badgerReadTxn{txn: s.db.NewTransactionAt(readTs, false)}
}

// NewWriteTxn opens a managed Badger write transaction. Dgraph commits it at a
// caller-provided commit timestamp through WriteTxn.CommitAt.
func (s BadgerStore) NewWriteTxn() WriteTxn {
	return badgerWriteTxn{txn: s.db.NewTransactionAt(math.MaxUint64, true)}
}

// IsClosed reports whether the underlying Badger database has been closed.
func (s BadgerStore) IsClosed() bool {
	return s.db.IsClosed()
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

func (t badgerReadTxn) NewIterator(opts IteratorOptions) Iterator {
	return badgerIterator{itr: t.txn.NewIterator(toBadgerIteratorOptions(opts))}
}

func (t badgerReadTxn) NewKeyIterator(key []byte, opts IteratorOptions) Iterator {
	return badgerIterator{itr: t.txn.NewKeyIterator(key, toBadgerIteratorOptions(opts))}
}

func (t badgerReadTxn) Discard() {
	t.txn.Discard()
}

type badgerWriteTxn struct {
	txn *badger.Txn
}

func (t badgerWriteTxn) SetEntry(entry Entry) error {
	badgerEntry := &badger.Entry{
		Key:       entry.Key,
		Value:     entry.Value,
		UserMeta:  entry.UserMeta,
		ExpiresAt: entry.ExpiresAt,
	}
	if entry.DiscardEarlierVersions {
		badgerEntry = badgerEntry.WithDiscard()
	}
	return t.txn.SetEntry(badgerEntry)
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

type badgerIterator struct {
	itr *badger.Iterator
}

func (it badgerIterator) Rewind() {
	it.itr.Rewind()
}

func (it badgerIterator) Seek(key []byte) {
	it.itr.Seek(key)
}

func (it badgerIterator) Valid() bool {
	return it.itr.Valid()
}

func (it badgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.itr.ValidForPrefix(prefix)
}

func (it badgerIterator) Item() Item {
	return badgerItem{item: it.itr.Item()}
}

func (it badgerIterator) Next() {
	it.itr.Next()
}

// Error is part of the backend-neutral iterator seam. Badger does not expose
// iterator errors on its public iterator type; errors are reported while
// materializing values instead.
func (it badgerIterator) Error() error {
	return nil
}

func (it badgerIterator) Close() {
	it.itr.Close()
}

type badgerItem struct {
	item *badger.Item
}

func (i badgerItem) Key() []byte {
	return i.item.Key()
}

func (i badgerItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)
}

func (i badgerItem) Value(fn func([]byte) error) error {
	return i.item.Value(fn)
}

func (i badgerItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i badgerItem) UserMeta() byte {
	return i.item.UserMeta()
}

func (i badgerItem) Version() uint64 {
	return i.item.Version()
}

func (i badgerItem) ExpiresAt() uint64 {
	return i.item.ExpiresAt()
}

func (i badgerItem) IsDeletedOrExpired() bool {
	return i.item.IsDeletedOrExpired()
}

func (i badgerItem) DiscardEarlierVersions() bool {
	return i.item.DiscardEarlierVersions()
}

func (i badgerItem) ValueSize() int64 {
	return i.item.ValueSize()
}

func toBadgerIteratorOptions(opts IteratorOptions) badger.IteratorOptions {
	badgerOpts := badger.DefaultIteratorOptions
	badgerOpts.Prefix = opts.Prefix
	badgerOpts.Reverse = opts.Reverse
	badgerOpts.AllVersions = opts.AllVersions
	badgerOpts.PrefetchValues = opts.PrefetchValues
	return badgerOpts
}

var (
	_ Store    = BadgerStore{}
	_ ReadTxn  = badgerReadTxn{}
	_ WriteTxn = badgerWriteTxn{}
	_ Iterator = badgerIterator{}
	_ Item     = badgerItem{}
)
