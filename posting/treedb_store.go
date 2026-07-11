/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// TreeDBCommitMode selects the acknowledgement boundary used by TreeDBStore.
type TreeDBCommitMode uint8

const (
	// TreeDBCommitDurable acknowledges after TreeDB's synced MVCC commit.
	TreeDBCommitDurable TreeDBCommitMode = iota
	// TreeDBCommitRelaxed acknowledges after atomic publication without an
	// fsync boundary.
	TreeDBCommitRelaxed
)

var (
	// ErrTreeDBTTLUnsupported is returned before a transaction containing a TTL
	// can publish any staged mutation.
	ErrTreeDBTTLUnsupported = errors.New("posting treedb: TTL entries are unsupported")
	// ErrTreeDBEnvelope identifies a malformed Dgraph-owned value envelope.
	ErrTreeDBEnvelope = errors.New("posting treedb: malformed value envelope")
	// ErrTreeDBTxnClosed identifies use after commit or discard.
	ErrTreeDBTxnClosed = errors.New("posting treedb: transaction is closed")
	// ErrTreeDBCommitMode identifies an unsupported adapter commit mode.
	ErrTreeDBCommitMode = errors.New("posting treedb: invalid commit mode")
)

const (
	treeDBEnvelopeMagic0  byte = 0xd7
	treeDBEnvelopeMagic1  byte = 0x47
	treeDBEnvelopeVersion byte = 1
	treeDBEnvelopeHeader       = 5

	treeDBEnvelopeDiscard byte = 1 << 0
)

// TreeDBStore owns exactly one TreeDB handle and exactly one external-MVCC
// owner. Close is idempotent and closes that handle at most once.
type TreeDBStore struct {
	db         *treedb.DB
	mvcc       *mvcc.Store
	commitMode mvcc.CommitMode
	mode       TreeDBCommitMode
	durability string

	lifecycleMu  sync.Mutex
	commitWG     sync.WaitGroup
	mutationPool sync.Pool
	closeOnce    sync.Once
	closeErr     error
	closed       atomic.Bool

	// beforeCommitForTest lets adapter tests hold an admitted commit without
	// depending on storage timing. Production stores leave it nil.
	beforeCommitForTest func()
}

// OpenTreeDBStore opens and owns a TreeDB posting store. opts.Durability must
// match mode: durable commits require TreeDB's durable mode, while relaxed
// commits may use either relaxed TreeDB profile.
func OpenTreeDBStore(opts treedb.Options, mode TreeDBCommitMode) (*TreeDBStore, error) {
	commitMode, err := toMVCCCommitMode(mode)
	if err != nil {
		return nil, err
	}
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open posting TreeDB: %w", err)
	}
	store := &TreeDBStore{
		db: db, mvcc: mvcc.New(db), commitMode: commitMode, mode: mode,
		durability: db.DurabilityMode(),
	}
	store.mutationPool.New = func() any {
		return &treeDBMutationBatch{mutations: make([]mvcc.Mutation, 0, 16)}
	}
	if commitMode == mvcc.CommitDurable && db.DurabilityMode() != "wal_on_sync" &&
		!bytes.HasPrefix([]byte(db.DurabilityMode()), []byte("wal_on_sync+")) {
		_ = store.Close()
		return nil, fmt.Errorf("%w: durable adapter with TreeDB mode %q", mvcc.ErrDurabilityUnavailable, db.DurabilityMode())
	}
	return store, nil
}

func toMVCCCommitMode(mode TreeDBCommitMode) (mvcc.CommitMode, error) {
	switch mode {
	case TreeDBCommitDurable:
		return mvcc.CommitDurable, nil
	case TreeDBCommitRelaxed:
		return mvcc.CommitRelaxed, nil
	default:
		return 0, fmt.Errorf("%w: %d", ErrTreeDBCommitMode, mode)
	}
}

func (s *TreeDBStore) NewReadTxn(readTs uint64) ReadTxn {
	return &treeDBReadTxn{store: s, readTs: readTs, iterators: make(map[*treeDBIterator]struct{})}
}

func (s *TreeDBStore) NewWriteTxn() WriteTxn {
	batch := s.mutationPool.Get().(*treeDBMutationBatch)
	return &treeDBWriteTxn{store: s, batch: batch}
}

func (s *TreeDBStore) IsClosed() bool {
	return s == nil || s.closed.Load()
}

// Close releases the one TreeDB handle owned by this adapter. Repeated calls
// return the result of the first close without closing TreeDB again.
func (s *TreeDBStore) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.closed.Store(true)
		s.lifecycleMu.Unlock()

		// No new commit can be admitted after closed is published. Wait for every
		// commit accepted before that boundary before closing the owned DB.
		s.commitWG.Wait()

		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()
		if s.db != nil {
			s.closeErr = s.db.Close()
		}
	})
	return s.closeErr
}

// TreeDBStoreStatus is the effective owner/lifecycle state used by the Alpha
// backend manager. DurabilityMode is TreeDB's resolved profile, not merely the
// adapter mode requested by the caller.
type TreeDBStoreStatus struct {
	Closed         bool
	CommitMode     TreeDBCommitMode
	DurabilityMode string
	DurableCommits bool
}

// Status returns immutable configuration plus current close state without
// exposing the owned TreeDB handle.
func (s *TreeDBStore) Status() TreeDBStoreStatus {
	if s == nil {
		return TreeDBStoreStatus{Closed: true}
	}
	return TreeDBStoreStatus{
		Closed: s.IsClosed(), CommitMode: s.mode, DurabilityMode: s.durability,
		DurableCommits: s.mode == TreeDBCommitDurable,
	}
}

// Stats returns a detached diagnostic snapshot from the owned TreeDB handle.
func (s *TreeDBStore) Stats() (map[string]string, error) {
	if s == nil {
		return nil, fmt.Errorf("stats posting TreeDB: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return nil, fmt.Errorf("stats posting TreeDB: %w", treedb.ErrClosed)
	}
	stats := s.db.Stats()
	return cloneTreeDBStats(stats), nil
}

func cloneTreeDBStats(stats map[string]string) map[string]string {
	if stats == nil {
		return nil
	}
	out := make(map[string]string, len(stats))
	for key, value := range stats {
		out[key] = value
	}
	return out
}

// ValueLogGC runs TreeDB value-log maintenance through the store owner.
func (s *TreeDBStore) ValueLogGC(ctx context.Context, opts treedb.ValueLogGCOptions) (treedb.ValueLogGCStats, error) {
	if s == nil {
		return treedb.ValueLogGCStats{}, fmt.Errorf("value-log GC posting TreeDB: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return treedb.ValueLogGCStats{}, fmt.Errorf("value-log GC posting TreeDB: %w", treedb.ErrClosed)
	}
	stats, err := s.db.ValueLogGC(ctx, opts)
	if err != nil {
		return stats, fmt.Errorf("value-log GC posting TreeDB: %w", err)
	}
	return stats, nil
}

// CompactStorage runs full storage maintenance through the store owner.
func (s *TreeDBStore) CompactStorage(ctx context.Context, opts treedb.CompactStorageOptions) (treedb.CompactStorageStats, error) {
	if s == nil {
		return treedb.CompactStorageStats{}, fmt.Errorf("compact posting TreeDB storage: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return treedb.CompactStorageStats{}, fmt.Errorf("compact posting TreeDB storage: %w", treedb.ErrClosed)
	}
	stats, err := s.db.CompactStorage(ctx, opts)
	if err != nil {
		return stats, fmt.Errorf("compact posting TreeDB storage: %w", err)
	}
	return stats, nil
}

// DiscardFloor exposes gomap's persisted global discard floor for Dgraph-owned
// maintenance orchestration.
func (s *TreeDBStore) DiscardFloor() (uint64, error) {
	return s.mvcc.DiscardFloor()
}

// AdvanceDiscardFloor persists a monotonic discard floor using the adapter's
// configured acknowledgement boundary.
func (s *TreeDBStore) AdvanceDiscardFloor(timestamp uint64) error {
	return s.mvcc.AdvanceDiscardFloor(timestamp, s.commitMode)
}

// PruneVersions removes obsolete versions below the persisted discard floor.
func (s *TreeDBStore) PruneVersions(batchSize int) (mvcc.PruneStats, error) {
	return s.mvcc.PruneVersions(mvcc.PruneOptions{BatchSize: batchSize, Mode: s.commitMode})
}

type treeDBWriteTxn struct {
	mu        sync.Mutex
	store     *TreeDBStore
	batch     *treeDBMutationBatch
	stickyErr error
	closed    bool
}

type treeDBMutationBatch struct {
	mutations []mvcc.Mutation
}

func (t *treeDBWriteTxn) SetEntry(entry Entry) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrTreeDBTxnClosed
	}
	if entry.ExpiresAt != 0 {
		t.stickyErr = fmt.Errorf("%w: key %x expires at %d", ErrTreeDBTTLUnsupported, entry.Key, entry.ExpiresAt)
		return t.stickyErr
	}
	// One allocation owns both the key and envelope while preserving the
	// caller-copy boundary. This avoids the map/string/copy staging overhead on
	// Dgraph's common one-entry managed transactions.
	owned := make([]byte, len(entry.Key)+treeDBEnvelopeHeader+len(entry.Value))
	key := owned[:len(entry.Key)]
	copy(key, entry.Key)
	value := owned[len(entry.Key):]
	encodeTreeDBEnvelopeInto(value, entry)
	t.setMutation(mvcc.Mutation{Key: key, Value: value})
	return nil
}

func (t *treeDBWriteTxn) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrTreeDBTxnClosed
	}
	owned := append([]byte(nil), key...)
	t.setMutation(mvcc.Mutation{Key: owned, Delete: true})
	return nil
}

func (t *treeDBWriteTxn) setMutation(mutation mvcc.Mutation) {
	// Dgraph batches are small and overwhelmingly unique. A reverse linear
	// search preserves last-operation-wins without allocating a map or strings.
	for i := len(t.batch.mutations) - 1; i >= 0; i-- {
		if bytes.Equal(t.batch.mutations[i].Key, mutation.Key) {
			t.batch.mutations[i] = mutation
			return
		}
	}
	t.batch.mutations = append(t.batch.mutations, mutation)
}

func (t *treeDBWriteTxn) CommitAt(commitTs uint64, cb func(error)) error {
	batch, err := t.prepareCommit()
	if cb == nil {
		defer t.store.releaseMutationBatch(batch)
		if err != nil {
			return err
		}
		return t.store.commitAt(commitTs, batch.mutations)
	}
	if err != nil {
		t.store.releaseMutationBatch(batch)
		go cb(err)
		return nil
	}
	if err := t.store.admitCommit(); err != nil {
		t.store.releaseMutationBatch(batch)
		go cb(err)
		return nil
	}
	// Match Badger's callback form: an admitted commit runs behind the callback
	// boundary, permitting TxnWriter to pipeline commits. The store lifecycle
	// gate keeps the DB open through the storage commit. Complete that gate
	// before invoking the callback so a callback may itself call Close.
	go func() {
		err := t.store.commitAtAdmitted(commitTs, batch.mutations)
		t.store.releaseMutationBatch(batch)
		t.store.commitWG.Done()
		cb(err)
	}()
	return nil
}

func (t *treeDBWriteTxn) prepareCommit() (*treeDBMutationBatch, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrTreeDBTxnClosed
	}
	t.closed = true
	batch := t.batch
	t.batch = nil
	if t.stickyErr != nil {
		return batch, t.stickyErr
	}
	return batch, nil
}

func (s *TreeDBStore) releaseMutationBatch(batch *treeDBMutationBatch) {
	if s == nil || batch == nil {
		return
	}
	for i := range batch.mutations {
		batch.mutations[i] = mvcc.Mutation{}
	}
	if cap(batch.mutations) <= 64 {
		batch.mutations = batch.mutations[:0]
		s.mutationPool.Put(batch)
	}
}

func (s *TreeDBStore) admitCommit() error {
	if s == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed.Load() || s.db == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	s.commitWG.Add(1)
	return nil
}

func (s *TreeDBStore) commitAt(commitTs uint64, mutations []mvcc.Mutation) error {
	if err := s.admitCommit(); err != nil {
		return err
	}
	defer s.commitWG.Done()
	return s.commitAtAdmitted(commitTs, mutations)
}

func (s *TreeDBStore) commitAtAdmitted(commitTs uint64, mutations []mvcc.Mutation) error {
	if s.beforeCommitForTest != nil {
		s.beforeCommitForTest()
	}
	if err := s.mvcc.CommitAt(commitTs, mutations, s.commitMode); err != nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", err)
	}
	return nil
}

func (t *treeDBWriteTxn) Discard() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.closed = true
	batch := t.batch
	t.batch = nil
	t.mu.Unlock()
	t.store.releaseMutationBatch(batch)
}

func encodeTreeDBEnvelope(entry Entry) []byte {
	encoded := make([]byte, treeDBEnvelopeHeader+len(entry.Value))
	encodeTreeDBEnvelopeInto(encoded, entry)
	return encoded
}

func encodeTreeDBEnvelopeInto(encoded []byte, entry Entry) {
	encoded[0] = treeDBEnvelopeMagic0
	encoded[1] = treeDBEnvelopeMagic1
	encoded[2] = treeDBEnvelopeVersion
	if entry.DiscardEarlierVersions {
		encoded[3] |= treeDBEnvelopeDiscard
	}
	encoded[4] = entry.UserMeta
	copy(encoded[treeDBEnvelopeHeader:], entry.Value)
}

func decodeTreeDBEnvelope(encoded []byte) (value []byte, userMeta byte, discard bool, err error) {
	if len(encoded) < treeDBEnvelopeHeader || encoded[0] != treeDBEnvelopeMagic0 ||
		encoded[1] != treeDBEnvelopeMagic1 || encoded[2] != treeDBEnvelopeVersion {
		return nil, 0, false, fmt.Errorf("%w: header", ErrTreeDBEnvelope)
	}
	if encoded[3]&^treeDBEnvelopeDiscard != 0 {
		return nil, 0, false, fmt.Errorf("%w: flags 0x%02x", ErrTreeDBEnvelope, encoded[3])
	}
	// Callers pass an owned MVCC result/iterator value, so the payload slice is
	// already insulated from TreeDB buffers and needs no second copy here.
	value = encoded[treeDBEnvelopeHeader:]
	return value, encoded[4], encoded[3]&treeDBEnvelopeDiscard != 0, nil
}

type treeDBReadTxn struct {
	mu        sync.Mutex
	store     *TreeDBStore
	readTs    uint64
	discarded bool
	iterators map[*treeDBIterator]struct{}
}

func (t *treeDBReadTxn) Get(key []byte) (Item, error) {
	t.mu.Lock()
	discarded := t.discarded
	t.mu.Unlock()
	if discarded || t.store == nil || t.store.IsClosed() {
		return nil, fmt.Errorf("get posting TreeDB item: %w", treedb.ErrClosed)
	}
	result, err := t.store.mvcc.GetAt(key, t.readTs)
	if err != nil {
		return nil, fmt.Errorf("get posting TreeDB item: %w", err)
	}
	if result.State == mvcc.Absent || result.State == mvcc.Tombstone {
		return nil, badger.ErrKeyNotFound
	}
	item, err := treeDBItemFromVersion(mvcc.Version{
		Key: append([]byte(nil), key...), Value: result.Value,
		Timestamp: result.Timestamp, State: result.State,
	})
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (t *treeDBReadTxn) NewIterator(opts IteratorOptions) Iterator {
	return t.newIterator(nil, false, opts)
}

func (t *treeDBReadTxn) NewKeyIterator(key []byte, opts IteratorOptions) Iterator {
	owned := make([]byte, len(key))
	copy(owned, key)
	// Badger's NewKeyIterator always exposes the complete retained history for
	// the exact key, independent of the caller's AllVersions option.
	opts.AllVersions = true
	return t.newIterator(owned, true, opts)
}

func (t *treeDBReadTxn) newIterator(exactKey []byte, hasExactKey bool, opts IteratorOptions) Iterator {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.readTs == 0 {
		return &treeDBIterator{err: fmt.Errorf("open posting TreeDB iterator: %w", mvcc.ErrZeroTimestamp)}
	}
	if t.discarded || t.store == nil || t.store.IsClosed() {
		return &treeDBIterator{err: fmt.Errorf("open posting TreeDB iterator: %w", treedb.ErrClosed), closed: true}
	}
	it := &treeDBIterator{
		store: t.store, readTs: t.readTs, opts: copyTreeDBIteratorOptions(opts),
		exactKey: exactKey, hasExactKey: hasExactKey, owner: t,
	}
	t.iterators[it] = struct{}{}
	return it
}

func (t *treeDBReadTxn) Discard() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.discarded {
		t.mu.Unlock()
		return
	}
	t.discarded = true
	iterators := make([]*treeDBIterator, 0, len(t.iterators))
	for it := range t.iterators {
		iterators = append(iterators, it)
	}
	t.iterators = nil
	t.mu.Unlock()
	for _, it := range iterators {
		it.close(false)
	}
}

func copyTreeDBIteratorOptions(opts IteratorOptions) IteratorOptions {
	opts.Prefix = append([]byte(nil), opts.Prefix...)
	return opts
}

type treeDBIterator struct {
	store       *TreeDBStore
	readTs      uint64
	raw         *mvcc.VersionIterator
	opts        IteratorOptions
	exactKey    []byte
	hasExactKey bool
	current     treeDBItem
	valid       bool
	err         error
	closed      bool
	owner       *treeDBReadTxn
}

func (it *treeDBIterator) Rewind() {
	if it.closed || it.err != nil {
		return
	}
	it.valid = false
	if !it.openRawLocked() {
		return
	}
	it.positionLocked()
}

func (it *treeDBIterator) Seek(key []byte) {
	if it.closed || it.err != nil {
		return
	}
	// Keep ordinary seeks on the iterator's physical snapshot. Dgraph's
	// watermark excludes late publication below an active read timestamp, and
	// reopening every seek adds material overhead. Reverse seek of an empty key
	// is Badger's rewind-to-end special case and requires a fresh reverse view.
	if (it.raw == nil || (it.opts.Reverse && len(key) == 0)) && !it.openRawLocked() {
		return
	}
	timestamp := uint64(math.MaxUint64)
	if it.opts.Reverse {
		if len(key) == 0 {
			it.valid = false
			it.positionLocked()
			return
		}
		// Physical MVCC order is timestamp-descending, so reverse seek uses the
		// lowest valid timestamp to land on the oldest visible target-key version.
		timestamp = 1
	}
	it.raw.Seek(key, timestamp)
	it.valid = false
	it.positionLocked()
}

func (it *treeDBIterator) Valid() bool {
	return !it.closed && it.err == nil && it.valid
}

func (it *treeDBIterator) ValidForPrefix(prefix []byte) bool {
	return !it.closed && it.err == nil && it.valid && bytes.HasPrefix(it.current.key, prefix)
}

func (it *treeDBIterator) Item() Item {
	if it.closed || it.err != nil || !it.valid {
		return &treeDBItem{}
	}
	return &it.current
}

func (it *treeDBIterator) Next() {
	if it.closed || it.err != nil || !it.valid || it.raw == nil {
		return
	}
	if it.opts.AllVersions {
		it.raw.Next()
	}
	it.valid = false
	it.positionLocked()
}

func (it *treeDBIterator) Error() error {
	return it.err
}

func (it *treeDBIterator) Close() {
	it.close(true)
}

func (it *treeDBIterator) close(unregister bool) {
	if it == nil {
		return
	}
	if it.closed {
		return
	}
	it.closed = true
	if it.raw != nil {
		if err := it.raw.Close(); err != nil && it.err == nil {
			it.err = fmt.Errorf("close posting TreeDB iterator: %w", err)
		}
	}
	owner := it.owner
	if unregister && owner != nil {
		owner.mu.Lock()
		delete(owner.iterators, it)
		owner.mu.Unlock()
	}
}

func (it *treeDBIterator) openRawLocked() bool {
	if it.raw != nil {
		if err := it.raw.Close(); err != nil {
			it.err = fmt.Errorf("close posting TreeDB iterator for rewind: %w", err)
			it.raw = nil
			return false
		}
		it.raw = nil
	}
	if it.store == nil || it.store.IsClosed() {
		it.err = fmt.Errorf("open posting TreeDB iterator: %w", treedb.ErrClosed)
		return false
	}
	options := mvcc.VersionIteratorOptions{
		Prefix: append([]byte(nil), it.opts.Prefix...), ReadTimestamp: it.readTs, Reverse: it.opts.Reverse,
	}
	if it.hasExactKey {
		options.Prefix = nil
		options.LowerBound = append([]byte(nil), it.exactKey...)
		options.UpperBound = append(append([]byte(nil), it.exactKey...), 0)
	}
	raw, err := it.store.mvcc.IterateVersions(options)
	if it.hasExactKey && errors.Is(err, mvcc.ErrInvalidKey) {
		// A codec-maximum logical key cannot be extended with the synthetic NUL
		// upper bound. Fall back to the codec's prefix bounds; the adapter's exact
		// key filter below excludes longer logical siblings.
		options.Prefix = append([]byte(nil), it.exactKey...)
		options.LowerBound = append([]byte(nil), it.exactKey...)
		options.UpperBound = nil
		raw, err = it.store.mvcc.IterateVersions(options)
	}
	if err != nil {
		it.err = fmt.Errorf("open posting TreeDB iterator: %w", err)
		return false
	}
	it.raw = raw
	return true
}

func (it *treeDBIterator) positionLocked() {
	if it.raw == nil || it.err != nil {
		return
	}
	for it.raw.Valid() {
		first := it.raw.Entry()
		key := first.Key
		selected := first
		if !it.opts.AllVersions {
			it.raw.Next()
			for it.raw.Valid() && bytes.Equal(it.raw.Entry().Key, key) {
				candidate := it.raw.Entry()
				if candidate.Timestamp > selected.Timestamp {
					selected = candidate
				}
				it.raw.Next()
			}
		}
		if it.hasExactKey && !bytes.Equal(key, it.exactKey) {
			if it.opts.AllVersions {
				it.raw.Next()
			}
			continue
		}
		if !it.opts.AllVersions && selected.State == mvcc.Tombstone {
			continue
		}
		item, err := treeDBItemFromVersion(selected)
		if err != nil {
			it.err = err
			return
		}
		it.current = item
		it.valid = true
		return
	}
	it.valid = false
	if err := it.raw.Error(); err != nil {
		it.err = fmt.Errorf("iterate posting TreeDB: %w", err)
	}
}

type treeDBItem struct {
	key      []byte
	value    []byte
	userMeta byte
	version  uint64
	discard  bool
	deleted  bool
}

func treeDBItemFromVersion(version mvcc.Version) (treeDBItem, error) {
	item := treeDBItem{key: version.Key, version: version.Timestamp}
	if version.State == mvcc.Tombstone {
		item.deleted = true
		return item, nil
	}
	value, meta, discard, err := decodeTreeDBEnvelope(version.Value)
	if err != nil {
		return treeDBItem{}, fmt.Errorf("decode posting TreeDB item at version %d: %w", version.Timestamp, err)
	}
	item.value = value
	item.userMeta = meta
	item.discard = discard
	return item, nil
}

func (i *treeDBItem) Key() []byte               { return i.key }
func (i *treeDBItem) KeyCopy(dst []byte) []byte { return append(dst[:0], i.key...) }
func (i *treeDBItem) Value(fn func([]byte) error) error {
	if i.deleted {
		return nil
	}
	if fn == nil {
		return nil
	}
	return fn(i.value)
}
func (i *treeDBItem) ValueCopy(dst []byte) ([]byte, error) {
	if i.deleted {
		return dst[:0], nil
	}
	return append(dst[:0], i.value...), nil
}
func (i *treeDBItem) UserMeta() byte               { return i.userMeta }
func (i *treeDBItem) Version() uint64              { return i.version }
func (i *treeDBItem) ExpiresAt() uint64            { return 0 }
func (i *treeDBItem) IsDeletedOrExpired() bool     { return i.deleted }
func (i *treeDBItem) DiscardEarlierVersions() bool { return i.discard }
func (i *treeDBItem) ValueSize() int64             { return int64(len(i.value)) }

var (
	_ Store    = (*TreeDBStore)(nil)
	_ ReadTxn  = (*treeDBReadTxn)(nil)
	_ WriteTxn = (*treeDBWriteTxn)(nil)
	_ Iterator = (*treeDBIterator)(nil)
	_ Item     = (*treeDBItem)(nil)
)
