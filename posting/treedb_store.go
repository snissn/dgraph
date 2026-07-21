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

	// Match the shape of Badger's bounded callback write pipeline without
	// coupling this adapter to Badger internals. The bounded queue applies
	// backpressure before owned mutation batches can grow without limit.
	treeDBCommitQueueCapacity = 64
	// The active commit plus this queue is the adapter's ownership bound. The
	// dispatcher must retain the same total bound when it moves requests from
	// the channel into its dependency-aware pending set.
	treeDBCommitOwnershipLimit = treeDBCommitQueueCapacity + 1
	// A small fixed window gives independent durable commits an opportunity to
	// join TreeDB's command-WAL group commit without creating one goroutine per
	// admitted request. Batches that touch a key already in flight remain FIFO
	// behind that dependency.
	treeDBCommitConcurrency = 8
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
	workerWG     sync.WaitGroup
	queueActive  bool
	commitQueue  chan treeDBCommitRequest
	commitSlots  chan struct{}
	mutationPool sync.Pool
	closeOnce    sync.Once
	closeErr     error
	closed       atomic.Bool

	// beforeCommitForTest lets adapter tests hold an admitted commit without
	// depending on storage timing. Production stores leave it nil.
	beforeCommitForTest func()
	// commitStartedForTest observes a commit after the scheduler has admitted it
	// to TreeDB. It is deliberately test-only so production scheduling does not
	// acquire an extra lock or copy mutation keys.
	commitStartedForTest func(uint64, []mvcc.Mutation)
	// commitFinishedForTest observes the storage result before the scheduler
	// delivers an acknowledgement. Production stores leave it nil.
	commitFinishedForTest func(uint64, error)
}

// OpenTreeDBStore opens and owns a TreeDB posting store. The adapter commit
// mode selects whether each MVCC commit uses Write or WriteSync. TreeDB's
// canonical production profiles all support an explicit WriteSync opt-up even
// when their ordinary acknowledgement policy is relaxed.
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
		durability:  db.DurabilityMode(),
		commitQueue: make(chan treeDBCommitRequest, treeDBCommitQueueCapacity),
		commitSlots: make(chan struct{}, treeDBCommitOwnershipLimit),
	}
	store.mutationPool.New = func() any {
		return &treeDBMutationBatch{
			mutations: make([]mvcc.Mutation, 0, 16),
			arena:     make([]byte, 0, 4<<10),
		}
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
		close(s.commitQueue)
		s.workerWG.Wait()

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
	if s == nil {
		return 0, fmt.Errorf("discard floor posting TreeDB: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return 0, fmt.Errorf("discard floor posting TreeDB: %w", treedb.ErrClosed)
	}
	return s.mvcc.DiscardFloor()
}

// AdvanceDiscardFloor persists a monotonic discard floor using the adapter's
// configured acknowledgement boundary.
func (s *TreeDBStore) AdvanceDiscardFloor(timestamp uint64) error {
	if s == nil {
		return fmt.Errorf("advance discard floor posting TreeDB: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return fmt.Errorf("advance discard floor posting TreeDB: %w", treedb.ErrClosed)
	}
	return s.mvcc.AdvanceDiscardFloor(timestamp, s.commitMode)
}

// PruneVersions removes obsolete versions below the persisted discard floor.
func (s *TreeDBStore) PruneVersions(batchSize int) (mvcc.PruneStats, error) {
	if s == nil {
		return mvcc.PruneStats{}, fmt.Errorf("prune posting TreeDB versions: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return mvcc.PruneStats{}, fmt.Errorf("prune posting TreeDB versions: %w", treedb.ErrClosed)
	}
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
	arena     []byte
	// chunks retains every previous arena backing array after growth. Staged
	// mutations still point into those arrays, and release must scrub all of
	// them rather than only the final arena.
	chunks [][]byte
}

type treeDBCommitRequest struct {
	sequence uint64
	commitTs uint64
	batch    *treeDBMutationBatch
	callback func(error)
	done     chan error
}

type treeDBCommitCompletion struct {
	request treeDBCommitRequest
	err     error
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
	// One arena region owns both the key and envelope while preserving the
	// caller-copy boundary. The pooled arena avoids per-mutation allocations on
	// Dgraph's common small managed transactions.
	owned := t.batch.ownBytes(len(entry.Key) + treeDBEnvelopeHeader + len(entry.Value))
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
	owned := t.batch.ownBytes(len(key))
	copy(owned, key)
	t.setMutation(mvcc.Mutation{Key: owned, Delete: true})
	return nil
}

func (b *treeDBMutationBatch) ownBytes(size int) []byte {
	if size > cap(b.arena)-len(b.arena) {
		if len(b.arena) != 0 {
			b.chunks = append(b.chunks, b.arena)
		}
		capacity := cap(b.arena) * 2
		if capacity < 4<<10 {
			capacity = 4 << 10
		}
		if capacity < size {
			capacity = size
		}
		b.arena = make([]byte, size, capacity)
		return b.arena[:size:size]
	}
	start := len(b.arena)
	b.arena = b.arena[:start+size]
	return b.arena[start : start+size : start+size]
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
		if err != nil {
			t.store.releaseMutationBatch(batch)
			return err
		}
		return t.store.commitSync(commitTs, batch)
	}
	if err != nil {
		t.store.releaseMutationBatch(batch)
		go cb(err)
		return nil
	}
	request := treeDBCommitRequest{commitTs: commitTs, batch: batch, callback: cb}
	if err := t.store.enqueueCommit(request); err != nil {
		t.store.releaseMutationBatch(batch)
		go cb(err)
		return nil
	}
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
	for i := range batch.chunks {
		clear(batch.chunks[i])
		batch.chunks[i] = nil
	}
	clear(batch.arena)
	poolable := cap(batch.mutations) <= 64 && cap(batch.arena) <= 64<<10 && cap(batch.chunks) <= 16
	if poolable {
		batch.mutations = batch.mutations[:0]
		batch.arena = batch.arena[:0]
		batch.chunks = batch.chunks[:0]
		s.mutationPool.Put(batch)
	}
}

func (s *TreeDBStore) enqueueCommit(request treeDBCommitRequest) error {
	if s == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	// Holding lifecycleMu across the bounded send gives concurrent callers one
	// admission order and prevents Close from closing the queue between the
	// closed check and send. The storage worker never takes lifecycleMu, so a
	// full queue still makes progress and deliberately backpressures the caller.
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed.Load() || s.db == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	// Retain the previous one-active-plus-64-queued ownership bound even though
	// the scheduler may move queued requests into its own pending set. Holding
	// lifecycleMu while this blocks is safe: completion never needs that lock,
	// and Close cannot race a request between its closed check and admission.
	s.acquireTreeDBCommitSlot()
	if !s.queueActive {
		// Direct synchronous commits admitted before callback mode was activated
		// must publish before the first queued request. lifecycleMu prevents a new
		// direct admission while this wait establishes the handoff boundary.
		s.commitWG.Wait()
		s.queueActive = true
		s.workerWG.Add(1)
		go s.runCommitQueue()
	}
	s.commitWG.Add(1)
	s.commitQueue <- request
	return nil
}

func (s *TreeDBStore) runCommitQueue() {
	defer s.workerWG.Done()
	if s.mode != TreeDBCommitDurable {
		s.runSerialCommitQueue()
		return
	}
	s.runDurableCommitQueue()
}

// runSerialCommitQueue retains the original callback-queue behavior for
// relaxed commits. The relaxed Alpha guardrail is sensitive to publication
// contention, while only durable commits need concurrent arrival to form a
// command-WAL group at their acknowledgement barrier.
func (s *TreeDBStore) runSerialCommitQueue() {
	for request := range s.commitQueue {
		err := s.commitAtAdmitted(request.commitTs, request.batch.mutations)
		s.completeTreeDBCommit(treeDBCommitCompletion{request: request, err: err}, nil, nil)
		s.deliverTreeDBCommit(treeDBCommitCompletion{request: request, err: err})
	}
}

func (s *TreeDBStore) runDurableCommitQueue() {
	completed := make(chan treeDBCommitCompletion, treeDBCommitConcurrency)
	pending := make([]treeDBCommitRequest, 0, treeDBCommitQueueCapacity)
	inFlightKeys := make(map[string]struct{})
	finished := make(map[uint64]treeDBCommitCompletion)
	inFlight := 0
	nextSequence := uint64(1)
	nextDelivery := uint64(1)
	queue := s.commitQueue

	for queue != nil || len(pending) != 0 || inFlight != 0 {
		// Admit as much FIFO work as the bounded window permits. A blocked
		// request does not prevent a later independent request from joining the
		// current group; a conflicting request remains pending until its earlier
		// dependency completes.
		for inFlight < treeDBCommitConcurrency {
			index := nextIndependentTreeDBCommit(pending, inFlightKeys)
			if index < 0 {
				break
			}
			request := pending[index]
			pending = append(pending[:index], pending[index+1:]...)
			reserveTreeDBCommitKeys(request.batch.mutations, inFlightKeys)
			inFlight++
			go func(request treeDBCommitRequest) {
				err := s.commitAtAdmitted(request.commitTs, request.batch.mutations)
				completed <- treeDBCommitCompletion{request: request, err: err}
			}(request)
		}

		if queue == nil {
			completion := <-completed
			s.completeTreeDBCommit(completion, inFlightKeys, finished)
			inFlight--
			nextDelivery = s.deliverTreeDBCommits(finished, nextDelivery)
			continue
		}
		select {
		case request, ok := <-queue:
			if !ok {
				queue = nil
				continue
			}
			request.sequence = nextSequence
			nextSequence++
			pending = append(pending, request)
		case completion := <-completed:
			s.completeTreeDBCommit(completion, inFlightKeys, finished)
			inFlight--
			nextDelivery = s.deliverTreeDBCommits(finished, nextDelivery)
		}
	}
}

func nextIndependentTreeDBCommit(pending []treeDBCommitRequest, inFlightKeys map[string]struct{}) int {
	for i := range pending {
		independent := true
		for _, mutation := range pending[i].batch.mutations {
			if _, exists := inFlightKeys[string(mutation.Key)]; exists {
				independent = false
				break
			}
		}
		if independent {
			return i
		}
	}
	return -1
}

func reserveTreeDBCommitKeys(mutations []mvcc.Mutation, inFlightKeys map[string]struct{}) {
	for _, mutation := range mutations {
		inFlightKeys[string(mutation.Key)] = struct{}{}
	}
}

func (s *TreeDBStore) completeTreeDBCommit(
	completion treeDBCommitCompletion, inFlightKeys map[string]struct{}, finished map[uint64]treeDBCommitCompletion,
) {
	for _, mutation := range completion.request.batch.mutations {
		if inFlightKeys != nil {
			delete(inFlightKeys, string(mutation.Key))
		}
	}
	s.releaseMutationBatch(completion.request.batch)
	s.commitWG.Done()
	s.releaseTreeDBCommitSlot()
	if finished != nil {
		finished[completion.request.sequence] = completion
	}
}

// deliverTreeDBCommits preserves the original queue's externally observable
// acknowledgement order even when the underlying independent commits complete
// in a different order. A later synchronous caller therefore cannot return or
// receive an error ahead of an earlier admitted request.
func (s *TreeDBStore) deliverTreeDBCommits(finished map[uint64]treeDBCommitCompletion, next uint64) uint64 {
	for {
		completion, ok := finished[next]
		if !ok {
			return next
		}
		delete(finished, next)
		s.deliverTreeDBCommit(completion)
		next++
	}
}

func (s *TreeDBStore) deliverTreeDBCommit(completion treeDBCommitCompletion) {
	if completion.request.done != nil {
		completion.request.done <- completion.err
	}
	// Callbacks remain off the scheduler so they may call Close without
	// deadlocking the queue drain. Storage work and its owned buffers remain
	// bounded regardless of callback behavior.
	if completion.request.callback != nil {
		go completion.request.callback(completion.err)
	}
}

func (s *TreeDBStore) commitSync(commitTs uint64, batch *treeDBMutationBatch) error {
	if s == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	if s.closed.Load() || s.db == nil {
		s.lifecycleMu.Unlock()
		s.releaseMutationBatch(batch)
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	s.acquireTreeDBCommitSlot()
	if !s.queueActive {
		s.commitWG.Add(1)
		s.lifecycleMu.Unlock()
		defer s.commitWG.Done()
		defer s.releaseTreeDBCommitSlot()
		defer s.releaseMutationBatch(batch)
		return s.commitAtAdmitted(commitTs, batch.mutations)
	}

	done := make(chan error, 1)
	s.commitWG.Add(1)
	s.commitQueue <- treeDBCommitRequest{commitTs: commitTs, batch: batch, done: done}
	s.lifecycleMu.Unlock()
	return <-done
}

func (s *TreeDBStore) acquireTreeDBCommitSlot() {
	s.commitSlots <- struct{}{}
}

func (s *TreeDBStore) releaseTreeDBCommitSlot() {
	<-s.commitSlots
}

func (s *TreeDBStore) commitAtAdmitted(commitTs uint64, mutations []mvcc.Mutation) error {
	if s.beforeCommitForTest != nil {
		s.beforeCommitForTest()
	}
	if s.commitStartedForTest != nil {
		s.commitStartedForTest(commitTs, mutations)
	}
	err := s.mvcc.CommitAt(commitTs, mutations, s.commitMode)
	if s.commitFinishedForTest != nil {
		s.commitFinishedForTest(commitTs, err)
	}
	if err != nil {
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
