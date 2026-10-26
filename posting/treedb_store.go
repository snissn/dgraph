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
	"strconv"
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
	// TreeDBCommitExternallyRecoverable requires an explicit external recovery
	// owner; completion means installed materialization, backed by retained Raft.
	TreeDBCommitExternallyRecoverable
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
	// ErrTreeDBEntryTooLarge is terminal: retrying an empty storage transaction
	// cannot make its first entry fit the producer's bounded work envelope.
	ErrTreeDBEntryTooLarge = errors.New("posting treedb: entry exceeds storage preparation limit")
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
	// dispatcher retains this bound while collecting a durable cohort.
	treeDBCommitOwnershipLimit = treeDBCommitQueueCapacity + 1
	// An oversized individual transaction remains atomic; following requests
	// wait for the next cohort rather than growing that transaction's batch.
	treeDBCommitCohortBytes = 4 << 20
)

// TreeDBStore owns exactly one TreeDB handle and exactly one external-MVCC
// owner. Close is idempotent and closes that handle at most once.
type TreeDBStore struct {
	external   *treedb.ExternalRecovery
	db         *treedb.DB
	mvcc       *mvcc.Store
	commitMode mvcc.CommitMode
	mode       TreeDBCommitMode
	durability string

	lifecycleMu        sync.Mutex
	rootGate           sync.RWMutex // external snapshot install drains read handles
	rootEpoch          atomic.Uint64
	raftReadMu         sync.Mutex
	raftReadPins       uint64
	raftReadChanged    chan struct{}
	raftReadDraining   bool
	raftSemanticFloor  uint64
	raftPending        RaftPendingManifest
	commitWG           sync.WaitGroup
	workerWG           sync.WaitGroup
	queueActive        bool
	commitQueue        chan treeDBCommitRequest
	commitSlots        chan struct{}
	commitWake         chan struct{}
	commitSequence     atomic.Uint64
	commitSeal         atomic.Uint64
	commitCompleted    atomic.Uint64
	commitStagedBytes  atomic.Int64
	writeWorkLimit     uint64
	writeWorkEstimator treedb.MVCCWriteWorkEstimator
	stagedCommitGroups bool
	mutationPool       sync.Pool
	closeOnce          sync.Once
	closeErr           error
	closed             atomic.Bool
	durableCohorts     atomic.Uint64
	durableRequests    atomic.Uint64
	durableCohortMax   atomic.Uint64
	durableOverlap     atomic.Uint64
	durableSeals       atomic.Uint64

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
	// commitGroupsForTest injects the storage result at the ambiguity boundary.
	// Production stores always use the owned MVCC store.
	commitGroupsForTest func([]mvcc.CommitGroup, mvcc.CommitMode) error
}

// OpenTreeDBStore opens and owns a TreeDB posting store. The adapter commit
// mode selects whether each MVCC commit uses Write or WriteSync. TreeDB's
// canonical production profiles all support an explicit WriteSync opt-up even
// when their ordinary acknowledgement policy is relaxed.
func OpenTreeDBStore(opts treedb.Options, mode TreeDBCommitMode) (*TreeDBStore, error) {
	if mode == TreeDBCommitExternallyRecoverable {
		return nil, fmt.Errorf("external recovery mode requires OpenRaftTreeDBStore")
	}
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
		commitWake:  make(chan struct{}, 1),
	}
	store.initializeWriteWork()
	store.stagedCommitGroups = store.mvcc.SupportsCommitGroupStaging()
	store.mutationPool.New = newTreeDBMutationBatch
	return store, nil
}

// initializeWriteWork captures only immutable, options-derived shaping constants.
// External checkpoint installation reopens with the same options (only Dir
// changes), so these constants remain valid across root replacement and Close.
// They are never refreshed concurrently with SetEntry; engine admission still
// decides whether the current live history/resources fit.
func (s *TreeDBStore) initializeWriteWork() {
	s.writeWorkLimit = s.db.MVCCWriteWorkLimit()
	s.writeWorkEstimator = s.db.NewMVCCWriteWorkEstimator()
}

func newTreeDBMutationBatch() any {
	return &treeDBMutationBatch{
		mutations: make([]mvcc.Mutation, 0, 16),
		arena:     make([]byte, 0, 4<<10),
	}
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
	tracked := s.mode == TreeDBCommitExternallyRecoverable
	if tracked {
		s.rootGate.RLock()
	}
	return &treeDBReadTxn{store: s, readTs: readTs, rootReadHeld: tracked,
		iterators: make(map[*treeDBIterator]struct{})}
}

func (s *TreeDBStore) NewWriteTxn() WriteTxn {
	batch := s.mutationPool.Get().(*treeDBMutationBatch)
	return &treeDBWriteTxn{store: s, batch: batch, rootEpoch: s.rootEpoch.Load()}
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
		// Seal an open native successor before waiting for its completion. The
		// input channel stays open until every admitted commit has drained.
		s.wakeCommitSubmitter()

		// No new commit can be admitted after closed is published. Wait for every
		// commit accepted before that boundary before closing the owned DB.
		s.commitWG.Wait()
		close(s.commitQueue)
		s.workerWG.Wait()
		if s.external != nil {
			s.rootGate.Lock()
			defer s.rootGate.Unlock()
		}

		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()
		if s.db != nil {
			if s.external != nil {
				s.closeErr = s.external.Close()
			} else {
				s.closeErr = s.db.Close()
			}
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
	stats := cloneTreeDBStats(s.db.Stats())
	stats["dgraph.treedb.durable_cohorts_total"] = strconv.FormatUint(s.durableCohorts.Load(), 10)
	stats["dgraph.treedb.durable_requests_total"] = strconv.FormatUint(s.durableRequests.Load(), 10)
	stats["dgraph.treedb.durable_cohort_size_max"] = strconv.FormatUint(s.durableCohortMax.Load(), 10)
	stats["dgraph.treedb.commit_pipeline"] = "serial"
	if s.mode == TreeDBCommitDurable {
		stats["dgraph.treedb.commit_pipeline"] = "fifo"
		if s.stagedCommitGroups {
			stats["dgraph.treedb.commit_pipeline"] = "pressure_v4"
		}
	}
	stats["dgraph.treedb.durable_staged_input_bytes"] = strconv.FormatInt(s.commitStagedBytes.Load(), 10)
	stats["dgraph.treedb.durable_overlap_submissions_total"] = strconv.FormatUint(s.durableOverlap.Load(), 10)
	stats["dgraph.treedb.durable_producer_seals_total"] = strconv.FormatUint(s.durableSeals.Load(), 10)
	return stats, nil
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
	if s.external != nil {
		// GC changes physical reachability, not the semantic MVCC floor. The
		// owner's current/previous retained roots participate in the maintained
		// recoverable-root scan. Keep the DB pointer and owner lifecycle pinned
		// for the whole pass, including any metadata publication.
		var stats treedb.ValueLogGCStats
		err := s.external.Apply(func() error {
			var err error
			stats, err = s.db.ValueLogGC(ctx, opts)
			return err
		})
		if err != nil {
			return stats, fmt.Errorf("value-log GC posting TreeDB: %w", err)
		}
		return stats, nil
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
	if s.external != nil {
		if opts.UnsafeValueLogReclaimFencedUnreferenced {
			return treedb.CompactStorageStats{}, fmt.Errorf("external recovery: unsafe fenced reclaim is unsupported")
		}
		// This API only reorganizes physical storage. Its maintained retained-root
		// closure protects both certificates; no logical MVCC floor is advanced.
		var stats treedb.CompactStorageStats
		err := s.external.Apply(func() error {
			var err error
			stats, err = s.db.CompactStorage(ctx, opts)
			return err
		})
		if err != nil {
			return stats, fmt.Errorf("compact posting TreeDB storage: %w", err)
		}
		return stats, nil
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
// configured acknowledgement boundary. The caller must establish that reads,
// posting dependencies and future commits no longer need timestamps at or below
// this floor; Raft snapshot publication alone does not establish that boundary.
func (s *TreeDBStore) AdvanceDiscardFloor(timestamp uint64) error {
	if s == nil {
		return fmt.Errorf("advance discard floor posting TreeDB: %w", treedb.ErrClosed)
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.IsClosed() || s.db == nil {
		return fmt.Errorf("advance discard floor posting TreeDB: %w", treedb.ErrClosed)
	}
	if s.external != nil {
		return fmt.Errorf("external recovery prototype: discard-floor advancement is unsupported")
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
	if s.external != nil {
		return mvcc.PruneStats{}, fmt.Errorf("external recovery prototype: version pruning is unsupported")
	}
	return s.mvcc.PruneVersions(mvcc.PruneOptions{BatchSize: batchSize, Mode: s.commitMode})
}

type treeDBWriteTxn struct {
	mu        sync.Mutex
	store     *TreeDBStore
	batch     *treeDBMutationBatch
	stickyErr error
	closed    bool
	rootEpoch uint64
}

type treeDBMutationBatch struct {
	mutations []mvcc.Mutation
	arena     []byte
	work      uint64
	// chunks retains every previous arena backing array after growth. Staged
	// mutations still point into those arrays, and release must scrub all of
	// them rather than only the final arena.
	chunks [][]byte
}

type treeDBCommitRequest struct {
	sequence  uint64
	commitTs  uint64
	batch     *treeDBMutationBatch
	callback  func(error)
	done      chan error
	rootEpoch uint64
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
	if err := t.admitEntryWork(len(entry.Key), treeDBEnvelopeHeader+len(entry.Value)); err != nil {
		return err
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
	if err := t.admitEntryWork(len(key), 0); err != nil {
		return err
	}
	owned := t.batch.ownBytes(len(key))
	copy(owned, key)
	t.setMutation(mvcc.Mutation{Key: owned, Delete: true})
	return nil
}

// Check before copying or mutating the current transaction. CommitToDisk
// already commits a nonempty prefix on ErrTxnTooBig and retries the next key
// at the same timestamp. An oversized first entry instead fails terminally.
func (t *treeDBWriteTxn) admitEntryWork(keyBytes, valueBytes int) error {
	limit := t.store.writeWorkLimit
	if limit == 0 {
		return nil
	}
	work := t.store.writeWorkEstimator.Estimate(keyBytes, valueBytes)
	if work > limit {
		t.stickyErr = ErrTreeDBEntryTooLarge
		return t.stickyErr
	}
	if t.batch.work > limit-work {
		if len(t.batch.mutations) == 0 {
			t.stickyErr = ErrTreeDBEntryTooLarge
			return t.stickyErr
		}
		return badger.ErrTxnTooBig
	}
	t.batch.work += work
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
		return t.store.commitSync(commitTs, batch, t.rootEpoch)
	}
	if err != nil {
		t.store.releaseMutationBatch(batch)
		go cb(err)
		return nil
	}
	request := treeDBCommitRequest{commitTs: commitTs, batch: batch, callback: cb, rootEpoch: t.rootEpoch}
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
		batch.work = 0
		s.mutationPool.Put(batch)
	}
}

func (s *TreeDBStore) enqueueCommit(request treeDBCommitRequest) error {
	if s == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	// Wait for ownership outside lifecycleMu. A saturated durable queue must
	// not hold the lifecycle gate throughout somebody else's disk wait. The
	// subsequent closed check and Add/send share Close's admission authority.
	s.acquireTreeDBCommitSlot()
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed.Load() || s.db == nil {
		s.releaseTreeDBCommitSlot()
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	if request.rootEpoch != s.rootEpoch.Load() {
		s.releaseTreeDBCommitSlot()
		return fmt.Errorf("commit across posting root install: %w", ErrTreeDBTxnClosed)
	}
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
	request.sequence = s.commitSequence.Add(1)
	s.commitQueue <- request
	return nil
}

// SealCommitWindow publishes an admitted FIFO endpoint without waiting for
// storage or claiming completion. The endpoint coalesces duplicate producer
// wakeups; collection still follows the predecessor completion or capacity boundary and original bounds.
func (s *TreeDBStore) SealCommitWindow() {
	if s == nil || s.mode != TreeDBCommitDurable {
		return
	}
	endpoint := s.commitSequence.Load()
	for previous := s.commitSeal.Load(); endpoint > previous; previous = s.commitSeal.Load() {
		if s.commitSeal.CompareAndSwap(previous, endpoint) {
			s.durableSeals.Add(1)
			select {
			case s.commitWake <- struct{}{}:
			default:
			}
			return
		}
	}
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
		s.completeTreeDBCommit(treeDBCommitCompletion{request: request, err: err})
		s.deliverTreeDBCommit(treeDBCommitCompletion{request: request, err: err})
	}
}

// runDurableCommitQueue drains work that accumulated during the previous disk
// wait into one timestamp-preserving MVCC publication. There is no batching
// timer, per-request goroutine, conflict map or out-of-order completion queue.
// Versions of the same key at different timestamps can share the same sync.
func (s *TreeDBStore) runDurableCommitQueueFIFO() {
	var requests [treeDBCommitOwnershipLimit]treeDBCommitRequest
	var groups [treeDBCommitOwnershipLimit]mvcc.CommitGroup
	var results [treeDBCommitOwnershipLimit]error
	var carried treeDBCommitRequest
	hasCarried := false
	queue := s.commitQueue
	for {
		first := carried
		if !hasCarried {
			var ok bool
			first, ok = <-queue
			if !ok {
				return
			}
		}
		carried = treeDBCommitRequest{}
		hasCarried = false
		requests[0] = first
		n := 1
		ownedBytes := first.batch.ownedBytes()
		work := first.batch.work
	collect:
		for n < len(requests) && ownedBytes < treeDBCommitCohortBytes {
			select {
			case request, ok := <-queue:
				if !ok {
					queue = nil
					break collect
				}
				size := request.batch.ownedBytes()
				if size > treeDBCommitCohortBytes-ownedBytes ||
					(s.writeWorkLimit != 0 && (work > s.writeWorkLimit || request.batch.work > s.writeWorkLimit-work)) {
					carried, hasCarried = request, true
					break collect
				}
				requests[n] = request
				n++
				ownedBytes += size
				work += request.batch.work
			default:
				break collect
			}
		}
		s.durableCohorts.Add(1)
		s.durableRequests.Add(uint64(n))
		if uint64(n) > s.durableCohortMax.Load() {
			s.durableCohortMax.Store(uint64(n)) // only this worker writes it
		}
		if n == 1 {
			results[0] = s.commitAtAdmitted(first.commitTs, first.batch.mutations)
		} else {
			for i := range n {
				request := &requests[i]
				if s.beforeCommitForTest != nil {
					s.beforeCommitForTest()
				}
				if s.commitStartedForTest != nil {
					s.commitStartedForTest(request.commitTs, request.batch.mutations)
				}
				groups[i] = mvcc.CommitGroup{Timestamp: request.commitTs, Mutations: request.batch.mutations}
			}
			s.commitDurableGroups(groups[:n], results[:n])
			if s.commitFinishedForTest != nil {
				for i := range n {
					s.commitFinishedForTest(requests[i].commitTs, results[i])
				}
			}
		}
		for i := range n {
			completion := treeDBCommitCompletion{request: requests[i], err: results[i]}
			s.completeTreeDBCommit(completion)
			s.deliverTreeDBCommit(completion)
			requests[i] = treeDBCommitRequest{}
			groups[i] = mvcc.CommitGroup{}
			results[i] = nil
		}
		if queue == nil && !hasCarried {
			return
		}
	}
}

func (b *treeDBMutationBatch) ownedBytes() int {
	n := len(b.arena)
	for _, chunk := range b.chunks {
		n += len(chunk)
	}
	return n
}

// CommitGroupAt validates timestamps, keys, duplicate physical versions and the
// discard floor before storage admission. Only these explicit validation errors
// permit subdivision: one invalid request must not poison valid neighbours,
// and two successive writes of the same key at the same timestamp must retain
// FIFO last-write semantics. Storage errors can be ambiguous and are NEVER
// retried, even when a joined error also contains a validation sentinel.
func (s *TreeDBStore) commitDurableGroups(groups []mvcc.CommitGroup, results []error) {
	var err error
	if s.commitGroupsForTest != nil {
		err = s.commitGroupsForTest(groups, s.commitMode)
	} else {
		err = s.mvcc.CommitGroupAt(groups, s.commitMode)
	}
	validation := errors.Is(err, mvcc.ErrZeroTimestamp) || errors.Is(err, mvcc.ErrInvalidKey) ||
		errors.Is(err, mvcc.ErrDuplicateKey) || errors.Is(err, mvcc.ErrVersionBelowDiscardFloor)
	if len(groups) > 1 && validation && !errors.Is(err, mvcc.ErrStorage) {
		middle := len(groups) / 2
		s.commitDurableGroups(groups[:middle], results[:middle])
		s.commitDurableGroups(groups[middle:], results[middle:])
		return
	}
	if err != nil {
		err = fmt.Errorf("commit posting TreeDB transaction: %w", err)
	}
	for i := range results {
		results[i] = err
	}
}

func (s *TreeDBStore) completeTreeDBCommit(completion treeDBCommitCompletion) {
	s.releaseMutationBatch(completion.request.batch)
	s.commitWG.Done()
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
	// Admission ownership lasts through FIFO acknowledgement delivery, including
	// requests retained in the active cohort and its carried successor.
	s.releaseTreeDBCommitSlot()
}

func (s *TreeDBStore) commitSync(commitTs uint64, batch *treeDBMutationBatch, rootEpoch uint64) error {
	if s == nil {
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	if s.mode == TreeDBCommitDurable {
		done := make(chan error, 1)
		err := s.enqueueCommit(treeDBCommitRequest{commitTs: commitTs, batch: batch, done: done, rootEpoch: rootEpoch})
		if err != nil {
			s.releaseMutationBatch(batch)
			return err
		}
		s.SealCommitWindow()
		return <-done
	}
	// Every admission path reserves capacity before lifecycleMu. Callback
	// callers can already own every slot while waiting for this mutex.
	s.acquireTreeDBCommitSlot()
	s.lifecycleMu.Lock()
	if s.closed.Load() || s.db == nil {
		s.lifecycleMu.Unlock()
		s.releaseTreeDBCommitSlot()
		s.releaseMutationBatch(batch)
		return fmt.Errorf("commit posting TreeDB transaction: %w", treedb.ErrClosed)
	}
	if rootEpoch != s.rootEpoch.Load() {
		s.lifecycleMu.Unlock()
		s.releaseTreeDBCommitSlot()
		s.releaseMutationBatch(batch)
		return fmt.Errorf("commit across posting root install: %w", ErrTreeDBTxnClosed)
	}
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
	var err error
	if s.external != nil {
		err = s.external.Apply(func() error { return s.mvcc.CommitAt(commitTs, mutations, s.commitMode) })
	} else {
		err = s.mvcc.CommitAt(commitTs, mutations, s.commitMode)
	}
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
	mu           sync.Mutex
	store        *TreeDBStore
	readTs       uint64
	discarded    bool
	rootReadHeld bool
	iterators    map[*treeDBIterator]struct{}
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
	if t.rootReadHeld {
		t.store.rootGate.RUnlock()
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
	opened := it.raw == nil
	if (opened || (it.opts.Reverse && len(key) == 0)) && !it.openRawLocked() {
		return
	}
	// IterateVersions already positions a newly opened exact-key view. Reuse
	// that position only for the identical forward target; other targets must
	// still pass Seek's key validation, and later seeks must reposition.
	if opened && !it.opts.Reverse && it.hasExactKey && bytes.Equal(key, it.exactKey) {
		it.valid = false
		it.positionLocked()
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
		// Preserve a nonnil empty key, which means exact empty-key iteration.
		options.ExactKey = make([]byte, len(it.exactKey))
		copy(options.ExactKey, it.exactKey)
	}
	raw, err := it.store.mvcc.IterateVersions(options)
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
