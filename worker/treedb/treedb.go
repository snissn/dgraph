/*
 * SPDX-FileCopyrightText: 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

// Package treedb contains the Dgraph-side TreeDB integration scaffold.
//
// It intentionally does not replace the Alpha posting store yet. Dgraph's
// current posting-store contract exposes Badger-specific managed transactions,
// protobuf values, streams, subscriptions, encryption, and cache metrics across
// package boundaries. This package pins and compile-tests the TreeDB entry point
// that a future runtime backend switch can build on.
package treedb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	td "github.com/snissn/gomap/TreeDB"
)

const (
	// DefaultProfile is the durable TreeDB profile used for Dgraph posting-store
	// experiments. It enables TreeDB command WAL rather than benchmark-only
	// no-WAL modes.
	DefaultProfile = td.ProfileCommandWALDurable

	// DefaultKeepRecent mirrors Dgraph's current Badger posting-store retention
	// setting, which keeps a very large version window for externally managed
	// read timestamps.
	DefaultKeepRecent = uint64(math.MaxInt32)
)

var (
	// ErrUnsupportedFeature marks Badger capabilities that Dgraph currently uses
	// but this TreeDB integration scaffold must reject until TreeDB exposes a
	// matching contract.
	ErrUnsupportedFeature = errors.New("dgraph treedb integration: unsupported feature")

	errEmptyDir = errors.New("dgraph treedb integration: dir must be non-empty")
)

// OpenOptions describes a Dgraph-shaped TreeDB open.
type OpenOptions struct {
	Dir string

	// Profile defaults to DefaultProfile. Non-public/deprecated TreeDB profiles
	// are rejected because this package is a runtime integration surface, not a
	// benchmark harness.
	Profile td.Profile

	// KeepRecent defaults to DefaultKeepRecent.
	KeepRecent uint64

	// MemtableMode optionally overrides the TreeDB profile default.
	MemtableMode string

	// RequireEncryption must be false until TreeDB exposes a Dgraph-compatible
	// encrypted-at-rest/key-registry contract.
	RequireEncryption bool

	// RequireInMemory must be false. Dgraph's posting store is persistent.
	RequireInMemory bool
}

// Handle owns an opened TreeDB instance and the options used to create it.
type Handle struct {
	Options td.Options
	DB      *td.DB
}

// UnsupportedFeatures returns the known runtime blockers for replacing
// Dgraph's Badger posting store with TreeDB.
func UnsupportedFeatures() []string {
	return []string{
		"badger managed transaction compatibility: OpenManaged, NewTransactionAt, CommitAt, NewManagedWriteBatch, SetEntryAt",
		"TreeDB native conditional transactions currently return unsupported under the durable command-WAL profile",
		"badger entry metadata and TTL compatibility: Entry.UserMeta, Item.UserMeta, Entry.ExpiresAt",
		"badger all-version/key iterator compatibility: NewKeyIterator plus IteratorOptions.AllVersions/Prefix/PrefetchValues",
		"badger stream import/export compatibility: NewStreamAt, Stream.Orchestrate, NewStreamWriter",
		"badger subscription API used by worker.SubscribeForUpdates",
		"badger encryption/key-registry APIs used by posting stores, backups, debug, and raftwal",
		"badger protobuf compatibility: github.com/dgraph-io/badger/v4/pb KV/KVList/Match",
		"badger cache metrics and cache sizing APIs used by Dgraph monitoring",
	}
}

// ResolveOptions converts Dgraph integration settings into TreeDB options.
func ResolveOptions(cfg OpenOptions) (td.Options, error) {
	dir := strings.TrimSpace(cfg.Dir)
	if dir == "" {
		return td.Options{}, errEmptyDir
	}
	if cfg.RequireEncryption {
		return td.Options{}, unsupported("encryption/key registry")
	}
	if cfg.RequireInMemory {
		return td.Options{}, unsupported("in-memory posting store")
	}

	profile := cfg.Profile
	if profile == "" {
		profile = DefaultProfile
	}
	normalized, ok := td.NormalizePublicProfile(profile)
	if !ok {
		return td.Options{}, fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", profile, td.ProfileFlagHelp)
	}

	opts := td.OptionsFor(normalized, dir)
	opts.KeepRecent = DefaultKeepRecent
	if cfg.KeepRecent != 0 {
		opts.KeepRecent = cfg.KeepRecent
	}
	if mode := strings.TrimSpace(cfg.MemtableMode); mode != "" {
		opts.MemtableMode = strings.ToLower(mode)
	}
	return opts, nil
}

// Open opens TreeDB with Dgraph-shaped defaults. The caller owns the returned
// handle and must call Close.
func Open(cfg OpenOptions) (*Handle, error) {
	opts, err := ResolveOptions(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("create TreeDB directory: %w", err)
	}
	db, err := td.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Handle{Options: opts, DB: db}, nil
}

// Close closes the opened TreeDB handle. It is safe to call on a nil handle.
func (h *Handle) Close() error {
	if h == nil || h.DB == nil {
		return nil
	}
	return h.DB.Close()
}

func unsupported(feature string) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedFeature, feature)
}

type dgraphTreeDBAPI interface {
	Set([]byte, []byte) error
	Delete([]byte) error
	Get([]byte) ([]byte, error)
	GetVersioned([]byte) ([]byte, td.EntryRevision, error)
	AcquireSnapshot() td.Snapshot
	Iterator([]byte, []byte) (td.Iterator, error)
	ReverseIterator([]byte, []byte) (td.Iterator, error)
	NewBatch() td.Batch
	NewConditionalTxn() (*td.ConditionalTxn, error)
	ValueLogGC(context.Context, td.ValueLogGCOptions) (td.ValueLogGCStats, error)
	CompactStorage(context.Context, td.CompactStorageOptions) (td.CompactStorageStats, error)
	Stats() map[string]string
	Close() error
}

type dgraphTreeDBSnapshotAPI interface {
	Get([]byte) ([]byte, error)
	GetVersioned([]byte) ([]byte, td.EntryRevision, error)
	Iterate([]byte, []byte, func([]byte, []byte) error) error
	ReverseIterate([]byte, []byte, func([]byte, []byte) error) error
	Close() error
}

type dgraphTreeDBBatchAPI interface {
	Set([]byte, []byte) error
	Delete([]byte) error
	Write() error
	WriteSync() error
	Close() error
}

type dgraphTreeDBConditionalTxnAPI interface {
	GetVersioned([]byte) ([]byte, td.EntryRevision, error)
	SetWithRevision([]byte, []byte, td.EntryRevision) error
	DeleteWithRevision([]byte, td.EntryRevision) error
	Commit() error
	CommitSync() error
	Close() error
}

var (
	_ dgraphTreeDBAPI               = (*td.DB)(nil)
	_ dgraphTreeDBSnapshotAPI       = td.Snapshot(nil)
	_ dgraphTreeDBBatchAPI          = td.Batch(nil)
	_ dgraphTreeDBConditionalTxnAPI = (*td.ConditionalTxn)(nil)
)
