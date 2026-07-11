/*
 * SPDX-FileCopyrightText: 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

// Package treedb contains the Dgraph-side TreeDB integration scaffold.
//
// It intentionally does not replace the Alpha posting store yet. Dgraph's
// operational paths still expose Badger-specific protobuf values, streams,
// subscriptions, encryption, and cache metrics across package boundaries. The
// benchmark-minimal posting contract has a TreeDBStore implementation, while
// this package keeps runtime selection fail-closed until Alpha lifecycle wiring.
package treedb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	td "github.com/snissn/gomap/TreeDB"
)

const (
	// DefaultProfile is the durable TreeDB profile used for Dgraph posting-store
	// experiments. It enables TreeDB command WAL rather than benchmark-only
	// no-WAL modes.
	DefaultProfile = td.ProfileCommandWALDurable

	// DefaultKeepRecent leaves TreeDB's profile default in place. Dgraph's
	// Gomap external MVCC owns historical retention and pruning, so this scaffold
	// leaves the physical TreeDB generation-retention profile unchanged.
	DefaultKeepRecent uint64 = 0
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

	// Profile defaults to DefaultProfile. Non-command-WAL and benchmark-only
	// TreeDB profiles are rejected because this package is a runtime integration
	// surface, not a benchmark harness.
	Profile td.Profile

	// KeepRecent optionally overrides TreeDB's profile default. Leave it zero
	// until TreeDB supports Dgraph-managed timestamp reads.
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

// ResolveOptions converts Dgraph integration settings into TreeDB options.
func ResolveOptions(cfg OpenOptions) (td.Options, error) {
	dir := strings.TrimSpace(cfg.Dir)
	if dir == "" {
		return td.Options{}, errEmptyDir
	}
	if cfg.RequireEncryption {
		return td.Options{}, CheckRequiredFeatures(FeatureEncryptionKeyRegistry)
	}
	if cfg.RequireInMemory {
		return td.Options{}, CheckRequiredFeatures(FeatureInMemoryPostingStore)
	}

	profile := cfg.Profile
	if profile == "" {
		profile = DefaultProfile
	}
	normalized, ok := td.NormalizePublicProfile(profile)
	if !ok || !isDgraphRuntimeProfile(normalized) {
		return td.Options{}, fmt.Errorf("unsupported TreeDB profile %q; allowed: command_wal_durable or command_wal_relaxed", profile)
	}

	opts := td.OptionsFor(normalized, dir)
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

func isDgraphRuntimeProfile(profile td.Profile) bool {
	switch profile {
	case td.ProfileCommandWALDurable, td.ProfileCommandWALRelaxed:
		return true
	default:
		return false
	}
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
