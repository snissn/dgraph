/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	td "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func openRaftTransferStore(t *testing.T, identity string) *TreeDBStore {
	t.Helper()
	opts := td.OptionsFor(td.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	store, err := OpenRaftTreeDBStore(opts, identity)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func TestRaftTreeDBTransferDrainsReadersAndRejectsStaleWriters(t *testing.T) {
	source := openRaftTransferStore(t, "source")
	commitStoreEntry(t, source, 1, Entry{Key: []byte("key"), Value: []byte("source")})
	require.NoError(t, source.CheckpointRaft(context.Background(), []byte("source-cut")))
	cert, ok := source.RaftCheckpointCertificate()
	require.True(t, ok)
	var archive bytes.Buffer
	require.NoError(t, source.ExportRaftCheckpoint(context.Background(), &archive, cert.Generation, cert.Digest))
	receiver := openRaftTransferStore(t, "receiver")
	commitStoreEntry(t, receiver, 1, Entry{Key: []byte("key"), Value: []byte("old")})
	oldRead := receiver.NewReadTxn(math.MaxUint64)
	t.Cleanup(oldRead.Discard)
	it := oldRead.NewIterator(IteratorOptions{})
	it.Rewind()
	require.True(t, it.Valid())
	stale := receiver.NewWriteTxn()
	require.NoError(t, stale.SetEntry(Entry{Key: []byte("stale"), Value: []byte("must not publish")}))
	defer stale.Discard()
	commitEntered, releaseCommit, callback := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	receiver.beforeCommitForTest = func() {
		close(commitEntered)
		<-releaseCommit
	}
	admitted := receiver.NewWriteTxn()
	require.NoError(t, admitted.SetEntry(Entry{Key: []byte("admitted"), Value: []byte("old-root")}))
	require.NoError(t, admitted.CommitAt(2, func(err error) { callback <- err }))
	<-commitEntered
	started, authorized, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		_, err := receiver.InstallRaftCheckpoint(context.Background(), bytes.NewReader(archive.Bytes()),
			func(sourceCert td.ExternalRecoveryCheckpoint) ([]byte, error) {
				if sourceCert.Digest != cert.Digest || string(sourceCert.Metadata) != "source-cut" {
					return nil, errors.New("wrong source authority")
				}
				close(authorized)
				return []byte("receiver-accepted-cut"), nil
			})
		done <- err
	}()
	<-started
	// Wait until install has closed read admission. This observes the actual
	// adapter gate instead of guessing how long archive extraction takes.
	require.Eventually(t, func() bool {
		if receiver.rootGate.TryRLock() {
			receiver.rootGate.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	select {
	case <-authorized:
		t.Fatal("install authorization ran while old reader/iterator was live")
	default:
	}
	item, err := oldRead.Get([]byte("key"))
	require.NoError(t, err)
	value, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), value)
	oldRead.Discard() // Discard closes its iterator before releasing the root.
	select {
	case <-authorized:
		t.Fatal("install authorization ran while admitted storage was live")
	default:
	}
	close(releaseCommit)
	require.NoError(t, <-callback)
	require.NoError(t, <-done)
	<-authorized
	require.ErrorIs(t, stale.CommitAt(2, nil), ErrTreeDBTxnClosed)
	read := receiver.NewReadTxn(math.MaxUint64)
	defer read.Discard()
	item, err = read.Get([]byte("key"))
	require.NoError(t, err)
	value, err = item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("source"), value)
	installed, ok := receiver.RaftCheckpointCertificate()
	require.True(t, ok)
	require.Equal(t, "receiver", installed.Identity)
	require.Equal(t, []byte("receiver-accepted-cut"), installed.Metadata)
	require.Equal(t, cert.Root, installed.Root)
}
