/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/ristretto/v2/z"
)

func TestTreeDBCommitEventFilterAndKVConversion(t *testing.T) {
	matcher, err := newTreeDBCommitEventMatcher(&pb.SubscriptionRequest{
		Prefixes: [][]byte{[]byte("posting/")},
		Matches:  []*badgerpb.Match{{Prefix: []byte("xZ/type"), IgnoreBytes: "0"}},
	})
	require.NoError(t, err)
	event := posting.CommitEvent{
		CommitTs: 42,
		Mutations: []posting.CommitMutation{
			{Entry: posting.Entry{
				Key: []byte("posting/1"), Value: []byte("value"), UserMeta: 7, ExpiresAt: 9,
			}},
			{Entry: posting.Entry{Key: []byte("yZ/type/person")}, Delete: true},
			{Entry: posting.Entry{Key: []byte("unmatched"), Value: []byte("ignored")}},
		},
	}

	kvs := treeDBCommitEventKVList(event, matcher)
	require.Len(t, kvs.Kv, 2)
	require.Equal(t, []byte("posting/1"), kvs.Kv[0].Key)
	require.Equal(t, []byte("value"), kvs.Kv[0].Value)
	require.Equal(t, []byte{7}, kvs.Kv[0].Meta)
	require.Nil(t, kvs.Kv[0].UserMeta)
	require.Equal(t, uint64(42), kvs.Kv[0].Version)
	require.Equal(t, uint64(9), kvs.Kv[0].ExpiresAt)
	require.Equal(t, []byte("yZ/type/person"), kvs.Kv[1].Key)
	require.Nil(t, kvs.Kv[1].Value)
	require.Equal(t, []byte{0}, kvs.Kv[1].Meta)

	_, err = newTreeDBCommitEventMatcher(&pb.SubscriptionRequest{
		Matches: []*badgerpb.Match{{Prefix: []byte("bad"), IgnoreBytes: "nope"}},
	})
	require.Error(t, err)
}

func TestTreeDBCommitEventSubscriptionStreamsFutureCommitsAndCancels(t *testing.T) {
	oldConfig, oldState := Config, State
	bus := posting.NewCommitEventBus(8)
	t.Cleanup(func() {
		bus.Close()
		Config, State = oldConfig, oldState
	})
	Config.PostingStoreBackend = PostingStoreBackendTreeDB
	State.CommitEvents = bus
	ctx, cancel := context.WithCancel(context.Background())
	stream := &recordingSubscribeStream{ctx: ctx, sent: make(chan *badgerpb.KVList, 1)}
	done := make(chan error, 1)
	go func() {
		done <- (&grpcWorker{}).Subscribe(&pb.SubscriptionRequest{
			Prefixes: [][]byte{[]byte("treedb-subscription/")},
		}, stream)
	}()
	require.Eventually(t, func() bool { return bus.SubscriberCount() == 1 }, time.Second, time.Millisecond)

	db, err := badger.OpenManaged(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	store := posting.WithCommitEvents(posting.NewBadgerStore(db), bus)
	txn := store.NewWriteTxn()
	require.NoError(t, txn.SetEntry(posting.Entry{
		Key: []byte("treedb-subscription/key"), Value: []byte("future"), UserMeta: 5,
	}))
	require.NoError(t, txn.CommitAt(uint64(time.Now().UnixNano()), nil))
	select {
	case got := <-stream.sent:
		require.Len(t, got.Kv, 1)
		require.Equal(t, []byte("future"), got.Kv[0].Value)
		require.Equal(t, []byte{5}, got.Kv[0].Meta)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TreeDB commit event")
	}

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("TreeDB commit-event subscription did not cancel")
	}
}

func TestTreeDBDisabledSubscriptionsExitWithoutRetryOrStateAccess(t *testing.T) {
	oldConfig, oldState := Config, State
	t.Cleanup(func() { Config, State = oldConfig, oldState })
	Config.PostingStoreBackend = PostingStoreBackendTreeDB
	State.CommitEvents = nil

	require.NoError(t, (&grpcWorker{}).Subscribe(nil, nil))
	closer := z.NewCloser(1)
	SubscribeForUpdates(nil, "", func(*badgerpb.KVList) {
		t.Fatal("disabled TreeDB subscription invoked callback")
	}, 1, closer)
	done := make(chan struct{})
	go func() { closer.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled TreeDB subscription entered retry loop")
	}
}

type recordingSubscribeStream struct {
	ctx  context.Context
	sent chan *badgerpb.KVList
}

func (s *recordingSubscribeStream) Send(kvs *badgerpb.KVList) error {
	select {
	case s.sent <- kvs:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *recordingSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *recordingSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *recordingSubscribeStream) SetTrailer(metadata.MD)       {}
func (s *recordingSubscribeStream) Context() context.Context     { return s.ctx }
func (s *recordingSubscribeStream) SendMsg(interface{}) error {
	return errors.New("unexpected SendMsg")
}
func (s *recordingSubscribeStream) RecvMsg(interface{}) error {
	return errors.New("unexpected RecvMsg")
}

var _ pb.Worker_SubscribeServer = (*recordingSubscribeStream)(nil)
