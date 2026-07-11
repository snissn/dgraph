/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"fmt"

	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/trie"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
)

const treeDBSubscriptionMatchID uint64 = 1

type treeDBCommitEventMatcher struct {
	trie *trie.Trie
}

func newTreeDBCommitEventMatcher(req *pb.SubscriptionRequest) (*treeDBCommitEventMatcher, error) {
	matcher := &treeDBCommitEventMatcher{trie: trie.NewTrie()}
	if req == nil {
		return matcher, nil
	}
	for _, prefix := range req.GetPrefixes() {
		if err := matcher.trie.AddMatch(badgerpb.Match{Prefix: prefix}, treeDBSubscriptionMatchID); err != nil {
			return nil, fmt.Errorf("add TreeDB commit-event prefix match: %w", err)
		}
	}
	for _, match := range req.GetMatches() {
		if match == nil {
			continue
		}
		if err := matcher.trie.AddMatch(*match, treeDBSubscriptionMatchID); err != nil {
			return nil, fmt.Errorf("add TreeDB commit-event match: %w", err)
		}
	}
	return matcher, nil
}

func (m *treeDBCommitEventMatcher) matches(key []byte) bool {
	if m == nil || m.trie == nil {
		return false
	}
	_, ok := m.trie.Get(key)[treeDBSubscriptionMatchID]
	return ok
}

// treeDBCommitEventKVList converts only the Dgraph fields emitted by Badger's
// publisher. Badger's publisher puts Entry.UserMeta in KV.Meta and does not
// expose its internal deletion bit; preserve that behavior exactly here. This
// restricted bridge does not claim the full Badger Subscribe contract or
// historical replay.
func treeDBCommitEventKVList(event posting.CommitEvent, matcher *treeDBCommitEventMatcher) *badgerpb.KVList {
	out := &badgerpb.KVList{}
	for _, mutation := range event.Mutations {
		if !matcher.matches(mutation.Entry.Key) {
			continue
		}
		value := mutation.Entry.Value
		if mutation.Delete {
			value = nil
		}
		out.Kv = append(out.Kv, &badgerpb.KV{
			Key:       append([]byte(nil), mutation.Entry.Key...),
			Value:     append([]byte(nil), value...),
			Meta:      []byte{mutation.Entry.UserMeta},
			Version:   event.CommitTs,
			ExpiresAt: mutation.Entry.ExpiresAt,
		})
	}
	return out
}

func subscribeToTreeDBCommitEvents(
	ctx context.Context,
	req *pb.SubscriptionRequest,
	stream pb.Worker_SubscribeServer,
	bus *posting.CommitEventBus,
) error {
	matcher, err := newTreeDBCommitEventMatcher(req)
	if err != nil {
		return err
	}
	events, err := bus.Subscribe(ctx, 16)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return nil
			}
			kvs := treeDBCommitEventKVList(event, matcher)
			if len(kvs.Kv) == 0 {
				continue
			}
			if err := stream.Send(kvs); err != nil {
				return err
			}
		}
	}
}
