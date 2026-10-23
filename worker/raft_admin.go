/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/x"
)

type raftAdminDrop struct {
	all, dataOnly, ban bool
	namespace          uint64
	predicate          string
}

func (d raftAdminDrop) matches(key []byte) bool {
	if len(key) < 9 || (key[0] != x.DefaultPrefix && key[0] != x.ByteSplit && key[0] != x.ByteSchema && key[0] != x.ByteType) {
		return false
	}
	if d.all {
		return true
	}
	if binary.BigEndian.Uint64(key[1:9]) != d.namespace {
		return false
	}
	if d.dataOnly && (key[0] == x.ByteSchema || key[0] == x.ByteType) {
		return false
	}
	if d.predicate == "" {
		return true
	}
	parsed, err := x.Parse(key)
	return err == nil && parsed.Attr == d.predicate && !parsed.IsType()
}

func raftAdminDropProposal(p *pb.Proposal) (raftAdminDrop, bool, error) {
	var drop raftAdminDrop
	if p.DeleteNs != nil {
		if p.DeleteNs.Namespace == x.RootNamespace || p.StartTs == 0 || p.Mutations != nil || len(p.Kv) != 0 {
			return drop, true, &raftSemanticRejection{reason: "namespace deletion requires an isolated non-root namespace"}
		}
		drop.namespace, drop.ban = p.DeleteNs.Namespace, true
		return drop, true, nil
	}
	m := p.Mutations
	if m == nil {
		return drop, false, nil
	}
	switch m.DropOp {
	case pb.Mutations_ALL:
		drop.all = true
	case pb.Mutations_DATA, pb.Mutations_ALL_IN_NS:
		ns, err := strconv.ParseUint(m.DropValue, 0, 64)
		if err != nil {
			return drop, true, &raftSemanticRejection{reason: "invalid drop namespace"}
		}
		drop.namespace, drop.dataOnly = ns, m.DropOp == pb.Mutations_DATA
	default:
		found := false
		for _, edge := range m.Edges {
			if edge != nil && isDeletePredicateEdge(edge) {
				drop.predicate, found = edge.Attr, true
				break
			}
		}
		if !found {
			return drop, false, nil
		}
		if m.DropOp != pb.Mutations_NONE || len(m.Edges) != 1 || drop.predicate == "" {
			return drop, true, &raftSemanticRejection{reason: "predicate drop must be isolated"}
		}
		drop.namespace = x.ParseNamespace(drop.predicate)
	}
	if m.StartTs == 0 || len(m.Schema)+len(m.Types) != 0 || (drop.predicate == "" && len(m.Edges) != 0) {
		return drop, true, &raftSemanticRejection{reason: "drop requires an isolated timestamped proposal"}
	}
	return drop, true, nil
}

func checkRaftAdminProposal(p *pb.Proposal) (bool, error) {
	drop, handled, err := raftAdminDropProposal(p)
	if err != nil {
		return handled, err
	}
	if !handled {
		if p.Mutations == nil {
			return false, nil
		}
		check := func(attr string) error {
			err := State.TreeDBStore.CheckRaftNamespace(x.ParseNamespace(attr))
			if errors.Is(err, badger.ErrBannedKey) {
				return &raftSemanticRejection{reason: err.Error()}
			}
			return err
		}
		if p.Mutations.DropOp == pb.Mutations_TYPE {
			if err := check(p.Mutations.DropValue); err != nil {
				return true, err
			}
		}
		for _, edge := range p.Mutations.Edges {
			if edge != nil {
				if err := check(edge.Attr); err != nil {
					return true, err
				}
			}
		}
		for _, update := range p.Mutations.Schema {
			if update != nil {
				if err := check(update.Predicate); err != nil {
					return true, err
				}
			}
		}
		for _, update := range p.Mutations.Types {
			if update != nil {
				if err := check(update.TypeName); err != nil {
					return true, err
				}
			}
		}
		return false, nil
	}
	if !drop.all {
		if err := State.TreeDBStore.CheckRaftNamespace(drop.namespace); err != nil {
			if errors.Is(err, badger.ErrBannedKey) {
				if drop.ban {
					return true, nil
				}
				return true, &raftSemanticRejection{reason: err.Error()}
			}
			return true, err
		}
	}
	if posting.RaftPendingKeysInUse(drop.matches) {
		return true, &raftSemanticRejection{reason: "drop conflicts with pending transactions; resolve them and submit a new request"}
	}
	return true, nil
}

func (n *node) applyRaftAdminDrop(ctx context.Context, p *pb.Proposal) (bool, error) {
	drop, handled, err := raftAdminDropProposal(p)
	if !handled || err != nil {
		return handled, err
	}
	if _, err := checkRaftAdminProposal(p); err != nil {
		return true, err
	}
	if drop.ban && errors.Is(State.TreeDBStore.CheckRaftNamespace(drop.namespace), badger.ErrBannedKey) {
		return true, nil
	}
	ts := n.raftOrderedAssigned
	if p.StartTs > ts {
		ts = p.StartTs
	}
	if p.Mutations != nil && p.Mutations.StartTs > ts {
		ts = p.Mutations.StartTs
	}
	if ts == 0 {
		ts = 1
	}
	cap, err := State.TreeDBStore.BeginRaftSchemaTransition(ctx)
	if err != nil {
		return true, err
	}
	defer cap.Close()
	store := raftAdminStore{Store: postingStore, reads: cap.Store()}
	if err := posting.TombstoneRaftPostingKeys(ctx, store, ts, drop.matches); err != nil {
		return true, err
	}
	switch {
	case drop.dataOnly:
		// Schema/type definitions are retained for DropData.
	case drop.predicate != "":
		if err := schema.State().Delete(drop.predicate, ts); err != nil {
			return true, err
		}
	case drop.all:
		schema.State().DeleteAll()
	default:
		schema.State().DeletePredsForNs(drop.namespace)
	}
	if drop.ban {
		if err := State.TreeDBStore.BanRaftNamespace(drop.namespace); err != nil {
			return true, err
		}
	}
	if !drop.dataOnly && !drop.ban && drop.predicate == "" {
		ns := drop.namespace
		if drop.all {
			ns = x.RootNamespace
		}
		if n.gid == 1 {
			for _, update := range schema.InitialSchema(ns) {
				if err := writeSchemaForStore(store, update, ts); err != nil {
					return true, err
				}
				schema.State().Set(update.Predicate, update)
			}
		}
		for _, update := range schema.InitialTypes(ns) {
			if err := updateType(update.TypeName, update, ts); err != nil {
				return true, err
			}
		}
	}
	if err := cap.Complete(); err != nil {
		return true, fmt.Errorf("complete logical drop: %w", err)
	}
	return true, nil
}

type raftAdminStore struct {
	posting.Store
	reads posting.Store
}

func (s raftAdminStore) NewReadTxn(ts uint64) posting.ReadTxn { return s.reads.NewReadTxn(ts) }
