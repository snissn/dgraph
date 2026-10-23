/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/schema"
	"github.com/dgraph-io/dgraph/v25/types"
	"github.com/dgraph-io/dgraph/v25/x"
	"google.golang.org/protobuf/proto"
)

// ValidateRaftUniqueTxn checks the final private canonical transaction against
// the current ordered committed image. Frontend checks precede Raft admission:
// a schema may become unique before an already admitted prewrite arrives.
// Other pending transactions arbitrate through the maintained Zero conflict
// keys; their process-local presence never chooses a semantic rejection here.
// The caller holds ordered apply exclusion and has sealed txn with Update.
func ValidateRaftUniqueTxn(ctx context.Context, txn *Txn, readTs uint64) error {
	return validateRaftUniqueTxn(ctx, postingStore, txn, readTs)
}

func validateRaftUniqueTxn(ctx context.Context, store Store, txn *Txn, readTs uint64) error {
	var keys []string
	for key := range txn.cache.deltas {
		parsed, err := x.Parse([]byte(key))
		if err != nil {
			return err
		}
		if parsed.IsData() && !parsed.HasStartUid {
			if su, ok := schema.State().Get(ctx, parsed.Attr); ok && su.Unique {
				keys = append(keys, key)
			}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	ctx = schema.GetWriteContext(ctx)
	sort.Strings(keys)
	type valueIdentity struct{ attr, lang, value string }
	owners := make(map[valueIdentity]uint64)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		parsed, err := x.Parse([]byte(key))
		if err != nil {
			return err
		}
		if !parsed.IsData() || parsed.HasStartUid {
			continue
		}
		su, ok := schema.State().Get(ctx, parsed.Attr)
		if !ok || !su.Unique {
			continue
		}
		var delta pb.PostingList
		if err := proto.Unmarshal(txn.cache.deltas[key], &delta); err != nil {
			return err
		}
		for _, p := range delta.Postings {
			if p.Op == Del {
				continue
			}
			_, encoded, err := uniqueCanonicalValue(p, su.ValueType)
			if err != nil {
				return err
			}
			identity := valueIdentity{parsed.Attr, string(p.LangTag), string(encoded)}
			if owner, exists := owners[identity]; exists && owner != parsed.Uid {
				return fmt.Errorf("%w: unique value belongs to different subjects", ErrSchemaDataConflict)
			}
			owners[identity] = parsed.Uid
			edge := &pb.DirectedEdge{Attr: parsed.Attr, Lang: string(p.LangTag)}
			tokenizers := schema.State().Tokenizer(ctx, parsed.Attr)
			found := false
			for i, tokenizer := range tokenizers {
				name := tokenizer.Name()
				if (su.ValueType == pb.Posting_STRING && (name == "exact" || name == "hash")) || (su.ValueType == pb.Posting_INT && name == "int") {
					tokenizers, found = tokenizers[i:i+1], true
					break
				}
			}
			if !found {
				return fmt.Errorf("unique predicate lacks an equality tokenizer")
			}
			tokens, err := indexTokens(ctx, &indexMutationInfo{edge: edge, val: types.Val{Tid: types.TypeID(su.ValueType), Value: encoded}, tokenizers: tokenizers})
			if err != nil {
				return err
			}
			for _, token := range tokens {
				// Keep every committed candidate. A transaction's index deletes
				// cannot hide another value sharing the same hash/token; project
				// its final data below instead. New owners are checked by owners.
				index, err := readUniqueList(store, nil, x.IndexKey(parsed.Attr, token), readTs)
				if err != nil {
					return err
				}
				err = index.Iterate(readTs, 0, func(candidate *pb.Posting) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if candidate.Uid == parsed.Uid {
						return nil
					}
					list, err := readUniqueList(store, txn, x.DataKey(parsed.Attr, candidate.Uid), readTs)
					if err != nil {
						return err
					}
					return list.Iterate(readTs, 0, func(other *pb.Posting) error {
						if !bytes.Equal(other.LangTag, p.LangTag) {
							return nil
						}
						_, otherValue, err := uniqueCanonicalValue(other, su.ValueType)
						if err != nil {
							return err
						}
						if bytes.Equal(encoded, otherValue) {
							return fmt.Errorf("%w: unique value already belongs to another subject", ErrSchemaDataConflict)
						}
						return nil
					})
				})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func uniqueCanonicalValue(p *pb.Posting, target pb.Posting_ValType) (types.Val, []byte, error) {
	value, err := types.Convert(types.Val{Tid: types.TypeID(p.ValType), Value: p.Value}, types.TypeID(target))
	if err != nil {
		return types.Val{}, nil, err
	}
	encoded := types.Val{Tid: types.BinaryID}
	if err := types.Marshal(value, &encoded); err != nil {
		return types.Val{}, nil, err
	}
	return value, encoded.Value.([]byte), nil
}

// Project this transaction's final canonical delta over the ordered committed
// image. This preserves same-owner writes and atomic swaps/removal+reassignment,
// including earlier prewrites of the same transaction. The index is only a
// candidate source: actual values and language scope decide equality.
func readUniqueList(store Store, txn *Txn, key []byte, readTs uint64) (*List, error) {
	read := store.NewReadTxn(readTs)
	defer read.Discard()
	it := read.NewKeyIterator(key, IteratorOptions{AllVersions: true})
	defer it.Close()
	it.Seek(key)
	list, err := readPostingListForStore(key, it, store)
	if err != nil {
		return nil, err
	}
	var data []byte
	if txn != nil {
		data = txn.cache.deltas[string(key)]
	}
	if len(data) != 0 {
		var delta pb.PostingList
		if err := proto.Unmarshal(data, &delta); err != nil {
			return nil, err
		}
		for _, p := range delta.Postings {
			p.StartTs, p.CommitTs = readTs, 0
		}
		list.mutationMap.setCurrentEntries(readTs, &delta)
	}
	return list, nil
}
