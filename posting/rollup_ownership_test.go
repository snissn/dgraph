package posting

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/dgraph-io/dgraph/v25/codec"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func ownershipBorrowedList(t *testing.T) (*List, *pb.PostingList, uint64) {
	t.Helper()
	pack := codec.Encode([]uint64{1, 2, 3, 4}, blockSize)
	ref := pack.AllocRef
	t.Cleanup(func() {
		if a := z.AllocatorFrom(ref); a != nil {
			a.Release()
		}
	})
	plist := &pb.PostingList{Pack: pack, CommitTs: 10, Splits: make([]uint64, 0, 2)}
	plist.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x07})
	pack.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x08})
	return NewList(x.DataKey(x.AttrInRootNamespace("ownership"), 1), plist, 10), plist, ref
}

func TestRollupOwnershipBorrowedImmutableList(t *testing.T) {
	l, input, ref := ownershipBorrowedList(t)
	before := proto.Clone(input)
	l.RLock()
	out, err := l.rollup(math.MaxUint64, true)
	l.RUnlock()
	require.NoError(t, err)
	require.NotSame(t, input, out.plist, "rollup owns its header even with no deltas")
	require.Same(t, input.Pack, out.plist.Pack, "immutable UID payload remains borrowed")
	out.free()
	require.NotNil(t, z.AllocatorFrom(ref), "rollup must not release its borrowed input")
	for i := 0; i < 2; i++ {
		kvs, err := l.Rollup(nil, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, kvs, 1)
		require.NotNil(t, z.AllocatorFrom(ref))
		require.True(t, proto.Equal(before, input))
		require.Equal(t, 2, cap(input.Splits))
		wire := &pb.PostingList{}
		require.NoError(t, proto.Unmarshal(kvs[0].Value, wire))
		require.Zero(t, wire.Pack.AllocRef)
		require.Equal(t, []uint64{1, 2, 3, 4}, codec.Decode(wire.Pack, 0))
		require.Equal(t, input.ProtoReflect().GetUnknown(), wire.ProtoReflect().GetUnknown())
		require.Equal(t, input.Pack.ProtoReflect().GetUnknown(), wire.Pack.ProtoReflect().GetUnknown())
	}
}

func TestMarshalPostingListOwnershipConcurrent(t *testing.T) {
	_, input, ref := ownershipBorrowedList(t)
	expected := MarshalPostingList(input, nil).Value
	start := make(chan struct{})
	errs := make(chan error, 5)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 64; i++ {
				got := MarshalPostingList(input, nil).Value
				if !bytes.Equal(expected, got) {
					errs <- fmt.Errorf("concurrent marshal changed wire bytes")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 256; i++ {
			if input.Pack.AllocRef != ref {
				errs <- fmt.Errorf("marshal mutated borrowed allocator reference")
				return
			}
			_ = approxPostingListSize(input)
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, ref, input.Pack.AllocRef)
	require.NotNil(t, z.AllocatorFrom(ref))
}

func TestRollupOwnershipConcurrentCachedCopies(t *testing.T) {
	l, input, ref := ownershipBorrowedList(t)
	l.RLock()
	cachedCopy := copyList(l)
	l.RUnlock()
	expected, err := l.Rollup(nil, math.MaxUint64)
	require.NoError(t, err)
	start := make(chan struct{})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	for _, list := range []*List{l, cachedCopy} {
		wg.Add(1)
		go func(list *List) {
			defer wg.Done()
			<-start
			for i := 0; i < 32; i++ {
				got, err := list.Rollup(nil, math.MaxUint64)
				if err != nil || len(got) != 1 || !bytes.Equal(got[0].Value, expected[0].Value) {
					errs <- fmt.Errorf("cached rollup changed: %v", err)
					return
				}
			}
		}(list)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 128; i++ {
			if input.Pack.AllocRef != ref {
				errs <- fmt.Errorf("borrowed reference changed")
				return
			}
			_ = l.ApproximateSize()
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NotNil(t, z.AllocatorFrom(ref))
	require.Equal(t, 2, cap(input.Splits))
}

func TestRollupOwnershipRecursiveSplits(t *testing.T) {
	oldMax := maxListSize
	maxListSize = 64
	defer func() { maxListSize = oldMax }()
	l, input, borrowed := ownershipBorrowedList(t)
	posts := make([]*pb.Posting, 0, 2044)
	for uid := uint64(5); uid <= 2048; uid++ {
		posts = append(posts, &pb.Posting{Uid: uid, Op: Set})
	}
	l.mutationMap.insertCommittedPostings(&pb.PostingList{CommitTs: 11, Postings: posts})
	l.RLock()
	out, err := l.rollup(math.MaxUint64, true)
	l.RUnlock()
	require.NoError(t, err)
	defer out.free()
	require.Len(t, out.ownedAllocs, 1, "one encoder owns one allocator despite recursive split views")
	owned := out.ownedAllocs[0].Ref
	require.NotEqual(t, borrowed, owned)
	require.Greater(t, len(out.parts), 2)
	var uids []uint64
	for _, start := range out.splits() {
		part := out.parts[start]
		require.Equal(t, owned, part.Pack.AllocRef)
		uids = append(uids, codec.Decode(part.Pack, 0)...)
		wire := &pb.PostingList{}
		require.NoError(t, proto.Unmarshal(MarshalPostingList(part, nil).Value, wire))
		require.Zero(t, wire.Pack.AllocRef)
	}
	require.Len(t, uids, 2048)
	for i, uid := range uids {
		require.Equal(t, uint64(i+1), uid)
	}
	out.free()
	require.Nil(t, z.AllocatorFrom(owned))
	require.NotNil(t, z.AllocatorFrom(borrowed))
	require.Equal(t, []uint64{1, 2, 3, 4}, codec.Decode(input.Pack, 0))
	// Serialization fails only after a fresh encoded output has been split.
	// Its owned encoder must still be released by Rollup's defer.
	before := ownershipEncoderCount(t)
	l.key = []byte{0xff}
	_, err = l.Rollup(nil, math.MaxUint64)
	require.ErrorContains(t, err, "only keys with default prefix")
	require.Equal(t, before, ownershipEncoderCount(t))
	require.NotNil(t, z.AllocatorFrom(borrowed))
}

func TestRollupOwnershipRemovedEmptyHeader(t *testing.T) {
	out := &rollupOutput{plist: &pb.PostingList{}, parts: make(map[uint64]*pb.PostingList)}
	enc := codec.Encoder{BlockSize: blockSize}
	enc.Add(17)
	pack := out.finishEncoder(&enc)
	ref := pack.AllocRef
	require.Len(t, out.ownedAllocs, 1)
	// Removing the last header must not remove its allocation from the owner.
	pack.Blocks = nil
	out.parts[1] = &pb.PostingList{Pack: pack}
	out.removeEmptySplits()
	require.Empty(t, out.parts)
	require.NotNil(t, z.AllocatorFrom(ref))
	out.free()
	require.Nil(t, z.AllocatorFrom(ref))
	out.free()
}

func ownershipEncoderCount(t *testing.T) int {
	t.Helper()
	fields := strings.Fields(z.Allocators())
	for i := 0; i+3 < len(fields); i++ {
		if fields[i] == "Tag:" && fields[i+1] == "enc" && fields[i+2] == "Num:" {
			n, err := strconv.Atoi(fields[i+3])
			require.NoError(t, err)
			return n
		}
	}
	return 0
}

func TestRollupOwnershipMultipartEncoderLifetime(t *testing.T) {
	for _, existing := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("existing-parts-%d", existing), func(t *testing.T) {
			store := NewBadgerStore(openPostingStoreTestDB(t))
			key := x.DataKey(x.AttrInRootNamespace("ownership-partial"), uint64(existing))
			splits := []uint64{1, 3, 5}
			for _, start := range splits[:existing] {
				partKey, err := x.SplitKey(key, start)
				require.NoError(t, err)
				pack := codec.Encode([]uint64{start, start + 1}, blockSize)
				wire := MarshalPostingList(&pb.PostingList{Pack: pack}, nil).Value
				commitStoreEntry(t, store, 10, Entry{Key: partKey, Value: wire, UserMeta: BitCompletePosting})
				codec.FreePack(pack)
			}
			l := NewList(key, &pb.PostingList{Splits: splits}, 10)
			l.readStore = store
			before := ownershipEncoderCount(t)
			if existing == len(splits) {
				l.RLock()
				out, err := l.rollup(math.MaxUint64, true)
				l.RUnlock()
				require.NoError(t, err)
				defer out.free()
				require.Len(t, out.ownedAllocs, len(splits))
				refs := make(map[uint64]bool)
				for _, alloc := range out.ownedAllocs {
					require.False(t, refs[alloc.Ref], "each completed encoder transfers a distinct allocation")
					refs[alloc.Ref] = true
					require.Same(t, alloc, z.AllocatorFrom(alloc.Ref))
				}
				out.free()
				for ref := range refs {
					require.Nil(t, z.AllocatorFrom(ref))
				}
				require.Equal(t, before, ownershipEncoderCount(t))
				return
			}
			_, err := l.Rollup(nil, math.MaxUint64)
			require.ErrorContains(t, err, "could not read list part")
			require.Equal(t, before, ownershipEncoderCount(t), "release both completed and currently unfinished encoders")
		})
	}
}
