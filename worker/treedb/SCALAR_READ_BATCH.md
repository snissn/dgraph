# Bounded scalar posting reads

The optional `posting.PostingBatchReader` capability serves the existing scalar
shortcut in `handleValuePostings`. Each existing calculate goroutine fetches at
most 32 keys, then consumes the results in original UID order. A final singleton
uses `GetSinglePosting`. List, language, count, expand-all and facet requests keep
their existing all-version reconstruction. Badger does not expose the capability,
including when decorated with commit events. Detailed per-key read latency
metrics explicitly select the scalar route, retaining their existing meaning.

`LocalCache.GetSinglePostings` checks deltas and in-memory posting lists before
forming the storage-miss batch. Delta precedence, `StaticValue` timestamps,
STAR_ALL and individual delete filtering share the scalar implementation. It
releases the cache lock before physical work. Prefetch does not populate the
shared cache or attach a read cut to it. The latest-data population followed by
historical fallback in `MemoryLayer.ReadData` remains unchanged.

`TreeDBStore.ReadPostingBatch` calls public `mvcc.Store.GetManyAt`. The GoMap
operation owns cut capture, discard-floor admission, successor work and cleanup;
the adapter owns the existing read-transaction root-install fence until that
synchronous call and decoding finish. Results are owned payloads. Envelope and
per-key read errors retain their input positions. A total producer error,
including a cut-close error returned alongside values, invalidates storage
results. Neither kind of error triggers a scalar retry. Unexpected result counts
fail closed. The commit-event decorator preserves the optional capability only
when the underlying store implements it.

The worker checks cancellation between chunks and before consuming returned
results. Cancellation does not detach active physical work: the same calculate
goroutine finishes the synchronous call and the existing errgroup joins all
calculate goroutines. Existing task admission, Raft read pins, checksum checks
and root-install ownership remain with their original owners. No additional
goroutine, pool, mmap, publication authority or global cache is introduced.

Prefetched errors are data until their input position is consumed. An earlier
protobuf or value-conversion error therefore wins over a later prefetched read
error, just as in the scalar loop. A batch total error is reported at its first
storage-miss position; preceding local results and their conversion still run
first. No successful result may hide that total error.

## Allocation and lifetime bounds

The worker retains one batch per existing calculate goroutine. The adapter's
metadata arrays are bounded by 32: worker keys, LocalCache miss keys and indices,
indexed posting results, producer MVCC results, and adapter payload results.
There are no duplicate key encodings when consuming a prefetched result. The
adapter passes owned GoMap values directly into envelope decoding, without an
additional payload copy; protobuf unmarshalling has the same ownership role as
the scalar route. Duplicate keys retain independent produced values.

The key bound is not a byte cap. Up to 32 admitted values can be live together,
plus their decoded postings and the normal output accumulated by the query.
GoMap's existing per-value/resource admission still applies. This bounded
prefetch can increase instantaneous live memory compared with one scalar key;
representative public tail and memory qualification remains required.

## Checks

`TestScalarBatchRealCallerActivation` distinguishes the old loop (zero batch
calls) from 32+32+singleton dispatch. `TestScalarBatchRealTreeDBCallerParity`
uses the public COW implementation through the production event decorator and
compares an older-timestamp response with the scalar route. Additional focused
controls cover local precedence, missing values, both delete shapes, malformed
protobuf/envelopes, ordered conversion/read errors, total errors returned with
values, no retries, bounds, independent owned values after Close, floors,
Discard, cancellation and all scalar/history exclusions.

Run the owning checks against the selected public GoMap dependency. During
private construction only, an alternate modfile may point to the paired GoMap
worktree; such tests do not qualify a public dependency pin or performance.

```sh
go test ./posting ./worker -count=1
go test -race ./posting ./worker \
  -run '^(TestPostingBatch|TestTreeDBPostingBatch|TestScalarBatch|TestRaftTreeDBTransferDrainsReadersAndRejectsStaleWriters|TestRaftTaskRejectsExpiredTimestampBeforeOracleAndCaches|TestRaftFutureOracleWaitDoesNotBlockMaintenanceAdmission)' \
  -count=1
```

This source and its focused checks are an optimization candidate. Public recovery,
throughput, latency, space, and current-head integration/review gates remain with
the parent qualification protocol.
