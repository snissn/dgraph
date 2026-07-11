<!--
SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
SPDX-License-Identifier: Apache-2.0
-->

# Posting store boundary

The benchmark-minimal posting path uses `Store` for timestamp-bound reads, prefix/reverse iteration,
exact-key all-version reconstruction, point reads, and atomic externally timestamped writes and
deletes. A read transaction supplies a common visibility timestamp while each iterator owns one
physical snapshot; `Rewind` refreshes that snapshot and ordinary `Seek` positions within it. This is
conformant for Dgraph's watermarked commit histories, where a commit below an executing read
timestamp cannot publish late. Point reads and newly created iterators at the same timestamp can
observe newly published state, matching managed Badger. `IteratorOptions.Prefix` plus `Seek` bounds
range iteration. Exact-key reconstruction has a separate `NewKeyIterator` operation so Badger
retains its exact-key bloom-filter optimization. `BadgerStore` is the production implementation and
preserves the existing zero-copy value callback used while decoding posting lists. Iterators match
Badger's single-goroutine contract: callers must not concurrently invoke an iterator or discard its
owning transaction while it is in use.

Schema has a matching, smaller local contract for live point reads and atomic deletes. It cannot
import `posting.Store` because posting imports schema. A future backend adapter therefore implements
both contracts while sharing its underlying database.

## Intentionally Badger-specific operational paths

These paths are outside the Alpha benchmark-minimal graph and retain concrete Badger APIs:

- posting index rebuild streams, temporary managed write batches, predicate/index drops, and
  namespace bans in `posting/index.go`;
- schema startup bulk loading via Badger Stream in `schema/schema.go`;
- worker backup, export, online restore, subscriptions, and cache resizing;
- bulk loader, debug tooling, and direct Badger compatibility helpers.

Those consumers need stream/protobuf translation, destructive maintenance, subscription, or cache
capabilities that are not part of the posting hot path. `Store.Close`, value-log GC, and database
stats are also excluded: lifecycle and maintenance are owned by Alpha's backend manager rather than
called from core posting/schema reads or mutations. The restricted TreeDB runtime must
capability-gate all of these paths instead of emulating or silently skipping them.
