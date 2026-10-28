# Local three-live-Alpha recovery qualification

This opt-in Linux integration test starts a real Zero and three Alpha processes,
all serving Raft group 1 with separate posting stores, Oracles and caches. It is
correctness/recovery qualification for the default-disabled ordered-Delta path,
not a benchmark or a production/default-readiness decision.

The production dependency pin is unchanged. Runtime qualification currently
requires the exact provisional GoMap #5301 module
`v0.6.2-0.20261028140238-b82615a042cd`. Build using an **external** modfile and
independently verify its replacement source against that published module archive.
The binary build-info guard checks the version/replacement name and records the
binary hash; the guard alone does not authenticate a local directory's contents.
Retain the external source/module/binary hashes and commands beside each run.

For example, with Go 1.26.4 and a previously verified `/absolute/prototype.mod`:

```sh
GOWORK=off go test -c -race -modfile=/absolute/prototype.mod \
  -o /mnt/fast4tb/my-qualification/dgraph-cluster.test ./dgraph
DGRAPH_ORDERED_CLUSTER_TEST_SERVER=1 \
DGRAPH_ORDERED_CLUSTER_BINARY=/mnt/fast4tb/my-qualification/dgraph-cluster.test \
DGRAPH_ORDERED_CLUSTER_DIR=/mnt/fast4tb/my-qualification/new-run \
GOWORK=off go test -race -modfile=/absolute/prototype.mod \
  ./worker/treedb/clusterqualification \
  -run '^TestOrderedDeltaThreeLiveAlphaRecovery$' -count=1 -timeout=7m -v
```

The artifact directory must be new; even a failed directory is never reused.
`DGRAPH_ORDERED_CLUSTER_PORT_BASE` optionally changes the Zero gRPC base port
(default 25000). The test preflights eleven loopback ports through base+10000.
Each child uses `GOMAXPROCS=2`, a 32 MiB Dgraph cache, ordinary durable TreeDB
`wal_on_sync`, post-commit events and `--survive=process`. It does not inject a
Raft WAL Sync, select filesystem survival, or enable Raft-checkpoint recovery.
High snapshot thresholds keep this bounded fixture on log replay/catchup; it
makes no snapshot installation/compacted-log claim.

The helper in `dgraph/ordered_cluster_server_test.go` exists only in the test
executable and invokes the real CLI. It installs the maintained test seam on the
exact posting main-DB command-WAL root (the parent checks its exported WAL-dir
stat). A single-use arm holds immediately before a real nonzero-LSN file sync,
with a 30-second error deadline. Release resumes the actual syscall; a separate
successful `AfterDependencyFileSync` marker witnesses its completion. The parent
releases every gate before cleanup, including assertion failures. Neither the
gate nor its configuration is compiled into the product binary.

Three disjoint transactions are prewritten through normal public RPCs. Original
A is held on all three live posting replicas. The test commits later decisions
and examines each replica's actual observer records. When the stream permits
speculation it requires distinct original identities, successful materialization
and accepted submission endpoints, with no callback/Oracle/proposal/Applied
publication before release. When the real stream puts an empty-transaction Delta
between decisions, the test requires that exact encoded entry descriptor and
preserves the watermark barrier; it never removes it or retries toward an
eligible sequence. The recorded number prepared before the crash may be 1, 2
or 3. Such a barrier branch is recovery/barrier evidence and does not prove
multi-original activation. The earlier positive held A/B/C evidence remains
separate from any later barrier branch.

The current Alpha leader is then SIGKILLed at the held pre-sync cut. The two
survivors execute their actual syncs and serve the decided rows; the killed
leader restarts from its original directories and serves the same data. Remaining
prewrites commit after recovery. Additional controls exercise concurrent public
commits, conflicting prewrites (one abort), an uncommitted public prewrite whose
unchanged transaction context commits through a survivor after another leader
crash, continued two-replica writes, catchup, all-Alpha process crashes after
acknowledged/read data, and fresh writes after reopening all three.

`CommitOrAbort`'s public result acknowledges Zero's decision. It can return while
Alpha native callbacks are held. The test explicitly distinguishes that result
from each Alpha's durable callback, original proposal acknowledgement and read
visibility. It does not strengthen the existing public acknowledgement contract.
Queries through each Alpha execute posting tasks locally because all three
serve the sole group; fresh normal reads exercise each replica's WaitForTs and
cache/store path.

A sole waiter records every process's actual PID, argv, requested signal and
terminal status. Only explicitly planned SIGKILLs are accepted as killed exits;
graceful shutdown requires exit 0. Terminal log checks reject panic/fatal/race
and known storage failure markers. Graceful Alpha logs must have complete lines
and no materialized original pending publication. Killed generations retain
pending/tail unknowns. The observer auditor checks contiguous session sequences
and original finalizer order, with synthetic negative controls. Its generic
"overlap" means materialization began before another original's Applied endpoint;
it is not a native-cohort or physical-sync membership proof.

Without `DGRAPH_ORDERED_CLUSTER_TEST_SERVER=1`, the same fixture can run a regular
Dgraph binary without test gates. It records whatever natural preparation is
observed and does not demand a scheduling-dependent activation count. Both modes
use the already merged Delta observer and are excluded from performance claims.

Remaining gates include merged dependency authority, public uninstrumented
performance, broader network partitions/leadership churn, snapshot/compaction,
and other crash cuts. SIGKILL with the host/filesystem alive is process recovery,
not power-loss certification. This finite three-replica test does not close all
recovery or distributed-system qualification gates.
