# Accepted ordinary-COW baseline adaptation — source packet

PREPARED_UNBUILT_UNEXECUTED. This is the coordinator-accepted limited adaptation,
not unchanged historical source and not qualification evidence. It executes no
workload and changes neither a numerical gate nor a workload protocol.

- Dgraph base: b87e967bc3c18d4c609e652d79c7ccb8e94ace1b.
- Gomap base: fc3c3db97ec2954d47a1adf731d501e7b73f181f; tree c202dbc8cbcfc85b929845433c322cc012168d39.
- Harness/report/launcher snapshot: a8e0ab8d00b313b23ca234a379ee51510e13c41c.
- Expected complete adapted Dgraph tree: 0ba5d97bc1d9c71f37b04c8e4b067ed6aee540e2.
- baseline.patch SHA256: 4aa5b3d0164f02b512877a5c37094ab51da17c7d812575f1c7466d1d99a2ea46.

## Exact accepted closure

`manifest.json` binds 18 complete files; 16 differ from b87. The only server
changes are the byte-exact historical worker/server_state.go COW selector,
fail-closed Stats witness and log (historical content SHA06060d3...), and the
single resolved Zero WorkerConfig log line from084bfe8 immediately after Parse.
The other 16 complete files are current livebench Go sources/tests, launcher and
its documentation. Two of these are already identical. No posting, Raft, schema,
protobuf, engine or public API implementation is backported. No Gomap source is
patched. Full expected adapted Git tree binds every untouched baseline path too.

`source/` contains complete replacement content. `baseline.patch` was replayed
against original Git blobs with exact context and resulting content checked; Git
parsed all16 changed files via apply --numstat. No checkout or Git index was
modified. The helper was AST syntax-checked, not executed against a source clone.

## Module closure and observer compatibility

Original b87 source go.mod/go.sum are preserved, including the July Gomap require
label. That label is NOT the actual Gomap source identity. Using only a fresh
replace over these inputs is insufficient for readonly construction: fc3c requires
compress88a, while original b87 lists87fb and lacks88a sums. The authentic original
build02 already resolved this exact indirect upgrade. This packet retains its
resolved dgraph.mod/dgraph.sum (hashes2451777d.../5951dd5a...), validated against
historical-build-manifest.json. No new dependency choice is introduced.

prepare_build_mod.py verifies clean exact source roots and the complete adapted
tree, then copies those historical resolved external module inputs and substitutes
ONLY the absolute fc3c replacement directory. It preserves both source module
files. It never applies/commits/builds/runs anything. Output must be fresh/outside
both repositories. Build module/sum hashes are emitted after path substitution.

The current resourcewitness CLI can be supplied prebuilt from the final tooling
source; it need not be added to the baseline. Its v1 local-replacement path binds
the exact clean adapted Dgraph revision/tree, exact clean fc3c revision/tree,
server buildinfo and hashes. Its published-module v2 path is inapplicable to this
historical replacement capsule. Build an isolated adaptation COMMIT first so the
observer can bind actual source bytes rather than falsely describe an invisible
Go overlay as pristine b87. Keep this packet/patch and original capsule hashes
beside the observer manifest. Do not hide vcs.modified or disable VCS metadata.

The new harness imports stdlib + existing dgo/v250/grpc (unchanged direct versions
in b87); it does not import new Gomap engine APIs. The observer's x/mod0.35.0 is
already present in b87. Build compatibility remains untested. Any readonly module
failure, source mismatch or required extra runtime adaptation is a STOP, not
permission to import later consumer/runtime code or modify the accepted scope.

## Later root-owned isolated application/build (NOT executed)

Use clean normal source clones OUTSIDE canonical checkouts. The coordinator owns
creation and commits. With DGRAPH_SOURCE initially exact clean b87, GOMAP_SOURCE
exact clean fc3c, and PACKET=this directory:

```sh
git -C "$DGRAPH_SOURCE" apply --check "$PACKET/baseline.patch"
git -C "$DGRAPH_SOURCE" apply "$PACKET/baseline.patch"
# Review exactly the16 manifest changes; stage only those paths and create a
# local adaptation commit. Its complete tree MUST equal0ba5d97... above.
python3 "$PACKET/prepare_build_mod.py" --dgraph-source "$DGRAPH_SOURCE" \
  --gomap-source "$GOMAP_SOURCE" --output "$BUILD_INPUTS"
```

After that, under the task's ordinary pinned Go1.26.9/CGO environment, retain actual
go version/go env/buildinfo and all commands. No GOGC/GOMEMLIMIT substitution:

```sh
cd "$DGRAPH_SOURCE"
export GOWORK=off
export GOFLAGS="-p=2 -mod=readonly -modfile=$BUILD_INPUTS/build.mod"
go list -m -json github.com/snissn/gomap > "$BUILD_INPUTS/gomap-module.json"
go list -m -json all > "$BUILD_INPUTS/modules.json"
go build -o "$BIN/dgraph" ./dgraph
go build -o "$BIN/livebench" ./worker/treedb/livebench/cmd
go build -o "$BIN/report" ./worker/treedb/livebench/reportcmd
go version -m "$BIN/dgraph" > "$BUILD_INPUTS/server-buildinfo.txt"
"$RESOURCEWITNESS" --mode manifest --server "$BIN/dgraph" \
  --dgraph-source "$DGRAPH_SOURCE" --gomap-source "$GOMAP_SOURCE" \
  --manifest "$BUILD_INPUTS/source-observer.json"
```

Paths must be fresh/created by the later root-owned wrapper; no command above was
executed here. Before execution freeze all binary hashes, source commit/tree and
packet hashes. Any final harness change after a8e needs explicit rebinding before
comparison; this packet does not assert that an unmerged tooling snapshot is the
final landed harness.

The maintained launcher remains unchanged. The BASELINE policy is store_wal:
SURVIVE=filesystem TREEDB_DURABLE_POLICY=store_wal
REQUIRE_TREEDB_MEMTABLE=cow_ordered. Never select raft_checkpoint on b87. Candidate
uses its separately accepted raft_checkpoint policy. Both need actual retained
/debug/store COW evidence at startup/restart and both resolved HardSync witnesses.
Zero legacy point_successor is valid; it is not a basis-slot activation claim.

## Eligibility still missing

No build, smoke, restart, ACK/suffix, performance, quiet-host or final-matrix proof
was executed. Metrics unavailable on historical fc3c remain unavailable, never
zero-filled; final completeness rules still apply. Final ordinary-COW baseline
eligibility requires the actual source/build/module/overlay manifest, public
startup/restart witness, then the unchanged predeclared durability/workload,
numerical, noise, resource and capability gates. The original historical
filesystem diagnostic had profiling enabled; it is not current acceptance.
