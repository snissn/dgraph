# Resource diagnostic witness observer

This small prebuilt CLI calls the maintained livebench `ResolvedConfig`, `ResolvedStore` and
`ExpectedObserved` functions. It rejects ambiguous resolved witnesses and verifies the exact
filesystem/TreeDB/raft_checkpoint/cow_ordered selection. It does not run a server, compile anything,
or participate in recovery.

Build only after the coordinator releases the build grant, from the committed Dgraph checkout:

```sh
go build -o /absolute/artifacts/resource-witness ./worker/treedb/raftproduction/resourcewitness
```

Immediately after building the server, while both normal-clone source repositories remain clean,
capture its source packet outside those repositories. Build with VCS metadata enabled and without an
active Go workspace or source overlays. The server must have `vcs.modified=false` and the exact
Dgraph source HEAD in its Go buildinfo. Its local Gomap module replacement must resolve to the
specified Gomap checkout. The packet records both clean HEAD/tree identities and go.mod hashes, the
actual server SHA-256, complete Go buildinfo, Go version, and replacement path/version.

```sh
/absolute/artifacts/resource-witness --mode manifest \
  --server /absolute/artifacts/dgraph \
  --dgraph-source /absolute/source/dgraph --gomap-source /absolute/source/gomap \
  --manifest /absolute/artifacts/resource-build-source.json
```

Manifest creation refuses overwrite. `--mode verify --server SERVER --manifest MANIFEST` rechecks
the exact binary, source cleanliness and canonical packet. `--mode runtime --config-log LOG`
validates one process; add `--store RESPONSE` to validate Alpha's posting route. Output is JSON;
errors return nonzero. The resource driver invokes these offline modes and retains raw runtime
sidecars plus hashes. It never runs `go build` or `go run`.

Go buildinfo does **not** contain the compiled local replacement tree SHA. This packet binds
observations of clean replacement source at build handoff; it is not proof that an arbitrary
supplied server was compiled from that tree. Preserve the actual build command and build-time
packet, keep source frozen through qualification, and use a reproducible build if stronger
provenance is required. The observer and manifest are evidence, not trusted recovery authority.
