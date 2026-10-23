# Resource diagnostic witness observer

This small prebuilt CLI calls the maintained livebench `ResolvedConfig`, `ResolvedStore` and
`ExpectedObserved` functions. It rejects ambiguous resolved witnesses and verifies the exact
filesystem/TreeDB/raft_checkpoint/cow_ordered selection. It does not run a server, compile anything,
or participate in recovery.

Build only after the coordinator releases the build grant, from the committed Dgraph checkout:

```sh
go build -o /absolute/artifacts/resource-witness ./worker/treedb/raftproduction/resourcewitness
```

Immediately after building the server, while both source repositories remain clean, capture its
source packet outside those repositories. Build with VCS metadata enabled and without an active Go
workspace or source overlays. The server must have `vcs.modified=false` and the exact Dgraph source
HEAD in its Go buildinfo. The packet records both clean HEAD/tree identities and go.mod hashes, the
actual server SHA-256, complete Go buildinfo and Go version. There are two strict Gomap source
modes:

- A local replacement must resolve to the specified clean Gomap checkout. This retains the original
  version-1 manifest format and replacement path/version checks.
- A published module must have no replacement in the binary or the Dgraph source module. Its exact
  buildinfo version and `h1` checksum must match the direct Dgraph `go.mod` pin and `go.sum`. The
  observer selects the specified Gomap checkout's files with the official `x/mod/zip.CheckDir`, then
  computes `dirhash.Hash1` and requires equality with that checksum. Version-2 manifests record the
  module content checksum. Nested modules, including evidence archives, are excluded by the same
  maintained selection used for module publication; the Git tree identity still records the whole
  checkout.

For published modules, provide a pristine, source-only normal clone of Gomap (with a `.git`
directory). A linked worktree's regular `.git` file is not omitted like the normal VCS directory and
can invalidate the checksum. Git cleanliness alone also does not exclude ignored build outputs:
module-file selection can include them and cause a checksum refusal. Do not put binaries, profiles
or generated artifacts in this checkout, or silently omit arbitrary files to obtain a match. The
observer hashes source files during offline manifest creation and verification; it neither downloads
modules nor invokes Go commands. Keep these potentially large reads outside measured workload
intervals.

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

Go buildinfo does **not** contain the compiled local replacement tree SHA. A version-1 packet binds
observations of clean replacement source at build handoff. Version 2 additionally checks published
module content against the compiler-recorded checksum, rather than assuming a pseudo-version label
identifies the supplied checkout. Neither mode independently rebuilds the server or proves an
arbitrary supplied binary's embedded buildinfo is truthful. Preserve the actual build command and
build-time packet, keep source frozen through qualification, and use a reproducible build if
stronger provenance is required. The observer and manifest are evidence, not trusted recovery
authority.
