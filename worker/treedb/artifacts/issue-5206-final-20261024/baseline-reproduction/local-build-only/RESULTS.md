# Accepted ordinary-COW baseline — build eligibility passed

One execution of the accepted wrapper completed successfully. Every subprocess
exited and was joined. No server, workload, observer manifest, or qualification
capture ran. Canonical source checkouts were not modified.

- Clean local adapted Dgraph commit:
  `7f2720bf0d7ed90e82b04c52911bff92e9f22726`.
- Complete adapted tree: `0ba5d97bc1d9c71f37b04c8e4b067ed6aee540e2`.
- Clean unmodified Gomap: `fc3c3db97ec2954d47a1adf731d501e7b73f181f`,
  tree `c202dbc8cbcfc85b929845433c322cc012168d39`.
- All 18 accepted file hashes and exactly 16 changed paths passed. Source module
  files stayed byte-exact. External `build.mod` and `build.sum` were unchanged by
  readonly module enumeration and all builds; no tidy or resolution adaptation.
- Go 1.26.9, CGO=1, GOMAXPROCS=2, GOWORK=off, GOTOOLCHAIN=local,
  task TMPDIR/shared task caches; GOGC and GOMEMLIMIT unset (ordinary GC).
- Server build: 28.311 seconds; livebench: 0.865; report: 0.164.
  All exited 0 with empty stderr. Binary metadata identifies the local adaptation
  commit and `vcs.modified=false`; exact local Gomap replacement is recorded.

Binary SHA256:

| Binary | SHA256 |
| --- | --- |
| `bin/dgraph` | `9e720b7b14f42a91e3bceaf89b50ae691739d69525073c857aea9f3f544af2f7` |
| `bin/livebench` | `c9c26cee493fd69bd1a6c64c6cebf3e04741d6340c6fff796810075d5712ddef` |
| `bin/report` | `353a404d282e228244036f2eca3f3f5d5eff1fd91553a34120d75191b1689a19` |

External module hashes: build.mod
`811b4349671612fba40b02cb750084bddd4026fc9ee6f1efac13217782a5764c`,
build.sum `5951dd5a2e63c085a555bf772487031de3ca16dda6e5644e99979e5eae182b04`.
The retained July require label is not the actual engine identity; actual engine
bytes are the exact clean fc3c source replacement above.

`commands.json` retains every argv, cwd, wall time, status and stdout/stderr hash;
the associated files retain complete output. `environment.json`, `18-go-env.stdout`,
all three buildinfo files, packet bindings and `RESULTS.json` bind construction.

Status is BUILD_ONLY_NO_RUNTIME_QUALIFICATION. Next steps require the final clean
resourcewitness manifest and separately authorized actual startup/restart COW and
resolved HardSync evidence, ACK/suffix recovery, and the unchanged matrix gates.
