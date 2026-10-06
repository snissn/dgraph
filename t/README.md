# Dgraph Testing Framework

> 📖 For a comprehensive guide to all Dgraph testing approaches (unit tests, build tags, test
> conventions), see [TESTING.md](../TESTING.md) in the repository root.

Dgraph employs a sophisticated testing framework that includes extensive test coverage. Due to the
comprehensive nature of these tests, a complete test run can take several hours, depending on your
hardware. To manage this complex testing process efficiently, we've developed a custom test
framework implemented in Go: [t.go](t.go). This specialized runner provides enhanced control and
flexibility beyond what's available through the standard Go testing framework.

## Requirements

The framework requires several external dependencies. You can check your system for the required
dependencies by running `make check`.

### Go

Version 1.22.12 or higher.

### Docker

The framework uses Docker extensively for integration testing. Tests will often "fail" if Docker is
not given enough resources, specifically memory. If you experience testing that seems to hang
waiting for connections to other cluster members, it's probably a memory issue.

The test runner takes a concurrency flag (./t -j=N) which will attempt to create _N_ number of
Dgraph clusters and run tests concurrently in those clusters. If you are testing on a machine with
limited resources, we advise you to not set this above _1_ (which is the default).

You can preserve the test Docker containers for failure analysis with the `--keep` flag.

### gotestsum

The framework uses [gotestsum](https://github.com/gotestyourself/gotestsum#install) for collating
test output and other advanced functions.

### protoc

On non-Linux systems, protocol buffer tests are skipped. On Linux systems, instructions for
installing and configuring protoc can be found in the
[Protocol Buffers repository](https://github.com/protocolbuffers/protobuf). Or,
`sudo apt update && sudo apt install -y protobuf-compiler`.

## Running Tests

Use the `make install` target in the top-level Makefile to build a binary with your changes. On
non-Linux systems (macOS), this automatically builds both a native binary and a Linux binary for
Docker-based tests.

First, build the `t` program if you haven't already:

```sh
make check && go build .
```

This will produce a `t` executable, which is the testing framework _cli_.

To see a list of available flags:

```sh
./t --help
```

### Testing packages

One popular use of the framework is to target a specific package for integration testing. For
instance, to test the GraphQL system:

```sh
./t --pkg=graphql/e2e/normal
```

Multiple packages can be specified by separated them with a comma.

### Testing suites

You can test one or more "suites" of functionality using the `--suite` flag. For instance:

```sh
./t --suite=core,vector
```

The `--help` flag lists the available suites.

### Testing single test functions

```sh
./t --test=TestParseCountValError
```

This flag uses `ack` to find all tests matching the specified name(s).

### Other useful flags

The `--dry` (dry-run) flag can be used to list the packages that will be included for testing
without actually invoking the tests.

The `--skip-slow` flag will skip tests known to be slow to complete.

## Docker Compose Conventions

The Docker-based integration tests follow a hierarchical file discovery system. When executing a
test, the framework first searches for a docker-compose.yml file in the test's immediate package
directory. If no file is found, it progressively checks parent directories until it locates one. The
root-level test configuration file is stored at
[../dgraph/docker-compose.yml](../dgraph/docker-compose.yml). When implementing tests that require
unique Docker configurations not covered by existing compose files, you should create a new
directory with your tests also containing a custom docker-compose.yml file tailored to your specific
testing requirements.

## Running tests on macOS

The build system automatically handles cross-compilation. When you run `make install` on macOS, it
builds both:

- A native macOS binary at `$GOPATH/bin/dgraph`
- A Linux binary at `$GOPATH/linux_<arch>/dgraph` (used by Docker tests)

No additional setup is required. Just run:

```sh
make install
cd t && make check && go build .
./t --pkg=<package>
```

### Troubleshooting

If tests fail to start, run `make check` to verify all dependencies are installed and the dgraph
binaries are properly built.

## Discovery and public ARM CI

`./t --list-packages --suite=integration` lists the existing selected package universe and returns
before plugin generation, binary rebuild, workers, cluster startup or cleanup. Package/suite/tag
filters remain the existing runner filters. Mutating runner options cannot accompany discovery.
`--dry` retains its existing behavior, including cluster setup.

Public personal-repository ARM CI discovers that same integration universe and selects two isolated
jobs by exact full package IDs: `/vector` packages and the complete remainder. Other repositories
retain the full `./t` invocation and existing native runner selector. Cluster concurrency remains
one, package timeout 30 minutes, failfast remains enabled, and each native job retains 60 minutes.
The stable `dgraph-tests` aggregate requires every expected job and source-bound query/result
artifact, a disjoint complete union, and at most 60 minutes from the earliest native job start to
the latest native job completion. Queue delays and duplicated setup therefore cannot silently extend
the original whole-gate deadline. This scheduling change does not guarantee either shard finishes.

Public-fallback integration2/load test steps keep their original commands and 90/30-minute job
limits. A shared best-effort observer streams bounded 30-second resource records (at most 180/60
ticks, 8 KiB per record), including available memory/swap, OOM counters, pressure, cgroup memory,
filesystem bytes/inodes, process PID/name/RSS and bounded Docker status/memory. Shutdown readings
include accessible kernel OOM victims and service state; inaccessible data is explicitly unknown.
The observer collects no process arguments or environment, installs no monitoring stack and changes
no resource limits. Its owned process is stopped within bounded cleanup; test failure/signal status
is preserved. Abrupt host loss may leave observations missing. These are causal observations, not an
OOM diagnosis or a remedy for the retained runner shutdowns. Hosted full CI remains required; all
four M3 performance HOLDs and the absence of performance qualification remain unchanged.

The same public-personal guard sets `DGRAPH_CI_PUBLIC_NATIVE=1` for load CI. The existing
`ComposeFileArgs` hook adds one overlay only for `systest/21million/live/docker-compose.yml`; its
Alpha receives `DGRAPH_ALPHA_CACHE=size-mb=1024;percentage=40,40,20;remove-on-update=false`. Other
fixtures and nonpublic runs retain their original configuration. Public integration2 adds
`go test -p=1` to serialize packages, retaining every package, assertion and 90-minute timeout. This
reduces configured cache allowance and package overlap; it does not cap RSS or a package's internal
cluster concurrency, and neither capacity nor deadline success is proven. Full native CI remains
required. A repeated resource shutdown or unchanged deadline failure requires a capacity decision
rather than another blind tuning head.

The committed root `go.sum` is the reproducible checksum-only output of the build's existing
`go mod tidy`, with unchanged `go.mod`, selected module versions and replacements. All three native
workflows require committed module inputs to remain unchanged after build and before test admission.
ARM keeps its exact source identity and coverage comparison against the aggregate checkout.
