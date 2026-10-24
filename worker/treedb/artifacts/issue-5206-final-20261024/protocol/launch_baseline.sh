set -euo pipefail
LANE=/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01
SOURCE=$LANE/source/prospective-dgraph
OUT=$LANE/runtime-merged-01
SERVER=$OUT/smoke/matrix/bin/dgraph
export PYTHONDONTWRITEBYTECODE=1
export TMPDIR=$LANE/tmp GOMODCACHE=$LANE/cache/mod GOCACHE=$LANE/cache/build
export PATH=$LANE/tools/go1.26.9/bin:$PATH GOTOOLCHAIN=local GOENV=off GOWORK=off
export GOPROXY=https://proxy.golang.org,direct GOSUMDB=sum.golang.org
export GOPRIVATE= GONOPROXY= GONOSUMDB= GOINSECURE= CGO_ENABLED=1 GOMAXPROCS=2 GOFLAGS=-p=2
unset GOROOT GOTOOLDIR GOGC GOMEMLIMIT
cd "$SOURCE"
test "$(go version)" = 'go version go1.26.9 linux/amd64'
python3 "$OUT/run_matrix_diagnostic_arm.py" baseline-filesystem-12
