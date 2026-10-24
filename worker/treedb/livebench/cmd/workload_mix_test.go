// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

func TestWriteHeavyMixChangesRPCsAndRecordedFingerprint(t *testing.T) {
	var writes, points, hops atomic.Int64
	dg := operationClient(t, func(_ context.Context, req *api.Request) (*api.Response, error) {
		resp := &api.Response{Txn: &api.TxnContext{StartTs: 1}}
		if len(req.Mutations) != 0 {
			resp.Uids = map[string]string{"w": fmt.Sprintf("0x%x", writes.Add(1)+10)}
		} else if strings.Contains(req.Query, "bench.next") {
			hops.Add(1)
			resp.Json = []byte(`{"q":[{"uid":"0x1","bench.value":"source","bench.next":[{"uid":"0x2","bench.value":"target"}]}]}`)
		} else {
			points.Add(1)
			resp.Json = []byte(`{"q":[{"uid":"0x1","bench.value":"source"}]}`)
		}
		return resp, nil
	})
	rows := make([]operationRow, 100)
	values, expected, err := exerciseWithMix(context.Background(), dg,
		map[string]expectedNode{"0x1": {Value: "source", NextValue: "target"}}, 100, 4, 0, 1000, "write-heavy", rows)
	if err != nil || len(values) != 100 || len(expected) != 80 || writes.Load() != 80 || points.Load() != 10 || hops.Load() != 10 {
		t.Fatalf("RPC mix writes=%d points=%d hops=%d samples=%d expected=%d err=%v", writes.Load(), points.Load(), hops.Load(), len(values), len(expected), err)
	}
	kinds := make(map[string]int)
	for _, row := range rows {
		kinds[row.Kind]++
	}
	if kinds["write"] != 80 || kinds["point_read"] != 10 || kinds["one_hop_read"] != 10 {
		t.Fatalf("diagnostic mix: %v", kinds)
	}
	mixed := runtimeConfig(options{mix: "mixed"})
	heavy := runtimeConfig(options{mix: "write-heavy"})
	if heavy.QueryMix["write"] != 80 || heavy.QueryMix["point_read"] != 10 || heavy.QueryMix["one_hop_read"] != 10 || mixed.Fingerprint() == heavy.Fingerprint() {
		t.Fatalf("workloads must record distinct actual mixes: %+v %+v", mixed, heavy)
	}
	if err := normalizeRuntimeOptions(&options{mix: "typo"}); err == nil {
		t.Fatal("unknown workload mix must fail before launching servers")
	}
}
