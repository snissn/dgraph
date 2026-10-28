//go:build linux

// SPDX-License-Identifier: Apache-2.0
package clusterqualification

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestOrderedDeltaThreeLiveAlphaRecovery is deliberately opt-in: it runs four
// real server processes and retains their databases, RPC receipts and exits.
// It never changes --survive=process or the ordinary durable TreeDB route.
func TestOrderedDeltaThreeLiveAlphaRecovery(t *testing.T) {
	binary := os.Getenv("DGRAPH_ORDERED_CLUSTER_BINARY")
	if binary == "" {
		t.Skip("set DGRAPH_ORDERED_CLUSTER_BINARY and a new DGRAPH_ORDERED_CLUSTER_DIR; see README.md")
	}
	root := os.Getenv("DGRAPH_ORDERED_CLUSTER_DIR")
	require.True(t, filepath.IsAbs(root), "artifact directory must be absolute")
	require.NoError(t, os.Mkdir(root, 0700), "refuse existing artifacts, including failed runs")
	receipts, err := os.OpenFile(filepath.Join(root, "receipts.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	var receiptMu sync.Mutex
	record := func(kind string, data any) {
		receiptMu.Lock()
		defer receiptMu.Unlock()
		if err := json.NewEncoder(receipts).Encode(map[string]any{"at": time.Now().UTC(), "kind": kind, "data": data}); err != nil {
			t.Errorf("write receipt: %v", err)
		}
	}
	defer func() {
		record("test_finished", map[string]any{"failed": t.Failed()})
		require.NoError(t, receipts.Close())
	}()
	info, err := buildinfo.ReadFile(binary)
	require.NoError(t, err)
	body, err := os.ReadFile(binary)
	require.NoError(t, err)
	sum := sha256.Sum256(body)
	// A test using the known broken tracked pin must never produce a green
	// qualification receipt. The external replacement is independently source-bound.
	expected := "v0.6.2-0.20261028140238-b82615a042cd"
	bound := false
	for _, dep := range info.Deps {
		if dep.Path == "github.com/snissn/gomap" {
			bound = dep.Version == expected && dep.Replace == nil || dep.Replace != nil && filepath.Base(dep.Replace.Path) == "gomap@"+expected
		}
	}
	require.True(t, bound, "requires the exact provisional #5301 b826 dependency; tracked a7 is not a runtime qualification")
	record("binary", map[string]any{"path": binary, "sha256": hex.EncodeToString(sum[:]), "build": info.String()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := 25000
	if s := os.Getenv("DGRAPH_ORDERED_CLUSTER_PORT_BASE"); s != "" {
		base, err = strconv.Atoi(s)
		require.NoError(t, err)
	}
	require.True(t, base >= 1024 && base+10000 <= 65535)
	// Reserve all ports together for preflight; release just before starting the
	// servers. A competing binder after release is an explicit startup failure.
	var reserved []net.Listener
	defer func() {
		for _, l := range reserved {
			_ = l.Close()
		}
	}()
	for _, port := range []int{base, base + 1000, base + 2000, base + 3000, base + 4000, base + 5000, base + 6000, base + 7000, base + 8000, base + 9000, base + 10000} {
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		reserved = append(reserved, l)
	}
	for _, l := range reserved {
		require.NoError(t, l.Close())
	}
	record("ports", map[string]any{"zero_grpc": base, "alpha_internal": []int{base + 2000, base + 5000, base + 8000}})
	var processes []*serverProcess
	defer func() {
		for i := len(processes) - 1; i >= 0; i-- {
			if !processes[i].exited() {
				// Cleanup must continue through every owned process after a failure.
				if err := processes[i].stop(false); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}
		}
	}()
	start := func(name string, args ...string) *serverProcess {
		p, err := startServer(binary, root, name, args, record)
		require.NoError(t, err)
		processes = append(processes, p)
		return p
	}
	testServer := os.Getenv("DGRAPH_ORDERED_CLUSTER_TEST_SERVER") == "1"
	gateRoot := filepath.Join(root, "test-gates")
	if testServer {
		require.NoError(t, os.Mkdir(gateRoot, 0700))
		// Release gates before process cleanup on every assertion or RPC failure.
		defer func() {
			for i := 0; i < 3; i++ {
				_ = os.WriteFile(filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1), "release"), []byte("cleanup"), 0600)
			}
		}()
	}
	common := []string{"--survive=process", "--bindall=false", "--telemetry=reports=false; sentry=false", "--logtostderr=true"}
	zeroArgs := append([]string{"zero", "--wal", filepath.Join(root, "zero-wal"), "--replicas=3", fmt.Sprintf("--port_offset=%d", base-5080)}, common...)
	zero := start("zero", zeroArgs...)
	httpClient := &http.Client{Timeout: 3 * time.Second}
	get := func(url string) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		res, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
		if res.StatusCode != 200 {
			return nil, fmt.Errorf("%s: HTTP %d: %s", url, res.StatusCode, b)
		}
		return b, err
	}
	waitLimit := func(label string, limit time.Duration, f func() error) {
		t.Helper()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.NewTimer(limit)
		defer deadline.Stop()
		var last error
		for {
			if last = f(); last == nil {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatalf("%s: %v; %v", label, last, ctx.Err())
			case <-deadline.C:
				t.Fatalf("%s: %v", label, last)
			}
		}
	}
	wait := func(label string, f func() error) { t.Helper(); waitLimit(label, 60*time.Second, f) }
	zeroURL := fmt.Sprintf("http://127.0.0.1:%d", base+1000)
	wait("zero health", func() error { _, e := get(zeroURL + "/health"); return e })
	alphas := make([]*serverProcess, 3)
	clients := make([]*dgo.Dgraph, 3)
	generations := make([]int, 3)
	alphaInternal := func(i int) int { return base + 2000 + 3000*i }
	alphaURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", alphaInternal(i)+1000) }
	startAlpha := func(i int) {
		generations[i]++
		args := []string{"alpha", "--zero", fmt.Sprintf("127.0.0.1:%d", base), "--my", fmt.Sprintf("127.0.0.1:%d", alphaInternal(i)), "--postings", filepath.Join(root, fmt.Sprintf("alpha-%d-p", i+1)), "--wal", filepath.Join(root, fmt.Sprintf("alpha-%d-w", i+1)), fmt.Sprintf("--port_offset=%d", alphaInternal(i)-7080), "--raft", fmt.Sprintf("idx=%d; group=1; snapshot-after-entries=1000000; snapshot-after-duration=24h", i+1), "--cache", "size-mb=32", "--posting-store", "backend=treedb; tier=benchmark_minimal; durability=durable; events=true", "--badger", "syncwrites=false"}
		alphas[i] = start(fmt.Sprintf("alpha-%d-generation-%d", i+1, generations[i]), append(args, common...)...)
	}
	for i := range alphas {
		if testServer {
			require.NoError(t, os.Mkdir(filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1)), 0700))
		}
		startAlpha(i)
	}
	for i := range alphas {
		i := i
		wait(fmt.Sprintf("alpha %d health", i+1), func() error { _, e := get(alphaURL(i) + "/health"); return e })
		clients[i], err = dgo.NewClient(fmt.Sprintf("127.0.0.1:%d", alphaInternal(i)+2000), dgo.WithGrpcOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		require.NoError(t, err)
		defer clients[i].Close()
	}
	membership := func() (*pb.MembershipState, error) {
		b, e := get(zeroURL + "/state")
		if e != nil {
			return nil, e
		}
		var state pb.MembershipState
		e = protojson.Unmarshal(b, &state)
		return &state, e
	}
	leader := func(exclude int) (int, error) {
		state, e := membership()
		if e != nil {
			return 0, e
		}
		g := state.Groups[1]
		if g == nil || len(g.Members) != 3 {
			return 0, fmt.Errorf("need three group-1 replicas: %v", state.Groups)
		}
		found := -1
		for i := range alphas {
			m := g.Members[uint64(i+1)]
			if m == nil {
				return 0, fmt.Errorf("missing member %d", i+1)
			}
			if m.Leader && i != exclude {
				if found >= 0 {
					return 0, fmt.Errorf("multiple advertised leaders")
				}
				found = i
			}
		}
		if found < 0 {
			return 0, fmt.Errorf("no current leader excluding %d", exclude+1)
		}
		return found, nil
	}
	var initialLeader int
	wait("three-member leader", func() error { var e error; initialLeader, e = leader(-1); return e })
	state, err := membership()
	require.NoError(t, err)
	record("three_live_membership", state)
	for i := range alphas {
		b, e := get(alphaURL(i) + "/debug/store")
		require.NoError(t, e)
		require.Contains(t, string(b), "wal_on_sync")
		require.Contains(t, string(b), "treedb.process.identity.wal_dir:"+filepath.Join(root, fmt.Sprintf("alpha-%d-p", i+1), "maindb", "wal"))
		require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("alpha-%d-initial-store.txt", i+1)), b, 0600))
	}
	err = clients[initialLeader].Alter(ctx, &api.Operation{Schema: "cluster_id: int .\ncluster_value: string ."})
	require.NoError(t, err)
	type row struct {
		UID   string `json:"uid"`
		ID    int    `json:"cluster_id"`
		Value string `json:"cluster_value"`
	}
	expectedRows := make(map[int]row)
	batch := func(label string, n int, active []int) {
		t.Helper()
		type pending struct {
			txn   *dgo.Txn
			r     row
			start uint64
		}
		txns := make([]pending, n)
		for j := range txns {
			id := len(expectedRows) + j + 1
			txn := clients[active[j%len(active)]].NewTxn()
			res, e := txn.Mutate(ctx, &api.Mutation{SetNquads: []byte(fmt.Sprintf("_:row <cluster_id> \"%d\"^^<xs:int> .\n_:row <cluster_value> \"value-%d\" .", id, id))})
			require.NoError(t, e)
			txns[j] = pending{txn: txn, r: row{UID: res.Uids["row"], ID: id, Value: fmt.Sprintf("value-%d", id)}, start: res.Txn.StartTs}
		}
		barrier := make(chan struct{})
		results := make(chan error, n)
		for _, p := range txns {
			go func(p pending) { <-barrier; results <- p.txn.Commit(ctx) }(p)
		}
		close(barrier)
		for range txns {
			require.NoError(t, <-results, "public commit %s", label)
		}
		for _, p := range txns {
			expectedRows[p.r.ID] = p.r
			record("public_commit_ack", map[string]any{"stage": label, "row": p.r, "start_ts": p.start})
		}
	}
	validate := func(label string, active []int) {
		t.Helper()
		want := make([]row, 0, len(expectedRows))
		for _, r := range expectedRows {
			want = append(want, r)
		}
		sort.Slice(want, func(i, j int) bool { return want[i].ID < want[j].ID })
		for _, i := range active {
			res, e := clients[i].NewReadOnlyTxn().Query(ctx, "{ rows(func: has(cluster_id)) { uid cluster_id cluster_value } }")
			require.NoError(t, e, "local replica %d stage %s", i+1, label)
			var decoded struct {
				Rows []row `json:"rows"`
			}
			require.NoError(t, json.Unmarshal(res.Json, &decoded))
			sort.Slice(decoded.Rows, func(i, j int) bool { return decoded.Rows[i].ID < decoded.Rows[j].ID })
			require.Equal(t, want, decoded.Rows, "replica %d stage %s", i+1, label)
			record("local_read_verified", map[string]any{"stage": label, "replica": i + 1, "read_ts": res.Txn.StartTs, "rows": len(want)})
		}
	}
	if testServer {
		type gateTxn struct {
			txn   *dgo.Txn
			row   row
			start uint64
		}
		var pending []gateTxn
		for j := 0; j < 3; j++ {
			txn := clients[j].NewTxn()
			res, e := txn.Mutate(ctx, &api.Mutation{SetNquads: []byte(fmt.Sprintf("_:gate <cluster_id> \"%d\"^^<xs:int> .\n_:gate <cluster_value> \"gate-%d\" .", j+1, j+1))})
			require.NoError(t, e)
			pending = append(pending, gateTxn{txn: txn, row: row{UID: res.Uids["gate"], ID: j + 1, Value: fmt.Sprintf("gate-%d", j+1)}, start: res.Txn.StartTs})
		}
		validate("gate-prewrites-invisible", []int{0, 1, 2})
		for i := 0; i < 3; i++ {
			require.NoError(t, os.WriteFile(filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1), "arm"), []byte("one physical sync"), 0600))
		}
		preparedCount, decidedCount := 0, 0
		for j, p := range pending {
			// Public CommitOrAbort is Zero's decision ACK, not an Alpha posting
			// callback. Read visibility and the original proposal ACK remain held.
			require.NoError(t, p.txn.Commit(ctx))
			record("gated_zero_decision_ack", map[string]any{"ordinal": j, "start_ts": p.start})
			decidedCount++
			classification := ""
			for i := 0; i < 3; i++ {
				i := i
				waitLimit("gated public transaction staged", 10*time.Second, func() error {
					previous := pending[0].start
					if j > 0 {
						previous = pending[j-1].start
					}
					kind, e := stagedOrWatermark(filepath.Join(root, alphas[i].name+".log"), p.start, pending[0].start, previous)
					if e == nil {
						if classification != "" && classification != kind {
							return fmt.Errorf("replicas disagree on staged/barrier outcome")
						}
						classification = kind
					}
					return e
				})
			}
			record("held_successor_classification", map[string]any{"ordinal": j, "start_ts": p.start, "outcome": classification})
			if classification == "watermark_barrier" {
				break
			}
			preparedCount++
			if j == 0 {
				for i := 0; i < 3; i++ {
					i := i
					wait("native gate held", func() error { _, e := os.Stat(filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1), "held")); return e })
				}
			}
		}
		for i := 0; i < 3; i++ {
			gate := filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1))
			_, e := os.Stat(filepath.Join(gate, "durable"))
			require.True(t, os.IsNotExist(e), "held sync must not report durable before release")
			require.NoError(t, verifyStaged(filepath.Join(root, alphas[i].name+".log"), pending[preparedCount-1].start, pending[0].start))
			for _, p := range pending[:preparedCount] {
				require.NoError(t, verifyStaged(filepath.Join(root, alphas[i].name+".log"), p.start, p.start))
			}
			audit, e := auditPreparation(filepath.Join(root, alphas[i].name+".log"))
			require.NoError(t, e)
			require.Equal(t, preparedCount, audit.UnpublishedMaterialized)
			require.Equal(t, preparedCount, audit.MaximumPending)
			require.Positive(t, preparedCount)
			record("held_live_preparation_audit", map[string]any{"replica": i + 1, "audit": audit})
		}
		// Kill the actual leader before its held physical sync can execute or
		// publish any prepared original. A is prepared on all three replicas;
		// B/C preparation depends on the actual captured watermark barriers.
		// The surviving posting replicas still own their native gates.
		var cutLeader int
		wait("leader at held sync cut", func() error { var e error; cutLeader, e = leader(-1); return e })
		require.NoError(t, alphas[cutLeader].stop(true))
		record("leader_killed_before_native_sync", map[string]any{"replica": cutLeader + 1, "originals_prepared": preparedCount, "zero_decisions": decidedCount, "callbacks_published": 0})
		survivors := make([]int, 0, 2)
		for i := 0; i < 3; i++ {
			if i == cutLeader {
				continue
			}
			survivors = append(survivors, i)
			require.NoError(t, os.WriteFile(filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1), "release"), []byte("release real sync"), 0600))
		}
		for _, i := range survivors {
			i := i
			wait("actual sync success", func() error {
				_, e := os.Stat(filepath.Join(gateRoot, fmt.Sprintf("alpha-%d", i+1), "durable"))
				return e
			})
		}
		for _, p := range pending[:decidedCount] {
			expectedRows[p.row.ID] = p.row
		}
		wait("held-cut successor leader", func() error { _, e := leader(cutLeader); return e })
		validate("held-cut-survivors-durable", survivors)
		startAlpha(cutLeader)
		wait("held-cut leader replay restart", func() error { _, e := get(alphaURL(cutLeader) + "/health"); return e })
		validate("held-cut-original-recovery", []int{0, 1, 2})
		for _, p := range pending[decidedCount:] {
			require.NoError(t, p.txn.Commit(ctx))
			expectedRows[p.row.ID] = p.row
		}
		validate("held-cut-remaining-prewrite-committed", []int{0, 1, 2})
		record("held_cut_verified", map[string]any{"live_posting_replicas": 3, "prepared_originals": preparedCount, "zero_decisions_before_crash": decidedCount, "native_gate": "leader killed before sync; two survivors release into real syscall; leader replays", "physical_cohort_membership": "not inferred"})
	}
	batch("initial", 48, []int{0, 1, 2})
	validate("initial", []int{0, 1, 2})
	// Normal public conflicting prewrites must still produce one aborted decision.
	first, second := clients[0].NewTxn(), clients[1].NewTxn()
	r := expectedRows[1]
	_, err = first.Mutate(ctx, &api.Mutation{SetNquads: []byte(fmt.Sprintf("<%s> <cluster_value> \"conflict-winner\" .", r.UID))})
	require.NoError(t, err)
	_, err = second.Mutate(ctx, &api.Mutation{SetNquads: []byte(fmt.Sprintf("<%s> <cluster_value> \"conflict-loser\" .", r.UID))})
	require.NoError(t, err)
	require.NoError(t, first.Commit(ctx))
	require.ErrorIs(t, second.Commit(ctx), dgo.ErrAborted)
	r.Value = "conflict-winner"
	expectedRows[1] = r
	record("conflict_verified", r)
	validate("after-conflict", []int{0, 1, 2})
	// Record naturally observed preparation separately from the held fixture.
	// The deterministic assertion is made on all three replicas before the cut.
	for i := range alphas {
		summary, e := auditPreparation(filepath.Join(root, alphas[i].name+".log"))
		require.NoError(t, e)
		record("live_preparation_audit", map[string]any{"replica": i + 1, "audit": summary})
	}
	var dead int
	wait("current leader before kill", func() error { var e error; dead, e = leader(-1); return e })
	// Prewrite through the public API before crashing the leader. Its response
	// supplies the exact original transaction context for a commit routed through
	// a surviving Alpha after election. No fresh StartTs or reconstructed keys.
	rawConn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", alphaInternal(dead)+2000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer rawConn.Close()
	inflightID := len(expectedRows) + 1
	inflight, err := api.NewDgraphClient(rawConn).Query(ctx, &api.Request{Mutations: []*api.Mutation{{SetNquads: []byte(fmt.Sprintf("_:pending <cluster_id> \"%d\"^^<xs:int> .\n_:pending <cluster_value> \"prewrite-before-leader-crash\" .", inflightID))}}})
	require.NoError(t, err)
	require.NotZero(t, inflight.Txn.StartTs)
	require.NotEmpty(t, inflight.Txn.Keys)
	record("pre_crash_uncommitted_prewrite", inflight.Txn)
	validate("uncommitted-prewrite-invisible", []int{0, 1, 2})
	state, err = membership()
	require.NoError(t, err)
	record("pre_kill_membership", state)
	require.NoError(t, alphas[dead].stop(true))
	record("leader_sigkill", map[string]int{"replica": dead + 1})
	active := make([]int, 0, 2)
	for i := range alphas {
		if i != dead {
			active = append(active, i)
		}
	}
	var next int
	wait("survivor leader election", func() error { var e error; next, e = leader(dead); return e })
	record("survivor_leader", map[string]int{"replica": next + 1})
	survivorConn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", alphaInternal(next)+2000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer survivorConn.Close()
	decision, err := api.NewDgraphClient(survivorConn).CommitOrAbort(ctx, inflight.Txn)
	require.NoError(t, err)
	require.False(t, decision.Aborted)
	require.Greater(t, decision.CommitTs, inflight.Txn.StartTs)
	expectedRows[inflightID] = row{UID: inflight.Uids["pending"], ID: inflightID, Value: "prewrite-before-leader-crash"}
	record("pre_crash_prewrite_commit_ack", decision)
	validate("pre-crash-prewrite-committed-on-survivors", active)
	batch("two-live-survivors", 24, active)
	validate("two-live-survivors", active)
	startAlpha(dead)
	wait("crashed leader restart", func() error { _, e := get(alphaURL(dead) + "/health"); return e })
	validate("rejoined-crashed-leader", []int{0, 1, 2})
	batch("rejoined-three-live", 24, []int{0, 1, 2})
	validate("rejoined-three-live", []int{0, 1, 2})
	// No synthetic WAL Sync: crash actual server processes after successful public
	// ACKs and reads, retaining the selected process-survival contract.
	for _, p := range alphas {
		require.NoError(t, p.stop(true))
	}
	record("all_alphas_sigkilled", map[string]int{"acknowledged_rows": len(expectedRows)})
	for i := range alphas {
		startAlpha(i)
	}
	for i := range alphas {
		i := i
		wait("all-alpha restart health", func() error { _, e := get(alphaURL(i) + "/health"); return e })
	}
	validate("all-alpha-process-recovery", []int{0, 1, 2})
	batch("post-recovery-fresh-write", 6, []int{0, 1, 2})
	validate("post-recovery-fresh-write", []int{0, 1, 2})
	for _, p := range alphas {
		require.NoError(t, p.stop(false))
	}
	require.NoError(t, zero.stop(false))
	for _, p := range processes {
		require.NoError(t, rejectFatalLog(filepath.Join(root, p.name+".log")))
		if !strings.HasPrefix(p.name, "alpha-") {
			continue
		}
		audit, e := auditPreparation(filepath.Join(root, p.name+".log"))
		require.NoError(t, e)
		if !p.killed {
			require.Zero(t, audit.UnpublishedMaterialized, "graceful process must drain every materialized original")
			require.False(t, audit.IncompleteTail, "graceful process must end with complete log records")
		}
		record("terminal_preparation_audit", map[string]any{"process": p.name, "audit": audit})
	}
	record("success", map[string]any{"rows": len(expectedRows), "live_alpha_replicas": 3, "survive": "process", "physical_grouping": "not inferred from this fixture"})
}

type serverProcess struct {
	cmd         *exec.Cmd
	done        chan struct{}
	err         error
	killed      bool
	logCloseErr error
	name        string
	record      func(string, any)
}

func startServer(binary, root, name string, args []string, record func(string, any)) (*serverProcess, error) {
	f, err := os.OpenFile(filepath.Join(root, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	var cmd *exec.Cmd
	childArgs, err := json.Marshal(args)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if os.Getenv("DGRAPH_ORDERED_CLUSTER_TEST_SERVER") == "1" {
		cmd = exec.Command(binary, "-test.run=^TestOrderedDeltaClusterServer$")
	} else {
		cmd = exec.Command(binary, args...)
	}
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Dir = root
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	// Select the actual experimental route; no native inquiry, sleeps or test gates
	// are added to the servers. The already merged Delta observer is diagnostic.
	for _, s := range os.Environ() {
		if !strings.HasPrefix(s, "DGRAPH_") && !strings.HasPrefix(s, "GOMAXPROCS=") {
			cmd.Env = append(cmd.Env, s)
		}
	}
	cmd.Env = append(cmd.Env, "GOMAXPROCS=2", "DGRAPH_ORDERED_DELTA_PROVISIONAL=1", "DGRAPH_DELTA_INQUIRY=1")
	if os.Getenv("DGRAPH_ORDERED_CLUSTER_TEST_SERVER") == "1" {
		cmd.Env = append(cmd.Env, "DGRAPH_ORDERED_CLUSTER_CHILD_ARGS="+string(childArgs))
		if strings.HasPrefix(name, "alpha-") {
			parts := strings.Split(name, "-")
			cmd.Env = append(cmd.Env, "DGRAPH_ORDERED_CLUSTER_CHILD_GATE="+filepath.Join(root, "test-gates", "alpha-"+parts[1]))
		}
	}
	if err = cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	p := &serverProcess{cmd: cmd, done: make(chan struct{}), name: name, record: record}
	record("process_start", map[string]any{"name": name, "pid": cmd.Process.Pid, "args": args, "process_argv": cmd.Args, "test_server": os.Getenv("DGRAPH_ORDERED_CLUSTER_TEST_SERVER") == "1", "environment": []string{"GOMAXPROCS=2", "DGRAPH_ORDERED_DELTA_PROVISIONAL=1", "DGRAPH_DELTA_INQUIRY=1"}})
	go func() {
		p.err = cmd.Wait()
		closeErr := f.Close()
		p.logCloseErr = closeErr
		record("process_exit", map[string]any{"name": name, "pid": cmd.Process.Pid, "exit_code": cmd.ProcessState.ExitCode(), "status": cmd.ProcessState.String(), "error": fmt.Sprint(p.err), "log_close_error": fmt.Sprint(closeErr)})
		p.err = errors.Join(p.err, closeErr)
		close(p.done)
	}()
	return p, nil
}
func (p *serverProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
func (p *serverProcess) stop(kill bool) error {
	if p.exited() {
		return fmt.Errorf("%s exited before requested stop: %v", p.name, p.err)
	}
	signal := os.Signal(syscall.SIGTERM)
	if kill {
		signal = syscall.SIGKILL
	}
	p.record("process_signal", map[string]any{"name": p.name, "pid": p.cmd.Process.Pid, "signal": signal.String()})
	if err := p.cmd.Process.Signal(signal); err != nil {
		return err
	}
	select {
	case <-p.done:
	case <-time.After(25 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		return fmt.Errorf("%s required unplanned forced kill after SIGTERM", p.name)
	}
	if kill {
		var e *exec.ExitError
		if errors.As(p.err, &e) {
			if s, ok := e.Sys().(syscall.WaitStatus); ok && s.Signaled() && s.Signal() == syscall.SIGKILL {
				p.killed = true
				return p.logCloseErr
			}
		}
		return fmt.Errorf("%s expected SIGKILL status, got %v", p.name, p.err)
	}
	return p.err
}
