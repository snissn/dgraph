// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	dgo "github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	dgraphBin, artifactDir, backend, class, cpuProfile                   string
	repeat, dataset, concurrency, warmup, timed, zeroOffset, alphaOffset int
	profileSeconds                                                       int
	seed                                                                 int64
	maxLoad                                                              float64
}

type process struct {
	cmd *exec.Cmd
	log *os.File
}

type expectedNode struct {
	Value     string
	NextValue string
}

func main() {
	var o options
	flag.StringVar(&o.dgraphBin, "dgraph-bin", "", "path to the dgraph binary")
	flag.StringVar(&o.artifactDir, "artifact-dir", "", "new per-run artifact directory")
	flag.StringVar(&o.backend, "backend", "", "badger or treedb")
	flag.StringVar(&o.class, "durability", "", "relaxed or durable")
	flag.IntVar(&o.repeat, "repeat", 1, "one-based repeat number")
	flag.IntVar(&o.dataset, "dataset-nodes", 500, "initial node count")
	flag.IntVar(&o.concurrency, "concurrency", 4, "timed workload concurrency")
	flag.IntVar(&o.warmup, "warmup-ops", 100, "excluded warmup operations")
	flag.IntVar(&o.timed, "timed-ops", 2000, "fixed operation count in timed phase")
	flag.IntVar(&o.zeroOffset, "zero-port-offset", 18000, "Zero port offset")
	flag.IntVar(&o.alphaOffset, "alpha-port-offset", 19000, "Alpha port offset")
	flag.Int64Var(&o.seed, "seed", 20260711, "workload seed")
	flag.StringVar(&o.cpuProfile, "cpu-profile", "", "new path for a separate-run Alpha CPU profile")
	flag.IntVar(&o.profileSeconds, "profile-seconds", 5, "CPU profile duration")
	flag.Float64Var(&o.maxLoad, "max-load1", float64(runtime.NumCPU())*.75, "exclude a run if host load1 exceeds this")
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "live benchmark:", err)
		os.Exit(1)
	}
}

func run(o options) (err error) {
	if o.dgraphBin == "" || o.artifactDir == "" || (o.backend != "badger" && o.backend != "treedb") || (o.class != "relaxed" && o.class != "durable") {
		return errors.New("--dgraph-bin, --artifact-dir, valid --backend, and valid --durability are required")
	}
	if o.repeat < 1 || o.dataset < 1 || o.concurrency < 1 || o.warmup < 1 || o.timed < 1 {
		return errors.New("--repeat, --dataset-nodes, --concurrency, --warmup-ops, and --timed-ops must be positive")
	}
	if o.cpuProfile != "" && o.profileSeconds < 1 {
		return errors.New("--profile-seconds must be positive when --cpu-profile is set")
	}
	if _, err := os.Stat(o.artifactDir); !os.IsNotExist(err) {
		return fmt.Errorf("artifact directory must not exist: %s", o.artifactDir)
	}
	if err := os.MkdirAll(o.artifactDir, 0o755); err != nil {
		return err
	}
	runDir := filepath.Join(o.artifactDir, "cluster")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	posting, badger := selectors(o.backend, o.class)
	config := livebench.Config{Backend: o.backend, DurabilityClass: o.class, PostingStore: posting, Badger: badger, DatasetNodes: o.dataset, Concurrency: o.concurrency, WarmupOps: o.warmup, TimedOps: o.timed, QueryMix: map[string]int{"point_read": 60, "one_hop_read": 20, "write": 20}, Topology: "single-zero-single-alpha", Seed: o.seed}
	r := livebench.Result{SchemaVersion: livebench.SchemaVersion, RunID: fmt.Sprintf("%s-%s-r%d", o.backend, o.class, o.repeat), Repeat: o.repeat, Config: config, LatencyMS: map[string]float64{}, Metrics: map[string]livebench.Metric{}}
	r.Context, err = collectContext(o)
	if err != nil {
		return fmt.Errorf("collect reproduction context: %w", err)
	}
	initialContaminants := contaminants(o.maxLoad)
	r.Context.Contaminants = initialContaminants
	if len(initialContaminants) > 0 {
		r.Context.Excluded = true
		r.Context.ExclusionReason = strings.Join(initialContaminants, "; ")
	}

	zeroArgs := []string{"zero", "--wal", filepath.Join(runDir, "zw"), "--replicas=1", fmt.Sprintf("--port_offset=%d", o.zeroOffset)}
	alphaArgs := []string{"alpha", "--zero", fmt.Sprintf("localhost:%d", 5080+o.zeroOffset), "--postings", filepath.Join(runDir, "p"), "--wal", filepath.Join(runDir, "w"), fmt.Sprintf("--port_offset=%d", o.alphaOffset), "--posting-store", posting, "--badger", badger}
	zero, err := start(o.dgraphBin, zeroArgs, filepath.Join(o.artifactDir, "zero.log"))
	if err != nil {
		return err
	}
	defer stopOnReturn(zero, "zero", &err)
	if err := waitHTTP(ctx, fmt.Sprintf("http://localhost:%d/health", 6080+o.zeroOffset), 60*time.Second); err != nil {
		return fmt.Errorf("zero readiness: %w", err)
	}
	alpha, err := start(o.dgraphBin, alphaArgs, filepath.Join(o.artifactDir, "alpha.log"))
	if err != nil {
		return err
	}
	defer stopOnReturn(alpha, "alpha", &err)
	httpBase := fmt.Sprintf("http://localhost:%d", 8080+o.alphaOffset)
	if err := waitHTTP(ctx, httpBase+"/health", 90*time.Second); err != nil {
		return fmt.Errorf("alpha readiness: %w", err)
	}
	dg, err := client(fmt.Sprintf("localhost:%d", 9080+o.alphaOffset))
	if err != nil {
		return err
	}
	defer dg.Close()

	r.SetupStarted = time.Now().UTC()
	dataset, err := setup(ctx, dg, o.dataset)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	_, warmupWrites, err := exercise(ctx, dg, dataset, o.warmup, 1, o.seed, 0x200000)
	if err != nil {
		return fmt.Errorf("warmup: %w", err)
	}
	r.SetupFinished = time.Now().UTC()
	_, storeBefore, _ := storeStatus(httpBase)
	promBefore, _ := prometheus(httpBase + "/debug/prometheus_metrics")
	cpuBefore, err := procCPU(alpha.cmd.Process.Pid)
	if err != nil {
		return fmt.Errorf("collect alpha CPU before timed phase: %w", err)
	}
	r.TimedStarted = time.Now().UTC()
	var profileDone chan error
	if o.cpuProfile != "" {
		profileDone = make(chan error, 1)
		go func() { profileDone <- captureCPUProfile(httpBase, o.cpuProfile, o.profileSeconds) }()
		time.Sleep(100 * time.Millisecond)
	}
	latencies, timedWrites, err := exercise(ctx, dg, dataset, o.timed, o.concurrency, o.seed, 0x300000)
	r.TimedFinished = time.Now().UTC()
	if err != nil {
		return fmt.Errorf("timed workload: %w", err)
	}
	if profileDone != nil {
		if err := <-profileDone; err != nil {
			return fmt.Errorf("CPU profile: %w", err)
		}
		r.Context.Profiles = append(r.Context.Profiles, o.cpuProfile)
	}
	r.Throughput = float64(o.timed) / r.TimedFinished.Sub(r.TimedStarted).Seconds()
	r.LatencyMS = livebench.Percentiles(latencies)
	cpuAfter, err := procCPU(alpha.cmd.Process.Pid)
	if err != nil {
		return fmt.Errorf("collect alpha CPU after timed phase: %w", err)
	}
	if cpuAfter < cpuBefore {
		return fmt.Errorf("alpha CPU counter regressed: before=%f after=%f", cpuBefore, cpuAfter)
	}
	hwm, err := procHWM(alpha.cmd.Process.Pid)
	if err != nil {
		return fmt.Errorf("collect Alpha RSS HWM: %w", err)
	}
	diskLogical, diskAllocated, err := diskUsage(filepath.Join(runDir, "p"))
	if err != nil {
		return fmt.Errorf("collect posting disk usage: %w", err)
	}
	promAfter, _ := prometheus(httpBase + "/debug/prometheus_metrics")
	statusAfter, storeAfter, err := storeStatus(httpBase)
	if err != nil {
		return fmt.Errorf("collect posting-store status: %w", err)
	}
	r.Metrics = metrics(o.backend, cpuAfter-cpuBefore, hwm, diskLogical, diskAllocated, promBefore, promAfter, storeBefore, storeAfter)
	r.Validation.BackendObserved = statusAfter["backend"]
	r.Validation.DurabilityObserved = statusAfter["profile"]
	r.Validation.UnsupportedOK = unsupportedOK(o.backend, statusAfter)
	r.Validation.SchemaOK = schemaOK(ctx, dg)
	expected := mergeExpected(dataset, warmupWrites, timedWrites)
	checksum, count, err := validatePosting(ctx, dg, expected)
	if err != nil {
		return err
	}
	r.Validation.PostingChecksum = checksum
	r.Validation.NodeCount = count

	dg.Close()
	if err := alpha.stop(); err != nil {
		return fmt.Errorf("stop alpha: %w", err)
	}
	recoveryStart := time.Now()
	alpha, err = start(o.dgraphBin, alphaArgs, filepath.Join(o.artifactDir, "alpha-restart.log"))
	if err != nil {
		return err
	}
	defer stopOnReturn(alpha, "restarted alpha", &err)
	if err := waitHTTP(ctx, httpBase+"/health", 90*time.Second); err != nil {
		return fmt.Errorf("alpha restart: %w", err)
	}
	recoverySeconds := time.Since(recoveryStart).Seconds()
	r.Metrics["recovery_seconds"] = livebench.Metric{Available: true, Value: recoverySeconds, Unit: "seconds", Source: "alpha process restart to healthy"}
	dg, err = client(fmt.Sprintf("localhost:%d", 9080+o.alphaOffset))
	if err != nil {
		return err
	}
	defer dg.Close()
	restartChecksum, restartCount, err := validatePosting(ctx, dg, expected)
	r.Validation.RestartOK = err == nil && restartChecksum == checksum && restartCount == count
	if err != nil {
		return fmt.Errorf("restart validation: %w", err)
	}
	for _, contaminant := range contaminants(o.maxLoad) {
		if !contains(r.Context.Contaminants, contaminant) {
			r.Context.Contaminants = append(r.Context.Contaminants, contaminant)
		}
	}
	if len(r.Context.Contaminants) > 0 {
		r.Context.Excluded = true
		r.Context.ExclusionReason = strings.Join(r.Context.Contaminants, "; ")
	}
	if err := r.Validate(); err != nil {
		return fmt.Errorf("result validation: %w", err)
	}
	r.Context.RawPath = filepath.Join(o.artifactDir, "result.json")
	// RawPath participates in validation, so update it before the final validation/write.
	if err := r.Validate(); err != nil {
		return fmt.Errorf("final result validation: %w", err)
	}
	return livebench.WriteImmutable(r.Context.RawPath, r)
}

func selectors(backend, class string) (string, string) {
	if backend == "treedb" {
		return fmt.Sprintf("backend=treedb; tier=benchmark_minimal; durability=%s; events=true", class), "syncwrites=false"
	}
	syncwrites := "false"
	if class == "durable" {
		syncwrites = "true"
	}
	return "backend=badger; tier=production; durability=durable; events=false", "syncwrites=" + syncwrites
}

func start(bin string, args []string, logPath string) (*process, error) {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if err := cmd.Start(); err != nil {
		f.Close()
		return nil, err
	}
	return &process{cmd: cmd, log: f}, nil
}
func (p *process) stop() error {
	if p == nil || p.cmd == nil || p.cmd.ProcessState != nil {
		return nil
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		_ = p.log.Close()
		if ee := new(exec.ExitError); errors.As(err, &ee) {
			return nil
		}
		return err
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-done
		_ = p.log.Close()
		return fmt.Errorf("forced kill: %w", err)
	}
}

func stopOnReturn(p *process, name string, runErr *error) {
	if err := p.stop(); err != nil && *runErr == nil {
		*runErr = fmt.Errorf("stop %s: %w", name, err)
	}
}

func waitHTTP(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				_ = resp.Body.Close()
				return fmt.Errorf("drain readiness response: %w", err)
			}
			if err := resp.Body.Close(); err != nil {
				return fmt.Errorf("close readiness response: %w", err)
			}
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return errors.New("timeout")
}
func client(addr string) (*dgo.Dgraph, error) {
	return dgo.NewClient(addr, dgo.WithGrpcOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
}

func setup(ctx context.Context, dg *dgo.Dgraph, n int) (map[string]expectedNode, error) {
	if err := dg.Alter(ctx, &api.Operation{Schema: "bench.value: string .\nbench.next: uid .\n"}); err != nil {
		return nil, err
	}
	ids := make([]string, 0, n)
	expected := make(map[string]expectedNode, n)
	for base := 1; base <= n; base += 100 {
		end := base + 100
		if end > n+1 {
			end = n + 1
		}
		var b strings.Builder
		for i := base; i < end; i++ {
			fmt.Fprintf(&b, "_:n%d <bench.value> %q .\n", i, value(i))
		}
		resp, err := dg.NewTxn().Mutate(ctx, &api.Mutation{SetNquads: []byte(b.String()), CommitNow: true})
		if err != nil {
			return nil, err
		}
		for i := base; i < end; i++ {
			uid := resp.Uids[fmt.Sprintf("n%d", i)]
			if uid == "" {
				return nil, fmt.Errorf("setup mutation omitted uid for n%d", i)
			}
			ids = append(ids, uid)
		}
	}
	var edges strings.Builder
	for i, uid := range ids {
		fmt.Fprintf(&edges, "<%s> <bench.next> <%s> .\n", uid, ids[(i+1)%len(ids)])
	}
	if _, err := dg.NewTxn().Mutate(ctx, &api.Mutation{SetNquads: []byte(edges.String()), CommitNow: true}); err != nil {
		return nil, err
	}
	for i, uid := range ids {
		expected[uid] = expectedNode{Value: value(i + 1), NextValue: value((i+1)%len(ids) + 1)}
	}
	return expected, nil
}

func exercise(ctx context.Context, dg *dgo.Dgraph, dataset map[string]expectedNode, ops, concurrency int, seed int64, writeBase int) ([]float64, map[string]expectedNode, error) {
	type sample struct {
		ms    float64
		uid   string
		value string
		write bool
		err   error
	}
	uids := make([]string, 0, len(dataset))
	for uid := range dataset {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	out := make(chan sample, ops)
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < ops; i += concurrency {
				start := time.Now()
				kind := (int(seed) + i*37) % 100
				uid := uids[(int(seed)+i*17)%len(uids)]
				var err error
				if kind < 60 {
					var resp *api.Response
					resp, err = dg.NewReadOnlyTxn().QueryWithVars(ctx, "query q($u: string) { q(func: uid($u)) { uid bench.value } }", map[string]string{"$u": uid})
					if err == nil {
						err = validateReadResponse(resp.Json, uid, dataset[uid], false)
					}
				} else if kind < 80 {
					var resp *api.Response
					resp, err = dg.NewReadOnlyTxn().QueryWithVars(ctx, "query q($u: string) { q(func: uid($u)) { uid bench.value bench.next { uid bench.value } } }", map[string]string{"$u": uid})
					if err == nil {
						err = validateReadResponse(resp.Json, uid, dataset[uid], true)
					}
				} else {
					writeID := writeBase + i
					resp, mutErr := dg.NewTxn().Mutate(ctx, &api.Mutation{SetNquads: []byte(fmt.Sprintf("_:w <bench.value> %q .", value(writeID))), CommitNow: true})
					err = mutErr
					if err == nil {
						out <- sample{ms: float64(time.Since(start).Microseconds()) / 1000, uid: resp.Uids["w"], value: value(writeID), write: true}
						continue
					}
				}
				out <- sample{ms: float64(time.Since(start).Microseconds()) / 1000, err: err}
			}
		}(worker)
	}
	go func() { wg.Wait(); close(out) }()
	values := make([]float64, 0, ops)
	writes := map[string]expectedNode{}
	for s := range out {
		if s.err != nil {
			return nil, nil, s.err
		}
		if s.write {
			if err := recordExpectedWrite(writes, s.uid, s.value); err != nil {
				return nil, nil, err
			}
		}
		values = append(values, s.ms)
	}
	return values, writes, nil
}

func recordExpectedWrite(writes map[string]expectedNode, uid, value string) error {
	if uid == "" {
		return errors.New("successful write mutation omitted uid for blank node w")
	}
	if value == "" {
		return errors.New("write result missing value")
	}
	if _, duplicate := writes[uid]; duplicate {
		return fmt.Errorf("duplicate write uid %s", uid)
	}
	writes[uid] = expectedNode{Value: value}
	return nil
}

func value(i int) string { return fmt.Sprintf("node-%08d-%s", i, strings.Repeat("x", 48)) }
func mergeExpected(parts ...map[string]expectedNode) map[string]expectedNode {
	out := map[string]expectedNode{}
	for _, part := range parts {
		for uid, node := range part {
			out[uid] = node
		}
	}
	return out
}

type queryNode struct {
	UID   string     `json:"uid"`
	Value string     `json:"bench.value"`
	Next  queryNodes `json:"bench.next"`
}

type queryNodes []queryNode

func (nodes *queryNodes) UnmarshalJSON(raw []byte) error {
	if string(raw) == "null" {
		*nodes = nil
		return nil
	}
	if len(raw) > 0 && raw[0] == '[' {
		return json.Unmarshal(raw, (*[]queryNode)(nodes))
	}
	var node queryNode
	if err := json.Unmarshal(raw, &node); err != nil {
		return err
	}
	*nodes = []queryNode{node}
	return nil
}

func validateReadResponse(raw []byte, uid string, expected expectedNode, oneHop bool) error {
	var data struct {
		Q []queryNode `json:"q"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("decode read response: %w", err)
	}
	if len(data.Q) != 1 || data.Q[0].UID != uid || data.Q[0].Value != expected.Value {
		return fmt.Errorf("point read mismatch for %s", uid)
	}
	if !oneHop {
		return nil
	}
	if expected.NextValue == "" {
		if len(data.Q[0].Next) != 0 {
			return fmt.Errorf("unexpected one-hop result for %s", uid)
		}
		return nil
	}
	if len(data.Q[0].Next) != 1 || data.Q[0].Next[0].Value != expected.NextValue {
		return fmt.Errorf("one-hop read mismatch for %s", uid)
	}
	return nil
}

func validatePosting(ctx context.Context, dg *dgo.Dgraph, expected map[string]expectedNode) (string, int, error) {
	resp, err := dg.NewReadOnlyTxn().Query(ctx, postingValidationQuery(len(expected)))
	if err != nil {
		return "", 0, err
	}
	var data struct {
		Q []queryNode `json:"q"`
	}
	if err := json.Unmarshal(resp.Json, &data); err != nil {
		return "", 0, err
	}
	rows, err := validatePostingRows(data.Q, expected)
	if err != nil {
		return "", len(rows), err
	}
	sort.Strings(rows)
	h := sha256.Sum256([]byte(strings.Join(rows, "\n")))
	return hex.EncodeToString(h[:]), len(rows), nil
}

func postingValidationQuery(expectedCount int) string {
	// Asking for one more than the expected cardinality makes any extra
	// bench.value node observable instead of relying on Dgraph's default limit.
	return fmt.Sprintf("{ q(func: has(bench.value), first: %d) { uid bench.value bench.next { uid bench.value } } }", expectedCount+1)
}

func validatePostingRows(nodes []queryNode, expected map[string]expectedNode) ([]string, error) {
	rows := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if _, duplicate := seen[node.UID]; duplicate {
			return rows, fmt.Errorf("duplicate posting row for %s", node.UID)
		}
		seen[node.UID] = struct{}{}
		want, ok := expected[node.UID]
		if !ok {
			return rows, fmt.Errorf("unexpected posting row for %s", node.UID)
		}
		row, err := canonicalPostingRow(node, want)
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
	missing := make([]string, 0, len(expected)-len(seen))
	for uid := range expected {
		if _, ok := seen[uid]; !ok {
			missing = append(missing, uid)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return rows, fmt.Errorf("omitted posting row for %s", missing[0])
	}
	return rows, nil
}

func canonicalPostingRow(node queryNode, expected expectedNode) (string, error) {
	if expected.Value != node.Value {
		return "", fmt.Errorf("posting value mismatch for %s", node.UID)
	}
	nextValue := ""
	if len(node.Next) > 1 {
		return "", fmt.Errorf("posting edge count mismatch for %s", node.UID)
	}
	if len(node.Next) == 1 {
		nextValue = node.Next[0].Value
	}
	if nextValue != expected.NextValue {
		return "", fmt.Errorf("posting edge mismatch for source value %q", node.Value)
	}
	return node.Value + "\x00" + nextValue, nil
}
func schemaOK(ctx context.Context, dg *dgo.Dgraph) bool {
	resp, err := dg.NewReadOnlyTxn().Query(ctx, "schema(pred: [bench.value, bench.next]) { predicate type }")
	return err == nil && strings.Contains(string(resp.Json), "bench.value") && strings.Contains(string(resp.Json), "bench.next")
}

func fetch(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func captureCPUProfile(base, path string, seconds int) error {
	if seconds < 1 {
		return errors.New("profile-seconds must be positive")
	}
	b, err := fetch(fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", base, seconds))
	if err != nil {
		return err
	}
	return livebench.WriteImmutableBytes(path, b)
}
func storeStatus(base string) (map[string]string, map[string]float64, error) {
	b, err := fetch(base + "/debug/store")
	if err != nil {
		return nil, nil, err
	}
	s := string(b)
	status := parseMapSection(s, "status=map[")
	stats := parseFloatMapSection(s, "stats=map[")
	return status, stats, nil
}
func parseMapSection(s, prefix string) map[string]string {
	out := map[string]string{}
	start := strings.Index(s, prefix)
	if start < 0 {
		return out
	}
	start += len(prefix)
	end := strings.IndexByte(s[start:], ']')
	if end < 0 {
		return out
	}
	for _, field := range strings.Fields(s[start : start+end]) {
		parts := strings.SplitN(field, ":", 2)
		if len(parts) == 2 {
			out[parts[0]] = parts[1]
		}
	}
	return out
}
func parseFloatMapSection(s, prefix string) map[string]float64 {
	raw := parseMapSection(s, prefix)
	out := map[string]float64{}
	for k, v := range raw {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			out[k] = n
		}
	}
	return out
}
func unsupportedOK(backend string, status map[string]string) bool {
	u := status["unsupported"]
	if backend == "badger" {
		return u == ""
	}
	if backend != "treedb" {
		return false
	}
	want := map[string]struct{}{
		"backup": {}, "export": {}, "import": {}, "restore": {}, "encryption": {},
		"in_memory": {}, "ttl": {}, "badger_subscribe": {}, "sort": {}, "count": {}, "inequality": {},
	}
	got := make(map[string]struct{}, len(want))
	for _, token := range strings.Split(u, ",") {
		token = strings.TrimSpace(token)
		if _, expected := want[token]; !expected {
			return false
		}
		if _, duplicate := got[token]; duplicate {
			return false
		}
		got[token] = struct{}{}
	}
	return len(got) == len(want)
}

func prometheus(url string) (map[string]float64, error) {
	b, err := fetch(url)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := strings.SplitN(fields[0], "{", 2)[0]
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err == nil {
			out[name] += v
		}
	}
	return out, sc.Err()
}
func delta(after, before map[string]float64, key string) (float64, bool) {
	a, aok := after[key]
	b, bok := before[key]
	return a - b, aok && bok
}

func metrics(backend string, cpu, hwm, diskLogical, diskAllocated float64, pb, pa, sb, sa map[string]float64) map[string]livebench.Metric {
	m := map[string]livebench.Metric{
		"cpu_seconds": {Available: true, Value: cpu, Unit: "seconds", Source: "/proc alpha utime+stime"}, "rss_peak_bytes": {Available: true, Value: hwm, Unit: "bytes", Source: "/proc alpha VmHWM"}, "disk_logical_bytes": {Available: true, Value: diskLogical, Unit: "bytes", Source: "posting directory walk"}, "disk_allocated_bytes": {Available: true, Value: diskAllocated, Unit: "bytes", Source: "posting directory stat blocks"},
		"recovery_seconds": {Available: false, Unit: "seconds", Source: "alpha restart", Reason: "populated after restart"},
	}
	writeBytes, writeAvail := delta(pa, pb, "badger_write_bytes_user")
	writeSource := "Badger Prometheus badger_write_bytes_user"
	if backend == "treedb" {
		writeBytes, writeAvail = delta(sa, sb, "treedb.command_wal.public_batch.set.bytes_total")
		writeSource = "TreeDB /debug/store public batch set bytes"
	}
	m["write_bytes"] = livebench.Metric{Available: writeAvail, Value: writeBytes, Unit: "bytes", Source: writeSource}
	physical := 0.0
	physicalOK := backend == "badger"
	for _, name := range []string{"badger_write_bytes_l0", "badger_write_bytes_vlog", "badger_write_bytes_compaction"} {
		v, ok := delta(pa, pb, name)
		physical += v
		physicalOK = physicalOK && ok
	}
	if writeAvail && writeBytes > 0 && physicalOK {
		m["write_amplification"] = livebench.Metric{Available: true, Value: physical / writeBytes, Unit: "ratio", Source: "Badger physical L0+vlog+compaction bytes / user bytes"}
	} else {
		m["write_amplification"] = livebench.Metric{Available: false, Unit: "ratio", Source: "backend diagnostics", Reason: "equivalent physical write-byte counter unavailable or logical write bytes zero"}
	}
	gc, gcOK := delta(pa, pb, "go_gc_duration_seconds_count")
	gcSource := "Prometheus Go runtime GC"
	flush, flushOK := 0.0, false
	flushSource := "Badger diagnostics"
	flushReason := "Badger exposes badger_write_num_vlog, which is not a semantic flush counter"
	check := 0.0
	checkOK := false
	checkSource := "backend diagnostics"
	if backend == "treedb" {
		gc, gcOK = delta(sa, sb, "treedb.maintenance.full_scan.gc_runs")
		gcSource = "TreeDB /debug/store"
		flush, flushOK = delta(sa, sb, "treedb.command_wal.flush.count_total")
		flushSource = "TreeDB /debug/store"
		flushReason = "TreeDB semantic command-WAL flush counter unavailable"
		check, checkOK = delta(sa, sb, "treedb.cache.auto_checkpoint.count")
		checkSource = "TreeDB /debug/store"
	}
	m["gc_cycles"] = availability(gc, gcOK, "count", gcSource, "counter unavailable")
	m["flushes"] = availability(flush, flushOK, "count", flushSource, flushReason)
	m["checkpoints"] = availability(check, checkOK, "count", checkSource, "counter unavailable for selected backend")
	return m
}
func availability(v float64, ok bool, unit, source, reason string) livebench.Metric {
	return livebench.Metric{Available: ok, Value: v, Unit: unit, Source: source, Reason: map[bool]string{true: "", false: reason}[ok]}
}

func procCPU(pid int) (float64, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	f := strings.Fields(string(b))
	if len(f) < 15 {
		return 0, errors.New("short proc stat")
	}
	u, err := strconv.ParseFloat(f[13], 64)
	if err != nil {
		return 0, fmt.Errorf("parse proc utime: %w", err)
	}
	s, err := strconv.ParseFloat(f[14], 64)
	if err != nil {
		return 0, fmt.Errorf("parse proc stime: %w", err)
	}
	return (u + s) / 100, nil
}
func procHWM(pid int) (float64, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			f := strings.Fields(line)
			if len(f) < 2 {
				return 0, errors.New("malformed VmHWM")
			}
			v, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				return 0, fmt.Errorf("parse VmHWM: %w", err)
			}
			return v * 1024, nil
		}
	}
	return 0, errors.New("VmHWM unavailable")
}
func diskUsage(root string) (float64, float64, error) {
	var logical, allocated float64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			logical += float64(info.Size())
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				allocated += float64(st.Blocks * 512)
			} else {
				return errors.New("filesystem stat does not expose allocated blocks")
			}
		}
		return nil
	})
	return logical, allocated, err
}

func collectContext(o options) (livebench.Context, error) {
	cmd := []string{os.Args[0]}
	cmd = append(cmd, os.Args[1:]...)
	dgraphSHA, err := commandRequired("git", "rev-parse", "HEAD")
	if err != nil {
		return livebench.Context{}, err
	}
	gomapVersion, err := commandRequired("go", "list", "-m", "-f", "{{.Version}}", "github.com/snissn/gomap")
	if err != nil {
		return livebench.Context{}, err
	}
	host, err := commandRequired("hostname")
	if err != nil {
		return livebench.Context{}, err
	}
	kernel, err := commandRequired("uname", "-srvmo")
	if err != nil {
		return livebench.Context{}, err
	}
	cpu, err := firstCPU()
	if err != nil {
		return livebench.Context{}, err
	}
	ram, err := totalRAM()
	if err != nil {
		return livebench.Context{}, err
	}
	storage, err := storageContext(o.artifactDir)
	if err != nil {
		return livebench.Context{}, err
	}
	environment := map[string]string{}
	for _, name := range []string{"GOWORK", "TMPDIR", "GOMAXPROCS", "GOFLAGS"} {
		environment[name] = os.Getenv(name)
	}
	return livebench.Context{DgraphSHA: dgraphSHA, GomapVersion: gomapVersion, Dirty: command("git", "status", "--porcelain") != "", GoVersion: runtime.Version(), Host: host, Kernel: kernel, CPU: cpu, TotalRAMBytes: ram, Storage: storage, Environment: environment, ExactCommand: cmd, RawPath: filepath.Join(o.artifactDir, "result.json")}, nil
}
func command(name string, args ...string) string {
	b, _ := exec.Command(name, args...).Output()
	return strings.TrimSpace(string(b))
}
func commandRequired(name string, args ...string) (string, error) {
	b, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(b)))
	}
	value := strings.TrimSpace(string(b))
	if value == "" {
		return "", fmt.Errorf("%s returned empty output", name)
	}
	return value, nil
}
func firstCPU() (string, error) {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "model name") {
			p := strings.SplitN(line, ":", 2)
			if len(p) == 2 {
				return strings.TrimSpace(p[1]), nil
			}
		}
	}
	return "", errors.New("CPU model unavailable")
}
func totalRAM() (uint64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				break
			}
			kib, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kib * 1024, nil
		}
	}
	return 0, errors.New("MemTotal unavailable")
}
func storageContext(path string) (livebench.StorageContext, error) {
	source, err := commandRequired("findmnt", "-no", "SOURCE", "-T", path)
	if err != nil {
		return livebench.StorageContext{}, err
	}
	filesystem, err := commandRequired("findmnt", "-no", "FSTYPE", "-T", path)
	if err != nil {
		return livebench.StorageContext{}, err
	}
	mountpoint, err := commandRequired("findmnt", "-no", "TARGET", "-T", path)
	if err != nil {
		return livebench.StorageContext{}, err
	}
	device := strings.SplitN(source, "[", 2)[0]
	if parent := command("lsblk", "-ndo", "PKNAME", device); parent != "" {
		device = filepath.Join("/dev", strings.Fields(parent)[0])
	}
	model, err := commandRequired("lsblk", "-dn", "-o", "MODEL", device)
	if err != nil {
		return livebench.StorageContext{}, err
	}
	sizeText, err := commandRequired("lsblk", "-dn", "-b", "-o", "SIZE", device)
	if err != nil {
		return livebench.StorageContext{}, err
	}
	size, err := strconv.ParseUint(strings.Fields(sizeText)[0], 10, 64)
	if err != nil {
		return livebench.StorageContext{}, fmt.Errorf("parse storage size: %w", err)
	}
	return livebench.StorageContext{Scope: "artifact_and_posting", Source: source, Model: model, SizeBytes: size, Filesystem: filesystem, Mountpoint: mountpoint}, nil
}
func contaminants(maxLoad float64) []string {
	var out []string
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		b, _ := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if strings.Contains(string(b), "construction_audit.py") {
			out = append(out, "construction_audit.py active")
			break
		}
	}
	b, _ := os.ReadFile("/proc/loadavg")
	f := strings.Fields(string(b))
	if len(f) > 0 {
		load, _ := strconv.ParseFloat(f[0], 64)
		if load > maxLoad {
			out = append(out, fmt.Sprintf("load1 %.2f exceeds %.2f", load, maxLoad))
		}
	}
	return out
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
