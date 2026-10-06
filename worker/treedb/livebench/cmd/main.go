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
	dgraphBin, artifactDir, backend, class, cpuProfile, endpointDiagnostic, operationDiagnostic, storageDiagnostic string
	repeat, dataset, concurrency, warmup, timed, zeroOffset, alphaOffset                                           int
	profileSeconds                                                                                                 int
	seed                                                                                                           int64
	maxLoad                                                                                                        float64
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
	flag.StringVar(&o.endpointDiagnostic, "endpoint-diagnostic", "", "new JSON sidecar for separate diagnostic-run posting endpoints; diagnostic overhead only")
	flag.StringVar(&o.operationDiagnostic, "operation-diagnostic", "", "new operation/server-phase JSON sidecar; diagnostic overhead excluded from acceptance")
	flag.StringVar(&o.storageDiagnostic, "storage-diagnostic", "", "new same-walk posting-file JSON sidecar; diagnostic overhead excluded from acceptance")
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
	if err := validateEndpointDiagnostic(o); err != nil {
		return err
	}
	if err := validateOperationDiagnostic(o); err != nil {
		return err
	}
	if err := validateStorageDiagnosticOption(o); err != nil {
		return err
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
	r.Context.Contaminants = contaminants(o.maxLoad)
	markDiagnostics(&r, o)

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
	var diagnostic *operationDiagnostic
	var beforeStore, beforeProm, afterStore, afterProm *operationRequest
	if o.operationDiagnostic != "" {
		diagnostic = &operationDiagnostic{SchemaVersion: 1, DiagnosticOnly: true, RunID: r.RunID,
			Backend: o.backend, DurabilityClass: o.class, Seed: o.seed, Concurrency: o.concurrency,
			ExpectedOperations: o.timed, Rows: make([]operationRow, o.timed),
			Before: operationBoundary{Boundary: "before_workload"},
			After:  operationBoundary{Boundary: "native_metrics_after"}}
		beforeStore, beforeProm = &diagnostic.Before.Store, &diagnostic.Before.Prometheus
		afterStore, afterProm = &diagnostic.After.Store, &diagnostic.After.Prometheus
	}
	_, storeBefore, storeErr := storeStatusObserved(httpBase, beforeStore)
	promBefore, promErr := prometheusObserved(httpBase+"/debug/prometheus_metrics", beforeProm)
	if diagnostic != nil && (storeErr != nil || promErr != nil) {
		return fmt.Errorf("operation diagnostic before boundary: %w", errors.Join(storeErr, promErr))
	}
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
	var operationRows []operationRow
	if diagnostic != nil {
		operationRows = diagnostic.Rows
	}
	latencies, timedWrites, err := exerciseWithOperations(ctx, dg, dataset, o.timed, o.concurrency, o.seed, 0x300000, operationRows)
	r.TimedFinished = time.Now().UTC()
	diagnosticWriteAttempted := false
	if diagnostic != nil {
		diagnostic.TimedStarted, diagnostic.TimedFinished = r.TimedStarted, r.TimedFinished
		diagnostic.WorkloadSucceeded = err == nil
		// Preserve drained rows if a later observer or validation fails. This
		// defer precedes older process-cleanup defers and never retries a write.
		defer func() {
			if !diagnosticWriteAttempted {
				err = errors.Join(err, writeOperationDiagnostic(o.operationDiagnostic, diagnostic, filepath.Join(runDir, "p")))
			}
		}()
	}
	if err != nil {
		return fmt.Errorf("timed workload: %w", err)
	}
	if o.endpointDiagnostic != "" {
		if err := captureEndpointDiagnostic(ctx, o.endpointDiagnostic, filepath.Join(runDir, "p"), httpBase, r.RunID, r.TimedFinished); err != nil {
			return fmt.Errorf("endpoint diagnostic: %w", err)
		}
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
	var storage *storageDiagnostic
	storageWriteAttempted := false
	if o.storageDiagnostic != "" {
		storage = newStorageDiagnostic(filepath.Join(runDir, "p"), r)
		// Retain partial observations on a walk or later metric failure before
		// cleanup stops Alpha. Failed artifacts are rejected by the validator.
		defer func() {
			if !storageWriteAttempted {
				storage.Error = fmt.Sprint(err)
				err = errors.Join(err, writeStorageDiagnostic(o.storageDiagnostic, storage, r))
			}
		}()
	}
	diskLogical, diskAllocated, err := diskUsageObserved(filepath.Join(runDir, "p"), storage)
	if err != nil {
		return fmt.Errorf("collect posting disk usage: %w", err)
	}
	promAfter, promErr := prometheusObserved(httpBase+"/debug/prometheus_metrics", afterProm)
	statusAfter, storeAfter, err := storeStatusObserved(httpBase, afterStore)
	if diagnostic != nil && promErr != nil {
		return fmt.Errorf("operation diagnostic after Prometheus boundary: %w", promErr)
	}
	if err != nil {
		return fmt.Errorf("collect posting-store status: %w", err)
	}
	r.Metrics = metrics(o.backend, cpuAfter-cpuBefore, hwm, diskLogical, diskAllocated, promBefore, promAfter, storeBefore, storeAfter)
	if diagnostic != nil {
		diagnosticWriteAttempted = true
		if err := writeOperationDiagnostic(o.operationDiagnostic, diagnostic, filepath.Join(runDir, "p")); err != nil {
			return fmt.Errorf("operation diagnostic: %w", err)
		}
	}
	if storage != nil {
		storageWriteAttempted = true
		if err := writeStorageDiagnostic(o.storageDiagnostic, storage, r); err != nil {
			return fmt.Errorf("storage diagnostic: %w", err)
		}
	}
	r.Validation.BackendObserved = statusAfter["backend"]
	r.Validation.DurabilityObserved = statusAfter["profile"]
	r.Validation.UnsupportedOK = unsupportedOK(o.backend, statusAfter)
	if err := validateSchema(ctx, dg); err != nil {
		return fmt.Errorf("schema validation: %w", err)
	}
	r.Validation.SchemaOK = true
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
	waitCtx, cancelWait := context.WithTimeout(ctx, timeout)
	defer cancelWait()
	for {
		probeCtx, cancelProbe := context.WithTimeout(waitCtx, 2*time.Second)
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
		if err != nil {
			cancelProbe()
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		var probeErr error
		ready := false
		if err == nil {
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				probeErr = fmt.Errorf("drain readiness response: %w", err)
			} else if err := resp.Body.Close(); err != nil {
				probeErr = fmt.Errorf("close readiness response: %w", err)
			} else {
				ready = resp.StatusCode == http.StatusOK
			}
			if probeErr != nil {
				_ = resp.Body.Close()
			}
		}
		probeTimedOut := probeCtx.Err() != nil
		cancelProbe()
		if ready {
			return nil
		}
		if probeErr != nil && !probeTimedOut {
			return probeErr
		}
		select {
		case <-waitCtx.Done():
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("readiness timeout after %s", timeout)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
func client(addr string) (*dgo.Dgraph, error) {
	return dgo.NewClient(addr, dgo.WithGrpcOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
}

const dgraphRPCTimeout = 30 * time.Second

func withDgraphRPCTimeout[T any](ctx context.Context, timeout time.Duration, call func(context.Context) (T, error)) (T, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return call(rpcCtx)
}

func dgraphAlter(ctx context.Context, dg *dgo.Dgraph, operation *api.Operation) error {
	_, err := withDgraphRPCTimeout(ctx, dgraphRPCTimeout, func(rpcCtx context.Context) (struct{}, error) {
		return struct{}{}, dg.Alter(rpcCtx, operation)
	})
	return err
}

func dgraphMutate(ctx context.Context, dg *dgo.Dgraph, mutation *api.Mutation) (*api.Response, error) {
	return withDgraphRPCTimeout(ctx, dgraphRPCTimeout, func(rpcCtx context.Context) (*api.Response, error) {
		return dg.NewTxn().Mutate(rpcCtx, mutation)
	})
}

func dgraphQuery(ctx context.Context, dg *dgo.Dgraph, query string) (*api.Response, error) {
	return withDgraphRPCTimeout(ctx, dgraphRPCTimeout, func(rpcCtx context.Context) (*api.Response, error) {
		return dg.NewReadOnlyTxn().Query(rpcCtx, query)
	})
}

func dgraphQueryWithVars(ctx context.Context, dg *dgo.Dgraph, query string, vars map[string]string) (*api.Response, error) {
	return withDgraphRPCTimeout(ctx, dgraphRPCTimeout, func(rpcCtx context.Context) (*api.Response, error) {
		return dg.NewReadOnlyTxn().QueryWithVars(rpcCtx, query, vars)
	})
}

func setup(ctx context.Context, dg *dgo.Dgraph, n int) (map[string]expectedNode, error) {
	if err := dgraphAlter(ctx, dg, &api.Operation{Schema: "bench.value: string .\nbench.next: uid .\n"}); err != nil {
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
		resp, err := dgraphMutate(ctx, dg, &api.Mutation{SetNquads: []byte(b.String()), CommitNow: true})
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
	if _, err := dgraphMutate(ctx, dg, &api.Mutation{SetNquads: []byte(edges.String()), CommitNow: true}); err != nil {
		return nil, err
	}
	for i, uid := range ids {
		expected[uid] = expectedNode{Value: value(i + 1), NextValue: value((i+1)%len(ids) + 1)}
	}
	return expected, nil
}

func exercise(ctx context.Context, dg *dgo.Dgraph, dataset map[string]expectedNode, ops, concurrency int, seed int64, writeBase int) ([]float64, map[string]expectedNode, error) {
	return exerciseWithOperations(ctx, dg, dataset, ops, concurrency, seed, writeBase, nil)
}

func exerciseWithOperations(ctx context.Context, dg *dgo.Dgraph, dataset map[string]expectedNode, ops, concurrency int, seed int64, writeBase int, rows []operationRow) ([]float64, map[string]expectedNode, error) {
	if rows != nil && len(rows) != ops {
		return nil, nil, errors.New("operation diagnostic row count differs from workload")
	}
	exerciseCtx, cancelExercise := context.WithCancel(ctx)
	defer cancelExercise()
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
				if rows != nil {
					name := "write"
					if kind < 60 {
						name = "point_read"
					} else if kind < 80 {
						name = "one_hop_read"
					}
					rows[i] = operationRow{Index: i, Worker: worker, Kind: name, StartedAt: start, Outcome: "ok"}
				}
				uid := uids[(int(seed)+i*17)%len(uids)]
				var err error
				if kind < 60 {
					var resp *api.Response
					resp, err = dgraphQueryWithVars(exerciseCtx, dg, "query q($u: string) { q(func: uid($u)) { uid bench.value } }", map[string]string{"$u": uid})
					if rows != nil {
						observeOperationRPC(&rows[i], resp, err)
					}
					if err == nil {
						err = validateReadResponse(resp.Json, uid, dataset[uid], false)
					}
				} else if kind < 80 {
					var resp *api.Response
					resp, err = dgraphQueryWithVars(exerciseCtx, dg, "query q($u: string) { q(func: uid($u)) { uid bench.value bench.next { uid bench.value } } }", map[string]string{"$u": uid})
					if rows != nil {
						observeOperationRPC(&rows[i], resp, err)
					}
					if err == nil {
						err = validateReadResponse(resp.Json, uid, dataset[uid], true)
					}
				} else {
					writeID := writeBase + i
					resp, mutErr := dgraphMutate(exerciseCtx, dg, &api.Mutation{SetNquads: []byte(fmt.Sprintf("_:w <bench.value> %q .", value(writeID))), CommitNow: true})
					err = mutErr
					if rows != nil {
						observeOperationRPC(&rows[i], resp, err)
					}
					if err == nil {
						elapsed := time.Since(start)
						if rows != nil {
							finishOperation(&rows[i], elapsed, nil)
						}
						out <- sample{ms: float64(elapsed.Microseconds()) / 1000, uid: resp.Uids["w"], value: value(writeID), write: true}
						continue
					}
				}
				elapsed := time.Since(start)
				if rows != nil {
					finishOperation(&rows[i], elapsed, err)
				}
				out <- sample{ms: float64(elapsed.Microseconds()) / 1000, err: err}
			}
		}(worker)
	}
	go func() { wg.Wait(); close(out) }()
	values := make([]float64, 0, ops)
	writes := map[string]expectedNode{}
	var workloadErr error
	for s := range out {
		if s.err != nil {
			cancelExercise()
			if rows == nil {
				return nil, nil, s.err
			}
			if workloadErr == nil {
				workloadErr = s.err
			}
			continue
		}
		if s.write {
			if err := recordExpectedWrite(writes, s.uid, s.value); err != nil {
				if rows == nil {
					return nil, nil, err
				}
				cancelExercise()
				if workloadErr == nil {
					workloadErr = err
				}
				continue
			}
		}
		values = append(values, s.ms)
	}
	if workloadErr != nil {
		return nil, nil, workloadErr
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
	resp, err := dgraphQuery(ctx, dg, postingValidationQuery(len(expected)))
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
	if expected.NextValue == "" {
		if len(node.Next) != 0 {
			return "", fmt.Errorf("unexpected posting edge for %s", node.UID)
		}
		return node.Value + "\x00", nil
	}
	if len(node.Next) != 1 {
		return "", fmt.Errorf("posting edge count mismatch for %s", node.UID)
	}
	if node.Next[0].Value != expected.NextValue {
		return "", fmt.Errorf("posting edge mismatch for source value %q", node.Value)
	}
	return node.Value + "\x00" + node.Next[0].Value, nil
}
func validateSchema(ctx context.Context, dg *dgo.Dgraph) error {
	resp, err := dgraphQuery(ctx, dg, "schema(pred: [bench.value, bench.next]) { predicate type }")
	if err != nil {
		return err
	}
	return validateSchemaJSON(resp.Json)
}

func validateSchemaJSON(raw []byte) error {
	var response struct {
		Schema []struct {
			Predicate string `json:"predicate"`
			Type      string `json:"type"`
		} `json:"schema"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return fmt.Errorf("decode schema response: %w", err)
	}
	want := map[string]string{"bench.value": "string", "bench.next": "uid"}
	seen := make(map[string]struct{}, len(want))
	for _, entry := range response.Schema {
		wantType, ok := want[entry.Predicate]
		if !ok {
			return fmt.Errorf("unexpected schema predicate %q", entry.Predicate)
		}
		if _, duplicate := seen[entry.Predicate]; duplicate {
			return fmt.Errorf("duplicate schema predicate %q", entry.Predicate)
		}
		if entry.Type != wantType {
			return fmt.Errorf("schema predicate %q has type %q, want %q", entry.Predicate, entry.Type, wantType)
		}
		seen[entry.Predicate] = struct{}{}
	}
	for predicate := range want {
		if _, ok := seen[predicate]; !ok {
			return fmt.Errorf("missing schema predicate %q", predicate)
		}
	}
	return nil
}

func fetch(url string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
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
	b, err := fetch(fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", base, seconds), time.Duration(seconds)*time.Second+30*time.Second)
	if err != nil {
		return err
	}
	return livebench.WriteImmutableBytes(path, b)
}
func storeStatus(base string) (map[string]string, map[string]float64, error) {
	return storeStatusObserved(base, nil)
}
func storeStatusObserved(base string, observation *operationRequest) (map[string]string, map[string]float64, error) {
	if observation != nil {
		observation.StartedAt = time.Now().UTC()
	}
	b, err := fetch(base+"/debug/store", 30*time.Second)
	if observation != nil {
		observation.FinishedAt = time.Now().UTC()
	}
	if err != nil {
		return nil, nil, err
	}
	s := string(b)
	if observation != nil {
		observation.Body = s
	}
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
	return prometheusObserved(url, nil)
}
func prometheusObserved(url string, observation *operationRequest) (map[string]float64, error) {
	if observation != nil {
		observation.StartedAt = time.Now().UTC()
	}
	b, err := fetch(url, 30*time.Second)
	if observation != nil {
		observation.FinishedAt = time.Now().UTC()
	}
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	raw := string(b)
	if observation != nil {
		observation.Body = raw
	}
	sc := bufio.NewScanner(strings.NewReader(raw))
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
	writeReason := ""
	if backend == "treedb" {
		writeBytes, writeAvail = delta(sa, sb, "treedb.command_wal.public_batch.set.bytes_total")
		writeSource = "TreeDB /debug/store public batch set bytes"
		pointAppends, pointAppendOK := delta(sa, sb, "treedb.command_wal.append.point.count_total")
		switch {
		case !pointAppendOK:
			writeAvail = false
			writeReason = "TreeDB point-append coverage counter unavailable"
		case pointAppends > 0:
			writeAvail = false
			writeReason = "TreeDB direct-point appends bypass the public-batch logical-byte counter"
		}
	}
	m["write_bytes"] = livebench.Metric{Available: writeAvail, Value: writeBytes, Unit: "bytes", Source: writeSource, Reason: writeReason}
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
	addTreeDBDiagnostics(m, backend, sb, sa)
	return m
}

type treeDBDiagnostic struct {
	metric string
	stat   string
	gauge  bool
}

var treeDBDiagnostics = []treeDBDiagnostic{
	{metric: "treedb_public_batch_write_calls", stat: "treedb.public.batch.write.calls_total"},
	{metric: "treedb_public_batch_write_sync_calls", stat: "treedb.public.batch.write_sync.calls_total"},
	{metric: "treedb_command_wal_append_point_calls", stat: "treedb.command_wal.append.point.count_total"},
	{metric: "treedb_group_commit_groups", stat: "treedb.command_wal.group_commit.groups_total"},
	{metric: "treedb_group_commit_commits", stat: "treedb.command_wal.group_commit.commits_total"},
	{metric: "treedb_group_commit_participants", stat: "treedb.command_wal.group_commit.participants_total"},
	{metric: "treedb_group_commit_syncs", stat: "treedb.command_wal.group_commit.syncs_total"},
	{metric: "treedb_group_commit_group_size_max", stat: "treedb.command_wal.group_commit.group_size_max", gauge: true},
	{metric: "treedb_command_wal_file_syncs", stat: "treedb.command_wal.file_sync.calls_total"},
	{metric: "treedb_value_log_syncs", stat: "treedb.cache.value_log.sync.calls_total"},
	{metric: "treedb_value_log_file_syncs", stat: "treedb.cache.value_log.file_sync.calls_total"},
	{metric: "treedb_point_successor_calls", stat: "treedb.cache.point_successor.calls_total"},
	{metric: "treedb_point_successor_sources", stat: "treedb.cache.point_successor.sources_total"},
	{metric: "treedb_point_successor_sources_max", stat: "treedb.cache.point_successor.sources_max", gauge: true},
	{metric: "treedb_iterator_snapshot_rotations", stat: "treedb.cache.iterator.snapshot_rotations_total"},
	{metric: "treedb_leaf_log_segment_rotations", stat: "treedb.cache.leaf_log_lanes.segment_rotations_total"},
}

func addTreeDBDiagnostics(out map[string]livebench.Metric, backend string, before, after map[string]float64) {
	for _, diagnostic := range treeDBDiagnostics {
		source := "TreeDB /debug/store timed-phase delta: " + diagnostic.stat
		if backend != "treedb" {
			out[diagnostic.metric] = availability(0, false, "count", source, "TreeDB-only diagnostic")
			continue
		}
		value, ok := delta(after, before, diagnostic.stat)
		if diagnostic.gauge {
			value, ok = after[diagnostic.stat]
			source = "TreeDB /debug/store process-lifetime high-water: " + diagnostic.stat
		}
		out[diagnostic.metric] = availability(value, ok, "count", source, "TreeDB diagnostic counter unavailable")
	}
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
	return diskUsageObserved(root, nil)
}

func diskUsageObserved(root string, observation *storageDiagnostic) (float64, float64, error) {
	return diskUsageWalk(root, observation, filepath.Walk)
}

// The optional rows use the same FileInfo as the native sums, never another stat
// or walk. Background writers can still change files during this sequential walk.
func diskUsageWalk(root string, observation *storageDiagnostic, walk func(string, filepath.WalkFunc) error) (float64, float64, error) {
	var logical, allocated float64
	if observation != nil {
		observation.WalkStartedAt = time.Now().UTC()
	}
	err := walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			size := info.Size()
			logical += float64(size)
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.New("filesystem stat does not expose allocated blocks")
			}
			allocated += float64(st.Blocks * 512)
			if observation != nil {
				observation.RegularFiles++
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				observation.Files = append(observation.Files, endpointFile{Path: filepath.ToSlash(rel),
					LogicalBytes: size, AllocatedBytes: st.Blocks * 512, StatBlocks: st.Blocks, ModifiedAt: info.ModTime().UTC()})
			}
		}
		return nil
	})
	if observation != nil {
		observation.WalkFinishedAt = time.Now().UTC()
		observation.WalkComplete = err == nil
		observation.LogicalBytes, observation.AllocatedBytes = logical, allocated
		if err != nil {
			observation.Error = err.Error()
		}
	}
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

const endpointQuiescence = 5 * time.Second

type endpointFile struct {
	Path           string    `json:"path"`
	LogicalBytes   int64     `json:"logical_bytes"`
	AllocatedBytes int64     `json:"allocated_bytes"`
	StatBlocks     int64     `json:"stat_blocks"`
	ModifiedAt     time.Time `json:"modified_at"`
}

type endpointObservation struct {
	Boundary        string             `json:"boundary"`
	TargetAt        time.Time          `json:"target_at"`
	StartedAt       time.Time          `json:"started_at"`
	FilesFinishedAt time.Time          `json:"files_finished_at"`
	StatusStartedAt time.Time          `json:"status_started_at"`
	FinishedAt      time.Time          `json:"finished_at"`
	StartDelayNS    int64              `json:"start_delay_ns"`
	EndDelayNS      int64              `json:"end_delay_ns"`
	LogicalBytes    int64              `json:"logical_bytes"`
	AllocatedBytes  int64              `json:"allocated_bytes"`
	Files           []endpointFile     `json:"files"`
	Status          map[string]string  `json:"status"`
	Counters        map[string]float64 `json:"counters"`
}

type endpointDiagnostic struct {
	SchemaVersion     int                   `json:"schema_version"`
	DiagnosticOnly    bool                  `json:"diagnostic_only"`
	RunID             string                `json:"run_id"`
	PostingDir        string                `json:"posting_dir"`
	TimedFinished     time.Time             `json:"timed_finished"`
	QuiescenceSeconds int                   `json:"quiescence_seconds"`
	Observations      []endpointObservation `json:"observations"`
}

// Endpoint observations are sequential metadata/status reads, not an atomic
// filesystem snapshot. Background publication may proceed during either read.
func observeEndpoint(root, httpBase, boundary string, target time.Time) (endpointObservation, error) {
	o := endpointObservation{Boundary: boundary, TargetAt: target, StartedAt: time.Now().UTC(), Files: []endpointFile{}}
	o.StartDelayNS = o.StartedAt.Sub(target).Nanoseconds()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("filesystem stat does not expose allocated blocks")
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		f := endpointFile{Path: filepath.ToSlash(rel), LogicalBytes: info.Size(),
			AllocatedBytes: st.Blocks * 512, StatBlocks: st.Blocks, ModifiedAt: info.ModTime().UTC()}
		o.Files = append(o.Files, f)
		o.LogicalBytes += f.LogicalBytes
		o.AllocatedBytes += f.AllocatedBytes
		return nil
	})
	o.FilesFinishedAt = time.Now().UTC()
	if err != nil {
		return o, fmt.Errorf("posting file metadata: %w", err)
	}
	o.StatusStartedAt = time.Now().UTC()
	o.Status, o.Counters, err = storeStatus(httpBase)
	o.FinishedAt = time.Now().UTC()
	o.EndDelayNS = o.FinishedAt.Sub(target).Nanoseconds()
	if err != nil {
		return o, fmt.Errorf("posting-store status: %w", err)
	}
	if o.Status["backend"] != "treedb" || len(o.Counters) == 0 {
		return o, errors.New("missing TreeDB status or numeric counters")
	}
	return o, nil
}

func captureEndpointDiagnostic(ctx context.Context, path, root, httpBase, runID string, finished time.Time) error {
	if _, err := diagnosticOutputPath(path, root, nil); err != nil {
		return err
	}
	d := endpointDiagnostic{SchemaVersion: 1, DiagnosticOnly: true, RunID: runID, PostingDir: root,
		TimedFinished: finished, QuiescenceSeconds: int(endpointQuiescence / time.Second)}
	for i, boundary := range []string{"timed_end", "quiescent"} {
		target := finished.Add(time.Duration(i) * endpointQuiescence)
		if delay := time.Until(target); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		o, err := observeEndpoint(root, httpBase, boundary, target)
		if err != nil {
			return fmt.Errorf("%s: %w", boundary, err)
		}
		d.Observations = append(d.Observations, o)
	}
	output, err := diagnosticOutputPath(path, root, nil)
	if err != nil {
		return err
	}
	return livebench.WriteImmutable(output, d)
}

// Validate before starting processes so a diagnostic cannot overwrite a native
// result/profile or add its own bytes to the observed posting directory.
func validateEndpointDiagnostic(o options) error {
	if o.endpointDiagnostic == "" {
		return nil
	}
	if (o.cpuProfile == "" && o.operationDiagnostic == "") || o.backend != "treedb" {
		return errors.New("--endpoint-diagnostic requires --backend treedb and --cpu-profile or --operation-diagnostic; diagnostic overhead is excluded from acceptance")
	}
	_, err := diagnosticOutputPath(o.endpointDiagnostic, filepath.Join(o.artifactDir, "cluster", "p"),
		[]string{o.cpuProfile, o.operationDiagnostic, o.storageDiagnostic})
	return err
}

type operationServerLatency struct {
	Available         bool   `json:"available"`
	TotalNS           uint64 `json:"total_ns"`
	AssignTimestampNS uint64 `json:"assign_timestamp_ns"`
	ParsingNS         uint64 `json:"parsing_ns"`
	ProcessingNS      uint64 `json:"processing_ns"`
	EncodingNS        uint64 `json:"encoding_ns"`
}

type operationRow struct {
	Index         int                    `json:"index"`
	Worker        int                    `json:"worker"`
	Kind          string                 `json:"kind"`
	StartedAt     time.Time              `json:"started_at"`
	RPCFinishedAt time.Time              `json:"rpc_finished_at"`
	FinishedAt    time.Time              `json:"finished_at"`
	RPCNS         int64                  `json:"rpc_ns"`
	WallNS        int64                  `json:"wall_ns"`
	ServerLatency operationServerLatency `json:"server_latency"`
	Outcome       string                 `json:"outcome"`
}

type operationRequest struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Body       string    `json:"body"`
}

type operationBoundary struct {
	Boundary   string           `json:"boundary"`
	Store      operationRequest `json:"store"`
	Prometheus operationRequest `json:"prometheus"`
}

type operationDiagnostic struct {
	SchemaVersion      int               `json:"schema_version"`
	DiagnosticOnly     bool              `json:"diagnostic_only"`
	RunID              string            `json:"run_id"`
	Backend            string            `json:"backend"`
	DurabilityClass    string            `json:"durability_class"`
	Seed               int64             `json:"seed"`
	Concurrency        int               `json:"concurrency"`
	ExpectedOperations int               `json:"expected_operations"`
	WorkloadSucceeded  bool              `json:"workload_succeeded"`
	TimedStarted       time.Time         `json:"timed_started"`
	TimedFinished      time.Time         `json:"timed_finished"`
	WriteStartedAt     time.Time         `json:"write_started_at"`
	Before             operationBoundary `json:"before"`
	After              operationBoundary `json:"after"`
	Rows               []operationRow    `json:"rows"`
}

// Each worker exclusively owns its deterministic indices. These rows never
// retain the response, key or payload, and are inspected only after worker drain.
func observeOperationRPC(row *operationRow, resp *api.Response, err error) {
	row.RPCFinishedAt = time.Now()
	row.RPCNS = row.RPCFinishedAt.Sub(row.StartedAt).Nanoseconds()
	if err != nil {
		row.Outcome = "rpc_error"
	}
	if resp != nil && resp.Latency != nil {
		l := resp.Latency
		row.ServerLatency = operationServerLatency{Available: true, TotalNS: l.TotalNs,
			AssignTimestampNS: l.AssignTimestampNs, ParsingNS: l.ParsingNs,
			ProcessingNS: l.ProcessingNs, EncodingNS: l.EncodingNs}
	}
}

func finishOperation(row *operationRow, elapsed time.Duration, err error) {
	row.WallNS = elapsed.Nanoseconds()
	row.FinishedAt = row.StartedAt.Add(elapsed)
	if err != nil && row.Outcome == "ok" {
		row.Outcome = "validation_error"
	}
}

// Mark all diagnostics at run setup; CPU profiling alone keeps its existing behavior.
func markDiagnostics(r *livebench.Result, o options) {
	if o.storageDiagnostic != "" {
		r.Context.Contaminants = append(r.Context.Contaminants, storageDiagnosticReason)
	}
	if o.operationDiagnostic != "" {
		r.Context.Contaminants = append(r.Context.Contaminants, "operation diagnostic overhead; excluded from performance acceptance")
	}
	if o.endpointDiagnostic != "" {
		r.Context.Contaminants = append(r.Context.Contaminants, "endpoint diagnostic observations and passive wait; excluded from performance acceptance")
	}
	if len(r.Context.Contaminants) != 0 {
		r.Context.Excluded = true
		r.Context.ExclusionReason = strings.Join(r.Context.Contaminants, "; ")
	}
}

// Serialization is outside the measured operation and workload boundaries.
// A failed workload is retained only after cancellation and all workers drain.
func writeOperationDiagnostic(path string, d *operationDiagnostic, postings string) error {
	output, err := diagnosticOutputPath(path, postings, nil)
	if err != nil {
		return err
	}
	if len(d.Rows) != d.ExpectedOperations || d.Concurrency < 1 || !d.TimedFinished.After(d.TimedStarted) {
		return errors.New("incomplete operation diagnostic")
	}
	for i, row := range d.Rows {
		kind := (int(d.Seed) + i*37) % 100
		name := "write"
		if kind < 60 {
			name = "point_read"
		} else if kind < 80 {
			name = "one_hop_read"
		}
		if row.Kind != name || (row.Outcome != "ok" && row.Outcome != "rpc_error" && row.Outcome != "validation_error") ||
			row.RPCFinishedAt.Sub(row.StartedAt).Nanoseconds() != row.RPCNS ||
			row.FinishedAt.Sub(row.StartedAt).Nanoseconds() != row.WallNS {
			return fmt.Errorf("invalid operation diagnostic identity or duration %d", i)
		}
		if row.Index != i || row.Worker != i%d.Concurrency || row.StartedAt.Before(d.TimedStarted) ||
			row.RPCFinishedAt.Before(row.StartedAt) || row.FinishedAt.Before(row.RPCFinishedAt) ||
			row.FinishedAt.After(d.TimedFinished) || row.RPCNS < 0 || row.WallNS < row.RPCNS {
			return fmt.Errorf("invalid operation diagnostic row %d", i)
		}
	}
	d.WriteStartedAt = time.Now().UTC()
	if d.WriteStartedAt.Before(d.TimedFinished) {
		return errors.New("operation diagnostic cannot serialize before timed finish")
	}
	return livebench.WriteImmutable(output, d)
}

// Resolve existing ancestors, then append the not-yet-created suffix. Both the
// posting root and outputs may have missing components at process preflight.
// Dangling/cyclic aliases and inaccessible ancestors fail closed.
func resolveDiagnosticPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	ancestor := absolute
	var missing []string
	for {
		_, err := os.Lstat(ancestor)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		missing = append(missing, filepath.Base(ancestor))
		ancestor = parent
	}
	resolved, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		resolved = filepath.Join(resolved, missing[i])
	}
	return resolved, nil
}

// Return the resolved write destination, not the alias supplied by the caller.
// Recheck at each observer write. O_EXCL still protects immutable files; this is
// not a defense against adversarial concurrent replacement of parent directories.
func diagnosticOutputPath(path, postings string, reserved []string) (string, error) {
	output, err := resolveDiagnosticPath(path)
	if err != nil {
		return "", err
	}
	root, err := resolveDiagnosticPath(postings)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, output)
	if err != nil {
		return "", err
	}
	if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("diagnostic must be outside the posting directory")
	}
	cluster, err := resolveDiagnosticPath(filepath.Dir(postings))
	if err != nil {
		return "", err
	}
	// Sidecars cannot occupy any live cluster state (posting, Alpha WAL or
	// Zero WAL), nor a directory that the run must create above that state.
	for _, pair := range [][2]string{{cluster, output}, {output, cluster}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return "", err
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", errors.New("diagnostic must not be inside or above the run cluster directory")
		}
	}
	artifactDir := filepath.Dir(filepath.Dir(postings))
	reserved = append(append([]string{}, reserved...), filepath.Join(artifactDir, "result.json"),
		filepath.Join(artifactDir, "zero.log"), filepath.Join(artifactDir, "alpha.log"), filepath.Join(artifactDir, "alpha-restart.log"))
	for _, name := range reserved {
		if name == "" {
			continue
		}
		canonical, err := resolveDiagnosticPath(name)
		if err != nil {
			return "", err
		}
		if canonical == output {
			return "", errors.New("diagnostic requires a separate sidecar filename")
		}
	}
	if _, err := os.Lstat(output); err == nil {
		return "", &os.PathError{Op: "create diagnostic", Path: output, Err: os.ErrExist}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return output, nil
}

func validateOperationDiagnostic(o options) error {
	if o.operationDiagnostic == "" {
		return nil
	}
	_, err := diagnosticOutputPath(o.operationDiagnostic, filepath.Join(o.artifactDir, "cluster", "p"),
		[]string{o.cpuProfile, o.endpointDiagnostic, o.storageDiagnostic})
	return err
}

const storageDiagnosticReason = "same-walk storage diagnostic overhead; excluded from performance acceptance"
const storageDiagnosticBoundary = "native_posting_disk_usage_prevalidation"

type storageDiagnostic struct {
	SchemaVersion   int            `json:"schema_version"`
	DiagnosticOnly  bool           `json:"diagnostic_only"`
	RunID           string         `json:"run_id"`
	Backend         string         `json:"backend"`
	DurabilityClass string         `json:"durability_class"`
	PostingDir      string         `json:"posting_dir"`
	Boundary        string         `json:"boundary"`
	TimedFinished   time.Time      `json:"timed_finished"`
	WalkStartedAt   time.Time      `json:"walk_started_at"`
	WalkFinishedAt  time.Time      `json:"walk_finished_at"`
	WriteStartedAt  time.Time      `json:"write_started_at"`
	WalkComplete    bool           `json:"walk_complete"`
	Error           string         `json:"error"`
	LogicalBytes    float64        `json:"logical_bytes"`
	AllocatedBytes  float64        `json:"allocated_bytes"`
	RegularFiles    int            `json:"regular_files"`
	Files           []endpointFile `json:"files"`
}

func newStorageDiagnostic(root string, r livebench.Result) *storageDiagnostic {
	return &storageDiagnostic{SchemaVersion: 1, DiagnosticOnly: true, RunID: r.RunID, Backend: r.Config.Backend,
		DurabilityClass: r.Config.DurabilityClass, PostingDir: root, Boundary: storageDiagnosticBoundary,
		TimedFinished: r.TimedFinished, Files: []endpointFile{}}
}

// Validation is against the native result from this run, not a new filesystem
// observation. Incomplete raw artifacts are retained but cannot validate.
func validateStorageDiagnostic(d *storageDiagnostic, r livebench.Result) error {
	if d == nil || d.SchemaVersion != 1 || !d.DiagnosticOnly || !d.WalkComplete || d.Error != "" || len(d.Files) == 0 {
		return errors.New("incomplete storage diagnostic")
	}
	if d.RegularFiles != len(d.Files) {
		return errors.New("storage diagnostic file count mismatch")
	}
	if d.RunID == "" || d.RunID != r.RunID || d.Backend != r.Config.Backend || d.DurabilityClass != r.Config.DurabilityClass ||
		(d.Backend != "treedb" && d.Backend != "badger") || (d.DurabilityClass != "relaxed" && d.DurabilityClass != "durable") ||
		r.Context.RawPath == "" || d.PostingDir != filepath.Join(filepath.Dir(r.Context.RawPath), "cluster", "p") ||
		d.Boundary != storageDiagnosticBoundary || !r.Context.Excluded || !strings.Contains(r.Context.ExclusionReason, storageDiagnosticReason) {
		return errors.New("storage diagnostic identity or boundary mismatch")
	}
	if d.TimedFinished.IsZero() || !d.TimedFinished.Equal(r.TimedFinished) || d.WalkStartedAt.IsZero() ||
		d.WalkStartedAt.Before(d.TimedFinished) || d.WalkFinishedAt.Before(d.WalkStartedAt) ||
		d.WriteStartedAt.IsZero() || d.WriteStartedAt.Before(d.WalkFinishedAt) {
		return errors.New("storage diagnostic walk envelope mismatch")
	}
	seen := make(map[string]bool, len(d.Files))
	var logical, allocated float64
	const maxExactBytes = 1<<53 - 1
	for _, f := range d.Files {
		if f.Path == "" || filepath.IsAbs(f.Path) || filepath.ToSlash(filepath.Clean(f.Path)) != f.Path || f.Path == "." ||
			f.Path == ".." || strings.HasPrefix(f.Path, "../") || strings.ContainsRune(f.Path, 0) || seen[f.Path] ||
			f.LogicalBytes < 0 || f.LogicalBytes > maxExactBytes || f.StatBlocks < 0 || f.StatBlocks > maxExactBytes/512 ||
			f.AllocatedBytes != f.StatBlocks*512 || f.ModifiedAt.IsZero() {
			return errors.New("invalid storage diagnostic file row")
		}
		seen[f.Path] = true
		logical += float64(f.LogicalBytes)
		allocated += float64(f.AllocatedBytes)
		if logical > maxExactBytes || allocated > maxExactBytes {
			return errors.New("storage diagnostic totals exceed exact native byte representation")
		}
	}
	if logical != d.LogicalBytes || allocated != d.AllocatedBytes {
		return errors.New("storage diagnostic file sum mismatch")
	}
	for name, want := range map[string]float64{"disk_logical_bytes": logical, "disk_allocated_bytes": allocated} {
		metric, ok := r.Metrics[name]
		source := "posting directory walk"
		if name == "disk_allocated_bytes" {
			source = "posting directory stat blocks"
		}
		if !ok || !metric.Available || metric.Unit != "bytes" || metric.Source != source || metric.Value != want {
			return fmt.Errorf("storage diagnostic native metric mismatch: %s", name)
		}
	}
	return nil
}

func writeStorageDiagnostic(path string, d *storageDiagnostic, r livebench.Result) error {
	if d == nil {
		return errors.New("missing storage diagnostic")
	}
	output, err := diagnosticOutputPath(path, filepath.Join(filepath.Dir(r.Context.RawPath), "cluster", "p"), nil)
	if err != nil {
		return err
	}
	d.WriteStartedAt = time.Now().UTC()
	validationErr := validateStorageDiagnostic(d, r)
	// Preserve even failed/partial raw observations with their error. Returning
	// validationErr prevents a native success result from being published.
	return errors.Join(validationErr, livebench.WriteImmutable(output, d))
}

func validateStorageDiagnosticOption(o options) error {
	if o.storageDiagnostic == "" {
		return nil
	}
	if o.cpuProfile != "" || o.endpointDiagnostic != "" {
		return errors.New("--storage-diagnostic excludes --cpu-profile and --endpoint-diagnostic waits; use the original native disk boundary")
	}
	_, err := diagnosticOutputPath(o.storageDiagnostic, filepath.Join(o.artifactDir, "cluster", "p"),
		[]string{o.cpuProfile, o.endpointDiagnostic, o.operationDiagnostic})
	return err
}
