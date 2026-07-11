/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/golang/glog"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/raftwal"
	dgraphtreedb "github.com/dgraph-io/dgraph/v25/worker/treedb"
	"github.com/dgraph-io/dgraph/v25/x"
	"github.com/dgraph-io/ristretto/v2/z"
	td "github.com/snissn/gomap/TreeDB"
)

const (
	// NOTE: SuperFlag defaults must include every possible option that can be used. This way, if a
	//       user makes a typo while defining a SuperFlag we can catch it and fail right away rather
	//       than fail during runtime while trying to retrieve an option that isn't there.
	//
	//       For easy readability, keep the options without default values (if any) at the end of
	//       the *Defaults string. Also, since these strings are printed in --help text, avoid line
	//       breaks.
	AuditDefaults  = `compress=false; days=10; size=100; dir=; output=; encrypt-file=;`
	BadgerDefaults = `compression=snappy; numgoroutines=8;`
	RaftDefaults   = `learner=false; snapshot-after-entries=10000; ` +
		`snapshot-after-duration=30m; pending-proposals=256; idx=; group=;`
	SecurityDefaults = `token=; whitelist=;`
	CDCDefaults      = `file=; kafka=; sasl_user=; sasl_password=; ca_cert=; client_cert=; ` +
		`client_key=; sasl-mechanism=PLAIN; tls=false;`
	LimitDefaults = `mutations=allow; query-edge=1000000; normalize-node=10000; ` +
		`mutations-nquad=1000000; disallow-drop=false; query-timeout=0ms; txn-abort-after=5m; ` +
		`max-retries=10; max-pending-queries=10000; shared-instance=false; type-filter-uid-limit=10`
	ZeroLimitsDefaults = `uid-lease=0; refill-interval=30s; disable-admin-http=false;`
	GraphQLDefaults    = `introspection=true; debug=false; extensions=true; poll-interval=1s; ` +
		`lambda-url=;`
	CacheDefaults        = `size-mb=4096; percentage=40,40,20; remove-on-update=false`
	FeatureFlagsDefaults = `normalize-compatibility-mode=; enable-detailed-metrics=false; log-slow-query-threshold=0`
)

// ServerState holds the state of the Dgraph server.
type ServerState struct {
	FinishCh chan struct{} // channel to wait for all pending reqs to finish.

	Pstore *badger.DB
	// PostingStore is the single posting-store handle used by the restricted
	// runtime. Pstore remains non-nil only for the production Badger backend.
	PostingStore posting.Store
	TreeDBStore  *posting.TreeDBStore
	CommitEvents *posting.CommitEventBus
	WALstore     *raftwal.DiskStorage
	gcCloser     *z.Closer // closer for valueLogGC

	needTs chan tsReq
}

// State is the instance of ServerState used by the current server.
var State ServerState

// InitServerState initializes this server's state.
func InitServerState() {
	Config.validate()

	State.FinishCh = make(chan struct{})
	State.needTs = make(chan tsReq, 100)

	State.InitStorage()
	go State.fillTimestampRequests()

	groupId, err := x.ReadGroupIdFile(Config.PostingDir)
	if err != nil {
		glog.Warningf("Could not read %s file inside posting directory %s.", x.GroupIdFileName,
			Config.PostingDir)
	}
	x.WorkerConfig.ProposedGroupId = groupId
}

func setBadgerOptions(opt badger.Options) badger.Options {
	opt = opt.WithSyncWrites(false).
		WithLogger(&x.ToGlog{}).
		WithEncryptionKey(x.WorkerConfig.EncryptionKey)

	// Disable conflict detection in badger. Alpha runs in managed mode and
	// perform its own conflict detection so we don't need badger's conflict
	// detection. Using badger's conflict detection uses memory which can be
	// saved by disabling it.
	opt.DetectConflicts = false

	// Settings for the data directory.
	return opt
}

func (s *ServerState) InitStorage() {
	var err error

	if x.WorkerConfig.EncryptionKey != nil {
		glog.Infof("Encryption feature enabled.")
	}

	{
		// Write Ahead Log directory
		x.Checkf(os.MkdirAll(Config.WALDir, 0700), "Error while creating WAL dir.")
		s.WALstore, err = raftwal.InitEncrypted(Config.WALDir, x.WorkerConfig.EncryptionKey)
		x.Check(err)
	}
	{
		// Postings directory
		// All the writes to posting store should be synchronous. We use batched writers
		// for posting lists, so the cost of sync writes is amortized.
		x.Checkf(s.openPostingStore(), "Error while opening posting store")
	}
	// Temp directory
	x.Check(os.MkdirAll(x.WorkerConfig.TmpDir, 0700))

	closerCount := 1
	if s.Pstore != nil {
		closerCount = 3
	}
	s.gcCloser = z.NewCloser(closerCount)
	if s.Pstore != nil {
		go x.RunVlogGC(s.Pstore, s.gcCloser)
		// Commenting this out because Badger is doing its own cache checks.
		go x.MonitorCacheHealth(s.Pstore, s.gcCloser)
	}
	go x.MonitorDiskMetrics("postings_fs", Config.PostingDir, s.gcCloser)
}

func (s *ServerState) openPostingStore() error {
	if err := os.MkdirAll(Config.PostingDir, 0o700); err != nil {
		return err
	}
	backend, err := NormalizePostingStoreBackend(Config.PostingStoreBackend)
	if err != nil {
		return err
	}
	if err := ValidatePostingStoreSelection(backend, Config.PostingStoreTier,
		Config.PostingStoreDurability, x.WorkerConfig.EncryptionKey != nil,
		Config.PostingStoreEvents); err != nil {
		return err
	}
	// Validation accepts selector values case-insensitively. Persist the same
	// normalized value used for runtime profile selection and status reporting.
	Config.PostingStoreDurability = normalizePostingStoreDurability(Config.PostingStoreDurability)
	glog.Infof("%s; tier=%s; durability=%s; post_commit_events=%t",
		PostingStoreBackendStatus(backend), Config.PostingStoreTier,
		Config.PostingStoreDurability, Config.PostingStoreEvents)
	switch backend {
	case PostingStoreBackendBadger:
		opt := x.WorkerConfig.Badger.
			WithDir(Config.PostingDir).WithValueDir(Config.PostingDir).
			WithNumVersionsToKeep(math.MaxInt32).
			WithNamespaceOffset(x.NamespaceOffset)
		opt = setBadgerOptions(opt)
		key := opt.EncryptionKey
		opt.EncryptionKey = nil
		glog.Infof("Opening postings BadgerDB with options: %+v\n", opt)
		opt.EncryptionKey = key
		s.Pstore, err = badger.OpenManaged(opt)
		opt.EncryptionKey = nil
		if err != nil {
			return fmt.Errorf("open Badger posting store: %w", err)
		}
		s.PostingStore = posting.NewBadgerStore(s.Pstore)
	case PostingStoreBackendTreeDB:
		profile := td.ProfileCommandWALDurable
		mode := posting.TreeDBCommitDurable
		if Config.PostingStoreDurability == PostingStoreDurabilityRelaxed {
			profile = td.ProfileCommandWALRelaxed
			mode = posting.TreeDBCommitRelaxed
		}
		treeOpts, err := dgraphtreedb.ResolveOptions(dgraphtreedb.OpenOptions{
			Dir: Config.PostingDir, Profile: profile,
			RequireInMemory: x.WorkerConfig.Badger.InMemory,
		})
		if err != nil {
			return fmt.Errorf("resolve TreeDB posting store: %w", err)
		}
		s.TreeDBStore, err = posting.OpenTreeDBStore(treeOpts, mode)
		if err != nil {
			return fmt.Errorf("open TreeDB posting store: %w", err)
		}
		s.PostingStore = s.TreeDBStore
	}
	if Config.PostingStoreEvents {
		s.CommitEvents = posting.NewCommitEventBus(256)
		s.PostingStore = posting.WithCommitEvents(s.PostingStore, s.CommitEvents)
	}
	return nil
}

// Dispose stops and closes all the resources inside the server state.
func (s *ServerState) Dispose() {
	s.gcCloser.SignalAndWait()
	s.closePostingStore()
	if err := s.WALstore.Close(); err != nil {
		glog.Errorf("Error while closing WAL store: %v", err)
	}
}

func (s *ServerState) closePostingStore() {
	if s.TreeDBStore != nil {
		if err := s.TreeDBStore.Close(); err != nil {
			glog.Errorf("Error while closing TreeDB postings store: %v", err)
		}
	} else if s.Pstore != nil {
		if err := s.Pstore.Close(); err != nil {
			glog.Errorf("Error while closing postings store: %v", err)
		}
	}
	if s.CommitEvents != nil {
		// Store close establishes the admitted-commit completion boundary. Bus
		// close then cancels subscriber backpressure and closes subscriptions.
		s.CommitEvents.Close()
	}
}

// PostingStoreRuntimeStatus exposes the effective experimental runtime selection.
func (s *ServerState) PostingStoreRuntimeStatus() map[string]string {
	status := map[string]string{
		"backend": Config.PostingStoreBackend, "tier": Config.PostingStoreTier,
		"durability":         Config.PostingStoreDurability,
		"post_commit_events": fmt.Sprint(Config.PostingStoreEvents),
	}
	if s.TreeDBStore != nil {
		treeStatus := s.TreeDBStore.Status()
		status["profile"] = treeStatus.DurabilityMode
		status["durable_commits"] = fmt.Sprint(treeStatus.DurableCommits)
		status["closed"] = fmt.Sprint(treeStatus.Closed)
		status["unsupported"] = "backup,export,import,restore,encryption,in_memory,ttl,badger_subscribe,sort,count,inequality"
	}
	return status
}

// PostingStoreStats returns backend-native diagnostics without manufacturing
// unavailable Badger cache counters for TreeDB.
func (s *ServerState) PostingStoreStats() (map[string]string, error) {
	if s.TreeDBStore != nil {
		return s.TreeDBStore.Stats()
	}
	if s.Pstore == nil {
		return nil, fmt.Errorf("posting store is not open")
	}
	return map[string]string{"backend": PostingStoreBackendBadger}, nil
}

// RunPostingStoreGC triggers the selected backend's value-log maintenance.
func (s *ServerState) RunPostingStoreGC(ctx context.Context) error {
	if s.TreeDBStore != nil {
		_, err := s.TreeDBStore.ValueLogGC(ctx, td.ValueLogGCOptions{})
		return err
	}
	if s.Pstore == nil {
		return fmt.Errorf("posting store is not open")
	}
	return s.Pstore.RunValueLogGC(0.5)
}

func (s *ServerState) GetTimestamp(readOnly bool) uint64 {
	tr := tsReq{readOnly: readOnly, ch: make(chan uint64)}
	s.needTs <- tr
	return <-tr.ch
}

func (s *ServerState) fillTimestampRequests() {
	const (
		initDelay = 10 * time.Millisecond
		maxDelay  = time.Second
	)

	defer func() {
		glog.Infoln("Exiting fillTimestampRequests")
	}()

	var reqs []tsReq
	for {
		// Reset variables.
		reqs = reqs[:0]
		delay := initDelay

		select {
		case <-s.gcCloser.HasBeenClosed():
			return
		case req := <-s.needTs:
		slurpLoop:
			for {
				reqs = append(reqs, req)
				select {
				case req = <-s.needTs:
				default:
					break slurpLoop
				}
			}
		}

		// Generate the request.
		num := &pb.Num{}
		for _, r := range reqs {
			if r.readOnly {
				num.ReadOnly = true
			} else {
				num.Val++
			}
		}

		// Execute the request with infinite retries.
	retry:
		if s.gcCloser.Ctx().Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.gcCloser.Ctx(), 10*time.Second)
		ts, err := Timestamps(ctx, num)
		cancel()
		if err != nil {
			glog.Warningf("Error while retrieving timestamps: %v with delay: %v."+
				" Will retry...\n", err, delay)
			time.Sleep(delay)
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
			goto retry
		}
		var offset uint64
		for _, req := range reqs {
			if req.readOnly {
				req.ch <- ts.ReadOnly
			} else {
				req.ch <- ts.StartId + offset
				offset++
			}
		}
		x.AssertTrue(ts.StartId == 0 || ts.StartId+offset-1 == ts.EndId)
	}
}

type tsReq struct {
	readOnly bool
	// A one-shot chan which we can send a txn timestamp upon.
	ch chan uint64
}
