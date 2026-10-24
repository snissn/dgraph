// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadedRuntimeRequiresEveryUnmodifiedSidecar(t *testing.T) {
	for _, phase := range []string{"startup", "after_alpha_restart"} {
		for _, role := range []string{"zero", "alpha"} {
			suffixes := []string{".config.log"}
			if role == "alpha" {
				suffixes = append(suffixes, ".store.txt")
			}
			for _, suffix := range suffixes {
				for _, damage := range []string{"missing", "truncated", "replaced"} {
					t.Run(phase+"/"+role+suffix+"/"+damage, func(t *testing.T) {
						set := validSet(t, 1)
						r := &set[0]
						path := filepath.Join(r.runtimeArtifactDir, "result.json")
						r.Context.RawPath = "/untrusted/json/path/result.json"
						if err := WriteImmutable(path, *r); err != nil {
							t.Fatal(err)
						}
						loaded, err := LoadResults([]string{path})
						if err != nil {
							t.Fatal(err)
						}
						set[0] = loaded[0]
						artifact := filepath.Join(r.runtimeArtifactDir, "runtime-"+phase+"-"+role+suffix)
						if damage == "missing" {
							if err := os.Remove(artifact); err != nil {
								t.Fatal(err)
							}
						} else {
							body := []byte{}
							if damage == "replaced" {
								body = []byte("replacement witness\n")
							}
							if err := os.WriteFile(artifact, body, 0o644); err != nil {
								t.Fatal(err)
							}
						}
						if _, err := LoadResults([]string{path}); err == nil {
							t.Fatal("damaged runtime artifact accepted on load")
						}
						if err := ValidateSet(set, 1); err == nil {
							t.Fatal("artifact changed after load accepted by matrix")
						}
					})
				}
			}
		}
	}
}

func TestRuntimeChecksWitnessMeaningEvenWithMatchingHash(t *testing.T) {
	for name, body := range map[string]string{
		"wrong config":     "x.WorkerConfig: {HardSync:true}\n",
		"duplicate config": "x.WorkerConfig: {HardSync:false}\nx.WorkerConfig: {HardSync:false}\n",
		"wrong backend":    "status=map[backend:badger profile:wal_on_sync]\nstats=map[treedb.cache.memtable_mode:cow_ordered]\n",
		"wrong memtable":   "status=map[backend:treedb profile:wal_on_sync]\nstats=map[treedb.cache.memtable_mode:btree]\n",
		"duplicate route":  "status=map[backend:treedb backend:badger profile:wal_on_sync]\nstats=map[treedb.cache.memtable_mode:cow_ordered]\n",
	} {
		t.Run(name, func(t *testing.T) {
			r := validResult(t, "treedb", "durable")
			identity := &r.Validation.Runtime[1]
			suffix := ".store.txt"
			sum := sha256.Sum256([]byte(body))
			if strings.Contains(name, "config") {
				suffix = ".config.log"
				identity.ConfigLogSHA256 = hex.EncodeToString(sum[:])
			} else {
				identity.StoreWitnessSHA256 = hex.EncodeToString(sum[:])
			}
			path := filepath.Join(r.runtimeArtifactDir, "runtime-startup-alpha"+suffix)
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := r.VerifyRuntimeArtifacts(r.runtimeArtifactDir); err == nil {
				t.Fatal("contradictory hashed witness accepted")
			}
		})
	}
}

func TestMatrixRejectsRuntimeIdentitiesWithoutDiskProvenance(t *testing.T) {
	set := validSet(t, 1)
	set[0].runtimeArtifactDir = ""
	if err := ValidateSet(set, 1); err == nil || !strings.Contains(err.Error(), "not verified on disk") {
		t.Fatalf("unverified in-memory result accepted: %v", err)
	}
}

func TestRuntimeIdentityRequiresActualStartupAndRestartRoutes(t *testing.T) {
	for name, mutate := range map[string]func(*Result){
		"missing Zero":         func(r *Result) { r.Validation.Runtime = r.Validation.Runtime[1:] },
		"missing restart":      func(r *Result) { r.Validation.Runtime = r.Validation.Runtime[:2] },
		"duplicate process":    func(r *Result) { r.Validation.Runtime[1] = r.Validation.Runtime[0] },
		"wrong resolved sync":  func(r *Result) { r.Validation.Runtime[0].ConfigWitness = "x.WorkerConfig: {HardSync:true}" },
		"unresolved flag only": func(r *Result) { r.Validation.Runtime[0].ConfigWitness = "--survive=process HardSync:false" },
		"duplicate survival flags": func(r *Result) {
			r.Validation.Runtime[0].Command = append(r.Validation.Runtime[0].Command, "--survive=process")
		},
		"restart wrong posting route": func(r *Result) { r.Validation.Runtime[3].Durability = "wal_on_sync" },
		"missing binary identity":     func(r *Result) { r.Context.BinarySHA256 = "" },
		"missing memtable":            func(r *Result) { r.Validation.Runtime[1].Memtable = "" },
	} {
		t.Run(name, func(t *testing.T) {
			r := validResult(t, "treedb", "relaxed")
			mutate(&r)
			if err := r.Validate(); err == nil {
				t.Fatal("incomplete or mismatched runtime accepted")
			}
		})
	}
}

func TestCheckpointPolicyRequiresFilesystemAndCOWBeforeAcceptance(t *testing.T) {
	r := validResult(t, "treedb", "durable")
	r.Config.SurvivalMode, r.Config.TreeDBPolicy, r.Config.TreeDBMemtable = "filesystem", "raft_checkpoint", "cow_ordered"
	r.Config.PostingStore = strings.Replace(r.Config.PostingStore, "durability=durable", "durability=raft_checkpoint", 1)
	r.Validation.DurabilityObserved = "raft_checkpoint"
	r.Validation.Runtime = runtimeFixture(r.Config)
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, name := range requiredTreeDBMetrics {
		if strings.HasPrefix(name, "treedb_group_commit_") {
			r.Metrics[name] = Metric{Available: false, Unit: "count", Source: "TreeDB /debug/store", Reason: RaftCheckpointNoCommandWAL}
		}
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("explicit posting-WAL absence for checkpoint profile rejected: %v", err)
	}
	required := r.Metrics["treedb_value_log_file_syncs"]
	r.Metrics["treedb_value_log_file_syncs"] = Metric{Available: false, Unit: "count", Source: "TreeDB /debug/store", Reason: RaftCheckpointNoCommandWAL}
	if err := r.Validate(); err == nil {
		t.Fatal("unavailable persistent value-log syncs accepted as posting WAL")
	}
	r.Metrics["treedb_value_log_file_syncs"] = required
	group := r.Metrics["treedb_group_commit_commits"]
	group.Reason = "debug sample failed"
	r.Metrics["treedb_group_commit_commits"] = group
	if err := r.Validate(); err == nil {
		t.Fatal("arbitrary missing group counter accepted for checkpoint profile")
	}
	group.Reason = RaftCheckpointNoCommandWAL
	r.Metrics["treedb_group_commit_commits"] = group
	for _, mode := range []string{"process", ""} {
		bad := r
		bad.Config.SurvivalMode = mode
		if err := bad.Validate(); err == nil {
			t.Fatal("checkpoint with weak/missing survival accepted")
		}
	}
	r.Validation.Runtime[3].Memtable = "btree"
	if err := r.Validate(); err == nil {
		t.Fatal("restart COW mismatch accepted")
	}
}

func TestSurvivalProfilesHaveDifferentWorkloadFingerprints(t *testing.T) {
	c := validResult(t, "treedb", "durable").Config
	process := c.Fingerprint()
	c.SurvivalMode = "filesystem"
	if c.Fingerprint() == process {
		t.Fatal("different survival profiles can be pooled")
	}
}
