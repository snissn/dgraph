// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"strings"
	"testing"
)

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
			r := validResult("treedb", "relaxed")
			mutate(&r)
			if err := r.Validate(); err == nil {
				t.Fatal("incomplete or mismatched runtime accepted")
			}
		})
	}
}

func TestCheckpointPolicyRequiresFilesystemAndCOWBeforeAcceptance(t *testing.T) {
	r := validResult("treedb", "durable")
	r.Config.SurvivalMode, r.Config.TreeDBPolicy, r.Config.TreeDBMemtable = "filesystem", "raft_checkpoint", "cow_ordered"
	r.Config.PostingStore = strings.Replace(r.Config.PostingStore, "durability=durable", "durability=raft_checkpoint", 1)
	r.Validation.DurabilityObserved = "raft_checkpoint"
	r.Validation.Runtime = runtimeFixture(r.Config)
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
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
	c := validResult("treedb", "durable").Config
	process := c.Fingerprint()
	c.SurvivalMode = "filesystem"
	if c.Fingerprint() == process {
		t.Fatal("different survival profiles can be pooled")
	}
}
