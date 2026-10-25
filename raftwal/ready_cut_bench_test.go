/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"testing"

	"github.com/dgraph-io/dgraph/v25/x"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// BenchmarkDiskStorageSaveRequiredSync measures the public persistence boundary
// used before apply/Advance. Setup and Close are excluded here; process proofs
// separately account for the whole lifecycle. No encoder or private append is
// substituted for Save, and no fault/counter hook runs inside the timed loop.
func BenchmarkDiskStorageSaveRequiredSync(b *testing.B) {
	for _, format := range []struct {
		name string
		open func(string, x.Sensitive) (*DiskStorage, error)
	}{{"legacy", InitEncrypted}, {"ready_cut", InitCheckpointEncrypted}} {
		b.Run(format.name, func(b *testing.B) {
			store, err := format.open(b.TempDir(), nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := store.Close(); err != nil {
					b.Error(err)
				}
			})
			store.SetUint(RaftId, 11)
			store.SetUint(GroupId, 1)
			store.RequireCheckpointCertificate()
			if err := store.Sync(); err != nil {
				b.Fatal(err)
			}
			entries := []raftpb.Entry{{Term: 1, Type: raftpb.EntryNormal, Data: bytes.Repeat([]byte{'R'}, 4096)}}
			hs := raftpb.HardState{Term: 1, Vote: 1}
			b.ReportAllocs()
			b.SetBytes(int64(len(entries[0].Data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries[0].Index = uint64(i) + 1
				hs.Commit = entries[0].Index
				if err := store.Save(&hs, entries, nil); err != nil {
					b.Fatal(err)
				}
				if err := store.Sync(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
