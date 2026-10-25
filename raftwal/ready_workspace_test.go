/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Exercise actual encrypted source reads and new-generation writes over several
// scratch chunks, including non-block-aligned value offsets and final chunks.
// These are correctness characterizations, not an invented allocation RED.
func TestReadyWorkspaceEncryptedReplacement(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	a := bytes.Repeat([]byte{'A'}, 17)
	b := bytes.Repeat([]byte{'B'}, readyScratchSize*2+19)
	c := bytes.Repeat([]byte{'C'}, readyScratchSize*2+37)
	for _, fail := range []bool{false, true} {
		name := "growth_certificate_reopen"
		if fail {
			name = "closed_exact_source_discards_private_output"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := InitCheckpointEncrypted(dir, key)
			require.NoError(t, err)
			s.RequireCheckpointCertificate()
			for i, v := range [][]byte{a, b, c} {
				saveReady(t, s, uint64(i+1), v)
				require.NoError(t, s.Sync())
			}
			selected := s.ready.name
			before, err := os.ReadFile(filepath.Join(dir, selected))
			require.NoError(t, err)
			require.NoError(t, s.AuthorizeCheckpoint(1))
			if fail {
				// Closing only the exact read source leaves every accepted byte intact.
				// The successor buffers its header/state/metadata before its first retained
				// payload read fails. Cleanup must discard that output, not flush it into
				// the predecessor or make the poisoned owner reusable.
				require.NoError(t, s.ready.f.Close())
			}
			err = s.CreateSnapshot(1, &raftpb.ConfState{Voters: []uint64{1}}, []byte("certificate, odd-sized"))
			if fail {
				require.ErrorIs(t, err, os.ErrClosed)
				require.True(t, RecoveryRequired(err))
				require.Error(t, s.Save(&raftpb.HardState{Term: 1, Vote: 1, Commit: 4}, []raftpb.Entry{{Index: 4, Term: 1, Data: []byte("rejected")}}, nil))
				require.Error(t, s.Close())
				after, err := os.ReadFile(filepath.Join(dir, selected))
				require.NoError(t, err)
				require.Equal(t, before, after, "private failed output cannot alter selected bytes")
			} else {
				require.NoError(t, err)
				require.NotEqual(t, selected, s.ready.name)
				// A subsequent ordinary append must target the installed generation.
				saveReady(t, s, 4, []byte("after replacement"))
				require.NoError(t, s.Sync())
				require.NoError(t, s.Close())
			}
			reopened, err := InitCheckpointEncrypted(dir, key)
			require.NoError(t, err)
			defer reopened.Close()
			end := uint64(5)
			if fail {
				end = 4
			}
			es, err := reopened.Entries(2, end, ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, b, es[0].Data)
			require.Equal(t, c, es[1].Data)
			hs, err := reopened.HardState()
			require.NoError(t, err)
			require.Equal(t, end-1, hs.Commit)
			if !fail {
				require.Equal(t, []byte("after replacement"), es[2].Data)
				snap, err := reopened.Snapshot()
				require.NoError(t, err)
				require.Equal(t, uint64(1), snap.Metadata.Index)
				require.Equal(t, []byte("certificate, odd-sized"), snap.Data)
			} else {
				original, err := reopened.Entries(1, 2, ^uint64(0))
				require.NoError(t, err)
				require.Equal(t, a, original[0].Data)
				require.NoError(t, reopened.Sync())
			}
		})
	}
}
