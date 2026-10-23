/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package zero

import (
	"sync"
	"testing"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/stretchr/testify/require"
)

// Exercise the SAME attachment seam used immediately before Oracle.Send, not
// an Alpha-manufactured assignment/checksum relationship. Membership transitions
// update the persisted Raft counter independently of timestamp assignment.
func TestOracleEmittedChecksumSequenceTracksMembershipNotAssignment(t *testing.T) {
	s := &Server{state: &pb.MembershipState{Counter: 10, Groups: map[uint32]*pb.Group{1: {Checksum: 111}}}}
	first := &pb.OracleDelta{MaxAssigned: 50}
	s.attachGroupChecksums(first)
	s.Lock()
	s.state.Counter = 11
	s.state.Groups[1].Checksum = 222
	s.Unlock()
	second := &pb.OracleDelta{MaxAssigned: 50}
	s.attachGroupChecksums(second)
	require.Equal(t, first.MaxAssigned, second.MaxAssigned)
	require.Equal(t, uint64(10), first.ChecksumSequence)
	require.Equal(t, uint64(11), second.ChecksumSequence)
	require.Equal(t, uint64(111), first.GroupChecksums[1])
	require.Equal(t, uint64(222), second.GroupChecksums[1])
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := uint64(12); seq < 1000; seq++ {
			s.Lock()
			s.state.Counter = seq
			s.state.Groups[1].Checksum = seq + 1000
			s.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		delta := &pb.OracleDelta{}
		s.attachGroupChecksums(delta)
		if delta.ChecksumSequence > 11 {
			require.Equal(t, delta.ChecksumSequence+1000, delta.GroupChecksums[1])
		}
		require.Zero(t, delta.MaxAssigned)
	}
	wg.Wait()
}
