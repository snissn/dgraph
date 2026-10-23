/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func TestTimestampContextCancellationDoesNotBlockDispatcher(t *testing.T) {
	s := &ServerState{needTs: make(chan tsReq), gcCloser: z.NewCloser(0)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.GetTimestampContext(ctx, false); done <- err }()
	var req tsReq
	select {
	case req = <-s.needTs:
	case <-time.After(time.Second):
		t.Fatal("timestamp not requested")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	// A Zero response arriving after cancellation must not strand later requests.
	select {
	case req.ch <- 42:
	case <-time.After(time.Second):
		t.Fatal("abandoned timestamp blocked delivery")
	}
	s.gcCloser.Signal()
	_, err := s.GetTimestampContext(context.Background(), false)
	require.ErrorIs(t, err, context.Canceled)
}
