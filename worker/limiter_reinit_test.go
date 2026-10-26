/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRateLimiterReconfigurationPreservesConditionAndDebt(t *testing.T) {
	oldMax := 4
	if limiter.c != nil {
		limiter.c.L.Lock()
		oldMax = limiter.max
		limiter.c.L.Unlock()
	}
	t.Cleanup(func() { configureRateLimiter(oldMax) })
	configureRateLimiter(4)
	require.NoError(t, limiter.incr(context.Background(), 0))
	condition := limiter.c
	configureRateLimiter(3)
	require.Same(t, condition, limiter.c)
	limiter.c.L.Lock()
	debt := limiter.iou
	limiter.c.L.Unlock()
	require.Equal(t, 1, debt, "reconfiguration must retain in-flight proposal ownership")
	limiter.decr(0)
	configureRateLimiter(4)
	var done sync.WaitGroup
	for range 4 {
		done.Go(func() {
			for range 1000 {
				if err := limiter.incr(context.Background(), 0); err != nil {
					t.Error(err)
					return
				}
				limiter.decr(0)
			}
		})
	}
	for i := range 100 {
		configureRateLimiter(4 + i%2)
	}
	done.Wait()
	limiter.c.L.Lock()
	debt = limiter.iou
	limiter.c.L.Unlock()
	require.Zero(t, debt)
}
