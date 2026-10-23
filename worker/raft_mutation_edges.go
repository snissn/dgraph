/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	stderrors "errors"

	"github.com/dgraph-io/dgraph/v25/posting"
	"github.com/dgraph-io/dgraph/v25/protos/pb"
	"github.com/dgraph-io/dgraph/v25/x"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// applyRaftMutationEdges is the existing mutation/index pipeline. Staging and
// ordinary application share its batching, retries and complete error drain.
func applyRaftMutationEdges(ctx context.Context, m *pb.Mutations, txn *posting.Txn) error {
	span := trace.SpanFromContext(ctx)
	process := func(edges []*pb.DirectedEdge) error {
		var retries int
		for _, edge := range edges {
			for {
				err := runMutation(ctx, edge, txn)
				if err == nil {
					break
				}
				if err != posting.ErrRetry {
					return err
				}
				retries++
			}
		}
		if retries > 0 {
			span.AddEvent("retries=true num=%d", trace.WithAttributes(
				attribute.Int("retries", retries)))
		}
		return nil
	}
	numGo, width := x.DivideAndRule(len(m.Edges))
	span.AddEvent("To apply: %d edges. NumGo: %d. Width: %d", trace.WithAttributes(
		attribute.Int("num_edges", len(m.Edges)),
		attribute.Int("num_go", numGo),
		attribute.Int("width", width)))

	if numGo == 1 {
		return process(m.Edges)
	}

	// We need to create batches such that no two batches contains the same entry (<Entity> + <Predicate>)
	// combination. We have already sorted the edges, all we need to do is increase the batch size until we reach
	// the next different entry. New number of chans would ne less than NumGo. So we can create the chan with
	// numGo.
	sameAttrAndUid := func(i, j int) bool {
		ei := m.Edges[i]
		ej := m.Edges[j]
		if ei.GetAttr() != ej.GetAttr() {
			return false
		}
		return ei.GetEntity() == ej.GetEntity()
	}
	numChanCreated := 0

	errCh := make(chan error, numGo)
	for i := 0; i < len(m.Edges); {
		end := i + width
		if end > len(m.Edges) {
			end = len(m.Edges)
		}

		for end < len(m.Edges) && sameAttrAndUid(end, end-1) {
			end++
		}

		numChanCreated += 1
		go func(start, end int) {
			errCh <- process(m.Edges[start:end])
		}(i, end)
		i = end
	}
	// Earlier we were returning after even if one thread had an error. We should wait for
	// all the transactions to finish. We call txn.Update() when this function exists. This could cause
	// a deadlock with runMutation.
	var errs error
	for range numChanCreated {
		if err := <-errCh; err != nil {
			errs = stderrors.Join(errs, err)
		}
	}
	return errs
}
