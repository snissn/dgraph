/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dgraph-io/dgraph/v25/graphql/resolve"
	"github.com/dgraph-io/dgraph/v25/graphql/schema"
	"github.com/dgraph-io/dgraph/v25/worker"
	"google.golang.org/grpc/metadata"
)

// Posting expiry is an explicit operator decision, never a local timer or an
// inferred safe frontier. Requiring a configured token also protects OSS servers
// whose Guardian middleware intentionally permits requests when ACL is disabled.
func authorizePostingHistoryRetirement(ctx context.Context) error {
	configured := worker.Config.AuthToken
	if configured == "" {
		return fmt.Errorf("posting history retirement requires a configured admin token")
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return fmt.Errorf("posting history retirement requires admin authentication")
	}
	tokens := md.Get("auth-token")
	if len(tokens) != 1 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(configured)) != 1 {
		return fmt.Errorf("posting history retirement requires admin authentication")
	}
	return nil
}

func resolveRetirePostingHistory(ctx context.Context, m schema.Mutation) (*resolve.Resolved, bool) {
	if err := authorizePostingHistoryRetirement(ctx); err != nil {
		return resolve.EmptyResult(m, err), false
	}
	floor, err := strconv.ParseUint(fmt.Sprint(m.ArgValue("floor")), 10, 64)
	if err != nil {
		return resolve.EmptyResult(m, fmt.Errorf("invalid posting retirement floor: %w", err)), false
	}
	if err := worker.RequestPostingHistoryFloor(ctx, floor); err != nil {
		return resolve.EmptyResult(m, err), false
	}
	return resolve.DataResult(m, map[string]interface{}{m.Name(): map[string]interface{}{
		"response": map[string]interface{}{"code": "Success", "message": "Posting history retirement applied"},
		"floor":    json.Number(strconv.FormatUint(floor, 10)),
	}}, nil), true
}
