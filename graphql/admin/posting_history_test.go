/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package admin

import (
	"context"
	"testing"

	"github.com/dgraph-io/dgraph/v25/graphql/resolve"
	"github.com/dgraph-io/dgraph/v25/graphql/schema"
	"github.com/dgraph-io/dgraph/v25/worker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestPostingHistoryRetirementRequiresExplicitAuthentication(t *testing.T) {
	old := worker.Config.AuthToken
	t.Cleanup(func() { worker.Config.AuthToken = old })
	ctx := func(tokens ...string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{"auth-token": tokens})
	}
	worker.Config.AuthToken = ""
	require.ErrorContains(t, authorizePostingHistoryRetirement(ctx("")), "configured admin token")
	worker.Config.AuthToken = "posting-test-secret"
	for _, bad := range []context.Context{context.Background(), ctx(), ctx("wrong"), ctx("posting-test-secret", "wrong")} {
		require.ErrorContains(t, authorizePostingHistoryRetirement(bad), "admin authentication")
	}
	require.NoError(t, authorizePostingHistoryRetirement(ctx("posting-test-secret")))
	middlewares := adminMutationMWConfig["retirePostingHistory"]
	require.True(t, containsMW(middlewares, resolve.IpWhitelistingMW4Mutation))
	require.True(t, containsMW(middlewares, resolve.GuardianOfTheGalaxyAuthMW4Mutation))
	require.True(t, containsMW(middlewares, resolve.LoggingMWMutation))
}

func TestPostingHistoryRetirementIsPublicAdminSchema(t *testing.T) {
	s, err := schema.FromString(graphqlAdminSchema, 0)
	require.NoError(t, err)
	operation, err := s.Operation(&schema.Request{Query: `mutation {
		retirePostingHistory(floor: 18446744073709551615) { floor response { code message } }
	}`})
	require.NoError(t, err)
	require.Len(t, operation.Mutations(), 1)
	require.Equal(t, "retirePostingHistory", operation.Mutations()[0].Name())
}
