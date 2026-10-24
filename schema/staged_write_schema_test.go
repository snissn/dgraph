/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package schema

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/dgraph/v25/protos/pb"
)

func TestStagedWriteSchemaPrecedenceAndIsolation(t *testing.T) {
	query := &pb.SchemaUpdate{Predicate: "name", ValueType: pb.Posting_STRING}
	mutation := &pb.SchemaUpdate{Predicate: "name", ValueType: pb.Posting_INT}
	staged := &pb.SchemaUpdate{Predicate: "name", ValueType: pb.Posting_BOOL}
	private := &pb.SchemaUpdate{Predicate: "private", ValueType: pb.Posting_UID, List: true}
	s := &state{
		predicate: map[string]*pb.SchemaUpdate{"name": query},
		mutSchema: map[string]*pb.SchemaUpdate{"name": mutation},
	}
	base := context.Background()
	ctx := WithStagedWriteSchema(base, map[string]*pb.SchemaUpdate{"name": staged, "private": private})
	nilEntry := WithStagedWriteSchema(base, map[string]*pb.SchemaUpdate{"name": nil})
	nilMap := WithStagedWriteSchema(base, nil)
	for _, tc := range []struct {
		name string
		ctx  context.Context
		pred string
		want *pb.SchemaUpdate
	}{
		{"query ignores overrides", ctx, "name", query},
		{"write prefers override", GetWriteContext(ctx), "name", staged},
		{"write falls back to mutation schema", GetWriteContext(base), "name", mutation},
		{"nil entry falls back to mutation schema", GetWriteContext(nilEntry), "name", mutation},
		{"nil map falls back to mutation schema", GetWriteContext(nilMap), "name", mutation},
		{"private schema available to staged write", GetWriteContext(ctx), "private", private},
		{"private schema absent from query", ctx, "private", nil},
		{"missing schema retains zero value", GetWriteContext(ctx), "missing", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := s.Get(tc.ctx, tc.pred)
			want := tc.want
			if want == nil {
				if ok || !proto.Equal(&got, &pb.SchemaUpdate{}) {
					t.Fatal("missing schema did not return the legacy zero value")
				}
				return
			}
			if !ok || !proto.Equal(&got, want) {
				t.Fatalf("selected schema=%v present=%v, want %v", &got, ok, want)
			}
		})
	}
	if len(s.predicate) != 1 || s.predicate["name"] != query ||
		len(s.mutSchema) != 1 || s.mutSchema["name"] != mutation {
		t.Fatal("staged schema lookup published or replaced global schema")
	}
	delete(s.mutSchema, "name")
	got, ok := s.Get(GetWriteContext(nilEntry), "name")
	if !ok || !proto.Equal(&got, query) {
		t.Fatal("absent override and mutation schema did not fall back to query schema")
	}
}

func TestStagedWriteSchemaRetainsInitializedProtoPointer(t *testing.T) {
	staged := &pb.SchemaUpdate{Predicate: "name", ValueType: pb.Posting_STRING, Upsert: true}
	// Marshal initializes protobuf message state, as schema persistence does.
	// The private seam must keep the same message rather than copy its lock.
	if _, err := proto.Marshal(staged); err != nil {
		t.Fatal(err)
	}
	ctx := WithStagedWriteSchema(context.Background(), map[string]*pb.SchemaUpdate{"name": staged, "nil": nil})
	got, ok := StagedWriteSchema(ctx, "name")
	if !ok || got != staged || !got.GetUpsert() {
		t.Fatal("private lookup did not retain the initialized schema pointer")
	}
	for _, tc := range []struct {
		ctx  context.Context
		pred string
	}{
		{ctx, "missing"},
		{ctx, "nil"},
		{WithStagedWriteSchema(context.Background(), nil), "name"},
		{context.Background(), "name"},
	} {
		if got, ok := StagedWriteSchema(tc.ctx, tc.pred); ok || got != nil {
			t.Fatal("absent override returned a schema pointer")
		}
	}
}
