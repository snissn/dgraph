/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package posting

import (
	"context"
	"fmt"
)

// RaftSchemaTransition owns the existing request/root drain for a logged schema
// operation. Unlike posting history maintenance it changes no discard floor,
// checkpoint or retention frontier. The ordered owner must retain the proposal
// and previous exact certificate until a subsequent certificate covers it.
type RaftSchemaTransition struct{ maintenance *RaftPostingMaintenance }

func (s *TreeDBStore) BeginRaftSchemaTransition(ctx context.Context) (*RaftSchemaTransition, error) {
	floor, err := s.DiscardFloor()
	if err != nil {
		return nil, err
	}
	m, err := s.BeginRaftPostingMaintenance(ctx, floor)
	if err != nil {
		return nil, err
	}
	// The caller can now perform mixed main/split/schema writes. Any return
	// before Complete conservatively seals admission until certified reopen.
	m.started = true
	return &RaftSchemaTransition{maintenance: m}, nil
}

func (s *RaftSchemaTransition) Store() Store { return s.maintenance.ReadStore() }

// Complete follows all schema/data/index callbacks and final schema publication.
// Dispose pending working Lists and the global cache before readers resume.
func (s *RaftSchemaTransition) Complete() error {
	if s == nil || !s.maintenance.valid() {
		return fmt.Errorf("stale schema transition capability")
	}
	resetRaftPostingCaches()
	s.maintenance.finished = true
	return nil
}

func (s *RaftSchemaTransition) Close() {
	if s != nil {
		s.maintenance.Close()
	}
}
