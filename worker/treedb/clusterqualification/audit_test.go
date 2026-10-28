// SPDX-License-Identifier: Apache-2.0
package clusterqualification

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type preparationAudit struct {
	Records                 uint64 `json:"records"`
	MaterializedOriginals   int    `json:"materialized_originals"`
	CompletedOriginals      int    `json:"completed_originals"`
	Overlaps                int    `json:"overlapping_original_preparations"`
	MaximumPending          int    `json:"maximum_unpublished_materialized_originals"`
	UnpublishedMaterialized int    `json:"unpublished_materialized_originals"`
	IncompleteTail          bool   `json:"incomplete_tail"`
}

type originalID struct{ Index, Key uint64 }
type originalTrace struct {
	phases       map[string]uint64
	materialized bool
}

// auditPreparation checks the merged observer's original identity and logical
// endpoints. A successor's materialize_begin before its predecessor's Applied
// endpoint demonstrates overlapping preparation only. The observer has no native
// DATA/captured-sync IDs and cannot establish physical sharing or sync savings.
// A killed process can end with a partial final line or pending originals; retain
// those unknowns rather than declaring its session complete.
func auditPreparation(path string) (preparationAudit, error) {
	var out preparationAudit
	b, err := os.ReadFile(path)
	if err != nil {
		return out, err
	}
	if len(b) > 0 && b[len(b)-1] != '\n' {
		out.IncompleteTail = true
		at := strings.LastIndexByte(string(b), '\n')
		if at < 0 {
			return out, fmt.Errorf("no complete log line")
		}
		b = b[:at+1]
	}
	originals := make(map[originalID]*originalTrace)
	active := make(map[originalID]bool)
	var session string
	var previousCompleted uint64
	finalizers := []string{"flush_begin", "flush_end", "cache_invalidation_begin", "cache_invalidation_end", "oracle_begin", "oracle_end", "proposal_done_begin", "applied_done"}
	for _, line := range strings.Split(string(b), "\n") {
		marker := strings.Index(line, "DELTA_INQUIRY ")
		if marker < 0 {
			continue
		}
		var e struct {
			Session  string `json:"session"`
			Sequence uint64 `json:"sequence"`
			Phase    string `json:"phase"`
			Index    uint64 `json:"index"`
			Key      uint64 `json:"key"`
			Error    string `json:"error"`
		}
		if err := json.Unmarshal([]byte(line[marker+len("DELTA_INQUIRY "):]), &e); err != nil {
			return out, err
		}
		if session == "" {
			session = e.Session
		}
		if session == "" || e.Session != session || e.Sequence != out.Records+1 {
			return out, fmt.Errorf("observer session/sequence gap at %d", e.Sequence)
		}
		out.Records++
		// Oracle stream cancellation at deliberate shutdown is expected; storage,
		// callback or finalization errors cannot qualify as successful progress.
		if e.Error != "" && e.Phase != "oracle_receive_end" && e.Phase != "oracle_propose_end" {
			return out, fmt.Errorf("observer %s error: %s", e.Phase, e.Error)
		}
		id := originalID{e.Index, e.Key}
		if e.Phase == "delta_begin" {
			if _, seen := originals[id]; seen {
				return out, fmt.Errorf("duplicate Delta identity %+v", id)
			}
			originals[id] = &originalTrace{phases: make(map[string]uint64)}
		}
		original := originals[id]
		if original == nil {
			continue
		}
		if e.Phase == "materialize_begin" && !original.materialized {
			original.materialized = true
			out.MaterializedOriginals++
			if len(active) > 0 {
				out.Overlaps++
			}
			active[id] = true
			if len(active) > out.MaximumPending {
				out.MaximumPending = len(active)
			}
		}
		for _, phase := range finalizers {
			if e.Phase != phase {
				continue
			}
			if original.phases[phase] != 0 {
				return out, fmt.Errorf("duplicate %s identity %+v", phase, id)
			}
			original.phases[phase] = e.Sequence
		}
		if e.Phase == "applied_done" {
			previous := uint64(0)
			for _, phase := range finalizers {
				seq := original.phases[phase]
				if seq <= previous {
					return out, fmt.Errorf("missing/reversed %s identity %+v", phase, id)
				}
				previous = seq
			}
			if id.Index <= previousCompleted {
				return out, fmt.Errorf("reversed original finalization index %d after %d", id.Index, previousCompleted)
			}
			previousCompleted = id.Index
			if original.materialized {
				out.CompletedOriginals++
				delete(active, id)
			}
		}
	}
	out.UnpublishedMaterialized = len(active)
	if out.Records == 0 {
		return out, fmt.Errorf("no Delta observer records")
	}
	return out, nil
}

func TestPreparationAudit(t *testing.T) {
	phases := []string{"delta_begin", "materialize_begin", "flush_begin", "flush_end", "cache_invalidation_begin", "cache_invalidation_end", "oracle_begin", "oracle_end", "proposal_done_begin", "applied_done"}
	makeLog := func(overlap bool, reverse bool) string {
		var lines []string
		seq := 0
		emit := func(index int, phase string) {
			seq++
			b, _ := json.Marshal(map[string]any{"session": "test", "sequence": seq, "index": index, "key": index + 100, "phase": phase})
			lines = append(lines, "DELTA_INQUIRY "+string(b))
		}
		emit(1, phases[0])
		emit(1, phases[1])
		if overlap {
			emit(2, phases[0])
			emit(2, phases[1])
		}
		order := append([]string(nil), phases[2:]...)
		if reverse {
			order[0], order[1] = order[1], order[0]
		}
		for _, p := range order {
			emit(1, p)
		}
		if !overlap {
			emit(2, phases[0])
			emit(2, phases[1])
		}
		for _, p := range phases[2:] {
			emit(2, p)
		}
		return strings.Join(lines, "\n") + "\n"
	}
	for _, tc := range []struct {
		name             string
		overlap, reverse bool
		want             int
		bad              bool
	}{{"overlap", true, false, 1, false}, {"serial", false, false, 0, false}, {"bad-finalizer", true, true, 0, true}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "alpha.log")
			require.NoError(t, os.WriteFile(path, []byte(makeLog(tc.overlap, tc.reverse)), 0600))
			out, err := auditPreparation(path)
			if tc.bad {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, out.Overlaps)
			require.Equal(t, 2, out.CompletedOriginals)
			require.Zero(t, out.UnpublishedMaterialized)
		})
	}
	t.Run("gap", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "alpha.log")
		require.NoError(t, os.WriteFile(path, []byte(strings.Replace(makeLog(true, false), `"sequence":2`, `"sequence":99`, 1)), 0600))
		_, err := auditPreparation(path)
		require.Error(t, err)
	})
	t.Run("killed-tail", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "alpha.log")
		lines := strings.Split(makeLog(true, false), "\n")
		require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines[:4], "\n")+"\nDELTA_INQUIRY {"), 0600))
		out, err := auditPreparation(path)
		require.NoError(t, err)
		require.True(t, out.IncompleteTail)
		require.Equal(t, 2, out.UnpublishedMaterialized)
	})
}

// verifyStaged requires the later public transaction's actual posting producer
// on this replica while the first original has no durable callback/publication.
func verifyStaged(path string, start, blockedStart uint64) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	last := strings.LastIndexByte(string(b), '\n')
	if last < 0 {
		return fmt.Errorf("no complete observer line")
	}
	b = b[:last+1]
	type event struct {
		Phase string `json:"phase"`
		Start uint64 `json:"start_ts"`
		Index uint64 `json:"index"`
		Key   uint64 `json:"key"`
		Error string `json:"error"`
	}
	var events []event
	var target, blocked originalID
	for _, line := range strings.Split(string(b), "\n") {
		marker := strings.Index(line, "DELTA_INQUIRY ")
		if marker < 0 {
			continue
		}
		var e event
		if err := json.Unmarshal([]byte(line[marker+len("DELTA_INQUIRY "):]), &e); err != nil {
			return err
		}
		events = append(events, e)
		if e.Phase == "materialize_begin" {
			if e.Start == start {
				target = originalID{e.Index, e.Key}
			}
			if e.Start == blockedStart {
				blocked = originalID{e.Index, e.Key}
			}
		}
	}
	if target.Index == 0 || blocked.Index == 0 {
		return fmt.Errorf("waiting for actual materialization start=%d blocked=%d", start, blockedStart)
	}
	if start != blockedStart && target == blocked {
		return fmt.Errorf("transactions must retain distinct original proposals")
	}
	producerEnded, submitted := false, false
	for _, e := range events {
		if (originalID{e.Index, e.Key}) == target {
			if e.Error != "" {
				return fmt.Errorf("original producer/submission error: %s", e.Error)
			}
			producerEnded = producerEnded || e.Phase == "materialize_end"
			submitted = submitted || e.Phase == "submit_return"
		}
		if (originalID{e.Index, e.Key}) == blocked && (e.Phase == "callback" || e.Phase == "oracle_end" || e.Phase == "proposal_done_begin" || e.Phase == "applied_done") {
			return fmt.Errorf("held original published %s before native gate release", e.Phase)
		}
	}
	if !producerEnded || !submitted {
		return fmt.Errorf("waiting for successful producer/submission endpoints start=%d", start)
	}
	return nil
}

func rejectFatalLog(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fatal := regexp.MustCompile(`(?m)^(?:panic:|fatal error:|F(?:[0-9]{4}|[0-9]{8})[ \t])|WARNING: DATA RACE|ordered Delta window incomplete|Error while applying txn status to disk`)
	if match := fatal.Find(b); match != nil {
		return fmt.Errorf("%s contains failure marker %q", filepath.Base(path), match)
	}
	return nil
}

// A real public stream may put a watermark between commit-bearing originals.
// Such an entry must remain a barrier; a test must not erase it or rerun until
// the source happens to produce consecutive decisions.
func stagedOrWatermark(path string, start, blockedStart, previousStart uint64) (string, error) {
	if err := verifyStaged(path, blockedStart, blockedStart); err != nil {
		return "", err
	}
	if err := verifyStaged(path, start, blockedStart); err == nil {
		return "staged", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var previous originalID
	type event struct {
		Phase    string            `json:"phase"`
		Kind     string            `json:"kind"`
		Start    uint64            `json:"start_ts"`
		Index    uint64            `json:"index"`
		Key      uint64            `json:"key"`
		TxnCount int               `json:"txn_count"`
		Txns     []json.RawMessage `json:"txns"`
	}
	var events []event
	last := strings.LastIndexByte(string(b), '\n')
	if last < 0 {
		return "", fmt.Errorf("waiting for complete observer line")
	}
	for _, line := range strings.Split(string(b[:last+1]), "\n") {
		marker := strings.Index(line, "DELTA_INQUIRY ")
		if marker < 0 {
			continue
		}
		var e event
		if err := json.Unmarshal([]byte(line[marker+len("DELTA_INQUIRY "):]), &e); err != nil {
			return "", err
		}
		events = append(events, e)
		if e.Phase == "materialize_begin" && e.Start == previousStart {
			previous = originalID{e.Index, e.Key}
		}
	}
	for _, e := range events {
		if e.Phase == "enqueue_begin" && e.Index == previous.Index+1 && e.Kind == "delta" && len(e.Txns) == 0 && e.TxnCount == 0 {
			return "watermark_barrier", nil
		}
	}
	return "", fmt.Errorf("waiting for materialization or actual intervening watermark start=%d", start)
}

func TestStagingAndBarrierAudit(t *testing.T) {
	write := func(t *testing.T, tail []map[string]any) string {
		t.Helper()
		events := []map[string]any{{"phase": "materialize_begin", "index": 10, "key": 100, "start_ts": 1}, {"phase": "submit_return", "index": 10, "key": 100}, {"phase": "materialize_end", "index": 10, "key": 100}}
		events = append(events, tail...)
		var b strings.Builder
		for _, e := range events {
			raw, err := json.Marshal(e)
			require.NoError(t, err)
			b.WriteString("DELTA_INQUIRY ")
			b.Write(raw)
			b.WriteByte('\n')
		}
		p := filepath.Join(t.TempDir(), "alpha.log")
		require.NoError(t, os.WriteFile(p, []byte(b.String()), 0600))
		return p
	}
	t.Run("actual-empty-delta", func(t *testing.T) {
		p := write(t, []map[string]any{{"phase": "enqueue_begin", "index": 11, "key": 101, "kind": "delta", "max_assigned": 20}})
		kind, err := stagedOrWatermark(p, 2, 1, 1)
		require.NoError(t, err)
		require.Equal(t, "watermark_barrier", kind)
	})
	t.Run("nonempty-delta-is-not-watermark", func(t *testing.T) {
		p := write(t, []map[string]any{{"phase": "enqueue_begin", "index": 11, "key": 101, "kind": "delta", "txns": []any{map[string]any{"start_ts": 2, "commit_ts": 20}}}})
		_, err := stagedOrWatermark(p, 2, 1, 1)
		require.Error(t, err)
	})
	t.Run("producer-end-required", func(t *testing.T) {
		p := write(t, []map[string]any{{"phase": "materialize_begin", "index": 11, "key": 101, "start_ts": 2}})
		require.Error(t, verifyStaged(p, 2, 1))
	})
	t.Run("successful-successor", func(t *testing.T) {
		p := write(t, []map[string]any{{"phase": "materialize_begin", "index": 11, "key": 101, "start_ts": 2}, {"phase": "submit_return", "index": 11, "key": 101}, {"phase": "materialize_end", "index": 11, "key": 101}})
		kind, err := stagedOrWatermark(p, 2, 1, 1)
		require.NoError(t, err)
		require.Equal(t, "staged", kind)
	})
	t.Run("callback-before-release-rejected", func(t *testing.T) {
		p := write(t, []map[string]any{{"phase": "callback", "index": 10, "key": 100}})
		require.Error(t, verifyStaged(p, 1, 1))
	})
	t.Run("fatal-in-killed-generation-rejected", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "alpha.log")
		for _, failure := range []string{"panic: storage failed\n", "F20261028 00:00:00 storage failed\n", "WARNING: DATA RACE\n"} {
			require.NoError(t, os.WriteFile(p, []byte(failure), 0600))
			require.Error(t, rejectFatalLog(p))
		}
	})
}
