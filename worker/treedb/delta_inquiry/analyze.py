#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Offline diagnostic upper bound, never a semantic eligibility certificate."""
import argparse
import collections
import json


def analyze(events):
    if not events or [e['sequence'] for e in events] != list(range(1, len(events)+1)):
        raise ValueError('missing, reordered or mixed-process sequence')
    if any(e['version'] != 1 for e in events):
        raise ValueError('unsupported version')
    if events[0]['phase'] != 'enabled' or events[-1]['phase'] != 'apply_closed':
        raise ValueError('incomplete trace; retain raw evidence, no frequency inference')
    times = [e['elapsed_ns'] for e in events]
    if any(type(t) is not int or t < 0 for t in times) or times != sorted(times):
        raise ValueError('invalid monotonic observation times')
    offered, per = {}, collections.defaultdict(dict)
    for e in events:
        index, phase = e['index'], e['phase']
        if phase == 'enqueue_begin':
            if index in offered:
                raise ValueError('repeated Raft index; analyze sessions separately')
            offered[index] = e
        per[index].setdefault(phase, []).append(e)
    def balanced(index):
        requests = collections.defaultdict(collections.Counter)
        for phase in ('submit_begin', 'submit_return', 'callback'):
            for e in per[index].get(phase, []):
                requests[e['request']][phase] += 1
        return bool(requests) and all(dict(counts) == {
            'submit_begin': 1, 'submit_return': 1, 'callback': 1
        } for counts in requests.values())
    candidates, excluded = [], collections.Counter()
    ordered = sorted(offered)
    for a, b in zip(ordered, ordered[1:]):
        left, right = offered[a], offered[b]
        reason = None
        def one(index, phase):
            es = per[index].get(phase, [])
            return es[0] if len(es) == 1 else None
        if b != a + 1:
            reason = 'interposed_raft_index_gap'
        elif left['kind'] != 'delta' or right['kind'] != 'delta':
            reason = 'interposed_non_delta'
        elif not all(one(i, 'delta_begin') and one(i, 'delta_begin')['kind'] ==
                     'ordinary_store_wal_candidate' for i in (a, b)):
            reason = 'excluded_profile_or_duplicate_skip'
        elif any(e.get('error') for i in (a, b) for es in per[i].values() for e in es):
            reason = 'error'
        elif not all(balanced(i) for i in (a, b)):
            reason = 'unbalanced_submission'
        elif not all(one(i, 'oracle_end') and one(i, 'applied_done') for i in (a, b)):
            reason = 'incomplete_application'
        elif not all(any(e.get('entries', 0) > 0 for e in per[i].get('submit_begin', [])) for i in (a, b)):
            reason = 'no_storage_submission'
        else:
            txns = left.get('txns', []) + right.get('txns', [])
            starts = [x.get('start_ts', 0) for x in txns]
            if not starts or 0 in starts or len(set(starts)) != len(starts):
                reason = 'duplicate_or_invalid_start_ts'
            elif not all(x.get('commit_ts', 0) > x['start_ts'] for x in txns):
                reason = 'abort_or_invalid_decision'
            elif not one(b, 'enqueue_end') or not one(a, 'flush_end'):
                reason = 'missing_queue_or_flush_boundary'
            elif one(b, 'enqueue_end')['sequence'] >= one(a, 'flush_end')['sequence']:
                reason = 'not_confirmed_queued_before_flush_end'
            else:
                candidates.append({'predecessor': a, 'successor': b,
                                   'successor_enqueue_end': one(b, 'enqueue_end')['sequence'],
                                   'predecessor_flush_end': one(a, 'flush_end')['sequence'],
                                   'proposal_bytes': left['bytes'] + right['bytes']})
        if reason:
            excluded[reason] += 1
    durations = {}
    for index, phases in per.items():
        row = {}
        for name in ('flush', 'hard_sync', 'cache_invalidation', 'oracle', 'materialize'):
            begins, ends = phases.get(name+'_begin', []), phases.get(name+'_end', [])
            if len(begins) == len(ends) == 1:
                row[name] = ends[0]['elapsed_ns'] - begins[0]['elapsed_ns']
        if row:
            durations[index] = row
    return {'schema_version': 1, 'build': events[0].get('build', {}),
            'phase_durations_ns': durations, 'diagnostic_only': True,
            'semantic_independence_proven': False,
            'offered_entries': len(offered),
            'delta_entries': sum(e['kind'] == 'delta' for e in offered.values()),
            'adjacent_candidate_pairs': candidates,
            'excluded_adjacent_pairs': dict(excluded),
            'errors': [e for e in events if e.get('error')],
            'note': 'Adapter submission is not physical sync; tracing perturbs arrivals. '
                    'Pairs overlap and cannot be summed as independent cohorts.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', help='one Alpha process log, including startup and normal apply close')
    args = parser.parse_args()
    events = []
    with open(args.log) as source:
        for line in source:
            if 'DELTA_INQUIRY ' in line:
                events.append(json.loads(line.split('DELTA_INQUIRY ', 1)[1]))
    print(json.dumps(analyze(events), indent=2))


if __name__ == '__main__':
    main()
