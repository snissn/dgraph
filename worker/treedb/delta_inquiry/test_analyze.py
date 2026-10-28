# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import unittest
from analyze import analyze


def fixture():
    records = []
    def event(index, phase, **fields):
        records.append(dict(version=1, sequence=len(records)+1, index=index,
                            phase=phase, elapsed_ns=(len(records)+1)*100, **fields))
    event(0, 'enabled')
    for i in (1, 2):
        event(i, 'enqueue_begin', kind='delta', bytes=100,
              txns=[dict(start_ts=i*10, commit_ts=i*10+1)])
        event(i, 'enqueue_end')
    for i in (1, 2):
        event(i, 'delta_begin', kind='ordinary_store_wal_candidate')
        event(i, 'submit_begin', request=1, entries=1)
        event(i, 'submit_return', request=1)
        event(i, 'flush_begin')
        event(i, 'callback', request=1)
        event(i, 'flush_end')
        event(i, 'oracle_end')
        event(i, 'applied_done')
    event(0, 'apply_closed')
    return records


class AnalyzeTests(unittest.TestCase):
    def test_candidate_is_only_upper_bound(self):
        result = analyze(fixture())
        self.assertEqual(len(result['adjacent_candidate_pairs']), 1)
        self.assertFalse(result['semantic_independence_proven'])

    def test_barriers_and_errors(self):
        for label, change in [
            ('interposed_non_delta', lambda es: es[3].update(kind='mutation')),
            ('excluded_profile_or_duplicate_skip', lambda es: es[5].update(kind='excluded_profile')),
            ('error', lambda es: es[9].update(error='sync failed')),
            ('duplicate_or_invalid_start_ts', lambda es: es[3].update(txns=es[1]['txns'])),
            ('abort_or_invalid_decision', lambda es: es[3].update(txns=[dict(start_ts=20)])),
            ('no_storage_submission', lambda es: es[14].update(entries=0)),
        ]:
            with self.subTest(label=label):
                es = copy.deepcopy(fixture())
                change(es)
                result = analyze(es)
                self.assertEqual(result['excluded_adjacent_pairs'], {label: 1})
                self.assertFalse(result['adjacent_candidate_pairs'])

    def test_missing_callback_and_index_gap(self):
        es = fixture()
        es[9]['phase'] = 'lost_callback'
        self.assertEqual(analyze(es)['excluded_adjacent_pairs'], {'unbalanced_submission': 1})
        es = fixture()
        for e in es:
            if e['index'] == 2:
                e['index'] = 3
        self.assertEqual(analyze(es)['excluded_adjacent_pairs'], {'interposed_raft_index_gap': 1})

    def test_incomplete_or_mixed_session_refused(self):
        for es in (fixture()[:-1], fixture()[1:], fixture()+fixture()):
            with self.assertRaises(ValueError):
                analyze(es)

    def test_enqueue_overlap_not_confirmation(self):
        es = fixture()
        e = es.pop(4)
        es.insert(-1, e)
        for i, e in enumerate(es):
            e['sequence'] = i+1
            e['elapsed_ns'] = (i+1)*100
        self.assertEqual(analyze(es)['excluded_adjacent_pairs'],
                         {'not_confirmed_queued_before_flush_end': 1})


if __name__ == '__main__':
    unittest.main()
