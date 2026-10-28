# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import unittest
from upstream import analyze_upstream, complete_events


def fixture():
    records = []
    def event(phase, **fields):
        n = len(records)+1
        records.append(dict(version=1, session='test', sequence=n, index=0, phase=phase,
                            elapsed_ns=n*100, captured_ns=n*100-1, **fields))
    event('enabled')
    event('oracle_stream_open', stream=1)
    for m in (1,):
        event('oracle_received', stream=1, message=m, txns=[dict(start_ts=m, commit_ts=m+10)])
        event('oracle_send_begin', stream=1, message=m)
        event('oracle_send_end', stream=1, message=m)
    event('oracle_consumed', stream=1, message=1, aggregate=1)
    event('oracle_aggregate_ready', stream=1, aggregate=1, entries=1)
    event('oracle_propose_begin', stream=1, aggregate=1, attempt=1)
    event('oracle_proposal_encoded', stream=1, aggregate=1, attempt=1, key=22, bytes=99)
    for m in (2, 3):
        event('oracle_received', stream=1, message=m, txns=[dict(start_ts=m, commit_ts=m+10)])
        event('oracle_send_begin', stream=1, message=m)
        event('oracle_send_end', stream=1, message=m)
    event('oracle_propose_end', stream=1, aggregate=1, attempt=1)
    # Shutdown is permitted to retain explicitly unconsumed receipts.
    event('oracle_receiver_closed', stream=1)
    event('oracle_stream_closed', stream=1)
    event('oracle_closed')
    event('apply_closed')
    return records


class UpstreamTests(unittest.TestCase):
    def test_hidden_receipts_and_admission_are_separate(self):
        result = analyze_upstream(fixture())
        wait = result['proposal_wait_observations'][0]
        self.assertEqual(wait['received_messages'], [2, 3])
        self.assertEqual(wait['admitted_messages'], [2, 3])
        self.assertEqual(wait['received_classifications'], {'commit_decisions_unqualified': 2})
        self.assertEqual(len(result['unconsumed_messages']), 2)
        self.assertFalse(result['counterfactual_aggregate_boundaries_known'])
        self.assertFalse(result['physical_sync_overlap_proven'])
        es = fixture()
        send = next(e for e in es if e['phase'] == 'oracle_send_end' and e['message'] == 3)
        send['phase'] = 'oracle_send_canceled'
        self.assertEqual(analyze_upstream(es)['admitted_messages'], 2)

    def test_prior_backlog_is_not_lost_from_wait_observations(self):
        es = fixture()
        for e in es:
            if e.get('message') == 2:
                e['captured_ns'] = 650
        wait = analyze_upstream(es)['proposal_wait_observations'][0]
        self.assertEqual(wait['received_unconsumed_at_begin'], [2])
        self.assertEqual(wait['admitted_unconsumed_at_begin'], [2])
        self.assertEqual(wait['received_messages'], [3])

    def test_cross_message_contradictions_are_retained(self):
        es = fixture()
        receive = next(e for e in es if e['phase'] == 'oracle_received' and e['message'] == 3)
        receive['txns'] = [dict(start_ts=2, commit_ts=99)]
        wait = analyze_upstream(es)['proposal_wait_observations'][0]
        self.assertEqual(wait['repeated_start_ts'], [2])
        self.assertEqual(wait['contradictory_start_ts'], [2])
        self.assertEqual(wait['received_decisions'], 2)

    def test_capture_time_can_precede_emission_order(self):
        es = fixture()
        receive = next(e for e in es if e['phase'] == 'oracle_received' and e['message'] == 2)
        receive['captured_ns'] = 750  # actual capture before proposal begin (799)
        result = analyze_upstream(es)
        self.assertEqual(result['proposal_wait_observations'][0]['received_messages'], [3])
        self.assertEqual(result['proposal_wait_observations'][0]['admitted_messages'], [2, 3])

    def test_decision_classifications_preserve_exclusions(self):
        for txns, extra, label in [
            ([], {}, 'watermark_only'), ([dict(start_ts=2)], {}, 'contains_abort'),
            ([dict(start_ts=2, commit_ts=1)], {}, 'invalid_decision'),
            ([None], {}, 'invalid_decision'),
            ([dict(start_ts=2, commit_ts=12)]*2, {}, 'repeated_start_ts'),
            ([dict(start_ts=2, commit_ts=12)], dict(unknown_fields=True), 'unknown_fields'),
        ]:
            es = fixture()
            receive = next(e for e in es if e['phase'] == 'oracle_received' and e['message'] == 2)
            receive.update(txns=txns, **extra)
            self.assertEqual(analyze_upstream(es)['proposal_wait_observations'][0]
                             ['received_classifications'][label], 1)

    def test_incomplete_mixed_or_invalid_trace_refused(self):
        original = fixture()
        for bad in (original[:-1], original+original, original[1:],
                    [e for e in original if e['phase'] != 'oracle_receiver_closed']):
            with self.assertRaises(ValueError):
                analyze_upstream(bad)
        for phase in ('oracle_send_end', 'oracle_propose_end', 'oracle_proposal_encoded'):
            es = copy.deepcopy(original)
            next(e for e in es if e['phase'] == phase)['phase'] = 'lost_event'
            with self.assertRaises(ValueError):
                analyze_upstream(es)

    def test_legacy_oracle_publication_is_distinct_from_upstream(self):
        es = fixture()
        expected = analyze_upstream(es)
        for phase in ('enqueue_begin', 'enqueue_end', 'apply_begin', 'delta_begin',
                      'materialize_begin', 'materialize_end', 'submit_begin', 'submit_return',
                      'callback', 'flush_begin', 'flush_end', 'cache_invalidation_begin',
                      'cache_invalidation_end', 'oracle_begin', 'oracle_end',
                      'proposal_done_begin', 'applied_done'):
            # Streamless legacy records remain covered by sequence/session
            # integrity, but cannot masquerade as Zero-stream observations.
            es.insert(-1, dict(version=1, session='test', phase=phase, index=77))
        for n, event in enumerate(es, 1):
            event.update(sequence=n, elapsed_ns=n*100, captured_ns=n*100-1)
        self.assertEqual(analyze_upstream(es), expected)

    def test_unknown_oracle_phase_and_real_unknown_stream_refused(self):
        es = fixture()
        next(e for e in es if e['phase'] == 'oracle_send_begin')['phase'] = 'oracle_mystery'
        with self.assertRaisesRegex(ValueError, 'unknown oracle observation phase'):
            analyze_upstream(es)
        es = fixture()
        next(e for e in es if e['phase'] == 'oracle_send_begin')['stream'] = 99
        with self.assertRaisesRegex(ValueError, 'unknown stream epoch'):
            analyze_upstream(es)

    def test_failure_before_encoding_retains_outcome(self):
        es = fixture()
        next(e for e in es if e['phase'] == 'oracle_proposal_encoded')['phase'] = 'not_encoded'
        next(e for e in es if e['phase'] == 'oracle_propose_end')['error'] = 'context canceled'
        wait = analyze_upstream(es)['proposal_wait_observations'][0]
        self.assertFalse(wait['encoded'])
        self.assertEqual(wait['disposition'], 'error')

    def test_complementary_process_slices_refused(self):
        es = fixture()
        es[-1]['session'] = 'different-process'
        with self.assertRaises(ValueError):
            analyze_upstream(es)

    def test_large_descriptor_cardinality_and_nil(self):
        es = fixture()
        parent = next(e for e in es if e['phase'] == 'oracle_received' and e['message'] == 2)
        parent.pop('txns')
        parent['txn_count'] = 2
        # Place children at end to emphasize explicit parent IDs over adjacency.
        for ordinal in range(2):
            es.append(dict(version=1, session='test', sequence=len(es)+1, index=0, phase='decision_part',
                           elapsed_ns=(len(es)+1)*100, parent_sequence=parent['sequence'],
                           ordinal=ordinal, start_ts=2+ordinal, commit_ts=12+ordinal))
        self.assertEqual(len(complete_events(es)[parent['sequence']-1]['txns']), 2)
        self.assertEqual(analyze_upstream(es)['received_messages'], 3)
        es[-1]['ordinal'] = 0
        with self.assertRaises(ValueError):
            analyze_upstream(es)


if __name__ == '__main__':
    unittest.main()
