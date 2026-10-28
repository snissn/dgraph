# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import random
import unittest
from unittest import mock
import upstream
from test_upstream import fixture


class BruteCaptureIndex:
    def __init__(self, records):
        self.records = records

    def during(self, begin, end):
        return [key for key, event in self.records.items()
                if begin < event['captured_ns'] < end]


class BrutePendingIndex:
    def __init__(self, received, admitted, consumed):
        self.received, self.admitted, self.consumed = received, admitted, consumed

    def at(self, begin):
        received = [key for key, event in self.received.items()
                    if event['captured_ns'] <= begin and (key not in self.consumed or
                    self.consumed[key]['captured_ns'] > begin)]
        admitted = [key for key in received if key in self.admitted and
                    self.admitted[key]['captured_ns'] <= begin]
        return received, admitted


class IndexedTests(unittest.TestCase):
    def test_capture_overlap_equal_boundaries_and_emission_order(self):
        # The insertion order intentionally differs from observation-time order.
        records = {(1, n): dict(captured_ns=t) for n, t in enumerate((30, 10, 20, 20, 40))}
        indexed, brute = upstream._CaptureIndex(records), BruteCaptureIndex(records)
        for begin in (0, 10, 20, 30, 40):
            for end in (begin, 20, 30, 40, 50):
                if end >= begin:
                    self.assertEqual(indexed.during(begin, end), brute.during(begin, end))

    def test_pending_sweep_matches_reference_with_delayed_send_return(self):
        rng = random.Random(34)
        received, admitted, consumed = {}, {}, {}
        for n in range(200):
            key = (1, n)
            captured = rng.randrange(1, 60)
            received[key] = dict(captured_ns=captured)
            if rng.randrange(4):
                admitted[key] = dict(captured_ns=captured+rng.randrange(20))
                if rng.randrange(3):
                    # Consumption may precede the sender's observed return.
                    consumed[key] = dict(captured_ns=captured+rng.randrange(20))
        indexed = upstream._PendingIndex(received, admitted, consumed)
        brute = BrutePendingIndex(received, admitted, consumed)
        # Repeated/equal and reversed synthetic starts exercise reset behavior.
        for begin in list(range(90))+[20, 20, 10, 80, 80]:
            self.assertEqual(indexed.at(begin), brute.at(begin))

    def test_full_results_equal_original_queries(self):
        cases = [fixture()]
        changed = copy.deepcopy(fixture())
        for event in changed:
            if event.get('message') == 2:
                event['captured_ns'] = 650
        cases.append(changed)
        changed = copy.deepcopy(fixture())
        next(e for e in changed if e['phase'] == 'oracle_received' and e['message'] == 2)['captured_ns'] = 799
        cases.append(changed)  # exactly at proposal-begin observation
        for events in cases:
            actual = upstream.analyze_upstream(events)
            with mock.patch.object(upstream, '_CaptureIndex', BruteCaptureIndex), \
                 mock.patch.object(upstream, '_PendingIndex', BrutePendingIndex):
                expected = upstream.analyze_upstream(events)
            self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
