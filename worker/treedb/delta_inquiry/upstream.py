#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Observed Zero receive backlog; never counterfactual admission or physical sync."""
import argparse
import bisect
import collections
import json


def complete_events(events):
    if not events or [e['sequence'] for e in events] != list(range(1, len(events)+1)):
        raise ValueError('missing, reordered or mixed-process sequence')
    if any(e['version'] != 1 for e in events):
        raise ValueError('unsupported version')
    sessions = {e.get('session') for e in events}
    if sessions != {None} and (len(sessions) != 1 or not next(iter(sessions))):
        raise ValueError('missing or mixed process session')
    if events[0]['phase'] != 'enabled':
        raise ValueError('missing startup')
    times = [e['elapsed_ns'] for e in events]
    if any(type(t) is not int or t < 0 for t in times) or times != sorted(times):
        raise ValueError('invalid emission times')
    # Capture times may cross emission order: capture precedes the logging lock.
    for e in events:
        if 'captured_ns' in e and (type(e['captured_ns']) is not int or
                                  not 0 <= e['captured_ns'] <= e['elapsed_ns']):
            raise ValueError('invalid capture time')
    if sum(e['phase'] == 'apply_closed' for e in events) != 1:
        raise ValueError('incomplete apply trace')
    result = [dict(e) for e in events]
    parents = {e['sequence']: e for e in result if e.get('txn_count', 0) > 0}
    parts = collections.defaultdict(list)
    for e in events:
        if e['phase'] == 'decision_part':
            if e.get('parent_sequence') not in parents:
                raise ValueError('orphan decision part')
            parts[e['parent_sequence']].append(e)
    for seq, parent in parents.items():
        children = parts[seq]
        if [e['ordinal'] for e in children] != list(range(parent['txn_count'])):
            raise ValueError('missing or repeated decision part')
        parent['txns'] = [None if e.get('nil_txn') else
                          dict(start_ts=e.get('start_ts', 0), commit_ts=e.get('commit_ts', 0))
                          for e in children]
        parent['unknown_fields'] = parent.get('unknown_fields', False) or any(
            e.get('unknown_fields') for e in children)
    return result



class _CaptureIndex:
    """Capture-time queries, returned in original emission/insertion order."""
    def __init__(self, records):
        self.rank = {key: rank for rank, key in enumerate(records)}
        self.items = sorted(records, key=lambda key: records[key]['captured_ns'])
        self.times = [records[key]['captured_ns'] for key in self.items]

    def during(self, begin, end):
        lo = bisect.bisect_right(self.times, begin)
        hi = bisect.bisect_left(self.times, end)
        return sorted(self.items[lo:hi], key=self.rank.__getitem__)


class _PendingIndex:
    """Sweep observed receipt/handoff/consumption endpoints up to a wait start."""
    def __init__(self, received, admitted, consumed):
        self.rank = {key: rank for rank, key in enumerate(received)}
        self.events = sorted((event['captured_ns'], kind, key)
                             for kind, records in enumerate((received, admitted, consumed))
                             for key, event in records.items())
        self._reset()

    def _reset(self):
        self.position = 0
        self.previous = -1
        self.received = set()
        self.admitted = set()

    def at(self, begin):
        # Production starts are monotonic within each stream. Preserve the
        # reference result even for synthetic inputs with reversed wait starts.
        if begin < self.previous:
            self._reset()
        while self.position < len(self.events) and self.events[self.position][0] <= begin:
            _, kind, key = self.events[self.position]
            if kind == 0:
                self.received.add(key)
            elif kind == 1:
                if key in self.received:
                    self.admitted.add(key)
            else:
                self.received.discard(key)
                self.admitted.discard(key)
            self.position += 1
        self.previous = begin
        return (sorted(self.received, key=self.rank.__getitem__),
                sorted(self.admitted, key=self.rank.__getitem__))


# The existing Raft apply observer publishes oracle_begin/end without a stream.
# Only this explicit namespace belongs to the Zero-receive owner. Unknown oracle
# phases fail closed rather than silently disappearing from completeness checks.
UPSTREAM_PHASES = frozenset((
    'oracle_stream_open', 'oracle_receiver_closed', 'oracle_stream_closed',
    'oracle_closed', 'oracle_receive_end', 'oracle_received', 'oracle_send_begin',
    'oracle_send_end', 'oracle_send_canceled', 'oracle_consumed',
    'oracle_aggregate_ready', 'oracle_aggregate_skipped', 'oracle_aggregate_failed',
    'oracle_propose_begin', 'oracle_propose_end', 'oracle_proposal_encoded',
))
LEGACY_ORACLE_PHASES = frozenset(('oracle_begin', 'oracle_end'))


def analyze_upstream(events):
    events = complete_events(events)
    if not events[0].get('session'):
        raise ValueError('upstream capture requires process session identity')
    if any(e['phase'].startswith('oracle_') and e['phase'] not in
           UPSTREAM_PHASES | LEGACY_ORACLE_PHASES for e in events):
        raise ValueError('unknown oracle observation phase')
    oracle = [e for e in events if e['phase'] in UPSTREAM_PHASES]
    if not oracle:
        raise ValueError('no upstream observer records; no zero-frequency inference')
    if sum(e['phase'] == 'oracle_closed' for e in oracle) != 1:
        raise ValueError('incomplete oracle owner')
    streams = [e['stream'] for e in oracle if e['phase'] == 'oracle_stream_open']
    if streams != list(range(1, len(streams)+1)):
        raise ValueError('missing or repeated stream epoch')
    for stream in streams:
        for phase in ('oracle_receiver_closed', 'oracle_stream_closed'):
            if sum(e['phase'] == phase and e.get('stream') == stream for e in oracle) != 1:
                raise ValueError('incomplete stream')
    if any(e.get('stream', 0) not in streams for e in oracle if e['phase'] != 'oracle_closed'):
        raise ValueError('unknown stream epoch')
    per_message, per_aggregate = collections.defaultdict(list), collections.defaultdict(list)
    for e in oracle:
        if e.get('message'):
            per_message[e['stream'], e['message']].append(e)
        if e.get('aggregate'):
            per_aggregate[e['stream'], e['aggregate']].append(e)
    def one(records, phase):
        selected = [e for e in records if e['phase'] == phase]
        if len(selected) != 1:
            raise ValueError('missing or repeated '+phase)
        return selected[0]
    received, admitted, consumed = {}, {}, {}
    for key, records in per_message.items():
        r = one(records, 'oracle_received')
        one(records, 'oracle_send_begin')
        ends = [e for e in records if e['phase'] in ('oracle_send_end', 'oracle_send_canceled')]
        if len(ends) != 1:
            raise ValueError('incomplete send outcome')
        received[key] = r
        if ends[0]['phase'] == 'oracle_send_end':
            admitted[key] = ends[0]
        cs = [e for e in records if e['phase'] == 'oracle_consumed']
        if len(cs) > 1 or (cs and key not in admitted):
            raise ValueError('invalid message consumption')
        if cs:
            consumed[key] = cs[0]
        for e in (r, ends[0], *cs):
            if 'captured_ns' not in e:
                raise ValueError('missing capture time')
        if r['captured_ns'] > ends[0]['captured_ns'] or (
                cs and r['captured_ns'] > cs[0]['captured_ns']):
            raise ValueError('invalid message partial order')
    for stream in streams:
        ids = sorted(message for s, message in received if s == stream)
        if ids != list(range(1, len(ids)+1)):
            raise ValueError('missing receive message')
    received_index, admitted_index, pending_index = {}, {}, {}
    for stream in streams:
        rs = {key: e for key, e in received.items() if key[0] == stream}
        ads = {key: e for key, e in admitted.items() if key[0] == stream}
        cs = {key: e for key, e in consumed.items() if key[0] == stream}
        received_index[stream] = _CaptureIndex(rs)
        admitted_index[stream] = _CaptureIndex(ads)
        pending_index[stream] = _PendingIndex(rs, ads, cs)
    waits = []
    for (stream, aggregate), records in sorted(per_aggregate.items()):
        members = [e['message'] for e in records if e['phase'] == 'oracle_consumed']
        terminal = [e for e in records if e['phase'] in (
            'oracle_aggregate_ready', 'oracle_aggregate_skipped', 'oracle_aggregate_failed')]
        # Cancellation/leadership/closed channel may discard a partly built original.
        if not terminal:
            waits.append(dict(stream=stream, aggregate=aggregate, disposition='unfinished_aggregate', members=members))
            continue
        if len(terminal) != 1:
            raise ValueError('multiple aggregate dispositions')
        desc = terminal[0]
        if desc['phase'] != 'oracle_aggregate_ready':
            waits.append(dict(stream=stream, aggregate=aggregate, disposition=desc['phase'], members=members))
            continue
        if desc['entries'] != len(members):
            raise ValueError('aggregate membership mismatch')
        starts = [e for e in records if e['phase'] == 'oracle_propose_begin']
        if [e['attempt'] for e in starts] != list(range(1, len(starts)+1)) or not starts:
            raise ValueError('invalid attempt sequence')
        for start in starts:
            attempt = start['attempt']
            attempt_records = [e for e in records if e.get('attempt') == attempt]
            end = one(attempt_records, 'oracle_propose_end')
            encoded_records = [e for e in attempt_records if e['phase'] == 'oracle_proposal_encoded']
            if len(encoded_records) > 1 or (not encoded_records and not end.get('error')):
                raise ValueError('missing or repeated proposal encoding')
            encoded = encoded_records[0] if encoded_records else {}
            begin_ns, end_ns = start['captured_ns'], end['captured_ns']
            if begin_ns > end_ns:
                raise ValueError('reversed proposal wait')
            received_pending, admitted_pending = pending_index[stream].at(begin_ns)
            during = received_index[stream].during(begin_ns, end_ns)
            admission = admitted_index[stream].during(begin_ns, end_ns)
            classifications = collections.Counter()
            decision_counts = collections.Counter()
            decision_values = collections.defaultdict(set)
            for key in during:
                for txn in received[key].get('txns', []):
                    if txn is not None:
                        decision_counts[txn.get('start_ts', 0)] += 1
                        decision_values[txn.get('start_ts', 0)].add(txn.get('commit_ts', 0))
            for key in during:
                txns = received[key].get('txns', [])
                if received[key].get('unknown_fields'):
                    label = 'unknown_fields'
                elif not txns:
                    label = 'watermark_only'
                elif any(t is None or not t.get('start_ts') or (
                        t.get('commit_ts', 0) and t['commit_ts'] <= t['start_ts']) for t in txns):
                    label = 'invalid_decision'
                elif any(not t.get('commit_ts') for t in txns):
                    label = 'contains_abort'
                elif len({t['start_ts'] for t in txns}) != len(txns):
                    label = 'repeated_start_ts'
                else:
                    label = 'commit_decisions_unqualified'
                classifications[label] += 1
            waits.append(dict(stream=stream, aggregate=aggregate, attempt=attempt,
                              disposition='error' if end.get('error') else 'returned',
                              members=members, encoded=bool(encoded), proposal_key=encoded.get('key'),
                              received_unconsumed_at_begin=[m for _, m in received_pending],
                              admitted_unconsumed_at_begin=[m for _, m in admitted_pending],
                              proposal_bytes=encoded.get('bytes'), received_messages=[m for _, m in during],
                              admitted_messages=[m for _, m in admission],
                              received_classifications=dict(classifications),
                              received_decisions=sum(decision_counts.values()),
                              repeated_start_ts=[s for s, n in decision_counts.items() if n > 1],
                              contradictory_start_ts=[s for s, values in decision_values.items() if len(values) > 1],
                              received_frontiers=[dict(message=m,
                                  checksum_sequence=received[s, m].get('checksum_sequence', 0),
                                  checksum_digest=received[s, m].get('checksum_digest', '')) for s, m in during]))
    return dict(diagnostic_only=True, counterfactual_aggregate_boundaries_known=False,
                semantic_independence_proven=False, physical_sync_overlap_proven=False,
                session=events[0]['session'], streams_opened=len(streams), received_messages=len(received), admitted_messages=len(admitted),
                consumed_messages=len(consumed),
                unconsumed_messages=[dict(stream=s, message=m) for s, m in received if (s, m) not in consumed],
                proposal_wait_observations=waits,
                note='Receipt/admission observations in this perturbed process only; no Zero send time, '
                     'counterfactual B/C aggregates, prewrite eligibility, physical sync estimate or performance claim.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log')
    args = parser.parse_args()
    with open(args.log) as source:
        events = [json.loads(line.split('DELTA_INQUIRY ', 1)[1]) for line in source if 'DELTA_INQUIRY ' in line]
    print(json.dumps(analyze_upstream(events), indent=2))


if __name__ == '__main__':
    main()
