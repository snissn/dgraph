#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Diagnostic native voter join/rejoin with pending state and all-process crash.

No timing, power-loss or production-tier acceptance is inferred from this proof.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import signal
import subprocess
import time
from verify_history_floor import request, healthy, stop


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dgraph-bin', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--zero-offset', type=int, default=41000)
    parser.add_argument('--alpha-offset', type=int, default=42000)
    args = parser.parse_args()
    binary, out = Path(args.dgraph_bin).resolve(), Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    token = 'native-peer-diagnostic'
    result = {'success': False, 'diagnostic_only': True, 'commands': [],
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    logs, alphas, zero = [], {}, None
    bases = {i: f'http://localhost:{8080 + args.alpha_offset + i * 100}' for i in range(1, 4)}
    def start(label, command):
        log = (out / (label + '.log')).open('xb')
        logs.append(log)
        result['commands'].append([str(binary)] + command)
        return subprocess.Popen([str(binary)] + command, stdout=log, stderr=log)
    def start_zero(label):
        process = start('zero-' + label, ['zero', '--wal', str(out / 'zw'), '--replicas=3',
            f'--port_offset={args.zero_offset}', '--survive=filesystem'])
        healthy(process, f'http://localhost:{6080 + args.zero_offset}')
        return process
    def start_alpha(i, label):
        return start(f'alpha{i}-' + label, ['alpha', '--zero', f'localhost:{5080 + args.zero_offset}',
            '--postings', str(out / f'p{i}'), '--wal', str(out / f'w{i}'),
            f'--port_offset={args.alpha_offset + i * 100}', '--survive=filesystem',
            '--raft', f'idx={i};group=1', '--security', f'token={token}',
            '--posting-store', 'backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true',
            '--badger', 'syncwrites=false'])
    def call(i, path, body, content='application/json', allow_error=False):
        return request(bases[i], path, body, token, content, allow_error)
    def floor(value):
        deadline = time.monotonic() + 45
        while True:
            got = call(1, '/admin', {'query': 'mutation($s:UInt64!){retirePostingHistory(floor:$s){floor}}',
                                  'variables': {'s': value}}, allow_error=True)
            if not got.get('errors'):
                return got
            if 'precede assigned' not in str(got) or time.monotonic() >= deadline:
                raise RuntimeError(str(got))
            time.sleep(.1)
    def verify(i, value):
        got = call(i, '/query', '{q(func:eq(peer_value,"' + value + '")){peer_value}}', 'application/dql')
        if got['data']['q'] != [{'peer_value': value}]:
            raise RuntimeError(f'peer{i} missing {value}: {got}')
    def native(i, label, pending=None):
        data = (out / f'alpha{i}-{label}.log').read_text()
        matches = re.findall(r'Native checkpoint installed: index=(\d+) source=(\d+) receiver=(\d+) pending=(\d+) checksum_sequence=(\d+) terminal=(\d+)', data)
        if not matches or (pending is not None and not any(int(m[3]) == pending and int(m[4]) > 0 and int(m[5]) > 0 for m in matches)):
            raise RuntimeError(f'peer{i} missing native install with pending={pending}, checksum authority and terminal decisions')
        installed = data.rfind('Native checkpoint installed:')
        barrier = data.rfind('committed replay completed through')
        ready = data.rfind('Server is ready: OK')
        if not installed < barrier < ready:
            raise RuntimeError(f'peer{i} health precedes snapshot catch-up barrier')
        result[f'native_{i}_{label}'] = matches
    try:
        zero = start_zero('initial')
        alphas[1] = start_alpha(1, 'initial')
        healthy(alphas[1], bases[1])
        call(1, '/alter', 'peer_value: string @index(exact) .', 'application/rdf')
        seed = call(1, '/mutate?commitNow=true', {'set': [{'uid': '_:seed', 'peer_value': 'seed'}]})
        seed_floor = int(seed['extensions']['txn']['start_ts'])
        pending = call(1, '/mutate', {'set': [{'uid': '_:pending', 'peer_value': 'pending'}]})
        txn = pending['extensions']['txn']
        result['pending_transaction'] = txn
        result['seed_floor'] = floor(seed_floor)
        # A resolved transaction ABOVE S must travel in the exact native root
        # alongside pending state and the sequenced Zero checksum authority.
        result['retained_decision'] = call(1, '/mutate?commitNow=true', {'set': [{'uid': '_:resolved', 'peer_value': 'resolved'}]})
        verify(1, 'resolved')
        for i in [2, 3]:
            alphas[i] = start_alpha(i, 'join')
            healthy(alphas[i], bases[i])
            verify(i, 'seed')
            verify(i, 'resolved')
            native(i, 'join', 1)
        result['pending_commit'] = call(3, '/commit?startTs=' + str(txn['start_ts']),
            {'keys': txn.get('keys', []), 'preds': txn.get('preds', [])})
        for i in [1, 2, 3]:
            verify(i, 'pending')
        alphas[2].kill()
        alphas[2].wait(timeout=15)
        later = call(1, '/mutate?commitNow=true', {'set': [{'uid': '_:later', 'peer_value': 'after-offline'}]})
        result['offline_floor'] = floor(int(later['extensions']['txn']['start_ts']))
        alphas[2] = start_alpha(2, 'rejoin')
        healthy(alphas[2], bases[2])
        verify(2, 'after-offline')
        native(2, 'rejoin')
        acknowledged = call(1, '/mutate?commitNow=true', {'set': [{'uid': '_:ack', 'peer_value': 'immediate-ack'}]})
        # No network or filesystem work between the acknowledged response and
        # these adjacent termination syscalls.
        for i in [1, 2, 3]:
            alphas[i].send_signal(signal.SIGKILL)
        zero.send_signal(signal.SIGKILL)
        for process in list(alphas.values()) + [zero]:
            process.wait(timeout=15)
        result['immediate_ack'] = acknowledged
        zero = start_zero('restart')
        for i in [1, 2, 3]:
            alphas[i] = start_alpha(i, 'restart')
        for i in [1, 2, 3]:
            healthy(alphas[i], bases[i])
            for value in ['seed', 'resolved', 'pending', 'after-offline', 'immediate-ack']:
                verify(i, value)
        result['success'] = True
    except Exception as error:
        result['error'] = repr(error)
    finally:
        # Stop concurrently so shutdown of one quorum member cannot delay the
        # others beyond their own shutdown deadline.
        for process in list(alphas.values()) + [zero]:
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGINT)
        for process in list(alphas.values()) + [zero]:
            stop(process)
        for log in logs:
            log.close()
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
