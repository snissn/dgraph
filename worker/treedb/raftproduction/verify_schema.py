#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded gRPC schema correctness, Zero conflict decisions and process recovery.

Diagnostic only: no throughput, power-loss or production-tier acceptance. Build
the companion separately; this launcher never builds or silently retries a run.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import selectors
import signal
import socket
import subprocess
import sys
import time
import urllib.request

sys.dont_write_bytecode = True
from verify_history_floor import healthy, stop


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1 << 20), b''):
            value.update(data)
    return value.hexdigest()


def source_identity(path):
    def git(*args):
        return subprocess.check_output(['git', '-C', str(path), *args])
    others = git('ls-files', '--others', '--exclude-standard', '-z').split(b'\0')
    return {'directory': str(path), 'head': git('rev-parse', 'HEAD').decode().strip(),
            'status': git('status', '--porcelain').decode(),
            'diff_sha256': hashlib.sha256(git('diff', '--binary', 'HEAD')).hexdigest(),
            'untracked_sha256': {name.decode(): digest(path / name.decode())
                                 for name in others if name and (path / name.decode()).is_file()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dgraph-bin', required=True)
    parser.add_argument('--proof-bin', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--policy', choices=['raft_checkpoint', 'badger'], required=True)
    parser.add_argument('--zero-offset', type=int, default=46000)
    parser.add_argument('--alpha-offset', type=int, default=47000)
    parser.add_argument('--source-dir', default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument('--gomap-source-dir', default=str(Path(__file__).resolve().parents[4] / 'gomap'))
    parser.add_argument('--go-bin', default='go', help='tool for read-only go version -m binary identity')
    args = parser.parse_args()
    binary, companion, out = (Path(value).resolve() for value in
                              [args.dgraph_bin, args.proof_bin, args.out])
    out.mkdir(parents=True, exist_ok=False)
    token = 'schema-grpc-correctness-diagnostic'
    base = f'http://localhost:{8080 + args.alpha_offset}'
    result = {'success': False, 'diagnostic_only': True, 'policy': args.policy,
              'contract': 'real gRPC; public Alter/CommitNow ACK then immediate all-process SIGKILL; Alpha-only orderly pending checkpoint keeps Zero alive',
              'limitations': ['No deterministic delay between frontend schema admission and ordered Raft apply; delayed-admission race remains a focused Go proof.',
                              'Small fixtures do not force cold splits or qualify scratch/rebuild resource budgets.',
                              'Process SIGKILL is not a power-loss experiment.',
                              'Historical raw gRPC proof covers UID posting history; schema definitions themselves are current, not historical snapshots.',
                              'Root namespace only; non-root namespace isolation remains a focused ordered-apply proof.'],
              'commands': [], 'phases': [], 'cuts': [], 'runtime': []}
    logs, zero, alpha, driver = [], None, None, None
    zero_generation = alpha_generation = 0
    state_path = out / 'state.json'

    def save():
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')

    def launch(label, command):
        log = (out / (label + '.log')).open('xb')
        logs.append(log)
        result['commands'].append(command)
        return subprocess.Popen(command, stdout=log, stderr=log)

    def start_zero():
        nonlocal zero, zero_generation
        zero_generation += 1
        zero = launch(f'zero-{zero_generation}', [str(binary), 'zero', '--wal', str(out/'zw'),
            '--replicas=1', f'--port_offset={args.zero_offset}', '--survive=filesystem'])
        healthy(zero, f'http://localhost:{6080 + args.zero_offset}')

    def start_alpha():
        nonlocal alpha, alpha_generation
        alpha_generation += 1
        selector = ('backend=badger;tier=production;durability=durable;events=false'
                    if args.policy == 'badger' else
                    'backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true')
        alpha = launch(f'alpha-{alpha_generation}', [str(binary), 'alpha', '--zero',
            f'localhost:{5080 + args.zero_offset}', '--postings', str(out/'p'), '--wal', str(out/'w'),
            f'--port_offset={args.alpha_offset}', '--survive=filesystem', '--security', f'token={token}',
            '--posting-store', selector, '--badger', 'syncwrites=' + ('true' if args.policy == 'badger' else 'false')])
        healthy(alpha, base)
        capture_runtime()

    def capture_runtime():
        if digest(binary) != result['dgraph_sha256'] or digest(companion) != result['proof_sha256']:
            raise RuntimeError('executed binary changed during campaign')
        witnesses = []
        for role, generation, process in [('zero', zero_generation, zero), ('alpha', alpha_generation, alpha)]:
            log = out / f'{role}-{generation}.log'
            raw = log.read_text()
            lines = [line for line in raw.splitlines() if 'x.WorkerConfig:' in line]
            if len(lines) != 1 or re.findall(r'(?:^|[ {])HardSync:(true|false)(?:[ }]|$)', lines[0]) != ['true']:
                raise RuntimeError(f'{role} lacks unambiguous resolved filesystem survival')
            witnesses.append({'role': role, 'pid': process.pid, 'generation': generation,
                              'config': lines[0], 'log_sha256_at_health': digest(log)})
        with urllib.request.urlopen(base + '/debug/store', timeout=30) as response:
            store = response.read().decode()
        def section(prefix):
            if store.count(prefix) != 1:
                raise RuntimeError('missing or ambiguous ' + prefix)
            tail = store.split(prefix, 1)[1]
            if ']' not in tail:
                raise RuntimeError('truncated store status')
            fields = {}
            for item in tail.split(']', 1)[0].split():
                if ':' in item:
                    key, value = item.split(':', 1)
                    if key in fields:
                        raise RuntimeError('duplicate store status field ' + key)
                    fields[key] = value
            return fields
        status, stats = section('status=map['), section('stats=map[')
        expected = ('badger', 'syncwrites=true') if args.policy == 'badger' else ('treedb', 'raft_checkpoint')
        if (status.get('backend'), status.get('profile')) != expected:
            raise RuntimeError(f'posting route mismatch: {status}')
        if args.policy == 'raft_checkpoint' and stats.get('treedb.cache.memtable_mode') != 'cow_ordered':
            raise RuntimeError('candidate did not activate cow_ordered')
        (out / f'runtime-alpha-{alpha_generation}.store.txt').write_text(store)
        entry = {'processes': witnesses, 'store_status': status,
                 'store_sha256': hashlib.sha256(store.encode()).hexdigest()}
        if alpha_generation > 1 and args.policy == 'raft_checkpoint':
            raw = (out / f'alpha-{alpha_generation}.log').read_text()
            recovery = re.search(r'recovery: certified=(\d+) oracle=(\d+)', raw)
            replay = re.search(r'committed replay completed through (\d+) before health', raw)
            if not recovery or not replay:
                raise RuntimeError('missing exact recovery/committed replay before health')
            entry['recovery'] = {'certified_index': int(recovery[1]), 'oracle': int(recovery[2]),
                                 'replayed_through': int(replay[1])}
        result['runtime'].append(entry)

    def phase(name):
        nonlocal driver
        command = [str(companion), '--addr', f'localhost:{9080 + args.alpha_offset}',
                   '--phase', name, '--auth-token', token]
        if state_path.exists():
            command += ['--state', str(state_path)]
        result['commands'].append(command)
        error_file = (out / (name + '.stderr')).open('xb')
        logs.append(error_file)
        # Unbuffered binary reads prevent TextIOWrapper read-ahead from hiding
        # the transcript line from communicate after consuming the cut event.
        driver = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error_file, bufsize=0)
        with selectors.DefaultSelector() as selector:
            selector.register(driver.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=180):
                raise RuntimeError('companion phase deadline: ' + name)
            first = driver.stdout.readline()
        try:
            event = json.loads(first)
        except (ValueError, TypeError) as error:
            (out / (name + '.jsonl')).write_bytes(first)
            raise RuntimeError('invalid companion event: ' + repr(first)) from error
        if event.get('event') == 'ack_cut' and event.get('success'):
            # Keep these adjacent to the first ACK event: no artifact writes,
            # status polls or validation reads before termination syscalls.
            try:
                alpha.send_signal(signal.SIGKILL)
                zero.send_signal(signal.SIGKILL)
            except Exception:
                (out / (name + '.jsonl')).write_bytes(first)
                raise
            killed_ns = time.time_ns()
            result['cuts'].append({'phase': name, 'response_to_kill_ns': killed_ns - event['response_unix_ns']})
            alpha.wait(timeout=15)
            zero.wait(timeout=15)
        remainder, _ = driver.communicate(timeout=20)
        (out / (name + '.jsonl')).write_bytes(first + remainder)
        if driver.returncode != 0 or not event.get('success'):
            raise RuntimeError(f'phase {name} failed: {event.get("error", "companion exit " + str(driver.returncode))}')
        state_path.write_text(json.dumps(event['state'], indent=2) + '\n')
        result['phases'].append({'phase': name, 'event': event['event'], 'transcript': name + '.jsonl'})
        save()
        if event['event'] == 'ack_cut':
            start_zero()
            start_alpha()
        elif event['event'] == 'alpha_restart':
            # Zero MUST stay alive: restarting Zero intentionally aborts old
            # pending timestamps and would not test conflict arbitration.
            alpha.send_signal(signal.SIGINT)
            alpha.wait(timeout=60)
            if alpha.returncode != 0:
                raise RuntimeError('Alpha orderly pending close failed')
            closed_log = (out / f'alpha-{alpha_generation}.log').read_text()
            if args.policy == 'raft_checkpoint' and not re.search(r'certified:.*pending=2(?:\s|$)', closed_log):
                raise RuntimeError('missing actual canonical two-pending checkpoint before reopen')
            result['pending_restart'] = {'zero_pid': zero.pid, 'alpha_exit': alpha.returncode,
                                         'log': f'alpha-{alpha_generation}.log'}
            if zero.poll() is not None:
                raise RuntimeError('Zero exited during pending-state reopen')
            start_alpha()

    try:
        result['dgraph_sha256'], result['proof_sha256'] = digest(binary), digest(companion)
        result['sources'] = {'dgraph': source_identity(Path(args.source_dir).resolve()),
                             'gomap': source_identity(Path(args.gomap_source_dir).resolve())}
        for label, path in [('dgraph', binary), ('proof', companion)]:
            info = subprocess.check_output([args.go_bin, 'version', '-m', str(path)], text=True)
            (out / (label + '.buildinfo.txt')).write_text(info)
            result[label + '_buildinfo_sha256'] = hashlib.sha256(info.encode()).hexdigest()
        for port in [5080+args.zero_offset, 6080+args.zero_offset, 7080+args.alpha_offset,
                     8080+args.alpha_offset, 9080+args.alpha_offset]:
            with socket.socket() as check:
                check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                check.bind(('127.0.0.1', port))
        save()
        start_zero()
        start_alpha()
        for name in ['seed', 'count-cut', 'count-check', 'conversion-cut', 'conversion-check',
                     'conversion-final', 'unique-prepare', 'unique-decide', 'final']:
            phase(name)
        if args.policy == 'raft_checkpoint':
            result['uncertified_suffix_replayed'] = any(
                entry.get('recovery', {}).get('replayed_through', 0) >
                entry.get('recovery', {}).get('certified_index', 0)
                for entry in result['runtime'])
            if not result['uncertified_suffix_replayed']:
                raise RuntimeError('no observed retained suffix replay beyond the certified index')
        result['sources_at_finish'] = {'dgraph': source_identity(Path(args.source_dir).resolve()),
                                      'gomap': source_identity(Path(args.gomap_source_dir).resolve())}
        if result['sources_at_finish'] != result['sources']:
            raise RuntimeError('source trees changed during the campaign; binary source attribution requires a new run')
        result['success'] = True
    except Exception as error:
        result['error'] = repr(error)
    finally:
        if driver is not None and driver.poll() is None:
            driver.kill()
            trailing, _ = driver.communicate(timeout=10)
            (out / 'interrupted-driver.stdout').write_bytes(trailing)
        stop(alpha)
        stop(zero)
        for log in logs:
            log.close()
        save()
    print(json.dumps(result, indent=2))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
