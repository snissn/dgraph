#!/usr/bin/env python3
# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Diagnostic actual alter/drop, pending refusal, Zero outage and orderly close.

Process correctness only. No timing, power-loss or production-tier acceptance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import time
from verify_history_floor import request, healthy, stop


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dgraph-bin', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--zero-offset', type=int, default=44000)
    p.add_argument('--alpha-offset', type=int, default=45000)
    a = p.parse_args()
    binary, out = Path(a.dgraph_bin).resolve(), Path(a.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    base, token = f'http://localhost:{8080+a.alpha_offset}', 'admin-lifecycle-diagnostic'
    result = {'success': False, 'diagnostic_only': True, 'commands': [],
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    zero = alpha = None
    logs = []
    def launch(label, command):
        log = (out / (label + '.log')).open('xb'); logs.append(log)
        result['commands'].append([str(binary)] + command)
        return subprocess.Popen([str(binary)] + command, stdout=log, stderr=log)
    def start_zero(label):
        proc = launch('zero-'+label, ['zero','--wal',str(out/'zw'),'--replicas=1',
            f'--port_offset={a.zero_offset}','--survive=filesystem'])
        healthy(proc, f'http://localhost:{6080+a.zero_offset}')
        return proc
    def start_alpha(label):
        proc = launch('alpha-'+label, ['alpha','--zero',f'localhost:{5080+a.zero_offset}',
            '--postings',str(out/'p'),'--wal',str(out/'w'),f'--port_offset={a.alpha_offset}',
            '--survive=filesystem','--security',f'token={token}',
            '--posting-store','backend=treedb;tier=benchmark_minimal;durability=raft_checkpoint;events=true'])
        healthy(proc, base)
        return proc
    def call(path, body, content='application/json', error=False):
        return request(base,path,body,token,content,error)
    def alter(op):
        deadline = time.monotonic()+30
        while True:
            got=call('/alter',op,error=True)
            if not got.get('errors'): return got
            if 'pending transactions' not in str(got) or time.monotonic()>=deadline: raise RuntimeError(str(got))
            time.sleep(.1)
    def mutate(value, commit=True, start=None):
        path='/mutate'+('?commitNow=true' if commit else '')
        if start is not None: path+='?startTs='+str(start)
        return call(path,{'set':[{'uid':'_:x','admin_value':value}]})
    def query(value, expected=True, ts=None):
        got=call('/query'+('' if ts is None else '?startTs='+str(ts)),
            '{q(func:eq(admin_value,"'+value+'")){admin_value}}','application/dql')
        want=[{'admin_value':value}] if expected else []
        if got['data']['q'] != want: raise RuntimeError(f'query {value}: {got}')
        return got
    try:
        zero=start_zero('initial'); alpha=start_alpha('initial')
        alter('admin_value: string @index(exact) .')
        mutate('before-data')
        pending=mutate('pending',False)['extensions']['txn']
        refused=call('/alter',{'drop_op':'DATA'},error=True)
        if 'pending transactions' not in str(refused): raise RuntimeError(f'missing pure refusal: {refused}')
        result['pending_drop_refusal']=refused
        query('before-data')
        call('/commit?startTs='+str(pending['start_ts'])+'&abort=true',{})
        result['drop_data']=alter({'drop_op':'DATA'})
        query('before-data',False)
        mutate('before-predicate'); query('before-predicate')
        result['drop_predicate']=alter({'drop_attr':'admin_value'})
        definition=call('/query','schema(pred:[admin_value]){predicate}','application/dql')
        if definition['data'].get('schema'): raise RuntimeError(f'predicate schema survived: {definition}')
        alter('admin_value: string @index(exact) .')
        mutate('before-all'); query('before-all')
        acknowledged=call('/alter',{'drop_all':True})
        # Adjacent termination syscalls immediately follow the public alter ACK.
        alpha.send_signal(signal.SIGKILL); zero.send_signal(signal.SIGKILL)
        alpha.wait(timeout=15); zero.wait(timeout=15)
        result['drop_all_ack']=acknowledged
        zero=start_zero('after-drop'); alpha=start_alpha('after-drop')
        definition=call('/query','schema(pred:[admin_value]){predicate}','application/dql')
        if definition['data'].get('schema'): raise RuntimeError(f'drop all schema survived: {definition}')
        alter('admin_value: string @index(exact) .')
        committed=mutate('after-drop')
        fixed_ts=committed['extensions']['txn']['commit_ts']
        query('after-drop',ts=fixed_ts)
        pending=mutate('outage-first',False)['extensions']['txn']
        zero.kill(); zero.wait(timeout=15)
        result['zero_outage_read']=query('after-drop',ts=fixed_ts)
        alpha.send_signal(signal.SIGINT)
        try:
            alpha.wait(timeout=15)
        except subprocess.TimeoutExpired:
            alpha.send_signal(signal.SIGQUIT)
            alpha.wait(timeout=15)
            raise RuntimeError('Alpha close timed out; SIGQUIT stack retained')
        if alpha.returncode != 0:
            raise RuntimeError(f'Alpha close returned {alpha.returncode}')
        result['orderly_close_exit_code'] = alpha.returncode
        # The orderly Alpha close must force a canonical pending checkpoint
        # while Zero remains absent. Its log is retained as lifecycle evidence.
        zero=start_zero('after-outage'); alpha=start_alpha('after-close')
        keys=pending.get('keys',[])
        preds=pending.get('preds',[])
        decision=call('/commit?startTs='+str(pending['start_ts']),{'keys':keys,'preds':preds},error=True)
        result['restored_pending_decision']=decision
        if 'aborted' not in str(decision).lower(): raise RuntimeError(f'missing Zero restart abort: {decision}')
        query('outage-first',False); query('after-drop')
        mutate('after-close'); query('after-close')
        result['success']=True
    except Exception as error:
        result['error']=repr(error)
    finally:
        stop(alpha); stop(zero)
        for log in logs: log.close()
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
    return 0 if result['success'] else 1


if __name__=='__main__': raise SystemExit(main())
