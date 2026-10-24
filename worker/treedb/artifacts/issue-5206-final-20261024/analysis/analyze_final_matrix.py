#!/usr/bin/env python3
"""Read the three frozen matrix arms. No execution, network, or source mutation.

Writes a new JSON/Markdown pair only after all original 36 + 6 profile rows exist.
This supplementary reader does not replace the maintained reports/witness verifier.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

LOCK = 'STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE'
MAPPING_SHA = '4b4eda272d3b68fe0ee2bd455b32ffbff404ba89a8c5e21844ee4fcc5c378df2'
ARMS = {
    'process': ('process-evidence', 'process-12', 'process'),
    'filesystem': ('filesystem-evidence', 'filesystem-12', 'filesystem'),
    'baseline': ('baseline-evidence', 'baseline-filesystem-12', 'filesystem'),
}
IDENTITIES = {
    False: ('bad4ced2c88e15076f73e3647a72e965f63be2b9',
            '29a2fdf153ce6e214abf9502d7970de727a38967',
            '46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175'),
    True: ('7f2720bf0d7ed90e82b04c52911bff92e9f22726',
           'fc3c3db97ec2954d47a1adf731d501e7b73f181f',
           '58075e248543eb56ec618fe4099c04fd7fc741817558765e83e98af6432960a8'),
}
METRICS = ('p95', 'p99', 'cpu_seconds', 'rss_peak_bytes',
           'disk_logical_bytes', 'disk_allocated_bytes',
           'recovery_read_seconds', 'recovery_read_write_seconds')
WORKLOAD = {
    'recovery_probe': 'read-commit-read-v1',
    'dataset_nodes': 500, 'concurrency': 4, 'warmup_ops': 100, 'timed_ops': 2000,
    'query_mix_percent': {'one_hop_read': 20, 'point_read': 60, 'write': 20},
    'topology': 'single-zero-single-alpha', 'seed': 20260711,
}
BOUNDS = [
    'Campaign STOP is locked by the original process arm. No overall PASS is emitted.',
    'Profiles are excluded diagnostic rows, never pooled with the original 36.',
    'The original 0.97 throughput parity rule uses a ratio of independent medians.',
    'Filesystem source comparisons pair the same backend/class/repeat; Badger drift is not normalized away.',
    'Ancillary >5% paired-median growth requires FAIL/review. <=5% is not automatic acceptance.',
    'Missing, unavailable, nonfinite, or nonpositive ratio operands are UNQUALIFIED; no imputation or selected-repeat median.',
    'No original process baseline exists; process/filesystem contrasts are not source-regression qualification.',
    'p95/p99 are aggregate mixed-operation latency; CPU is timed Alpha /proc, not whole-system CPU.',
    'RSS is Alpha VmHWM including setup, not timed live RSS or equilibrium.',
    'Disk covers postings only; read logical and allocated bytes together, not full Raft/Zero/temp storage.',
    'Aligned recovery is clean Alpha restart with Zero alive, not crash/power-loss latency.',
    'Legacy recovery_seconds is HTTP health and remains descriptive only.',
    'Operational counters retain per-repeat availability and source; zero can be a valid measured counter.',
    'GC/flush/checkpoint definitions differ by backend and cannot establish equal work or equilibrium.',
    'Whole-system CPU, per-op allocations, equivalent physical write amplification, physical I/O wait/bytes, live Raft debt, and complete ACK-phase timing are unavailable.',
    'This artifact reader checks stored source/manifest bindings and original reports; it does not re-execute binary/module or runtime-sidecar verification.',
]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def positive(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value > 0)


def observation(row, metric):
    if metric in ('p95', 'p99'):
        value = row.get('latency_ms', {}).get(metric)
        return {'available': positive(value), 'value': value, 'unit': 'ms',
                'source': 'aggregate mixed-operation latency_ms.' + metric}
    return row.get('metrics', {}).get(metric, {
        'available': False, 'value': None, 'reason': 'metric missing'})


def pair_changes(candidate, baseline, metric, descriptive=False):
    pairs = []
    for repeat in (1, 2, 3):
        c, b = observation(candidate[repeat], metric), observation(baseline[repeat], metric)
        valid = c.get('available') is True and b.get('available') is True
        valid = valid and positive(c.get('value')) and positive(b.get('value'))
        valid = valid and c.get('unit') == b.get('unit')
        pairs.append({'repeat': repeat, 'candidate': c, 'baseline': b,
                      'change_percent': 100 * (c['value'] / b['value'] - 1) if valid else None,
                      'status': 'OBSERVED' if valid else 'UNQUALIFIED'})
    complete = all(p['change_percent'] is not None for p in pairs)
    median = statistics.median(p['change_percent'] for p in pairs) if complete else None
    status = ('UNQUALIFIED' if not complete else 'DESCRIPTIVE_ONLY' if descriptive else
              'FAIL_REVIEW_REQUIRED' if median > 5 else 'WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE')
    return {'metric': metric, 'pairs': pairs, 'paired_median_change_percent': median,
            'status': status}


def bind_manifest(manifest, receipt, baseline):
    revision, gomap, binary = IDENTITIES[baseline]
    require(manifest.get('version') == (1 if baseline else 2), 'manifest version mismatch')
    require(manifest.get('vcs_revision') == revision, 'manifest VCS mismatch')
    require(manifest.get('vcs_modified') == 'false', 'manifest modified source')
    require(manifest.get('dgraph_source', {}).get('revision') == revision, 'Dgraph source mismatch')
    require(manifest.get('gomap_source', {}).get('revision') == gomap, 'Gomap source mismatch')
    require(manifest.get('server_sha256') == binary == receipt.get('executed_server_sha256'),
            'executed binary/manifest/receipt mismatch')
    require(receipt.get('source_sha') == revision, 'invocation source mismatch')
    require(manifest.get('go_version') == 'go1.26.9', 'manifest toolchain mismatch')
    if baseline:
        require(manifest.get('gomap_replacement') == manifest['gomap_source']['directory'],
                'accepted baseline replacement does not bind its source directory')
    else:
        require(manifest.get('gomap_replacement') == '', 'candidate replacement forbidden')
        require(manifest.get('gomap_module_version') == 'v0.6.2-0.20261023204715-29a2fdf153ce',
                'candidate module version mismatch')
        require(manifest.get('gomap_module_sum') == 'h1:Y85xjKKdrqj5UXLCzYCuVzYXbk0oDGf8zVBwqowuxrs=',
                'candidate module checksum mismatch')


def check_row(row, manifest, arm, survival, baseline, profile=False):
    c, x, v = row['config'], row['context'], row['validation']
    require(row.get('schema_version') == 5, arm + ': schema must be v5')
    require(x.get('dirty') is False and x.get('excluded') is False,
            arm + ': dirty/excluded original row')
    require(not x.get('contaminants'), arm + ': classified contaminant present')
    require(x.get('binary_sha256') == manifest['server_sha256'], arm + ': row binary mismatch')
    require(x.get('dgraph_sha') == manifest['vcs_revision'], arm + ': row source mismatch')
    expected_module = manifest['gomap_module_version']
    if baseline:
        expected_module += ' => ' + manifest['gomap_replacement'] + '@(devel)'
    require(x.get('gomap_version') == expected_module, arm + ': row module mismatch')
    require(x.get('go_version') == manifest['go_version'], arm + ': row toolchain mismatch')
    info = x.get('binary_build_info', '')
    require(('build\tvcs.revision=' + manifest['vcs_revision']) in info
            and 'build\tvcs.modified=false' in info, arm + ': build-info VCS mismatch')
    if baseline:
        require(('dep\tgithub.com/snissn/gomap\t' + manifest['gomap_module_version'] + '\n'
                 + '=>\t' + manifest['gomap_replacement'] + '\t(devel)\t\n') in info,
                arm + ': accepted baseline build-info replacement mismatch')
    else:
        require(('dep\tgithub.com/snissn/gomap\t' + manifest['gomap_module_version'] + '\t'
                 + manifest['gomap_module_sum']) in info, arm + ': build-info module mismatch')
        require('\n=>\t' not in info, arm + ': unexpected build-info replacement')
    require(c.get('survival_mode') == survival, arm + ': survival mismatch')
    for key, value in WORKLOAD.items():
        if profile and key == 'timed_ops':
            continue
        require(c.get(key) == value, arm + ': workload mismatch: ' + key)
    backend, cls = c.get('backend'), c.get('durability_class')
    require(backend in ('badger', 'treedb') and cls in ('relaxed', 'durable'), arm + ': invalid cell')
    policy = 'raft_checkpoint' if arm == 'filesystem' and cls == 'durable' else 'store_wal'
    # Badger omits both optional TreeDB-only selector fields.
    if backend == 'treedb':
        require(c.get('required_treedb_memtable') == 'cow_ordered', arm + ': required TreeDB memtable mismatch')
        require(c.get('treedb_durable_policy') == policy, arm + ': posting owner mismatch')
    require(v.get('backend_observed') == backend, arm + ': backend witness mismatch')
    require(all(v.get(k) is True for k in ('schema_ok', 'restart_ok', 'unsupported_ok')),
            arm + ': failed semantic validation')
    require(v.get('posting_checksum') and v.get('node_count', 0) >= 500, arm + ': missing logical result')
    witnesses = {(w.get('process'), w.get('phase')): w for w in v.get('runtime', [])}
    for process in ('zero', 'alpha'):
        for phase in ('startup', 'after_alpha_restart'):
            w = witnesses.get((process, phase), {})
            require(w.get('survival_mode') == survival
                    and w.get('hard_sync') is (survival == 'filesystem'), arm + ': survival witness missing')
            require(w.get('config_log_sha256'), arm + ': missing runtime log binding')
            if process == 'alpha' and backend == 'treedb':
                require(w.get('memtable') == 'cow_ordered' and w.get('store_witness_sha256'),
                        arm + ': missing actual COW/store witness')
    require(positive(row.get('throughput_ops_s')), arm + ': invalid throughput')
    if not profile:
        require(not x.get('profiles'), arm + ': profile row entered decision matrix')


def load_arm(root, arm, input_bindings):
    folder, name, survival = ARMS[arm]
    evidence = root / folder
    def read(relative):
        path = evidence / relative
        require(path.is_file(), 'missing input: ' + str(path))
        input_bindings[str(path.relative_to(root))] = sha(path)
        return json.loads(path.read_text())
    receipt = read(name + '-invocation.json')
    require(receipt.get('exit_code') == 0 and receipt.get('status') in ('STOP', 'ADVANCE'),
            arm + ': arm not completed normally')
    require(receipt.get('comparison_mapping_sha256') == MAPPING_SHA, arm + ': comparison mapping mismatch')
    require(receipt.get('performance_qualification') is None, arm + ': unexpected qualification')
    if arm != 'process':
        require(receipt.get('whole_campaign_qualification') == LOCK, arm + ': campaign lock missing')
    manifest_path = evidence / (name + '-source-manifest.json')
    manifest = read(name + '-source-manifest.json')
    require(sha(manifest_path) == receipt.get('source_manifest_sha256'), arm + ': manifest hash mismatch')
    bind_manifest(manifest, receipt, arm == 'baseline')
    report = evidence / name / 'report.md'
    require(report.is_file() and sha(report) == receipt.get('report_sha256'), arm + ': original report hash mismatch')
    input_bindings[str(report.relative_to(root))] = sha(report)
    text = report.read_text()
    require('Logical parity and restart gate: **PASS**' in text, arm + ': original correctness report missing')
    expected = ('STOP advancement/integration at this phase' if receipt['status'] == 'STOP'
                else 'ADVANCE experimental integration to the next phase')
    require(expected in text, arm + ': report/invocation classification mismatch')
    live = evidence / name / 'live'
    files = sorted(live.glob('*/result.json'))
    require(len(files) == 12, arm + ': expected exactly twelve original rows')
    records, cells, parity = [], {}, set()
    for path in files:
        row = read(str(path.relative_to(evidence)))
        check_row(row, manifest, arm, survival, arm == 'baseline')
        c = row['config']; repeat = row.get('repeat')
        require(type(repeat) is int and repeat in (1, 2, 3), arm + ': invalid repeat')
        key = (c['backend'], c['durability_class'])
        expected_id = '%s-%s-r%d' % (*key, repeat)
        require(row.get('run_id') == expected_id == path.parent.name, arm + ': run/path mismatch')
        require(repeat not in cells.setdefault(key, {}), arm + ': duplicate repeat')
        cells[key][repeat] = row
        parity.add((row['validation']['posting_checksum'], row['validation']['node_count']))
        records.append({'path': str(path.relative_to(root)), 'sha256': sha(path), 'result': row})
    require(len(parity) == 1, arm + ': original logical parity mismatch')
    require(set(cells) == {(b, c) for b in ('badger', 'treedb') for c in ('relaxed', 'durable')},
            arm + ': incomplete cell matrix')
    require(all(set(rows) == {1, 2, 3} for rows in cells.values()), arm + ': incomplete repeats')
    profiles = []
    paths = sorted((evidence / name / 'profiles').glob('*/result.json'))
    require(len(paths) == 2, arm + ': expected both excluded diagnostic profile rows')
    require({p.parent.name for p in paths} == {'treedb-relaxed-run', 'treedb-durable-run'}, arm + ': profile shape mismatch')
    for path in paths:
        row = read(str(path.relative_to(evidence)))
        check_row(row, manifest, arm, survival, arm == 'baseline', profile=True)
        require(row['config']['backend'] == 'treedb' and row['context'].get('profiles'), arm + ': invalid profile row')
        profiles.append({'path': str(path.relative_to(root)), 'sha256': sha(path),
                         'exclusion_role': 'DIAGNOSTIC_PROFILE_NOT_IN_36', 'result': row})
    throughput = []
    operational = []
    for cls in ('relaxed', 'durable'):
        values = {b: [cells[b, cls][r]['throughput_ops_s'] for r in (1, 2, 3)] for b in ('badger', 'treedb')}
        medians = {b: statistics.median(v) for b, v in values.items()}
        ratio = medians['treedb'] / medians['badger']
        throughput.append({'class': cls, 'repeats': values, 'medians_ops_s': medians,
                           'ratio_of_medians': ratio, 'delta_percent': (ratio - 1) * 100,
                           'original_parity_gate': 'PASS' if ratio >= .97 else 'FAIL',
                           'gate_ratio_minimum': .97})
        for backend in ('badger', 'treedb'):
            rows = cells[backend, cls]
            keys = sorted(set().union(*(r.get('metrics', {}) for r in rows.values())))
            for metric in keys:
                obs = [observation(rows[r], metric) for r in (1, 2, 3)]
                good = [o['value'] for o in obs if o.get('available') is True
                        and isinstance(o.get('value'), (int, float)) and not isinstance(o['value'], bool)
                        and math.isfinite(o['value'])]
                operational.append({'backend': backend, 'class': cls, 'metric': metric,
                                    'observations': obs, 'available_count': len(good),
                                    'all_three_median': statistics.median(good) if len(good) == 3 else None})
    return {'receipt': receipt, 'source_manifest': manifest, 'original_report': str(report.relative_to(root)),
            'rows': records, 'profiles_excluded': profiles, 'throughput_parity': throughput,
            'operational_metrics_absolute': operational}, cells


def render(summary):
    lines = ['# Final matrix supplementary analysis', '', '**Whole campaign: STOP locked. No overall PASS or promotion.**', '',
             'All original reports and rows remain authoritative. This reader preserves the numerical failures and is not a replacement gate.', '',
             '## Within-arm original throughput rule', '',
             '| Arm | Class | Badger median ops/s | TreeDB median ops/s | TreeDB/Badger | Delta % | Original 0.97 gate |',
             '|---|---|---:|---:|---:|---:|---|']
    for arm, data in summary['arms'].items():
        for x in data['throughput_parity']:
            m = x['medians_ops_s']
            lines.append(f"| {arm} | {x['class']} | {m['badger']:.4f} | {m['treedb']:.4f} | {x['ratio_of_medians']:.6f} | {x['delta_percent']:.4f} | {x['original_parity_gate']} |")
    lines += ['', '## Filesystem source-paired ancillary changes', '',
              'All three changes are candidate/baseline minus one, in percent. A missing repeat makes the median UNQUALIFIED. <=5% is review-only.', '',
              '| Backend | Class | Metric | r1 % | r2 % | r3 % | Paired median % | Disposition |',
              '|---|---|---|---:|---:|---:|---:|---|']
    def fmt(v):
        return 'UNQUALIFIED' if v is None else f'{v:.4f}'
    for x in summary['filesystem_source_pairs']:
        for m in x['lower_is_better']:
            changes = ' | '.join(fmt(p['change_percent']) for p in m['pairs'])
            lines.append(f"| {x['backend']} | {x['class']} | {m['metric']} | {changes} | {fmt(m['paired_median_change_percent'])} | {m['status']} |")
    lines += ['', '## Filesystem source-paired throughput', '',
              'No new 5% throughput allowance is authorized. Any observed decline requires review; non-decline is not acceptance.', '',
              '| Backend | Class | r1 % | r2 % | r3 % | Paired median % | Review |',
              '|---|---|---:|---:|---:|---:|---|']
    for x in summary['filesystem_source_pairs']:
        t = x['throughput']; changes = ' | '.join(fmt(p['change_percent']) for p in t['pairs'])
        lines.append(f"| {x['backend']} | {x['class']} | {changes} | {fmt(t['paired_median_change_percent'])} | {t['status']} |")
    lines += ['', '## Every original repeat', '',
              '| Arm | Run | Ops/s | p95 ms | p99 ms | Result SHA256 |', '|---|---|---:|---:|---:|---|']
    for arm, data in summary['arms'].items():
        for entry in data['rows']:
            r = entry['result']; lat = r.get('latency_ms', {})
            lines.append(f"| {arm} | {r['run_id']} | {r['throughput_ops_s']:.4f} | {fmt(lat.get('p95'))} | {fmt(lat.get('p99'))} | {entry['sha256']} |")
    lines += ['', '## Excluded profile rows', '']
    for arm, data in summary['arms'].items():
        for entry in data['profiles_excluded']:
            lines.append(f"- {arm}: `{entry['path']}` SHA256 `{entry['sha256']}`; excluded diagnostic, never a decision repeat.")
    lines += ['', '## Operational observations and bounds', '',
              'JSON retains every absolute metric observation (availability, value, source, reason), all-three medians only, and legacy HTTP-health recovery descriptively. No missing metric becomes zero.', '']
    lines += ['- ' + text for text in BOUNDS]
    lines += ['', '## Input bindings', '']
    lines += [f'- `{path}`: `{digest}`' for path, digest in sorted(summary['input_sha256'].items())]
    return '\n'.join(lines) + '\n'


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    ap.add_argument('--output-prefix', type=Path, required=True, help='New output path prefix; refuses existing .json/.md')
    args = ap.parse_args()
    root = args.root.resolve()
    outputs = [Path(str(args.output_prefix) + ext) for ext in ('.json', '.md')]
    require(all(not p.exists() for p in outputs), 'refusing existing analysis output')
    bindings = {}
    for name, expected in [('MATRIX_COMPARISON_MAPPING.md', MAPPING_SHA)]:
        require(sha(root / name) == expected, 'frozen comparison mapping changed')
        bindings[name] = expected
    amendment = root / 'DIAGNOSTIC_CONTINUATION_AMENDMENT.json'
    require(json.loads(amendment.read_text()).get('whole_campaign_qualification') == LOCK, 'missing campaign STOP lock')
    bindings[amendment.name] = sha(amendment)
    summary = {'version': 1, 'whole_campaign_qualification': LOCK, 'overall_pass': None,
               'performance_qualification': None, 'bounds': BOUNDS, 'arms': {},
               'filesystem_source_pairs': [], 'input_sha256': bindings}
    matrices = {}
    for arm in ARMS:
        summary['arms'][arm], matrices[arm] = load_arm(root, arm, bindings)
    require(summary['arms']['process']['receipt']['status'] == 'STOP', 'prior failed process arm not retained')
    require(sum(len(x['rows']) for x in summary['arms'].values()) == 36, 'original row conservation failed')
    for backend in ('badger', 'treedb'):
        for cls in ('relaxed', 'durable'):
            c, b = matrices['filesystem'][backend, cls], matrices['baseline'][backend, cls]
            lower = [pair_changes(c, b, m) for m in METRICS]
            # Express throughput as observations with the same finite-positive checks.
            tc = {r: {'metrics': {'throughput': {'available': True, 'value': c[r]['throughput_ops_s'], 'unit': 'ops/s'}}} for r in (1, 2, 3)}
            tb = {r: {'metrics': {'throughput': {'available': True, 'value': b[r]['throughput_ops_s'], 'unit': 'ops/s'}}} for r in (1, 2, 3)}
            throughput = pair_changes(tc, tb, 'throughput', descriptive=True)
            median = throughput['paired_median_change_percent']
            throughput['status'] = ('UNQUALIFIED' if median is None else 'DECLINE_REVIEW_REQUIRED'
                                    if any(p['change_percent'] < 0 for p in throughput['pairs']) else 'NO_OBSERVED_DECLINE_NOT_ACCEPTANCE')
            summary['filesystem_source_pairs'].append({'backend': backend, 'class': cls,
                                                       'throughput': throughput, 'lower_is_better': lower})
    summary['analysis_script_sha256'] = sha(Path(__file__))
    # No outputs are opened until all inputs and exact matrix conservation validate.
    with outputs[0].open('x') as f:
        json.dump(summary, f, indent=2, allow_nan=False); f.write('\n')
    with outputs[1].open('x') as f:
        f.write(render(summary))
    print(json.dumps({'whole_campaign_qualification': LOCK, 'original_rows': 36,
                      'excluded_profiles': 6, 'outputs': {str(p): sha(p) for p in outputs}}))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, TypeError) as error:
        print('UNQUALIFIED: ' + str(error), file=sys.stderr)
        sys.exit(2)
