"""Independent offline audit; reads retained bytes only, writes own audit data."""
from pathlib import Path, PurePosixPath
import collections
import hashlib
import json
import math
import statistics
import tarfile
R = Path(__file__).resolve().parents[1]
E = R / 'baseline-evidence'
N = 'baseline-filesystem-12'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return json.loads(p.read_text())
def close(a,b): return math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-10)
archive=R/(N+'-EVIDENCE.tar')
assert archive.stat().st_size==8089600 and sha(archive)=='8c84c738db0119e90dacef38663222144e80edcf549b81e8049b79dc7da993e9'
with tarfile.open(archive) as t:
    members=t.getmembers()
    assert len(members)==len({m.name for m in members})==161
    for m in members:
        p=PurePosixPath(m.name)
        assert not p.is_absolute() and '..' not in p.parts and m.isfile(),m.name
        assert t.extractfile(m).read()==(E/m.name).read_bytes(),m.name
selected=read(E/(N+'-SELECTED_FILES.json'))
assert len(selected)==160
for name,digest in selected.items(): assert sha(E/name)==digest,name
manifest=read(E/(N+'-source-manifest.json')); receipt=read(E/(N+'-invocation.json'))
assert manifest['version']==1 and manifest['vcs_revision']=='7f2720bf0d7ed90e82b04c52911bff92e9f22726' and manifest['vcs_modified']=='false'
assert manifest['gomap_source']['revision']=='fc3c3db97ec2954d47a1adf731d501e7b73f181f'
assert manifest['gomap_replacement']==manifest['gomap_source']['directory']
assert manifest['server_sha256']==receipt['executed_server_sha256']=='58075e248543eb56ec618fe4099c04fd7fc741817558765e83e98af6432960a8'
assert sha(E/(N+'-source-manifest.json'))==receipt['source_manifest_sha256']
assert sha(E/N/'report.md')==receipt['report_sha256']
assert receipt['status']=='STOP' and receipt['exit_code']==0 and receipt['whole_campaign_qualification']=='STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE'
assert manifest['go_version']=='go1.26.9'
for key in ('GOGC','GOMEMLIMIT'): assert receipt['environment'][key] is None
live=sorted((E/N/'live').glob('*/result.json')); profiles=sorted((E/N/'profiles').glob('*/result.json'))
assert len(live)==12 and len(profiles)==2
counts=collections.Counter(); cells=collections.defaultdict(list); baseline_rows=[]
for p in live+profiles:
    d=read(p);c=d['config'];v=d['validation'];x=d['context'];profile=p in profiles
    assert d['schema_version']==5 and x['dirty'] is False and x['excluded'] is False and not x['contaminants']
    assert x['binary_sha256']==manifest['server_sha256'] and x['dgraph_sha']==manifest['vcs_revision'] and x['go_version']==manifest['go_version']
    assert x['gomap_version']==manifest['gomap_module_version']+' => '+manifest['gomap_replacement']+'@(devel)'
    assert x['binary_build_info']==manifest['build_info']
    for key,value in {'dataset_nodes':500,'warmup_ops':100,'concurrency':4,'seed':20260711,'survival_mode':'filesystem','recovery_probe':'read-commit-read-v1','topology':'single-zero-single-alpha','query_mix_percent':{'one_hop_read':20,'point_read':60,'write':20}}.items():assert c[key]==value,(p,key)
    assert c['timed_ops']==((100000 if c['durability_class']=='relaxed' else 10000) if profile else 2000)
    assert all(v[key] is True for key in ('schema_ok','restart_ok','unsupported_ok'))
    assert v['backend_observed']==c['backend']
    if c['backend']=='treedb': assert c['required_treedb_memtable']=='cow_ordered' and c['treedb_durable_policy']=='store_wal'
    assert len(v['runtime'])==4
    witnesses={(w['process'],w['phase']):w for w in v['runtime']};assert len(witnesses)==4
    for (role,phase),w in witnesses.items():
        assert w['hard_sync'] is True and w['survival_mode']=='filesystem'
        cp=p.parent/f'runtime-{phase}-{role}.config.log'
        assert sha(cp)==w['config_log_sha256']
        assert w['config_witness'] in cp.read_text() and 'HardSync:true' in w['config_witness']
        raw=p.parent/('zero.log' if role=='zero' else 'alpha.log' if phase=='startup' else 'alpha-restart.log')
        assert raw.read_bytes().startswith(cp.read_bytes())
        counts['profile_configs' if profile else 'original_configs']+=1
        if role=='alpha':
            sp=p.parent/f'runtime-{phase}-alpha.store.txt'
            assert sha(sp)==w['store_witness_sha256'];st=sp.read_text()
            assert 'backend:'+c['backend'] in st
            if c['backend']=='treedb':
                assert w['memtable']=='cow_ordered' and 'treedb.cache.memtable_mode:cow_ordered' in st
                assert 'treedb.cache.memtable_mode_config:fixed' in st
                want='wal_on_sync' if c['durability_class']=='durable' else 'wal_on_relaxed_sync'
                assert w['durability']==want and 'profile:'+want in st
            else:
                want='syncwrites=true' if c['durability_class']=='durable' else 'syncwrites=false'
                assert w['durability']==want and 'profile:'+want in st
            counts['profile_stores' if profile else 'original_stores']+=1
    if profile: assert x.get('profiles')
    else:
        assert not x.get('profiles');cells[c['backend'],c['durability_class']].append(d['repeat'])
        baseline_rows.append({'run_id':d['run_id'],'throughput_ops_s':d['throughput_ops_s'],'result_sha256':sha(p)})
assert counts=={'original_configs':48,'original_stores':24,'profile_configs':8,'profile_stores':4}
assert len(cells)==4 and all(sorted(v)==[1,2,3] for v in cells.values())
preview=R/'baseline-micro-preview/raw.txt'; finalmicro=E/N/'microbench/raw.stdout'
assert preview.read_bytes()==finalmicro.read_bytes()
analysis=read(R/'final-matrix-analysis-01.json')
assert analysis['overall_pass'] is None and analysis['performance_qualification'] is None
assert analysis['whole_campaign_qualification']=='STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE'
matrices={};report_math={};parity=set()
for arm,folder,name in [('process','process-evidence','process-12'),('filesystem','filesystem-evidence','filesystem-12'),('baseline','baseline-evidence',N)]:
    paths=sorted((R/folder/name/'live').glob('*/result.json'));assert len(paths)==12
    dd=[read(p) for p in paths];matrices[arm]={(d['config']['backend'],d['config']['durability_class'],d['repeat']):d for d in dd};assert len(matrices[arm])==12
    for d in dd:parity.add((d['validation']['posting_checksum'],d['validation']['node_count']))
    for path,d in zip(paths,dd):
        a=next(x for x in analysis['arms'][arm]['rows'] if x['result']['run_id']==d['run_id'])
        assert a['result']==d and a['sha256']==sha(path)
    report_math[arm]={}
    for cls in ('relaxed','durable'):
        med={b:statistics.median(matrices[arm][b,cls,r]['throughput_ops_s'] for r in (1,2,3)) for b in ('badger','treedb')}
        ratio=med['treedb']/med['badger'];delta=(ratio-1)*100
        a=next(x for x in analysis['arms'][arm]['throughput_parity'] if x['class']==cls)
        assert all(close(a['medians_ops_s'][b],med[b]) for b in med) and close(a['ratio_of_medians'],ratio) and close(a['delta_percent'],delta)
        assert a['original_parity_gate']==('PASS' if ratio>=.97 else 'FAIL')
        report=(R/folder/name/'report.md').read_text()
        assert f'**{delta:+.2f}%**' in report
        report_math[arm][cls]={'medians':med,'ratio':ratio,'delta_percent':delta,'gate':a['original_parity_gate']}
assert len(parity)==1
pair_data=[]
for backend in ('badger','treedb'):
    for cls in ('relaxed','durable'):
        a=next(x for x in analysis['filesystem_source_pairs'] if x['backend']==backend and x['class']==cls)
        for metric in ('p95','p99','cpu_seconds','rss_peak_bytes','disk_logical_bytes','disk_allocated_bytes','recovery_read_seconds','recovery_read_write_seconds','throughput'):
            changes=[];av=a['throughput'] if metric=='throughput' else next(x for x in a['lower_is_better'] if x['metric']==metric)
            for repeat in (1,2,3):
                c,b=matrices['filesystem'][backend,cls,repeat],matrices['baseline'][backend,cls,repeat]
                if metric=='throughput':cv,bv=c['throughput_ops_s'],b['throughput_ops_s']
                elif metric in ('p95','p99'):cv,bv=c['latency_ms'][metric],b['latency_ms'][metric]
                else:
                    co,bo=c['metrics'][metric],b['metrics'][metric]
                    assert co['available'] is True and bo['available'] is True and co['unit']==bo['unit']
                    cv,bv=co['value'],bo['value']
                assert cv>0 and bv>0 and math.isfinite(cv) and math.isfinite(bv)
                change=100*(cv/bv-1);changes.append(change)
                assert close(av['pairs'][repeat-1]['change_percent'],change)
            median=statistics.median(changes)
            assert close(av['paired_median_change_percent'],median)
            expected=('DECLINE_REVIEW_REQUIRED' if min(changes)<0 else 'NO_OBSERVED_DECLINE_NOT_ACCEPTANCE') if metric=='throughput' else ('FAIL_REVIEW_REQUIRED' if median>5 else 'WITHIN_5_PERCENT_REVIEW_NOT_ACCEPTANCE')
            assert av['status']==expected
            pair_data.append({'backend':backend,'class':cls,'metric':metric,'all_three_changes_percent':changes,'paired_median_percent':median,'status':expected})
output={'archive_sha256':sha(archive),'archive_members':161,'selected_files_verified':160,'source_manifest_sha256':sha(E/(N+'-source-manifest.json')),'report_sha256':sha(E/N/'report.md'),'invocation_sha256':sha(E/(N+'-invocation.json')),'runtime_counts':dict(counts),'baseline_rows':baseline_rows,'all_36_cross_arm_logical_parity':list(parity),'original_report_math':report_math,'all_source_pairs':pair_data,'final_micro_sha256':sha(finalmicro),'analysis_json_sha256':sha(R/'final-matrix-analysis-01.json'),'analysis_md_sha256':sha(R/'final-matrix-analysis-01.md'),'analysis_script_sha256':sha(R/'analyze_final_matrix.py'),'whole_campaign':'STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE','overall_pass':None}
(E/'BASELINE_AUDIT_DATA.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({k:v for k,v in output.items() if k not in ('baseline_rows','all_source_pairs')},indent=2))
