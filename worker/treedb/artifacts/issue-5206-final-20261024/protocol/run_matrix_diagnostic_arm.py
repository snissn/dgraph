import datetime,hashlib,json,os,pathlib,signal,subprocess,sys,time
LANE=pathlib.Path('/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01')
OUT=LANE/'runtime-merged-01'
ARM=sys.argv[1]
assert ARM in ('process-12','filesystem-12','baseline-filesystem-12')
BASELINE=ARM=='baseline-filesystem-12'
SOURCE=LANE/'source'/('baseline-dgraph' if BASELINE else 'prospective-dgraph')
GOMAP=LANE/'source'/('baseline-gomap' if BASELINE else 'final-gomap')
WITNESS=LANE/'build-merged/bin/resource-witness'
EXPECTED='7f2720bf0d7ed90e82b04c52911bff92e9f22726' if BASELINE else 'bad4ced2c88e15076f73e3647a72e965f63be2b9'
RESULT=OUT/(ARM+'-invocation.json')
assert not RESULT.exists() and not (OUT/ARM).exists()
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=SOURCE,text=True).strip()==EXPECTED
assert not subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all'],cwd=SOURCE,text=True).strip()
for stage in ('schema-ack-suffix','floor-ack','admin-close','native-peers'):
 x=json.loads((OUT/stage/'result.json').read_text());assert x['success'] and x['diagnostic_only']
assert json.loads((OUT/'native-peers/result.json').read_text())['binary_sha256']=='46763a6bfcd02c37a44fb36e8c866dd7bbf7e2ca9777ad0a0aefb1ada9325175'
amendment=json.loads((OUT/'DIAGNOSTIC_CONTINUATION_AMENDMENT.json').read_text())
assert ARM in ('filesystem-12','baseline-filesystem-12')
assert amendment['whole_campaign_qualification']=='STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE'
prior='filesystem-12' if BASELINE else 'process-12'
previous=json.loads((OUT/(prior+'-invocation.json')).read_text())
assert previous['status'] in ('ADVANCE','STOP') and previous['exit_code']==0
previous_rows=list((OUT/prior/'live').glob('*/result.json'));assert len(previous_rows)==12
assert 'Logical parity and restart gate: **PASS**' in (OUT/prior/'report.md').read_text()
assert json.loads((OUT/'process-12-invocation.json').read_text())['status']=='STOP'
env=dict(os.environ,SURVIVE='process' if ARM=='process-12' else 'filesystem',TREEDB_DURABLE_POLICY='raft_checkpoint' if ARM=='filesystem-12' else 'store_wal',REQUIRE_TREEDB_MEMTABLE='cow_ordered',REPEATS='3',DATASET_NODES='500',WARMUP_OPS='100',TIMED_OPS='2000',CONCURRENCY='4',PYTHONDONTWRITEBYTECODE='1')
for k in ('COUNT','BENCHTIME','PROFILE_RELAXED_OPS','PROFILE_DURABLE_OPS','PROFILE_SECONDS'):env.pop(k,None)
if BASELINE:env['GOFLAGS']='-p=2 -mod=readonly -modfile='+str(LANE/'baseline-build-inputs/build.mod')
r={'arm':ARM,'status':'PREFLIGHT','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_sha':EXPECTED,'environment':{k:env.get(k) for k in ['SURVIVE','TREEDB_DURABLE_POLICY','REQUIRE_TREEDB_MEMTABLE','REPEATS','DATASET_NODES','WARMUP_OPS','TIMED_OPS','CONCURRENCY','GOMAXPROCS','GOFLAGS','GOTOOLCHAIN','GOWORK','GOGC','GOMEMLIMIT']},'outer_deadline_seconds':10800,'interrupt_grace_seconds':45,'comparison_mapping_sha256':hashlib.sha256((OUT/'MATRIX_COMPARISON_MAPPING.md').read_bytes()).hexdigest(),'performance_qualification':None}
r['collection_role']='DIAGNOSTIC_CONTINUATION_ONLY'
r['whole_campaign_qualification']='STOP_LOCKED_PRIOR_PROCESS_ARM_NUMERIC_FAILURE'
r['protocol_amendment_sha256']=hashlib.sha256((OUT/'DIAGNOSTIC_CONTINUATION_AMENDMENT.json').read_bytes()).hexdigest()
def save():RESULT.write_text(json.dumps(r,indent=2)+'\n')
save()
try:
 quiet=OUT/('quiet-'+ARM);quiet.mkdir()
 sys.dont_write_bytecode=True
 sys.path.insert(0,str(LANE/'source/final-gomap/scripts'))
 from mvcc_primary_cost_gate import acquire_quiet_window
 try:
  acquire_quiet_window(quiet)
  r['physical_quiet']='ACQUIRED_ORIGINAL_WINDOW_NOT_CONTINUOUS'
 except RuntimeError as e:
  acq=json.loads((quiet/'preflight-acquisition.json').read_text())
  if str(e)!='host quiet preflight acquisition exhausted; no timed samples started' or acq['status']!='exhausted':raise
  r['physical_quiet']='EXHAUSTED_ORIGINAL_CAPTURE_RETAINED_EXPLICIT_USER_OVERRIDE'
  r['physical_quiet_error']=str(e)
 r['status']='RUNNING';save()
 command=['bash','worker/treedb/run_durability_ab.sh','--artifact-dir',str(OUT/ARM)];r['command']=command;save()
 with (OUT/(ARM+'.stdout')).open('xb') as stdout,(OUT/(ARM+'.stderr')).open('xb') as stderr:
  p=subprocess.Popen(command,cwd=SOURCE,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
  try:rc=p.wait(timeout=10800)
  except subprocess.TimeoutExpired:
   os.killpg(p.pid,signal.SIGINT)
   try:p.wait(timeout=45)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
   raise RuntimeError('original arm exceeded frozen10800s outer deadline')
 r['exit_code']=rc
 if rc!=0:raise RuntimeError('original arm exited '+str(rc))
 actual=OUT/ARM/'bin/dgraph'
 packet=OUT/(ARM+'-source-manifest.json')
 for label,args in [('manifest',['--mode','manifest','--dgraph-source',str(SOURCE),'--gomap-source',str(GOMAP)]),('verify',['--mode','verify'])]:
  with (OUT/(ARM+'-'+label+'.stdout')).open('xb') as stdout,(OUT/(ARM+'-'+label+'.stderr')).open('xb') as stderr:
   subprocess.run([str(WITNESS),*args,'--server',str(actual),'--manifest',str(packet)],cwd=SOURCE,env=env,stdout=stdout,stderr=stderr,check=True)
 m=json.loads(packet.read_text());r['executed_server_sha256']=m['server_sha256'];r['source_manifest_sha256']=hashlib.sha256(packet.read_bytes()).hexdigest()
 assert m['dgraph_source']['revision']==EXPECTED
 assert m['version']==(1 if BASELINE else 2)
 report=(OUT/ARM/'report.md').read_text();r['report_sha256']=hashlib.sha256((OUT/ARM/'report.md').read_bytes()).hexdigest()
 assert ('ADVANCE experimental integration to the next phase' in report)!=('STOP advancement/integration at this phase' in report)
 r['status']='ADVANCE' if 'ADVANCE experimental integration to the next phase' in report else 'STOP'
except Exception as e:
 r['status']='FAILED';r['error']=repr(e)
finally:
 r['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save();print(json.dumps(r,indent=2))
sys.exit(0 if r['status']=='ADVANCE' else 1)
