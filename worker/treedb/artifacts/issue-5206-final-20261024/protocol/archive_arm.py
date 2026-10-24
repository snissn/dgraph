import pathlib,sys,tarfile,hashlib,json
p=pathlib.Path('/home/mikers/.cache/gomap-raft-production-20261023T0744Z/native-final-01/runtime-merged-01');arm=sys.argv[1];assert arm in ['process-12','filesystem-12','baseline-filesystem-12'];a=p/(arm+'-EVIDENCE.tar');assert not a.exists();files=[]
root=p/arm
files += [f for f in root.iterdir() if f.is_file()]
files += [f for f in (root/'microbench').iterdir() if f.is_file()]
for q in (root/'live').iterdir():
 if q.is_dir():files += [f for f in q.iterdir() if f.is_file()]
if (root/'profiles').exists():
 for q in (root/'profiles').iterdir():
  if q.is_file():files.append(q)
  elif q.is_dir():files += [f for f in q.iterdir() if f.is_file()]
files += [f for f in (p/('quiet-'+arm)).iterdir() if f.is_file()]
files += [f for f in p.glob(arm+'*') if f.is_file() and f!=a]
files=sorted(set(files));hashes={str(f.relative_to(p)):hashlib.file_digest(f.open('rb'),'sha256').hexdigest() for f in files}
manifest=p/(arm+'-SELECTED_FILES.json');manifest.write_text(json.dumps(hashes,indent=2)+'\n');files.append(manifest)
with tarfile.open(a,'w') as t:
 for f in files:t.add(f,arcname=str(f.relative_to(p)),recursive=False)
r={'archive':a.name,'bytes':a.stat().st_size,'sha256':hashlib.file_digest(a.open('rb'),'sha256').hexdigest(),'selected_files':len(files),'omitted':'database directories and binaries retained remotely; executedserver hash-boundmanifest'}
(p/(arm+'-ARCHIVE_RECEIPT.json')).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
