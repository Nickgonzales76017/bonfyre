import hashlib,json,os,statistics,subprocess,tempfile,time
from pathlib import Path
root=Path.cwd(); proof=root/'packs/no-simulation/model-hash-20261013'
binaries={'previous':Path(json.loads((proof/'installation.json').read_text())['rollback']),'candidate':root/'cmd/BonfyreModel/bonfyre-model'}
report={'synthetic':True,'model_downloaded':False,'model_executed':False,'bytes':128*1024*1024+17,'runs':[],'binaries':{name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in binaries.items()}}
with tempfile.TemporaryDirectory(prefix='bonfyre-model-hash-benchmark-') as tmp:
 folder=Path(tmp);cache=folder/'cache';cache.mkdir();body=cache/'synthetic-hash.bin'
 block=bytes(range(256))*4096; digest=hashlib.sha256()
 with body.open('wb') as f:
  for _ in range(128):f.write(block);digest.update(block)
  f.write(b'partial-last-byte');digest.update(b'partial-last-byte')
 report['bytes']=body.stat().st_size;report['sha256']=digest.hexdigest()
 env=dict(os.environ,BONFYRE_MODEL_DB=str(folder/'models.db'),BONFYRE_MODEL_CACHE=str(cache),BONFYRE_CATALOG_DB=str(folder/'catalog.db'))
 manifest=folder/'manifest.json';manifest.write_text(json.dumps({'id':'synthetic-hash','name':'Synthetic bytes','format':'bin','sha256':digest.hexdigest(),'sources':[]}))
 setup=subprocess.run([str(binaries['candidate']),'add',str(manifest)],env=env,capture_output=True);assert setup.returncode==0,setup.stderr
 for i in range(5):
  for name in (['previous','candidate'] if i%2==0 else ['candidate','previous']):
   start=time.perf_counter();r=subprocess.run([str(binaries[name]),'verify','synthetic-hash'],env=env,capture_output=True);elapsed=time.perf_counter()-start
   assert r.returncode==0,(name,r.stderr);assert digest.hexdigest().encode() in r.stdout
   report['runs'].append({'binary':name,'seconds':elapsed,'exit_code':r.returncode,'digest_exact':True})
 report['median_seconds']={name:statistics.median(row['seconds'] for row in report['runs'] if row['binary']==name) for name in binaries}
 report['speedup']=report['median_seconds']['previous']/report['median_seconds']['candidate']
 readback=hashlib.sha256()
 with body.open('rb') as f:
  for chunk in iter(lambda:f.read(1024*1024),b''):readback.update(chunk)
 assert readback.hexdigest()==digest.hexdigest()
report['source_unchanged']=True
(proof/'benchmark.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
