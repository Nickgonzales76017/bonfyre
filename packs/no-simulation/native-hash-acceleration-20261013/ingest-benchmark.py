import hashlib,json,platform,shutil,statistics,subprocess,tempfile,time
from pathlib import Path
root=Path(__file__).resolve().parents[3];proof=Path(__file__).resolve().parent
bins={'before':proof/'bonfyre-ingest-before','system':root/'cmd/BonfyreIngest/bonfyre-ingest'}
rows=[];size=64*1024*1024+17
with tempfile.TemporaryDirectory(prefix='bonfyre-ingest-bench-') as temp:
 path=Path(temp)/'input.bin';block=bytes(i%251 for i in range(1024*1024));remaining=size;oracle=hashlib.sha256()
 with path.open('wb') as stream:
  while remaining:
   part=block[:min(remaining,len(block))];stream.write(part);oracle.update(part);remaining-=len(part)
 expected=oracle.hexdigest()
 for repeat in range(5):
  for key in (['before','system'] if repeat%2==0 else ['system','before']):
   output=Path(temp)/'output';start=time.perf_counter();run=subprocess.run([str(bins[key]),str(path),str(output)],capture_output=True,text=True,check=True);elapsed=time.perf_counter()-start
   manifest=json.loads((output/'intake-manifest.json').read_text());actual=hashlib.sha256()
   with (output/manifest['normalized_path']).open('rb') as body:
    while chunk:=body.read(1024*1024):actual.update(chunk)
   assert manifest['status']=='ready' and manifest['source_content_hash']==manifest['normalized_content_hash']==manifest['content_hash']==actual.hexdigest()==expected
   assert manifest['normalized_bytes']==size
   rows.append({'repeat':repeat,'backend':key,'elapsed_seconds':elapsed,'sha256':actual.hexdigest(),'bytes':size});shutil.rmtree(output)
 report={'platform':platform.platform(),'bytes':size,'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'runs':rows,'median_seconds':{key:statistics.median(r['elapsed_seconds'] for r in rows if r['backend']==key) for key in bins}}
 (proof/'ingest-benchmark.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report['median_seconds']))
