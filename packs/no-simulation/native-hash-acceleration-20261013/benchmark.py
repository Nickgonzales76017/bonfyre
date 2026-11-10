import hashlib,json,os,platform,re,statistics,subprocess,tempfile,time
from pathlib import Path
root=Path(__file__).resolve().parents[3]
proof=Path(__file__).resolve().parent
bins={'before':proof/'bonfyre-hash-before','system':root/'cmd/BonfyreHash/bonfyre-hash'}
report={'platform':platform.platform(),'machine':platform.machine(),'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'fixtures':[]}
with tempfile.TemporaryDirectory(prefix='bonfyre-native-hash-bench-') as temp:
 for size in [8192,8*1024*1024+17,128*1024*1024+17]:
  path=Path(temp)/'input.bin';block=bytes(i%251 for i in range(1024*1024));remaining=size;oracle=hashlib.sha256()
  with path.open('wb') as out:
   while remaining:
    part=block[:min(remaining,len(block))];out.write(part);oracle.update(part);remaining-=len(part)
  expected=oracle.hexdigest();rows=[]
  for binary in bins.values():
   warm=subprocess.run([str(binary),'file',str(path)],capture_output=True,text=True,check=True)
   assert warm.stdout.split()[0]==expected
  for repeat in range(5):
   for key in (['before','system'] if repeat%2==0 else ['system','before']):
    start=time.perf_counter();run=subprocess.run(['/usr/bin/time','-l',str(bins[key]),'file',str(path)],capture_output=True,text=True,check=True);elapsed=time.perf_counter()-start
    assert run.stdout.split()[0]==expected
    rss=int(re.search(r'(\d+)\s+maximum resident set size',run.stderr).group(1))
    rows.append({'repeat':repeat,'backend':key,'elapsed_seconds':elapsed,'maximum_resident_bytes':rss,'digest':expected})
  summary={key:{'median_seconds':statistics.median(r['elapsed_seconds'] for r in rows if r['backend']==key),'median_rss_bytes':statistics.median(r['maximum_resident_bytes'] for r in rows if r['backend']==key)} for key in bins}
  item={'bytes':size,'sha256':expected,'runs':rows,'summary':summary};report['fixtures'].append(item)
  (proof/'benchmark.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'bytes':size,'summary':summary}),flush=True)
