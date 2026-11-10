import {csvRead,csvWrite,isoTime,quantile,text} from './data.mjs';
/** Analyze observed records. No random arrivals, generated timings, or predictions. */
export function importTraces(input,mapping={case:'case_id',activity:'activity',start:'start',end:'end',resource:'resource',source:'source_url'}){
  const parsed=csvRead(input);for(const k of ['case','activity','start'])if(!parsed.headers.includes(mapping[k]))throw Error(`Choose the ${k} column.`);
  const interval=Boolean(mapping.end&&parsed.headers.includes(mapping.end)),events=[],held=[],seen=new Set();
  for(const row of parsed.rows){try{
    if(row.error)throw Error(row.error);const r=Object.fromEntries(parsed.headers.map((h,i)=>[h,row.values[i]]));
    const caseId=text(r[mapping.case]?.trim(),'Case',200),activity=text(r[mapping.activity]?.trim(),'Activity',300);
    if(!caseId||!activity)throw Error('Case and activity are required.');
    const start=isoTime(r[mapping.start]),end=interval?isoTime(r[mapping.end]):start;
    if(end<start)throw Error('End precedes start.');
    const resource=mapping.resource?r[mapping.resource]??'':'',source=mapping.source?r[mapping.source]??'':'';
    const fingerprint=[caseId,activity,start,end,resource].map(s=>`${String(s).length}:${s}`).join('|');
    if(seen.has(fingerprint))throw Error('Duplicate event; retained once.');seen.add(fingerprint);
    events.push({id:`row-${row.row}`,caseId,activity,start,end,resource,source,row:row.row,interval});
  }catch(e){held.push({row:row.row,reason:e.message,values:row.values});}}
  return {events,held,headers:parsed.headers,mapping,mode:interval?'interval':'timestamp',sourceRows:parsed.rows.length};
}
function covered(events){let n=0,end=-Infinity;for(const e of [...events].sort((a,b)=>a.start-b.start||a.end-b.end)){n+=Math.max(0,e.end-Math.max(end,e.start));end=Math.max(end,e.end);}return n;}
export function analyzeTraces(events){
  if(!Array.isArray(events)||events.length>100000)throw Error('Choose at most 100,000 observed events.');
  const groups=new Map(),activities=new Map(),edges=new Map(),variants=new Map();
  for(const e of events){if(!Number.isSafeInteger(e.start)||!Number.isSafeInteger(e.end)||e.end<e.start)throw Error('Invalid retained event interval.');
    if(!groups.has(e.caseId))groups.set(e.caseId,[]);groups.get(e.caseId).push(e);
    if(!activities.has(e.activity))activities.set(e.activity,{name:e.activity,events:[],caseIds:new Set()});const a=activities.get(e.activity);a.events.push(e);a.caseIds.add(e.caseId);
  }
  const cases=[];
  for(const [id,items]of groups){const trace=[...items].sort((a,b)=>a.start-b.start||a.end-b.end||a.row-b.row),start=trace.reduce((v,e)=>Math.min(v,e.start),Infinity),end=trace.reduce((v,e)=>Math.max(v,e.end),-Infinity),hasIntervals=trace.every(e=>e.interval),active=hasIntervals?covered(trace):null;
    const sequence=trace.map(e=>e.activity),key=sequence.map(s=>`${s.length}:${s}`).join('|');
    const c={id,trace,start,end,elapsed:end-start,active,unrecorded:active===null?null:end-start-active,sequence,key,overlap:hasIntervals?trace.reduce((n,e)=>n+e.end-e.start,0)-active:null};cases.push(c);
    if(!variants.has(key))variants.set(key,{key,sequence,cases:[]});variants.get(key).cases.push(c);
    for(let i=1;i<trace.length;i++){const prev=trace[i-1],e=trace[i],key=`${prev.activity.length}:${prev.activity}${e.activity}`;
      if(!edges.has(key))edges.set(key,{from:prev.activity,to:e.activity,count:0,gaps:[],overlap:0});const edge=edges.get(key);edge.count++;
      if(e.start>=prev.end)edge.gaps.push(e.start-prev.end);else edge.overlap++;
    }
  }
  const spans=cases.map(c=>c.elapsed),points=[];for(const e of events)if(e.interval&&e.end>e.start){points.push([e.start,1],[e.end,-1]);}points.sort((a,b)=>a[0]-b[0]||a[1]-b[1]);let active=0,peak=0;for(const [,d]of points){active+=d;peak=Math.max(peak,active);}
  return {events:events.length,cases:cases.sort((a,b)=>a.id.localeCompare(b.id)),p50:quantile(spans,.5),p90:quantile(spans,.9),peakConcurrency:peak,
    activities:[...activities.values()].map(a=>({name:a.name,count:a.events.length,cases:a.caseIds.size,p50:a.events.every(e=>e.interval)?quantile(a.events.map(e=>e.end-e.start),.5):null,p90:a.events.every(e=>e.interval)?quantile(a.events.map(e=>e.end-e.start),.9):null})),
    edges:[...edges.values()].map(e=>({...e,p50Gap:quantile(e.gaps,.5)})),variants:[...variants.values()].sort((a,b)=>b.cases.length-a.cases.length)};
}
export function replay(c,time){return c.trace.map(e=>({...e,state:time<e.start?'pending':time>=e.end?'finished':'active'}));}
export function exportCases(result){return csvWrite(['case_id','observed_span_ms','covered_ms','unrecorded_ms','overlapping_work_ms','events'],result.cases.map(c=>[c.id,c.elapsed,c.active,c.unrecorded,c.overlap,c.trace.length]));}
