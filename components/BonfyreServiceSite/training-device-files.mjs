import {checkDeviceActivity} from './training-devices.mjs';
import {checkDay} from './training-model.mjs';
import {planSchedule,dayOffset,localDay} from './training-plan.mjs';
const ns='http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2';
// DOMParser stays in the browser. No entity resolver or external network reader.
export async function readTCX(xml,{Parser=globalThis.DOMParser}={}){
  if(typeof xml!=='string'||new TextEncoder().encode(xml).length>8*1024*1024)throw Error('Choose a TCX file smaller than 8 MB.');
  if(/<!DOCTYPE|<!ENTITY/i.test(xml))throw Error('This workout file contains unsupported XML declarations.');
  const doc=new Parser().parseFromString(xml,'application/xml');
  if(doc.getElementsByTagName('parsererror').length||doc.documentElement.localName!=='TrainingCenterDatabase'||doc.documentElement.namespaceURI!==ns)throw Error('Choose a valid TCX workout file.');
  const children=(node,name)=>[...node.children].filter(n=>n.localName===name&&n.namespaceURI===ns);
  const child=(node,name)=>children(node,name)[0];
  const value=(node,name)=>child(node,name)?.textContent.trim();
  const numeric=(node,name)=>{const raw=value(node,name);if(!raw||!Number.isFinite(Number(raw)))throw Error(`Workout ${name} is missing or invalid.`);return Number(raw);};
  const date=raw=>{if(!raw||!/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(raw)||!Number.isFinite(Date.parse(raw)))throw Error('Workout time needs a date and timezone.');checkDay(raw.slice(0,10));return Date.parse(raw);};
  const activities=child(doc.documentElement,'Activities'),rows=activities?children(activities,'Activity'):[];
  if(!rows.length||rows.length>100)throw Error('Choose a file with 1–100 recorded workouts.');
  const results=[];
  for(const row of rows){
    const sport=row.getAttribute('Sport');if(!['Running','Biking','Other'].includes(sport))throw Error('Unsupported TCX activity type.');
    const startedAt=date(value(row,'Id')),laps=children(row,'Lap');
    if(!laps.length||laps.length>1000)throw Error('Workout lap information is missing or too large.');
    let seconds=0,distanceM=0,endedAt=startedAt,weightedHR=0,hrSeconds=0,lastLapEnd=startedAt;
    for(const lap of laps){
      const start=date(lap.getAttribute('StartTime')),duration=numeric(lap,'TotalTimeSeconds'),distance=numeric(lap,'DistanceMeters');
      if(start<lastLapEnd||duration<=0||duration>604800||distance<0||distance>1e7)throw Error('Workout laps overlap or contain invalid totals.');
      // Trackpoints can include stopped time beyond the lap's active seconds.
      let end=start+Math.ceil(duration*1000);
      for(const track of children(lap,'Track'))for(const point of children(track,'Trackpoint')){const raw=value(point,'Time');if(raw){const at=date(raw);if(at<start||at>start+604800000)throw Error('Track time is outside this workout.');end=Math.max(end,at);}}
      seconds+=duration;distanceM+=distance;endedAt=Math.max(endedAt,end);lastLapEnd=end;
      const hr=child(lap,'AverageHeartRateBpm');if(hr){const n=numeric(hr,'Value');if(!Number.isInteger(n)||n<1||n>300)throw Error('Workout heart rate is invalid.');weightedHR+=n*duration;hrSeconds+=duration;}
    }
    const base={source:'TCX',name:sport==='Running'?'Run':sport==='Biking'?'Ride':'Watch workout',sport,startedAt,endedAt,seconds,distanceM,averageBpm:hrSeconds&&Math.abs(hrSeconds-seconds)<0.001?Math.round(weightedHR/hrSeconds):null};
    // Independent of file name, XML whitespace and device-generated notes.
    const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(base)));
    const id='tcx:'+Array.from(new Uint8Array(digest),b=>b.toString(16).padStart(2,'0')).join('');
    if(!results.some(r=>r.id===id))results.push(checkDeviceActivity({id,...base}));
  }
  return results;
}
function escapeICS(s){return String(s).replaceAll('\\','\\\\').replaceAll('\r','').replaceAll('\n','\\n').replaceAll(';','\\;').replaceAll(',','\\,');}
function fold(line){let result='',part='',bytes=0;for(const c of line){const n=new TextEncoder().encode(c).length;if(bytes+n>75){result+=part+'\r\n';part=' ';bytes=1;}part+=c;bytes+=n;}return result+part;}
export function planCalendar(enrollment,history=[],now=new Date()){
  const rows=planSchedule(enrollment,history).filter(r=>r.status==='planned'&&r.day>=localDay(now));
  if(!rows.length)throw Error('This plan has no upcoming sessions to add.');
  const stamp=now.toISOString().replace(/[-:]/g,'').replace(/\.\d{3}/,'');
  const lines=['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//Bonfyre//Training//EN','CALSCALE:GREGORIAN','METHOD:PUBLISH'];
  for(const row of rows){const identity=new TextEncoder().encode(JSON.stringify([enrollment.id,row.id]));const uid=Array.from(identity,b=>b.toString(16).padStart(2,'0')).join('');lines.push('BEGIN:VEVENT',`UID:${uid}@training.bonfyre`,`DTSTAMP:${stamp}`,`DTSTART;VALUE=DATE:${row.day.replaceAll('-','')}`,`DTEND;VALUE=DATE:${dayOffset(row.day,1).replaceAll('-','')}`,`SUMMARY:${escapeICS(row.routine.name)}`,`DESCRIPTION:${escapeICS(enrollment.plan.name+' · Week '+row.week+'\n'+row.routine.exercises.map(e=>e.name+' · '+e.sets.length+' sets').join('\n'))}`,'TRANSP:TRANSPARENT','END:VEVENT');}
  lines.push('END:VCALENDAR');return lines.map(fold).join('\r\n')+'\r\n';
}
