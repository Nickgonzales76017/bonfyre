import {checkRoutine,checkDay} from './training-model.mjs';
const name=(s,max=160,empty=false)=>{if(typeof s!=='string'||s.length>max||(!empty&&!s.trim()))throw Error('Enter a plan name and valid week labels.');};
const id=s=>{if(typeof s!=='string'||!s||s.length>160)throw Error('Invalid plan identity.');};
export function checkPlan(plan,{draft=false}={}){
  if(!plan||typeof plan!=='object')throw Error('Invalid training plan.');id(plan.id);name(plan.name,160,draft);name(plan.description,1000,draft);
  if(!Array.isArray(plan.routines)||plan.routines.length>50)throw Error('A plan supports up to 50 routine versions.');
  const routines=new Set();for(const r of plan.routines){checkRoutine(r);if(routines.has(r.id))throw Error('Duplicate plan routine.');routines.add(r.id);}
  if(!Array.isArray(plan.weeks)||!plan.weeks.length||plan.weeks.length>52)throw Error('Choose between 1 and 52 weeks.');
  const sessions=new Set();let count=0;
  for(const week of plan.weeks){name(week.name,80,draft);if(!Array.isArray(week.sessions)||week.sessions.length>14)throw Error('A week supports up to 14 sessions.');for(const s of week.sessions){id(s.id);if(sessions.has(s.id))throw Error('Duplicate plan session.');sessions.add(s.id);if(!Number.isInteger(s.day)||s.day<1||s.day>7||!routines.has(s.routineId))throw Error('Choose a day and routine for every session.');count++;}}
  if(!draft&&!count)throw Error('Add a session before publishing the plan.');
  if(plan.source){id(plan.source.creator);id(plan.source.program);id(plan.source.version);}
  return plan;
}
export function checkEnrollment(enrollment){id(enrollment.id);checkPlan(enrollment.plan);checkDay(enrollment.startDay);dayOffset(enrollment.startDay,enrollment.plan.weeks.length*7-1);if(typeof enrollment.archived!=='boolean'||!Array.isArray(enrollment.skipped)||new Set(enrollment.skipped).size!==enrollment.skipped.length)throw Error('Invalid plan progress.');const slots=new Set(enrollment.plan.weeks.flatMap(w=>w.sessions.map(s=>s.id)));if(enrollment.skipped.some(s=>!slots.has(s)))throw Error('Skipped session does not belong to this plan.');return enrollment;}
export function checkPlanSession(value){if(!value)return;id(value.enrollmentId);id(value.slotId);name(value.planName);checkDay(value.scheduledDay);}
export function dayOffset(day,offset){checkDay(day);if(!Number.isInteger(offset)||Math.abs(offset)>36600)throw Error('Invalid calendar offset.');const date=new Date(day+'T12:00:00Z');date.setUTCDate(date.getUTCDate()+offset);return checkDay(date.toISOString().slice(0,10));}
export const localDay=(date=new Date())=>`${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,'0')}-${String(date.getDate()).padStart(2,'0')}`;
export function planSchedule(enrollment,history=[],active=null){checkEnrollment(enrollment);return enrollment.plan.weeks.flatMap((w,week)=>w.sessions.map(s=>{
  const completed=history.find(h=>h.planSession?.enrollmentId===enrollment.id&&h.planSession.slotId===s.id),running=active?.planSession?.enrollmentId===enrollment.id&&active.planSession.slotId===s.id;
  return {...s,week:week+1,weekName:w.name,day:dayOffset(enrollment.startDay,week*7+s.day-1),routine:enrollment.plan.routines.find(r=>r.id===s.routineId),status:completed?'completed':running?'active':enrollment.skipped.includes(s.id)?'skipped':'planned',workoutId:completed?.id??null};
})).sort((a,b)=>a.day.localeCompare(b.day)||a.week-b.week);}
