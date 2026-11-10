import {test,expect} from 'bun:test';
import {decodeHeartRate,heartRateConnection,currentHeartRate,heartRateSummary} from '../training-devices.mjs';
import {planCalendar} from '../training-device-files.mjs';
import {trainingMaterial} from '../training-workspace.mjs';
import {newTraining,trainingCommand,exportTraining,importTraining} from '../training-model.mjs';
const bytes=(...n)=>new DataView(Uint8Array.from(n).buffer),routine=()=>structuredClone(trainingMaterial('packs/aurekai-fitness-studio').creators[0].routines[0]);
test('Bluetooth heart-rate flags preserve 16-bit readings, contact, energy and beat intervals',()=>{
 expect(decodeHeartRate(bytes(0,90))).toEqual({bpm:90,contact:null,energyKJ:null,rrMs:[]});
 expect(decodeHeartRate(bytes(31,4,1,12,0,0,4))).toEqual({bpm:260,contact:true,energyKJ:12,rrMs:[1000]});
 expect(decodeHeartRate(bytes(4,120)).contact).toBe(false);
 for(const bad of [bytes(),bytes(1,3),bytes(8,80),bytes(16,80,1),bytes(16,80),bytes(0,0),bytes(0,80,1)])expect(()=>decodeHeartRate(bad)).toThrow();
 expect(currentHeartRate({status:'connected',reading:{bpm:80,at:1000}},11001)).toBe(null);
 expect(currentHeartRate({status:'disconnected',reading:{bpm:80,at:1000}},1001)).toBe(null);
});
function mockSensor(){
 const characteristic=new EventTarget();characteristic.startNotifications=async()=>characteristic;
 const device=new EventTarget();device.name='Chest strap';device.gatt={connected:false,connect:async()=>{device.gatt.connected=true;return {getPrimaryService:async name=>{expect(name).toBe('heart_rate');return {getCharacteristic:async name=>{expect(name).toBe('heart_rate_measurement');return characteristic;}};}};},disconnect:()=>{device.gatt.connected=false;device.dispatchEvent(new Event('gattserverdisconnected'));}};
 const emit=(...b)=>{characteristic.value=bytes(...b);characteristic.dispatchEvent(new Event('characteristicvaluechanged'));};return {device,characteristic,emit};
}
test('sensor chooser scopes access, disconnect clears readings and malformed notifications never linger',async()=>{
 const {device,emit}=mockSensor();let filter;const c=heartRateConnection({bluetooth:{requestDevice:async options=>{filter=options;return device;}},clock:()=>1000});await c.connect();expect(filter).toEqual({filters:[{services:['heart_rate']}]});expect(c.state.status).toBe('connected');emit(0,130);expect(c.state.reading).toEqual({at:1000,bpm:130});emit(4,130);expect(c.state.reading).toBe(null);emit(0,120);emit(1,3);expect(c.state.reading).toBe(null);emit(0,125);device.gatt.disconnect();expect(c.state.status).toBe('disconnected');expect(c.state.reading).toBe(null);emit(0,180);expect(c.state.reading).toBe(null);
 const denied=heartRateConnection({bluetooth:{requestDevice:async()=>{throw Object.assign(Error('cancelled'),{name:'NotFoundError'});}}});await denied.connect();expect(denied.state.message).toBe('No sensor selected.');
});
test('disconnect while a chooser is pending cannot reconnect later',async()=>{
 let resolve;const selected=new Promise(r=>{resolve=r;}),{device}=mockSensor(),c=heartRateConnection({bluetooth:{requestDevice:()=>selected}});const pending=c.connect();c.disconnect();resolve(device);await pending;expect(c.state.status).toBe('disconnected');expect(device.gatt.connected).toBe(false);
});
test('heart rate belongs to the running workout and survives export without changing completion',()=>{
 const r=routine();let s=trainingCommand(newTraining('member:test'),{type:'routine.save',routine:r},1000);s=trainingCommand(s,{type:'start',routineId:r.id,id:'workout'},1000);
 const command={type:'heart-rate',workoutId:'workout',sample:{at:1100,bpm:121,device:'Chest strap'}};s=trainingCommand(s,command,1200);expect(s.active.heartRate.length).toBe(1);expect(()=>trainingCommand(s,command,1300)).toThrow();expect(()=>trainingCommand(s,{...command,workoutId:'other'},1300)).toThrow();expect(()=>trainingCommand(s,{type:'finish'},1400)).toThrow();
 s=trainingCommand(s,{type:'pause'},1300);expect(()=>trainingCommand(s,{...command,sample:{...command.sample,at:1400}},1400)).toThrow();s=trainingCommand(s,{type:'resume'},1500);s=trainingCommand(s,{type:'set',exerciseId:r.exercises[0].id,setId:r.exercises[0].sets[0].id,values:{done:true}},1600);s=trainingCommand(s,{type:'finish'},1700);expect(heartRateSummary(s.history[0].heartRate)).toEqual({average:121,peak:121,count:1});expect(importTraining(exportTraining(s),'member:other').history[0].heartRate).toEqual(s.history[0].heartRate);
 s=trainingCommand(s,{type:'start',routineId:r.id,id:'next'},2000);expect(s.active.heartRate).toBeUndefined();
});
test('device activities merge idempotently without becoming logged sets and invalid batches are atomic',()=>{
 const a={id:'tcx:'+'a'.repeat(64),source:'TCX',name:'Run',sport:'Running',startedAt:1000,endedAt:61000,seconds:60,distanceM:180,averageBpm:125},before=newTraining('member:test');let s=trainingCommand(before,{type:'device.import',activities:[a,a]},70000);expect(s.deviceActivities).toEqual([a]);expect(s.history).toEqual([]);expect(before.deviceActivities).toBeUndefined();expect(()=>trainingCommand(s,{type:'device.import',activities:[{...a,id:'tcx:'+'b'.repeat(64)},{...a,seconds:-1}]},70000)).toThrow();expect(s.deviceActivities.length).toBe(1);expect(importTraining(exportTraining(s),'member:other').deviceActivities).toEqual([a]);s=trainingCommand(s,{type:'device.remove',id:a.id},70000);expect(s.deviceActivities).toEqual([]);
});
test('phone calendars preserve calendar dates, escape names and use stable event IDs',()=>{
 const r=routine();r.name='Strength, legs; back\\core\nBEGIN:VEVENT';const e={id:'enroll',startDay:'2026-10-31',archived:false,skipped:['skip'],plan:{id:'plan',name:'é'.repeat(80),description:'Weekly training',routines:[r],weeks:[{name:'Week',sessions:[{id:'one',day:2,routineId:r.id},{id:'skip',day:3,routineId:r.id}]}]}};
 const first=planCalendar(e,[],new Date('2026-10-11T00:00:00Z')),second=planCalendar(e,[],new Date('2026-10-12T00:00:00Z'));expect(first).toContain('DTSTART;VALUE=DATE:20261101\r\nDTEND;VALUE=DATE:20261102');expect(first.match(/BEGIN:VEVENT/g)?.length).toBe(2); // one event plus escaped literal in summary
 expect(first).toContain('SUMMARY:Strength\\, legs\\; back\\\\core\\nBEGIN:VEVENT');expect(first.match(/^UID:.*$/m)?.[0]).toEqual(second.match(/^UID:.*$/m)?.[0]);expect(first.split('\r\n').every(line=>new TextEncoder().encode(line).length<=75)).toBe(true);expect(first.match(/^BEGIN:VEVENT$/gm)?.length).toBe(1);
});
