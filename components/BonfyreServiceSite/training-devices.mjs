// Bluetooth Heart Rate Service 0x180D, measurement characteristic 0x2A37.
export function decodeHeartRate(value) {
  if (!(value instanceof DataView) || value.byteLength < 2) throw Error('Incomplete heart-rate reading.');
  const flags=value.getUint8(0); let offset=1;
  const take16=()=>{if(offset+2>value.byteLength)throw Error('Incomplete heart-rate reading.');const n=value.getUint16(offset,true);offset+=2;return n;};
  const bpm=flags&1?take16():value.getUint8(offset++);
  const contact=flags&4?!!(flags&2):null;
  const energyKJ=flags&8?take16():null,rrMs=[];
  if(flags&16){if(offset===value.byteLength||(value.byteLength-offset)%2)throw Error('Incomplete beat interval.');while(offset<value.byteLength)rrMs.push(take16()*1000/1024);}
  if(offset!==value.byteLength)throw Error('Unexpected heart-rate data.');
  if(bpm<1||bpm>300)throw Error('No usable heart-rate reading.');
  return {bpm,contact,energyKJ,rrMs};
}
export function heartRateConnection({bluetooth=globalThis.navigator?.bluetooth,onChange=()=>{},clock=Date.now}={}) {
  let device=null,characteristic=null,generation=0;
  let state={status:'disconnected',name:'Heart-rate sensor',reading:null,message:''};
  const emit=patch=>{state={...state,...patch};onChange({...state});};
  const changed=event=>{try{const reading=decodeHeartRate(event.target.value);emit({reading:reading.contact===false?null:{bpm:reading.bpm,at:clock()},message:reading.contact===false?'Adjust the sensor against your skin.':''});}catch(error){emit({reading:null,message:error.message});}};
  function cleanup(){characteristic?.removeEventListener('characteristicvaluechanged',changed);characteristic=null;if(device){device.removeEventListener('gattserverdisconnected',lost);if(device.gatt?.connected)device.gatt.disconnect();}device=null;}
  function lost(){generation++;cleanup();emit({status:'disconnected',reading:null,message:'Connection lost. Reconnect when the sensor is nearby.'});}
  return {
    get state(){return {...state};},
    async connect(){
      if(!bluetooth?.requestDevice)throw Error('Bluetooth pairing is unavailable in this browser. Use Chrome on Android or a supported desktop browser.');
      if(state.status==='connecting')return;
      const run=++generation;cleanup();emit({status:'connecting',reading:null,message:''});
      let selected;
      try{
        selected=await bluetooth.requestDevice({filters:[{services:['heart_rate']}]});
        if(run!==generation)return;
        device=selected;device.addEventListener('gattserverdisconnected',lost);
        const server=await device.gatt.connect();
        if(run!==generation){if(selected.gatt.connected)selected.gatt.disconnect();return;}
        const service=await server.getPrimaryService('heart_rate');
        const measurement=await service.getCharacteristic('heart_rate_measurement');
        if(run!==generation)return;
        characteristic=measurement;characteristic.addEventListener('characteristicvaluechanged',changed);
        await characteristic.startNotifications();
        if(run!==generation)return;
        emit({status:'connected',name:String(device.name||'Heart-rate sensor').slice(0,120),message:'Waiting for a reading.'});
      }catch(error){if(run!==generation)return;cleanup();emit({status:'disconnected',reading:null,message:error.name==='NotFoundError'?'No sensor selected.':error.message});}
    },
    disconnect(){generation++;cleanup();emit({status:'disconnected',reading:null,message:''});}
  };
}
export function currentHeartRate(connection,now=Date.now()){
  const r=connection.reading;return connection.status==='connected'&&r&&now>=r.at&&now-r.at<=10000?r:null;
}
export function checkHeartRate(samples,startedAt,endedAt=8.64e15){
  if(samples===undefined)return;
  if(!Array.isArray(samples)||samples.length>21600)throw Error('Too many heart-rate readings.');
  let previous=-1;
  for(const s of samples){if(!Number.isSafeInteger(s.at)||s.at<startedAt||s.at>endedAt||s.at<=previous||!Number.isInteger(s.bpm)||s.bpm<1||s.bpm>300||typeof s.device!=='string'||!s.device.trim()||s.device.length>120)throw Error('Invalid heart-rate reading.');previous=s.at;}
}
export function heartRateSummary(samples=[]){return samples.length?{average:Math.round(samples.reduce((n,s)=>n+s.bpm,0)/samples.length),peak:Math.max(...samples.map(s=>s.bpm)),count:samples.length}:null;}
export function checkDeviceActivity(a){
  if(!a||typeof a.id!=='string'||!/^tcx:[a-f0-9]{64}$/.test(a.id)||typeof a.name!=='string'||!a.name.trim()||a.name.length>160||!['Running','Biking','Other'].includes(a.sport)||a.source!=='TCX')throw Error('Invalid device workout.');
  if(!Number.isSafeInteger(a.startedAt)||a.startedAt<0||!Number.isSafeInteger(a.endedAt)||a.endedAt<=a.startedAt||a.endedAt>8.64e15||!Number.isFinite(a.seconds)||a.seconds<=0||a.seconds>604800||a.seconds>(a.endedAt-a.startedAt)/1000+1||!Number.isFinite(a.distanceM)||a.distanceM<0||a.distanceM>1e7)throw Error('Invalid workout duration or distance.');
  if(a.averageBpm!==null&&(!Number.isInteger(a.averageBpm)||a.averageBpm<1||a.averageBpm>300))throw Error('Invalid average heart rate.');
  return a;
}
