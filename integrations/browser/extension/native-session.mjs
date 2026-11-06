// A bounded carrier session, not a work owner. Only a definitive pre-dispatch
// session_spent refusal is retried. Lost acknowledgements require owner readback.
export function createNativeSession(runtime, {timeoutMs=90000}={}) {
  let port=null, next=1, pending=null, serial=Promise.resolve();
  const fail=(refused,reason)=>({ok:false,refused,reason});
  function drop() { const old=port;port=null;try{old?.disconnect();}catch{} }
  function connect() {
    const owned=runtime.connectNative('com.bonfyre.browser_host');port=owned;
    owned.onMessage.addListener(message=>{
      if(port===owned&&pending?.id===message.id){const job=pending;pending=null;clearTimeout(job.timer);job.done(message);}
    });
    owned.onDisconnect.addListener(()=>{
      const reason=runtime.lastError?.message??'Native session ended.';
      if(port!==owned)return;
      port=null;
      if(pending){const job=pending;pending=null;clearTimeout(job.timer);job.done(fail('disconnected',reason+' Reload the native item before retrying a change.'));}
    });
  }
  function once(operation,more) {
    return new Promise(done=>{
      try { if(!port)connect(); }
      catch {done(fail('disconnected','The native host could not be started.'));return;}
      const id=next++;
      const timer=setTimeout(()=>{
        if(pending?.id!==id)return;
        pending=null;done(fail('uncertain','No acknowledgement arrived. Reload the native item; this change was not replayed.'));drop();
      },timeoutMs);
      pending={id,timer,done};
      try{port.postMessage({...more,id,operation});}
      catch{clearTimeout(timer);pending=null;drop();done(fail('disconnected','The native connection failed. This change was not replayed.'));}
    });
  }
  return {
    ask(operation,more={}) {
      const task=async()=>{
        let answer=await once(operation,more);
        if(answer.refused==='session_spent'){drop();answer=await once(operation,more);}
        return answer;
      };
      const result=serial.then(task,task);serial=result.then(()=>{},()=>{});return result;
    },
    close(){if(pending){const job=pending;pending=null;clearTimeout(job.timer);job.done(fail('disconnected','Panel connection closed; reload the native item.'));}drop();}
  };
}
