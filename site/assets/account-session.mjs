/** Ephemeral presentation of the existing same-origin Auth owner, never a second login store. */
export function createAccountSession({read, revoke, now = Date.now, maxAge = 15000, timeout = 10000} = {}) {
  if (typeof read !== 'function' || typeof revoke !== 'function') throw new TypeError('Explicit account readers are required.');
  if (!Number.isFinite(maxAge) || maxAge < 0 || maxAge > 60000 || !Number.isFinite(timeout) || timeout < 1 || timeout > 30000) throw new RangeError('Invalid session observation bounds.');
  let state = Object.freeze({kind:'unknown', user:null, checkedAt:null}), epoch = 0, pending = null, mutation = null, disposed = false;
  const listeners = new Set(), diagnostics = [];
  const bounded = (fn, signal) => new Promise((resolve,reject) => {
    const abort=()=>reject(signal.reason||Error('Account request timed out.'));
    signal.addEventListener('abort',abort,{once:true});
    Promise.resolve().then(()=>{if(signal.aborted)throw signal.reason;return fn(signal);}).then(resolve,reject).finally(()=>signal.removeEventListener('abort',abort));
  });
  const record = error => { diagnostics.push(String(error?.message || error).slice(0,240)); if (diagnostics.length > 8) diagnostics.shift(); };
  const emit = next => {
    state = Object.freeze(next);
    for (const listener of [...listeners]) try { listener(state); } catch (error) { record(error); }
    return state;
  };
  const invalidate = reason => {
    epoch++; pending?.controller.abort(); pending = null;
    if (!disposed) emit({kind:'unknown', user:null, checkedAt:null, reason});
  };
  const observe = result => {
    if (!result || typeof result !== 'object') throw Error('The account owner did not return a result.');
    if (result.status === 401) return {kind:'anonymous', user:null, checkedAt:now()};
    if (result.status !== 200 || result.data?.valid !== true) throw Error('The account owner could not confirm a session.');
    const user = result.data.user;
    if (!user || !Number.isSafeInteger(user.id) || user.id < 1 || typeof user.name !== 'string' || user.name.length > 512) throw Error('The account response has invalid identity fields.');
    // Do not retain credentials, grants or the entire owner response in the presentation cache.
    return {kind:'authenticated', user:Object.freeze({id:user.id, name:user.name}), checkedAt:now()};
  };
  async function refresh({force = false} = {}) {
    if (disposed) throw Error('Session presentation has been disposed.');
    if (mutation) return state;
    if (pending) return pending.promise;
    if (!force && state.checkedAt !== null && now() - state.checkedAt >= 0 && now() - state.checkedAt < maxAge) return state;
    const token = ++epoch, controller = new AbortController();
    // Never keep private identity visible while a resumed document is being rechecked.
    emit({kind:'checking', user:null, checkedAt:null});
    const timer = setTimeout(() => controller.abort(), timeout);
    const job = {controller, promise:null}; pending = job;
    job.promise = (async () => {
      try {
        const result = await bounded(read,controller.signal);
        if (disposed || token !== epoch || controller.signal.aborted) return state;
        return emit(observe(result));
      } catch (error) {
        if (disposed || token !== epoch) return state;
        record(error);
        return emit({kind:'unavailable', user:null, checkedAt:null});
      } finally { clearTimeout(timer); if (pending === job) pending = null; }
    })();
    return job.promise;
  }
  async function logout() {
    if (disposed) throw Error('Session presentation has been disposed.');
    if (mutation) return mutation;
    invalidate('logout-check');
    const token = epoch, controller = new AbortController(), timer = setTimeout(() => controller.abort(), timeout);
    const work = (async () => {
      let accepted = false;
      try {
        const response = await bounded(revoke,controller.signal);
        accepted = response?.status === 401 || response?.status === 204 || response?.status === 200;
      } catch (error) { record(error); }
      finally { clearTimeout(timer); }
      if (disposed || epoch !== token) return {confirmed:false, accepted, state};
      // Read the owner to reconcile even a lost mutation response. Never repeat the POST.
      const check = new AbortController(), readTimer = setTimeout(() => check.abort(), timeout);
      try {
        const result = await bounded(read,check.signal);
        if (!disposed && epoch === token && !check.signal.aborted) emit(observe(result));
      } catch (error) { if (!disposed && epoch === token) {record(error); emit({kind:'unavailable',user:null,checkedAt:null});} }
      finally { clearTimeout(readTimer); }
      return {confirmed:epoch === token && state.kind === 'anonymous', accepted, state};
    })();
    mutation = work;
    try {return await work;} finally {if (mutation === work) mutation = null;}
  }
  return Object.freeze({
    get state() {return state;}, get busy() {return Boolean(pending || mutation);},
    refresh, logout,
    invalidate: (reason='changed') => invalidate(reason),
    subscribe(listener) {if (disposed || typeof listener !== 'function') throw Error('A live subscriber is required.'); listeners.add(listener); try {listener(state);} catch (error) {record(error);} return () => listeners.delete(listener);},
    diagnostics: () => [...diagnostics],
    dispose() {invalidate('disposed'); disposed = true; listeners.clear();}
  });
}

async function readBounded(response, maxBytes=128*1024) {
  if (!response.body) throw Error('The account response has no body.');
  const reader = response.body.getReader(), chunks = []; let size = 0;
  try {
    for (;;) {const {done,value} = await reader.read(); if (done) break; size += value.byteLength; if (size > maxBytes) throw Error('Account response exceeded its limit.'); chunks.push(value);}
  } catch (error) {await reader.cancel().catch(()=>{}); throw error;} finally {reader.releaseLock();}
  const body = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) {body.set(chunk,offset); offset += chunk.byteLength;}
  return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(body));
}
export function createNativeAccountSession({fetch:request=globalThis.fetch, origin=globalThis.location?.origin, ...options}={}) {
  const base = new URL(origin);
  if (!['http:','https:'].includes(base.protocol) || base.username || base.password || base.pathname !== '/' || base.search || base.hash) throw Error('Use the current native origin.');
  const invoke = async (path, method, signal) => request(new URL(path,base).href, {method, signal, credentials:'same-origin', cache:'no-store', redirect:'error', headers:{Accept:'application/json'}});
  return createAccountSession({...options,
    read:async signal => {const r=await invoke('/account','GET',signal); if(r.status!==200)return {status:r.status}; if(!/application\/json\b/i.test(r.headers.get('content-type')||''))throw Error('Expected the native account representation.'); return {status:r.status,data:await readBounded(r)};},
    revoke:async signal => {const r=await invoke('/logout','POST',signal); return {status:r.status};}
  });
}
/** Install once at the native host, not in each pane. Broadcasts contain no identity or tokens. */
export function bindAccountLifecycle(session, window, {channelName='aurekai-account-observation'}={}) {
  let disposed=false;
  const refresh=()=>{if(!disposed) void session.refresh({force:true}).catch(()=>{});};
  const hide=()=>session.invalidate('document-suspended');
  const show=event=>{if(event.persisted)refresh();};
  const visible=()=>{if(window.document.visibilityState==='visible')refresh();};
  const channel=typeof window.BroadcastChannel==='function'?new window.BroadcastChannel(channelName):null;
  if(channel)channel.onmessage=event=>{if(event.data==='recheck'){session.invalidate('another-tab');refresh();}};
  window.addEventListener('pagehide',hide);window.addEventListener('pageshow',show);window.document.addEventListener('visibilitychange',visible);
  return Object.freeze({changed(){session.invalidate('local-change');channel?.postMessage('recheck');refresh();},dispose(){disposed=true;window.removeEventListener('pagehide',hide);window.removeEventListener('pageshow',show);window.document.removeEventListener('visibilitychange',visible);channel?.close();}});
}
