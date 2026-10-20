  // Coalesce only an in-flight identity read; never retain a completed session.
  K._session = null;
  let sessionEpoch = 0, sessionFlight = null;
  K.invalidateSession = () => { sessionEpoch++; const old = sessionFlight; sessionFlight = null; old?.controller.abort(); K._session = null; };
  K.session = () => {
    if (sessionFlight) return sessionFlight.promise;
    const flight = {epoch:sessionEpoch, controller:new AbortController(), promise:null}; sessionFlight = flight;
    const timer = setTimeout(() => flight.controller.abort(), 8000);
    const state = name => Object.freeze({state:name, valid:false, who:null, user:null});
    flight.promise = (async () => {
      try {
        const response = await fetch('/account', {headers:{Accept:'application/json'}, credentials:'same-origin', cache:'no-store', redirect:'error', signal:flight.controller.signal});
        if (response.status === 401) return flight.epoch === sessionEpoch ? state('anonymous') : state('superseded');
        if (!response.ok || !/^application\/json(?:;|$)/i.test(response.headers.get('content-type') || '')) throw Error('Invalid identity response.');
        const reader = response.body.getReader(), chunks = []; let length = 0;
        try { for (;;) { const part = await reader.read(); if (part.done) break; length += part.value.byteLength; if (length > 32768) { await reader.cancel(); throw Error('Identity response too large.'); } chunks.push(part.value); } }
        finally { reader.releaseLock(); }
        const bytes = new Uint8Array(length); let offset = 0;
        for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
        const data = JSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(bytes)), user = data?.user;
        if (flight.epoch !== sessionEpoch) return state('superseded');
        if (data.valid !== true || !user || !Number.isSafeInteger(user.id) || user.id < 1 ||
            !['name','email','tier'].every(key => user[key] == null || (typeof user[key] === 'string' && user[key].length <= 1024))) throw Error('Invalid account.');
        const profile = Object.freeze({id:user.id, name:user.name || '', email:user.email || '', tier:user.tier || ''});
        return Object.freeze({state:'authenticated', valid:true, who:profile.name || 'this account', user:profile});
      } catch { return state(flight.epoch === sessionEpoch ? 'unavailable' : 'superseded'); }
      finally { clearTimeout(timer); if (sessionFlight === flight) sessionFlight = null; }
    })();
    return flight.promise;
  };
  window.addEventListener('pagehide', K.invalidateSession);
  document.addEventListener('visibilitychange', () => { if (document.hidden) K.invalidateSession(); });
