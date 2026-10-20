// Candidate for the existing account page. Auth is read through the shared kit.
(() => {
  'use strict';
  const $ = selector => document.querySelector(selector);
  const out = $('#out'), signed = $('#signed'), identity = $('#identity');
  const connections = $('#connections'), presence = $('#presence');
  const connect = $('#connect'), logout = $('#logout');
  const privateRoots = [presence,...document.querySelectorAll('.account-tools-host [hx-get]')].filter(Boolean);
  for (const root of privateRoots) { root.setAttribute('hx-trigger','account-ready'); root.setAttribute('hx-history','false'); }
  if (!out || !signed || !identity || !connections || !presence || !connect || !logout) throw Error('Incomplete account page.');
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const row = (label, value) => `<div><span class="label">${esc(label)}</span><strong>${esc(value ?? '—')}</strong></div>`;
  let epoch = 0, checkEpoch = 0, linksEpoch = 0, user = null, checking = null, posting = false, suspended = false, lastCheck = 0, checkTimer = 0;
  const reads = new Set(), profileRequests = new Map();
  function notice(root, message, failed = false) {
    let node = root.querySelector('[data-account-notice]');
    if (!node) { node = document.createElement('p'); node.dataset.accountNotice = ''; node.className = 'note'; root.append(node); }
    node.setAttribute('role', failed ? 'alert' : 'status'); node.textContent = message;
  }
  function cancelReads() { for (const controller of reads) controller.abort(); reads.clear(); }
  function clearPrivate() {
    epoch++; cancelReads(); linksEpoch++; user = null; signed.hidden = true;
    identity.replaceChildren(); connections.replaceChildren(); for (const root of privateRoots) root.replaceChildren();
    for (const [xhr, request] of profileRequests) if (request.method === 'get') xhr.abort(); profileRequests.clear();
    for (const shelf of document.querySelectorAll('.account-tools-host')) shelf.hidden = true;
    connect.reset();
  }
  function signalChange() { window.dispatchEvent(new Event('bonfyre:account-changed')); }
  function unknown() {
    clearPrivate(); out.hidden = false; document.documentElement.dataset.accountState = 'unavailable';
    out.innerHTML = '<strong>Your account could not be checked.</strong><p>No sign-out has been confirmed.</p><button type="button" data-account-retry>Check again</button>';
  }
  function anonymous(message = 'Sign in to open your account.') {
    clearPrivate(); out.hidden = false; document.documentElement.dataset.accountState = 'anonymous';
    const next = encodeURIComponent(location.pathname + location.search + location.hash);
    out.innerHTML = `<strong>${esc(message)}</strong><p><a href="/session/continue?next=${next}">Sign in</a> or <a href="/signup?next=${next}">create an account</a>.</p>`;
  }
  async function request(path, {method = 'GET', body} = {}) {
    const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 10000);
    if (method === 'GET') reads.add(controller);
    try {
      const response = await fetch(path, {method, body, headers:{Accept:'application/json'}, credentials:'same-origin', cache:'no-store', signal:controller.signal});
      if (response.status === 401 || response.status === 403) return {ok:false, status:response.status, body:null};
      if (method === 'POST' && response.redirected && new URL(response.url).pathname !== '/account') throw Error('Unexpected account continuation.');
      const reader = response.body?.getReader(), chunks = []; let size = 0;
      if (reader) try { for (;;) { const part = await reader.read(); if (part.done) break; size += part.value.byteLength; if (size > 1024 * 1024) { await reader.cancel(); throw Error('Account response exceeds its limit.'); } chunks.push(part.value); } } finally { reader.releaseLock(); }
      const bytes = new Uint8Array(size); let at = 0; for (const chunk of chunks) { bytes.set(chunk,at); at += chunk.byteLength; }
      const text = new TextDecoder('utf-8',{fatal:true}).decode(bytes), json = /^application\/json(?:;|$)/i.test(response.headers.get('content-type') || '');
      if (method === 'GET' && !json) throw Error('Account read returned an incompatible response.');
      return {ok:response.ok, status:response.status, body: json && text ? JSON.parse(text) : null};
    } finally { clearTimeout(timer); reads.delete(controller); }
  }
  function linkCard(link) {
    const id = Number(link.id), name = link.display_name || link.remote_account || link.provider || 'Account link';
    let url;
    try { url = new URL(link.profile_url); if (url.protocol !== 'https:' || url.username || url.password) url = null; } catch { url = null; }
    const title = url ? `<a href="${esc(url.href)}" rel="me noopener noreferrer" target="_blank">${esc(name)}</a>` : `<b>${esc(name)}</b>`;
    const remove = Number.isSafeInteger(id) && id > 0 ? `<form action="/account/connections/revoke" method="post"><input type="hidden" name="link_id" value="${id}"><button class="remove" type="submit">Remove</button></form>` : '';
    return `<article class="external-item"><span>${title}<br><small>${esc(link.provider)} · ${esc(link.remote_account)}</small></span>${remove}<p><span class="status">${esc(link.proof_state === 'declared' ? 'Declared — not verified' : link.proof_state)}</span><br>Public profile link. Provider access is configured separately.</p></article>`;
  }
  async function loadLinks() {
    if (!user || suspended) return;
    const accountTicket = epoch, ticket = ++linksEpoch;
    connections.setAttribute('aria-busy', 'true');
    try {
      const response = await request('/account/connections');
      if (accountTicket !== epoch || ticket !== linksEpoch || !user || suspended) return;
      if (response.status === 401) { await refresh(true); return; }
      if (!response.ok || !Array.isArray(response.body?.links) || response.body.links.length > 1000) throw Error('Account links unavailable.');
      const links = response.body.links.filter(link => link && typeof link === 'object' && link.proof_state !== 'revoked');
      connections.innerHTML = links.length ? links.map(linkCard).join('') : '<p class="empty">No outside accounts linked yet.</p>';
    } catch {
      if (accountTicket !== epoch || ticket !== linksEpoch || !user || suspended) return;
      connections.innerHTML = '<p role="alert">Account links could not be loaded. Your verified account is still open.</p><button type="button" data-links-retry>Retry account links</button>';
    } finally { if (accountTicket === epoch && ticket === linksEpoch) connections.removeAttribute('aria-busy'); }
  }
  function openPresence(ticket) {
    const run = () => { if (ticket === epoch && user && !suspended && window.htmx) for (const root of privateRoots) { htmx.process(root); htmx.trigger(root, 'account-ready'); } };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', run, {once:true}); else run();
  }
  function refresh(force = false) {
    if (suspended) return Promise.resolve(null);
    if (checking && !force) return checking.promise;
    if (!force && user && Date.now() - lastCheck < 15000) return Promise.resolve(user);
    if (force) K.invalidateSession();
    const ticket = ++checkEpoch, pending = {promise:null}; checking = pending;
    if (!user) { out.hidden = false; out.textContent = 'Checking your account…'; }
    pending.promise = (async () => {
      try {
        const session = await K.session();
        if (ticket !== checkEpoch || suspended) return null;
        lastCheck = Date.now();
        if (session.state === 'superseded') return null;
        if (session.state === 'anonymous') { anonymous(); signalChange(); return null; }
        if (!session.valid || !session.user) { unknown(); signalChange(); return null; }
        const changed = user?.id !== session.user.id;
        if (changed) clearPrivate();
        user = session.user;
        $('#welcome').textContent = `Welcome, ${user.name || 'friend'}.`;
        identity.innerHTML = row('Name', user.name) + row('Email', user.email) + row('Plan', user.tier) + row('Account', `#${user.id}`);
        document.documentElement.dataset.accountState = 'authenticated'; out.hidden = true; out.replaceChildren(); signed.hidden = false;
        for (const shelf of document.querySelectorAll('.account-tools-host')) shelf.hidden = false;
        if (changed || privateRoots.some(root => !root.childElementCount)) openPresence(epoch);
        void loadLinks(); if (changed) signalChange(); return user;
      } catch { if (ticket === checkEpoch && !suspended) { unknown(); signalChange(); } return null; }
    })().finally(() => { if (checking === pending) { checking = null; clearTimeout(checkTimer); if (!suspended) checkTimer = setTimeout(() => void refresh(true),60000); } });
    return pending.promise;
  }
  function formBody(form) {
    const body = new URLSearchParams();
    for (const [name, value] of new FormData(form)) { if (typeof value !== 'string') throw Error('Only text fields are accepted.'); body.append(name, value); }
    return body;
  }
  async function writeLink(form, path, message) {
    if (posting || !user || suspended) return;
    const ticket = epoch, button = form.querySelector('button'), body = formBody(form);
    posting = true; button.disabled = true;
    try {
      const response = await request(path, {method:'POST', body});
      if (ticket !== epoch || suspended) return;
      if (response.status === 401) { await refresh(true); return; }
      if (!response.ok) throw Error('Write unconfirmed.');
      if (form === connect) form.reset(); notice(connect, message); await loadLinks();
    } catch {
      if (ticket === epoch && !suspended) { notice(connect, 'The change could not be confirmed. Review the refreshed links before trying again.', true); await loadLinks(); }
    } finally { posting = false; if (button.isConnected) button.disabled = false; }
  }
  connect.addEventListener('submit', event => { event.preventDefault(); void writeLink(connect, '/account/connections', 'Account link saved.'); });
  connections.addEventListener('submit', event => {
    const form = event.target; if (!(form instanceof HTMLFormElement) || new URL(form.action).pathname !== '/account/connections/revoke') return;
    event.preventDefault(); void writeLink(form, '/account/connections/revoke', 'Account link removed.');
  });
  connections.addEventListener('click', event => { if (event.target.closest('[data-links-retry]')) void loadLinks(); });
  out.addEventListener('click', event => { if (event.target.closest('[data-account-retry]')) void refresh(true); });
  logout.addEventListener('submit', async event => {
    event.preventDefault(); if (posting || !user || suspended) return;
    const button = logout.querySelector('button'); posting = true; button.disabled = true;
    try {
      // Always reconcile the cookie after the request, including a lost reply.
      await request('/logout', {method:'POST'}).catch(() => null);
      if (suspended) return;
      await refresh(true);
      if (user) notice(logout, 'Sign-out was not confirmed. Your session is still active.', true);
      else if (document.documentElement.dataset.accountState === 'anonymous') anonymous('Signed out of this territory.');
    } finally { posting = false; button.disabled = false; }
  });
  document.addEventListener('htmx:beforeRequest', event => {
    const detail = event.detail, element = detail?.elt;
    if (!element || !privateRoots.some(root => element === root || root.contains(element))) return;
    if (!user || suspended) { event.preventDefault(); return; }
    profileRequests.set(detail.xhr, {epoch, method:String(detail.requestConfig?.verb || 'get').toLowerCase()});
  });
  document.addEventListener('htmx:beforeSwap', event => {
    const detail = event.detail;
    if (!detail || !privateRoots.some(root => detail.target === root || root.contains(detail.target))) return;
    if (!user || suspended || profileRequests.get(detail.xhr)?.epoch !== epoch) { detail.shouldSwap = false; event.preventDefault(); }
  });
  document.addEventListener('htmx:afterRequest', event => {
    const detail=event.detail, context=profileRequests.get(detail?.xhr);profileRequests.delete(detail?.xhr);
    if(!context||context.epoch!==epoch||!user||suspended||detail.successful!==false)return;
    const root=privateRoots.find(root=>detail.target===root||root.contains(detail.target));if(!root)return;
    if(detail.xhr?.status===401){void refresh(true);return;}
    root.replaceChildren();const text=document.createElement('p');text.setAttribute('role','alert');
    text.textContent=detail.xhr?.status===403?'This account does not have access to these controls.':'This account section could not be loaded. Your verified account is still open.';
    const retry=document.createElement('button');retry.type='button';retry.textContent='Retry this section';retry.onclick=()=>{if(user&&!suspended)htmx.trigger(root,'account-ready');};root.append(text,retry);
  });
  function suspend() { suspended = true; clearTimeout(checkTimer); checkEpoch++; checking = null; K.invalidateSession(); clearPrivate(); out.hidden = false; out.textContent = 'Checking your account…'; }
  window.addEventListener('pagehide', suspend);
  window.addEventListener('pageshow', event => { suspended = false; if (event.persisted) void refresh(true); });
  document.addEventListener('visibilitychange', () => { if (document.hidden) suspend(); else { suspended = false; void refresh(true); } });
  window.addEventListener('focus', () => { if (!suspended) void refresh(); });
  void refresh(true);
})();
