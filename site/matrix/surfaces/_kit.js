/* Shared kit for the clone surfaces.
 *
 * Every surface below borrows a market interface's *behaviour* -- the layout,
 * density and gesture people already know -- and reads Aurekai's own live
 * endpoints. Nothing here imports anybody's ontology: the records are the same
 * subjects the rest of the estate addresses, rendered in a shape a person has
 * already learned somewhere else.
 *
 * Three rules hold across all of them, because each has a failure that looks
 * fine:
 *
 *   a 4xx is an answer      htmx and fetch both leave a placeholder up forever
 *                           if a refusal is treated as an outage. Every pane
 *                           reports refused / empty / failed as three different
 *                           things and offers the session when one is needed.
 *
 *   verbs are offered       Authority gates at effect time. A chip that posted
 *                           would be asking for a decision this surface has no
 *                           standing to carry, so nothing here submits.
 *
 *   the source is named     Every pane says which endpoint produced it. A
 *                           surface that hides its source cannot be checked
 *                           against the deployment it claims to describe.
 */
(() => {
  const K = {};

  K.host = location.hostname;
  K.territory = K.host.endsWith('.life') ? 'life'
              : K.host.endsWith('.shop') ? 'shop'
              : K.host.endsWith('.work') ? 'work'
              : 'live';

  /* A token in this browser's storage is not a session.
   *
   * The estate's session is an HttpOnly cookie, which script cannot read by
   * design. The kit asked localStorage instead and treated any string it found
   * as proof of being signed in -- so a stale token left by an old login made
   * every surface claim a session the estate had already stopped honouring,
   * and a real cookie session with no such string read as signed out.
   *
   * The cookie is authoritative. It is sent because these requests are
   * same-origin, and whether it is any good is the estate's answer to give.
   * The legacy bearer is still attached when present, because a token holder
   * that is not a browser has no cookie -- but its presence is never the
   * signal.
   */
  /* Browser authentication is the same-origin HttpOnly session cookie. */
  K.token = () => null;

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

  /* Signed in is a question only the estate can answer. */
  K.signedIn = async () => (await K.session()).valid;

  /* One fetch, three outcomes, never a silent blank. */
  K.get = async (path, accept) => {
    const headers = { Accept: accept || 'application/json' };
    const t = K.token();
    if (t) headers.Authorization = 'Bearer ' + t;
    let r;
    try {
      r = await fetch(path, { headers, credentials: 'same-origin' });
    } catch (e) {
      return { ok: false, kind: 'failed', status: 0, detail: String(e), path };
    }
    if (r.status === 401 || r.status === 403)
      return { ok: false, kind: 'refused', status: r.status, path };
    if (!r.ok)
      return { ok: false, kind: 'failed', status: r.status, path };
    const body = accept && accept.includes('html') ? await r.text() : await r.json();
    return { ok: true, kind: 'ok', status: r.status, body, path };
  };

  /* The three refusal states, rendered. A withheld pane offers the session
   * that would fill it; withholding without offering is a dead end wearing the
   * clothes of a loading state. */
  K.note = (res, what) => {
    const el = document.createElement('div');
    el.className = 'k-note ' + (res.kind === 'refused' ? 'k-refused' : 'k-failed');
    if (res.kind === 'refused') {
      /* Whether this reader is signed in is asked of the estate rather than of
       * this browser's storage, and the note is corrected when the answer
       * arrives: telling somebody to sign in when they already are, because a
       * token string was missing, is the failure this replaced. */
      el.innerHTML = '<strong>' + what + ' needs an admitted session.</strong>'
        + '<p>Authority-gated. Other panes on this page answer without one, so this '
        + 'is a refusal rather than an absent subject.</p>'
        + '<p class="k-session-note"><a class="k-btn" href="/login">Sign in</a></p>';
      K.session().then(s => {
        const slot = el.querySelector('.k-session-note');
        if (slot && s.valid)
          slot.textContent = 'You are signed in as ' + s.who + ' and this scope was '
            + 'still refused: the session you hold does not carry it.';
        else if(slot && s.state !== 'anonymous') slot.textContent = 'Account access could not be checked. Open Account to retry.';
      }).catch(() => {});
    } else {
      el.innerHTML = '<strong>' + what + ' did not answer.</strong>'
        + '<p><code>' + res.path + '</code> returned ' + (res.status || 'no response')
        + '. Nothing is rendered rather than a stale or invented list.</p>';
    }
    return el;
  };

  K.empty = (what, path) => {
    const el = document.createElement('div');
    el.className = 'k-note k-empty';
    el.innerHTML = '<strong>' + what + ' is empty.</strong><p><code>' + path
      + '</code> answered and returned nothing. That is an empty projection, not a '
      + 'refusal and not an absent subject.</p>';
    return el;
  };

  /* Parse the /_surface/<name> record fragments the address organ serves.
   * They are <dl class="surface-record"> with <dt>/<dd> pairs, and are the same
   * bytes a machine reading that path receives. */
  K.records = (html) => {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    return [...doc.querySelectorAll('.surface-record')].map(dl => {
      const rec = {};
      const dts = [...dl.querySelectorAll('dt')];
      const dds = [...dl.querySelectorAll('dd')];
      dts.forEach((dt, i) => { rec[dt.textContent.trim()] = (dds[i] || {}).textContent || ''; });
      return rec;
    });
  };

  K.surface = async (name) => {
    const res = await K.get('/_surface/' + name, 'text/html');
    if (!res.ok) return res;
    return { ...res, records: K.records(res.body) };
  };

  /* A verb chip. Offered, never submitted. */
  K.verbs = (list) => {
    const wrap = document.createElement('div');
    wrap.className = 'k-verbs';
    for (const v of list) {
      const s = document.createElement('span');
      s.className = 'k-verb';
      s.textContent = v;
      s.title = v + ' is offered here. Whether you may is decided at effect time '
              + 'by the owner of the effect, not by this surface.';
      wrap.appendChild(s);
    }
    return wrap;
  };

  K.sourceLine = (paths) => {
    const p = document.createElement('p');
    p.className = 'k-source';
    p.innerHTML = 'Reading ' + paths.map(x => '<code>' + x + '</code>').join(' · ')
      + ' — the same bytes a machine asking for this path receives.';
    return p;
  };

  K.mount = (el, node) => { el.innerHTML = ''; el.appendChild(node); };


  /* Avatars from DiceBear (https://dicebear.com), rendered from the subject's
   * own name as the seed, so the same room gets the same face everywhere in the
   * estate and nothing has to be uploaded or stored.
   *
   * The style is chosen by what the subject *is*, because a human face on a
   * venue or a binary would be the surface asserting a person exists:
   *
   *   subject   a room, a venue, an act, a programme   -> shapes
   *   machine   a binary, a queue, an endpoint         -> identicon
   *   peer      a federated instance                   -> rings
   *   person    an actual human actor                  -> initials
   *
   * Style licences: shapes is CC BY 4.0 (Alexis Doreau); identicon, rings and
   * initials are CC0 1.0. Attribution is carried in the page footer.
   */
  K.AVATAR_STYLE = { subject: 'shapes', machine: 'identicon', peer: 'rings',
                     person: 'initials' };

  K.avatar = (seed, kind, size) => {
    const style = K.AVATAR_STYLE[kind] || K.AVATAR_STYLE.subject;
    const p = new URLSearchParams({ seed: String(seed == null ? '' : seed) });
    if (size) p.set('size', String(size));
    p.set('radius', '12');
    return 'https://api.dicebear.com/9.x/' + style + '/svg?' + p.toString();
  };

  /* An <img> that falls back to the initials block if the avatar service is
   * unreachable. A broken image where a face should be reads as a broken page;
   * a monogram reads as a subject that has no picture, which is the truth. */
  K.avatarImg = (seed, kind, size) => {
    const s = String(seed == null ? '' : seed);
    const el = document.createElement('img');
    el.src = K.avatar(s, kind, size);
    el.alt = '';
    el.loading = 'lazy';
    el.width = size || 44; el.height = size || 44;
    el.dataset.seed = s;
    el.addEventListener('error', () => {
      const box = document.createElement('span');
      box.className = el.className;
      box.setAttribute('style', el.getAttribute('style') || '');
      box.textContent = s.replace(/[^a-z0-9 ]/gi, ' ').trim().split(/\s+/)
        .slice(0, 2).map(w => w[0]).join('').toUpperCase() || '?';
      el.replaceWith(box);
    }, { once: true });
    return el;
  };

  K.avatarAttribution = 'Avatars by <a href="https://dicebear.com">DiceBear</a>, generated '
    + "from each subject's own name. Shapes is CC BY 4.0 (Alexis Doreau); identicon, rings "
    + 'and initials are CC0 1.0.';


  /* ---- the address plane -------------------------------------------------
   *
   * A host on this estate is not a deployment, it is an institution in a
   * territory. leaflounge.aurekai.shop is the Leaf Lounge as the market sees
   * it; leaflounge.aurekai.life is the same institution as its own people see
   * it; theleaflounge.store is the same institution again, under a domain it
   * owns. Same code, same surfaces, different subject.
   *
   * So a surface must not guess whose host it is on from the hostname. It asks
   * the host, once, and the host answers from the Matrix. A page that guessed
   * would be right for leaflounge.aurekai.shop and wrong for
   * theleaflounge.store, which is exactly the case that matters.
   */
  K.here = null;
  K.address = async () => {
    if (K.here) return K.here;
    try {
      const r = await fetch('/.well-known/bonfyre', {headers:{Accept:'application/json'}, credentials:'same-origin'});
      K.here = r.ok ? await r.json() : {host: location.hostname, territory: K.territory};
    } catch (_) {
      K.here = {host: location.hostname, territory: K.territory};
    }
    return K.here;
  };

  /* The whole estate's address plane, for the surfaces that show it. */
  K.estate = async () => {
    try {
      const r = await fetch('/.well-known/bonfyre/estate', {headers:{Accept:'application/json'}, credentials:'same-origin'});
      return r.ok ? await r.json() : null;
    } catch (_) { return null; }
  };

  /* A subject's canonical address, and its room. The same path on every host,
   * because the address belongs to the subject and not to the territory. */
  K.subject = (kind, id) => '/s/' + encodeURIComponent(kind) + '/' + encodeURIComponent(id);
  K.room    = (kind, id) => '/room/' + encodeURIComponent(kind) + '/' + encodeURIComponent(id);

  /* An <a> to the subject, so a clone is a representation you can leave. */
  K.subjectLink = (kind, id, label, cls) => {
    const a = document.createElement('a');
    a.href = K.subject(kind, id);
    a.textContent = label === undefined ? id : label;
    a.title = 'Its address on this estate: ' + K.subject(kind, id)
            + ' — the same path on every host. This surface is one representation of it.';
    if (cls) a.className = cls;
    return a;
  };

  /* The institution's other territories, from the Matrix rather than from
   * string surgery on the hostname. Returns [] when this host holds only one. */
  K.institutionHosts = async () => {
    const a = await K.address();
    return (a && a.institution_hosts) || [];
  };

  /* Renders the switch as links, marking the one you are standing on. Any
   * surface can drop this in and become navigable across the institution. */
  K.hostSwitch = async (current) => {
    const hosts = await K.institutionHosts();
    const nav = document.createElement('nav');
    nav.className = 'k-hosts';
    nav.setAttribute('aria-label', 'This institution across territories');
    if (hosts.length < 2) return nav;
    for (const h of hosts) {
      const a = document.createElement('a');
      a.href = location.protocol + '//' + h.host + (current || location.pathname);
      a.textContent = '.' + h.territory;
      a.title = h.host + ' — audience ' + (h.audience || 'unstated');
      if (h.host === location.hostname) a.setAttribute('aria-current', 'page');
      nav.appendChild(a);
    }
    return nav;
  };


  /* ---- four territories, four sets of UX physics -------------------------
   *
   * Section 69: they are projections over one Matrix, not four products, but
   * their physics differ -- and building them as one feed with different icons
   * would lose exactly what makes each market leader good at what it does.
   *
   * So the difference here is not the chips. It is the ordering principle, and
   * each one answers a different question about the same records:
   *
   *   .live  what deserves attention now      ordered by what is on, decaying
   *   .life  what matters in my world         not ordered by attention at all
   *   .shop  what economic opportunity is useful   ordered by the offer
   *   .work  what should move forward         ordered by what blocks what
   *
   * A verb outside its territory is a category error, so the verb lists are
   * per territory and nothing merges them.
   */
  K.PHYSICS = {
    live: {
      question: 'What deserves attention now?',
      principle: 'fast attention / occurrence',
      orders_by: 'what is on now, and how hard it is pressing',
      decays: true,
      verbs: ['WATCH','JOIN','GO','FOLLOW','SAVE','SHARE'],
      /* attention is pressure: something happening now outranks something
         merely published, and a room reporting its fill outranks one that did
         not. This is the only territory where recency is allowed to rank. */
      rank: r => {
        const on = /doors|tonight|now|set |playing|open/i.test(r.tonight_status || '') ? 2 : 0;
        const pct = Number((String(r.capacity_note||'').match(/(\d+)\s*percent/)||[])[1] || 0);
        return on * 100 + pct;
      },
      cant: 'attention is not obligation. Nothing on this feed is expecting you, and a '
          + 'capacity note is what the room published rather than what is true at the door.'
    },
    life: {
      question: 'What matters in my world?',
      principle: 'durable personal world',
      orders_by: 'nothing — this is what you already keep, in a stable order',
      decays: false,
      verbs: ['MEET','JOIN','GO','SAVE','HELP','FOLLOW','PLAN','REMEMBER','SHARE','MOVE',
              'ASSIGN'],
      /* The load-bearing difference. A personal world ranked by attention
         would be a recommendation feed wearing the clothes of a kept set, and
         the thing that makes it yours is that it does not move when the world
         gets loud. Stable, alphabetical, no recency anywhere. */
      rank: null,
      cant: 'a kept subject is not a recommended one. Nothing here was ranked for you, and '
          + 'nothing moves up because it got busy.'
    },
    shop: {
      question: 'What economic opportunity is useful?',
      principle: 'economic opportunity / exchange',
      orders_by: 'the offer — what is on, and at what price',
      decays: false,
      verbs: ['BUY','OFFER','JOIN','SAVE','COMPARE','REQUEST','SELL','FUND','CONTRIBUTE'],
      /* An offer ranks; a description does not. A record with no offer sorts
         below every record that has one, because in this territory a subject
         without terms is not an opportunity. */
      rank: r => (r.offer_usd ? 1000 - Number(r.offer_usd) : -1),
      cant: 'an offer is terms a seller will accept. It is not a sale, a reservation, or '
          + 'anything anybody has bought.'
    },
    work: {
      question: 'What should move forward?',
      principle: 'obligation / execution',
      orders_by: 'what blocks what, and what undoing it would cost',
      decays: false,
      verbs: ['APPLY','APPROVE','CLAIM','FUND','CONTACT','BOOK','DO','REVIEW','ASSIGN',
              'RESPOND','ESCALATE'],
      rank: null,
      cant: 'an obligation is not an assignment, and a requirement is not funding.'
    }
  };

  K.physics = (t) => K.PHYSICS[t || K.territory] || K.PHYSICS.live;

  /* Order a record set by its territory's own principle. .life gets a stable
   * order on purpose: sorting a kept world by anything would be ranking it. */
  K.order = (records, t) => {
    const p = K.physics(t);
    const out = records.slice();
    if (!p.rank) {
      out.sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));
      return out;
    }
    out.sort((a, b) => p.rank(b) - p.rank(a)
                    || String(a.name || '').localeCompare(String(b.name || '')));
    return out;
  };

  /* A verb outside its territory is a category error, so this is the only
   * place a surface should get its verbs from. */
  K.verbsFor = (t) => K.physics(t).verbs.slice();


  /* ---- the fifteen commerce facts ----------------------------------------
   *
   * Section 70: .shop needs its own falsehood list, and it is long.
   *
   *   Product != Listing != Inventory != AvailabilityObservation != Offer !=
   *   Quote != Cart != Checkout != Commitment != Payment != Settlement !=
   *   Fulfilment != Delivery != Satisfaction != Ownership
   *
   * Each stage names the one owner that can establish it, because these
   * collapse when a subsystem is asked a question it cannot answer. The
   * dangerous collapses *skip* rather than step -- "paid" read as "delivered",
   * "delivered" read as "owned" -- so non-implication holds over every pair,
   * not over adjacent ones.
   *
   * And no stage may be skipped in the other direction either: an exchange
   * reporting delivery with no fulfilment is describing something the estate
   * never saw happen.
   */
  K.COMMERCE = [
    {stage:'Product', owner:'a catalogue',
     knows:'what a product is',
     cannot:'whether anybody has one',
     here:'/_surface/capabilities'},
    {stage:'Listing', owner:'a catalogue entry with terms attached',
     knows:'that this is offered somewhere, described this way',
     cannot:'that the description is current, or that the seller still means it',
     here:'/capability, /ucp/catalog, /acp/products'},
    {stage:'Inventory', owner:'the merchant',
     knows:'a count the merchant keeps',
     cannot:'that this buyer can obtain one',
     here:null},
    {stage:'AvailabilityObservation', owner:'an observer, at a time and a place',
     knows:'that it was obtainable then, there',
     cannot:'that it is obtainable now, here',
     here:null},
    {stage:'Offer', owner:'the seller',
     knows:'terms the seller will accept',
     cannot:'that anyone accepted them',
     here:'/capability'},
    {stage:'Quote', owner:'the pricing owner, for one caller',
     knows:'what it would cost this caller now',
     cannot:'anything held, reserved or promised',
     here:'/mpp/quote'},
    {stage:'Cart', owner:'the buyer, assembling',
     knows:'what one person put together, before anybody agreed to any of it',
     cannot:'that any of it is obtainable together',
     here:null},
    {stage:'Checkout', owner:'the exchange',
     knows:'that an intent to buy was expressed',
     cannot:'that it was accepted',
     here:null},
    {stage:'Commitment', owner:'CommitmentGraph',
     knows:'who owes what to whom, under which conditions',
     cannot:'that the resource moved',
     here:null},
    {stage:'Payment', owner:'a payment rail',
     knows:'that a charge was authorized',
     cannot:'that money settled',
     here:'the 402 on /capability/<name>'},
    {stage:'Settlement', owner:'the ledger, once a facilitator verifies',
     knows:'that money moved',
     cannot:'that anything was done for it',
     here:null},
    {stage:'Fulfilment', owner:'the seller, doing the thing',
     knows:'that the seller acted',
     cannot:'that it reached anybody',
     here:null},
    {stage:'Delivery', owner:'a courier',
     knows:'that a parcel moved',
     cannot:'that anyone wanted its contents',
     here:'/epcis/events'},
    {stage:'Satisfaction', owner:'the buyer, afterwards',
     knows:'that the person who received it says it was what they wanted',
     cannot:'that they hold it lawfully',
     here:null},
    {stage:'Ownership', owner:'.life, after the transaction leaves the store',
     knows:'that this is now part of somebody\'s world',
     cannot:'anything about how it got there',
     here:null}
  ];

  /* Non-implication over every pair, not adjacent ones: the dangerous
   * collapses skip. Answers for any two stages in either direction, because
   * reading "delivered" off "paid" is as wrong as the reverse. */
  K.commerceGap = (a, b) => {
    const i = K.COMMERCE.findIndex(x => x.stage === a);
    const j = K.COMMERCE.findIndex(x => x.stage === b);
    if (i < 0 || j < 0 || i === j) return null;
    const lo = K.COMMERCE[Math.min(i, j)], hi = K.COMMERCE[Math.max(i, j)];
    return lo.stage + ' does not establish ' + hi.stage + ': ' + lo.owner
         + ' knows ' + lo.knows + ' and not ' + hi.knows + '. Only ' + hi.owner
         + ' can establish that.';
  };

  /* What this deployment actually reaches, probed rather than declared. */
  K.commerceReached = async () => {
    const out = {};
    for (const s of K.COMMERCE)
      out[s.stage] = {reached:false, why:'no owner for this is admitted here'};
    const book = await K.surface('capabilities');
    if (book.ok && book.records.length)
      out.Product = {reached:true, why:book.records.length + ' in the native book'};
    const cap = await K.get('/capability');
    if (cap.ok && (cap.body.capabilities||[]).length) {
      const n = cap.body.capabilities.length;
      out.Listing = {reached:true, why:n + ' carried by three carriers'};
      out.Offer   = {reached:true, why:n + ' with terms a seller will accept'};
      const first = cap.body.capabilities[0];
      const q = await K.get('/mpp/quote?capability=' + encodeURIComponent(first.name));
      if (q.ok) out.Quote = {reached:true, why:'priced for this caller, holding nothing'};
      try {
        const g = await fetch(first.resource, {headers:{Accept:'application/json'}});
        if (g.status === 402) {
          const b = await g.json();
          const settles = b.settlement && b.settlement.facilitator_configured;
          out.Payment = {reached:false,
            why:'the resource answers 402 and wants a charge; nothing has authorized one'};
          out.Settlement = {reached:false,
            why: settles ? 'a facilitator is configured and nothing has settled'
                         : 'facilitator_configured is false, so nothing here can settle at all'};
        }
      } catch (_) {}
    }
    const ep = await K.get('/epcis/events');
    if (ep.ok) out.Delivery = {reached:false,
      why: ep.body.empty_reason || 'the event list is empty'};
    return out;
  };


  /* ---- the mark ----------------------------------------------------------
   *
   * Aurekai x Bonfyre, as a lockup rather than a word. Bonfyre is the engine;
   * Aurekai is the larger umbrella, and the two plates say which is which
   * before anybody reads a sentence about it.
   *
   *   [ knight ]  x  [ BY ]  BONFYRE
   *     paper          ink
   *
   * The knight is an engraving, so it keeps a paper ground on every theme --
   * inverting it for dark mode would turn a woodcut into a photographic
   * negative. The wordmark plate takes the page's ink instead, which is what
   * makes the pair read as two things beside each other rather than one badge.
   *
   * Every surface here runs a different donor's chrome, so this brings its own
   * styles, scoped and injected once, and depends on no donor class.
   */
  K.MARK_CSS = `
.bf-mk{display:inline-flex;align-items:center;gap:.5rem;margin-right:1.1rem;
  text-decoration:none;color:inherit;background:none;border:0;padding:0;cursor:pointer;
  font:inherit;line-height:1;flex:0 0 auto}
.bf-mk-plate{width:2.5rem;height:2.5rem;border-radius:.75rem;display:grid;place-items:center;
  flex:0 0 auto;overflow:hidden;border:1px solid rgba(128,128,128,.35);
  transition:transform .14s ease,box-shadow .14s ease}
.bf-mk:hover .bf-mk-plate{transform:translateY(-1px);box-shadow:0 2px 10px rgba(0,0,0,.18)}
.bf-mk-knight{background:#f4f2ec}
.bf-mk-knight img{width:88%;height:88%;object-fit:contain;display:block;mix-blend-mode:multiply}
.bf-mk-by{background:#0e1210;color:#f2f4f0;
  font:800 .82rem/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:-.04em}
.bf-mk-x{font:400 .78rem/1 ui-monospace,monospace;opacity:.45;flex:0 0 auto}
.bf-mk-word{font:800 .95rem/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:-.045em;
  white-space:nowrap}
.bf-mk-sm .bf-mk-plate{width:1.75rem;height:1.75rem;border-radius:.5rem}
.bf-mk-sm .bf-mk-by{font-size:.62rem}
.bf-mk-sm .bf-mk-word{font-size:.8rem}
.bf-mk-pop{position:fixed;z-index:2147483000;width:min(23rem,calc(100vw - 2rem));
  background:#0e1210;color:#e9efe6;border:1px solid #2a332c;border-radius:.75rem;
  box-shadow:0 10px 40px rgba(0,0,0,.45);padding:1rem 1.05rem;
  font:400 .82rem/1.5 ui-sans-serif,system-ui,sans-serif}
.bf-mk-pop[hidden]{display:none}
.bf-mk-pop h3{margin:0 0 .15rem;font:700 .95rem/1.2 ui-sans-serif,system-ui,sans-serif}
.bf-mk-pop .bf-mk-sub{margin:0 0 .7rem;opacity:.62;
  font:400 .72rem/1.4 ui-monospace,monospace}
.bf-mk-pop p{margin:0 0 .6rem}
.bf-mk-pop dl{margin:.7rem 0 0;display:grid;grid-template-columns:auto 1fr;gap:.2rem .7rem;
  font:400 .72rem/1.5 ui-monospace,monospace}
.bf-mk-pop dt{opacity:.5;text-transform:uppercase;letter-spacing:.08em;font-size:.62rem;
  align-self:center}
.bf-mk-pop dd{margin:0;word-break:break-word}
.bf-mk-pop .bf-mk-hosts{display:flex;gap:.45rem;flex-wrap:wrap;margin-top:.7rem}
.bf-mk-pop .bf-mk-hosts a{color:#b9d49a;text-decoration:none;
  font:400 .72rem/1 ui-monospace,monospace;border:1px solid #2a332c;border-radius:999px;
  padding:.28rem .55rem}
.bf-mk-pop .bf-mk-hosts a[aria-current]{background:#1b2a1d;color:#e9efe6}
.bf-mk-scrim{position:fixed;inset:0;z-index:2147482999;background:transparent}
.bf-mk-scrim[hidden]{display:none}
@media (prefers-reduced-motion:reduce){.bf-mk:hover .bf-mk-plate{transform:none}}
`;

  K.markStyles = () => {
    if (document.getElementById('bf-mk-style')) return;
    const st = document.createElement('style');
    st.id = 'bf-mk-style';
    st.textContent = K.MARK_CSS;
    document.head.appendChild(st);
  };

  /* Aurekai x Bonfyre. Pass {word:false} for a chrome that has no room for the
   * wordmark, {small:true} for a dense header. */
  K.mark = (opts) => {
    opts = opts || {};
    K.markStyles();
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'bf-mk' + (opts.small ? ' bf-mk-sm' : '');
    b.setAttribute('aria-haspopup', 'dialog');
    b.setAttribute('aria-expanded', 'false');
    b.title = 'Aurekai × Bonfyre — what this is';
    b.innerHTML =
      '<span class="bf-mk-plate bf-mk-knight">'
    + '<img src="/assets/aurekai-knight.png" alt="" width="40" height="40" '
    + 'loading="eager" decoding="async"></span>'
    + '<span class="bf-mk-x" aria-hidden="true">×</span>'
    + '<span class="bf-mk-plate bf-mk-by">BŸ</span>'
    + (opts.word === false ? '' : '<span class="bf-mk-word">BONFŸRE</span>');
    b.setAttribute('aria-label', 'Aurekai times Bonfyre');
    b.addEventListener('click', (e) => { e.preventDefault(); K.markPopover(b); });
    return b;
  };

  /* Swap a placeholder for the mark, keeping whatever spacing classes the
   * donor's own header put on that slot. replaceWith drops them otherwise, and
   * the mark ends up welded to the nav beside it. */
  K.mountMark = (id, opts) => {
    const slot = document.getElementById(id || 'bf-mk');
    if (!slot) return null;
    const m = K.mark(opts);
    if (slot.className) m.className += ' ' + slot.className;
    slot.replaceWith(m);
    return m;
  };

  K.markPopover = async (anchor) => {
    let pop = document.getElementById('bf-mk-pop');
    let scrim = document.getElementById('bf-mk-scrim');
    if (pop && !pop.hidden) { close(); return; }
    if (!pop) {
      scrim = document.createElement('div');
      scrim.id = 'bf-mk-scrim'; scrim.className = 'bf-mk-scrim'; scrim.hidden = true;
      pop = document.createElement('div');
      pop.id = 'bf-mk-pop'; pop.className = 'bf-mk-pop'; pop.hidden = true;
      pop.setAttribute('role', 'dialog');
      pop.setAttribute('aria-label', 'Aurekai and Bonfyre');
      document.body.append(scrim, pop);
      scrim.addEventListener('click', close);
      addEventListener('keydown', e => { if (e.key === 'Escape') close(); });
    }
    function close() {
      pop.hidden = true; scrim.hidden = true;
      if (anchor) anchor.setAttribute('aria-expanded', 'false');
    }

    const a = await K.address();
    const inst = (a.institution || '').replace(/^[a-z]+:/, '').replace(/^aurekai:/, 'aurekai / ');
    const hosts = a.institution_hosts || [];
    pop.innerHTML =
      '<h3>Aurekai × BonfŸre</h3>'
    + '<p class="bf-mk-sub">the umbrella × the engine</p>'
    + '<p>Bonfŷre is the engine: it addresses subjects, derives work from the difference '
    + 'between states, routes it, records what happened and projects the result. Aurekai is '
    + 'the larger umbrella the engine runs under.</p>'
    + '<p>This page is one projection. The same engine answers the same subjects on every '
    + 'host below, in that territory’s own physics.</p>'
    + '<dl>'
    + (inst ? '<dt>institution</dt><dd>' + inst + '</dd>' : '')
    + '<dt>host</dt><dd>' + (a.host || location.hostname) + '</dd>'
    + '<dt>territory</dt><dd>.' + (a.territory || '') + '</dd>'
    + (a.audience ? '<dt>audience</dt><dd>' + a.audience + '</dd>' : '')
    + (a.world ? '<dt>world</dt><dd>' + a.world + '</dd>' : '')
    + '</dl>'
    + (hosts.length > 1
        ? '<div class="bf-mk-hosts">' + hosts.map(h =>
            '<a href="' + location.protocol + '//' + h.host + location.pathname + '"'
            + (h.host === location.hostname ? ' aria-current="page"' : '')
            + '>.' + h.territory + '</a>').join('') + '</div>'
        : '')
    + '<div class="bf-mk-hosts"><a href="/territory">the address plane</a>'
    + '<a href="/">the estate</a></div>';

    pop.hidden = false; scrim.hidden = false;
    if (anchor) {
      anchor.setAttribute('aria-expanded', 'true');
      const r = anchor.getBoundingClientRect();
      pop.style.top = Math.min(r.bottom + 8, innerHeight - pop.offsetHeight - 8) + 'px';
      pop.style.left = Math.max(8, Math.min(r.left, innerWidth - pop.offsetWidth - 8)) + 'px';
    }
  };


  /* ---------------------------------------------------------------- extent
   *
   * S68: "Extent decides the action layer. Only a page or a workspace carries
   * one; a chip, card or section is a reference to a surface, and there is no
   * room to act inside a reference."
   *
   * The dial is served, not simulated: /_surface/<name>?extent=card is
   * answered by the address organ, which caps the records and withholds the
   * effect class itself. A client that dimmed its own verbs would be a client
   * being polite about a rule; this is the rule.
   */
  K.EXTENTS = ['chip', 'card', 'section', 'page', 'workspace'];
  K.extentRank = (e) => K.EXTENTS.indexOf(e);
  K.extentActs = (e) => K.extentRank(e) >= 3;
  K.EXTENT_ROWS = { chip: 1, card: 3, section: 8, page: 0, workspace: 0 };

  K.surfaceAt = async (name, extent) => {
    const q = extent ? '?extent=' + encodeURIComponent(extent) : '';
    const res = await K.get('/_surface/' + name + q, 'application/json');
    if (!res.ok) return res;
    return { ...res, records: res.body.records || [], meta: res.body };
  };

  K.catalogue = async () => {
    const res = await K.get('/_surface/');
    if (!res.ok) return res;
    return { ...res, surfaces: res.body.surfaces || [] };
  };

  /* ------------------------------------------------------------------ mode
   *
   * S76: four public territories, but two of them contain two materially
   * different experiences, and the answer is a mode within the territory
   * rather than a fifth silo -- aurekai.business would duplicate the Matrix
   * instead of projecting it.
   *
   * Verbs are per mode, not per territory, and a mode's verbs are a subset of
   * its territory's. Holding that rule is what caught the real gap: .life did
   * not carry ASSIGN until household needed it for chores.
   */
  K.MODES = {
    live: [
      { id: 'occurrence', label: 'Occurrence', of: 'live',
        blurb: 'What is happening now, and what a stay would be worth.',
        verbs: ['WATCH', 'JOIN', 'GO', 'FOLLOW', 'SAVE', 'SHARE'] }
    ],
    life: [
      { id: 'personal', label: 'Personal / social', of: 'life',
        blurb: 'A kept world. Nothing here is ranked by attention.',
        verbs: ['MEET', 'JOIN', 'GO', 'SAVE', 'HELP', 'FOLLOW', 'SHARE', 'REMEMBER'] },
      { id: 'household', label: 'Household / private', of: 'life',
        blurb: 'The smallest serious institution: people, home, food, money, time, memory.',
        verbs: ['PLAN', 'MOVE', 'SAVE', 'HELP', 'REMEMBER', 'ASSIGN', 'JOIN'] }
    ],
    shop: [
      { id: 'exchange', label: 'Exchange', of: 'shop',
        blurb: 'Economic opportunity, ordered by the offer.',
        verbs: ['BUY', 'OFFER', 'JOIN', 'SAVE', 'COMPARE', 'REQUEST', 'SELL', 'FUND',
                'CONTRIBUTE'] }
    ],
    work: [
      { id: 'professional', label: 'Human / professional', of: 'work',
        blurb: 'A person’s own obligations: what they can take, do and answer.',
        verbs: ['APPLY', 'CLAIM', 'CONTACT', 'BOOK', 'DO', 'RESPOND', 'REVIEW'] },
      { id: 'business', label: 'Business', of: 'work',
        blurb: 'ERP, finance, capital, BI, quant — what an institution approves and funds.',
        verbs: ['APPROVE', 'FUND', 'ASSIGN', 'REVIEW', 'ESCALATE', 'DO', 'CLAIM'] }
    ]
  };

  K.modes = (t) => K.MODES[t || K.territory] || K.MODES.live;
  K.mode = (t, id) => {
    const list = K.modes(t);
    return list.find(m => m.id === id) || list[0];
  };

  /* -------------------------------------------------------------- retention
   *
   * S61: retention is a lattice, not a boolean, and promotion up it is never
   * automatic. Nothing is promoted because it was popular, referenced, or
   * merely survived -- only evidence and explicit Actions crystallize.
   *
   * The dial is a floor: what this operator admits onto their own surface.
   * Setting a floor hides the tiers below it and says why, which is a
   * different statement from those tiers being empty.
   */
  K.RETENTION = [
    { tier: 'FLASH',         lives: 'seconds/minutes', holds: 'reactions, typing, live presence' },
    { tier: 'CHAT',          lives: 'minutes/hours',   holds: 'live discussion' },
    { tier: 'MOMENT',        lives: 'hours/days',      holds: 'a casual post, a local observation' },
    { tier: 'SOCIAL',        lives: 'days/weeks',      holds: 'ordinary public publication' },
    { tier: 'SAVED',         lives: 'user-selected',   holds: 'a person or Circle chose to keep it' },
    { tier: 'INSTITUTIONAL', lives: 'while it means something',
      holds: 'Event, Offer, Membership, Work, Proposal' },
    { tier: 'EVIDENTIARY',   lives: 'per policy',      holds: 'claim and effect witnesses' },
    { tier: 'ECONOMIC',      lives: 'ledger retention', holds: 'commitment, settlement' }
  ];
  K.retentionRank = (t) => K.RETENTION.findIndex(r => r.tier === t);

  /* A pattern's floor is the lowest tier its records could belong to. It is
   * read from what the pattern reads, never guessed from how busy it looks --
   * that guess is precisely the automatic promotion S61 forbids. */
  K.RETENTION_OF_PATTERN = {
    'night-events': 'MOMENT', 'night-live': 'MOMENT', 'night-life': 'SAVED',
    'night-shop': 'INSTITUTIONAL', 'night-work': 'INSTITUTIONAL',
    'night-subject': 'SAVED', 'doors': 'SOCIAL', 'ledger-doors': 'SOCIAL',
    'nav': 'SOCIAL', 'wiki': 'SOCIAL', 'surfaces': 'SOCIAL',
    'federations': 'MOMENT', 'federation-grammar': 'SOCIAL',
    'gyms': 'INSTITUTIONAL', 'frappe-apps': 'INSTITUTIONAL',
    'representations': 'INSTITUTIONAL', 'routes': 'INSTITUTIONAL',
    'backends': 'INSTITUTIONAL', 'capabilities': 'INSTITUTIONAL',
    'funding': 'ECONOMIC', 'capital-routes': 'ECONOMIC',
    'capital-transformations': 'ECONOMIC'
  };
  K.retentionOf = (pattern) => K.RETENTION_OF_PATTERN[pattern] || 'SOCIAL';

  /* --------------------------------------------------------------- foreign
   *
   * S60: external platforms are human distribution edges. TikTok carries the
   * video; Aurekai carries the meaning. What returns is a ForeignOccurrence,
   * and it returns as an observation with a receipt -- never as a claim about
   * the source, and never as a connection to it.
   */
  K.foreignEdges = async () => {
    const res = await K.get('/foreign/edges');
    if (!res.ok) return res;
    return { ...res, edges: res.body.edges || [], schema: res.body.schema };
  };

  /* Observing a source is an effect, and it is the one the estate already
   * owns: /studio/v1/self/foreign/observe does a robots preflight, retains the
   * permitted bytes with a receipt, and returns an observe-only boundary. A
   * session that does not carry `self` is refused here rather than at the
   * source. */
  K.observeForeign = async (twinId, url) => {
    const t = K.token();
    const headers = { 'Content-Type': 'application/json', Accept: 'application/json' };
    if (t) headers.Authorization = 'Bearer ' + t;
    let r;
    try {
      r = await fetch('/studio/v1/self/foreign/observe', {
        method: 'POST', headers, credentials: 'same-origin',
        body: JSON.stringify({ twin_id: twinId, url: url })
      });
    } catch (e) {
      return { ok: false, kind: 'failed', status: 0, detail: String(e),
               path: '/studio/v1/self/foreign/observe' };
    }
    const body = await r.text();
    if (r.status === 401 || r.status === 403)
      return { ok: false, kind: 'refused', status: r.status,
               path: '/studio/v1/self/foreign/observe' };
    if (!r.ok)
      return { ok: false, kind: 'failed', status: r.status, detail: body,
               path: '/studio/v1/self/foreign/observe' };
    try { return { ok: true, kind: 'ok', status: r.status, body: JSON.parse(body) }; }
    catch (_) { return { ok: true, kind: 'ok', status: r.status, body: { raw: body } }; }
  };


  /* The census says how to read a source; this is the only thing that reads
   * it. A row carries where the list is, which field is the title, where the
   * picture lives and how to build a URL around it. Nothing here knows that
   * PeerTube calls it thumbnailPath or that Lemmy calls it thumbnail_url --
   * that is the row's to say, and a reader that knows it has copied the
   * contract out of the census where the two can then disagree.
   *
   * Cannot infer: retained bytes -> a live source. This parses what was
   * retained under a receipt. Reading it for display is not promotion to
   * truth, and an empty result means this estate read nothing, not that the
   * source published nothing. */
  /* A source that answers for today keys its answer by today's date, so the
   * path to the list is a date. Writing the date into the row makes the row
   * true for one day and quietly empty after that -- the same reason the URL
   * carries a token rather than a date. Same vocabulary, both halves of the
   * contract: {today} and {yesterday} are Y/M/D for a URL path, and the -iso
   * forms are the dashed date a JSON key is written with. */
  K.dateToken = (s) => {
    if (!s || s.indexOf('{') < 0) return s;
    const d = new Date(), y = new Date(d.getTime() - 86400000);
    const w = new Date(d.getTime() - 7 * 86400000);
    const p = (n) => String(n).padStart(2, '0');
    const iso = (t) => t.getUTCFullYear() + '-' + p(t.getUTCMonth() + 1) + '-' + p(t.getUTCDate());
    const path = (t) => t.getUTCFullYear() + '/' + p(t.getUTCMonth() + 1) + '/' + p(t.getUTCDate());
    return s.replace(/\{today-iso\}/g, iso(d))
            .replace(/\{yesterday-iso\}/g, iso(y))
            .replace(/\{week-ago-iso\}/g, iso(w))
            .replace(/\{today\}/g, path(d))
            .replace(/\{yesterday\}/g, path(y))
            .replace(/\{week-ago\}/g, path(w));
  };

  K.walk = (obj, path) => {
    if (!path) return obj;               /* a body that is itself the list */
    path = K.dateToken(path);
    return path.split('.').reduce((o, k) => {
      if (o == null) return o;
      if (Array.isArray(o)) return /^\d+$/.test(k) ? o[+k] : undefined;
      if (typeof o !== 'object') return undefined;
      return o[k];
    }, obj);
  };

  /* A source that marks its own adult or sensitive material is taken at its
   * word. Honouring the flag is reading a field, not editorial judgement. */
  K.flagged = (x) => {
    for (const f of ['nsfw', 'sensitive', 'over_18', 'is_nsfw']) {
      if (x[f] === true) return true;
      if (x.post && x.post[f] === true) return true;
      if (x.community && x.community[f] === true) return true;
    }
    return false;
  };

  K._feedPic = (n) => {
    for (const sel of ['media\\:thumbnail', 'media\\:content', 'enclosure',
                       'thumbnail', 'image']) {
      const e = n.querySelector(sel);
      const u = e && (e.getAttribute('url') || e.getAttribute('href') || e.textContent);
      if (u && /^https?:/.test(u.trim())) return u.trim();
    }
    /* Some feeds only put the picture inside escaped HTML in the body. */
    const body = (n.querySelector('description, content, summary') || {}).textContent || '';
    const m = body.match(/<img[^>]+src=["']([^"']+)["']/i);
    return m ? m[1] : '';
  };

  K._feedOut = (n) => {
    const l = n.querySelector('link');
    const u = l && (l.getAttribute('href') || l.textContent);
    if (u && /^https?:/.test(u.trim())) return u.trim();
    const g = n.querySelector('guid, id');
    const gu = g && g.textContent;
    return gu && /^https?:/.test(gu.trim()) ? gu.trim() : '';
  };

  /* An image path that is not already absolute is completed the way the row
   * says: @field names a field on the item carrying the host that made it (a
   * federated set answers for many hosts, and a relative path resolves against
   * the maker, not the answerer); pre|post wraps an identifier; anything else
   * is a fixed origin to prepend. */
  K._imageURL = (item, row) => {
    let img = row.image_field ? K.walk(item, row.image_field) : '';
    if (typeof img !== 'string' || !img) return typeof img === 'string' ? img : '';
    if (/^https?:/.test(img)) return img;
    const base = row.image_base || '';
    if (base.startsWith('@')) {
      const h = K.walk(item, base.slice(1));
      return h ? 'https://' + String(h) + img : '';
    }
    if (base.includes('|')) {
      const cut = base.indexOf('|');
      return base.slice(0, cut) + img + base.slice(cut + 1);
    }
    return base ? base + img : '';
  };

  K.readSource = async (row, opts) => {
    const o = opts || {};
    const limit = o.limit || 24;
    let text;
    /* Boxed material is not projected. The caller can ask for it explicitly --
     * /observations/flagged has to be able to show what it is holding. */
    if (!o.includeFlagged) {
      const boxed = await K.flaggedTwins();
      if (boxed.has(row.twin))
        return { row, items: [], boxed: true,
                 why: 'held back from ordinary projection until its presentation '
                      + 'state is resolved' };
    }
    try {
      const r = await fetch('/foreign/body?twin=' + encodeURIComponent(row.twin),
                            { credentials: 'same-origin' });
      if (!r.ok) return { row, error: r.status, items: [] };
      text = await r.text();
    } catch (e) { return { row, error: 0, items: [] }; }

    if ((row.extract || '').startsWith('rss:')) {
      let doc;
      try { doc = new DOMParser().parseFromString(text, 'application/xml'); }
      catch (_) { return { row, error: 'parse', items: [] }; }
      const items = [...doc.querySelectorAll(row.extract.slice(4))].slice(0, limit)
        .map(n => {
          const d = n.querySelector('pubDate, updated, published, date');
          const who = n.querySelector('dc\\:creator, author > name, source');
          return {
            title: ((n.querySelector(row.title_field || 'title') || {}).textContent || '').trim(),
            meta: d ? (d.textContent || '').trim().slice(0, 16) : '',
            img: K._feedPic(n),
            href: K._feedOut(n),
            who: (who && (who.textContent || '').trim()) || row.source
          };
        }).filter(x => x.title);
      return { row, items };
    }

    let json;
    try { json = JSON.parse(text); } catch (_) { return { row, error: 'parse', items: [] }; }
    const arr = K.walk(json, row.extract);
    if (!Array.isArray(arr)) return { row, error: 'shape', items: [] };
    const items = arr.filter(x => x && !K.flagged(x)).slice(0, limit).map(x => {
      const t = K.walk(x, row.title_field);
      const m = row.meta_field ? K.walk(x, row.meta_field) : '';
      const h = row.link_field ? K.walk(x, row.link_field) : '';
      const s = row.sub_field ? K.walk(x, row.sub_field) : '';
      const img = K._imageURL(x, row);
      return {
        title: typeof t === 'string' ? t : (t == null ? '' : String(t)),
        meta: m == null || m === '' ? '' : String(m),
        img: typeof img === 'string' ? img : '',
        href: typeof h === 'string' ? h : '',
        who: (typeof s === 'string' && s) ? s : row.source,
        raw: x
      };
    }).filter(x => x.title);
    return { row, items };
  };


  /* --------------------------------------------------- the presentation layer
   *
   * A surface says what elements exist. A template says what they look like.
   * Keeping them apart is what stops the equation being "fifteen worlds,
   * fifteen designs" and then "two hundred and thirty-eight themes, two
   * hundred and thirty-eight sites".
   *
   * The roles are a closed list on purpose. An open token bag would let an
   * authored row set any custom property and reach past presentation into the
   * layout the surface owns.
   *
   * Cannot infer: a design language -> the publication it was studied from.
   * Wearing a newsroom's typography does not make anything on the page
   * reported, and a template row's reference is provenance, not a dependency.
   */
  K.ROLES = ['ground', 'ink', 'display', 'accent', 'muted', 'rule', 'panel',
             'face-display', 'face-body', 'face-mono', 'display-size',
             'display-weight', 'display-transform', 'display-tracking',
             'display-leading', 'radius', 'card-border', 'card-shadow',
             'gap', 'measure'];

  /* What a screen boxed does not get projected.
   *
   * A source's own nsfw/sensitive flags are honoured per item, which covers
   * only what a source chose to mark. A flag is this estate's own decision
   * about retained material, and a reader that does not ask about it will
   * happily turn boxed bytes into a card on the front door.
   *
   * Cannot infer: an unflagged twin is not a screened-and-cleared one. Most
   * material is never looked at by a screen that could see anything. */
  K._flagged = null;
  K.flaggedTwins = async () => {
    if (K._flagged) return K._flagged;
    const res = await K.surface('flagged-observations');
    K._flagged = new Set(res.ok
      ? (res.records || []).filter(r => (r.review_state || '') !== 'cleared')
                           .map(r => r.twin)
      : []);
    return K._flagged;
  };

  K._templates = null;
  K.templates = async () => {
    if (K._templates) return K._templates;
    const res = await K.surface('presentation-templates');
    K._templates = res.ok ? (res.records || []) : [];
    return K._templates;
  };

  /* Returns the template row it applied, or null. Every failure path returns
   * null and changes nothing, so the surface still renders whole under its own
   * defaults -- a template that half-applies is worse than one that does not
   * apply at all. */
  K.dress = async (name, el) => {
    if (!name) return null;
    const rows = await K.templates();
    const t = rows.find(r => r.template === name);
    if (!t) return null;

    let tok = t.layout_tokens;
    if (typeof tok === 'string') { try { tok = JSON.parse(tok); } catch (_) { tok = null; } }
    if (!tok || typeof tok !== 'object') return null;

    /* Inline, so a role the template sets beats the surface's own default
     * without either selector having to be more specific than the other. */
    const target = el || document.body;
    for (const k of K.ROLES) {
      const v = tok[k] !== undefined ? tok[k] : tok[k.replace(/-/g, '_')];
      if (typeof v === 'string' && v) target.style.setProperty('--w-' + k, v);
    }
    /* The faces have to arrive from somewhere, and only one font host is
     * reachable here; a face named and not loaded renders in the fallback
     * stack and the language is quietly not the language. */
    const href = tok.font_href || tok['font-href'];
    if (typeof href === 'string' && href.startsWith('https://fonts.googleapis.com/')
        && !document.querySelector('link[href="' + href + '"]')) {
      const l = document.createElement('link');
      l.rel = 'stylesheet'; l.href = href;
      document.head.appendChild(l);
    }
    document.documentElement.dataset.template = t.template;
    if (t.family) document.body.classList.add('t-' + t.family);
    return t;
  };

  /* What the address asked for beats what a row was authored with, so a
   * language is something a reader can try rather than something an author has
   * to commit to. */
  K.askedTemplate = () => {
    try { return new URLSearchParams(location.search).get('template') || null; }
    catch (_) { return null; }
  };

  /* ------------------------------------------------------------ the shape
   *
   * The whole point of putting density in the address: a shape is a URL, so it
   * can be handed to someone. A preference kept in this browser is a setting;
   * a preference in the address is a product someone else can open.
   *
   *   ron.aurekai.work/console?mode=business&keep=INSTITUTIONAL
   *                           &d=funding:section,capital-routes:card
   */
  K.shapeFromURL = (search) => {
    const p = new URLSearchParams(search === undefined ? location.search : search);
    const density = {};
    for (const part of (p.get('d') || '').split(',')) {
      if (!part) continue;
      const [name, ext] = part.split(':');
      if (name && K.EXTENTS.includes(ext)) density[name] = ext;
    }
    const x = p.get('x');
    return {
      mode: p.get('mode') || K.modes()[0].id,
      keep: p.get('keep') || 'FLASH',
      /* One extent for the whole surface, and only the exceptions named. The
       * first version wrote every pattern into `d`, which produced a correct
       * address nobody would ever send anyone. */
      extent: K.EXTENTS.includes(x) ? x : null,
      density,
      off: (p.get('off') || '').split(',').filter(Boolean)
    };
  };

  K.shapeToURL = (shape, path) => {
    const p = new URLSearchParams();
    if (shape.mode && shape.mode !== K.modes()[0].id) p.set('mode', shape.mode);
    if (shape.keep && shape.keep !== 'FLASH') p.set('keep', shape.keep);
    if (shape.extent && shape.extent !== 'page') p.set('x', shape.extent);
    const d = Object.entries(shape.density)
      .filter(([, v]) => v !== (shape.extent || 'page'))
      .map(([k, v]) => k + ':' + v).join(',');
    if (d) p.set('d', d);
    if (shape.off.length) p.set('off', shape.off.join(','));
    const q = p.toString();
    return (path || location.pathname) + (q ? '?' + q : '');
  };


  /* ------------------------------------------------------- the estate's dial
   *
   * The compiled estate pages -- /, /protocols, /market, /notes, /observations
   * and the rest -- load their content as HTMX fragments from the same
   * /_surface/ paths the console reads. They got the mark and nothing else,
   * which left the density dial as something only one page could turn.
   *
   * This puts it in the shared chrome. It reads `x` from the page's own URL,
   * rewrites every /_surface/ fragment request to carry that extent, and
   * re-fetches. The rule stays where it belongs: the organ caps the records and
   * withholds the effect class, and a page asking for more than a pattern was
   * authored to carry is refused by the organ rather than talked out of it
   * here.
   *
   * The dial is not shown where there is nothing to narrow.
   */
  K.FRAGMENT_SELECTOR = '[hx-get^="/_surface/"]';

  K.fragmentAt = (url, extent) => {
    const [path, q] = url.split('?');
    const p = new URLSearchParams(q || '');
    if (extent && extent !== 'page') p.set('extent', extent); else p.delete('extent');
    const s = p.toString();
    return path + (s ? '?' + s : '');
  };

  K.applyExtentToFragments = (extent) => {
    const els = [...document.querySelectorAll(K.FRAGMENT_SELECTOR)];
    for (const el of els) {
      const next = K.fragmentAt(el.getAttribute('hx-get'), extent);
      el.setAttribute('hx-get', next);
      if (window.htmx) htmx.ajax('GET', next, { target: el, swap: 'innerHTML' });
    }
    return els.length;
  };

  K.shapeBar = (mountId) => {
    const host = document.getElementById(mountId);
    if (!host) return null;
    const first = document.querySelector(K.FRAGMENT_SELECTOR);
    if (!first) { host.remove(); return null; }

    /* The dial sits with what it narrows, not in the navigation.
     *
     * It was mounted at the end of the nav first, which on the estate's own
     * pages meant either past the right edge of a nav already wider than the
     * viewport, or -- once the nav was allowed to wrap -- a two-line nav whose
     * first line was clipped by the header's own vertical centring. Neither is
     * the nav's fault. A control that changes a page's content belongs above
     * that content. */
    host.remove();
    first.parentNode.insertBefore(host, first);
    host.style.display = 'block';
    host.style.margin = '0 0 .6rem';

    const p = new URLSearchParams(location.search);
    let cur = K.EXTENTS.includes(p.get('x')) ? p.get('x') : 'page';

    if (!document.getElementById('bf-shape-css')) {
      const st = document.createElement('style');
      st.id = 'bf-shape-css';
      st.textContent = '.bf-shape{display:inline-flex;align-items:center;gap:.2rem;'
        + 'vertical-align:middle;font-size:.66rem;letter-spacing:.04em}'
        + '.bf-shape .lb{opacity:.55;text-transform:uppercase;margin-right:.15rem}'
        + '.bf-shape button{font:inherit;font-size:.66rem;cursor:pointer;padding:.1rem .38rem;'
        + 'border-radius:5px;border:1px solid currentColor;background:transparent;'
        + 'color:inherit;opacity:.5}'
        + '.bf-shape button[aria-pressed="true"]{opacity:1;font-weight:700}'
        + '.bf-shape button:hover{opacity:.85}';
      document.head.appendChild(st);
    }

    const bar = document.createElement('span');
    bar.className = 'bf-shape';
    bar.setAttribute('aria-label', 'Density of this page');
    const lb = document.createElement('span');
    lb.className = 'lb'; lb.textContent = 'density';
    bar.appendChild(lb);

    const draw = () => {
      [...bar.querySelectorAll('button')].forEach(b =>
        b.setAttribute('aria-pressed', String(b.dataset.ext === cur)));
    };
    for (const e of K.EXTENTS) {
      /* Workspace is not offered here. These pages compose patterns authored at
       * page extent, and asking the organ for more would be refused -- offering
       * a control whose only outcome is a 409 is worse than not offering it. */
      if (e === 'workspace') continue;
      const b = document.createElement('button');
      b.type = 'button'; b.dataset.ext = e; b.textContent = e;
      b.title = K.extentActs(e)
        ? 'Whole patterns, with their action layer.'
        : e + ': at most ' + K.EXTENT_ROWS[e] + ', and no action layer — a '
          + e + ' is a reference to a surface, not room to act inside one.';
      b.onclick = () => {
        cur = e;
        const q = new URLSearchParams(location.search);
        if (e === 'page') q.delete('x'); else q.set('x', e);
        const qs = q.toString();
        history.replaceState(null, '', location.pathname + (qs ? '?' + qs : ''));
        draw();
        if (!K.applyExtentToFragments(e) || !window.htmx) location.reload();
      };
      bar.appendChild(b);
    }
    draw();
    host.appendChild(bar);
    if (cur !== 'page') K.applyExtentToFragments(cur);
    return bar;
  };


  /* ----------------------------------------------------------- the frame
   *
   * subject / context / territory / audience / extent, carried in the address
   * so that changing the view keeps the world. The territory and audience come
   * from the host -- they are the host's, not a parameter -- and the rest is
   * the query string.
   */
  K.frame = (search) => {
    const p = new URLSearchParams(search === undefined ? location.search : search);
    return {
      subject: p.get('subject') || null,
      context: p.get('context') || null,
      territory: (K.here && K.here.territory) || K.territory,
      audience: (K.here && K.here.audience) || null,
      extent: K.EXTENTS.includes(p.get('x')) ? p.get('x') : null,
      route: location.pathname.replace(/\/$/, '') || '/'
    };
  };

  /* Moving the frame to another view keeps every coordinate but the route. */
  K.frameURL = (frame, route) => {
    const p = new URLSearchParams(location.search);
    if (frame.subject) p.set('subject', frame.subject); else p.delete('subject');
    if (frame.context) p.set('context', frame.context); else p.delete('context');
    if (frame.extent && frame.extent !== 'page') p.set('x', frame.extent); else p.delete('x');
    const q = p.toString();
    return (route || frame.route) + (q ? '?' + q : '');
  };

  /* ------------------------------------------------------- the renderers */
  K._renderers = null;
  K.renderers = async () => {
    if (K._renderers) return K._renderers;
    const res = await K.get('/_surface/renderers');
    K._renderers = res.ok ? (res.body.records || []) : [];
    return K._renderers;
  };

  /* Which route each renderer is deployed at today. Phase one of the
   * separation preserves every existing route; the renderer is what became
   * portable, not the URL it was extracted from. */
  K.VIEW_ROUTE = {
    vertical: '/discover', stage: '/stage', thread: '/channels', board: '/board',
    community: '/community', market: '/listings', search: '/parcels',
    spatial: '/parcels', work: '/inbox', inspector: '/territory'
  };

  /* Section 14's grouping. A view control that lists ten peers is a route list
   * with a new heading; these are the kinds of thing a person is doing. */
  K.VIEW_GROUP = [
    ['social',   ['vertical', 'stage', 'thread', 'community']],
    ['visual',   ['board', 'spatial']],
    ['economic', ['market']],
    ['work',     ['work', 'search']],
    ['inspect',  ['inspector']]
  ];

  /* ----------------------------------------------------------- the closure
   *
   * What behaviour is running for this actor, resolved by PackGraph: the
   * activated packs and their dependencies, what an overlay hid, the views the
   * closure asks for, the external reality it needs, and the policy ceiling
   * the overlays tightened to.
   *
   * The closure is why two people get different products from one kernel. It
   * is not a permission: Authority decides every effect at effect time.
   */
  K._closure = null;
  K.closure = async (scenario) => {
    if (K._closure && !scenario) return K._closure;
    const res = await K.get('/packs/closure' + (scenario
      ? '?scenario=' + encodeURIComponent(scenario) : ''));
    const c = res.ok ? res.body : null;
    if (!scenario) K._closure = c;
    return c;
  };

  /* A view illuminates when three things agree: a pack in this actor's closure
   * asks for the grammar, the renderer is deployed here, and this host serves
   * a pattern it can render.
   *
   * Any one of them alone is wrong. Deployment alone offers Ron a live stage
   * because the estate happens to have one. Closure alone offers a grammar
   * nothing here can fill. And a served pattern nobody renders is a projection
   * with no grammar. */
  K.viewsFor = async (frame) => {
    const [rends, cat, closure] = await Promise.all([
      K.renderers(), K.catalogue(), K.closure()
    ]);
    /* A closure that resolves nothing narrows nothing. An actor who activates
     * no packs is not an actor for whom every grammar is wrong -- it is an
     * actor nobody has configured, and unlighting the whole drawer would make
     * the estate's own hosts useless while looking like a considered refusal. */
    const asked = (closure && (closure.views || []).length)
      ? closure.views.map(v => v.view) : null;
    const hiddenViews = closure ? (closure.views_hidden || []) : [];
    const served = new Set((cat.ok ? cat.surfaces : []).map(s => s.pattern));
    const servedNames = [...served].join(' ').toLowerCase();
    return rends.map(r => {
      const claims = (r.renders || '').split(/[;,]/).map(s => s.trim()).filter(Boolean);
      const hit = claims.find(c => {
        const head = c.split(/\s+/)[0].toLowerCase();
        return served.has(head) || servedNames.includes(head);
      });
      const route = K.VIEW_ROUTE[r.renderer] || null;
      const wanted = !asked || asked.includes(r.renderer);
      const hidden = hiddenViews.includes(r.renderer);
      return {
        ...r, route, in_closure: wanted, hidden_by_overlay: hidden,
        lit: !!route && wanted && !hidden,
        renders_here: hit || null,
        why: !route ? 'not deployed on this host'
           : hidden ? 'an overlay in this closure hides it; the pattern behind it still '
                    + 'answers, and a machine asking for it gets the same bytes'
           : !wanted ? 'no pack in this closure asks for this grammar'
           : (hit ? 'renders ' + hit + ' on this host'
                  : 'deployed here; the pattern it renders is this surface’s own')
      };
    });
  };

  /* -------------------------------------------------------- view control */
  K.VIEW_CSS = `
.bf-view{position:relative;display:inline-flex;align-items:center;gap:.3rem;
  font-family:var(--bf-mono,'IBM Plex Mono',ui-monospace,monospace)}
.bf-view>i{font-style:normal;font-size:.58rem;letter-spacing:.1em;text-transform:uppercase;
  opacity:.55}
.bf-view button.bf-vtrig{font:inherit;font-size:.68rem;cursor:pointer;padding:.12rem .5rem;
  border-radius:5px;border:1px solid currentColor;background:transparent;color:inherit;
  opacity:.85}
.bf-view button.bf-vtrig:hover{opacity:1}
.bf-vdraw{position:absolute;top:calc(100% + .4rem);left:0;z-index:900;min-width:19rem;
  max-height:min(70vh,32rem);overflow:auto;padding:.5rem;border-radius:9px;
  background:#12151b;color:#e8eaef;border:1px solid #2a3140;
  box-shadow:0 18px 44px rgba(0,0,0,.55);font-family:inherit}
.bf-vdraw h4{margin:.45rem 0 .2rem;font-size:.56rem;letter-spacing:.12em;text-transform:uppercase;
  color:#6f7a90;font-weight:600}
.bf-vdraw h4:first-child{margin-top:0}
.bf-vrow{display:grid;grid-template-columns:5.6rem minmax(0,1fr);gap:.5rem;align-items:baseline;
  padding:.2rem .35rem;border-radius:5px;text-decoration:none;color:inherit}
.bf-vrow:hover{background:#1b202a}
.bf-vrow b{font-size:.72rem;font-weight:600}
.bf-vrow span{font-size:.64rem;color:#8b94a8;line-height:1.35}
.bf-vrow.off{opacity:.4;pointer-events:none}
.bf-vrow[aria-current]{background:#20252f;box-shadow:inset 2px 0 0 #ffb020}
.bf-vrow[aria-current] b{color:#ffb020}
.bf-vfoot{margin-top:.5rem;padding-top:.45rem;border-top:1px solid #2a3140;font-size:.62rem;
  color:#6f7a90;line-height:1.5}
.bf-vfoot code{color:#ffb020}
`;

  K.viewControl = async (mountId) => {
    const host = document.getElementById(mountId);
    if (!host) return null;
    if (!document.getElementById('bf-view-css')) {
      const st = document.createElement('style');
      st.id = 'bf-view-css';
      st.textContent = K.VIEW_CSS;
      document.head.appendChild(st);
    }
    const frame = K.frame();
    const views = await K.viewsFor(frame);
    if (!views.length) { host.remove(); return null; }

    const wrap = document.createElement('span');
    wrap.className = 'bf-view';
    const label = document.createElement('i');
    label.textContent = 'view';
    const trig = document.createElement('button');
    trig.type = 'button';
    trig.className = 'bf-vtrig';
    const mine = views.find(v => v.route === frame.route);
    trig.textContent = (mine ? mine.canonical_name.replace(/Renderer$/, '') : 'all views') + ' ▾';
    trig.setAttribute('aria-expanded', 'false');
    wrap.appendChild(label);
    wrap.appendChild(trig);

    const draw = document.createElement('div');
    draw.className = 'bf-vdraw';
    draw.hidden = true;
    /* Preferred views first inside each group, because an overlay that said
     * "work first" meant first and not merely present. */
    const order = (K._closure && (K._closure.views || []).map(v => v.view)) || [];
    const rank = (id) => { const i = order.indexOf(id); return i < 0 ? 99 : i; };
    for (const [group, ids] of K.VIEW_GROUP) {
      const inGroup = ids.map(id => views.find(v => v.renderer === id)).filter(Boolean)
        .sort((a, b) => rank(a.renderer) - rank(b.renderer));
      if (!inGroup.length) continue;
      const h = document.createElement('h4');
      h.textContent = group;
      draw.appendChild(h);
      for (const v of inGroup) {
        const a = document.createElement('a');
        a.className = 'bf-vrow' + (v.lit ? '' : ' off');
        a.href = v.lit ? K.frameURL(frame, v.route) : '#';
        if (v.route === frame.route) a.setAttribute('aria-current', 'page');
        const b = document.createElement('b');
        b.textContent = v.canonical_name.replace(/Renderer$/, '');
        const s = document.createElement('span');
        s.textContent = (v.grammar || '').split(/[,;]/)[0];
        a.appendChild(b); a.appendChild(s);
        a.title = v.canonical_name + (v.donor ? ' — ' + v.donor + '’s grammar' : '')
                + '\n' + v.why + '\ncannot infer: ' + (v.cannot_infer || '');
        draw.appendChild(a);
      }
    }
    const foot = document.createElement('div');
    foot.className = 'bf-vfoot';
    const c = K._closure;
    foot.innerHTML = 'A view is a grammar, not a destination: the frame — subject, context, '
      + 'territory, audience — stays, and only the way you read it changes.'
      + (c && c.pack_count
           ? '<br>' + c.pack_count + ' packs running for <code>' + c.actor
             + '</code>' + (c.views_hidden && c.views_hidden.length
               ? ', ' + c.views_hidden.length + ' view'
                 + (c.views_hidden.length === 1 ? '' : 's') + ' hidden by an overlay' : '')
             + '. <a href="/packs">the closure</a>'
         : c ? '<br>No pack closure for <code>' + c.actor + '</code>, so this is what is '
               + 'deployed rather than what was chosen. <a href="/packs">the graph</a>'
         : '')
      + '<br>Registry at <code>/_surface/renderers</code>.';
    draw.appendChild(foot);
    wrap.appendChild(draw);

    trig.onclick = () => {
      draw.hidden = !draw.hidden;
      trig.setAttribute('aria-expanded', String(!draw.hidden));
    };
    addEventListener('click', (e) => {
      if (!wrap.contains(e.target)) { draw.hidden = true; trig.setAttribute('aria-expanded', 'false'); }
    });
    addEventListener('keydown', (e) => { if (e.key === 'Escape') draw.hidden = true; });

    host.appendChild(wrap);
    return wrap;
  };

  K.nav = (current) => {
    const items = [
      ['/discover', 'Discover'], ['/board', 'Boards'], ['/community', 'Community'],
      ['/listings', 'Listings'], ['/parcels', 'Parcels'],
      ['/inbox', 'Inbox'], ['/channels', 'Channels'], ['/stage', 'Stage'],
      ['/territory', 'Territory']
    ];
    const nav = document.createElement('nav');
    nav.className = 'k-nav';
    nav.setAttribute('aria-label', 'Surfaces');
    for (const [href, label] of items) {
      const a = document.createElement('a');
      a.href = href; a.textContent = label;
      if (href === current) a.setAttribute('aria-current', 'page');
      nav.appendChild(a);
    }
    return nav;
  };

  window.K = K;
})();
