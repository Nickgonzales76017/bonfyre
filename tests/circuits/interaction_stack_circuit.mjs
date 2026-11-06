#!/usr/bin/env bun
// Person-level interaction stack: subjects, apertures, lenses, rooms, creation and desk.
import { options, circuit } from './client.mjs';

const matches = (html, expression) => [...String(html).matchAll(expression)].map(match => match[1]);
const acts = html => matches(html, /<span class="ap-do">([^<]*)<\/span>/g);
const directions = html => matches(html, /<span class="tr-label">([^<]*)<\/span>/g);

export async function run(config, transport = circuit(config)) {
  if (!config.parcel) throw new Error('--parcel is required for interaction-stack assertions');
  const { http, check, skip, finish } = transport;
  const get = async (path, token = '', effect = false) => http(path, { headers: { Accept: 'text/html' }, token, effect, timeout: 300_000 });
  const body = response => typeof response.data === 'string' ? response.data : response.text;
  const parcel = encodeURIComponent(config.parcel);
  const anon = await get(`/s/parcel/${parcel}`), anonHtml = body(anon);
  check('subject renders for stranger', anon.status === 200 && anonHtml.includes('ap-do'), `${acts(anonHtml).length} actions`);
  if (config.token) {
    const auth = await get(`/s/parcel/${parcel}`, config.token), a1 = new Set(acts(anonHtml)), a2 = new Set(acts(body(auth)));
    check('same subject renders for operator', auth.status === 200, `${a2.size} actions`);
    check('apertures differ', [...a1].some(value => !a2.has(value)) || [...a2].some(value => !a1.has(value)));
    check('only operator offered write act', a2.has('Watch this parcel') && !a1.has('Watch this parcel'));
    check('stranger told how to act instead', [...a1].some(value => value.includes('Sign in')));
    check('aperture stays within 2–5 actions', a2.size >= 2 && a2.size <= 5);
  } else skip('authenticated aperture comparison', 'set BONFYRE_CIRCUIT_TOKEN');
  const dirs = directions(anonHtml);
  check('directions offered', dirs.length >= 3, dirs.slice(0, 4).join(', '));
  check('unsupported direction is named', dirs.includes('Near') && dirs.includes('Changed since'));
  check('live direction carries a count', /\d+ (other parcels|parcels)/.test(anonHtml));
  check('subject lists ambient carriers', ['RESO Web API', 'OGC API Features', 'MISMO 3.4'].every(value => anonHtml.includes(value)));

  const lens = await get('/lens');
  check('lenses are cuts, not tabs', lens.status === 200 && body(lens).split('tr-dir').length - 1 >= 3);
  const seen = [];
  for (const slug of ['vacant-land', 'concentrated-owners', 'newest-building']) {
    const response = await get(`/lens/${slug}`), html = body(response);
    const selection = html.match(/<dt>Selection<\/dt><dd class="lens-field">([^<]*)/)?.[1] || '';
    const representation = html.match(/<dt>Representation<\/dt><dd class="lens-field">([^<]*)/)?.[1] || '';
    const items = matches(html, /<span class="li-weight">([^<]*)<\/span>/g);
    seen.push({ slug, selection, representation, items: items.length });
    check(`lens ${slug} returns populated cut`, response.status === 200 && items.length > 0, `${items.length} items`);
  }
  check('every lens declares selection and representation', seen.every(row => row.selection && row.representation));
  check('lenses differ in both dimensions', new Set(seen.map(row => row.selection)).size === 3 && new Set(seen.map(row => row.representation)).size === 3);
  const room = await get(`/room/parcel/${parcel}`), roomHtml = body(room);
  const bands = matches(roomHtml, /<h3 class="room-h">([^<]*)<\/h3>/g);
  check('room shaped by its subject', room.status === 200 && bands.length >= 3, bands.join(' | '));
  check('room names missing source fact', roomHtml.includes('is-absent') && roomHtml.includes('no prior state'));
  check('unbuilt room says so', body(await get('/room/occurrence/nothing-here')).includes('built rather than templated'));

  for (const [sentence, want] of [['I need somebody to help translate this README', 'Work'], ['selling this cutting for $20', 'Offer'], ['we are starting a neighborhood clean-up', 'Group'], ['https://example.org/feed.xml', 'ForeignTwin']]) {
    const response = await get(`/make/read?${new URLSearchParams({ text: sentence })}`);
    check(`sentence reads as ${want}`, response.status === 200 && acts(body(response))[0] === want, sentence.slice(0, 34));
  }
  check('creation is proposed, never performed', body(await get('/make/read?text=hello')).includes('nothing exists until you choose'));

  const stamp = Date.now().toString(36);
  if (config.apply && config.token) {
    const create = async (type, text, token = config.token) => get(`/make/create?${new URLSearchParams({ type, text })}`, token, true);
    check('stranger cannot create', body(await create('Work', 'help translate', '')).includes('write-scoped account'));
    const made = {};
    for (const type of ['Work', 'Group', 'Note']) {
      const response = await create(type, `${type.toLowerCase()} circuit ${stamp}`), html = body(response);
      made[type] = html.match(/href="([^"]*)">Go to it/)?.[1];
      check(`${type} created through owning organ`, response.status === 200 && html.includes(`${type} created`), made[type] || html.slice(0, 60));
    }
    check('created object returns an address', Object.values(made).every(Boolean));
    if (made.Note) check('created note is immediately addressable', (await get(made.Note)).status === 200);
    const unknown = body(await create('Nonsense', `whatever ${stamp}`));
    check('unmakeable type creates nothing', unknown.includes('could not be made') && unknown.includes('Nothing was created'));
  } else skip('owned object creation circuit', 'requires --apply and BONFYRE_CIRCUIT_TOKEN');

  const frontier = await get('/surfaces'), frontierHtml = body(frontier);
  check('SurfaceIR frontier renders', frontier.status === 200 && frontierHtml.includes('SurfaceIR'));
  const facts = Object.fromEntries([...frontierHtml.matchAll(/<dt>([^<]*)<\/dt><dd>([^<]*)<\/dd>/g)].map(match => [match[1], match[2]]));
  check('frontier reports thresholds', 'Candidate at' in facts && 'Proof needs' in facts);
  const shapes = [...frontierHtml.matchAll(/li-weight">(\d+)&times;<\/span><span class="li-name">([^<]*)<br>.*?pill [a-z]*">([a-z]+)<\/span><br>(\d+) viewer/g)];
  check('traces recorded', shapes.length > 0, `${shapes.length} shapes`);
  check('closure is part of shape', new Set(shapes.map(row => row[2].split('&middot;').at(-1).trim())).size > 1);
  const candidateAt = Number(facts['Candidate at']?.split(' ')[0] || 5), proofNeeds = Number(facts['Proof needs']?.split(' ')[0] || 3);
  check('proof requires repeats and distinct viewers', shapes.every(row => (row[3] === 'proven') === (Number(row[1]) >= candidateAt && Number(row[4]) >= proofNeeds)));
  check('single-viewer shape is not proven', shapes.every(row => Number(row[4]) >= proofNeeds || row[3] !== 'proven'));
  check('cooling is deliberate', frontierHtml.includes('cooling one into a fixed page is a deliberate act'));

  const cards = [], kinds = [];
  for (let i = 0; i < 8; i++) {
    const response = await get(`/flow/at?i=${i}`);
    if (response.status !== 200) break;
    const html = body(response);
    cards.push([html, html.match(/class="fl-title[^"]*">([^<]*)</)?.[1] || '']);
    kinds.push(html.match(/class="fl-kind">([^<]*)</)?.[1] || '?');
  }
  check('traversal answers at every position', kinds.length === 8, `${kinds.length}`);
  check('consecutive positions have different object kinds', kinds.every((kind, i) => i === 0 || kind !== kinds[i - 1]));
  check('object grammar changes', new Set(kinds).size >= 3);
  check('cards name real objects', cards.every(([, name]) => name));
  check('cards open shown objects', cards.every(([html]) => /class="button primary" href="\/\S/.test(html)));
  const at3a = body(await get('/flow/at?i=3')), at3b = body(await get('/flow/at?i=3'));
  check('position reopens same object', at3a.match(/fl-title[^"]*">([^<]*)</)?.[1] === at3b.match(/fl-title[^"]*">([^<]*)</)?.[1]);
  const flow = await get('/flow');
  check('shell holds position', flow.status === 200 && body(flow).includes('hx-get="/flow/at?i=0"') && !body(flow).includes('<button class="fl-next"'));
  check('every card offers one onward route', cards.every(([html]) => html.split('data-flow-next').length - 1 === 1));

  const capabilityRoom = await get('/room/capability/parcel-underwrite');
  check('capability room shaped by capability', capabilityRoom.status === 200 && body(capabilityRoom).includes('What it costs, in every grammar') && body(capabilityRoom).includes('Whether it can settle'));
  check('settlement is fact not promise', body(capabilityRoom).includes('facilitator'));
  const noteRoute = body(await get('/notes/')).match(/href="\/notes\/([a-z0-9-]+)"/)?.[1];
  if (noteRoute) { const note = await get(`/room/note/${noteRoute}`); check('note room separates publication and readability', note.status === 200 && body(note).includes('Who can read it') && body(note).includes('Publishing and readability are separate')); }
  check('unbuilt kind says unbuilt', body(await get('/room/wombat/nothing')).includes('built rather than templated'));
  const combined = body(await get(`/make/read?${new URLSearchParams({ text: "We're starting a neighborhood clean-up and need help this Saturday" })}`));
  const order = acts(combined);
  check('combination offered above parts', order.length > 0 && order[0].includes('+'));
  check('parts remain available', new Set(order).has('Group') && new Set(order).has('Work'));
  if (config.apply && config.token) {
    const both = body(await get(`/make/create?${new URLSearchParams({ type: 'Group+Work', text: `circuit cleanup ${stamp}` })}`, config.token, true));
    const results = [...both.matchAll(/<span class="ap-do">(\w+) (created|not created)<\/span>/g)].map(match => [match[1], match[2]]);
    check('each part created by its organ', JSON.stringify(results.filter(([, state]) => state === 'created').map(([type]) => type)) === JSON.stringify(['Group', 'Work']));
    check('each part links to owner', (both.match(/class="ap-act is-strongest" href="\//g) || []).length === 2);
    const partial = body(await get(`/make/create?${new URLSearchParams({ type: 'ForeignTwin+Offer', text: `selling circuit bike ${stamp}` })}`, config.token, true));
    const parts = Object.fromEntries([...partial.matchAll(/<span class="ap-do">(\w+) (created|not created)<\/span>/g)].map(match => [match[1], match[2]]));
    check('partial failure named', parts.ForeignTwin === 'not created' && parts.Offer === 'created');
    check('successful part not rolled back', partial.includes('left nothing behind, and can be made on their own'));
  } else skip('combined owned creation circuit', 'requires --apply and BONFYRE_CIRCUIT_TOKEN');

  const desk = await get('/studio/'), deskHtml = body(desk);
  check('desk renders', desk.status === 200 && deskHtml.includes('waiting on a decision'));
  const queues = [...deskHtml.matchAll(/<h3>([^<]*)<\/h3><span class="desk-n">(\d+)<\/span>/g)].map(match => [match[1], Number(match[2])]);
  check('desk holds decision queues', queues.length >= 4);
  check('desk not wall of controls', (deskHtml.match(/<h2>/g) || []).length <= 1);
  const anonRows = (deskHtml.match(/class="desk-row"/g) || []).length;
  check('stranger sees counts not unpublished rows', anonRows === 0 && queues.some(([, count]) => count > 0));
  check('withheld rows explained', deskHtml.includes('nobody has published yet'));
  const clear = [...deskHtml.matchAll(/<section class="desk-q is-clear">([\s\S]*?)<\/section>/g)].map(match => match[1]);
  check('empty queues explain what fills them', clear.every(section => section.includes('Nothing open.')));
  check('queue with rows not marked clear', clear.every(section => !section.includes('class="desk-row"')));
  check('viewer without write scope told why', deskHtml.includes('cannot close any of it') || deskHtml.includes('not close it'));
  check('unsorted panel keeps own address', deskHtml.includes('/studio/controls') && (await get('/studio/controls')).status === 200);
  check('open line has a route', matches(deskHtml, /class="desk-row" href="([^"]*)"/g).every(href => href.startsWith('/')));
  if (config.token) {
    const writer = body(await get('/studio/', config.token));
    const shown = (writer.match(/class="desk-row"/g) || []).length;
    const withheld = matches(writer, /desk-more">(\d+) more not shown/g).reduce((sum, value) => sum + Number(value), 0);
    const total = matches(writer, /<h3>[^<]*<\/h3><span class="desk-n">(\d+)<\/span>/g).reduce((sum, value) => sum + Number(value), 0);
    check('writer sees unpublished rows', shown > 0, `${shown}`);
    check('shown plus withheld equals counted', shown + withheld === total, `${shown}+${withheld} vs ${total}`);
    const anonActs = new Set(matches(deskHtml, /class="desk-a">([^<]*) &rarr;/g));
    const writerActs = new Set(matches(writer, /class="desk-a">([^<]*) &rarr;/g));
    check('act depends on viewer', [...anonActs].some(value => !writerActs.has(value)) || [...writerActs].some(value => !anonActs.has(value)));
  } else skip('writer desk reconciliation', 'set BONFYRE_CIRCUIT_TOKEN');
  return finish();
}

if (import.meta.main) {
  try { process.exitCode = await run(options()); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
