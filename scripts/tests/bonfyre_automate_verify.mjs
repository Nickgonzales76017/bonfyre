import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';

const [tmp, firstExit, secondExit] = process.argv.slice(2);
function publicationSourceDeltas(base) {
  const counts = new Map();
  for (const file of readdirSync(join(base,'deltas')).filter(x=>x.endsWith('.json'))) {
    const delta = JSON.parse(readFileSync(join(base,'deltas',file),'utf8'));
    if (delta.schema !== 'bonfyre-fact-delta.v1' || !Array.isArray(delta.writes))
      throw Error('unrecognized native CMS FactDelta');
    for (const row of delta.writes) if (typeof row === 'string' && row.startsWith('cms:publication_source:')) {
      const source = row.slice('cms:publication_source:'.length);
      counts.set(source,(counts.get(source)||0)+1);
    }
  }
  return Object.fromEntries([...counts].sort(([a],[b])=>a.localeCompare(b)));
}
// This separately exercises the retryable state transition after the first
// two clean materialization ticks, using the same real CMS batch owner.
if (process.argv.at(-1) === '--retries') {
  const [stateRoot, heldExitA, heldExitB, recoveredExit, replayedExit] = process.argv.slice(2);
  const entries = readdirSync(join(stateRoot,'state','runs')).filter(x=>x.endsWith('.json')).sort();
  const last = entries.slice(-4).map(x=>JSON.parse(readFileSync(join(stateRoot,'state','runs',x),'utf8')));
  const material = last.map(r=>r.steps.find(s=>s.step==='publication-material'));
  const fail = message => { console.error('retained generation retry: FAIL: '+message); process.exit(1); };
  if(entries.length!==6 || material.some(s=>!s?.ok) ||
      [heldExitA,heldExitB,recoveredExit,replayedExit].some(n=>Number(n)!==0))
    fail('each real owner tick must finish and retain its own receipt');
  for(const [index,item] of material.slice(0,2).entries()){
    if(item.outcome?.status!=='partial_with_explicit_source_holds' ||
        item.outcome?.sources_held!==1 || item.outcome?.owner_operations!==1 ||
        item.outcome?.new_publication_occurrences!==0)
      fail('refused source '+index+' was cached or misreported instead of retried');
  }
  if(material[2].outcome?.status!=='complete' || material[2].outcome?.owner_operations!==1 ||
      material[2].outcome?.new_publication_occurrences!==1 || material[2].outcome?.sources_held!==0)
    fail('new retained grant-notice source was not admitted exactly once');
  if(material[3].outcome?.owner_operations!==0 ||
      material[3].outcome?.skipped!=='source_receipts_and_native_CMS_generation_unchanged' ||
      material[3].outcome?.new_publication_occurrences!==0)
    fail('completed generation did not skip the fourth unchanged source cut');
  const db = new Database(join(stateRoot,'cms.db'),{readonly:true});
  let bySource;
  try {bySource=db.query('SELECT source_ref,COUNT(*) AS n FROM publication_occurrence GROUP BY source_ref ORDER BY source_ref').all();}
  finally{db.close();}
  if(bySource.length!==5 || bySource.some(x=>x.n!==1) ||
      !bySource.some(x=>x.source_ref==='civic-bulletin') ||
      !bySource.some(x=>x.source_ref==='newsroom-twin') ||
      !bySource.some(x=>x.source_ref==='podcast-twin') ||
      !bySource.some(x=>x.source_ref==='podcasting-2-0') ||
      !bySource.some(x=>x.source_ref==='buzzcast'))
    fail('native CMS did not retain exactly one occurrence per published RSS source');
  const nativeSourceDeltas = publicationSourceDeltas(stateRoot);
  if(Object.keys(nativeSourceDeltas).length!==5 ||
      bySource.some(x=>nativeSourceDeltas[x.source_ref]!==1))
    fail('incremental native CMS incorrectly invalidated unchanged publication sources');
  console.log(JSON.stringify({schema:'bonfyre.retained-source.retry-evidence.v1',
    source_families:3,source_contracts:5,partial_ticks:2,partial_native_owner_invocations:2,
    recovered_native_rows:1,final_noop_owner_invocations:0,source_rows:bySource,
    native_fact_deltas:nativeSourceDeltas,prevented_noop_delta_fanout:true,
    network_fetches:material.reduce((n,s)=>n+(s.outcome.network_fetches||0),0)}));
  process.exit(0);
}
if (process.argv.at(-1) === '--sink-reconnect') {
  const [base, offlineExit, dormantExit, reconnectExit, replayExit] = process.argv.slice(2);
  const names = readdirSync(join(base,'state','runs')).filter(x=>x.endsWith('.json')).sort();
  const recent = names.slice(-4).map(x=>JSON.parse(readFileSync(join(base,'state','runs',x),'utf8'))
    .steps.find(s=>s.step==='publication-material'));
  const fail = detail => { console.error('CMS sink transition: FAIL: '+detail); process.exit(1); };
  if(names.length!==10 || recent.some(x=>!x?.ok) ||
      [offlineExit,dormantExit,reconnectExit,replayExit].some(x=>Number(x)!==0))
    fail('all four source materialization ticks must retain successful receipts');
  const [offline,dormant,reconnected,quiet] = recent.map(x=>x.outcome);
  if(offline?.status!=='material_admitted_delta_delivery_pending' || offline.owner_operations!==1 ||
      offline.new_publication_occurrences!==1 || offline.pending_source_deltas!==1 || offline.delta_delivery_configured!==false)
    fail('material admitted without a sink did not leave an explicit durable outbox obligation');
  if(dormant?.owner_operations!==0 || dormant.pending_source_deltas!==1 ||
      dormant.skipped!=='source_receipts_and_native_CMS_generation_unchanged')
    fail('disconnected stable source reran instead of reusing its scoped generation');
  if(reconnected?.status!=='complete' || reconnected.owner_operations!==1 ||
      reconnected.new_publication_occurrences!==0 || reconnected.pending_source_deltas!==0 ||
      reconnected.delta_delivery_configured!==true)
    fail('enabling FactDelta sink did not flush the durable source revision: '+JSON.stringify({offline,dormant,reconnected,quiet}));
  if(quiet?.owner_operations!==0 || quiet.pending_source_deltas!==0 ||
      quiet.skipped!=='source_receipts_and_native_CMS_generation_unchanged')
    fail('delivered source generated duplicate work on the next stable tick');
  const db=new Database(join(base,'cms.db'),{readonly:true});
  let materialCount,pendingCount;
  try {materialCount=db.query("SELECT count(*) AS n FROM publication_occurrence WHERE source_ref='community-notices'").get().n;
    pendingCount=db.query("SELECT count(*) AS n FROM _publication_delta_outbox WHERE namespace='root'").get().n;}
  finally{db.close();}
  const deltas=publicationSourceDeltas(base);
  if(materialCount!==1 || pendingCount!==0 || Object.keys(deltas).length!==6 ||
      Object.values(deltas).some(n=>n!==1) || deltas['community-notices']!==1)
    fail('source publication or exact downstream fact deltas failed readback');
  console.log(JSON.stringify({schema:'bonfyre.material.sink-transition.v1',
    disconnected_ingests:1,held_outbox_revisions:1,disconnected_noop_owner_invocations:0,
    reconnected_delta_replays:1,reconnected_new_material:0,stable_owner_invocations:0,
    canonical_community_records:materialCount,outbox_pending_after_delivery:pendingCount,
    source_scoped_deltas:deltas,network_fetches:recent.reduce((n,x)=>n+(x.outcome.network_fetches||0),0)}));
  process.exit(0);
}
const problems = [];
const check = (ok, why) => { if (!ok) problems.push(why); };
const runs = readdirSync(join(tmp, 'state', 'runs')).filter(x => x.endsWith('.json')).sort();
check(runs.length >= 2, `two runs are recorded separately (${runs.length})`);
const records = runs.map(x => JSON.parse(readFileSync(join(tmp, 'state', 'runs', x), 'utf8')));
const one = Object.fromEntries((records[0]?.steps || []).map(s => [s.step, s]));
const firstMaterial = one['publication-material'];
check(one['civic-index']?.ok === true && one['civic-index']?.outcome?.owner_operations === 0,
  'scheduler does not crawl civic providers without a declared published civic source');
check(firstMaterial?.ok === true, 'native cross-domain retained-source materialization completes');
check(firstMaterial?.outcome?.owner_operations === 1 && firstMaterial.outcome?.network_fetches === 0,
  'news and podcast use one already-retained native compiler pass, not network recrawls');
check(firstMaterial?.outcome?.new_publication_occurrences >= 2,
  'different provider families become actual native PublicationOccurrence rows');
check(Number(firstExit) === 1, `failed step exits 1 (${firstExit})`);
check(!one.credentials?.ok, 'readable key fails credentials');
for (const name of ['census-items', 'archive', 'atlas', 'link', 'acoustic', 'contributions', 'frontier-state'])
  check(one[name]?.ok, `${name} continues despite credential failure`);
check(one.archive?.outcome?.kept >= 1, 'items are archived');
const near = one.neighbourhood?.outcome || {};
check(Object.hasOwn(near, 'placed'), 'placed things are counted');
if (near.placed >= 2) check(['vgpu-node', 'vgpu-mock', 'cpu'].includes(near.backend), 'neighbourhood backend is recorded');
check(one.contributions?.outcome?.receipts?.unknown_sources === 0, 'contributions land on source channels');
check((one['frontier-state']?.outcome?.report?.channels?.admitted || 0) + (one['frontier-state']?.outcome?.report?.channels?.active || 0) >= 1, 'census admits channels');
check(one['census-items']?.outcome?.items >= 3, 'census items are read');
check(one.link?.outcome?.relations?.mentions >= 1, 'linker writes Fort Worth mention');
check(!['sources', 'needs', 'inventories', 'channels', 'catalogues'].some(n => Object.hasOwn(one, n)), 'weekly steps wait');
const two = Object.fromEntries((records.at(-1)?.steps || []).map(s => [s.step, s]));
check(two['publication-material']?.ok === true && two['publication-material']?.outcome?.new_publication_occurrences === 0,
  'second material tick admits no duplicate publication after receipt replay');
check(two['publication-material']?.outcome?.owner_operations === 0 &&
  two['publication-material']?.outcome?.skipped === 'source_receipts_and_native_CMS_generation_unchanged',
  'unchanged news and podcast sources skip the entire native parse/index generation: '+JSON.stringify({first:firstMaterial?.outcome,second:two['publication-material']?.outcome}));
const initialDeltas = publicationSourceDeltas(tmp);
check(Object.keys(initialDeltas).length===4 &&
  ['newsroom-twin','podcast-twin','podcasting-2-0','buzzcast'].every(s=>initialDeltas[s]===1),
  'first native batch wrote exactly one source dependency delta per changed feed and none on replay');
check(Number(secondExit) === 0, `clean run exits 0 (${secondExit})`);
check(two.credentials?.outcome?.bootstrap?.count === 1 && two.credentials?.outcome?.bootstrap?.shown === false, 'secret is generated but not shown');
check(two.atlas?.outcome?.projected === 0, 'unchanged words are not re-encoded');
const key = join(tmp, 'private.key');
check(existsSync(key) && (statSync(key).mode & 0o077) === 0, 'key stays private');
check(!/[0-9a-f]{32,}/.test(readFileSync(join(tmp, 'state', 'runs', runs.at(-1)), 'utf8')), 'run record has no generated secret');
if (problems.length) {
  console.error('automation runner: FAIL\n' + problems.map(x => `  - ${x}`).join('\n'));
  console.error(JSON.stringify(records.flatMap(r => r.steps.filter(s => !s.ok))).slice(0, 1500));
  process.exit(1);
}
console.log('automation runner: PASS');
