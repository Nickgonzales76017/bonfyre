// Public profile release proof from the canonical, read-only CMS state.
// This is a release canary, not a content importer or a publication owner.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {Database} from 'bun:sqlite';
const routeOk=route=>typeof route==='string'&&/^\/blog\/[a-z0-9-]{1,96}\/$/.test(route);
const hostOk=host=>typeof host==='string'&&host.length<=253&&/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+aurekai\.(?:work|live|life|shop)$/.test(host);
export function selectMemberJournalCanaries(dbPath){
 // A WAL store has no shared-memory file while no connection holds it (as while
 // its service restarts), and a read-only open cannot create one.
 let db=null;
 for(let i=0;i<4&&!db;i++){
  try{const d=new Database(dbPath,{readonly:true,create:false});d.query('SELECT 1').get();db=d;}
  catch{if(i<3)Bun.sleepSync(500);}
 }
 // No other connection holds the WAL index: open without create so SQLite may
 // make it; this canary still only runs SELECTs.
 if(!db)db=new Database(dbPath,{readwrite:true,create:false});
 try{
  const selected=db.query(`SELECT s.site_id,s.host,count(*) AS total
   FROM site_presentation s JOIN site_page p ON p.namespace=s.site_id
   WHERE s.namespace='root' AND s.status='published' AND s.profile='member-profile-v1'
   AND p.status='published' AND p.route LIKE '/blog/%'
   GROUP BY s.site_id,s.host ORDER BY total DESC,s.host LIMIT 65`).all();
  // Every published member journal is part of the release. A highest-count
  // canary alone could hide a second site's broken or missing article routes.
  assert(selected.length<=64,'Member journal release has more than 64 sites; verify in bounded cohorts.');
  const canaries=[];let totalRoutes=0;
  for(const member of selected){
  assert(hostOk(member.host),'Published member journal host is invalid.');
  const rows=db.query(`SELECT route FROM site_page
   WHERE namespace=?1 AND status='published' AND route LIKE '/blog/%'
   ORDER BY route`).all(member.site_id);
  assert(rows.length===member.total&&rows.length<=2048,'Journal publication census changed or exceeded its bounded readback.');
  const routes=rows.map(x=>x.route);assert(routes.every(routeOk),'Journal publication has an invalid semantic route.');
  // Publication metadata alone cannot satisfy an article release. The same
  // CMS snapshot must contain a typed, public authored body for every route.
  // Missing tables, invalid contracts and metadata-only pages refuse the cut.
  const material=db.query(`SELECT p.route, EXISTS(
    SELECT 1 FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route
    AND f.slot='showcase' AND f.audience='public' AND f.status='published'
    AND (f.kind LIKE 'profile-%' OR f.kind LIKE 'ui:%' OR f.kind LIKE 'service:%')
    AND CASE WHEN json_valid(f.projection_contract)
      THEN COALESCE(json_extract(f.projection_contract,'$.html'),0)=1 ELSE 0 END
    AND CASE WHEN json_valid(f.semantic_tags)
      THEN COALESCE(json_extract(f.semantic_tags,'$.authoring_v1.version'),0)=1 ELSE 0 END
    AND length(trim(COALESCE(f.body,'')))>0
  ) AS has_body FROM site_page p
  WHERE p.namespace=?1 AND p.status='published' AND p.route LIKE '/blog/%'
  ORDER BY p.route`).all(member.site_id);
  assert.equal(material.length,routes.length,'Member journal body census changed.');
  const missing=material.filter((x,i)=>x.route!==routes[i]||!x.has_body);
  assert.equal(missing.length,0,'Member journal has published pages without released authored bodies: '+missing.slice(0,3).map(x=>x.route).join(', '));
  totalRoutes+=routes.length;
  assert(totalRoutes<=8192,'Member journal release exceeds the bounded article readback.');
  const route_sha256=createHash('sha256').update([member.site_id,member.host,...routes].join('\\n')).digest('hex');
  canaries.push({site_id:member.site_id,host:member.host,total:routes.length,route_sha256,routes});
  }
  return canaries;
 }finally{db.close();}
}
// Retain the bounded legacy one-site reader for consumers of prior releases.
export function selectMemberJournalCanary(dbPath){return selectMemberJournalCanaries(dbPath)[0]??null;}
export async function verifyMemberJournalReadback(canary,read,{publicRead=false}={}){
 if(!canary)return null;
 const base=publicRead?'https://'+canary.host:null;
 const readHost=(route)=>read(base,route,publicRead?undefined:canary.host);
 const index=await readHost('/blog/');
 assert.equal(index.status,200,canary.host+' public journal index');
 assert(index.text.includes('class="fd ak-journal-list"'),canary.host+' must compose the native publication index');
 const profile=await readHost('/?tab=posts');
 assert.equal(profile.status,200,canary.host+' public profile posts');
 assert(profile.text.includes('data-profile-journal'),canary.host+' public profile posts must show the composed journal');
 const routes=canary.routes; // Every article is verified both locally and on its public host.
 const results=[];
 // Avoid serial cold-start amplification while respecting the native six-worker service.
 for(let i=0;i<routes.length;i+=6){
  const batch=await Promise.all(routes.slice(i,i+6).map(async route=>{
   const result=await readHost(route);
   assert.equal(result.status,200,canary.host+route+' published article');
   assert(result.text.includes('<link rel="canonical" href="https://'+canary.host+route+'"'),
     canary.host+route+' must keep its exact publication identity');
   assert(result.text.includes('class="ak-zen-body ak-zen-text"'),canary.host+route+' must have an authored reading surface');
    const articleBody='<div class="ak-zen-body ak-zen-text">';
    const start=result.text.indexOf(articleBody);
    const end=start<0?-1:result.text.indexOf('</div>',start+articleBody.length);
    const excerpt=end<0?'':result.text.slice(start+articleBody.length,end);
    const prose=excerpt.replace(/<[^>]*>/g,' ').replace(/&(?:#[0-9]+|[a-z]+);/gi,' ').replace(/\s+/g,' ').trim();
    assert(prose.length>=20,canary.host+route+' must render editorial prose, not only an empty journal shell');
   return {host:canary.host,route,status:200,sha256:result.sha256};
  }));
  results.push(...batch);
 }
 return {host:canary.host,site_id:canary.site_id,total:canary.total,route_sha256:canary.route_sha256,
   routes_verified:results.length,publicRead,index_sha256:index.sha256,profile_sha256:profile.sha256,
   articles:results};
}
