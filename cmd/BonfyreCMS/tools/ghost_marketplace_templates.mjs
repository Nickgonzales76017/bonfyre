#!/usr/bin/env bun
// Marketplace identity compiles to reference candidates, never selected profiles.
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const rules = JSON.parse(readFileSync(resolve(root, 'data/ghost-marketplace-rules.json'), 'utf8'));
const sha = value => createHash('sha256').update(value).digest('hex');
export const acceptance = 'responsive at chip/card/section/page/workspace extents; keyboard reachable; valid semantic HTML; alt/fallback policy for media; readable empty/refused/failed states; navigation remains linked; typography, hierarchy, balance, contrast and negative space remain coherent; template changes presentation only; Authority still gates every effect at its owner';
export const cannotInfer = 'a Ghost marketplace reference -> Ghost code, paid assets, Handlebars, Ghost ontology or a Ghost backend is installed; visual resemblance -> identical source implementation; a presentation template -> a semantic owner, a subject, a pack, an institution, authority, publication state or permission to act';
export const fingerprintCannotInfer = 'a DesignFingerprint -> copied Ghost theme code, CSS, JavaScript, paid asset, Ghost ontology or an installed Ghost backend; a visual vector -> a semantic claim, subject identity, source admission, audience entitlement, authority or permission to act';
const dressRoles = ['ground','ink','display','accent','muted','rule','panel','face_display','face_body','face_mono','display_size','display_weight','display_transform','display_tracking','display_leading','radius','card_border','card_shadow','gap','measure'];
const vectorKeys = [...dressRoles.slice(0, 20), 'content_max','grid_min','media_ratio','section_space','headline_scale','body_leading','accent_policy','variation_seed'];
const choose = (values, seed) => values[seed % values.length];
export const slugify = text => text.normalize('NFKD').replace(/[^\x00-\x7F]/g, '').replace(/[^a-zA-Z0-9]+/g, '-').replace(/^-|-$/g, '').toLowerCase() || 'unnamed';
const primaryCategory = categories => ['Magazine','News','Newsletter','Podcast','Photography','Portfolio','Docs','Blog'].find(x => categories.includes(x)) || 'Blog';
function visualTokens(name, category, profile, seed, cardMode) {
  const base = rules.visual_bases[profile.family], fonts = rules.fontsets[choose(rules.category_fontsets[category], seed[9])];
  const digest = sha(name), unit = Number.parseInt(digest.slice(0, 8), 16) / 0xFFFFFFFF;
  const hue = (base.hue + (unit - .5) * 24 + 360) % 360;
  const accent = `hsl(${hue.toFixed(4)}deg ${base.sat}% ${base.light}%)`;
  const rule = profile.family === 'audio' ? 'rgba(255,255,255,.15)' : 'rgba(16,24,20,.14)';
  return { ground:base.ground, ink:base.ink, display:base.display, accent, muted:base.muted, rule, panel:base.panel,
    face_display:fonts.display, face_body:fonts.body, face_mono:fonts.mono,
    display_size:`clamp(2.2rem,${(4.8+(seed[6]%9)*.35).toFixed(2)}vw,${(4.2+(seed[7]%8)*.35).toFixed(2)}rem)`,
    display_weight:String([400,500,600,700][seed[8]%4]), display_transform:profile.family==='newsroom'&&seed[5]%3===0?'uppercase':'none',
    display_tracking:choose(['-.065em','-.045em','-.025em','-.01em'],seed[4]), display_leading:choose(['.88','.94','1','1.06'],seed[3]),
    radius:choose(rules.radii,seed[3]), card_border:cardMode==='borderless'?'0':'1px solid var(--w-rule)',
    card_shadow:['borderless','ruled'].includes(cardMode)?'none':'0 12px 34px rgba(0,0,0,.09)',
    gap:choose(rules.gaps,seed[2]), measure:choose(['48ch','56ch','64ch','72ch'],seed[1]), font_href:fonts.href,
    content_max:choose(rules.widths,seed[0]), grid_min:choose(rules.grid_mins,seed[1]), media_ratio:choose(rules.ratios,seed[4]), section_space:choose(rules.section_spaces,seed[5]),
    headline_scale:Math.round((1+(seed[6]%8)*.05)*100)/100, body_leading:Math.round((1.42+(seed[7]%6)*.05)*100)/100,
    accent_policy:choose(rules.accents,seed[8]), variation_seed:digest.slice(0,16) };
}
export function compileRow(name, categoryText) {
  const categories = categoryText.split(',').map(x=>x.trim()).filter(Boolean), category = primaryCategory(categories), profile = rules.profiles[category];
  const seed = [...createHash('sha256').update(name).digest().subarray(0,12)], slug=slugify(name);
  const density=choose(profile.densities,seed[9]), hero=choose(profile.heroes,seed[10]), nav=choose(profile.navs,seed[11]), media=choose(profile.media,seed[0]+seed[5]);
  const typography=choose(profile.types,seed[1]+seed[6]), card=choose(profile.cards,seed[2]+seed[7]), chrome=choose(profile.chrome,seed[3]+seed[8]);
  return { template:`ghost-${slug}`, reference_source:'ghost-marketplace@2026-09-06', reference_name:name,
    reference_url:`https://ghost.org/themes/${slug}/`, reference_author:'', reference_categories:categories.join(','), reference_price:'', family:profile.family,
    renderer_mix:profile.renderer_mix, density, hero_mode:hero, navigation_mode:nav, media_mode:media, typography_mode:typography, card_mode:card, chrome_mode:chrome,
    territory_affinity:profile.territory, semantic_roles:{accepts:profile.roles,required:['title'],optional:profile.roles.filter(x=>x!=='title')},
    layout_tokens:visualTokens(name,category,profile,seed,card), acceptance_contract:acceptance, cannot_infer:cannotInfer, status:'reference_candidate' };
}
export function fingerprintRow(template) {
  const vector=Object.fromEntries(vectorKeys.map(key=>[key,template.layout_tokens[key]]));
  return { fingerprint_id:`ghost:${template.template}`, template:template.template, reference_source:template.reference_source, reference_name:template.reference_name,
    family:template.family, renderer_affinity:template.renderer_mix, density:template.density, visual_vector:vector,
    acceptance_contract:template.acceptance_contract, cannot_infer:fingerprintCannotInfer, status:'reference_candidate' };
}
export function readCensus(path) {
  const lines=readFileSync(path,'utf8').trimEnd().split(/\r?\n/), header=lines.shift().split('\t');
  const nameIndex=header.indexOf('name'), catIndex=header.indexOf('categories');
  if(nameIndex<0||catIndex<0) throw Error('census needs name and categories columns');
  const rows=lines.filter(Boolean).map(line=>{const fields=line.split('\t');return [fields[nameIndex]?.trim()||'',fields[catIndex]?.trim()||'']});
  if(!rows.length) throw Error(`no theme rows in ${path}`);
  if(new Set(rows.map(x=>x[0])).size!==rows.length) throw Error('duplicate marketplace theme names in census');
  return rows;
}
export function compileCensus(path, expectCount=238) {
  const census=readCensus(path);
  if(expectCount && census.length!==expectCount) throw Error(`expected ${expectCount} themes, found ${census.length}`);
  const docs=census.map(([name,cats])=>compileRow(name,cats)), byFingerprint=new Map();
  for(const doc of docs){
    const tok=doc.layout_tokens, missing=dressRoles.filter(x=>!Object.hasOwn(tok,x));
    if(missing.length) throw Error(`${doc.template} missing dresser roles: ${missing.join(',')}`);
    if(!tok.font_href?.startsWith('https://fonts.googleapis.com/')) throw Error(`${doc.template} has no loadable font declaration`);
    const key=JSON.stringify([tok.ground,tok.accent,tok.face_display]);
    if(byFingerprint.has(key)) throw Error(`duplicate visual fingerprint: ${doc.template} and ${byFingerprint.get(key)}`);
    byFingerprint.set(key,doc.template);
  }
  const fingerprints=docs.map(fingerprintRow);
  if(new Set(fingerprints.map(x=>x.fingerprint_id)).size!==fingerprints.length) throw Error('duplicate DesignFingerprint identity');
  if(new Set(fingerprints.map(x=>JSON.stringify(x.visual_vector))).size!==fingerprints.length) throw Error('duplicate closed DesignFingerprint visual vector');
  return {docs,fingerprints};
}
if(import.meta.main){
  const args={census:resolve(root,'data/ghost-marketplace-2026-09-06.tsv'),out:'','fingerprints-out':'',expect:'238',observe:[], 'observations-out':'',foreign:'',project:resolve(root,'../../components/BonfyreProject/bonfyre-project')};
  for(let i=2;i<process.argv.length;i++){
    const key=process.argv[i].slice(2);
    if(!process.argv[i].startsWith('--')||!Object.hasOwn(args,key)) throw Error(`unknown option ${process.argv[i]}`);
    if(key==='observe') args.observe.push(process.argv[++i]); else args[key]=process.argv[++i];
  }
  if((args.observe.length||args['observations-out'])&&(!args['observations-out']||!args.foreign)) throw Error('public-page observation requires --observe LABEL=HTTPS_URL, --observations-out and --foreign');
  const {docs,fingerprints}=compileCensus(args.census,Number(args.expect));
  const write=(path,rows)=>{const content=rows.map(x=>JSON.stringify(x)).join('\n')+'\n';if(path){mkdirSync(dirname(path),{recursive:true});writeFileSync(path,content)}else process.stdout.write(content)};
  write(args.out,docs);
  if(args['fingerprints-out'])write(args['fingerprints-out'],fingerprints);
  if(args['observations-out']){
    const {observeGhostPage}=await import('./ghost_public_anatomy.mjs');
    const observations=[];
    for(const item of args.observe){
      const boundary=item.indexOf('=');
      if(boundary<1||boundary===item.length-1) throw Error('--observe requires LABEL=HTTPS_URL');
      observations.push(await observeGhostPage(item.slice(0,boundary),item.slice(boundary+1),{project:args.project,foreignRoot:args.foreign}));
    }
    if(!observations.length)throw Error('--observations-out requires at least one --observe');
    write(args['observations-out'],observations);
  }
}
