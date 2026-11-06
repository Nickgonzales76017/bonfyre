#!/usr/bin/env bun
// One-shot evidence reconciliation. The repository's Atlas (bonfyre-atlas, C)
// remains the sole parser, field-schema authority and validator. No fixed line
// numbers or fuzzy replacements.
import {readFileSync, writeFileSync, existsSync, mkdirSync, mkdtempSync, rmSync, copyFileSync, lstatSync, realpathSync} from 'node:fs';
import {join, dirname, resolve, relative} from 'node:path';
import {tmpdir, homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
const sha = value => createHash('sha256').update(value).digest('hex');
const fail = message => { throw Error(message); };
const collections = {architecture:'architectures',interaction:'interactions',view:'views',phenotype:'phenotypes',edge_class:'edge_classes'};

// The native Atlas: parse, schema, files and validate are its subcommands.
export function nativeAtlas(repo) {
  // This checkout's program, run against the repository being reconciled.
  const organ = join(import.meta.dir, '../cmd/BonfyreAtlas'), bin = process.env.BONFYRE_ATLAS_BIN || join(organ, 'bonfyre-atlas');
  if (!existsSync(bin)) { const m = spawnSync('make', ['-s', '-C', organ], { encoding: 'utf8' }); if (m.status !== 0) fail(`bonfyre-atlas is not built: ${m.stderr}`); }
  const call = (argv, env = {}) => spawnSync(bin, argv, { cwd: repo, encoding: 'utf8', maxBuffer: 64 << 20,
    env: { ...process.env, BONFYRE_ATLAS_DIR: join(repo, 'architecture'), ...env } });
  const json = argv => { const r = call(argv); if (r.status !== 0) fail(r.stderr || r.stdout); return JSON.parse(r.stdout); };
  return {
    fieldSchema() { const v = json(['schema']); return { ...v, lists: new Set(v.list_fields), scalars: new Set(v.scalar_fields), kinds: new Set(v.block_kinds) }; },
    yaffFiles() { const r = call(['files']); if (r.status !== 0) fail(r.stderr); return r.stdout.split('\n').filter(Boolean); },
    parseFile(file, _schema, undeclared = {}) {
      const v = json(['parse', file]);
      for (const [key, files] of Object.entries(v.undeclared)) (undeclared[key] ??= []).push(...files);
      return Object.values(v.files)[0];
    },
    // Validate a staged copy; witnesses and sources must open from the real tree.
    check(stageDir, realDir) {
      const r = call(['validate', '--strict-duplicates', '--opens-from', realDir], { BONFYRE_ATLAS_DIR: stageDir });
      const lines = r.stdout.split('\n');
      if (r.status === 0) { const m = /(\d+) architectures, \d+ views, (\d+) interactions/.exec(lines[0]) || []; return { errors: [], architectures: Number(m[1]), interactions: Number(m[2]) }; }
      return { errors: lines.slice(1).map(l => l.trim()).filter(Boolean) };
    },
    run(argv) { const r = call(argv); if (r.status !== 0) fail(`bonfyre-atlas ${argv[0]} failed: ${r.stderr || r.stdout}`); return r.stdout; },
  };
}

// Text ranges only: their meaning is always obtained from the native parser.
export function blockRanges(text, schema) {
  const rows = [...text.matchAll(/^([^\s#][^\r\n]*)(?:\r?\n|$)/gm)];
  const out = [];
  for (let i=0; i<rows.length; i++) {
    const split = /^(\S+)\s+(.+?)\s*$/.exec(rows[i][1]);
    if (!split || !schema.kinds.has(split[1])) continue;
    out.push({kind:split[1],id:split[2],start:rows[i].index,
      body:rows[i].index+rows[i][0].length,end:rows[i+1]?.index??text.length});
  }
  return out;
}
export function mergeEvidence(documents, parsed, desired, schema, replacements=[]) {
  const next = new Map(documents), locations = new Map(), edits=[];
  for (const [path,text] of documents) {
    const spans = blockRanges(text,schema), blocks=parsed.get(path)??[];
    if (spans.length!==blocks.length) fail(`Text/parser disagreement: ${path}`);
    for (let i=0;i<blocks.length;i++) {
      const b=blocks[i],s=spans[i],key=b.kind+' '+b.ident;
      if (b.kind!==s.kind||b.ident!==s.id) fail(`Text/parser identity disagreement: ${path}`);
      if (locations.has(key)) fail(`Duplicate declaration: ${key}`);
      locations.set(key,{path,block:b});
    }
  }
  const incoming=new Set();
  for (const b of desired) {
    const key=b.kind+' '+b.ident;
    if(incoming.has(key)) fail(`Duplicate evidence identity: ${key}`);
    incoming.add(key);
    const found=locations.get(key);
    if(!found) {
      if(b.kind!=='interaction') fail(`Missing existing declaration: ${key}`);
      for(const end of ['source','destination']) if(!locations.has('architecture '+b.scalars[end])) fail(`Missing ${end}: ${key}`);
      const path=locations.get('architecture '+b.scalars.source).path;
      const lines=[key,...Object.entries(b.scalars).map(([k,v])=>'  '+k+' '+v),
        ...Object.entries(b.lists).flatMap(([k,vs])=>vs.map(v=>'  '+k+' '+v))];
      next.set(path,next.get(path).replace(/\s*$/,'')+'\n\n'+lines.join('\n')+'\n');
      locations.set(key,{path,block:b});edits.push({identity:key,action:'add_interaction',path});continue;
    }
    for(const [name,value] of Object.entries(b.scalars)) {
      if(found.block.scalars[name]!==value) fail(`Conflicting ${name} in ${key}; current declaration preserved`);
    }
    const path=found.path,text=next.get(path),span=blockRanges(text,schema).find(s=>s.kind===b.kind&&s.id===b.ident);
    if(!span) fail(`Declaration moved during in-memory merge: ${key}`);
    const additions=[];
    for(const [name,values] of Object.entries(b.lists)) {
      if(!schema.lists.has(name)) fail(`Unrecognized list field: ${name}`);
      const have=new Set(found.block.lists[name]??[]);
      for(const value of values) if(!have.has(value)) {additions.push('  '+name+' '+value);have.add(value);}
    }
    if(additions.length) {
      const body=text.slice(span.start,span.end).replace(/\s*$/,'');
      next.set(path,text.slice(0,span.start)+body+'\n'+additions.join('\n')+'\n\n'+text.slice(span.end));
      edits.push({identity:key,action:'add_fields',count:additions.length,path});
    }
  }
  for(const r of replacements) {
    const key=r.kind+' '+r.id,found=locations.get(key);if(!found)fail(`Replacement identity missing: ${key}`);
    const path=found.path,text=next.get(path),span=blockRanges(text,schema).find(s=>s.kind===r.kind&&s.id===r.id);
    let body=text.slice(span.start,span.end);const lines=body.split(/\r?\n/);
    const hits=lines.map((s,i)=>s.trim()===r.from?i:-1).filter(i=>i>=0);
    if(hits.length>1)fail(`Ambiguous old evidence in ${key}`);
    if(!hits.length){edits.push({identity:key,action:'newer_or_already_reconciled_field_preserved'});continue;}
    if(lines.some(s=>s.trim()===r.to))lines.splice(hits[0],1);else lines[hits[0]]='  '+r.to;
    next.set(path,text.slice(0,span.start)+lines.join('\n')+text.slice(span.end));
    edits.push({identity:key,action:'replace_exact_superseded_field',path});
  }
  return {documents:next,edits};
}
export const replacements=[
 {kind:'architecture',id:'fleming-site',from:'known_gap the offer terms (closing costs paid, seven-day closing, zero fees) are unconfirmed and still on the page',to:'known_gap the revised CMS record removed the fixed closing-time, guaranteed-purchase and universal fee promises; the qualified no-out-of-pocket-cost positioning and final disclosures still need business confirmation'},
 {kind:'architecture',id:'cmd-web-address',from:'known_gap source-only, not built; realizes WebAddressCapability; deeper development toward the BonfyreFS level (WiringSpec consumes/produces, full contract) is ongoing',to:'known_gap native WebAddress builds and deployed consumer paths have separate witnesses; full WiringSpec consumes/produces completion remains separate'},
 {kind:'architecture',id:'cmd-source-frontier',from:'known_gap source-only, not built; deeper WiringSpec fact flow and authority admission are separate work',to:'known_gap native command and packaged collector are built and installed; general authority admission and additional upstream endpoints remain separate'},
 {kind:'architecture',id:'program-fabric',from:'known_gap ordinary resolution is bounded to 256 candidates per typed cut and explicit collection limits; it refuses incomplete projections rather than silently narrowing them; row-level nutrition calculation and bulk TikTok collection are not implemented by this resolver',to:'known_gap ordinary resolution is bounded to 256 candidates per typed cut and explicit collection limits; it refuses incomplete projections rather than silently narrowing them; nutrition formulation belongs to Recipe and bounded bulk video collection belongs to SourceFrontier; this resolver supplies their owner references rather than accepting or running effects itself'}
];
function run(argv,cwd) {
 const r=spawnSync(argv[0],argv.slice(1),{cwd,encoding:'utf8',timeout:120000,maxBuffer:4*1024*1024});
 if(r.error||r.status!==0) fail(`${argv.slice(0,3).join(' ')} failed: ${r.error?.message??r.stderr??r.stdout}`);
 return r.stdout;
}
export async function reconcile({repo,evidence,apply=false,output}) {
 repo=realpathSync(resolve(repo));const dir=join(repo,'architecture'),git=run(['git','rev-parse','--show-toplevel'],repo).trim();
 if(realpathSync(git)!==repo)fail('Use the repository root, not a subdirectory');
 const core=nativeAtlas(repo);
 const schema=core.fieldSchema(),files=core.yaffFiles(),docs=new Map(),parsed=new Map();
 for(const file of files) {
   if(!lstatSync(file).isFile()||lstatSync(file).isSymbolicLink())fail('Only regular Atlas files are updated');
   const name=relative(repo,file);docs.set(name,readFileSync(file,'utf8'));parsed.set(name,core.parseFile(file,schema));
 }
 const undeclared={},desired=core.parseFile(evidence,schema,undeclared);
 if(Object.keys(undeclared).length)fail('Unknown evidence fields: '+Object.keys(undeclared).join(','));
 const merged=mergeEvidence(docs,parsed,desired,schema,replacements);
 const stage=mkdtempSync(join(tmpdir(),'aurekai-atlas-check-'));
 const schemaBytes=readFileSync(join(dir,'atlas-fields.cue'));
 try {
   for(const [name,text] of merged.documents){mkdirSync(dirname(join(stage,name)),{recursive:true});writeFileSync(join(stage,name),text);}
   const atlas=core.check(join(stage,'architecture'),dir);
   if(atlas.errors.length)fail('Reconciled Atlas invalid:\n'+atlas.errors.join('\n'));
   const changed=[...docs.keys()].filter(p=>docs.get(p)!==merged.documents.get(p));
   const proof={architectures:atlas.architectures,interactions:atlas.interactions,
     changed_files:changed,edits:merged.edits,validation_errors:[],applied:false};
   const againParsed=new Map([...docs.keys()].map(p=>[p,core.parseFile(join(stage,p),schema)]));
   const again=mergeEvidence(merged.documents,againParsed,desired,schema,replacements);
   if([...again.documents].some(([p,t])=>t!==merged.documents.get(p)))fail('Reconciliation is not idempotent');
   proof.idempotent=true;
   if(!changed.length){proof.state='already_reconciled';if(apply){core.run(['validate']);core.run(['export']);}return proof;}
   output=resolve(output??join(dirname(evidence),'runs',new Date().toISOString().replace(/[:.]/g,'-')));mkdirSync(output,{recursive:true});
   let patch='';
   for(const p of changed){const d=spawnSync('/usr/bin/diff',['-u','--label','a/'+p,'--label','b/'+p,join(repo,p),join(stage,p)],{encoding:'utf8',maxBuffer:8*1024*1024});if(d.status!==1)fail('Diff generation failed');patch+=d.stdout;}
   const patchPath=join(output,'RECONCILED.patch');writeFileSync(patchPath,patch);proof.patch=patchPath;
   const checkSnapshot=()=>{
      const current=core.yaffFiles().map(p=>relative(repo,p));
      if(current.join('\n')!==[...docs.keys()].join('\n'))fail('Atlas file membership changed; rerun to reconcile');
      if(!readFileSync(join(dir,'atlas-fields.cue')).equals(schemaBytes))fail('Atlas schema changed; rerun');
      for(const [p,text] of docs)if(readFileSync(join(repo,p),'utf8')!==text)fail(`Concurrent edit to ${p}; no stale write attempted, rerun`);
   };
   checkSnapshot();run(['git','apply','--check',patchPath],repo);
   if(apply) {
     for(const p of changed){const dest=join(output,'before',p);mkdirSync(dirname(dest),{recursive:true});writeFileSync(dest,docs.get(p));}
     proof.before=Object.fromEntries(changed.map(p=>[p,sha(docs.get(p))]));
     writeFileSync(join(output,'RESULT.json'),JSON.stringify({...proof,state:'validated_not_yet_applied'},null,2));
     checkSnapshot();run(['git','apply','--whitespace=error-all',patchPath],repo);
     proof.applied=true;proof.state='applied';
     // Publish an honest receipt even if a later validation/export is interrupted.
     writeFileSync(join(output,'RESULT.json'),JSON.stringify({...proof,exported:false},null,2));
     core.run(['validate']);
     core.run(['export']);proof.exported=true;
   } else proof.state='checked_only';
   writeFileSync(join(output,'RESULT.json'),JSON.stringify(proof,null,2)+'\n');return proof;
 } finally {rmSync(stage,{recursive:true,force:true});}
}
if(import.meta.main) {
 try {
   const args=process.argv.slice(2),v={repo:join(homedir(),'Developer/Bonfyre'),evidence:join(import.meta.dir,'nutrition-collection.yaff')};
   for(let i=0;i<args.length;i++)if(args[i]==='--apply')v.apply=true;else if(args[i]==='--check')v.apply=false;
   else if(['--repo','--evidence','--output'].includes(args[i])&&args[i+1])v[args[i].slice(2)]=args[++i];else fail('Use --repo ROOT --evidence FILE [--check|--apply] [--output DIR]');
   console.log(JSON.stringify(await reconcile(v),null,2));
 }catch(e){console.error(e.message);process.exitCode=1;}
}
