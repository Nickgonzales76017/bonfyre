// Real filesystem + compiled production reader. No model, network or estate
// data is used. The normal target links the repository's libbonfyre SHA-256.
import {test,after} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,readdir,lstat,rm,symlink,chmod} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,join,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync,spawn} from 'node:child_process';
import {retainWorkspaceFiles,materializeWorkspacePackage,workspaceSourceBytes} from '../../../scripts/materialize_workspace_package.mjs';
import {manifestRows,readWorkspaceFile,loadWorkspaceFiles,readWorkspaceBody,verifyWorkspacePackage,retainWorkspaceMaterial,workspaceLayout} from '../../../scripts/workspace_material.mjs';

const repo=resolve(import.meta.dirname,'../../..');
const temp=await mkdtemp(join(tmpdir(),'workspace-native-'));
const reader=process.env.BONFYRE_WORKSPACE_TEST_READER??join(temp,'reader');
if(!process.env.BONFYRE_WORKSPACE_TEST_READER){
  const built=spawnSync(process.env.CC??'cc',['-std=c11','-O2','-Wall','-Wextra','-Werror',
    '-I'+join(repo,'lib/libbonfyre/include'),join(import.meta.dirname,'workspace_package_reader.c'),
    join(repo,'lib/libbonfyre/src/bf_sha256.c'),'-o',reader],{encoding:'utf8'});
  assert.equal(built.status,0,built.stderr||built.error?.message);
}
after(()=>rm(temp,{recursive:true,force:true}));
const sha=b=>createHash('sha256').update(b).digest('hex');
const bodyAt=(root,b)=>join(root,'_bodies/sha256',sha(b).slice(0,2),sha(b));
const files=(text='hello')=>new Map([['editor/index.html',Buffer.from(text)],['assets/model.glb',Buffer.from('retained-model')]]);
const manifestOf=map=>'path\tsha256\tbytes\n'+[...map].sort(([a],[b])=>a.localeCompare(b,'en')).map(([path,b])=>`${path}\t${sha(b)}\t${b.length}\n`).join('');
const native=(root,id,path)=>spawnSync(reader,[root,id,path],{maxBuffer:20*1024*1024,timeout:5000});
const caseDir=async()=>mkdtemp(join(temp,'case-'));
async function entries(root,prefix=''){
  const out=[];for(const name of await readdir(root)){const p=join(root,name),st=await lstat(p);
    if(st.isDirectory())out.push(...await entries(p,prefix+name+'/'));
    else out.push({path:prefix+name,bytes:st.size,mtime:st.mtimeMs,ino:st.ino});}
  return out.sort((a,b)=>a.path.localeCompare(b.path));
}
async function forged(root,text,layout){const id=sha(text);await mkdir(join(root,id));await writeFile(join(root,id,'MANIFEST.tsv'),text);if(layout!==undefined)await writeFile(join(root,id,'STORAGE.tsv'),layout);return id;}
async function bothRefuse(root,id,path){await assert.rejects(readWorkspaceFile(root,id,path));assert.equal(native(root,id,path).status,2);}

test('new releases contain manifests, not copied assets; native bytes match JS bytes',async()=>{
  const root=await caseDir(),map=files(),r=await retainWorkspaceFiles(map,{root});
  assert.deepEqual(await readdir(r.dir),['MANIFEST.tsv','STORAGE.tsv']);assert.equal(r.id,sha(manifestOf(map)));
  for(const [path,b]of map){assert.deepEqual(await readWorkspaceFile(root,r.id,path),b);const n=native(root,r.id,path);assert.equal(n.status,0);assert.deepEqual(n.stdout,b);}
  assert.deepEqual(await loadWorkspaceFiles(root,r.id),new Map([...map].sort(([a],[b])=>a.localeCompare(b,'en'))));
  assert.equal(r.newBodies,2);assert.equal((await verifyWorkspacePackage(root,r.id)).layout,'shared-bodies-v1');
});
test('a repeat leaves all bytes, modification times and inodes unchanged',async()=>{
  const root=await caseDir(),r=await retainWorkspaceFiles(files(),{root}),before=await entries(root);
  const again=await retainWorkspaceFiles(files(),{root});assert.equal(again.id,r.id);
  assert.equal(again.writtenBodyBytes,0);assert.equal(again.manifestBytes,0);assert.equal(again.newBodies,0);
  assert.deepEqual(await entries(root),before);
});
test('changed release stores only changed bodies and shares aliases',async()=>{
  const root=await caseDir(),map=files(),a=await retainWorkspaceFiles(map,{root});
  map.set('editor/index.html',Buffer.from('changed'));map.set('assets/alias.glb',map.get('assets/model.glb'));
  const b=await retainWorkspaceFiles(map,{root});assert.notEqual(a.id,b.id);assert.equal(b.newBodies,1);assert.equal(b.writtenBodyBytes,7);
  assert.equal(b.reusedBodyBytes,2*Buffer.byteLength('retained-model'));
  assert.deepEqual(native(root,b.id,'assets/alias.glb').stdout,Buffer.from('retained-model'));
  assert.equal(native(root,a.id,'editor/index.html').stdout.toString(),'hello');
});
test('seven releases retain one 8 MiB asset rather than seven copies',async()=>{
  const root=await caseDir(),asset=Buffer.alloc(8*1024*1024,42);let logical=0,written=0;
  for(let i=0;i<7;i++){const html=Buffer.from(`release ${i}`),r=await retainWorkspaceFiles(new Map([['assets/model.glb',asset],['editor/index.html',html]]),{root});
    logical+=asset.length+html.length;written+=r.writtenBodyBytes;assert.equal(native(root,r.id,'assets/model.glb').status,0);}
  const stored=(await entries(root)).filter(e=>e.path.startsWith('_bodies/')).reduce((a,e)=>a+e.bytes,0);
  assert.equal(stored,written);assert.equal(logical-stored,6*asset.length);
  console.log(`material-volume\tlogical=${logical}\tbody_bytes=${stored}\tavoided_body_bytes=${logical-stored}`);
});
test('legacy trees remain readable and are not automatically rewritten',async()=>{
  const root=await caseDir(),map=files(),text=manifestOf(map),id=await forged(root,text);
  for(const [path,body]of map){await mkdir(dirname(join(root,id,path)),{recursive:true});await writeFile(join(root,id,path),body);}
  const before=await entries(root),r=await retainWorkspaceFiles(map,{root});assert.equal(r.id,id);assert.equal(r.layout,'legacy-tree');
  assert.deepEqual(await entries(root),before);assert.equal(native(root,id,'editor/index.html').stdout.toString(),'hello');
});
test('source manifests keep their exact byte order and release identity',async()=>{
  const root=await caseDir(),map=files(),text='path\tsha256\tbytes\n'+[...map].map(([p,b])=>`${p}\t${sha(b)}\t${b.length}\n`).join('');
  const r=await retainWorkspaceMaterial(map,{root,manifest:text});assert.equal(r.id,sha(text));assert.equal(await readFile(join(r.dir,'MANIFEST.tsv'),'utf8'),text);
});
for(const name of ['../outside','/absolute','bad//path','a/./b','MANIFEST.tsv','STORAGE.tsv','a\tbad','a\nbad','a'.repeat(1001)]){
  test(`invalid name is refused before any publication: ${name.slice(0,30)}`,async()=>{
    const root=await caseDir();await assert.rejects(retainWorkspaceFiles(new Map([[name,Buffer.from('x')]]),{root}));assert.deepEqual(await readdir(root),[]);
  });
}
test('file/directory collisions refuse before any retained bytes',async()=>{
  const root=await caseDir();await assert.rejects(retainWorkspaceFiles(new Map([['a',Buffer.from('x')],['a-else',Buffer.from('y')],['a/b',Buffer.from('z')]]),{root}));assert.deepEqual(await readdir(root),[]);
});
test('caller mutation after invocation does not alter admitted output',async()=>{
  const root=await caseDir(),body=Buffer.from('before'),pending=retainWorkspaceFiles(new Map([['a',body]]),{root});body.fill(0);
  const r=await pending;assert.equal((await readWorkspaceFile(root,r.id,'a')).toString(),'before');
});
test('corrupt shared bodies refuse without repair or copying a substitute',async()=>{
  const root=await caseDir(),map=files(),r=await retainWorkspaceFiles(map,{root}),path=bodyAt(root,map.get('assets/model.glb'));
  await chmod(path,0o600);await writeFile(path,'corrupt');await bothRefuse(root,r.id,'assets/model.glb');
  await assert.rejects(retainWorkspaceFiles(map,{root}),/Source changed/);assert.equal(await readFile(path,'utf8'),'corrupt');
});
test('a missing shared body never falls back to a release-local shadow',async()=>{
  const root=await caseDir(),map=files(),r=await retainWorkspaceFiles(map,{root});await rm(bodyAt(root,map.get('assets/model.glb')));
  await mkdir(join(r.dir,'assets'));await writeFile(join(r.dir,'assets/model.glb'),map.get('assets/model.glb'));await bothRefuse(root,r.id,'assets/model.glb');
});
test('undeclared digests and paths are not a package read capability',async()=>{
  const root=await caseDir(),r=await retainWorkspaceFiles(files(),{root});await bothRefuse(root,r.id,'_bodies/sha256/'+sha('retained-model'));
  await bothRefuse(root,r.id,'../'+r.id+'/MANIFEST.tsv');await bothRefuse(root,sha('other'),'assets/model.glb');
});
test('unknown or oversized storage markers refuse instead of falling back',async()=>{
  const root=await caseDir(),r=await retainWorkspaceFiles(files(),{root}),path=join(r.dir,'STORAGE.tsv');await chmod(path,0o600);
  for(const value of ['layout\tother\n','x'.repeat(129),'']){await writeFile(path,value);await bothRefuse(root,r.id,'editor/index.html');}
});
test('incomplete published directories are refused, not repaired',async()=>{
  const root=await caseDir(),id=sha(manifestOf(files()));await mkdir(join(root,id));
  await assert.rejects(retainWorkspaceFiles(files(),{root}),/Incomplete workspace release/);assert.deepEqual(await readdir(root),[id]);
});
test('missing legacy files refuse even when an identical shared body exists',async()=>{
  const root=await caseDir(),map=files();await retainWorkspaceFiles(map,{root});
  const other=new Map(map);other.set('different.html',Buffer.from('different'));const id=await forged(root,manifestOf(other));
  await bothRefuse(root,id,'assets/model.glb');await assert.rejects(retainWorkspaceFiles(other,{root}));
});
test('symlink body, manifest, marker and release directories are refused',async()=>{
  for(const member of ['body','manifest','marker','release']){
    const root=await caseDir(),map=files(),r=await retainWorkspaceFiles(map,{root});
    const path=member==='body'?bodyAt(root,map.get('assets/model.glb')):member==='manifest'?join(r.dir,'MANIFEST.tsv'):member==='marker'?join(r.dir,'STORAGE.tsv'):r.dir;
    const displaced=path+'.real';const {rename}=await import('node:fs/promises');await rename(path,displaced);await symlink(displaced,path);
    await bothRefuse(root,r.id,'assets/model.glb');
  }
});
test('symlink body-store parents and configured roots are refused',async()=>{
  const root=await caseDir(),r=await retainWorkspaceFiles(files(),{root}),{rename}=await import('node:fs/promises');
  await rename(join(root,'_bodies'),join(root,'other'));await symlink('other',join(root,'_bodies'));await bothRefuse(root,r.id,'editor/index.html');
  const alias=root+'-alias';await symlink(root,alias);await bothRefuse(alias,r.id,'editor/index.html');
  await assert.rejects(retainWorkspaceFiles(files(),{root:alias}));
});
test('plain client-file reader refuses nested symlinks',async()=>{
  const root=await caseDir();await mkdir(join(root,'real'));await writeFile(join(root,'real/file'),'client');await symlink('real',join(root,'alias'));
  assert.equal(native(root,'--file','real/file').stdout.toString(),'client');assert.equal(native(root,'--file','alias/file').status,2);
});
for(const mutate of [
  text=>text.replace(/\t5\n/,'\t5e0\n'),
  text=>text.replace(/\t5\n/,'\t+5\n'),
  text=>text.replace(/\t5\n/,'\t5 \n'),
  text=>text+'editor/index.html\t'+sha('hello')+'\t5\n',
  text=>text+'z\0hidden\t'+sha('')+'\t0\n',
  text=>text+'a\t'+sha('x')+'\t1\na-else\t'+sha('x')+'\t1\na/b\t'+sha('x')+'\t1\n',
  text=>text+'same-hash-different-length\t'+sha('hello')+'\t4\n',
  text=>text+'\n',
  text=>text.slice(0,-1),
]){
  test('native and JS reject malformed full manifest, including unrelated late entries',async()=>{
    const root=await caseDir(),text=mutate(manifestOf(files())),id=await forged(root,text,workspaceLayout);
    assert.throws(()=>manifestRows(text));await bothRefuse(root,id,'editor/index.html');
  });
}
test('zero-byte bodies are shared and readable by both consumers',async()=>{
  const root=await caseDir(),r=await retainWorkspaceFiles(new Map([['a',Buffer.alloc(0)],['b',Buffer.alloc(0)]]),{root});
  assert.equal(r.newBodies,1);assert.equal(native(root,r.id,'a').status,0);assert.equal((await readWorkspaceFile(root,r.id,'b')).length,0);
});
test('concurrent processes publish one release without partial winners',async()=>{
  const root=await caseDir(),module=new URL('../../../scripts/materialize_workspace_package.mjs',import.meta.url).href;
  const code=`import {retainWorkspaceFiles} from ${JSON.stringify(module)};const r=await retainWorkspaceFiles(new Map([['a',Buffer.alloc(100000,7)]]),{root:process.argv[1]});console.log(r.id);`;
  const run=()=>new Promise((resolve,reject)=>{const p=spawn(process.execPath,['--input-type=module','-e',code,root]);let out='',err='';p.stdout.on('data',b=>out+=b);p.stderr.on('data',b=>err+=b);p.on('error',reject);p.on('close',code=>code?reject(Error(err)):resolve(out.trim()));});
  const ids=await Promise.all([run(),run(),run(),run()]);assert.equal(new Set(ids).size,1);assert.equal(native(root,ids[0],'a').stdout.length,100000);
  assert.equal((await readdir(root)).filter(n=>n.startsWith('.workspace-')).length,0);await verifyWorkspacePackage(root,ids[0]);
});
test('a source stream is canceled immediately when it exceeds its byte bound',async()=>{
  let canceled=false,count=0;const stream=new ReadableStream({pull(c){count++;c.enqueue(new Uint8Array(4));},cancel(){canceled=true;}});
  await assert.rejects(workspaceSourceBytes(new Response(stream),5),/bound/);assert.equal(canceled,true);assert.ok(count<=4);
});
test('failed parallel source downloads are joined and publish no partial source',async()=>{
  const root=await caseDir(),map=new Map([['a',Buffer.from('a')],['b',Buffer.from('b')]]);let active=0;
  const fetcher=async(url,{signal})=>{if(url.endsWith('RELEASE.tsv'))return new Response(manifestOf(map));active++;
    try{await new Promise((resolve,reject)=>{const timer=setTimeout(resolve,15);signal.addEventListener('abort',()=>{clearTimeout(timer);reject(signal.reason);},{once:true});});return new Response('wrong');}finally{active--;}};
  await assert.rejects(materializeWorkspacePackage({root,commit:'a'.repeat(40),fetcher}));assert.equal(active,0);assert.deepEqual(await readdir(root),[]);
});
test('body lookup checks digest and length before a public source can reuse it',async()=>{
  const root=await caseDir(),r=await retainWorkspaceFiles(files(),{root});assert.ok(r.id);
  assert.equal((await readWorkspaceBody(root,sha('retained-model'),14)).toString(),'retained-model');
  await assert.rejects(readWorkspaceBody(root,sha('retained-model'),13));assert.equal(await readWorkspaceBody(root,sha('absent'),6),null);
});

// Interleave the writer's actual hard-link retirement with reader syscalls in
// an isolated child. The production module and real filesystem are unchanged.
for (const phase of ['open','read','metadata','corrupt','repeated']) {
  test(`shared body read handles staging-link retirement: ${phase}`,async()=>{
    const root=await caseDir(),body=Buffer.from('retained-model');
    await retainWorkspaceFiles(files(),{root});
    const target=bodyAt(root,body),alias=join(root,'staging-link');
    const module=new URL('../../../scripts/workspace_material.mjs',import.meta.url).href;
    const code=`
      import fs from 'node:fs/promises';
      import {syncBuiltinESMExports} from 'node:module';
      const [root,givenTarget,alias,phase,digest,module]=process.argv.slice(1);
      const target=await fs.realpath(givenTarget);
      await fs.link(target,alias);
      const originalOpen=fs.open,originalStat=fs.lstat;
      let count=0,changed=false;
      const change=async()=>{
        changed=true;
        if(phase==='metadata')await fs.chmod(target,0o600);
        else {
          await fs.unlink(alias);
          if(phase==='corrupt'){
            await fs.chmod(target,0o600);await fs.writeFile(target,'damaged--model');await fs.chmod(target,0o400);
          }
        }
      };
      fs.lstat=async(path,...args)=>{
        if(path===target&&phase==='repeated'&&count>0)await fs.link(target,alias);
        return originalStat(path,...args);
      };
      fs.open=async(path,...args)=>{
        if(path===target){count++;if(phase!=='read'&&(!changed||phase==='repeated'))await change();}
        const handle=await originalOpen(path,...args);
        if(path===target&&phase==='read'&&!changed){
          const read=handle.read.bind(handle);
          handle.read=async(...values)=>{const result=await read(...values);if(!changed)await change();return result;};
        }
        return handle;
      };
      syncBuiltinESMExports();
      const {readWorkspaceBody}=await import(module);
      try{const body=await readWorkspaceBody(root,digest,14);console.log('OK '+count+' '+body.toString('hex'));}
      catch(error){console.log('REFUSED '+count+' '+error.message);}
    `;
    const result=spawnSync(process.execPath,['--input-type=module','-e',code,root,target,alias,phase,sha(body),module],{encoding:'utf8',timeout:10000});
    assert.equal(result.status,0,result.stderr);
    if(phase==='open'||phase==='read')assert.equal(result.stdout.trim(),'OK 2 '+body.toString('hex'));
    else if(phase==='repeated')assert.match(result.stdout,/^REFUSED 3 Workspace file changed/);
    else assert.match(result.stdout,/^REFUSED 1 Workspace file changed/);
  });
}
