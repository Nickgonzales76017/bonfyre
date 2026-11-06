import {test,expect,beforeAll,afterAll} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,realpathSync,readFileSync,writeFileSync,rmSync,copyFileSync,chmodSync,symlinkSync,readlinkSync,mkdirSync,existsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {installRepresentation} from '../representation-release.mjs';
import {verifyRelease} from '../representation-entry.mjs';
const repo=resolve(import.meta.dir,'../..'),home=realpathSync(mkdtempSync(join(tmpdir(),'bf-release-'))),prefix=join(home,'install');
let release;const env={...process.env,BONFYRE_BUN_BINARY:process.execPath};
const run=(...args)=>Bun.spawnSync([join(prefix,'bin/bonfyre-runtime'),'representation',...args],{cwd:home,env,stdout:'pipe',stderr:'pipe',timeout:15000});
beforeAll(async()=>{
 const build=Bun.spawnSync(['make','-C',join(repo,'cmd/BonfyreRuntime'),'bonfyre-runtime'],{cwd:repo,stdout:'pipe',stderr:'pipe',timeout:45000});if(build.exitCode)throw Error(build.stderr.toString());
 release=await installRepresentation({prefix});copyFileSync(join(repo,'cmd/BonfyreRuntime/bonfyre-runtime'),join(prefix,'bin/bonfyre-runtime'));chmodSync(join(prefix,'bin/bonfyre-runtime'),0o755);
});
afterAll(()=>rmSync(home,{recursive:true,force:true}));
test('installed native Runtime discovers the exact release outside the checkout',()=>{
 const result=run('doctor');expect(result.exitCode).toBe(0);expect(result.stdout.toString()).toContain(release.digest);expect(result.stdout.toString()).toContain('state INSTALLED_VERIFIED');expect(result.stdout.toString()).not.toContain(repo);
 expect(verifyRelease(release.release).files).toBeGreaterThan(12);expect(run('help').stdout.toString()).toContain('verify --estate');
});
test('installed apply and read-only verify use one owner across process restarts',()=>{
 const estate=join(home,'estate');mkdirSync(estate);const db=new Database(join(estate,'cms.db'));db.exec("CREATE TABLE selected(id INTEGER,v TEXT);INSERT INTO selected VALUES(1,'first');CREATE TABLE unrelated(v TEXT)");db.close();
 const args=['--estate',estate,'--kind','cms.selected'];const first=run('apply',...args);if(first.exitCode)throw Error(first.stderr.toString());expect(first.stdout.toString()).toContain('lowered 1');
 const second=run('verify',...args);expect(second.exitCode).toBe(0);expect(second.stdout.toString()).toContain('state REUSABLE');
 const update=new Database(join(estate,'cms.db'));update.exec("INSERT INTO unrelated VALUES('unselected')");update.close();const warm=run('apply',...args);expect(warm.exitCode).toBe(0);expect(warm.stdout.toString()).toContain('source_snapshots 0');
 const changed=new Database(join(estate,'cms.db'));changed.exec("UPDATE selected SET v='second'");changed.close();expect(run('verify',...args).exitCode).toBe(3);
 expect(run('apply',...args).exitCode).toBe(0);expect(run('verify',...args).exitCode).toBe(0);
});
test('implicit mutation, mixed operations and unknown options are refused',()=>{
 for(const args of [['apply'],['apply','--kind','cms.selected'],['doctor','--estate',home],['verify','--apply','yes'],['unknown'],['status','--kind','cms.selected']])expect(run(...args).exitCode).not.toBe(0);
});
test('reinstallation reuses the exact release and refuses a stale expected current',async()=>{
 const again=await installRepresentation({prefix,expectedCurrent:release.digest});expect(again.digest).toBe(release.digest);expect(again.previous).toBe(release.digest);
 await expect(installRepresentation({prefix,expectedCurrent:'0'.repeat(64)})).rejects.toThrow('Active release changed');expect(run('doctor').exitCode).toBe(0);
});
test('corrupt installed code is refused rather than using a valid checkout copy',()=>{
 const path=join(release.release,'scripts/representation-manifold-shift.mjs'),before=readFileSync(path);writeFileSync(path,Buffer.concat([before,Buffer.from('\n// drift\n')]));
 try{const failed=run('doctor');expect(failed.exitCode).not.toBe(0);expect(failed.stderr.toString()).toContain('digest or inventory mismatch');}finally{writeFileSync(path,before);}
 expect(run('doctor').exitCode).toBe(0);
});
test('missing native executable and unowned release members refuse',()=>{
 const path=join(release.release,'cmd/BonfyreHash/bonfyre-hash'),bytes=readFileSync(path);rmSync(path);
 try{expect(run('doctor').exitCode).not.toBe(0);}finally{writeFileSync(path,bytes,{mode:0o755});}
 const extra=join(release.release,'unowned');writeFileSync(extra,'unexpected');try{expect(run('doctor').exitCode).not.toBe(0);}finally{rmSync(extra);}
});
test('a failed new build preserves the active link and working launcher',async()=>{
 const fake=join(home,'fail-build');mkdirSync(fake);writeFileSync(join(fake,'make'),'#!/bin/sh\nexit 67\n',{mode:0o755});const original=process.env.PATH,current=join(prefix,'share/bonfyre/representation/current'),before=readlinkSync(current);
 process.env.PATH=fake+':'+original;try{await expect(installRepresentation({prefix})).rejects.toThrow('build failed');}finally{process.env.PATH=original;}
 expect(readlinkSync(current)).toBe(before);expect(run('doctor').exitCode).toBe(0);expect(existsSync(join(prefix,'share/bonfyre/representation/activation.lock'))).toBe(false);
});
test('another activation lock or an escaping current link is not overridden',async()=>{
 const base=join(prefix,'share/bonfyre/representation'),lock=join(base,'activation.lock');mkdirSync(lock);try{await expect(installRepresentation({prefix})).rejects.toThrow();expect(existsSync(lock)).toBe(true);}finally{rmSync(lock,{recursive:true});}
 const current=join(base,'current'),prior=readlinkSync(current);rmSync(current);symlinkSync(home,current);try{await expect(installRepresentation({prefix})).rejects.toThrow('escapes');expect(readlinkSync(current)).toBe(home);}finally{rmSync(current);symlinkSync(prior,current);}
});
