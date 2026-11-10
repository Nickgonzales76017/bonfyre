import {test, expect} from 'bun:test';
import {mkdtempSync, mkdirSync, readFileSync, writeFileSync, symlinkSync, lstatSync, rmSync} from 'node:fs';
import {join, resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {freeze} from '../release/connections_rollout.mjs';

const root=resolve(import.meta.dir,'../..');
const source='scripts/release/connections_rollout.mjs';
const sourceBytes=()=>readFileSync(join(root,source));
const target='bin/bonfyre-connection-catalog';
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');

function withCandidate(run){
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-connections-freeze-'));
 mkdirSync(join(dir,'bin'),{recursive:true});
 try{return run(dir,join(dir,target));}
 finally{rmSync(dir,{recursive:true,force:true});}
}

test('connection stage replaces only an inherited active-release symlink',()=>{
 withCandidate((dir,staged)=>{
  const active=join(dir,'active-connection-catalog');
  writeFileSync(active,'immutable active binary');
  symlinkSync(active,staged);
  const row=freeze(dir,source,target,0o755);
  expect(lstatSync(staged).isSymbolicLink()).toBe(false);
  expect(readFileSync(staged)).toEqual(sourceBytes());
  expect(readFileSync(active,'utf8')).toBe('immutable active binary');
  expect(row.material.sha256).toBe(hash(sourceBytes()));
 });
});

test('connection stage also replaces a dangling symlink instead of following it',()=>{
 withCandidate((dir,staged)=>{
  const absent=join(dir,'nonexistent-active-release');
  symlinkSync(absent,staged);
  freeze(dir,source,target,0o755);
  expect(lstatSync(staged).isSymbolicLink()).toBe(false);
  expect(readFileSync(staged)).toEqual(sourceBytes());
 });
});

test('a different regular frozen member remains an immutable collision',()=>{
 withCandidate((dir,staged)=>{
  writeFileSync(staged,'a conflicting immutable binary');
  expect(()=>freeze(dir,source,target,0o755)).toThrow('An existing frozen release member differs');
  expect(readFileSync(staged,'utf8')).toBe('a conflicting immutable binary');
 });
});
