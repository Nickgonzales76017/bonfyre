import {test,expect} from 'bun:test';
import {mkdtempSync,copyFileSync,writeFileSync,chmodSync,existsSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
const binary=join(import.meta.dir,'../bonfyre-transcribe');
test('caption length rejects malformed values before creating output',()=>{
  const dir=mkdtempSync(join(tmpdir(),'transcribe-flags-'));
  try{
    for(const value of ['0','501','nan','18junk','-1']){
      const out=join(dir,'out-'+value),r=spawnSync(binary,['missing.wav',out,'--max-len',value],{encoding:'utf8'});
      expect(r.status).not.toBe(0);expect(r.stderr).toContain('--max-len must be');expect(existsSync(out)).toBe(false);
    }
  }finally{rmSync(dir,{recursive:true,force:true})}
});
test('managed transcription resolves the adjacent helper even outside the install directory',()=>{
  const dir=mkdtempSync(join(tmpdir(),'transcribe installed '));
  try{
    const exe=join(dir,'bonfyre-transcribe'),helper=join(dir,'bonfyre-media-prep');
    copyFileSync(binary,exe);chmodSync(exe,0o755);
    writeFileSync(helper,'#!/bin/sh\necho "adjacent-helper:$1" >&2\nexit 9\n');chmodSync(helper,0o755);
    const env={...process.env};delete env.BONFYRE_MEDIA_PREP_BINARY;
    const r=spawnSync(exe,['missing.wav',join(dir,'out'),'--no-vocab','--max-len','18'],{encoding:'utf8',cwd:tmpdir(),env});
    expect(r.status).not.toBe(0);
    expect(r.stderr).toContain('adjacent-helper:denoise');
    expect(r.stderr).toContain('adjacent-helper:normalize');
    expect(r.stderr).not.toContain('execvp: No such file');
  }finally{rmSync(dir,{recursive:true,force:true})}
});
