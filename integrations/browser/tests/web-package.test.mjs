import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readdirSync,readFileSync,existsSync,mkdirSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {buildWebWorkbench,WEB_FILES} from '../web-package.mjs';
const source=resolve(import.meta.dir,'../extension'),sha=b=>createHash('sha256').update(b).digest('hex');
test('web release copies the existing editor bytes and excludes native host and private state',()=>{
 const root=mkdtempSync(join(tmpdir(),'workbench-web-'));try{
  const target=join(root,'public'),r=buildWebWorkbench(target);expect(r.staticOnly).toBe(true);expect(r.files).toHaveLength(20);expect(WEB_FILES).toContain('workbench-interactions.mjs');expect(WEB_FILES).toContain('cycle-reschedule.css');
  for(const file of r.files){expect(sha(readFileSync(join(target,file.name)))).toBe(file.sha256);expect(readFileSync(join(source,file.name))).toEqual(readFileSync(join(target,file.name)));}
  for(const name of ['manifest.json','background.js','panel.js','panel.html','_worker.js','host','receipts','drafts'])expect(existsSync(join(target,name))).toBe(false);
  const headers=readFileSync(join(target,'_headers'),'utf8');expect(headers).toContain("connect-src 'self'");expect(headers).toContain("form-action 'none'");expect(readFileSync(join(target,'_redirects'),'utf8')).toContain('/ /workbench.html');
  expect(sha(readFileSync(join(target,'release.tsv')))).toBe(r.release);
  for(const name of WEB_FILES.filter(p=>p.endsWith('.mjs'))){const text=readFileSync(join(target,name),'utf8');for(const hit of text.matchAll(/(?:from\s*|import\s*\()(['"])\.\/([^'"]+)\1/g))expect(WEB_FILES.includes(hit[2])).toBe(true);}
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('packaging refuses a preexisting directory and cannot replace it',()=>{
 const root=mkdtempSync(join(tmpdir(),'workbench-existing-'));try{const prior=readdirSync(root);expect(()=>buildWebWorkbench(root)).toThrow('new output');expect(readdirSync(root)).toEqual(prior);}finally{rmSync(root,{recursive:true,force:true});}
});
test('public projection keeps native-only controls distinct without removing the extension paths',()=>{
 const code=readFileSync(join(source,'workbench.mjs'),'utf8');expect(code).toContain("location.protocol!=='chrome-extension:'");expect(code).toContain("['capture','open-collected','hold-work']");expect(code).toContain("sendNativeMessage");expect(code).toContain('web-workspace-notice');
 expect(readFileSync(join(source,'workbench.html'),'utf8')).toContain('Open full studio');
});
