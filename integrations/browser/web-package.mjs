#!/usr/bin/env bun
// A static projection of the existing browser workbench, not another editor.
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,lstatSync,existsSync,renameSync,rmSync} from 'node:fs';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
const here=dirname(fileURLToPath(import.meta.url));
export const WEB_FILES=Object.freeze(['workbench.html','workbench.mjs','workbench.css','cycle-reschedule.css','contents.mjs','contents-operations.mjs','contents-workspace.mjs','contents-workspace.css','workbench-shell.mjs','workbench-actions.mjs','workbench-interactions.mjs','program-editor.mjs','program-editor.css','capture.mjs','observation.mjs','recurring-commerce.mjs','store.mjs','catalog.json','studio.html','studio.mjs']);
const sha=b=>createHash('sha256').update(b).digest('hex');
export function buildWebWorkbench(destination){
  const target=resolve(destination),parent=dirname(target);
  if(existsSync(target))throw Error('Choose a new output directory.');
  mkdirSync(parent,{recursive:true});
  let stage=mkdtempSync(join(parent,'.workbench-'));
  try{
    const rows=[];
    for(const name of WEB_FILES){
      const path=join(here,'extension',name),stat=lstatSync(path);
      if(!stat.isFile()||stat.size>2*1024*1024)throw Error('Unsafe or oversized web source: '+name);
      const body=readFileSync(path),digest=sha(body);writeFileSync(join(stage,name),body);
      if(sha(readFileSync(path))!==digest)throw Error('Source changed while packaging: '+name);
      rows.push({name,sha256:digest,bytes:body.length});
    }
    // All editor code and candidate state stay on this origin. No intake Worker.
    writeFileSync(join(stage,'_headers'),'/*\n  X-Content-Type-Options: nosniff\n  Referrer-Policy: strict-origin-when-cross-origin\n  X-Robots-Tag: noindex\n  Content-Security-Policy: default-src \'self\'; script-src \'self\'; style-src \'self\' \'unsafe-inline\'; img-src \'self\' data:; connect-src \'self\'; object-src \'none\'; frame-ancestors \'none\'; base-uri \'none\'; form-action \'none\'\n');
    writeFileSync(join(stage,'_redirects'),'/ /workbench.html 302\n');
    const manifest=rows.map(r=>r.sha256+'\t'+r.name+'\n').join('');
    writeFileSync(join(stage,'release.tsv'),manifest);
    writeFileSync(join(stage,'README.txt'),'Aurekai Browser Workbench\n\nOpen workbench.html from a static HTTP server. The web workspace accepts saved captures, compares selected sources, edits the existing 36 program templates and retains drafts in browser-local storage. It does not collect another tab, upload captures, activate subscriptions, submit native work or share drafts between devices. Those native actions require the separately installed extension and its approved host.\n\nThe files in release.tsv are copied from integrations/browser/extension. Regenerate this release with bun integrations/browser/web-package.mjs NEW_DIRECTORY.\n');
    renameSync(stage,target);stage=null;
    return {directory:target,release:sha(manifest),files:rows,staticOnly:true};
  }finally{if(stage)rmSync(stage,{recursive:true,force:true});}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{if(process.argv.length!==3)throw Error('usage: web-package.mjs NEW_DIRECTORY');const r=buildWebWorkbench(process.argv[2]);console.log('state PACKAGED\nrelease '+r.release+'\nfiles '+r.files.length+'\ndirectory '+r.directory);}catch(error){console.error(error.message);process.exitCode=1;}
}
