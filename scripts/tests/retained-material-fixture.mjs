import {mkdtempSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {homedir,tmpdir} from 'node:os';
import {pathToFileURL} from 'node:url';
import {prepareHistoryResearch} from '../history_research_preparation.mjs';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {decode} from '../../integrations/browser/workspace-msgpack.mjs';
export const baseRelease='c70d97e7004b69149941f1805f16d7f556792110b51977befc39bd663983e654';
export async function historyFixture(){
 const home=join(homedir(),'.bonfyre'),temp=mkdtempSync(join(tmpdir(),'reference-owner-'));
 try{for(const name of ['research.mjs','data.mjs'])writeFileSync(join(temp,name),await readWorkspaceFile(home+'/workspace-packages',baseRelease,'instruments/'+name));
  const {sourceRecord}=await import(pathToFileURL(join(temp,'research.mjs')).href),packets=[['01f4c06b4a58dee2f6ff213d7be34bac10b67e9cb03bf419f38f11cad1e2d8cb','.mpk'],['71e2fcf99aa6cfb7efd08417780a0f48c000fbf45a04d3bacd6f9cc93687dc64','.estates.mpk']].map(([sha256,suffix])=>({sha256,bytes:readFileSync(home+'/operator/history-material/'+sha256+suffix)}));
  const plan=await prepareHistoryResearch({packets,makeSource:sourceRecord,target:{binding:'ws-'+('1'.repeat(40)),release:baseRelease,subject:'user:1',scope:'self',pack:'experience.connected-instruments.workspace'}}),rows=plan.entries.map(e=>({...e,revision:1}));
  return {plan,rows,packets:packets.map(p=>decode(p.bytes,{maxBytes:16*1024*1024})),store:{async readLocal(prefix){return structuredClone(rows.filter(r=>r.key.startsWith(prefix)));},async readVersion(key){return structuredClone(rows.find(r=>r.key===key)||null);}}};
 }finally{rmSync(temp,{recursive:true,force:true});}
}
