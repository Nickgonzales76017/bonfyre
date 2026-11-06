import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {queueOwner,nativeMission} from './native-queue-adapter.mjs';
import {materializeSourceExperience} from './source-experience-projection.mjs';
import {prepareSourceExperienceHandoffs} from './source-experience-admission.mjs';
import {decode,one} from '../experience-handoffs/contract.mjs';

const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
const binary=resolve(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue');

test('verified native source completion materializes reusable, authority-free handoffs',async()=>{
  const home=mkdtempSync(join(tmpdir(),'source-experience-handoff-'));
  try{
    const data=Buffer.from('<a href="/catalogue/">Catalogue</a><a href="/training/">Training</a>');
    const hash=digest(data),dir=join(home,'source');mkdirSync(dir);
    const body=join(dir,hash+'.html'),receipt=join(dir,hash+'.receipt.json');
    writeFileSync(body,data);
    writeFileSync(receipt,JSON.stringify({source_url:'https://aurekai-work-samples.pages.dev/',
      policy_decision:'allow',http_status:200,content_sha256:hash}));
    const intent={mission:'source:work-samples',family:'source-sync',digest:hash,ifAbsent:true};
    const owner=queueOwner({binary,db:join(home,'native.db')});
    await owner.enqueue(intent);
    const id=nativeMission(intent);
    const claim=(await owner.run(['claim',id,'job','handoff-test','--family','source-sync','--lease-ms','60000']))[0];
    const projection=materializeSourceExperience({source:{digest:hash,body,receipt},root:home});
    const checkpoint=(await owner.run(['checkpoint-read',id,'job']))[0];
    const saved=(await owner.run(['checkpoint-save',id,'job','handoff-test',claim.claim_token,
      '--expected-revision',String(checkpoint.revision),'--artifact-uri',projection.uri]))[0];
    await owner.run(['ack',id,'job','handoff-test',claim.claim_token,
      '--checkpoint-revision',String(saved.revision),'--output-uri',projection.uri]);
    const handoff=await prepareSourceExperienceHandoffs({owner,root:home,digest:hash});
    expect(handoff.admitted.map(x=>x.experience)).toEqual(['catalogue-workspace','training-workspace']);
    for(const item of handoff.admitted){
      const rows=decode(readFileSync(fileURLToPath(item.artifactUri),'utf8'));
      expect(one(rows,'source_digest')).toBe(hash);
      expect(one(rows,'authority')).toBe('none');
      expect(one(rows,'destination_experience')).toBe(item.experience);
    }
    expect(await prepareSourceExperienceHandoffs({owner,root:home,digest:hash})).toEqual(handoff);
    writeFileSync(body,'corrupted after native completion');
    await expect(prepareSourceExperienceHandoffs({owner,root:home,digest:hash})).rejects.toThrow();
  }finally{rmSync(home,{recursive:true,force:true});}
});
