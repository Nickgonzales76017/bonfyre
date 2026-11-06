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

const sha=raw=>createHash('sha256').update(raw).digest('hex');
const binary=resolve(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue');

test('native source worker materializes a verified typed experience handoff before completion',async()=>{
  const home=mkdtempSync(join(tmpdir(),'bonfyre-source-experience-'));
  try{
    const body=Buffer.from('<a href="/catalogue/">Catalogue</a><a href="/training/">Training</a>');
    const digest=sha(body),directory=join(home,'source');mkdirSync(directory);
    const file=join(directory,digest+'.html'),receiptPath=join(directory,digest+'.receipt.json');
    writeFileSync(file,body);writeFileSync(receiptPath,JSON.stringify({
      source_url:'https://aurekai-work-samples.pages.dev/',
      policy_decision:'allow',http_status:200,content_sha256:digest
    }));
    const source={body:file,receipt:receiptPath,digest};
    const intent={mission:'source:work-samples:canary-1',family:'source-sync',digest,ifAbsent:true};
    const owner=queueOwner({binary,db:join(home,'workgraph.db')});
    const admitted=await owner.enqueue(intent);
    expect(admitted.accepted).toBe(true);
    const id=nativeMission(intent),worker='source-experience-test';
    const claim=(await owner.run(['claim',id,'job',worker,'--family','source-sync','--lease-ms','60000']))[0];
    expect(claim.claim_token).toMatch(/^[a-f0-9]{64}$/);
    const projection=materializeSourceExperience({source,root:home});
    expect(projection.experiences).toEqual(['catalogue-workspace','training-workspace']);
    const rows=decode(readFileSync(fileURLToPath(projection.uri),'utf8'));
    expect(one(rows,'authority')).toBe('none');
    const checkpoint=(await owner.run(['checkpoint-read',id,'job']))[0];
    const saved=(await owner.run(['checkpoint-save',id,'job',worker,claim.claim_token,
      '--expected-revision',String(checkpoint.revision),'--artifact-uri',projection.uri]))[0];
    await owner.run(['ack',id,'job',worker,claim.claim_token,
      '--checkpoint-revision',String(saved.revision),'--output-uri',projection.uri]);
    const row=(await owner.read(id))[0],record=(await owner.run(['checkpoint-read',id,'job']))[0];
    expect(row.status).toBe('complete');
    expect(row.attempt).toBe(1);
    expect(row.output_uri).toBe(projection.uri);
    expect(record.artifact_uri).toBe(projection.uri);
    expect(record.revision).toBe(1);
    expect((await owner.enqueue(intent)).accepted).toBe(true);
    expect((await owner.read(id))).toHaveLength(1);
    writeFileSync(file,Buffer.from('corruption'));
    expect(()=>materializeSourceExperience({source,root:home})).toThrow();
    expect((await owner.read(id))[0].output_uri).toBe(projection.uri);
  }finally{rmSync(home,{recursive:true,force:true});}
});
