import {test,expect} from 'bun:test';
import {join} from 'node:path';
import {homedir,tmpdir} from 'node:os';
import {mkdtempSync,rmSync,existsSync,readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {materializeSourceExperience} from './source-experience-projection.mjs';
import {decode,one} from '../experience-handoffs/contract.mjs';

test('retained live ForeignTwin material produces actual owned experience candidates without network or authority',()=>{
  const digest='6977bf9a8c97148abcd6e474437f2fab14c68b6467a614ef24cccd9ec74a7c38';
  const sourceRoot=join(homedir(),'.bonfyre','edge-workgraph','source');
  const source={digest,body:join(sourceRoot,digest+'.html'),receipt:join(sourceRoot,digest+'.receipt.json')};
  expect(existsSync(source.body)).toBe(true);
  expect(existsSync(source.receipt)).toBe(true);
  const home=mkdtempSync(join(tmpdir(),'edge-retained-source-'));
  try{
    const projection=materializeSourceExperience({source,root:home});
    const text=readFileSync(fileURLToPath(projection.uri),'utf8'),rows=decode(text);
    expect(one(rows,'source_digest')).toBe(digest);
    expect(one(rows,'authority')).toBe('none');
    expect(projection.candidates).toBeGreaterThan(0);
    expect(rows.get('candidate').length).toBe(projection.candidates);
    expect(createHash('sha256').update(text).digest('hex')).toBe(projection.digest);
    console.log('retained-source candidate experiences: '+projection.experiences.join(', '));
  }finally{rmSync(home,{recursive:true,force:true});}
});
