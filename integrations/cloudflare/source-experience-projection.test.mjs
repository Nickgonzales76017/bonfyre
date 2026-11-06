import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,mkdirSync,writeFileSync,readFileSync,readdirSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {decode,one} from '../experience-handoffs/contract.mjs';
import {discoverExperienceRoutes,compileSourceExperience,materializeSourceExperience} from './source-experience-projection.mjs';

const SOURCE='https://aurekai-work-samples.pages.dev/';
const sha=x=>createHash('sha256').update(x).digest('hex');
const homepage=Buffer.from([
  '<!doctype html><html><body>',
  '<a href="/catalogue/">Catalogue</a>',
  '<a href="https://aurekai-work-samples.pages.dev/catalogue/#items">Duplicate canonical route</a>',
  '<a href="/documents/">Documents</a>',
  '<a href="https://itad-desk-upgrade.aurekai-work-samples.pages.dev/itad-exchange/">ITAD</a>',
  '<a href="https://brief-studio-upgrade.aurekai-work-samples.pages.dev/brief-studio/">Brief</a>',
  '<a href="https://evil.test/catalogue/">Unowned</a>',
  '<a href="https://aurekai-work-samples.pages.dev/catalogue/?write=true">Query not canonical</a>',
  '<a href="javascript:alert(1)">Not a source</a>',
  '<!-- <a href="/audio/">Commented out</a> -->',
  '</body></html>'
].join(''));
const receipt=body=>({source_url:SOURCE,policy_decision:'allow',http_status:200,content_sha256:sha(body),observed_at:'2026-10-15T07:00:00Z'});

test('real authored route links become deduplicated, bounded, review-only typed candidates',()=>{
  const found=discoverExperienceRoutes(homepage);
  expect(found.map(x=>x.id)).toEqual(['acquisition-brief-studio','catalogue-workspace','document-workspace','itad-acquisition-desk']);
  const projected=compileSourceExperience({body:homepage,receipt:receipt(homepage),receiptPath:'/native/receipts/exact',expectedDigest:sha(homepage)});
  const rows=decode(projected.text);
  expect(one(rows,'authority')).toBe('none');
  expect(one(rows,'source_digest')).toBe(sha(homepage));
  expect(rows.get('candidate')).toHaveLength(4);
  expect(projected.text).not.toContain('Unowned');
  expect(projected.text).not.toContain('javascript');
});

test('wrong source policy, changed digest and unsupported body refuse materialization',()=>{
  const options={body:homepage,receipt:receipt(homepage),receiptPath:'/native/receipt',expectedDigest:sha(homepage)};
  for(const change of [{receipt:{...options.receipt,policy_decision:'deny'}},
    {receipt:{...options.receipt,source_url:'https://evil.test/'}},
    {expectedDigest:'a'.repeat(64)},{body:Buffer.from('changed')}])
    expect(()=>compileSourceExperience({...options,...change})).toThrow();
  expect(()=>discoverExperienceRoutes(Buffer.alloc(1048577))).toThrow();
  const empty=Buffer.from('<html><body>Soft 200 error, no actual experience.</body></html>');
  expect(()=>compileSourceExperience({body:empty,receipt:receipt(empty),
    receiptPath:'/native/receipt',expectedDigest:sha(empty)})).toThrow(/source needs review/);
});

test('source material survives restart and retains immutable file content',()=>{
  const root=mkdtempSync(join(tmpdir(),'edge-experience-projection-'));
  try{
    const dir=join(root,'source');mkdirSync(dir);
    const digest=sha(homepage),body=join(dir,'retained.html'),receiptPath=join(dir,'native.receipt.json');
    writeFileSync(body,homepage);writeFileSync(receiptPath,JSON.stringify(receipt(homepage)));
    const source={digest,body,receipt:receiptPath};
    const first=materializeSourceExperience({source,root});
    expect(first.status).toBe('materialized-review-candidates');
    expect(first.candidates).toBe(4);
    expect(first.authority).toBe('none');
    expect(sha(readFileSync(fileURLToPath(first.uri)))).toBe(first.digest);
    expect(materializeSourceExperience({source,root})).toEqual(first);
    expect(readdirSync(join(root,'experience-projections'))).toEqual([first.digest+'.yaff']);
    writeFileSync(body,'changed without native receipt');
    expect(()=>materializeSourceExperience({source,root})).toThrow();
    expect(sha(readFileSync(fileURLToPath(first.uri)))).toBe(first.digest);
  }finally{rmSync(root,{recursive:true,force:true});}
});
