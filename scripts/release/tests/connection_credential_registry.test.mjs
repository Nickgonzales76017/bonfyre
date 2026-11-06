import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {mergeConnectionCredentialRegistries} from '../connection_credential_registry.mjs';
const root=resolve(import.meta.dir,'../../..');
const base=readFileSync(join(root,'cmd/BonfyreCredentials/provider-credentials.json'));
const supplement=readFileSync(join(root,'integrations/browser/onboarding/connection-credential-supplement.json'));
test('connection credential supplement compiles without replacing canonical declarations',()=>{
 const merged=JSON.parse(mergeConnectionCredentialRegistries(base,supplement)),baseRows=JSON.parse(base),extra=JSON.parse(supplement);
 expect(merged.slice(0,baseRows.length)).toEqual(baseRows);expect(merged.length).toBe(baseRows.length+extra.length);
 expect(new Set(merged.map(x=>x.provider)).size).toBe(merged.length);
 for(const id of ['firecrawl','openrouter','groq','cohere']){
  const row=merged.find(x=>x.provider===id);expect(row).toBeTruthy();expect(row.fields.map(x=>x.field)).toContain('api_key');expect(row.verify.url).toMatch(/^https:\/\//);expect(row.verify.expect).toBe(200);
 }
});
test('supplement duplication is refused',()=>{
 const one=JSON.stringify([JSON.parse(base)[0]]);expect(()=>mergeConnectionCredentialRegistries(base,one)).toThrow(/duplicates/);
});
