import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {normalize,compile,classifyWorkflowTitle,expansionQueries,WORKFLOW_PATTERNS} from '../tools/operator_supply.mjs';

const row=(extra={})=>({schema:'bonfyre-operator-observation.v1',source:'fixture',source_kind:'operator_profile',
  identity:'fixture:operator:one',profile_url:'https://operators.example.test/one',display_name:'Operator One',
  locality:'Dallas',region:'TX',country:'US',workflow_families:['marketplace_listing'],capabilities:['listing'],
  languages:['en'],observed_at:'2026-10-05T00:00:00Z',contact_mode:'profile',...extra});

test('one title can project into several reusable workflow families',()=>{
  const x=classifyWorkflowTitle('WordPress SEO and digital marketing website repair with Facebook ads');
  expect(x).toContain('website_build');expect(x).toContain('website_repair');expect(x).toContain('seo');
  expect(x).toContain('digital_marketing');expect(x).toContain('paid_ads');
  expect(WORKFLOW_PATTERNS.length).toBeGreaterThanOrEqual(20);
});
test('actual website editing and developer titles reach the appropriate workflow',()=>{
  expect(classifyWorkflowTitle('Looking for Website Editor/Designer')).toContain('website_repair');
  expect(classifyWorkflowTitle('Website Developer')).toContain('website_build');
  expect(classifyWorkflowTitle('Website updates and maintenance')).toContain('website_repair');
});
test('operator standing is bounded evidence, not an inferred capability',()=>{
  const x=normalize(row({standing:{platforms:['facebook_marketplace','ebay'],account_rating:4.8,multi_listing_capacity:true}}));
  expect(x.standing).toEqual({platforms:['facebook_marketplace','ebay'],account_rating:4.8,multi_listing_capacity:true});
  expect(()=>normalize(row({standing:{platforms:[{}]}}))).toThrow('invalid standing value');
  expect(()=>normalize(row({standing:Object.fromEntries(Array.from({length:33},(_,i)=>['k'+i,true]))}))).toThrow('invalid standing');
});
test('English and Spanish fanout separates supply, demand and market-rate searches',()=>{
  const q=expansionQueries({cities:['Dallas, TX'],workflows:['appointment_setting'],languages:['en','es'],
    sources:['indeed','craigslist_supply','craigslist_demand']});
  expect(q).toHaveLength(6);
  expect(q.find(x=>x.language==='es'&&x.source==='craigslist_supply').query).toContain('concertador de citas');
  expect(q.find(x=>x.source==='indeed').side).toBe('market_rate');
  expect(q.find(x=>x.source==='craigslist_demand').side).toBe('demand');
  expect(q.every(x=>x.cannot_infer.length===2)).toBe(true);
});
test('compile emits distinct profile evidence even when several people share a directory URL',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'operator-supply-'));try{
    const input=join(dir,'in.jsonl'),channels=join(dir,'channels.jsonl'),profiles=join(dir,'profiles.jsonl');
    const a=row({identity:'fixture:one'}),b=row({identity:'fixture:two',display_name:'Operator Two'});
    writeFileSync(input,[a,b].map(JSON.stringify).join('\n')+'\n');
    const result=await compile(input,{channels,profiles});
    expect(result.admitted).toBe(2);expect(result.identities).toBe(2);
    expect(readFileSync(profiles,'utf8').trim().split('\n')).toHaveLength(2);
    expect(readFileSync(channels,'utf8').trim().split('\n')).toHaveLength(2);
  }finally{rmSync(dir,{recursive:true,force:true})}
});
