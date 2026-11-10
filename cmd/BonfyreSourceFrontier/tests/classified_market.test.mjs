import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {parseIndex,replayIndexes,marketSelection,selectEnrichmentRows,enrichListings,routeListings} from '../tools/classified_market.mjs';
const meta={city:'Dallas',slug:'dallas',cat:'mar',side:'demand',family:'marketing_jobs',url:'https://www.craigslist.org/search/area/dallas?cat=mar',digest:'fixture'};
const listing=(host='www.craigslist.org')=>`<li class="cl-static-search-result" title="Website editor"><a href="https://${host}/view/d/editor/abc"><div class="title">Website editor</div><div class="location">Dallas</div></a></li>`;
const page=(category='mar',rows=listing())=>`<head><link href='https://www.craigslist.org/search/area/dallas?cat=${category}' rel='canonical'></head><body>${rows}</body>`;
test('routing revalidates both parties, ignores edited body text and keeps commercial acceptance unresolved',()=>{
  const dir=mkdtempSync(join(tmpdir(),'classified-routing-'));
  try{
    const keep=(name,url,html)=>{
      const body_path=join(dir,name+'.body'),receipt_path=join(dir,name+'.json');writeFileSync(body_path,html);
      writeFileSync(receipt_path,JSON.stringify({source_url:url,body_path,content_sha256:createHash('sha256').update(html).digest('hex'),policy_decision:'allow',http_status:200,observed_at:'2026-10-01T00:00:00Z'}));
      return receipt_path;
    };
    const address='https://www.craigslist.org/view/d/item/one',receipt_path=keep('seller',address,'<section id="postingbody">CNC mill. Pickup only.</section>');
    const rule={id:'fixture',name:'Fixture buyer',source_url:'https://example.com/referrals',terms_stated:'Fee after purchase',terms_excerpt:'Fee after purchase',match_any:['cnc','mill','C++'],receipt_path:keep('buyer','https://example.com/referrals','<p>Fee after <b>purchase</b></p>')};
    const file=join(dir,'input.jsonl'),row={address,receipt_path,title:'Edited dental title',body:'dental chair',side:'supply'};
    writeFileSync(file,[row,row,{...row,address:address+'wrong'}, {...row,address:address+'failed',error:'HTTP 403'}].map(JSON.stringify).join('\n'));
    const result=routeListings({evidence:[file],providers:[rule],out:join(dir,'out')});
    expect(result.network_requests).toBe(0);expect(result.distinct_verified_sources).toBe(1);expect(result.refused_rows).toBe(2);
    const candidate=JSON.parse(readFileSync(result.output,'utf8'));
    expect(candidate.matched_body_terms).toEqual(['cnc','mill']);expect(candidate.source.observed_at).toBe('2026-10-01T00:00:00Z');
    expect(candidate.state).toBe('needs_commercial_review');expect(candidate.unresolved).toContain('Owner permission for an introduction');
    const groupedRule={...rule,match_groups:[['pickup','shipping'],['mill']]};
    const grouped=routeListings({evidence:[file],providers:[groupedRule],out:join(dir,'grouped')});
    expect(grouped.candidate_associations).toBe(1);
    expect(JSON.parse(readFileSync(grouped.output,'utf8')).matched_body_groups).toEqual([['pickup'],['mill']]);
    for(const groups of [[['pickup'],['bulk']],[['mil']],[['dental']]]){
      const filtered=routeListings({evidence:[file],providers:[{...rule,match_groups:groups}],out:join(dir,'filtered')});
      expect(filtered.candidate_associations).toBe(0);
    }
    for(const groups of [[],[[]],['bulk'],[[7]],null]){
      expect(()=>routeListings({evidence:[file],providers:[{...rule,match_groups:groups}],out:join(dir,'invalid')})).toThrow('term groups');
    }
    const prefix=join(dir,'installed owner with spaces'),owner=join(import.meta.dir,'..');
    const installed=spawnSync('make',['install-classified-market','MANAGED_PREFIX='+prefix],{cwd:owner,encoding:'utf8'});
    expect(installed.status).toBe(0);
    const rulesFile=join(dir,'rules.json'),ledger=join(dir,'unused.db');writeFileSync(rulesFile,JSON.stringify([{...groupedRule,exclude_any:['pickup only']}]));
    const native=spawnSync(join(prefix,'bin/bonfyre-source-frontier'),['classified-market','route','--evidence',file,'--providers',rulesFile,'--out',join(dir,'native out'),'--db',ledger],{encoding:'utf8',env:{...process.env,BONFYRE_BUN_BINARY:process.execPath,BONFYRE_CLASSIFIED_MARKET:join(prefix,'lib/source-frontier/classified_market.mjs')}});
    expect(native.status).toBe(0);expect(JSON.parse(native.stdout).candidate_associations).toBe(0);expect(JSON.parse(native.stdout).withheld_associations).toBe(1);expect(existsSync(ledger)).toBe(false);
    expect(()=>routeListings({evidence:[file],providers:[{...rule,terms_excerpt:'Guaranteed payment'}],out:join(dir,'bad')})).toThrow('excerpt missing');
    expect(()=>routeListings({evidence:[file],providers:[{...rule,source_url:'https://example.net/referrals'}],out:join(dir,'bad')})).toThrow('source or policy');
    const providerReceipt=JSON.parse(readFileSync(rule.receipt_path));writeFileSync(providerReceipt.body_path,'Changed terms');
    expect(()=>routeListings({evidence:[file],providers:[rule],out:join(dir,'bad')})).toThrow('provider body digest');
    expect(()=>routeListings({evidence:[result.output],providers:[rule],out:join(dir,'out')})).toThrow('separate');
  }finally{rmSync(dir,{recursive:true,force:true})}
});
test('seller restrictions are withheld from retained text and remain auditable separately from observation failures',()=>{
  const dir=mkdtempSync(join(tmpdir(),'classified-restrictions-'));
  try{
    const keep=(name,url,html)=>{
      const body_path=join(dir,name+'.body'),receipt_path=join(dir,name+'.json');
      writeFileSync(body_path,html);writeFileSync(receipt_path,JSON.stringify({source_url:url,body_path,content_sha256:createHash('sha256').update(html).digest('hex'),policy_decision:'allow',http_status:200,observed_at:'2026-10-01T00:00:00Z'}));
      return receipt_path;
    };
    const rows=[
      ['declines',"CNC lathe. DON'T NEED HELP SELLING THIS ITEM",'CNC lathe'],
      ['ordinary','CNC lathe. Loading help available.',"CNC lathe. Don't need help selling this item"],
      ['boundary','CNC lathe. No brokerside loading available.','CNC lathe']
    ].map(([id,body,edited])=>{
      const address='https://www.craigslist.org/view/d/machine/'+id;
      return {address,side:'supply',title:'CNC lathe',body:edited,receipt_path:keep(id,address,'<section id="postingbody">'+body+'</section>')};
    });
    const evidence=join(dir,'input.jsonl');writeFileSync(evidence,rows.map(JSON.stringify).join('\n'));
    const rule={id:'finder',name:'Fixture buyer',source_url:'https://example.com/referrals',receipt_path:keep('buyer','https://example.com/referrals','<p>Fee after purchase</p>'),terms_stated:'Fee after purchase',terms_excerpt:'Fee after purchase',match_any:['lathe'],exclude_any:["don't need help selling this item",'no brokers']};
    const result=routeListings({evidence:[evidence],providers:[rule],out:join(dir,'out')});
    expect(result.matched_associations).toBe(3);expect(result.candidate_associations).toBe(2);
    expect(result.withheld_associations).toBe(1);expect(result.refused_rows).toBe(0);expect(result.network_requests).toBe(0);
    const held=JSON.parse(readFileSync(result.withheld_output,'utf8'));
    expect(held.source.address).toBe(rows[0].address);expect(held.source.receipt_path).toBe(rows[0].receipt_path);
    expect(held.state).toBe('withheld_by_provider_rule');expect(held.excluded_body_terms).toEqual(["don't need help selling this item"]);
    const accepted=readFileSync(result.output,'utf8').trim().split('\n').map(JSON.parse);
    expect(accepted.map(x=>x.source.address)).toEqual(rows.slice(1).map(x=>x.address));
    const resale=routeListings({evidence:[evidence],providers:[{...rule,transaction_model:'purchase_resale',exclude_any:['no dealers','no resellers']}],out:join(dir,'resale')});
    expect(resale.candidate_associations).toBe(3);expect(resale.withheld_associations).toBe(0);
    const purchaseCandidate=JSON.parse(readFileSync(resale.output,'utf8').split('\n')[0]);
    expect(purchaseCandidate.transaction_model).toBe('purchase_resale');
    expect(purchaseCandidate.unresolved).toContain('Buyer bid for the exact lot and acceptance conditions');
    expect(purchaseCandidate.unresolved).not.toContain('Written fee basis, registration and payment trigger');
    expect(()=>routeListings({evidence:[evidence],providers:[{...rule,transaction_model:'settled'}],out:join(dir,'invalid-model')})).toThrow('transaction model');
    for(const exclude_any of [[],null,[3],[''],['x'.repeat(101)]])expect(()=>routeListings({evidence:[evidence],providers:[{...rule,exclude_any}],out:join(dir,'bad')})).toThrow('exclusion terms');
    expect(()=>routeListings({evidence:[result.withheld_output],providers:[rule],out:join(dir,'out')})).toThrow('separate');
  }finally{rmSync(dir,{recursive:true,force:true})}
});
test('enrichment balances markets and retains same-title alternatives without claiming common owners',()=>{
  const row=(id,market,title,side='demand')=>({address:'https://www.craigslist.org/view/d/item/'+id,market,title,side});
  const rows=[row('a','dallas','Buy servers'),row('b','dallas','Buy servers'),row('c','dallas','Buy Cisco'),row('d','houston','Buy RAM'),row('e','houston','Sell servers','supply')];
  const plan=selectEnrichmentRows(rows,{side:'demand',query:'servers,cisco,ram',limit:2});
  expect(plan.matching).toBe(4);expect(plan.distinct_title_groups).toBe(3);
  expect(plan.selected.map(x=>x.market)).toEqual(['dallas','houston']);
  expect(plan.selected[0].same_title_addresses).toHaveLength(2);
  expect(()=>selectEnrichmentRows([{...rows[0],address:'https://notcraigslist.org/a'}])).toThrow('public Craigslist');
  expect(()=>selectEnrichmentRows(rows,{limit:0})).toThrow('limit');
});
test('full-post enrichment preserves refusals, checks digests and distinguishes repeated bodies',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'classified-enrich-'));
  try{
    const rows=['ok','repeat','blocked','drift','gone'].map((title,i)=>({title,market:'dallas',side:'supply',address:'https://www.craigslist.org/view/d/item/'+title}));
    const observer=async(_project,_foreign,_twin,address)=>{
      const name=address.split('/').at(-1),bodyPath=join(dir,name+'.body');
      const html=name==='gone'?'<body>removed</body>':'<section id="postingbody">32 sealed kits<br>Buyer pickup</section><p>post id: '+(name==='repeat'?'2':'1')+'</p>';
      writeFileSync(bodyPath,html);
      return {bodyPath,receiptPath:join(dir,name+'.json'),receipt:{source_url:address,policy_decision:name==='blocked'?'deny':'allow',http_status:200,content_sha256:name==='drift'?'bad':createHash('sha256').update(html).digest('hex'),observed_at:'2026-10-08T00:00:00Z'}};
    };
    const result=await enrichListings({rows,out:join(dir,'out'),observer});
    expect(result.observed).toBe(2);expect(result.failed).toBe(3);
    expect(result.distinct_observed_post_ids).toBe(2);expect(result.distinct_observed_bodies).toBe(1);
    const saved=readFileSync(result.output,'utf8').trim().split('\n').map(JSON.parse);
    expect(saved[1].same_body_as).toBe(rows[0].address);
    expect(saved[2].error).toContain('refused');expect(saved[3].error).toContain('digest');expect(saved[4].error).toContain('no posting body');
    expect(saved[0].body).toContain('Buyer pickup');
  }finally{rmSync(dir,{recursive:true,force:true})}
});
test('retained enrichment never fetches in offline mode and refuses cross-source receipts or changed bodies',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'classified-retained-'));
  try{
    const rows=['ok','wrong-source','drift','missing'].map(title=>({title,market:'dallas',side:'supply',address:'https://www.craigslist.org/view/d/item/'+title}));
    const evidence=join(dir,'evidence.jsonl'),saved=[];
    for(const row of rows.slice(0,3)){
      const body_path=join(dir,row.title+'.body'),receipt_path=join(dir,row.title+'.json');
      const html='<section id="postingbody">Machine for sale</section>';
      writeFileSync(body_path,html);
      writeFileSync(receipt_path,JSON.stringify({source_url:row.title==='wrong-source'?rows[0].address:row.address,policy_decision:'allow',http_status:200,body_path,content_sha256:row.title==='drift'?'bad':createHash('sha256').update(html).digest('hex'),observed_at:'2026-10-01T00:00:00Z'}));
      saved.push({...row,receipt_path});
    }
    writeFileSync(evidence,saved.map(JSON.stringify).join('\n'));
    const result=await enrichListings({rows,out:join(dir,'out'),evidence:[evidence],offline:true,observer:()=>{throw Error('network must not run')}});
    expect(result.network_observations).toBe(0);expect(result.retained_observations_reused).toBe(3);
    expect(result.observed).toBe(1);expect(result.failed).toBe(3);
    const output=readFileSync(result.output,'utf8').trim().split('\n').map(JSON.parse);
    expect(output[0].observed_at).toBe('2026-10-01T00:00:00Z');
    expect(output[1].error).toContain('source mismatch');expect(output[2].error).toContain('digest');expect(output[3].error).toContain('offline');
    await expect(enrichListings({rows,out:dir,evidence:[join(dir,'enriched-listings.jsonl')]})).rejects.toThrow('separate');
  }finally{rmSync(dir,{recursive:true,force:true})}
});
test('wholesale searches wanted goods and inventory without turning displayed prices into quotes',()=>{
  const selection=marketSelection({profile:'wholesale',markets:'dallas,houston,dallas'});
  expect(selection.metros.map(x=>x[1])).toEqual(['dallas','houston']);
  expect(selection.categories).toContainEqual(['waa','demand','goods_wanted']);
  expect(()=>marketSelection({profile:'wholesle'})).toThrow('profile');
  expect(()=>marketSelection({markets:'unknown'})).toThrow('unknown market');
  const first=listing().replace('<div class="location">Dallas</div>','<div class="price">$1</div>');
  const second=listing().replace('/abc','/def').replace('Website editor','Laptop lot');
  const rows=parseIndex(page('sya',first+second),{...meta,cat:'sya',side:'supply',family:'computers',url:meta.url.replace('cat=mar','cat=sya')});
  expect(rows).toHaveLength(2);
  expect(rows[0].location).toBe('');expect(rows[0].asking_price_text).toBe('$1');
  expect(rows[0].market_kind).toBe('goods');expect(rows[0].side).toBe('supply');
  expect(rows[1].location).toBe('Dallas');expect(rows[1].asking_price_text).toBe('');
  expect(rows[0].truth).toBe('observed_index_title_only');
});
test('HTTP-success fallback to for-sale cannot become marketing demand',()=>{
  expect(()=>parseIndex(page('sss'),meta)).toThrow('identity mismatch');
  expect(()=>parseIndex(listing(),meta)).toThrow('canonical identity');
  expect(()=>parseIndex(page('mar').replace('</head>',"<link rel='canonical' href='https://www.craigslist.org/search/area/dallas?cat=sss'></head>"),meta)).toThrow('ambiguous');
  const rows=parseIndex(page(),meta);expect(rows).toHaveLength(1);expect(rows[0].category).toBe('mar');
});
test('preserves title classification but rejects lookalike listing hosts',()=>{
  expect(parseIndex(page('mar',listing('notcraigslist.org')),meta)).toHaveLength(0);
  expect(parseIndex(page('mar',listing('dallas.craigslist.org')),meta)[0].workflows).toContain('website_repair');
  expect(()=>parseIndex(page().replace('/area/dallas?','/area/houston?'),meta)).toThrow('identity mismatch');
});
test('offline replay separates identity fallback and digest drift without rewriting evidence',()=>{
  const dir=mkdtempSync(join(tmpdir(),'classified-replay-'));
  try{
    const input=join(dir,'input.json'),out=join(dir,'out'),items=[];
    for(const [name,html,cat] of [['good',page(),'mar'],['fallback',page('sss'),'mkt'],['drift',page(),'mar']]){
      const body_path=join(dir,name);writeFileSync(body_path,html);
      items.push({...meta,cat,url:meta.url.replace('cat=mar','cat='+cat),body_path,digest:name==='drift'?'bad':createHash('sha256').update(html).digest('hex')});
    }
    const original=JSON.stringify(items);writeFileSync(input,original);
    expect(()=>replayIndexes({indexes:input,out:dir})).toThrow('separate');
    const result=replayIndexes({indexes:input,out});
    expect(result.network_requests).toBe(0);expect(result.index_pages).toBe(1);expect(result.refused_indexes).toBe(2);expect(result.demand_listings).toBe(1);
    expect(readFileSync(input,'utf8')).toBe(original);
    const checked=JSON.parse(readFileSync(result.outputs.indexFile,'utf8'));
    expect(checked[1].error).toContain('identity mismatch');expect(checked[2].error).toContain('digest mismatch');
    expect(readFileSync(result.outputs.rowFile,'utf8').trim().split('\n')).toHaveLength(1);
  }finally{rmSync(dir,{recursive:true,force:true})}
});
