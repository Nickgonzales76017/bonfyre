#!/usr/bin/env bun
// Reviewed recurring-life pack: native CMS reference profiles and ProgramFabric templates.
// These are declarations, not merchant onboarding, subscriptions, charges, or fulfillment.
import {readFileSync, mkdtempSync, copyFileSync, rmSync, existsSync} from 'node:fs';
import {dirname, resolve, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tmpdir, homedir} from 'node:os';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const repo=resolve(dirname(fileURLToPath(import.meta.url)), '..');
const profileRows=[
 ['threadbeast','ThreadBeast','wardrobe_keep_all','style','https://www.threadbeast.com/how-it-works','Monthly personalized streetwear purchased as a whole box; preview and swaps before shipment. No traditional returns; the source describes a seven-day cancellation window after receipt. Plans start at $60.'],
 ['stitch-fix','Stitch Fix','try_before_buy','style','https://support.stitchfix.com/support/solutions/articles/153000250371-how-the-fix-experience-works','Optional scheduled or on-demand styling. The ordinary $20 styling fee credits items kept from that Fix; it is not the box price. Promotional first-Fix terms are excluded.'],
 ['crispy-club','Crispy Club','discovery_box','food','https://crispyclub.co/products/subscription-box','Three selected chili crisps per shipment. The page lists $49.99, a no-repeat promise and shipment-preparation billing, but also shows Sold out. Availability is not inferred.'],
 ['lata','Lata','discovery_box','food','https://lata.shop/products/monthly-discovery-box','Four curated conservas per monthly Discovery Box, listed at $49.99 with shipping calculated separately. Prepaid gifting options also exist.'],
 ['fuego-box','Fuego Box','discovery_box','food','https://www.fuegobox.com/','User-supplied hot-sauce subscription example. Current first-party terms could not be retrieved; no price, stock, or availability is asserted.'],
 ['bokksu','Bokksu','discovery_box','food','https://bokksu.com/products/classic-bokksu','Japanese snack-and-tea curation with a culture guide. The one-month option lists $39.99 plus shipping. Allergen information does not establish individually safe customization.'],
 ['bottomless','Bottomless','replenishment','food','https://www.bottomless.com/kuma','Coffee replenishment driven by a connected scale rather than a fixed calendar, with a pre-order notice and controls to postpone or change orders. Current membership price is not captured.'],
 ['rinse','Rinse','recurring_service','home','https://www.rinse.com/faqs/','Rinse Repeat prepays laundry bags; Rinse Go is a fee-waiver membership. Entitlements differ. Address coverage and current rollover rules must be checked; old rollover assumptions are not retained.'],
 ['filtereasy','FilterEasy','replenishment','home','https://filtereasy.com/product/20x24x1','Filter subscriptions require exact dimensions, quantity and compatible specification. A single-size product listing is not a universal filter price.'],
 ['barkbox','BarkBox','discovery_box','pets','https://www.barkbox.com/faq/','Themed dog toys and treats with plan-dependent preferences and commitments. Month-to-month and multi-month plans differ; no universal checkout price is inferred.']
];
const templateRows=[
 ['heat-passport','Heat Passport','discovery_box','Three condiments with a heat ceiling and repeat controls.','heat ceiling, flavors, ingredient exclusions'],
 ['conserva-supper-club','Conserva Supper Club','discovery_box','A tinned-fish tasting with pairings and optional shared supper.','species exclusions, spice preference, portions'],
 ['japan-snack-correspondence','Japan Snack Correspondence','discovery_box','Snack and tea curation with maker stories and an optional tasting circle.','sweet/savory balance, exclusions, recipient'],
 ['coffee-at-the-right-time','Coffee At The Right Time','replenishment','Reorder from measured or explicitly reported depletion.','grind, roast, stock, daily use, lead time'],
 ['weeknight-rescue','Weeknight Rescue','recurring_service','Meal preparation with an explicit active-cooking-time ceiling.','servings, dietary requirements, minutes, away dates'],
 ['two-person-pantry','Two-Person Pantry','replenishment','Right-size staples to household consumption.','household size, stock, usage, substitution'],
 ['hosting-without-the-errands','Hosting Without The Errands','hybrid','Ingredients, flowers and optional cleanup under separate sellers.','guests, occasion, menu, area, seller split'],
 ['neighborhood-bakery-round','Neighborhood Bakery Round','discovery_box','Rotating bakery pickup with a real cutoff and collection window.','pickup point, portions, exclusions'],
 ['streetwear-rotation','Streetwear Rotation','wardrobe_keep_all','Previewable clothing purchased as a complete style drop.','sizes, style, swaps, exchange policy'],
 ['try-on-wardrobe','Try-On Wardrobe','try_before_buy','Item-level keep/return decisions with separately credited styling.','sizes, item budgets, fee, return window'],
 ['quarterly-closet-reset','Quarterly Closet Reset','hybrid','Seasonal edit combining care, alterations and optional new pieces.','wardrobe, alterations, area, season'],
 ['socks-when-worn-out','Socks When Worn Out','replenishment','Replace basics from a wear signal rather than novelty accumulation.','size, material, wear signal, quantity'],
 ['laundry-off-my-list','Laundry Off My List','recurring_service','Pickup and laundry with quota distinguished from fee membership.','bags, turnaround, pickup window, area'],
 ['sunday-home-reset','Sunday Home Reset','recurring_service','Repeated cleaning with named tasks and completion evidence.','rooms, scope, pets, access, away dates'],
 ['filters-that-fit','Filters That Fit','replenishment','The correct filter dimensions and replacement rhythm.','sizes, specification, quantity, interval'],
 ['home-steward-visit','Home Steward Visit','recurring_service','A bounded maintenance visit for recurring household tasks.','tasks, minutes, property permission, area'],
 ['car-ready-monday','Car Ready Monday','recurring_service','Car care with a defined scope and actual booking capacity.','vehicle, scope, location, interval'],
 ['lawn-and-garden-rhythm','Lawn And Garden Rhythm','recurring_service','Season-aware visits with weather rescheduling.','area, scope, season, weather policy'],
 ['pet-joy-without-the-pile','Pet Joy Without The Pile','discovery_box','Pet toys and treats with skip controls for unused stock.','size, play style, ingredients, unused items'],
 ['pet-pantry-refill','Pet Pantry Refill','replenishment','Compatible pet food replenished from use and lead time.','food identity, consumption, stock, lead time'],
 ['walks-covered','Walks Covered','recurring_service','Recurring dog walks with verified area and capacity.','pet needs, duration, time window, area'],
 ['grooming-rhythm','Grooming Rhythm','recurring_service','Grooming capacity on a pet-specific interval.','size, coat, interval, area'],
 ['flowers-for-someone','Flowers For Someone','finite_gift','A finite flower series that never turns the recipient into a payer.','recipient acceptance, deliveries, dates, preferences'],
 ['care-package-seasons','Care Package Seasons','finite_gift','A finite preference-aware care-package series.','recipient, cycle count, exclusions, message'],
 ['host-gift-reserve','Host Gift Reserve','access_pass','Gift-selection service with actual goods priced separately.','entitlements, redemptions, goods budget'],
 ['the-local-saturday','The Local Saturday','access_pass','A bookable rotation of classes and cultural outings.','interests, distance, time, guest policy'],
 ['museum-and-makers-circle','Museum And Makers Circle','access_pass','Social museum and maker visits including free alternatives.','interests, free options, capacity, guests'],
 ['hobby-bench','Hobby Bench','discovery_box','Skill-building kits and project challenges.','skill, tools already owned, project preferences'],
 ['reading-and-walking-club','Reading And Walking Club','access_pass','Social reading and walking using library access where useful.','reading, library access, area, schedule'],
 ['date-night-rotation','Date Night Rotation','hybrid','Previewed date-night components with separate venue and goods terms.','shared preferences, date, budget, confirmations'],
 ['office-commons','Office Commons','hybrid','Team pantry and consumables with approved budgets and separate suppliers.','headcount, budget, delivery, supplier split'],
 ['remote-team-ritual','Remote Team Ritual','hybrid','Opt-in distributed tastings or hobby sessions.','members, regions, consent, event time'],
 ['resident-welcome-season','Resident Welcome Season','finite_gift','Property-sponsored finite gifting, separate from resident enrollment.','sponsor, acceptance, cycles, property scope'],
 ['room-parent-supply-round','Room Parent Supply Round','replenishment','Voluntary class supplies from an actual school or teacher request.','approved need, donor budget, relationship, inventory'],
 ['community-producer-club','Community Producer Club','discovery_box','Local producer batches with cohort size and pickup point.','makers, minimum cohort, pickup, exclusions'],
 ['away-week-coordination','Away-Week Coordination','hybrid','Travel adjustments remain separate requests for each provider.','away dates, affected services, controls, receipts']
];
const owners=['CMS','ActorGraph','AuthorityGraph','InformationPolicy','CommitmentGraph','ProtocolMachine','WorkGraph','Queue','Finance','Meter','ForeignTwin','WebAddress'];
const profiles=profileRows.map(([id,name,pattern,grouping,url,summary])=>({ext_id:`recurring-provider-${id}`,name,url,says:JSON.stringify({summary,checked_on:'2026-10-03',source_status:id==='fuego-box'?'user_supplied_current_unverified':'first_party_page_retrieved'}),estate:'recurring-commerce',grouping,functions:pattern,related:url,cannot_infer:'Independent reference profile: not affiliated, merchant-claimed, connected, in stock, or purchasable through Aurekai.',status:'published'}));
const templates=templateRows.map(([id,name,archetype,promise,setup],i)=>({template_id:`recurring-${id}`,name,archetype,ordinal:1000+i,subject_kinds:['person','household','institution','project'],semantic_slots:['sources','work','evidence','community'],capability_families:['source_intake','identity','workflow','capacity_market','reporting'],owner_bindings:owners,funding_candidates:[],surface_policy:'bind_into_existing_world',page_policy:'no_dedicated_page_required',description:`${promise} Setup: ${setup}. Part of the recurring-life authoring pack; product and seller terms must be supplied before activation.`,cannot_infer:'Authoring template, not an operating merchant, paid subscription, fulfillment capability, or brand affiliation.',status:'published'}));
if(new Set(templates.map(t=>t.template_id)).size!==36)throw Error('Duplicate template identity');
const mode=process.argv[2]??'--check';
if(!['--check','--apply'].includes(mode))throw Error('Usage: recurring-commerce-load.mjs --check|--apply [existing-cms.db]');
const report={profiles:profiles.length,templates:templates.length,patterns:new Set(templates.map(t=>t.archetype)).size,created:0,unchanged:0,conflicts:[],committed:false,deployed:false,billing_connected:false};
if(mode==='--apply'){
 const db=resolve(process.argv[3]??join(homedir(),'.bonfyre/estate-local/cms.db'));
 const binary=join(homedir(),'.bonfyre/bin/bonfyre-cms');
 if(!existsSync(db)||!existsSync(binary))throw Error('Existing native CMS and database required');
 const schemas=mkdtempSync(join(tmpdir(),'aurekai-recurring-schemas-'));
 try{
  for(const type of ['external_resource','program_template'])copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',`${type}.json`),join(schemas,`${type}.json`));
  const call=args=>{const r=spawnSync(binary,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',timeout:30000,maxBuffer:20000000});if(r.status!==0)throw Error(`CMS failed: ${r.stderr}`);const result=JSON.parse(r.stdout);if(result.error)throw Error(JSON.stringify(result));return result;};
  for(const [type,key,rows]of [['external_resource','ext_id',profiles],['program_template','template_id',templates]]){
   const list=call(['entry','list',type,'--ns','root','--limit','10000']).data;
   if(!Array.isArray(list)||list.length>=10000)throw Error('List incomplete; refusing ambiguous upsert');
   const existing=new Map(list.map(x=>[x.attributes[key],x]));
   for(const row of rows){const old=existing.get(row[key]);if(old){if(Object.entries(row).every(([k,v])=>JSON.stringify(old.attributes[k])===JSON.stringify(v)))report.unchanged++;else report.conflicts.push(row[key]);continue;}
    const result=call(['entry','create',type,'--ns','root',JSON.stringify(row)]);
    if(!result.data?.id&&!result.id)throw Error(`Missing native create identity: ${JSON.stringify(result)}`);
    report.created++;
   }
  }
  report.database=db;
 }finally{rmSync(schemas,{recursive:true,force:true});}
}
report.pack_sha256=createHash('sha256').update(JSON.stringify({profiles,templates})).digest('hex');
console.log(JSON.stringify(report,null,2));
if(report.conflicts.length)process.exitCode=2;
