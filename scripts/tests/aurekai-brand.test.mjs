import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {AUREKAI_VOICE,channelFor,isAurekaiOwnedBrandHost} from '../../branding/voice.mjs';

const root=fileURLToPath(new URL('../../',import.meta.url));
const src=path=>readFileSync(join(root,path),'utf8');
const signature=AUREKAI_VOICE.signature;

test('one corporate voice retains the exact four approved lines',()=>{
 assert.equal(AUREKAI_VOICE.campaign,'Every brand is built on software. The ones everyone talks about are built on Aurekai.');
 assert.equal(AUREKAI_VOICE.hero,'Every brand runs on software. The ones that lead run on Aurekai.');
 assert.equal(AUREKAI_VOICE.authority,'Software runs every business. Aurekai powers the ones people remember.');
 assert.equal(signature,'Built on software. Driven by Aurekai.');
 assert.match(AUREKAI_VOICE.distinction,/Bonfÿre/);
});
test('four apex messages and the independent-brand boundary are explicit',()=>{
 const headings={work:'Build the business people remember.',life:'Make your corner of the internet your own.',
   live:'Give your work more places to go.',shop:'Build the business behind every product.'};
 for(const [apex,title] of Object.entries(headings)){
   assert.equal(channelFor('aurekai.'+apex)?.title,title);
   assert.equal(isAurekaiOwnedBrandHost('aurekai.'+apex),true);
 }
 for(const host of ['customer.aurekai.work','aurekai.work.evil.net','customer.example','nickgonzales76017.github.io'])
   assert.equal(isAurekaiOwnedBrandHost(host),false,host+' must not inherit corporate branding');
 assert.equal(channelFor('customer.aurekai.work'),null);
});
test('corporate home and social-card image show the operating business',()=>{
 const h=src('site/index.html');
 assert.match(h,/Every brand runs on software\.<br><em>The ones that lead run on Aurekai\.<\/em>/);
 assert.match(h,/Bonfÿre is the native engine/);
 assert.ok(h.includes(signature));
 assert.match(h,/text-wrap:balance/);
 assert.match(h,/og:image:alt/);
 assert.match(h,/studio-product\.png/);
});
test('product, application, research, editorial and technical destinations retain distinct voices',()=>{
 const placements=[
  ['site/explore.html','Build the business<br><em>people remember.</em>'],
  ['site/discover/index.html','See what software<br><em>makes possible.</em>'],
  ['site/market.html','The research behind<br><em>what we build next.</em>'],
  ['site/docs.html','Bonfÿre · The native software behind Aurekai'],
  ['site/fieldwork/index.html',signature],
  ['site/series/index.html',signature],
  ['editorial/templates/journal-index.html',signature],
  ['site/matrix/index.html','Bonfÿre: the native software behind Aurekai.']
 ];
 for(const [path,copy] of placements)assert.ok(src(path).includes(copy),path+' missing audited copy');
 for(const path of ['site/index.html','site/explore.html','site/discover/index.html'])
   assert.match(src(path),/property="og:image"/,path+' missing social-preview media');
 assert.ok(!src('site/docs.html').includes('The ones that lead run on Aurekai.'),'technology stays distinct from campaign');
});
test('Work Samples receives the brand; independent sites keep their names',()=>{
 const content=JSON.parse(src('packs/aurekai-work-samples/site/content.json'));
 assert.equal(content.business.name,'Aurekai');
 assert.match(content.business.page_title,/Built on software\. Driven by Aurekai/);
 const hero=content.sections.find(x=>x.kind==='hero');
 assert.equal(hero.headline,'Built on software. *Driven by Aurekai.*');
 assert.match(content.footer.about,/Independent customer brands remain their own/);
 assert.ok(content.navigation.some(x=>x.label==='About Aurekai'&&x.href.endsWith('/bonfyre/about/')));
 assert.match(src('components/BonfyreServiceSite/README.md'),/independent customer sites retain their own names and branding/);
});
test('GitHub, model and distribution documents distinguish business and native engine',()=>{
 for(const path of ['README.md','Aurekai/README.md','docs/aurekai-bonfyre-model.md','branding/social-copy.md'])
   assert.ok(src(path).includes(signature),path+' missing approved signature');
 assert.match(src('docs/client-operations-portfolio.md'),/client retains its name, customers and operating authority/);
 assert.match(src('branding/social-copy.md'),/prepared, not posted/i);
 assert.match(src('branding/README.md'),/not evidence that named market leaders already use Aurekai/);
 assert.match(src('branding/README.md'),/Do not project this copy onto customer-managed/);
});
test('public company landing connects product proof, engine role and all four destinations',()=>{
 const page=src('site/about/index.html');
 assert.match(page,/<title>About Aurekai/);
 assert.match(page,/Built on software.<br><em>Driven by Aurekai.<\/em>/);
 assert.match(page,/Bonfÿre is the native engine/);
 assert.match(page,/studio-product\.png/);
 for(const host of ['aurekai.work','aurekai.life','aurekai.live','aurekai.shop'])
   assert.ok(page.includes('https://'+host+'/'),host+' link missing');
 for(const channel of Object.values(AUREKAI_VOICE.channels))
   assert.ok(page.includes(channel.title),channel.title+' not mapped');
 assert.match(page,/not a claim that particular famous businesses/i);
 assert.ok(src('site/index.html').includes('href="about/"'));
 assert.ok(src('site/explore.html').includes('href="about/"'));
 assert.ok(src('site/discover/index.html').includes('href="../about/"'));
});

test('native production-gallery mirrors the four exact apex texts without claiming activation',()=>{
 const c=src('cmd/BonfyreWebAddress/src/production_home.h');
 assert.match(c,/location\.hostname/);
 assert.match(c,/const territory=location\.hostname\.split/);
 for(const [apex,value] of Object.entries(AUREKAI_VOICE.channels)){
   assert.ok(c.includes("'"+apex+"':"),'missing '+apex+' classifier');
   assert.ok(c.includes(value.title),'missing '+apex+' title');
   assert.ok(c.includes(value.description),'missing '+apex+' specific description');
 }
 assert.match(c,/pathname/);
 assert.match(src('branding/README.md'),/not a live deployment/);
});
