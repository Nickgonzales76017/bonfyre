import {test} from 'node:test';
import assert from 'node:assert/strict';
import {articleOnlyChanges,retainedPresentationChanges,classify,currentReleaseScope,verifyRetainedAssets} from '../pages-editorial-scope.mjs';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
test('edited publication and its own renderer may reuse a retained release',()=>{
 assert.equal(articleOnlyChanges([
  'editorial/posts/new.md','editorial/media-sources.tsv',
  'editorial/templates/journal-index.html','site/blog/article-editions.css',
  'scripts/render-editorial.sh','scripts/editorial_publication.py',
  '.github/workflows/journal.yml'
 ]),true);
});
test('native and release changes cannot bypass full studio verification',()=>{
 for(const path of ['site/spaces/RELEASE.tsv','site/index.html','site/blog/index.html',
  'cmd/BonfyreSurface/src/main.c','components/BonfyreServiceSite/index.mjs',
  '.github/workflows/pages.yml','scripts/pages-editorial-scope.mjs',
  'editorial/../site/spaces/index.html']){
  assert.equal(articleOnlyChanges(['editorial/posts/x.md',path]),false,path);
 }
});
test('manual dispatch, missing receipt or empty changes cannot use fast path',()=>{
 assert.equal(classify({event:'workflow_dispatch',changed:['editorial/posts/x.md'],releaseFound:true}),false);
 assert.equal(classify({changed:['editorial/posts/x.md'],releaseFound:false}),false);
 assert.equal(classify({changed:[],releaseFound:true}),false);
 assert.equal(classify({changed:['editorial/posts/x.md'],releaseFound:true}),true);
});

test('site and native brand sources can reuse cryptographically verified studio releases',()=>{
  const changed=[
    'site/index.html','site/about/index.html','site/explore.html',
    'site/discover/index.html','site/market.html','site/fieldwork/index.html',
    'site/matrix/index.html','site/matrix/front-brand.css',
    'branding/voice.mjs','branding/social-copy.md','branding/README.md',
    'cmd/BonfyreWebAddress/src/front_brand.h',
    'cmd/BonfyreWebAddress/src/page_core_rendering.h',
    'cmd/BonfyreWebAddress/generated/native_brand_cuts.h',
    'cmd/BonfyreWebAddress/tools/compile_brand_cuts.mjs',
    'cmd/BonfyreWebAddress/tests/front_arrival_test.sh',
    'scripts/tests/aurekai-brand.test.mjs'
  ];
  assert.equal(retainedPresentationChanges(changed),true);
  assert.equal(classify({event:'push',changed,releaseFound:true}),true);
  assert.equal(articleOnlyChanges(changed),false,'editorial classification stays narrow');
});
test('no shared production, retained output, CMS, packages or release policy enters static reuse',()=>{
  const forbidden=[
    'site/spaces/RELEASE.tsv','site/spaces/catalogue/index.html',
    'site/instruments/SOURCE.tsv','site/instruments/index.html',
    'packs/aurekai-work-samples/site/content.json',
    'components/BonfyreServiceSite/site-renderer.mjs',
    'scripts/upgrade-spaces.mjs','scripts/build-instruments.mjs',
    'scripts/prepare-spaces-discovery.mjs','scripts/prepare-instruments-discovery.mjs',
    'scripts/retain-tested-pages.mjs','scripts/pages-editorial-scope.mjs',
    '.github/workflows/pages.yml',
    'cmd/BonfyreWebAddress/src/main.c',
    'cmd/BonfyreCMS/content-types/site_page.json',
    'site/index.html/../spaces/index.html',
    'site/other/index.html','branding/evil.mjs','editorial/../site/index.html',
    ''
  ];
  for(const path of forbidden){
    assert.equal(retainedPresentationChanges(['site/about/index.html',path]),false,path);
    assert.equal(classify({changed:[path],releaseFound:true}),false,path);
  }
  assert.equal(classify({event:'workflow_dispatch',changed:['site/about/index.html'],releaseFound:true}),false);
});

function releaseFixture(t){
  const root=mkdtempSync(join(tmpdir(),'aurekai-retained-'));
  t.after(()=>rmSync(root,{recursive:true,force:true}));
  const put=(path,content)=>{const filename=join(root,path);mkdirSync(dirname(filename),{recursive:true});writeFileSync(filename,content);};
  const rows=(prefix,manifest,fields)=>{
    const data=[fields.join('\t')];
    for(let i=0;i<5;i++){
      const name='asset-'+i+'.html',body='<p>Verified '+prefix+' '+i+'</p>\n';
      put(prefix+'/'+name,body);
      const sha=createHash('sha256').update(body).digest('hex');
      data.push(fields.length===3?
        [name,sha,Buffer.byteLength(body)].join('\t'):
        ['fake-source-'+i,prefix=== 'site/instruments'?name:'',sha,Buffer.byteLength(body)].join('\t'));
    }
    put(manifest,data.join('\n')+'\n');
  };
  put('site/index.html','<!doctype html><h1>Current brand</h1>\n');
  put('site/spaces/catalogue/index.html','<!doctype html><h1>Catalogue</h1>\n');
  rows('site/spaces','site/spaces/RELEASE.tsv',['path','sha256','bytes']);
  rows('site/instruments','site/instruments/SOURCE.tsv',['source','projection','sha256','bytes']);
  const git=(args,extra={})=>{
    const cmd=spawnSync('git',args,{cwd:root,encoding:'utf8',timeout:10000,
      env:{...process.env,...extra,GIT_CONFIG_NOSYSTEM:'1'}});
    assert.equal(cmd.status,0,cmd.stderr||cmd.stdout);
  };
  git(['init','-q']);
  git(['config','user.name','Fixture Author']);
  git(['config','user.email','author@example.test']);
  git(['add','.']);
  git(['commit','-qm','release(spaces): retain verified studio and instrument projections'],{
    GIT_AUTHOR_NAME:'github-actions[bot]',
    GIT_AUTHOR_EMAIL:'41898282+github-actions[bot]@users.noreply.github.com'
  });
  return {root,put,git};
}
test('real Git release reuses hashed retained studio only for audited brand source changes',t=>{
  const f=releaseFixture(t);
  f.put('site/about/index.html','<!doctype html><h1>About Aurekai</h1>\n');
  f.put('branding/voice.mjs','export const signature="Built on software. Driven by Aurekai.";\n');
  f.git(['add','.']);f.git(['commit','-qm','Improve corporate experience and social source']);
  const state=currentReleaseScope({cwd:f.root,event:'push'});
  assert.equal(state.editorial_only,true,JSON.stringify(state));
  assert.equal(state.receipts.spaces,5);
  assert.equal(state.receipts.instruments,5);
  const manual=currentReleaseScope({cwd:f.root,event:'workflow_dispatch'});
  assert.equal(manual.editorial_only,false);
});
test('drifted retained bytes force full verification, even with brand-only Git history',t=>{
  const f=releaseFixture(t);
  f.put('site/about/index.html','<h1>About Aurekai</h1>');
  f.git(['add','.']);f.git(['commit','-qm','Improve About']);
  assert.equal(currentReleaseScope({cwd:f.root,event:'push'}).editorial_only,true);
  f.put('site/spaces/asset-2.html','<p>Changed without a new receipt</p>');
  const denied=currentReleaseScope({cwd:f.root,event:'push'});
  assert.equal(denied.editorial_only,false);
  assert.match(denied.reason,/artifact verification refused/);
  assert.throws(()=>verifyRetainedAssets(f.root),/changed/);
});
test('one changed studio builder prevents reuse even after valid published branding',t=>{
  const f=releaseFixture(t);
  f.put('branding/voice.mjs','export const signature="Aurekai";');
  f.put('scripts/upgrade-spaces.mjs','// shared studio builder changed\n');
  f.git(['add','.']);f.git(['commit','-qm','Brand and shared studio changed']);
  const state=currentReleaseScope({cwd:f.root,event:'push'});
  assert.equal(state.editorial_only,false);
  assert.match(state.reason,/non-presentation/);
});
