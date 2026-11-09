import {test} from 'node:test';
import assert from 'node:assert/strict';
import {articleOnlyChanges,retainedPresentationChanges,classify} from '../pages-editorial-scope.mjs';
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
