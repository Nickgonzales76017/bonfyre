import {test} from 'node:test';
import assert from 'node:assert/strict';
import {articleOnlyChanges,classify} from '../pages-editorial-scope.mjs';
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
