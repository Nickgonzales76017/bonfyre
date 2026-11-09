import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const driver=process.env.PLAYWRIGHT_MODULE;
if(!driver)throw Error('PLAYWRIGHT_MODULE must point to the pinned browser dependency');
const {chromium}=await import(pathToFileURL(driver).href);
const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_PATH,args:['--no-sandbox']});
const page=await browser.newPage();
const errors=[];
page.on('pageerror',err=>errors.push(err.message));
try{
  for(const width of [390,768,1440]){
    await page.setViewportSize({width,height:900});
    await page.goto('http://127.0.0.1:8765/blog/',{waitUntil:'domcontentloaded'});
    const all=page.locator('[data-journal-grid] > [data-article]');
    assert.equal(await all.count(),66,'all 66 editorial records should render');
    assert.equal(await page.locator('a.journal-card-link').count(),66,'one complete link per article');
    assert.equal(await page.locator('[data-cover-kind]').count(),66,'every article has an edition cover');
    assert.equal(await page.locator('[data-journal-grid] [data-article]:visible').count(),66);
    const geometry=await page.locator('[data-journal-grid]').evaluate((grid)=>{
      const cards=[...grid.querySelectorAll('[data-article]')];
      const rect=grid.getBoundingClientRect();
      return {viewport:window.innerWidth,scroll:document.documentElement.scrollWidth,gridWidth:rect.width,
        broken:cards.filter(card=>{const b=card.getBoundingClientRect();return b.width<180 || b.left<rect.left-2 || b.right>rect.right+2;}).length};
    });
    assert.equal(geometry.broken,0,'clipped or undersized journal cards at '+width);
    assert.ok(geometry.scroll<=width+3,'horizontal overflow at '+width+': '+JSON.stringify(geometry));
    await page.locator('[data-journal-view="list"]').click();
    assert.equal(await page.locator('[data-journal-grid].is-list').count(),1);
    assert.equal(await page.locator('[data-journal-view="list"][aria-pressed="true"]').count(),1);
    assert.equal(await page.locator('[data-journal-grid] [data-article]:visible').count(),66);
    const cats=await page.locator('[data-article]').evaluateAll(rows=>[...new Set(rows.map(n=>n.dataset.category))]);
    assert.ok(cats.length>=6);
    const chosen=cats[0];
    await page.locator('[data-filter]').filter({hasText:chosen}).first().click();
    const remaining=await page.locator('[data-journal-grid] [data-article]:visible').count();
    assert.ok(remaining>0 && remaining<66,'category filter did not reduce cards');
    await page.locator('[data-filter="All"]').click();
    await page.locator('#journal-search').fill('stable identifiers');
    const matches=await page.locator('[data-journal-grid] [data-article]:visible').count();
    assert.ok(matches>0 && matches<66,'search filter does not constrain result set');
    await page.locator('#journal-search').fill('');
    assert.equal(await page.locator('[data-journal-grid] [data-article]:visible').count(),66,'clear search restores all cards');
    assert.equal(await page.locator('#journal-result-count').textContent().then(s=>s.includes('66')),true);
  }
  assert.deepEqual(errors,[],'journal page must not raise client errors');
  console.log('EDITORIAL_DIRECTORY_BROWSER_PASS 66 articles / 3 viewport sizes / filters / list / no card clipping');
}finally{await browser.close();}
