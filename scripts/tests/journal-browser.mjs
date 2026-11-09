// Real-browser contract for the generated Aurekai Journal, not a screenshot
// gallery or synthetic fixture. Run against the same static site used for Pages.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const origin=process.env.JOURNAL_ORIGIN || 'http://127.0.0.1:8765';
const moduleName=process.env.PLAYWRIGHT_MODULE || 'playwright';
const {chromium}=await import(moduleName);
const expected=readFileSync('site/blog/post-index.tsv','utf8').trimEnd().split('\n').length-1;
assert.ok(expected>=46,'Publication index unexpectedly small');
const browser=await chromium.launch({
  headless:true,executablePath:process.env.CHROME_PATH || undefined,
  args:process.env.CI?['--no-sandbox']:[]
});
const page=await browser.newPage();
const exceptions=[];
page.on('pageerror',e=>exceptions.push(e.message));
try{
  for(const [width,height,columns] of [[1440,900,3],[980,820,2],[390,844,1]]){
    await page.setViewportSize({width,height});
    await page.goto(origin+'/blog/',{waitUntil:'domcontentloaded'});
    assert.equal(await page.locator('[data-article]').count(),expected);
    const info=await page.evaluate(()=>{
      const grid=document.querySelector('[data-journal-grid]');
      const cards=[...document.querySelectorAll('[data-article]')];
      const boxes=cards.map(x=>x.getBoundingClientRect());
      const cardIsComplete=cards.every(x=>x.querySelector('.ed-card-cover')&&
        x.querySelector('.ed-card-content h3')&&x.querySelector('.journal-card-link[href]'));
      return {
        tracks:getComputedStyle(grid).gridTemplateColumns.split(/\s+/).filter(Boolean).length,
        docOverflow:document.documentElement.scrollWidth-innerWidth,
        cardOutside:boxes.filter(b=>b.width<180||b.left<-1||b.right>innerWidth+1).length,
        cardIsComplete, zeroHeight:boxes.filter(b=>b.height<130).length,
        coverTitles:cards.filter(x=>{
          const cover=x.querySelector('.ed-card-feature')?.textContent.trim();
          return cover && cover===x.querySelector('h3')?.textContent.trim();
        }).length
      };
    });
    assert.equal(info.tracks,columns,'Grid columns at '+width);
    assert.ok(info.docOverflow<=2,'Horizontal page overflow at '+width+': '+JSON.stringify(info));
    assert.equal(info.cardOutside,0,'Clipped cards at '+width);
    assert.equal(info.zeroHeight,0,'Collapsed cards at '+width);
    assert.ok(info.cardIsComplete,'Some cards lack a cover, title or link');
    assert.ok(info.coverTitles<=2,'Article covers repeat the same title');
    await page.getByRole('button',{name:'List view'}).click();
    assert.equal(await page.locator('[data-journal-grid].is-list').count(),1);
    assert.equal(await page.locator('[data-journal-view="list"]').getAttribute('aria-pressed'),'true');
    await page.getByRole('button',{name:'Grid view'}).click();
    assert.equal(await page.locator('[data-journal-grid].is-list').count(),0);
  }
  await page.getByRole('button',{name:'List view'}).focus();
  await page.keyboard.press('Enter');
  assert.equal(await page.locator('[data-journal-grid].is-list').count(),1,'List control is keyboard-operable');
  await page.locator('#journal-search').fill('council');
  const narrow=await page.locator('[data-article]:visible').count();
  assert.ok(narrow>0 && narrow<expected);
  assert.match(await page.locator('#journal-result-count').textContent(),new RegExp('Showing '+narrow+' of '+expected));
  await page.locator('#journal-search').fill('');
  await page.locator('[data-filter="Business"]').click();
  assert.equal(await page.locator('[data-article]:visible:not([data-category="Business"])').count(),0);
  assert.equal(await page.locator('[data-journal-grid].is-list').count(),1,'Filter preserves layout');
  await page.goto(origin+'/blog/the-machine-told-me-so/',{waitUntil:'domcontentloaded'});
  assert.equal(await page.locator('h1#article-title').count(),1);
  assert.ok(await page.locator('.ed-chapter-head').count()>=2);
  assert.ok(await page.locator('.journal-source-note').count()===1);
  assert.ok(await page.locator('.journal-conversation-card').count()>=1);
  assert.equal(exceptions.length,0,'Browser JS errors: '+exceptions.join('; '));
  console.log('JOURNAL_BROWSER_PASS '+expected+' editions / responsive 1440, 980, 390 / grid-list / keyboard / search / article');
}finally{
  await browser.close();
}
