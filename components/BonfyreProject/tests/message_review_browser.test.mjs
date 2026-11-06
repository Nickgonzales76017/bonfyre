import {expect,test} from 'bun:test';
import {writeFileSync} from 'node:fs';
import {dirname,join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';

const report=process.env.MESSAGE_REVIEW_HTML;
const live=report?test:test.skip;
live('exported native review is readable on desktop and phone without remote requests',async()=>{
 const proof=[];
 await withChrome(async({open,events})=>{
  for(const width of [1440,390]){
   const {page,go,sessionId}=await open();
   await page('Emulation.setDeviceMetricsOverride',{width,height:1000,deviceScaleFactor:1,mobile:width<600});
   await go(pathToFileURL(resolve(report)).href);
   const {result}=await page('Runtime.evaluate',{returnByValue:true,expression:`(() => ({title:document.title,articles:document.querySelectorAll('article').length,overflow:document.documentElement.scrollWidth>innerWidth,smallText:[...document.querySelectorAll('p,pre,summary')].some(n=>parseFloat(getComputedStyle(n).fontSize)<12),actions:document.querySelectorAll('form,input,button,script').length,unescaped:document.querySelectorAll('iframe,img').length}))()`});
   expect(result.value.title).toBe('Client proposals for review');expect(result.value.articles).toBeGreaterThan(0);
   expect(result.value.overflow).toBe(false);expect(result.value.smallText).toBe(false);expect(result.value.actions).toBe(0);expect(result.value.unescaped).toBe(0);
   await page('Runtime.evaluate',{expression:`document.querySelectorAll('details').forEach(n=>n.open=true)`});
   const expanded=await page('Runtime.evaluate',{returnByValue:true,expression:'document.documentElement.scrollWidth<=innerWidth'});expect(expanded.result.value).toBe(true);
   await page('Runtime.evaluate',{expression:`document.querySelectorAll('details').forEach(n=>n.open=false)`});
   const shot=await page('Page.captureScreenshot',{format:'png'});writeFileSync(join(dirname(report),'review-'+width+'.png'),Buffer.from(shot.data,'base64'));
   const network=events.filter(e=>e.sessionId===sessionId&&e.method==='Network.requestWillBeSent').map(e=>e.params.request.url);
   expect(network.every(url=>url.startsWith('file:'))).toBe(true);
   proof.push({width,...result.value,expanded_fits:expanded.result.value,network});
  }
 });
 writeFileSync(join(dirname(report),'review-browser-verification.json'),JSON.stringify({at:new Date().toISOString(),report:resolve(report),proof},null,2)+'\n');
},60000);
