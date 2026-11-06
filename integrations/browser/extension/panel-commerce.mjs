import {capturePage} from './capture.mjs';
import {observe} from './observation.mjs';
import {readLocal,saveLocal} from './store.mjs';
const button=document.getElementById('carry'),status=document.getElementById('carry-status');
button.addEventListener('click',async()=>{
  button.disabled=true;status.textContent='Capturing only this selected page…';
  try {
    const [tab]=await chrome.tabs.query({active:true,lastFocusedWindow:true});
    if(!tab?.id)throw Error('Select a website and click the extension icon first.');
    const [result]=await chrome.scripting.executeScript({target:{tabId:tab.id},func:capturePage});
    if(!result?.result)throw Error('No capture returned.');
    const catalog=await(await fetch('./catalog.json')).json(),observation=await observe(result.result,catalog);
    const existing=(await readLocal('observation:')).find(r=>r.key===observation.id);
    if(!existing)await saveLocal(observation.id,observation,0);
    location.href='workbench.html?capture='+encodeURIComponent(observation.id);
  }catch(error){status.textContent=error.message||String(error);button.disabled=false;}
});
